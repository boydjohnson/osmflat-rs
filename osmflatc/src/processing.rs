use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::path::Path;

use node::storage::{NodeIdToIdxTDC, NodesTDC};
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, CompactOptions,
    DBRawIterator, Env, MemtableFactory, Options, ReadOptions, WriteBatch, WriteOptions, DB,
};
use tempfile::TempDir;
use way::storage::{
    WayByIdTDC, WayIdToIdxTDC, WayIdToMbbTDC, WayNodeRefTDC, WayNodeResolvedTDC, WayTDC,
};

use crate::error::OsmFlatcError;
use relation::storage::{
    RelationMemberResolvedTDC, RelationNodeMemberRefTDC, RelationWayMemberRefTDC, RELATIONS,
};

#[cfg(any(test, feature = "test-support"))]
pub mod mock;
pub mod node;
pub mod relation;
pub(crate) mod storage;
pub mod way;

/// Forward readahead window for full column-family scans (see
/// `RocksDB::iterator`). Sized in the low single-digit MB per RocksDB's own
/// tuning guidance for sequential scans -- large enough to keep an NVMe's
/// read pipeline full, small enough to not matter on an 8GB laptop.
const READAHEAD_BYTES: usize = 4 * 1024 * 1024;

pub trait Key: for<'a> From<&'a [u8]> {
    /// Append the encoded key to `out`.
    fn serialize_into(&self, out: &mut Vec<u8>);

    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.serialize_into(&mut out);
        out
    }
}

pub trait Value: for<'a> From<&'a [u8]> {
    /// Append the encoded value to `out`.
    fn serialize_into(&self, out: &mut Vec<u8>);

    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.serialize_into(&mut out);
        out
    }
}

pub trait TempDataCodec {
    type Key: Key;
    type Value: Value;
    const NAME: &'static str;
}

pub trait RocksDBUnsync {
    fn put<TDC: TempDataCodec>(&mut self, key: TDC::Key, value: TDC::Value);
}

pub trait RocksDBSync {
    fn get<TDC: TempDataCodec>(&self, key: &TDC::Key) -> Result<Option<TDC::Value>, OsmFlatcError>;

    /// Batched point lookup: one round trip into RocksDB for all `keys`
    /// instead of one per key, sharing the bloom-filter/block-cache setup
    /// cost across the whole batch. Order of the result matches `keys`.
    fn multi_get<TDC: TempDataCodec>(
        &self,
        keys: &[TDC::Key],
    ) -> Result<Vec<Option<TDC::Value>>, OsmFlatcError>;
}

#[derive(Default)]
pub struct WriteBatchInternal<'a> {
    batch: WriteBatch,
    families: BTreeMap<String, &'a ColumnFamily>,
    // Reused encode buffers: `WriteBatch` copies each put, so there is no
    // need for a fresh allocation per key and value.
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
}

impl<'a> WriteBatchInternal<'a> {
    pub fn inner(self) -> WriteBatch {
        self.batch
    }

    pub fn insert_cf(&mut self, name: &str, cf: &'a ColumnFamily) {
        self.families.insert(name.to_owned(), cf);
    }
}

impl<'a> RocksDBUnsync for WriteBatchInternal<'a> {
    fn put<TDC: TempDataCodec>(&mut self, key: TDC::Key, value: TDC::Value) {
        let fam = self.families.get(TDC::NAME).unwrap();
        self.key_buf.clear();
        key.serialize_into(&mut self.key_buf);
        self.value_buf.clear();
        value.serialize_into(&mut self.value_buf);
        self.batch.put_cf(fam, &self.key_buf, &self.value_buf);
    }
}

pub(crate) trait RocksDB {
    #[allow(clippy::type_complexity)]
    fn iterator<'a, TDC: TempDataCodec>(
        &'a self,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(TDC::Key, TDC::Value), OsmFlatcError>> + 'a>,
        OsmFlatcError,
    >
    where
        <TDC as TempDataCodec>::Key: 'a,
        <TDC as TempDataCodec>::Value: 'a;
}

impl RocksDBSync for DB {
    fn get<TDC: TempDataCodec>(&self, key: &TDC::Key) -> Result<Option<TDC::Value>, OsmFlatcError> {
        let cf = self.cf_handle(TDC::NAME).unwrap();

        self.get_cf(cf, key.serialize())
            .map_err(OsmFlatcError::RocksDB)
            .map(|v| v.map(|b| TDC::Value::from(&b[..])))
    }

    fn multi_get<TDC: TempDataCodec>(
        &self,
        keys: &[TDC::Key],
    ) -> Result<Vec<Option<TDC::Value>>, OsmFlatcError> {
        let cf = self.cf_handle(TDC::NAME).unwrap();
        let serialized: Vec<Vec<u8>> = keys.iter().map(Key::serialize).collect();

        self.batched_multi_get_cf(cf, &serialized, false)
            .into_iter()
            .map(|res| {
                res.map_err(OsmFlatcError::RocksDB)
                    .map(|opt| opt.map(|slice| TDC::Value::from(slice.as_ref())))
            })
            .collect()
    }
}

impl RocksDB for DB {
    fn iterator<'a, TDC: TempDataCodec>(
        &'a self,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(TDC::Key, TDC::Value), OsmFlatcError>> + 'a>,
        OsmFlatcError,
    >
    where
        <TDC as TempDataCodec>::Key: 'a,
        <TDC as TempDataCodec>::Value: 'a,
    {
        Ok(Box::new(range_iterator::<TDC>(self, &KeyRange::FULL)))
    }
}

/// A contiguous span of a column family's key space: `lower` inclusive,
/// `upper` exclusive, `None` meaning unbounded.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KeyRange {
    pub lower: Option<Vec<u8>>,
    pub upper: Option<Vec<u8>>,
}

impl KeyRange {
    pub const FULL: KeyRange = KeyRange {
        lower: None,
        upper: None,
    };
}

/// Scan the entries of `TDC`'s column family that fall in `range`, in key
/// order. Bounds are compared bytewise, so a bound may be a key prefix: e.g.
/// the 8-byte encoding of an id bounds both id-keyed families and families
/// whose keys start with that id.
pub(crate) fn range_iterator<'a, TDC: TempDataCodec>(
    db: &'a DB,
    range: &KeyRange,
) -> impl Iterator<Item = Result<(TDC::Key, TDC::Value), OsmFlatcError>> + 'a
where
    TDC::Key: 'a,
    TDC::Value: 'a,
{
    let cf = db.cf_handle(TDC::NAME).unwrap();

    // This drives the ordering passes' column-family scans. With the default
    // `ReadOptions` (readahead disabled) every block is a synchronous fetch
    // at queue depth 1 -- on fast NVMe that leaves most of the drive's
    // IOPS/bandwidth unused and the process CPU-idle, since nothing overlaps
    // the reads. A forward readahead window lets RocksDB prefetch ahead of
    // the iterator instead.
    let mut read_opts = ReadOptions::default();
    read_opts.set_readahead_size(READAHEAD_BYTES);
    if let Some(lower) = &range.lower {
        read_opts.set_iterate_lower_bound(lower.clone());
    }
    if let Some(upper) = &range.upper {
        read_opts.set_iterate_upper_bound(upper.clone());
    }

    let mut raw = db.raw_iterator_cf_opt(cf, read_opts);
    raw.seek_to_first();
    DecodingIter::<TDC::Key, TDC::Value> {
        raw,
        first: true,
        _codec: PhantomData,
    }
}

/// Split a compacted column family's key space into at most `max_ranges`
/// contiguous ranges of roughly equal size, for scanning in parallel.
///
/// Boundaries come from the start keys of the family's SST files (all
/// similarly sized after `finalize_bulk_cfs`), truncated to `prefix_len`
/// bytes so that entries sharing a key prefix -- e.g. all refs to one node
/// id -- always land in the same range. The ranges tile the whole key space
/// whatever the file layout, so balance depends on it but correctness does
/// not.
pub(crate) fn key_ranges(
    db: &DB,
    cf_name: &str,
    prefix_len: usize,
    max_ranges: usize,
) -> Result<Vec<KeyRange>, rocksdb::Error> {
    let mut starts: Vec<Vec<u8>> = db
        .live_files()?
        .into_iter()
        .filter(|f| f.column_family_name == cf_name)
        .filter_map(|f| f.start_key)
        .map(|mut k| {
            k.truncate(prefix_len);
            k
        })
        .collect();
    starts.sort_unstable();
    starts.dedup();
    // The smallest start key begins the first range, which is unbounded below.
    if !starts.is_empty() {
        starts.remove(0);
    }

    let wanted = max_ranges.max(1) - 1;
    let boundaries: Vec<Vec<u8>> = if starts.len() <= wanted {
        starts
    } else {
        (1..=wanted)
            .map(|i| starts[i * starts.len() / (wanted + 1)].clone())
            .collect()
    };

    let mut ranges = Vec::with_capacity(boundaries.len() + 1);
    let mut lower = None;
    for b in boundaries {
        ranges.push(KeyRange {
            lower,
            upper: Some(b.clone()),
        });
        lower = Some(b);
    }
    ranges.push(KeyRange { lower, upper: None });
    Ok(ranges)
}

/// Full-scan iterator that decodes each entry straight from the raw
/// iterator's borrowed key/value slices. The plain RocksDB iterator copies
/// every key and value into a fresh `Box<[u8]>` first; for the ordering
/// passes' scans over hundreds of millions of fixed-width entries that
/// allocate/free pair was the dominant CPU cost.
struct DecodingIter<'a, K, V> {
    raw: DBRawIterator<'a>,
    first: bool,
    _codec: PhantomData<fn() -> (K, V)>,
}

impl<K: Key, V: Value> Iterator for DecodingIter<'_, K, V> {
    type Item = Result<(K, V), OsmFlatcError>;

    fn next(&mut self) -> Option<Self::Item> {
        if !std::mem::take(&mut self.first) {
            if !self.raw.valid() {
                return None;
            }
            self.raw.next();
        }
        match self.raw.item() {
            Some((k, v)) => Some(Ok((K::from(k), V::from(v)))),
            None => self.raw.status().err().map(|e| Err(e.into())),
        }
    }
}

/// Write a batch to the scratch DB with the WAL disabled.
///
/// The scratch DB is a throwaway temporary database, recreated from scratch on
/// every run, so we never rely on the crash-recovery guarantees the write-ahead
/// log provides. Skipping it roughly halves the write I/O of the node and way
/// conversion passes.
pub(crate) fn write_batch_no_wal(db: &DB, batch: WriteBatch) -> Result<(), rocksdb::Error> {
    let mut opts = WriteOptions::default();
    opts.disable_wal(true);
    db.write_opt(batch, &opts)
}

/// Flush and manually compact column families at their write->read boundary.
///
/// The scratch DB runs in bulk-load mode (vector memtables, auto-compaction
/// disabled -- see [`create_db`]), which makes writes cheap but leaves each
/// column family as unsorted memtables plus a pile of overlapping L0 files.
/// Reading in that state would be pathological (point lookups scan every
/// unsorted memtable, iterators heap-merge across every L0 file), so every
/// write pass must call this on the families it wrote before any pass reads
/// them: one flush plus one full-range compaction yields a single sorted run,
/// doing the sorting work once that the skiplist/auto-compaction path would
/// have done continuously. Families compact in parallel, one thread each,
/// on top of RocksDB's own subcompaction parallelism.
pub(crate) fn finalize_bulk_cfs(db: &DB, cf_names: &[&str]) -> Result<(), rocksdb::Error> {
    std::thread::scope(|s| {
        let handles: Vec<_> = cf_names
            .iter()
            .map(|name| {
                s.spawn(move || -> Result<(), rocksdb::Error> {
                    let cf = db.cf_handle(name).unwrap();

                    let t0 = std::time::Instant::now();
                    db.flush_cf(cf)?;
                    log::debug!(
                        "[timing] phase=\"flush_cf({name})\" secs={:.3}",
                        t0.elapsed().as_secs_f64()
                    );

                    let t1 = std::time::Instant::now();
                    let mut opts = CompactOptions::default();
                    // Let the per-family compactions overlap instead of
                    // serializing on the manual-compaction exclusivity gate.
                    opts.set_exclusive_manual_compaction(false);
                    db.compact_range_cf_opt(cf, None::<&[u8]>, None::<&[u8]>, &opts);
                    log::debug!(
                        "[timing] phase=\"compact_cf({name})\" secs={:.3}",
                        t1.elapsed().as_secs_f64()
                    );

                    Ok(())
                })
            })
            .collect();
        for h in handles {
            h.join().expect("finalize_bulk_cfs worker panicked")?;
        }
        Ok(())
    })
}

/// Open the temporary RocksDB used to sort entities into spatial order.
///
/// The database lives in a freshly created temporary directory under
/// `scratch_parent` (chosen by the caller -- ideally a fast SSD with ample
/// free space, since this scratch data is I/O-heavy and can grow very large).
/// The returned [`TempDir`] owns that directory and removes it on drop, so
/// callers must keep it alive at least as long as the returned [`DB`].
///
/// `block_cache_bytes` sizes the shared block cache used across all column
/// families; larger values keep more index/filter blocks (and hot data)
/// resident during the random-read passes. `write_buffer_bytes` sizes each
/// memtable; peak memtable memory is roughly `write_buffer_bytes *
/// MAX_WRITE_BUFFER_NUMBER` per column family being written. Both are kept
/// small by default so the tool runs on modest machines, and scaled up via CLI
/// flags when converting a planet on a workstation.
pub fn create_db(
    scratch_parent: &Path,
    block_cache_bytes: usize,
    write_buffer_bytes: usize,
    max_open_files: i32,
) -> Result<(DB, TempDir, Options), Box<dyn std::error::Error>> {
    let scratch = tempfile::Builder::new()
        .prefix(".osmflatc-scratch-")
        .tempdir_in(scratch_parent)?;

    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    // Immutable memtables allowed to queue per column family before writes
    // stall. This is the real ceiling on *concurrent flush jobs* -- at most
    // two column families are actively written during the node/way bulk-load
    // passes, so `flush_threads` below can never
    // be kept busier than roughly `max_write_buffer_number` per active CF.
    // Scale it with cores so bigger machines can sustain more concurrent
    // flushes, but cap it well below `flush_threads`: each unit costs
    // `write_buffer_bytes` of peak memtable memory per column family, and
    // this tool needs to stay usable on an 8GB laptop by default.
    let max_write_buffer_number = cpus.clamp(2, 8) as i32;

    // The relation-index and ordering passes do huge numbers of random point
    // lookups against a planet-sized DB. Without a bloom filter every miss/hit
    // costs disk seeks, and with the default tiny block cache index/filter
    // blocks get evicted constantly -- that is what leaves the process I/O
    // bound at ~0% CPU. A shared large block cache plus per-SST bloom filters
    // keeps those lookups mostly in memory.
    let mut block_opts = BlockBasedOptions::default();
    let cache = Cache::new_lru_cache(block_cache_bytes);
    block_opts.set_block_cache(&cache);
    block_opts.set_bloom_filter(10.0, false);
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);

    let mut cf_opts = Options::default();
    cf_opts.set_max_write_buffer_number(max_write_buffer_number);
    cf_opts.set_write_buffer_size(write_buffer_bytes);
    cf_opts.set_block_based_table_factory(&block_opts);

    // Every column family follows a strict bulk-load lifecycle: one pass writes
    // it completely, later passes only read it (see `finalize_bulk_cfs`, called
    // at each write->read boundary). Profiling the node pass showed ~45% of all
    // CPU inside skiplist memtable inserts, ordering work we can defer: a
    // vector memtable turns every insert into a plain append and sorts once per
    // flush instead. Reads against an unflushed/uncompacted vector memtable
    // would be pathological, but the lifecycle guarantees none happen before
    // `finalize_bulk_cfs` has flushed and compacted the family.
    cf_opts.set_memtable_factory(MemtableFactory::Vector);
    // Auto-compaction would repeatedly rewrite data we are only going to read
    // after the (cheaper) single manual compaction at the end of the write
    // pass. The L0 stall triggers exist to let auto-compaction catch up, so
    // lift them out of reach too -- otherwise the accumulating L0 files from
    // flushed memtables would stall the bulk writes they are meant to protect.
    cf_opts.set_disable_auto_compactions(true);
    cf_opts.set_level_zero_file_num_compaction_trigger(i32::MAX);
    cf_opts.set_level_zero_slowdown_writes_trigger(i32::MAX);
    cf_opts.set_level_zero_stop_writes_trigger(i32::MAX);

    let cfs = [
        NodesTDC::NAME,
        NodeIdToIdxTDC::NAME,
        WayTDC::NAME,
        WayIdToMbbTDC::NAME,
        WayIdToIdxTDC::NAME,
        // Staging and temporary indexes for the way pass's sort-merge join
        // (see way.rs) -- same bulk-load lifecycle as everything else above:
        // one pass writes each fully, the next only reads it.
        WayByIdTDC::NAME,
        WayNodeRefTDC::NAME,
        WayNodeResolvedTDC::NAME,
        RELATIONS,
        // Temporary indexes for the relation-member ordering pass's
        // sort-merge join (see relation.rs) -- same bulk-load lifecycle.
        RelationNodeMemberRefTDC::NAME,
        RelationWayMemberRefTDC::NAME,
        RelationMemberResolvedTDC::NAME,
    ]
    .iter()
    .map(move |v| ColumnFamilyDescriptor::new(v.to_string(), cf_opts.clone()));

    // On a planet ingest the bottleneck is *flushing* memtables, not
    // compaction: the per-node CFs fill buffers faster than the single default
    // flush thread can write them, so writes stop with every immutable memtable
    // pending flush. RocksDB schedules flushes on the env's HIGH-priority pool
    // and compactions on the LOW pool, so size each pool explicitly (derived
    // from available cores) instead of relying on `increase_parallelism`, which
    // grows only the compaction pool and leaves flushes single-threaded. Flush
    // threads scale to all available cores rather than a fixed cap -- actual
    // flush concurrency during the write pass is still bounded by
    // `max_write_buffer_number` above, so this pool is sized to have headroom
    // rather than to be the limiting factor.
    let flush_threads = cpus.max(2) as i32;
    let compaction_threads = cpus.max(2) as i32;

    let mut env = Env::new()?;
    env.set_high_priority_background_threads(flush_threads);
    env.set_background_threads(compaction_threads);

    let mut db_opts = Options::default();
    db_opts.set_env(&env);
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);
    db_opts.set_max_background_jobs(flush_threads + compaction_threads);
    db_opts.set_max_subcompactions(4);
    // Concurrent memtable writes are only supported by the skiplist memtable;
    // with the vector memtable above, writers must take the write lock one
    // group at a time (an append under the lock is cheap, unlike the skiplist
    // insert this replaces).
    db_opts.set_allow_concurrent_memtable_write(false);
    // By default RocksDB keeps a handle open for every SST file. On a large
    // ingest the scratch DB grows to thousands of SSTs across its column
    // families, which exhausts the process file-descriptor limit (the macOS
    // default is only 256). Cap the table cache so RocksDB bounds its own fd
    // use; the caller derives this from the raised fd limit so it scales up on
    // bigger machines.
    db_opts.set_max_open_files(max_open_files);

    // Cheap ticker/histogram counters (block-cache hit/miss, bytes read, ...),
    // queried later via the returned `Options` to check whether a random-read
    // pass is actually landing in the block cache -- see the way-ordering
    // pass's per-chunk cache-hit logging in way.rs.
    db_opts.enable_statistics();

    let db = DB::open_cf_descriptors(&db_opts, scratch.path(), cfs)?;
    Ok((db, scratch, db_opts))
}

#[cfg(test)]
mod create_db_tests {
    use super::*;

    /// Opens the scratch DB and exercises a write -> flush -> read cycle. The
    /// flush runs on the env's high-priority pool, so this fails (segfault /
    /// error) if the `Env` set on the options is not kept alive past
    /// `create_db`. Also a basic regression guard on the option wiring.
    #[test]
    fn open_write_flush_read() {
        let dir = tempfile::tempdir().unwrap();
        let (db, _scratch, _db_opts) =
            create_db(dir.path(), 8 * 1024 * 1024, 4 * 1024 * 1024, 256).unwrap();

        let cf = db.cf_handle(NodesTDC::NAME).unwrap();
        let key = 42i64.to_be_bytes();
        db.put_cf(cf, key, [1u8, 2, 3, 4, 5, 6, 7, 8]).unwrap();

        // Force a memtable flush so the high-priority flush pool is used.
        db.flush_cf(cf).unwrap();

        let got = db.get_cf(cf, key).unwrap();
        assert_eq!(got.as_deref(), Some(&[1u8, 2, 3, 4, 5, 6, 7, 8][..]));
    }

    /// The decoding full-scan iterator yields every entry once, in key order,
    /// and stays exhausted; an empty column family yields nothing.
    #[test]
    fn iterator_scans_in_key_order() {
        use node::storage::NodeIdxLocValue;
        use storage::OsmIdKey;

        let dir = tempfile::tempdir().unwrap();
        let (db, _scratch, _db_opts) =
            create_db(dir.path(), 8 * 1024 * 1024, 4 * 1024 * 1024, 256).unwrap();

        assert!(<DB as RocksDB>::iterator::<NodeIdToIdxTDC>(&db)
            .unwrap()
            .next()
            .is_none());

        let mut batch = WriteBatchInternal::default();
        batch.insert_cf(NodeIdToIdxTDC::NAME, db.cf_handle(NodeIdToIdxTDC::NAME).unwrap());
        for id in [30, 10, 20] {
            batch.put::<NodeIdToIdxTDC>(
                OsmIdKey::new(id),
                NodeIdxLocValue::new(id as u64 * 2, 0, 0),
            );
        }
        write_batch_no_wal(&db, batch.inner()).unwrap();
        finalize_bulk_cfs(&db, &[NodeIdToIdxTDC::NAME]).unwrap();

        let mut iter = <DB as RocksDB>::iterator::<NodeIdToIdxTDC>(&db).unwrap();
        let got: Vec<(i64, u64)> = iter
            .by_ref()
            .map(|r| r.map(|(k, v)| (k.id, v.idx)).unwrap())
            .collect();
        assert_eq!(got, vec![(10, 20), (20, 40), (30, 60)]);
        assert!(iter.next().is_none());
    }

    /// `key_ranges` splits at SST start keys (truncated to the prefix), and
    /// scanning its ranges visits every entry exactly once, in order -- with
    /// all entries sharing a prefix in the same range.
    #[test]
    fn key_ranges_tile_the_key_space() {
        use way::storage::{WayNodeRefKey, WayNodeRefTDC};

        let dir = tempfile::tempdir().unwrap();
        let (db, _scratch, _db_opts) =
            create_db(dir.path(), 8 * 1024 * 1024, 4 * 1024 * 1024, 256).unwrap();
        let cf = db.cf_handle(WayNodeRefTDC::NAME).unwrap();

        // Three flushes of disjoint node-id spans give three SST files. Each
        // node id is referenced by two ways so prefix grouping is exercised.
        let mut expected = Vec::new();
        for span in [0..100i64, 100..200, 200..300] {
            let mut batch = WriteBatchInternal::default();
            batch.insert_cf(WayNodeRefTDC::NAME, cf);
            for node_id in span {
                for way_id in [7, 9] {
                    let key = WayNodeRefKey::new(node_id, way_id, 0);
                    batch.put::<WayNodeRefTDC>(key, storage::EmptyValue);
                    expected.push(key);
                }
            }
            write_batch_no_wal(&db, batch.inner()).unwrap();
            db.flush_cf(cf).unwrap();
        }

        let ranges = key_ranges(&db, WayNodeRefTDC::NAME, 8, 16).unwrap();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].lower, None);
        assert_eq!(ranges[0].upper, Some(100i64.to_be_bytes().to_vec()));
        assert_eq!(ranges[2].upper, None);

        let scanned: Vec<WayNodeRefKey> = ranges
            .iter()
            .flat_map(|r| range_iterator::<WayNodeRefTDC>(&db, r).map(|e| e.unwrap().0))
            .collect();
        assert_eq!(scanned, expected);

        // Capping the range count still tiles the space.
        let capped = key_ranges(&db, WayNodeRefTDC::NAME, 8, 2).unwrap();
        assert_eq!(capped.len(), 2);
        let total: usize = capped
            .iter()
            .map(|r| range_iterator::<WayNodeRefTDC>(&db, r).count())
            .sum();
        assert_eq!(total, expected.len());
    }
}
