use std::collections::BTreeMap;
use std::path::Path;

use node::storage::{NodeIdToIdxTDC, NodeIdToLonLatTDC, NodesTDC};
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, CompactOptions, Env,
    IteratorMode, MemtableFactory, Options, WriteBatch, WriteOptions, DB,
};
use tempfile::TempDir;
use way::storage::{WayIdToIdxTDC, WayIdToMbbTDC, WayTDC};

use crate::error::OsmFlatcError;
use relation::storage::{RELATIONS, RELATIONS_STRING_REFS};

#[cfg(any(test, feature = "test-support"))]
pub mod mock;
pub mod node;
pub mod relation;
pub(crate) mod storage;
pub mod way;

pub trait Key: From<Box<[u8]>> {
    fn serialize(&self) -> Vec<u8>;
}

pub trait Value: From<Box<[u8]>> {
    fn serialize(&self) -> Vec<u8>;
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
}

#[derive(Default)]
pub struct WriteBatchInternal<'a> {
    batch: WriteBatch,
    families: BTreeMap<String, &'a ColumnFamily>,
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
        self.batch.put_cf(fam, key.serialize(), value.serialize());
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
            .map(|v| v.map(|b| TDC::Value::from(b.into())))
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
        let cf = self.cf_handle(TDC::NAME).unwrap();

        let iter = self.iterator_cf(cf, IteratorMode::Start);
        Ok(Box::new(iter.map(|res| {
            res.map_err(OsmFlatcError::RocksDB)
                .map(|(k, v)| (TDC::Key::from(k), TDC::Value::from(v)))
        })))
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
                    db.flush_cf(cf)?;
                    let mut opts = CompactOptions::default();
                    // Let the per-family compactions overlap instead of
                    // serializing on the manual-compaction exclusivity gate.
                    opts.set_exclusive_manual_compaction(false);
                    db.compact_range_cf_opt(cf, None::<&[u8]>, None::<&[u8]>, &opts);
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
) -> Result<(DB, TempDir), Box<dyn std::error::Error>> {
    /// Immutable memtables allowed to queue per column family before writes
    /// stall. Kept modest so peak memory stays bounded; flush throughput (see
    /// below) is what actually keeps the queue drained.
    const MAX_WRITE_BUFFER_NUMBER: i32 = 4;

    let scratch = tempfile::Builder::new()
        .prefix(".osmflatc-scratch-")
        .tempdir_in(scratch_parent)?;

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
    cf_opts.set_max_write_buffer_number(MAX_WRITE_BUFFER_NUMBER);
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
        NodeIdToLonLatTDC::NAME,
        NodeIdToIdxTDC::NAME,
        WayTDC::NAME,
        WayIdToMbbTDC::NAME,
        WayIdToIdxTDC::NAME,
        RELATIONS,
        RELATIONS_STRING_REFS,
    ]
    .iter()
    .map(move |v| ColumnFamilyDescriptor::new(v.to_string(), cf_opts.clone()));

    // On a planet ingest the bottleneck is *flushing* memtables, not
    // compaction: the per-node CFs fill buffers faster than the single default
    // flush thread can write them, so writes stop with every immutable memtable
    // pending flush. RocksDB schedules flushes on the env's HIGH-priority pool
    // and compactions on the LOW pool, so size each pool explicitly (derived
    // from available cores) instead of relying on `increase_parallelism`, which
    // grows only the compaction pool and leaves flushes single-threaded.
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let flush_threads = (cpus / 4).clamp(2, 4) as i32;
    let compaction_threads = cpus.clamp(2, 16) as i32;

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

    let db = DB::open_cf_descriptors(&db_opts, scratch.path(), cfs)?;
    Ok((db, scratch))
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
        let (db, _scratch) = create_db(dir.path(), 8 * 1024 * 1024, 4 * 1024 * 1024, 256).unwrap();

        let cf = db.cf_handle(NodeIdToLonLatTDC::NAME).unwrap();
        let key = 42i64.to_be_bytes();
        db.put_cf(cf, key, [1u8, 2, 3, 4, 5, 6, 7, 8]).unwrap();

        // Force a memtable flush so the high-priority flush pool is used.
        db.flush_cf(cf).unwrap();

        let got = db.get_cf(cf, key).unwrap();
        assert_eq!(got.as_deref(), Some(&[1u8, 2, 3, 4, 5, 6, 7, 8][..]));
    }
}
