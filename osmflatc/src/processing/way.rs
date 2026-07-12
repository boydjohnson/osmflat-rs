use super::{finalize_bulk_cfs, write_batch_no_wal, WriteBatchInternal};
use crate::error::OsmFlatcError;
use crate::{
    add_string_table,
    osmpbf::{self, read_block, BlockIndex},
    processing::{
        node::storage::NodeIdToIdxTDC,
        storage::{EmptyValue, OrdinalKey, OsmIdKey, OsmIdxValue, OsmKey, RefKey},
        NodeLocations, RocksDB, RocksDBUnsync, TempDataCodec,
    },
    stats::{MissingRefs, Stats},
    strings::StringTable,
    Error, PhaseTimer, Progress, TagSerializer, BATCH_SIZE,
};
use geo::{BoundingRect, MultiPoint};
use log::info;
use parking_lot::Mutex;
use rayon::prelude::*;
use rocksdb::{statistics::Ticker, Options, DB};
use storage::{
    ResolvedRefTDC, WayIdToIdxTDC, WayIdToMbbTDC, WayMbbValue, WayRefByNodeTDC, WayTDC, WayValue,
};

pub(crate) mod storage;

fn serialize_ways(
    block: &osmpbf::PrimitiveBlock,
    batch: &mut impl RocksDBUnsync,
    node_locations: &NodeLocations,
    stringtable: &Mutex<StringTable>,
    coord_scale: i32,
) -> Result<Stats, OsmFlatcError> {
    let mut stats = Stats::default();

    let curve = osmflat::way_curve();

    // Shared across the worker threads; hold the lock only for the per-block
    // string interning, not the per-way serialization below.
    let string_refs = {
        let mut guard = stringtable.lock();
        add_string_table(&block.stringtable, &mut guard)?
    };

    for group in &block.primitivegroup {
        for pbf_way in &group.ways {
            debug_assert_eq!(pbf_way.keys.len(), pbf_way.vals.len(), "invalid input data");

            let mut key_vals = vec![];

            for i in 0..pbf_way.keys.len() {
                key_vals.push((
                    string_refs[pbf_way.keys[i] as usize],
                    string_refs[pbf_way.vals[i] as usize],
                ));
            }

            let mut locations = vec![];
            let mut node_refs = vec![];
            let mut ref_id = 0;
            for r in &pbf_way.refs {
                ref_id += r;

                node_refs.push(ref_id);

                if let Some((lon, lat)) = node_locations.get(ref_id)? {
                    locations.push((
                        lon as f64 / coord_scale as f64,
                        lat as f64 / coord_scale as f64,
                    ));
                }
            }

            let points: MultiPoint<_> = locations.into();

            let mbr = points.bounding_rect();
            if let Some(br) = mbr {
                let min_x = br.min().x;
                let min_y = br.min().y;
                let max_x = br.max().x;
                let max_y = br.max().y;

                let spatial_index = osmflat::bbox_index(&curve, min_x, min_y, max_x, max_y);

                let key = OsmKey::new(spatial_index, pbf_way.id);
                let value = WayValue::new(node_refs, key_vals);

                batch.put::<WayTDC>(key, value);

                let id_key = OsmIdKey::new(pbf_way.id);

                batch.put::<WayIdToMbbTDC>(
                    id_key,
                    WayMbbValue::new(
                        [min_x, min_y, max_x, max_y]
                            .into_iter()
                            .map(|v| (v * coord_scale as f64) as i32)
                            .collect(),
                    ),
                );
            }
        }
        stats.num_ways += group.ways.len();
    }

    Ok(stats)
}

#[allow(clippy::too_many_arguments)]
pub fn serialize_way_blocks(
    builder: &osmflat::OsmBuilder,
    db: &DB,
    db_opts: &Options,
    node_locations: &NodeLocations,
    mut way_ids: Option<flatdata::ExternalVector<osmflat::Id>>,
    way_by_id: Option<flatdata::ExternalVector<osmflat::IdxRef>>,
    blocks: Vec<BlockIndex>,
    data: &[u8],
    tags: &mut TagSerializer,
    stringtable: &mut StringTable,
    stats: &mut Stats,
    missing: &mut MissingRefs,
    coord_scale: i32,
) -> Result<(), Error> {
    let mut ways = builder.start_ways()?;
    let pb = Progress::new(blocks.len() as u64, "Converting ways");
    let mut nodes_index = builder.start_nodes_index()?;

    // The per-member RocksDB node-location lookups dominate this pass, so fan it
    // out across all Rayon workers rather than running on a single consumer. The
    // string table is shared behind a Mutex (locked once per block) and the
    // per-block `stats` are commutative, merged with `try_reduce`. Order is
    // irrelevant: ways are emitted later by iterating RocksDB in sorted key
    // order. `std::mem::take` moves the caller's table in for the duration and
    // it is restored below.
    let string_table = Mutex::new(std::mem::take(stringtable));
    let total = {
        let _t = PhaseTimer::start("ways_convert");
        blocks
            .into_par_iter()
            .map(|idx| -> Result<Stats, OsmFlatcError> {
                let block: osmpbf::PrimitiveBlock = read_block(data, &idx)?;

                let mut batch = WriteBatchInternal::default();
                for cf_name in [WayTDC::NAME, WayIdToMbbTDC::NAME] {
                    let cf = db.cf_handle(cf_name).unwrap();
                    batch.insert_cf(cf_name, cf);
                }

                let block_stats = serialize_ways(
                    &block,
                    &mut batch,
                    node_locations,
                    &string_table,
                    coord_scale,
                )?;

                write_batch_no_wal(db, batch.inner())?;

                pb.inc(1);

                Ok(block_stats)
            })
            .try_reduce(Stats::default, |mut a, b| {
                a += b;
                Ok(a)
            })?
    };
    *stats += total;
    *stringtable = string_table.into_inner();

    pb.finish();
    info!("Ways converted.");

    // Write->read boundary: the spatial-order scan below reads `WayTDC`, and
    // the relation pass point-looks-up `WayIdToMbb`.
    info!("Compacting way column families...");
    finalize_bulk_cfs(db, &[WayTDC::NAME, WayIdToMbbTDC::NAME])?;

    let mut batch = WriteBatchInternal::default();
    let cf = db.cf_handle(WayIdToIdxTDC::NAME).unwrap();
    batch.insert_cf(WayIdToIdxTDC::NAME, cf);

    let pb = Progress::new(
        stats.num_ways as u64,
        "Ordering ways by spatial index order",
    );

    // Resolving each way's node refs to their final archive indices used to be
    // one random RocksDB point lookup (or batched multi_get) per way against
    // `NodeIdToIdx` -- cheap per call, but measured (2026-07-11 bench) at ~70%
    // block-cache hit rate with tens of thousands of genuine cache misses per
    // 100k ways, each costing real disk-seek latency (~0.5-0.7ms/miss), which
    // dominates this pass's wall clock almost entirely (sequential write cost
    // is negligible by comparison). A sort-merge join fixes that structurally
    // instead of trying to make the random access cheaper: build an index of
    // every way-ref keyed by its *referenced node id* (`WayRefByNodeTDC`),
    // then merge-join two sorted, sequential scans -- that index and
    // `NodeIdToIdx` itself -- to resolve every ref with zero random reads.
    // Three passes total, all sequential:
    //   0. scan `WayTDC` once, assign each ref an `ordinal` (its position in the
    //      `nodes_index` array being built) and index it by node id.
    //   1. merge-join the node-id-sorted index against `NodeIdToIdx` -- resolved
    //      indices land in `ResolvedRefTDC`, keyed by `ordinal`.
    //   2. scan `WayTDC` again (same order, so ordinals realign) and merge in
    //      `ResolvedRefTDC` (sorted by ordinal) to do the actual writes.

    // Phase 0: index every way-ref by its referenced node id.
    let t_build = std::time::Instant::now();
    {
        let mut ref_batch = WriteBatchInternal::default();
        let ref_cf = db.cf_handle(WayRefByNodeTDC::NAME).unwrap();
        ref_batch.insert_cf(WayRefByNodeTDC::NAME, ref_cf);
        let mut ordinal: u64 = 0;
        for r in <DB as RocksDB>::iterator::<WayTDC>(db)? {
            let (_way_id, v) = r?;
            for &n in &v.node_refs {
                ref_batch.put::<WayRefByNodeTDC>(RefKey::new(n, ordinal), EmptyValue);
                ordinal += 1;
                if ordinal.is_multiple_of(BATCH_SIZE as u64) {
                    write_batch_no_wal(db, ref_batch.inner())?;
                    ref_batch = WriteBatchInternal::default();
                    let ref_cf = db.cf_handle(WayRefByNodeTDC::NAME).unwrap();
                    ref_batch.insert_cf(WayRefByNodeTDC::NAME, ref_cf);
                }
            }
        }
        write_batch_no_wal(db, ref_batch.inner())?;
        log::debug!(
            "[timing] phase=\"ways_ordering_build_refs\" secs={:.3} total_refs={ordinal}",
            t_build.elapsed().as_secs_f64()
        );
    }
    info!("Compacting way node-ref index...");
    finalize_bulk_cfs(db, &[WayRefByNodeTDC::NAME])?;

    // Phase 1: merge-join the node-id-sorted ref index against `NodeIdToIdx`.
    // Both iterators only ever move forward -- `node_cur` is advanced up to
    // (never past) each ref's node id, so the whole pass is O(refs + nodes)
    // sequential I/O instead of O(refs) random point lookups.
    let t_join = std::time::Instant::now();
    let cache_hit_before = db_opts.get_ticker_count(Ticker::BlockCacheDataHit);
    let cache_miss_before = db_opts.get_ticker_count(Ticker::BlockCacheDataMiss);
    {
        let mut node_iter = <DB as RocksDB>::iterator::<NodeIdToIdxTDC>(db)?;
        let mut node_cur: Option<(OsmIdKey, OsmIdxValue)> = node_iter.next().transpose()?;
        let mut resolved_batch = WriteBatchInternal::default();
        let resolved_cf = db.cf_handle(ResolvedRefTDC::NAME).unwrap();
        resolved_batch.insert_cf(ResolvedRefTDC::NAME, resolved_cf);
        let mut refs_seen: u64 = 0;
        let mut resolved_count: u64 = 0;
        for r in <DB as RocksDB>::iterator::<WayRefByNodeTDC>(db)? {
            let (ref_key, _) = r?;
            while let Some((node_key, _)) = &node_cur {
                if node_key.id < ref_key.target_id {
                    node_cur = node_iter.next().transpose()?;
                } else {
                    break;
                }
            }
            if let Some((node_key, node_val)) = &node_cur {
                if node_key.id == ref_key.target_id {
                    resolved_batch.put::<ResolvedRefTDC>(
                        OrdinalKey::new(ref_key.ordinal),
                        OsmIdxValue::new(node_val.idx),
                    );
                    resolved_count += 1;
                }
                // else `node_key.id > ref_key.target_id`: this ref's node is
                // absent from the archive -- leave it unresolved, same as the
                // old `multi_get` code's `None` for a missing key.
            }
            refs_seen += 1;
            if refs_seen.is_multiple_of(BATCH_SIZE as u64) {
                write_batch_no_wal(db, resolved_batch.inner())?;
                resolved_batch = WriteBatchInternal::default();
                let resolved_cf = db.cf_handle(ResolvedRefTDC::NAME).unwrap();
                resolved_batch.insert_cf(ResolvedRefTDC::NAME, resolved_cf);
            }
        }
        write_batch_no_wal(db, resolved_batch.inner())?;
        log::debug!(
            "[timing] phase=\"ways_ordering_merge_join\" secs={:.3} refs={refs_seen} resolved={resolved_count}",
            t_join.elapsed().as_secs_f64()
        );
    }
    let cache_hit_after = db_opts.get_ticker_count(Ticker::BlockCacheDataHit);
    let cache_miss_after = db_opts.get_ticker_count(Ticker::BlockCacheDataMiss);
    log::debug!(
        "[timing] phase=\"ways_ordering_merge_join_cache\" secs=0.000 block_cache_data_hit={} block_cache_data_miss={}",
        cache_hit_after - cache_hit_before,
        cache_miss_after - cache_miss_before
    );
    info!("Compacting way ref resolution index...");
    finalize_bulk_cfs(db, &[ResolvedRefTDC::NAME])?;

    // Phase 2: re-scan `WayTDC` (ordinals realign with phase 0's since both
    // scans see the same immutable, already-compacted CF in the same order)
    // and merge in `ResolvedRefTDC` -- another forward-only, sequential merge.
    let t_write = std::time::Instant::now();
    let mut resolved_iter = <DB as RocksDB>::iterator::<ResolvedRefTDC>(db)?;
    let mut resolved_cur: Option<(OrdinalKey, OsmIdxValue)> = resolved_iter.next().transpose()?;
    let mut ordinal: u64 = 0;
    for (way_idx, r) in <DB as RocksDB>::iterator::<WayTDC>(db)?.enumerate() {
        let (way_key, v) = r?;
        let way_id = way_key.id;
        let way = ways.grow()?;

        let tag_first_idx = tags.next_index();
        for &(k, val) in &v.key_vals {
            tags.serialize(k, val)?;
        }

        way.set_tag_first_idx(tag_first_idx);
        way.set_ref_first_idx(nodes_index.len() as u64);

        batch.put::<WayIdToIdxTDC>(OsmIdKey::new(way_id), OsmIdxValue::new(way_idx as u64));

        for &n in &v.node_refs {
            while let Some((key, _)) = &resolved_cur {
                if key.ordinal < ordinal {
                    resolved_cur = resolved_iter.next().transpose()?;
                } else {
                    break;
                }
            }
            let resolved_idx = match &resolved_cur {
                Some((key, val)) if key.ordinal == ordinal => Some(val.idx),
                _ => None,
            };
            if resolved_idx.is_none() {
                // A node referenced by a way but absent from the archive.
                missing.nodes_in_ways.insert(n);
            }
            nodes_index.grow()?.set_value(resolved_idx);
            ordinal += 1;
        }

        if let Some(ids) = &mut way_ids {
            ids.grow()?.set_value(way_id as u64);
        }

        pb.inc(1);

        if way_idx % BATCH_SIZE == 0 {
            write_batch_no_wal(db, batch.inner())?;
            batch = WriteBatchInternal::default();
            let cf = db.cf_handle(WayIdToIdxTDC::NAME).unwrap();
            batch.insert_cf(WayIdToIdxTDC::NAME, cf);
        }
    }
    log::debug!(
        "[timing] phase=\"ways_ordering_final_write\" secs={:.3}",
        t_write.elapsed().as_secs_f64()
    );

    write_batch_no_wal(db, batch.inner())?;

    pb.finish();

    // Write->read boundary: the reverse-id scan below and the relation pass
    // read `WayIdToIdx`.
    info!("Compacting way id->idx column family...");
    finalize_bulk_cfs(db, &[WayIdToIdxTDC::NAME])?;

    {
        let sentinel = ways.grow()?;
        sentinel.set_tag_first_idx(tags.next_index());
        sentinel.set_ref_first_idx(nodes_index.len() as u64);
    }
    ways.close()?;
    if let Some(ids) = way_ids {
        ids.close()?;
    }
    nodes_index.close()?;

    // Reverse index: `WayIdToIdx` is keyed by OSM id (big-endian), so iterating
    // it yields `(id, final_idx)` in ascending-id order. Emitting just the index
    // gives a permutation `p` with `ids.ways[p[k]]` ascending by id, which the
    // query side binary-searches. See the node path for the rationale.
    if let Some(mut by_id) = way_by_id {
        for r in <DB as RocksDB>::iterator::<WayIdToIdxTDC>(db)? {
            let (_id, idx) = r?;
            by_id.grow()?.set_value(idx.idx);
        }
        by_id.close()?;
    }

    info!("Ways processed");
    Ok(())
}
