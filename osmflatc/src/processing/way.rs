use super::{finalize_bulk_cfs, write_batch_no_wal, WriteBatchInternal};
use crate::error::OsmFlatcError;
use crate::{
    add_string_table,
    osmpbf::{self, read_block, BlockIndex},
    processing::{
        node::storage::{NodeIdToIdxTDC, NodeIdxLocValue},
        key_ranges, range_iterator,
        storage::{EmptyValue, OsmIdKey, OsmIdxValue, OsmKey},
        KeyRange, RocksDB, RocksDBUnsync, TempDataCodec,
    },
    stats::{MissingRefs, Stats},
    strings::StringTable,
    Error, PhaseTimer, Progress, TagSerializer, BATCH_SIZE,
};
use log::info;
use parking_lot::Mutex;
use rayon::prelude::*;
use rocksdb::DB;
use space_time::xzorder::xz2_sfc::XZ2SFC;
use storage::{
    ResolvedNodeValue, ResolvedWayValue, WayByIdTDC, WayIdToIdxTDC, WayIdToMbbTDC, WayMbbValue,
    WayNodeRefKey, WayNodeRefTDC, WayNodeResolvedTDC, WayPosKey, WayTDC, WayValue,
};

pub(crate) mod storage;

/// Stage a way for the node join: the way itself keyed by id, plus one
/// node-id-keyed index entry per node ref. No node lookups happen here.
fn serialize_ways(
    block: &osmpbf::PrimitiveBlock,
    batch: &mut impl RocksDBUnsync,
    stringtable: &Mutex<StringTable>,
) -> Result<Stats, OsmFlatcError> {
    let mut stats = Stats::default();

    // Shared across the worker threads; hold the lock only for the per-block
    // string interning, not the per-way serialization below.
    let string_refs = {
        let mut guard = stringtable.lock();
        add_string_table(&block.stringtable, &mut guard)?
    };

    for group in &block.primitivegroup {
        for pbf_way in &group.ways {
            debug_assert_eq!(pbf_way.keys.len(), pbf_way.vals.len(), "invalid input data");

            let key_vals = pbf_way
                .keys
                .iter()
                .zip(&pbf_way.vals)
                .map(|(&k, &v)| (string_refs[k as usize], string_refs[v as usize]))
                .collect();

            let mut node_refs = Vec::with_capacity(pbf_way.refs.len());
            let mut ref_id = 0;
            for (pos, r) in pbf_way.refs.iter().enumerate() {
                ref_id += r;
                node_refs.push(ref_id);
                batch.put::<WayNodeRefTDC>(
                    WayNodeRefKey::new(ref_id, pbf_way.id, pos as u32),
                    EmptyValue,
                );
            }

            batch.put::<WayByIdTDC>(
                OsmIdKey::new(pbf_way.id),
                WayValue::new(node_refs, key_vals),
            );
        }
        stats.num_ways += group.ways.len();
    }

    Ok(stats)
}

/// Resolve a way's nodes into its bounding box (in degrees), its spatial key,
/// and the value written to `WayTDC`. `resolved` holds the join output for
/// this way, in position order; refs with no row resolved to nothing.
/// Returns `None` for a way with no locatable node, which is dropped.
fn resolve_way(
    curve: &XZ2SFC,
    way_id: i64,
    way: WayValue,
    resolved: &[(u32, ResolvedNodeValue)],
    coord_scale: i32,
) -> Option<(OsmKey, ResolvedWayValue, WayMbbValue)> {
    let scale = coord_scale as f64;
    let mut node_idxs = vec![None; way.node_refs.len()];
    let mut bbox: Option<(f64, f64, f64, f64)> = None;
    for &(pos, node) in resolved {
        if let Some(slot) = node_idxs.get_mut(pos as usize) {
            *slot = Some(node.idx);
        }
        let (x, y) = (node.lon as f64 / scale, node.lat as f64 / scale);
        bbox = Some(match bbox {
            None => (x, y, x, y),
            Some((min_x, min_y, max_x, max_y)) => {
                (min_x.min(x), min_y.min(y), max_x.max(x), max_y.max(y))
            }
        });
    }
    let (min_x, min_y, max_x, max_y) = bbox?;

    let missing_node_ids = way
        .node_refs
        .iter()
        .zip(&node_idxs)
        .filter(|(_, idx)| idx.is_none())
        .map(|(&n, _)| n)
        .collect();

    let spatial_index = osmflat::bbox_index(curve, min_x, min_y, max_x, max_y);
    let mbb = [min_x, min_y, max_x, max_y]
        .into_iter()
        .map(|v| (v * scale) as i32)
        .collect();

    Some((
        OsmKey::new(spatial_index, way_id),
        ResolvedWayValue::new(node_idxs, missing_node_ids, way.key_vals),
        WayMbbValue::new(mbb),
    ))
}

/// Advance a forward-only node-id-keyed scan until `cur` is the first entry
/// with id >= `id`. Callers must ask for non-decreasing ids.
fn seek_forward<V>(
    iter: &mut impl Iterator<Item = Result<(OsmIdKey, V), OsmFlatcError>>,
    cur: &mut Option<(OsmIdKey, V)>,
    id: i64,
) -> Result<(), OsmFlatcError> {
    while matches!(cur, Some((key, _)) if key.id < id) {
        *cur = iter.next().transpose()?;
    }
    Ok(())
}

/// Number of key ranges a parallel scan is split into: several per worker,
/// so an unevenly dense range does not leave the other workers idle.
fn parallel_scan_ranges() -> usize {
    rayon::current_num_threads() * 4
}

/// Phase 1 of the way pass (see `serialize_way_blocks`): merge-join the
/// node-id-sorted ref index against `NodeIdToIdx` (index and location per
/// node), writing each resolved ref keyed by (way id, position).
///
/// Every input is sorted by node id, so the node-id space is split into
/// ranges joined independently in parallel; a node id's refs and its node
/// entries always fall in the same range. Output order does not matter, since
/// the output family is compacted before it is read. Kept out of line so
/// profiles attribute it separately.
#[inline(never)]
fn join_way_node_refs(db: &DB) -> Result<(), OsmFlatcError> {
    let _t = PhaseTimer::start("ways_join_nodes");
    let ranges = key_ranges(db, WayNodeRefTDC::NAME, 8, parallel_scan_ranges())?;
    let (refs_seen, resolved_count) = ranges
        .par_iter()
        .map(|range| join_way_node_refs_range(db, range))
        .try_reduce(|| (0, 0), |a, b| Ok((a.0 + b.0, a.1 + b.1)))?;
    log::debug!(
        "[ways_join_nodes] ranges={} refs={refs_seen} resolved={resolved_count}",
        ranges.len()
    );
    Ok(())
}

/// Join the refs whose node id falls in `range`; returns (refs, resolved).
fn join_way_node_refs_range(db: &DB, range: &KeyRange) -> Result<(u64, u64), OsmFlatcError> {
    let mut node_iter = range_iterator::<NodeIdToIdxTDC>(db, range);
    let mut node_cur: Option<(OsmIdKey, NodeIdxLocValue)> = node_iter.next().transpose()?;

    let resolved_cf = db.cf_handle(WayNodeResolvedTDC::NAME).unwrap();
    let mut batch = WriteBatchInternal::default();
    batch.insert_cf(WayNodeResolvedTDC::NAME, resolved_cf);
    let mut refs_seen: u64 = 0;
    let mut resolved_count: u64 = 0;
    for r in range_iterator::<WayNodeRefTDC>(db, range) {
        let (ref_key, _) = r?;
        let node_id = ref_key.node_id;

        seek_forward(&mut node_iter, &mut node_cur, node_id)?;
        // A node absent from the archive gets no row: the ref is simply
        // unresolved when its way is assembled.
        if let Some((key, node)) = &node_cur {
            if key.id == node_id {
                batch.put::<WayNodeResolvedTDC>(
                    WayPosKey::new(ref_key.way_id, ref_key.pos),
                    ResolvedNodeValue::new(node.idx, node.lon, node.lat),
                );
                resolved_count += 1;
            }
        }
        refs_seen += 1;
        if refs_seen.is_multiple_of(BATCH_SIZE as u64) {
            write_batch_no_wal(db, batch.inner())?;
            batch = WriteBatchInternal::default();
            batch.insert_cf(WayNodeResolvedTDC::NAME, resolved_cf);
        }
    }
    write_batch_no_wal(db, batch.inner())?;
    Ok((refs_seen, resolved_count))
}

/// Phase 2 of the way pass (see `serialize_way_blocks`): merge staged ways
/// with their resolved refs, compute spatial keys, and write `WayTDC` and
/// `WayIdToMbbTDC`.
///
/// Both inputs are sorted by way id, so the way-id space is split into ranges
/// processed independently in parallel, like the join. Kept out of line so
/// profiles attribute it separately.
#[inline(never)]
fn compute_way_bboxes(db: &DB, num_ways: usize, coord_scale: i32) -> Result<(), OsmFlatcError> {
    let _t = PhaseTimer::start("ways_bbox");
    let pb = Progress::new(num_ways as u64, "Computing way bounding boxes");
    let ranges = key_ranges(db, WayNodeResolvedTDC::NAME, 8, parallel_scan_ranges())?;
    ranges
        .par_iter()
        .try_for_each(|range| compute_way_bboxes_range(db, range, coord_scale, &pb))?;
    pb.finish();
    Ok(())
}

/// Compute bounding boxes for the ways whose id falls in `range`.
fn compute_way_bboxes_range(
    db: &DB,
    range: &KeyRange,
    coord_scale: i32,
    pb: &Progress,
) -> Result<(), OsmFlatcError> {
    let curve = osmflat::way_curve();

    let way_cf = db.cf_handle(WayTDC::NAME).unwrap();
    let mbb_cf = db.cf_handle(WayIdToMbbTDC::NAME).unwrap();
    let new_batch = || {
        let mut batch = WriteBatchInternal::default();
        batch.insert_cf(WayTDC::NAME, way_cf);
        batch.insert_cf(WayIdToMbbTDC::NAME, mbb_cf);
        batch
    };
    let mut batch = new_batch();

    let mut resolved_iter = range_iterator::<WayNodeResolvedTDC>(db, range);
    let mut resolved_cur: Option<(WayPosKey, ResolvedNodeValue)> =
        resolved_iter.next().transpose()?;
    let mut resolved = Vec::new();
    let mut ways_seen: usize = 0;
    for r in range_iterator::<WayByIdTDC>(db, range) {
        let (way_key, way) = r?;
        let way_id = way_key.id;

        resolved.clear();
        while let Some((key, val)) = &resolved_cur {
            if key.way_id > way_id {
                break;
            }
            if key.way_id == way_id {
                resolved.push((key.pos, *val));
            }
            resolved_cur = resolved_iter.next().transpose()?;
        }

        if let Some((key, value, mbb)) = resolve_way(&curve, way_id, way, &resolved, coord_scale) {
            batch.put::<WayTDC>(key, value);
            batch.put::<WayIdToMbbTDC>(way_key, mbb);
        }

        ways_seen += 1;
        if ways_seen.is_multiple_of(BATCH_SIZE) {
            write_batch_no_wal(db, batch.inner())?;
            batch = new_batch();
            // Per batch, not per way: `inc` can take a lock, and this runs
            // on every worker.
            pb.inc(BATCH_SIZE as u64);
        }
    }
    write_batch_no_wal(db, batch.inner())?;
    pb.inc((ways_seen % BATCH_SIZE) as u64);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn serialize_way_blocks(
    builder: &osmflat::OsmBuilder,
    db: &DB,
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

    // A way needs its nodes twice: their locations, for the bounding box that
    // determines its spatial key, and their final archive indices, for
    // `nodes_index`. `NodeIdToIdx` carries both, keyed by node id, so one
    // sort-merge join resolves every ref with sequential I/O only, instead of
    // a random lookup per ref:
    //   0. (parallel, per block) stage each way keyed by id, and index every
    //      node ref by (node id, way id, position).
    //   1. merge-join that index against `NodeIdToIdx`, writing each resolved
    //      ref keyed by (way id, position).
    //   2. merge the staged ways with the resolved refs (both way-id sorted),
    //      compute each way's bounding box and spatial key, and write it with
    //      its node indices inlined.
    //   3. scan the ways in spatial order and write the archive.

    // Phase 0. Order is irrelevant: everything downstream reads sorted scans.
    // The string table is shared behind a Mutex (locked once per block) and
    // the per-block `stats` are commutative, merged with `try_reduce`.
    // `std::mem::take` moves the caller's table in for the duration and it is
    // restored below.
    let string_table = Mutex::new(std::mem::take(stringtable));
    let total = {
        let _t = PhaseTimer::start("ways_convert");
        blocks
            .into_par_iter()
            .map(|idx| -> Result<Stats, OsmFlatcError> {
                let block: osmpbf::PrimitiveBlock = read_block(data, &idx)?;

                let mut batch = WriteBatchInternal::default();
                for cf_name in [WayByIdTDC::NAME, WayNodeRefTDC::NAME] {
                    let cf = db.cf_handle(cf_name).unwrap();
                    batch.insert_cf(cf_name, cf);
                }

                let block_stats = serialize_ways(&block, &mut batch, &string_table)?;

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

    info!("Compacting staged ways and node-ref index...");
    finalize_bulk_cfs(db, &[WayByIdTDC::NAME, WayNodeRefTDC::NAME])?;

    join_way_node_refs(db)?;
    info!("Compacting resolved way node refs...");
    finalize_bulk_cfs(db, &[WayNodeResolvedTDC::NAME])?;

    compute_way_bboxes(db, stats.num_ways, coord_scale)?;

    // Write->read boundary: the spatial-order scan below reads `WayTDC`, and
    // the relation pass point-looks-up `WayIdToMbb`.
    info!("Compacting way column families...");
    finalize_bulk_cfs(db, &[WayTDC::NAME, WayIdToMbbTDC::NAME])?;

    // Phase 3.
    let mut batch = WriteBatchInternal::default();
    let cf = db.cf_handle(WayIdToIdxTDC::NAME).unwrap();
    batch.insert_cf(WayIdToIdxTDC::NAME, cf);

    let pb = Progress::new(
        stats.num_ways as u64,
        "Ordering ways by spatial index order",
    );

    let t_write = std::time::Instant::now();
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

        for &idx in &v.node_idxs {
            nodes_index.grow()?.set_value(idx);
        }
        // Nodes referenced by a way but absent from the archive.
        missing.nodes_in_ways.extend(v.missing_node_ids);

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
    // it yields `(id, final_idx)` in ascending-id order. Emitting just the
    // index gives a permutation `p` with `ids.ways[p[k]]` ascending by id,
    // which the query side binary-searches. See the node path for the
    // rationale.
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
