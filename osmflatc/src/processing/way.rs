use std::io;

use super::WriteBatchInternal;
use crate::{
    add_string_table,
    osmpbf::{self, read_block, BlockIndex, PrimitiveBlock},
    parallel, pb_style,
    processing::{
        node::storage::{NodeIdToIdxTDC, NodeIdToLonLatTDC},
        storage::{OsmIdKey, OsmIdxValue, OsmKey},
        RocksDB, RocksDBSync, RocksDBUnsync, TempDataCodec,
    },
    stats::{MissingRefs, Stats},
    strings::StringTable,
    Error, TagSerializer, BATCH_SIZE,
};
use geo::{BoundingRect, MultiPoint};
use indicatif::ProgressBar;
use log::info;
use rocksdb::DB;
use storage::{WayIdToIdxTDC, WayIdToMbbTDC, WayMbbValue, WayTDC, WayValue};

pub(crate) mod storage;

fn serialize_ways(
    block: &osmpbf::PrimitiveBlock,
    batch: &mut impl RocksDBUnsync,
    db: &DB,
    stringtable: &mut StringTable,
    coord_scale: i32,
) -> Result<Stats, Error> {
    let mut stats = Stats::default();

    let curve = osmflat::way_curve();

    let string_refs = add_string_table(&block.stringtable, stringtable)?;

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

            let mut node_locations = vec![];
            let mut node_refs = vec![];
            let mut ref_id = 0;
            for r in &pbf_way.refs {
                ref_id += r;

                node_refs.push(ref_id);

                let node_id = OsmIdKey::new(ref_id);

                if let Some(n) = <DB as RocksDBSync>::get::<NodeIdToLonLatTDC>(db, &node_id)? {
                    node_locations.push((
                        n.lon as f64 / coord_scale as f64,
                        n.lat as f64 / coord_scale as f64,
                    ));
                }
            }

            let points: MultiPoint<_> = node_locations.into();

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
    mut way_ids: Option<flatdata::ExternalVector<osmflat::Id>>,
    blocks: Vec<BlockIndex>,
    data: &[u8],
    tags: &mut TagSerializer,
    stringtable: &mut StringTable,
    stats: &mut Stats,
    missing: &mut MissingRefs,
    coord_scale: i32,
) -> Result<(), Error> {
    let mut ways = builder.start_ways()?;
    let pb = ProgressBar::new(blocks.len() as u64)
        .with_style(pb_style())
        .with_prefix("Converting ways");
    let mut nodes_index = builder.start_nodes_index()?;
    parallel::parallel_process(
        blocks.into_iter(),
        |idx| {
            let block: osmpbf::PrimitiveBlock = read_block(data, &idx)?;
            Ok(block)
        },
        |block: io::Result<PrimitiveBlock>| -> Result<osmpbf::PrimitiveBlock, Error> {
            let block = block?;

            let mut batch = WriteBatchInternal::default();
            for cf_name in [WayTDC::NAME, WayIdToMbbTDC::NAME] {
                let cf = db.cf_handle(cf_name).unwrap();
                batch.insert_cf(cf_name, cf);
            }

            *stats += serialize_ways(&block, &mut batch, db, stringtable, coord_scale)?;

            db.write(batch.inner())?;

            pb.inc(1);

            Ok(block)
        },
    )?;

    pb.finish();
    info!("Ways converted.");

    let mut batch = WriteBatchInternal::default();
    let cf = db.cf_handle(WayIdToIdxTDC::NAME).unwrap();
    batch.insert_cf(WayIdToIdxTDC::NAME, cf);

    let pb = ProgressBar::new(stats.num_ways as u64)
        .with_style(pb_style())
        .with_prefix("Ordering ways by spatial index order");

    for (i, r) in <DB as RocksDB>::iterator::<WayTDC>(db)?.enumerate() {
        let (k, v) = r?;

        let way = ways.grow()?;

        let tag_first_idx = tags.next_index();

        for &(k, v) in &v.key_vals {
            tags.serialize(k, v)?;
        }

        way.set_tag_first_idx(tag_first_idx);
        way.set_ref_first_idx(nodes_index.len() as u64);

        let idx = i as u64;

        batch.put::<WayIdToIdxTDC>(OsmIdKey::new(k.id), OsmIdxValue::new(idx));

        for n in v.node_refs {
            let idx =
                <DB as RocksDBSync>::get::<NodeIdToIdxTDC>(db, &OsmIdKey::new(n))?.map(|v| v.idx);
            if idx.is_none() {
                // A node referenced by a way but absent from the archive.
                missing.nodes_in_ways.insert(n);
            }
            nodes_index.grow()?.set_value(idx);
        }

        if let Some(ids) = &mut way_ids {
            ids.grow()?.set_value(k.id as u64);
        }

        pb.inc(1);

        if i % BATCH_SIZE == 0 {
            db.write(batch.inner())?;
            batch = WriteBatchInternal::default();
            let cf = db.cf_handle(WayIdToIdxTDC::NAME).unwrap();
            batch.insert_cf(WayIdToIdxTDC::NAME, cf);
        }
    }

    db.write(batch.inner())?;

    pb.finish();

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
    info!("Ways processed");
    Ok(())
}
