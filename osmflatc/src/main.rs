mod args;
mod osmpbf;
mod parallel;
mod stats;
mod storage;
mod strings;

use crate::osmpbf::{build_block_index, read_block, BlockIndex, BlockType};
use crate::stats::Stats;
use crate::strings::StringTable;
use geo::BoundingRect;
use geo::MultiPoint;
use osmpbf::PrimitiveBlock;
use prost::Message;
use space_time::xzorder::xz2_sfc::XZ2SFC;
use storage::{
    break_key, break_node_lat_lon, break_node_values, break_relation_values, break_way_mbb,
    break_way_values, create_db, create_key, create_node_lat_lon, create_node_spatial_index,
    create_node_values, create_relation_values, create_way_mbb, create_way_values, RelationInfo,
    NODES, NODE_ID_TO_IDX, NODE_ID_TO_LAT_LON, RELATIONS, RELATIONS_STRING_REFS, RESOLUTION, WAYS,
    WAY_ID_TO_IDX, WAY_ID_TO_MBR,
};

use clap::Parser;
use flatdata::FileResourceStorage;
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use log::{error, info};
use memmap2::Mmap;

use ahash::AHashMap;
use rocksdb::{WriteBatch, DB};
use std::collections::{hash_map, VecDeque};
use std::fs::File;
use std::io;
use std::str;

type Error = Box<dyn std::error::Error>;

const BATCH_SIZE: usize = 5000;

fn serialize_header(
    header_block: &osmpbf::HeaderBlock,
    coord_scale: i32,
    builder: &osmflat::OsmBuilder,
    stringtable: &mut StringTable,
) -> io::Result<()> {
    let mut header = osmflat::Header::new();

    header.set_coord_scale(coord_scale);

    if let Some(ref bbox) = header_block.bbox {
        header.set_bbox_left((bbox.left / (1000000000 / coord_scale) as i64) as i32);
        header.set_bbox_right((bbox.right / (1000000000 / coord_scale) as i64) as i32);
        header.set_bbox_top((bbox.top / (1000000000 / coord_scale) as i64) as i32);
        header.set_bbox_bottom((bbox.bottom / (1000000000 / coord_scale) as i64) as i32);
    };

    header.set_writingprogram_idx(stringtable.insert("osmflatc"));

    if let Some(ref source) = header_block.source {
        header.set_source_idx(stringtable.insert(source));
    }

    if let Some(timestamp) = header_block.osmosis_replication_timestamp {
        header.set_replication_timestamp(timestamp);
    }

    if let Some(number) = header_block.osmosis_replication_sequence_number {
        header.set_replication_sequence_number(number);
    }

    if let Some(ref url) = header_block.osmosis_replication_base_url {
        header.set_replication_base_url_idx(stringtable.insert(url));
    }

    builder.set_header(&header)?;
    Ok(())
}

#[derive(PartialEq, Eq, Copy, Clone)]
struct I40 {
    x: [u8; 5],
}

impl I40 {
    fn from_u64(x: u64) -> Self {
        let x = x.to_le_bytes();
        debug_assert_eq!((x[5], x[6], x[7]), (0, 0, 0));
        Self {
            x: [x[0], x[1], x[2], x[3], x[4]],
        }
    }

    fn to_u64(self) -> u64 {
        let extented = [
            self.x[0], self.x[1], self.x[2], self.x[3], self.x[4], 0, 0, 0,
        ];
        u64::from_le_bytes(extented)
    }
}

#[allow(clippy::derived_hash_with_manual_eq)]
impl std::hash::Hash for I40 {
    fn hash<H>(&self, h: &mut H)
    where
        H: std::hash::Hasher,
    {
        // We manually implement Hash like this, since [u8; 5] is slower to hash
        // than u64 for some/many hash functions
        self.to_u64().hash(h)
    }
}

/// Holds tags external vector and deduplicates tags.
struct TagSerializer<'a> {
    tags: flatdata::ExternalVector<'a, osmflat::Tag>,
    tags_index: flatdata::ExternalVector<'a, osmflat::TagIndex>,
    dedup: AHashMap<(I40, I40), I40>, // deduplication table: (key_idx, val_idx) -> pos
}

impl<'a> TagSerializer<'a> {
    fn new(builder: &'a osmflat::OsmBuilder) -> io::Result<Self> {
        Ok(Self {
            tags: builder.start_tags()?,
            tags_index: builder.start_tags_index()?,
            dedup: AHashMap::new(),
        })
    }

    fn serialize(&mut self, key_idx: u64, val_idx: u64) -> Result<(), Error> {
        let idx = match self
            .dedup
            .entry((I40::from_u64(key_idx), I40::from_u64(val_idx)))
        {
            hash_map::Entry::Occupied(entry) => entry.get().to_u64(),
            hash_map::Entry::Vacant(entry) => {
                let idx = self.tags.len() as u64;
                let tag = self.tags.grow()?;
                tag.set_key_idx(key_idx);
                tag.set_value_idx(val_idx);
                entry.insert(I40::from_u64(idx));
                idx
            }
        };

        let tag_index = self.tags_index.grow()?;
        tag_index.set_value(idx);

        Ok(())
    }

    fn next_index(&self) -> u64 {
        self.tags_index.len() as u64
    }

    fn close(self) {
        if let Err(e) = self.tags.close() {
            panic!("failed to close tags: {}", e);
        }
        if let Err(e) = self.tags_index.close() {
            panic!("failed to close tags index: {}", e);
        }
    }
}

/// adds all strings in a table to the lookup and returns a vectors of
/// references to be used instead
fn add_string_table(
    pbf_stringtable: &osmpbf::StringTable,
    stringtable: &mut StringTable,
) -> Result<Vec<u64>, Error> {
    let mut result = Vec::with_capacity(pbf_stringtable.s.len());
    for x in &pbf_stringtable.s {
        let string = str::from_utf8(x)?;
        result.push(stringtable.insert(string));
    }
    Ok(result)
}

fn serialize_dense_nodes(
    block: &osmpbf::PrimitiveBlock,
    granularity: i32,
    db: &DB,
    stringtable: &mut StringTable,
    tags: &mut TagSerializer,
    coord_scale: i32,
) -> Result<Stats, Error> {
    let mut stats = Stats::default();
    let string_refs = add_string_table(&block.stringtable, stringtable)?;

    let curve = XZ2SFC::wgs84(RESOLUTION);
    let cf = db.cf_handle(NODES).unwrap();
    let node_id_to_lat_lon_cf = db.cf_handle(NODE_ID_TO_LAT_LON).unwrap();

    let mut batch = WriteBatch::default();

    for group in block.primitivegroup.iter() {
        let dense_nodes = group.dense.as_ref().unwrap();

        let pbf_granularity = block.granularity.unwrap_or(100);
        let lat_offset = block.lat_offset.unwrap_or(0);
        let lon_offset = block.lon_offset.unwrap_or(0);
        let mut lat = 0;
        let mut lon = 0;

        let mut tags_offset = 0;

        let mut id = 0;
        for i in 0..dense_nodes.id.len() {
            id += dense_nodes.id[i];

            lat += dense_nodes.lat[i];
            lon += dense_nodes.lon[i];
            let lat_ =
                ((lat_offset + (i64::from(pbf_granularity) * lat)) / granularity as i64) as i32;
            let lon_ =
                ((lon_offset + (i64::from(pbf_granularity) * lon)) / granularity as i64) as i32;

            let mut tag_first_idx = None;

            if tags_offset < dense_nodes.keys_vals.len() {
                tag_first_idx = Some(tags.next_index());
                loop {
                    let k = dense_nodes.keys_vals[tags_offset];
                    tags_offset += 1;

                    if k == 0 {
                        break; // separator
                    }

                    let v = dense_nodes.keys_vals[tags_offset];
                    tags_offset += 1;

                    tags.serialize(string_refs[k as usize], string_refs[v as usize])?;
                }
            }

            let spatial_index = create_node_spatial_index(
                &curve,
                lat_ as f64 / coord_scale as f64,
                lon_ as f64 / coord_scale as f64,
            );
            let key = create_key(spatial_index, id);
            let value = create_node_values(lat_, lon_, tag_first_idx.unwrap_or(0));
            batch.put_cf(cf, key, value);

            let key2 = id.to_be_bytes();
            let value2 = create_node_lat_lon(lat_, lon_);
            batch.put_cf(node_id_to_lat_lon_cf, key2, value2);
        }
        assert_eq!(tags_offset, dense_nodes.keys_vals.len());
        stats.num_nodes += dense_nodes.id.len();
    }

    db.write(batch)?;

    Ok(stats)
}

fn serialize_ways(
    block: &osmpbf::PrimitiveBlock,
    db: &DB,
    stringtable: &mut StringTable,
    tags: &mut TagSerializer,
    coord_scale: i32,
) -> Result<Stats, Error> {
    let mut stats = Stats::default();

    let curve = XZ2SFC::wgs84(RESOLUTION);
    let ways_cf = db.cf_handle(WAYS).unwrap();
    let node_id_to_lat_lon = db.cf_handle(NODE_ID_TO_LAT_LON).unwrap();
    let way_id_to_mbr_cf = db.cf_handle(WAY_ID_TO_MBR).unwrap();

    let string_refs = add_string_table(&block.stringtable, stringtable)?;

    let mut batch = WriteBatch::default();

    for group in &block.primitivegroup {
        for pbf_way in &group.ways {
            debug_assert_eq!(pbf_way.keys.len(), pbf_way.vals.len(), "invalid input data");

            let tag_first_index = tags.next_index();

            for i in 0..pbf_way.keys.len() {
                tags.serialize(
                    string_refs[pbf_way.keys[i] as usize],
                    string_refs[pbf_way.vals[i] as usize],
                )?;
            }

            let mut node_locations = vec![];
            let mut node_refs = vec![];
            let mut ref_id = 0;
            for r in &pbf_way.refs {
                ref_id += r;

                node_refs.push(ref_id);

                if let Some(n) = db.get_cf(node_id_to_lat_lon, ref_id.to_be_bytes())? {
                    let (lat, lon) = break_node_lat_lon(n);
                    node_locations.push((
                        lon as f64 / coord_scale as f64,
                        lat as f64 / coord_scale as f64,
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

                let spatial_index = curve.index(min_x, min_y, max_x, max_y);
                let key = create_key(spatial_index, pbf_way.id);
                let value = create_way_values(tag_first_index, node_refs);

                batch.put_cf(ways_cf, key, value);
                batch.put_cf(
                    way_id_to_mbr_cf,
                    pbf_way.id.to_be_bytes(),
                    create_way_mbb(
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
    db.write(batch)?;

    Ok(stats)
}

fn build_relations_index<I>(
    data: &[u8],
    block_index: I,
    db: &DB,
    stats: &mut Stats,
) -> Result<(AHashMap<i64, RelationInfo>, Vec<RelationInfo>), Error>
where
    I: ExactSizeIterator<Item = BlockIndex> + Send + 'static,
{
    let node_id_to_lat_lon_cf = db.cf_handle(NODE_ID_TO_LAT_LON).unwrap();
    let way_id_to_mbr_cf = db.cf_handle(WAY_ID_TO_MBR).unwrap();

    let mut found = AHashMap::new();
    let mut unresolved = vec![];

    let pb = ProgressBar::new(block_index.len() as u64)
        .with_style(pb_style())
        .with_prefix("Building relations index");
    parallel::parallel_process(
        block_index,
        |idx| read_block(data, &idx),
        |block: Result<osmpbf::PrimitiveBlock, _>| -> Result<(), Error> {
            for group in &block?.primitivegroup {
                for pbf_relation in &group.relations {
                    let mut relation_info = RelationInfo {
                        id: pbf_relation.id,
                        ..Default::default()
                    };

                    let mut memid = 0;
                    for i in 0..pbf_relation.roles_sid.len() {
                        memid += pbf_relation.memids[i];

                        let member_type =
                            osmpbf::relation::MemberType::try_from(pbf_relation.types[i]);
                        assert!(member_type.is_ok());

                        match member_type.unwrap() {
                            osmpbf::relation::MemberType::Node => {
                                let v = db
                                    .get_cf(node_id_to_lat_lon_cf, memid.to_be_bytes())?
                                    .map(break_node_lat_lon);

                                match v {
                                    Some((lat, lon)) => relation_info.points.push((lon, lat)),
                                    None => stats.num_unresolved_node_ids += 1,
                                }
                            }
                            osmpbf::relation::MemberType::Way => {
                                let v = db
                                    .get_cf(way_id_to_mbr_cf, memid.to_be_bytes())?
                                    .map(break_way_mbb);

                                match v {
                                    Some(mbr) => {
                                        relation_info.points.push((mbr[0], mbr[1]));
                                        relation_info.points.push((mbr[2], mbr[3]));
                                    }
                                    None => stats.num_unresolved_way_ids += 1,
                                }
                            }
                            osmpbf::relation::MemberType::Relation => {
                                relation_info.relation_ids.insert(memid);
                            }
                        }
                    }
                    if relation_info.is_ready() {
                        found.insert(pbf_relation.id, relation_info);
                    } else {
                        unresolved.push(relation_info)
                    }
                }
                pb.inc(1);
            }
            Ok(())
        },
    )?;
    pb.finish();

    Ok((found, unresolved))
}

fn resolve_all_relations(
    mut found: AHashMap<i64, RelationInfo>,
    unresolved: Vec<RelationInfo>,
) -> AHashMap<i64, RelationInfo> {
    let mut iterations = 0;

    let mut unresolved = unresolved.into_iter().collect::<VecDeque<RelationInfo>>();
    while let Some(mut p) = unresolved.pop_front() {
        if p.is_ready() {
            found.insert(p.id, p);
        } else {
            for rel_id in &p.relation_ids.clone() {
                if let Some(f) = found.get(rel_id) {
                    p.points.extend(&f.points);
                    p.relation_ids.remove(rel_id);
                }
            }
            unresolved.push_back(p);
        }
        iterations += 1;
        if iterations > 2_000_000 {
            break;
        }
    }
    found
}

#[allow(clippy::too_many_arguments)]
fn serialize_relations(
    pbf_relation: &osmpbf::Relation,
    rel_idx: u64,
    db: &DB,
    relations: &mut flatdata::ExternalVector<osmflat::Relation>,
    relation_ids: &mut Option<flatdata::ExternalVector<osmflat::Id>>,
    relation_members: &mut flatdata::MultiVector<osmflat::RelationMembers>,
    string_refs: Vec<u64>,
    tags: &mut TagSerializer,
) -> Result<Stats, Error> {
    let mut stats = Stats::default();

    let cf_node_id_to_idx = db.cf_handle(NODE_ID_TO_IDX).unwrap();
    let cf_way_id_to_idx = db.cf_handle(WAY_ID_TO_IDX).unwrap();

    debug_assert_eq!(
        pbf_relation.keys.len(),
        pbf_relation.vals.len(),
        "invalid input data"
    );

    let relation = relations.grow()?;
    if let Some(ids) = relation_ids {
        ids.grow()?.set_value(pbf_relation.id as u64);
    }

    relation.set_tag_first_idx(tags.next_index());
    for i in 0..pbf_relation.keys.len() {
        tags.serialize(
            string_refs[pbf_relation.keys[i] as usize],
            string_refs[pbf_relation.vals[i] as usize],
        )?;
    }

    debug_assert!(
        pbf_relation.roles_sid.len() == pbf_relation.memids.len()
            && pbf_relation.memids.len() == pbf_relation.types.len(),
        "invalid input data"
    );

    stats.num_relations = 1;

    let mut memid = 0;
    let mut members = relation_members.grow()?;

    for i in 0..pbf_relation.roles_sid.len() {
        memid += pbf_relation.memids[i];

        let member_type = osmpbf::relation::MemberType::try_from(pbf_relation.types[i]);
        debug_assert!(member_type.is_ok());

        match member_type.unwrap() {
            osmpbf::relation::MemberType::Node => {
                let idx = db
                    .get_cf(cf_node_id_to_idx, memid.to_be_bytes())?
                    .map(|v| u64::from_be_bytes(v[0..8].try_into().unwrap()));
                stats.num_unresolved_node_ids += idx.is_none() as usize;

                let member = members.add_node_member();
                member.set_node_idx(idx);
                member.set_role_idx(string_refs[pbf_relation.roles_sid[i] as usize]);
            }
            osmpbf::relation::MemberType::Way => {
                let idx = db
                    .get_cf(cf_way_id_to_idx, memid.to_be_bytes())?
                    .map(|v| u64::from_be_bytes(v[0..8].try_into().unwrap()));
                stats.num_unresolved_way_ids += idx.is_none() as usize;

                let member = members.add_way_member();
                member.set_way_idx(idx);
                member.set_role_idx(string_refs[pbf_relation.roles_sid[i] as usize]);
            }
            osmpbf::relation::MemberType::Relation => {
                let idx = rel_idx;
                let member = members.add_relation_member();
                member.set_relation_idx(Some(idx));
                member.set_role_idx(string_refs[pbf_relation.roles_sid[i] as usize]);
            }
        }
    }
    Ok(stats)
}

#[allow(clippy::too_many_arguments)]
fn serialize_dense_node_blocks(
    builder: &osmflat::OsmBuilder,
    granularity: i32,
    mut node_ids: Option<flatdata::ExternalVector<osmflat::Id>>,
    db: &DB,
    blocks: Vec<BlockIndex>,
    data: &[u8],
    tags: &mut TagSerializer,
    stringtable: &mut StringTable,
    stats: &mut Stats,
    coord_scale: i32,
) -> Result<(), Error> {
    let mut nodes = builder.start_nodes()?;
    let pb = ProgressBar::new(blocks.len() as u64)
        .with_style(pb_style())
        .with_prefix("Converting dense nodes");
    parallel::parallel_process(
        blocks.into_iter(),
        |idx| read_block(data, &idx),
        |block| -> Result<osmpbf::PrimitiveBlock, Error> {
            let block = block?;
            *stats +=
                serialize_dense_nodes(&block, granularity, db, stringtable, tags, coord_scale)?;

            pb.inc(1);
            Ok(block)
        },
    )?;
    pb.finish();

    let cf = db.cf_handle(NODES).unwrap();

    let node_id_to_idx_cf = db.cf_handle(NODE_ID_TO_IDX).unwrap();

    let pb = ProgressBar::new(stats.num_nodes as u64)
        .with_style(pb_style())
        .with_prefix("Ordering dense nodes in spatial index order");

    let mut batch = WriteBatch::default();
    for (i, r) in db.iterator_cf(cf, rocksdb::IteratorMode::Start).enumerate() {
        let (k, v) = r?;

        let idx = i as u64;

        let node = nodes.grow()?;
        let (_, id) = break_key(&k);
        let (lat, lon, tag_first_idx) = break_node_values(&v);
        node.set_lat(lat);
        node.set_lon(lon);
        node.set_tag_first_idx(tag_first_idx);

        if let Some(ids) = &mut node_ids {
            ids.grow()?.set_value(id as u64);
        }

        batch.put_cf(node_id_to_idx_cf, id.to_be_bytes(), idx.to_be_bytes());
        pb.inc(1);

        if i % BATCH_SIZE == 0 {
            db.write(batch)?;
            batch = WriteBatch::default();
        }
    }

    db.write(batch)?;
    pb.finish();

    // fill tag_first_idx of the sentry, since it contains the end of the tag range
    // of the last node
    nodes.grow()?.set_tag_first_idx(tags.next_index());
    nodes.close()?;
    if let Some(ids) = node_ids {
        ids.close()?;
    }
    info!("Dense nodes converted.");
    Ok(())
}

type PrimitiveBlockWithIds = (osmpbf::PrimitiveBlock, (Vec<Option<u64>>, Stats));

#[allow(clippy::too_many_arguments)]
fn serialize_way_blocks(
    builder: &osmflat::OsmBuilder,
    db: &DB,
    mut way_ids: Option<flatdata::ExternalVector<osmflat::Id>>,
    blocks: Vec<BlockIndex>,
    data: &[u8],
    tags: &mut TagSerializer,
    stringtable: &mut StringTable,
    stats: &mut Stats,
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
            *stats += serialize_ways(&block, db, stringtable, tags, coord_scale)?;
            pb.inc(1);

            Ok(block)
        },
    )?;

    pb.finish();
    info!("Ways converted.");

    let ways_cf = db.cf_handle(WAYS).unwrap();
    let way_id_to_idx_cf = db.cf_handle(WAY_ID_TO_IDX).unwrap();
    let node_id_to_idx_cf = db.cf_handle(NODE_ID_TO_IDX).unwrap();

    let pb = ProgressBar::new(stats.num_ways as u64)
        .with_style(pb_style())
        .with_prefix("Ordering ways by spatial index order");

    let mut batch = WriteBatch::default();

    for (i, r) in db
        .iterator_cf(ways_cf, rocksdb::IteratorMode::Start)
        .enumerate()
    {
        let (k, v) = r?;

        let (_, id) = break_key(&k);
        let (tag_first_idx, node_refs) = break_way_values(&v);
        let way = ways.grow()?;

        way.set_tag_first_idx(tag_first_idx);
        way.set_ref_first_idx(nodes_index.len() as u64);

        let idx = i as u64;

        batch.put_cf(way_id_to_idx_cf, id.to_be_bytes(), idx.to_be_bytes());

        for n in node_refs {
            let idx = db
                .get_cf(node_id_to_idx_cf, n.to_be_bytes())?
                .map(|v| u64::from_be_bytes(v[0..8].try_into().unwrap()));
            nodes_index.grow()?.set_value(idx);
        }

        if let Some(ids) = &mut way_ids {
            ids.grow()?.set_value(id as u64);
        }

        pb.inc(1);

        if i % BATCH_SIZE == 0 {
            db.write(batch)?;
            batch = WriteBatch::default();
        }
    }

    db.write(batch)?;

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

#[allow(clippy::too_many_arguments)]
fn serialize_relation_blocks(
    builder: &osmflat::OsmBuilder,
    db: &DB,
    mut relation_ids: Option<flatdata::ExternalVector<osmflat::Id>>,
    blocks: Vec<BlockIndex>,
    data: &[u8],
    tags: &mut TagSerializer,
    stringtable: &mut StringTable,
    stats: &mut Stats,
    coord_scale: i32,
) -> Result<(), Error> {
    // We need to build the index of relation ids first, since relations can refer
    // again to relations.
    let (found, unresolved) = build_relations_index(data, blocks.clone().into_iter(), db, stats)?;
    let found = resolve_all_relations(found, unresolved);

    let curve = XZ2SFC::wgs84(RESOLUTION);

    let relations_cf = db.cf_handle(RELATIONS).unwrap();
    let relations_string_refs = db.cf_handle(RELATIONS_STRING_REFS).unwrap();

    let pb = ProgressBar::new(blocks.len() as u64)
        .with_style(pb_style())
        .with_prefix("Converting relations");

    for v in blocks
        .into_iter()
        .map(|idx| read_block::<PrimitiveBlock>(data, &idx))
    {
        let block = v?;

        let string_refs = add_string_table(&block.stringtable, stringtable)?;

        pb.inc(1);

        for rel in block.primitivegroup.into_iter().flat_map(|g| g.relations) {
            let id = rel.id;
            if let Some(rel_info) = found.get(&id) {
                let points: MultiPoint<_> = rel_info
                    .points
                    .iter()
                    .map(|v| {
                        (
                            v.0 as f64 / coord_scale as f64,
                            v.1 as f64 / coord_scale as f64,
                        )
                    })
                    .collect::<Vec<_>>()
                    .into();
                if let Some(mbr) = points.bounding_rect() {
                    let spatial_index =
                        curve.index(mbr.min().x, mbr.min().y, mbr.max().x, mbr.max().y);
                    let key = create_key(spatial_index, id);
                    db.put_cf(relations_cf, key.clone(), rel.encode_to_vec())?;
                    db.put_cf(
                        relations_string_refs,
                        key,
                        create_relation_values(string_refs.as_slice()),
                    )?;
                }
            }
        }
    }

    let mut relations = builder.start_relations()?;
    let mut relation_members = builder.start_relation_members()?;

    let pb = ProgressBar::new(found.len() as u64)
        .with_style(pb_style())
        .with_prefix("Ordering relations");

    for (rel_idx, res) in db
        .iterator_cf(relations_cf, rocksdb::IteratorMode::Start)
        .zip(db.iterator_cf(relations_string_refs, rocksdb::IteratorMode::Start))
        .enumerate()
    {
        let (_, rel) = res.0?;
        let (_, string_refs) = res.1?;

        let relation = osmpbf::Relation::decode(rel.to_vec().as_slice())?;
        let string_refs = break_relation_values(&string_refs);

        *stats += serialize_relations(
            &relation,
            rel_idx as u64,
            db,
            &mut relations,
            &mut relation_ids,
            &mut relation_members,
            string_refs,
            tags,
        )?;
        pb.inc(1);
    }

    {
        let sentinel = relations.grow()?;
        sentinel.set_tag_first_idx(tags.next_index());
    }

    relations.close()?;
    if let Some(ids) = relation_ids {
        ids.close()?;
    }
    relation_members.close()?;

    pb.finish();
    info!("Relations converted.");

    Ok(())
}

fn gcd(a: i32, b: i32) -> i32 {
    let (mut x, mut y) = (a.min(b), a.max(b));
    while x > 1 {
        y %= x;
        std::mem::swap(&mut x, &mut y);
    }
    y
}

fn run(args: args::Args) -> Result<(), Error> {
    let input_file = File::open(&args.input)?;
    let input_data = unsafe { Mmap::map(&input_file)? };

    let storage = FileResourceStorage::new(args.output.clone());
    let builder = osmflat::OsmBuilder::new(storage.clone())?;

    // TODO: Would be nice not store all these strings in memory, but to flush them
    // from time to time to disk.
    let mut stringtable = StringTable::new();
    let mut tags = TagSerializer::new(&builder)?;

    info!(
        "Initialized new osmflat archive at: {}",
        &args.output.display()
    );

    info!("Building index of PBF blocks...");
    let block_index = build_block_index(&input_data);
    let mut greatest_common_granularity = 1000000000;
    for block in &block_index {
        if block.block_type == BlockType::DenseNodes {
            // only DenseNodes have coordinate we need to scale
            if let Some(block_granularity) = block.granularity {
                greatest_common_granularity =
                    gcd(greatest_common_granularity, block_granularity as i32);
            }
        }
    }
    let coord_scale = 1000000000 / greatest_common_granularity;
    info!(
        "Greatest common granularity: {}, Coordinate scaling factor: {}",
        greatest_common_granularity, coord_scale
    );

    // TODO: move out into a function
    let groups = block_index.into_iter().chunk_by(|b| b.block_type);
    let mut pbf_header = Vec::new();
    let mut pbf_dense_nodes = Vec::new();
    let mut pbf_ways = Vec::new();
    let mut pbf_relations = Vec::new();
    for (block_type, blocks) in &groups {
        match block_type {
            BlockType::Header => pbf_header = blocks.collect(),
            BlockType::Nodes => panic!("Found nodes block, only dense nodes are supported now"),
            BlockType::DenseNodes => pbf_dense_nodes = blocks.collect(),
            BlockType::Ways => pbf_ways = blocks.collect(),
            BlockType::Relations => pbf_relations = blocks.collect(),
        }
    }
    info!("PBF block index built.");

    // Serialize header
    if pbf_header.len() != 1 {
        return Err(format!(
            "Require exactly one header block, but found {}",
            pbf_header.len()
        )
        .into());
    }
    let idx = &pbf_header[0];
    let pbf_header: osmpbf::HeaderBlock = read_block(&input_data, idx)?;
    serialize_header(&pbf_header, coord_scale, &builder, &mut stringtable)?;
    info!("Header written.");

    let db = create_db()?;

    let mut stats = Stats::default();

    let ids_archive;
    let mut node_ids = None;
    let mut way_ids = None;
    let mut relation_ids = None;
    if args.ids {
        ids_archive = builder.ids()?;
        node_ids = Some(ids_archive.start_nodes()?);
        way_ids = Some(ids_archive.start_ways()?);
        relation_ids = Some(ids_archive.start_relations()?);
    }

    serialize_dense_node_blocks(
        &builder,
        greatest_common_granularity,
        node_ids,
        &db,
        pbf_dense_nodes,
        &input_data,
        &mut tags,
        &mut stringtable,
        &mut stats,
        coord_scale,
    )?;

    serialize_way_blocks(
        &builder,
        &db,
        way_ids,
        pbf_ways,
        &input_data,
        &mut tags,
        &mut stringtable,
        &mut stats,
        coord_scale,
    )?;

    serialize_relation_blocks(
        &builder,
        &db,
        relation_ids,
        pbf_relations,
        &input_data,
        &mut tags,
        &mut stringtable,
        &mut stats,
        coord_scale,
    )?;

    // Finalize data structures
    tags.close(); // drop the reference to stringtable

    info!("Writing stringtable to disk...");
    builder.set_stringtable(&stringtable.into_bytes())?;

    info!("osmflat archive built.");

    std::mem::drop(builder);
    osmflat::Osm::open(storage)?;

    info!("verified that osmflat archive can be opened.");

    println!("{stats}");
    Ok(())
}

fn pb_style() -> ProgressStyle {
    ProgressStyle::with_template("{prefix:>24} [{bar:23}] {pos}/{len}: {per_sec} {elapsed}")
        .unwrap()
        .progress_chars("=> ")
}

fn main() {
    let args = args::Args::parse();
    let level = match args.verbose {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(level))
        .format_target(false)
        .format_module_path(false)
        .format_timestamp_nanos()
        .init();

    if let Err(e) = run(args) {
        error!("{e}");
        std::process::exit(1);
    }
}
