mod args;
mod error;
mod osmpbf;
mod parallel;
mod processing;
mod stats;
mod storage;
mod strings;

use crate::osmpbf::{build_block_index, read_block, BlockIndex, BlockType};
use crate::processing::TempDataCodec;
use crate::stats::{MissingRefs, Stats};
use crate::strings::StringTable;
use geo::BoundingRect;
use geo::MultiPoint;
use osmpbf::PrimitiveBlock;
use processing::create_db;
use processing::node::serialize_dense_node_blocks;
use processing::node::storage::{NodeIdToIdxTDC, NodeIdToLonLatTDC};
use processing::storage::{OsmIdKey, OsmKey};
use processing::way::serialize_way_blocks;
use processing::way::storage::{WayIdToIdxTDC, WayIdToMbbTDC};
use processing::Key;
use processing::RocksDBSync;
use processing::{RELATIONS, RELATIONS_STRING_REFS};
use prost::Message;
use space_time::xzorder::xz2_sfc::XZ2SFC;
use storage::{break_node_lon_lat, break_relation_values, create_relation_values, RelationInfo};

use clap::Parser;
use flatdata::FileResourceStorage;
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use log::{error, info};
use memmap2::Mmap;

use ahash::AHashMap;
use rocksdb::DB;
use std::collections::{hash_map, VecDeque};
use std::fs::File;
use std::io;
use std::path::Path;
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

fn build_relations_index<I>(
    data: &[u8],
    block_index: I,
    db: &DB,
) -> Result<(AHashMap<i64, RelationInfo>, Vec<RelationInfo>), Error>
where
    I: ExactSizeIterator<Item = BlockIndex> + Send + 'static,
{
    let node_id_to_lat_lon_cf = db.cf_handle(NodeIdToLonLatTDC::NAME).unwrap();

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
                                    .map(break_node_lon_lat);

                                // Relation points are stored as (lon, lat) to match the
                                // way-member bbox corners pushed below. Missing members are
                                // counted later, in the emit pass, with osmium semantics.
                                if let Some((lon, lat)) = v {
                                    relation_info.points.push((lon, lat));
                                }
                            }
                            osmpbf::relation::MemberType::Way => {
                                let v = <DB as RocksDBSync>::get::<WayIdToMbbTDC>(
                                    db,
                                    &OsmIdKey::new(memid),
                                )?;

                                if let Some(mbr) = v {
                                    relation_info.points.push((mbr.mbb[0], mbr.mbb[1]));
                                    relation_info.points.push((mbr.mbb[2], mbr.mbb[3]));
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
    let mut unresolved: VecDeque<RelationInfo> = unresolved.into();

    // Pull member geometry from sub-relations in repeated passes until a full
    // pass makes no progress (handles nesting; terminates on cycles/missing).
    loop {
        let mut progressed = false;
        let mut remaining = VecDeque::with_capacity(unresolved.len());
        while let Some(mut p) = unresolved.pop_front() {
            for rel_id in p.relation_ids.clone() {
                if let Some(f) = found.get(&rel_id) {
                    p.points.extend(&f.points);
                    p.relation_ids.remove(&rel_id);
                    progressed = true;
                }
            }
            if p.is_ready() {
                found.insert(p.id, p);
                progressed = true;
            } else {
                remaining.push_back(p);
            }
        }
        unresolved = remaining;
        if unresolved.is_empty() || !progressed {
            break;
        }
    }

    // Stop dropping: keep every relation that could not be fully resolved, with
    // whatever member geometry it accumulated. Its unresolvable relation members
    // are simply ignored (and counted as missing in the emit pass).
    for p in unresolved {
        found.entry(p.id).or_insert(p);
    }
    found
}

#[allow(clippy::too_many_arguments)]
fn serialize_relations(
    pbf_relation: &osmpbf::Relation,
    mbb: [i32; 4],
    relation_id_to_idx: &AHashMap<i64, u64>,
    db: &DB,
    relations: &mut flatdata::ExternalVector<osmflat::Relation>,
    relation_ids: &mut Option<flatdata::ExternalVector<osmflat::Id>>,
    relation_members: &mut flatdata::MultiVector<osmflat::RelationMembers>,
    string_refs: Vec<u64>,
    tags: &mut TagSerializer,
    missing: &mut MissingRefs,
) -> Result<Stats, Error> {
    let mut stats = Stats::default();

    let cf_node_id_to_idx = db.cf_handle(NodeIdToIdxTDC::NAME).unwrap();
    let cf_way_id_to_idx = db.cf_handle(WayIdToIdxTDC::NAME).unwrap();

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
    relation.set_min_lon(mbb[0]);
    relation.set_min_lat(mbb[1]);
    relation.set_max_lon(mbb[2]);
    relation.set_max_lat(mbb[3]);
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
                if idx.is_none() {
                    missing.nodes_in_relations.insert(memid);
                }

                let member = members.add_node_member();
                member.set_node_idx(idx);
                member.set_role_idx(string_refs[pbf_relation.roles_sid[i] as usize]);
            }
            osmpbf::relation::MemberType::Way => {
                let idx = db
                    .get_cf(cf_way_id_to_idx, memid.to_be_bytes())?
                    .map(|v| u64::from_be_bytes(v[0..8].try_into().unwrap()));
                if idx.is_none() {
                    missing.ways_in_relations.insert(memid);
                }

                let member = members.add_way_member();
                member.set_way_idx(idx);
                member.set_role_idx(string_refs[pbf_relation.roles_sid[i] as usize]);
            }
            osmpbf::relation::MemberType::Relation => {
                // Resolve the referenced relation to its index in the
                // spatially-ordered relations vector. References to relations
                // not in the archive become `None` (INVALID_IDX).
                let idx = relation_id_to_idx.get(&memid).copied();
                if idx.is_none() {
                    missing.relations_in_relations.insert(memid);
                }
                let member = members.add_relation_member();
                member.set_relation_idx(idx);
                member.set_role_idx(string_refs[pbf_relation.roles_sid[i] as usize]);
            }
        }
    }
    Ok(stats)
}

/// Minimum bounding box `[min_lon, min_lat, max_lon, max_lat]` of a relation's
/// member points, scaled with `coord_scale`. Returns `None` when the relation
/// has no resolvable member geometry.
fn relation_mbb(points: &[(i32, i32)], coord_scale: i32) -> Option<[i32; 4]> {
    let cs = coord_scale as f64;
    let points: MultiPoint<f64> = points
        .iter()
        .map(|p| (p.0 as f64 / cs, p.1 as f64 / cs))
        .collect::<Vec<_>>()
        .into();
    let r = points.bounding_rect()?;
    Some([
        (r.min().x * cs) as i32,
        (r.min().y * cs) as i32,
        (r.max().x * cs) as i32,
        (r.max().y * cs) as i32,
    ])
}

/// Space-filling-curve index of a relation's bounding box. Computed from the
/// scaled `mbb` (not the raw member points) so it is identical to what the
/// query side recomputes from the stored bounding box.
fn relation_spatial_index(curve: &XZ2SFC, mbb: [i32; 4], coord_scale: i32) -> u64 {
    let cs = coord_scale as f64;
    osmflat::bbox_index(
        curve,
        mbb[0] as f64 / cs,
        mbb[1] as f64 / cs,
        mbb[2] as f64 / cs,
        mbb[3] as f64 / cs,
    )
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
    missing: &mut MissingRefs,
    coord_scale: i32,
) -> Result<(), Error> {
    // We need to build the index of relation ids first, since relations can refer
    // again to relations.
    let (found, unresolved) = build_relations_index(data, blocks.clone().into_iter(), db)?;
    let found = resolve_all_relations(found, unresolved);

    let relations_cf = db.cf_handle(RELATIONS).unwrap();
    let relations_string_refs = db.cf_handle(RELATIONS_STRING_REFS).unwrap();

    let curve = osmflat::way_curve();

    let pb = ProgressBar::new(blocks.len() as u64)
        .with_style(pb_style())
        .with_prefix("Converting relations");

    // Store every relation, keyed by its spatial index, so iterating the column
    // family yields spatial order. Relations with no resolvable member geometry
    // get the `RELATION_NO_BBOX` sentinel and a `u64::MAX` key so they sort last
    // and are never matched spatially (but are still emitted).
    for v in blocks
        .into_iter()
        .map(|idx| read_block::<PrimitiveBlock>(data, &idx))
    {
        let block = v?;

        let string_refs = add_string_table(&block.stringtable, stringtable)?;

        pb.inc(1);

        for rel in block.primitivegroup.into_iter().flat_map(|g| g.relations) {
            let id = rel.id;
            let mbb = found
                .get(&id)
                .and_then(|info| relation_mbb(&info.points, coord_scale));
            let spatial_index = match mbb {
                Some(mbb) => relation_spatial_index(&curve, mbb, coord_scale),
                None => u64::MAX,
            };
            let key = OsmKey::new(spatial_index, id).serialize();
            db.put_cf(relations_cf, &key, rel.encode_to_vec())?;
            db.put_cf(
                relations_string_refs,
                &key,
                create_relation_values(string_refs.as_slice()),
            )?;
        }
    }
    pb.finish();

    // First pass over the spatially-ordered relations: map each relation id to
    // its final index, so relation members can be resolved in the second pass
    // (a relation may reference another relation that sorts after it).
    let mut relation_id_to_idx: AHashMap<i64, u64> = AHashMap::new();
    for (idx, res) in db
        .iterator_cf(relations_cf, rocksdb::IteratorMode::Start)
        .enumerate()
    {
        let (key, _) = res?;
        relation_id_to_idx.insert(OsmKey::from(key).id, idx as u64);
    }

    let mut relations = builder.start_relations()?;
    let mut relation_members = builder.start_relation_members()?;

    let pb = ProgressBar::new(relation_id_to_idx.len() as u64)
        .with_style(pb_style())
        .with_prefix("Ordering relations");

    // Second pass: write the relations in spatial order, resolving members.
    for res in db
        .iterator_cf(relations_cf, rocksdb::IteratorMode::Start)
        .zip(db.iterator_cf(relations_string_refs, rocksdb::IteratorMode::Start))
    {
        let (key, rel) = res.0?;
        let (_, string_refs) = res.1?;

        let id = OsmKey::from(key).id;
        let relation = osmpbf::Relation::decode(rel.to_vec().as_slice())?;
        let string_refs = break_relation_values(&string_refs);

        // Relations without resolvable member geometry carry the sentinel bbox.
        let mbb = found
            .get(&id)
            .and_then(|info| relation_mbb(&info.points, coord_scale))
            .unwrap_or(osmflat::RELATION_NO_BBOX);

        *stats += serialize_relations(
            &relation,
            mbb,
            &relation_id_to_idx,
            db,
            &mut relations,
            &mut relation_ids,
            &mut relation_members,
            string_refs,
            tags,
            missing,
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

    // Keep `_scratch` alive for the whole conversion; dropping it removes the
    // temporary RocksDB directory. `db` (declared here) is dropped before
    // `_scratch`, closing the database before its files are deleted.
    let scratch_parent = args.output.parent().unwrap_or_else(|| Path::new("."));
    let (db, _scratch) = create_db(scratch_parent)?;

    let mut stats = Stats::default();
    let mut missing = MissingRefs::default();

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
        &mut missing,
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
        &mut missing,
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
    println!("{missing}");
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
