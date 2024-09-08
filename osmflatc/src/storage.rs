use std::collections::BTreeSet;

use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use space_time::xzorder::xz2_sfc::XZ2SFC;

pub const RESOLUTION: u32 = 12;

pub const NODES: &str = "nodes";
pub const NODE_ID_TO_IDX: &str = "node_id_to_idx";
pub const NODE_ID_TO_LAT_LON: &str = "node_id_to_lat_lon";
pub const WAYS: &str = "ways";
pub const WAY_ID_TO_IDX: &str = "way_id_to_idx";
pub const WAY_ID_TO_MBR: &str = "way_id_to_mbr";
pub const RELATIONS: &str = "relations";
pub const RELATIONS_STRING_REFS: &str = "relations_string_refs";

pub fn create_key(spatial_index: u64, id: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);

    out.extend(spatial_index.to_be_bytes());
    out.extend(id.to_be_bytes());
    out
}

pub fn create_node_spatial_index(curve: &XZ2SFC, lat: f64, lon: f64) -> u64 {
    curve.index(lon, lat, lon, lat)
}

pub fn break_key(bytes: &[u8]) -> (u64, i64) {
    assert!(bytes.len() == 16, "Key bytes were unexpected length");
    let spatial_index = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let id = i64::from_be_bytes(bytes[8..16].try_into().unwrap());
    (spatial_index, id)
}

pub fn create_node_values(lat: i32, lon: i32, tag_first_index: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);

    out.extend(lat.to_be_bytes());
    out.extend(lon.to_be_bytes());
    out.extend(tag_first_index.to_be_bytes());
    out
}

pub fn create_node_lat_lon(lat: i32, lon: i32) -> Vec<u8> {
    let mut out = Vec::with_capacity(8);
    out.extend(lat.to_be_bytes());
    out.extend(lon.to_be_bytes());
    out
}

pub fn break_node_lat_lon(bytes: Vec<u8>) -> (i32, i32) {
    assert!(bytes.len() == 8, "Value bytes were unexpected length");

    let lat = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
    let lon = i32::from_be_bytes(bytes[4..8].try_into().unwrap());
    (lat, lon)
}

pub fn break_node_values(bytes: &[u8]) -> (i32, i32, u64) {
    assert!(bytes.len() == 16, "Value bytes were unexpected length");

    let lat = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
    let lon = i32::from_be_bytes(bytes[4..8].try_into().unwrap());
    let tag_first_idx = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
    (lat, lon, tag_first_idx)
}

pub fn create_way_values(tag_first_index: u64, node_refs: Vec<i64>) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 * (node_refs.len() + 1));

    out.extend(tag_first_index.to_be_bytes());

    for r in node_refs {
        out.extend(r.to_be_bytes());
    }
    out
}

pub fn break_way_values(bytes: &[u8]) -> (u64, Vec<i64>) {
    let tag_first_index = u64::from_be_bytes(bytes[0..8].try_into().unwrap());

    (
        tag_first_index,
        bytes[8..]
            .chunks(8)
            .map(|v| i64::from_be_bytes(v[0..8].try_into().unwrap()))
            .collect(),
    )
}

pub fn create_relation_values(string_refs: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 * string_refs.len());
    for s in string_refs {
        out.extend(s.to_be_bytes());
    }
    out
}

pub fn break_relation_values(bytes: &[u8]) -> Vec<u64> {
    bytes
        .chunks(8)
        .map(|chunk| u64::from_be_bytes(chunk[0..8].try_into().unwrap()))
        .collect()
}

pub fn create_way_mbb(mbb: Vec<i32>) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 * mbb.len());
    for m in mbb {
        out.extend(m.to_be_bytes());
    }
    out
}

pub fn break_way_mbb(bytes: Vec<u8>) -> Vec<i32> {
    bytes
        .chunks(4)
        .map(|chunk| i32::from_be_bytes(chunk[0..4].try_into().unwrap()))
        .collect()
}

pub fn create_db() -> Result<DB, Box<dyn std::error::Error>> {
    let path = "db.db";
    let mut cf_opts = Options::default();
    cf_opts.set_max_write_buffer_number(16);

    let cfs = [
        NODES,
        NODE_ID_TO_IDX,
        NODE_ID_TO_LAT_LON,
        WAYS,
        WAY_ID_TO_IDX,
        WAY_ID_TO_MBR,
        RELATIONS,
        RELATIONS_STRING_REFS,
    ]
    .iter()
    .map(move |v| ColumnFamilyDescriptor::new(v.to_string(), cf_opts.clone()));

    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);
    db_opts.increase_parallelism(16);
    db_opts.set_write_buffer_size(512 * 1024 * 1024);

    Ok(DB::open_cf_descriptors(&db_opts, path, cfs)?)
}

#[derive(Debug, Default)]
pub struct RelationInfo {
    pub id: i64,
    pub points: Vec<(i32, i32)>,
    pub relation_ids: BTreeSet<i64>,
}

impl RelationInfo {
    pub fn is_ready(&self) -> bool {
        self.relation_ids.is_empty()
    }
}
