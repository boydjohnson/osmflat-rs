use super::{Key, Value};

#[derive(Debug, Clone, Copy)]
pub struct OsmKey {
    pub spatial_index: u64,
    pub id: i64,
}

impl OsmKey {
    pub fn new(spatial_index: u64, id: i64) -> Self {
        Self { spatial_index, id }
    }
}

impl From<Box<[u8]>> for OsmKey {
    fn from(bytes: Box<[u8]>) -> Self {
        let bytes = &bytes[..];
        assert!(bytes.len() == 16, "Key bytes were unexpected length");
        let spatial_index = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let id = i64::from_be_bytes(bytes[8..16].try_into().unwrap());
        Self { spatial_index, id }
    }
}

impl Key for OsmKey {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16);
        out.extend(self.spatial_index.to_be_bytes());
        out.extend(self.id.to_be_bytes());
        out
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct OsmIdKey {
    pub id: i64,
}

impl OsmIdKey {
    pub fn new(id: i64) -> Self {
        OsmIdKey { id }
    }
}

impl Key for OsmIdKey {
    fn serialize(&self) -> Vec<u8> {
        self.id.to_be_bytes().to_vec()
    }
}

impl From<Box<[u8]>> for OsmIdKey {
    fn from(value: Box<[u8]>) -> Self {
        let id = i64::from_be_bytes(value[0..8].try_into().unwrap());
        OsmIdKey { id }
    }
}

pub struct OsmIdxValue {
    pub idx: u64,
}

impl OsmIdxValue {
    pub fn new(idx: u64) -> Self {
        OsmIdxValue { idx }
    }
}

impl Value for OsmIdxValue {
    fn serialize(&self) -> Vec<u8> {
        self.idx.to_be_bytes().to_vec()
    }
}

impl From<Box<[u8]>> for OsmIdxValue {
    fn from(value: Box<[u8]>) -> Self {
        OsmIdxValue {
            idx: u64::from_be_bytes(value[0..8].try_into().unwrap()),
        }
    }
}

/// Key for a sort-merge join's forward index: sorts primarily by the
/// referenced id (a node or way id, depending on which target CF it will be
/// merge-joined against) and secondarily by `ordinal` only to keep entries
/// for the same referenced id distinct. A full column-family scan then
/// visits ids in the same ascending order as the id-keyed CF being resolved
/// against (e.g. `NodeIdToIdxTDC`, `WayIdToIdxTDC`), so the two can be
/// merge-joined with a single forward pass over each instead of one random
/// point lookup per reference. `ordinal` is the reference's position in
/// whatever flat array is being built (assigned once, re-derived identically
/// on a second scan in the same order), used to route each resolved index
/// back to the right slot. Shared by the way- and relation-ordering passes;
/// see way.rs/relation.rs for how each uses it.
#[derive(Debug, Clone, Copy)]
pub struct RefKey {
    pub target_id: i64,
    pub ordinal: u64,
}

impl RefKey {
    pub fn new(target_id: i64, ordinal: u64) -> Self {
        Self { target_id, ordinal }
    }
}

impl Key for RefKey {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16);
        out.extend(self.target_id.to_be_bytes());
        out.extend(self.ordinal.to_be_bytes());
        out
    }
}

impl From<Box<[u8]>> for RefKey {
    fn from(bytes: Box<[u8]>) -> Self {
        let target_id = i64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let ordinal = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        Self { target_id, ordinal }
    }
}

/// Key for a sort-merge join's resolved-output index: an `ordinal` (see
/// `RefKey`), so a full scan replays resolved indices in the exact order a
/// later pass re-derives ordinals while re-scanning the same source data --
/// another sequential merge instead of a random lookup.
#[derive(Debug, Clone, Copy)]
pub struct OrdinalKey {
    pub ordinal: u64,
}

impl OrdinalKey {
    pub fn new(ordinal: u64) -> Self {
        Self { ordinal }
    }
}

impl Key for OrdinalKey {
    fn serialize(&self) -> Vec<u8> {
        self.ordinal.to_be_bytes().to_vec()
    }
}

impl From<Box<[u8]>> for OrdinalKey {
    fn from(bytes: Box<[u8]>) -> Self {
        Self {
            ordinal: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
        }
    }
}

/// Zero-byte value: a `RefKey`-indexed CF only needs the key (it's really
/// just an index), so there is nothing to store per entry.
pub struct EmptyValue;

impl Value for EmptyValue {
    fn serialize(&self) -> Vec<u8> {
        Vec::new()
    }
}

impl From<Box<[u8]>> for EmptyValue {
    fn from(_: Box<[u8]>) -> Self {
        EmptyValue
    }
}
