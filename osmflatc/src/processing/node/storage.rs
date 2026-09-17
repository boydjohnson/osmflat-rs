use crate::processing::storage::OsmIdKey;
use crate::processing::storage::OsmKey;
use crate::processing::TempDataCodec;
use crate::processing::Value;

pub struct NodeValue {
    pub lon: i32,
    pub lat: i32,
    pub refs: Vec<(u64, u64)>,
}

impl NodeValue {
    pub fn new(lon: i32, lat: i32, refs: Vec<(u64, u64)>) -> Self {
        Self { lon, lat, refs }
    }
}

impl From<&[u8]> for NodeValue {
    fn from(bytes: &[u8]) -> Self {
        // Layout: lon (i32 LE), lat (i32 LE), then 16-byte (key, value) ref pairs.
        // Coordinates are stored inline so the spatial-ordering pass can read
        // them straight from this sequential scan instead of doing a random
        // `NodeIdToLonLat` lookup per node.
        let lon = i32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let lat = i32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let refs = bytes[8..]
            .chunks(16)
            .map(|chunk| {
                let key = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
                let value = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
                (key, value)
            })
            .collect();
        Self { lon, lat, refs }
    }
}

impl Value for NodeValue {
    fn serialize_into(&self, out: &mut Vec<u8>) {
        out.extend(&self.lon.to_le_bytes());
        out.extend(&self.lat.to_le_bytes());
        for &(key, value) in &self.refs {
            out.extend(&key.to_le_bytes());
            out.extend(&value.to_le_bytes());
        }
    }
}

pub struct NodesTDC;

impl TempDataCodec for NodesTDC {
    type Key = OsmKey;
    type Value = NodeValue;

    const NAME: &'static str = "NODES";
}

/// A node's final archive index together with its (scaled) location. Stored
/// by node id so the way and relation passes resolve both from one sorted
/// scan or point lookup.
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct NodeIdxLocValue {
    pub idx: u64,
    pub lon: i32,
    pub lat: i32,
}

impl NodeIdxLocValue {
    pub fn new(idx: u64, lon: i32, lat: i32) -> Self {
        Self { idx, lon, lat }
    }
}

impl Value for NodeIdxLocValue {
    fn serialize_into(&self, out: &mut Vec<u8>) {
        out.extend(self.idx.to_be_bytes());
        out.extend(self.lon.to_be_bytes());
        out.extend(self.lat.to_be_bytes());
    }
}

impl From<&[u8]> for NodeIdxLocValue {
    fn from(bytes: &[u8]) -> Self {
        Self {
            idx: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            lon: i32::from_be_bytes(bytes[8..12].try_into().unwrap()),
            lat: i32::from_be_bytes(bytes[12..16].try_into().unwrap()),
        }
    }
}

pub struct NodeIdToIdxTDC;

impl TempDataCodec for NodeIdToIdxTDC {
    type Key = OsmIdKey;

    type Value = NodeIdxLocValue;

    const NAME: &'static str = "NODE_ID_TO_IDX";
}

#[cfg(test)]
mod tests {
    use super::{NodeIdxLocValue, NodeValue};
    use crate::processing::Value;

    #[test]
    fn node_idx_loc_value_roundtrips() {
        let v = NodeIdxLocValue::new(u64::MAX - 1, -180_000_000, 90_000_000);
        assert_eq!(NodeIdxLocValue::from(v.serialize().as_slice()), v);
    }

    #[test]
    fn node_value_roundtrips_with_coords_and_tags() {
        let v = NodeValue::new(-180_000_000, 90_000_000, vec![(1, 2), (3, 4)]);
        let back = NodeValue::from(v.serialize().as_slice());
        assert_eq!(back.lon, -180_000_000);
        assert_eq!(back.lat, 90_000_000);
        assert_eq!(back.refs, vec![(1, 2), (3, 4)]);
    }

    #[test]
    fn node_value_roundtrips_without_tags() {
        let back = NodeValue::from(NodeValue::new(7, -7, vec![]).serialize().as_slice());
        assert_eq!((back.lon, back.lat), (7, -7));
        assert!(back.refs.is_empty());
    }
}
