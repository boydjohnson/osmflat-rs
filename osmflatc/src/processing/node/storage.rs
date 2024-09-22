use crate::processing::storage::OsmIdKey;
use crate::processing::storage::OsmIdxValue;
use crate::processing::storage::OsmKey;
use crate::processing::TempDataCodec;
use crate::processing::Value;

pub struct NodeValue {
    pub refs: Vec<(u64, u64)>,
}

impl NodeValue {
    pub fn new(refs: Vec<(u64, u64)>) -> Self {
        Self { refs }
    }
}

impl From<Box<[u8]>> for NodeValue {
    fn from(bytes: Box<[u8]>) -> Self {
        let refs = bytes
            .chunks(16)
            .map(|chunk| {
                let key = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
                let value = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
                (key, value)
            })
            .collect();
        Self { refs }
    }
}

impl Value for NodeValue {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16 * self.refs.len());
        for &(key, value) in &self.refs {
            out.extend(&key.to_le_bytes());
            out.extend(&value.to_le_bytes());
        }
        out
    }
}

pub struct NodesTDC;

impl TempDataCodec for NodesTDC {
    type Key = OsmKey;
    type Value = NodeValue;

    const NAME: &'static str = "NODES";
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct NodeLonLatValue {
    pub lon: i32,
    pub lat: i32,
}

impl NodeLonLatValue {
    pub fn new(lon: i32, lat: i32) -> Self {
        NodeLonLatValue { lon, lat }
    }
}

impl Value for NodeLonLatValue {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8);
        out.extend_from_slice(&self.lon.to_be_bytes());
        out.extend_from_slice(&self.lat.to_be_bytes());
        out
    }
}

impl From<Box<[u8]>> for NodeLonLatValue {
    fn from(value: Box<[u8]>) -> Self {
        let lon = i32::from_be_bytes(value[0..4].try_into().unwrap());
        let lat = i32::from_be_bytes(value[4..8].try_into().unwrap());
        NodeLonLatValue::new(lon, lat)
    }
}

pub struct NodeIdToLonLatTDC;

impl TempDataCodec for NodeIdToLonLatTDC {
    type Key = OsmIdKey;

    type Value = NodeLonLatValue;

    const NAME: &'static str = "NODE_ID_TO_LON_LAT";
}

pub struct NodeIdToIdxTDC;

impl TempDataCodec for NodeIdToIdxTDC {
    type Key = OsmIdKey;

    type Value = OsmIdxValue;

    const NAME: &'static str = "NODE_ID_TO_IDX";
}
