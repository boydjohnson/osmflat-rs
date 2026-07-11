use crate::processing::{
    storage::{OsmIdKey, OsmIdxValue, OsmKey},
    Key, TempDataCodec, Value,
};

pub struct WayValue {
    pub node_refs: Vec<i64>,
    pub key_vals: Vec<(u64, u64)>,
}

impl WayValue {
    pub fn new(node_refs: Vec<i64>, key_vals: Vec<(u64, u64)>) -> Self {
        Self {
            node_refs,
            key_vals,
        }
    }
}

impl From<Box<[u8]>> for WayValue {
    fn from(bytes: Box<[u8]>) -> Self {
        let num = u64::from_be_bytes(bytes[..8].try_into().unwrap()) as usize;
        let node_refs = bytes[8..]
            .chunks(8)
            .take(num)
            .map(|v| i64::from_be_bytes(v[0..8].try_into().unwrap()))
            .collect();

        let key_vals = bytes[(num * 8 + 8)..]
            .chunks(16)
            .map(|c| {
                (
                    u64::from_be_bytes(c[0..8].try_into().unwrap()),
                    u64::from_be_bytes(c[8..16].try_into().unwrap()),
                )
            })
            .collect();

        WayValue {
            node_refs,
            key_vals,
        }
    }
}

impl Value for WayValue {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 * self.node_refs.len() + 16 * self.key_vals.len() + 8);

        let num = self.node_refs.len() as u64;

        out.extend(num.to_be_bytes());

        for r in &self.node_refs {
            out.extend(r.to_be_bytes());
        }

        for (k, v) in &self.key_vals {
            out.extend(k.to_be_bytes());
            out.extend(v.to_be_bytes());
        }

        out
    }
}

pub struct WayTDC;

impl TempDataCodec for WayTDC {
    type Key = OsmKey;

    type Value = WayValue;

    const NAME: &'static str = "WAYS";
}

pub struct WayIdToIdxTDC;

impl TempDataCodec for WayIdToIdxTDC {
    type Key = OsmIdKey;

    type Value = OsmIdxValue;

    const NAME: &'static str = "WAY_ID_TO_IDX";
}

pub struct WayMbbValue {
    pub mbb: Vec<i32>,
}

impl WayMbbValue {
    pub fn new(mbb: Vec<i32>) -> Self {
        WayMbbValue { mbb }
    }
}

impl Value for WayMbbValue {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 * self.mbb.len());
        for m in &self.mbb {
            out.extend(m.to_be_bytes());
        }
        out
    }
}

impl From<Box<[u8]>> for WayMbbValue {
    fn from(bytes: Box<[u8]>) -> Self {
        WayMbbValue {
            mbb: bytes
                .chunks(4)
                .map(|chunk| i32::from_be_bytes(chunk[0..4].try_into().unwrap()))
                .collect(),
        }
    }
}

pub struct WayIdToMbbTDC;

impl TempDataCodec for WayIdToMbbTDC {
    const NAME: &'static str = "WAY_ID_TO_MBB";

    type Key = OsmIdKey;
    type Value = WayMbbValue;
}

/// Key for `WayRefByNodeTDC`: sorts primarily by the referenced node id (and
/// secondarily by `ordinal` only to keep entries with the same node id
/// distinct), so a full column-family scan visits node ids in the same
/// ascending order as `NodeIdToIdxTDC` -- the two can then be merge-joined
/// with a single forward pass over each instead of one random point lookup
/// per ref. `ordinal` is a way-ref's position in the flat `nodes_index`
/// array being built (spatial-iteration order, assigned once and re-derived
/// identically on a second scan), used to route each resolved index back to
/// the right slot.
#[derive(Debug, Clone, Copy)]
pub struct NodeRefKey {
    pub node_id: i64,
    pub ordinal: u64,
}

impl NodeRefKey {
    pub fn new(node_id: i64, ordinal: u64) -> Self {
        Self { node_id, ordinal }
    }
}

impl Key for NodeRefKey {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16);
        out.extend(self.node_id.to_be_bytes());
        out.extend(self.ordinal.to_be_bytes());
        out
    }
}

impl From<Box<[u8]>> for NodeRefKey {
    fn from(bytes: Box<[u8]>) -> Self {
        let node_id = i64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let ordinal = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        Self { node_id, ordinal }
    }
}

/// Zero-byte value: `WayRefByNodeTDC` only needs the key (the CF is really
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

pub struct WayRefByNodeTDC;

impl TempDataCodec for WayRefByNodeTDC {
    type Key = NodeRefKey;
    type Value = EmptyValue;

    const NAME: &'static str = "WAY_REF_BY_NODE";
}

/// Key for `ResolvedRefTDC`: a way-ref's `ordinal` (see `NodeRefKey`), so a
/// full scan replays resolved indices in the exact order the final write
/// pass re-derives ordinals while re-scanning `WayTDC` -- another sequential
/// merge instead of a random lookup.
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

/// A ref whose node id has no entry here was referenced by a way but is
/// absent from the archive -- no row is written for it, mirroring the old
/// `multi_get` code's `None` result for a missing key.
pub struct ResolvedRefTDC;

impl TempDataCodec for ResolvedRefTDC {
    type Key = OrdinalKey;
    type Value = OsmIdxValue;

    const NAME: &'static str = "RESOLVED_REF_BY_ORDINAL";
}
