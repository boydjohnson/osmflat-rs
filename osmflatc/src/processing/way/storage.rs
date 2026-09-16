use crate::processing::{
    storage::{EmptyValue, OsmIdKey, OsmIdxValue, OsmKey},
    Key, TempDataCodec, Value,
};

/// A way as read from the PBF: raw OSM node ids (in way order) and interned
/// tag string ids. Stored keyed by way id until its node locations have been
/// merge-joined in and its spatial key is known.
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

pub struct WayByIdTDC;

impl TempDataCodec for WayByIdTDC {
    type Key = OsmIdKey;

    type Value = WayValue;

    const NAME: &'static str = "WAYS_BY_ID";
}

/// Sentinel for a node ref whose node is absent from the archive.
const UNRESOLVED_IDX: u64 = u64::MAX;

/// A way ready to be written to the archive: each node ref already resolved to
/// its final node index (`None` if the node is absent), the OSM ids of those
/// absent nodes (for the missing-refs report), and tags.
pub struct ResolvedWayValue {
    pub node_idxs: Vec<Option<u64>>,
    pub missing_node_ids: Vec<i64>,
    pub key_vals: Vec<(u64, u64)>,
}

impl ResolvedWayValue {
    pub fn new(
        node_idxs: Vec<Option<u64>>,
        missing_node_ids: Vec<i64>,
        key_vals: Vec<(u64, u64)>,
    ) -> Self {
        Self {
            node_idxs,
            missing_node_ids,
            key_vals,
        }
    }
}

impl From<Box<[u8]>> for ResolvedWayValue {
    fn from(bytes: Box<[u8]>) -> Self {
        let num_refs = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let num_missing = u32::from_be_bytes(bytes[4..8].try_into().unwrap()) as usize;
        let mut rest = &bytes[8..];

        let (refs, tail) = rest.split_at(num_refs * 8);
        let node_idxs = refs
            .chunks(8)
            .map(|c| match u64::from_be_bytes(c.try_into().unwrap()) {
                UNRESOLVED_IDX => None,
                idx => Some(idx),
            })
            .collect();
        rest = tail;

        let (missing, tail) = rest.split_at(num_missing * 8);
        let missing_node_ids = missing
            .chunks(8)
            .map(|c| i64::from_be_bytes(c.try_into().unwrap()))
            .collect();
        rest = tail;

        let key_vals = rest
            .chunks(16)
            .map(|c| {
                (
                    u64::from_be_bytes(c[0..8].try_into().unwrap()),
                    u64::from_be_bytes(c[8..16].try_into().unwrap()),
                )
            })
            .collect();

        Self {
            node_idxs,
            missing_node_ids,
            key_vals,
        }
    }
}

impl Value for ResolvedWayValue {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            8 + 8 * self.node_idxs.len()
                + 8 * self.missing_node_ids.len()
                + 16 * self.key_vals.len(),
        );
        out.extend((self.node_idxs.len() as u32).to_be_bytes());
        out.extend((self.missing_node_ids.len() as u32).to_be_bytes());
        for idx in &self.node_idxs {
            out.extend(idx.unwrap_or(UNRESOLVED_IDX).to_be_bytes());
        }
        for id in &self.missing_node_ids {
            out.extend(id.to_be_bytes());
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

    type Value = ResolvedWayValue;

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

/// One way->node reference, keyed so a full scan visits refs in ascending
/// *node id* order -- the order of `NodeIdToIdx` and `NodeIdToLonLat` -- for
/// the way pass's sort-merge join. `way_id` and `pos` (the ref's position in
/// the way) route the resolved node back to its slot.
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct WayNodeRefKey {
    pub node_id: i64,
    pub way_id: i64,
    pub pos: u32,
}

impl WayNodeRefKey {
    pub fn new(node_id: i64, way_id: i64, pos: u32) -> Self {
        Self {
            node_id,
            way_id,
            pos,
        }
    }
}

impl Key for WayNodeRefKey {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(20);
        out.extend(self.node_id.to_be_bytes());
        out.extend(self.way_id.to_be_bytes());
        out.extend(self.pos.to_be_bytes());
        out
    }
}

impl From<Box<[u8]>> for WayNodeRefKey {
    fn from(bytes: Box<[u8]>) -> Self {
        Self {
            node_id: i64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            way_id: i64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            pos: u32::from_be_bytes(bytes[16..20].try_into().unwrap()),
        }
    }
}

pub struct WayNodeRefTDC;

impl TempDataCodec for WayNodeRefTDC {
    type Key = WayNodeRefKey;
    type Value = EmptyValue;

    const NAME: &'static str = "WAY_NODE_REF";
}

/// Output key of the join: sorts by way id, then position within the way, so
/// a scan replays each way's resolved nodes in way order, aligned with a scan
/// of `WayByIdTDC`.
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct WayPosKey {
    pub way_id: i64,
    pub pos: u32,
}

impl WayPosKey {
    pub fn new(way_id: i64, pos: u32) -> Self {
        Self { way_id, pos }
    }
}

impl Key for WayPosKey {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(12);
        out.extend(self.way_id.to_be_bytes());
        out.extend(self.pos.to_be_bytes());
        out
    }
}

impl From<Box<[u8]>> for WayPosKey {
    fn from(bytes: Box<[u8]>) -> Self {
        Self {
            way_id: i64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            pos: u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
        }
    }
}

/// A resolved way->node ref: the node's archive index and/or its location.
/// Both come from node-id-keyed stores written for every node, so in practice
/// both are present; each is optional so a ref present in only one store
/// behaves exactly as the separate lookups did before.
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct ResolvedNodeValue {
    pub idx: Option<u64>,
    pub location: Option<(i32, i32)>,
}

impl ResolvedNodeValue {
    pub fn new(idx: Option<u64>, location: Option<(i32, i32)>) -> Self {
        Self { idx, location }
    }
}

impl Value for ResolvedNodeValue {
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16);
        out.extend(self.idx.unwrap_or(UNRESOLVED_IDX).to_be_bytes());
        if let Some((lon, lat)) = self.location {
            out.extend(lon.to_be_bytes());
            out.extend(lat.to_be_bytes());
        }
        out
    }
}

impl From<Box<[u8]>> for ResolvedNodeValue {
    fn from(bytes: Box<[u8]>) -> Self {
        let idx = match u64::from_be_bytes(bytes[0..8].try_into().unwrap()) {
            UNRESOLVED_IDX => None,
            idx => Some(idx),
        };
        let location = (bytes.len() >= 16).then(|| {
            (
                i32::from_be_bytes(bytes[8..12].try_into().unwrap()),
                i32::from_be_bytes(bytes[12..16].try_into().unwrap()),
            )
        });
        Self { idx, location }
    }
}

pub struct WayNodeResolvedTDC;

impl TempDataCodec for WayNodeResolvedTDC {
    type Key = WayPosKey;
    type Value = ResolvedNodeValue;

    const NAME: &'static str = "WAY_NODE_RESOLVED";
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip<V: Value>(v: &V) -> V {
        V::from(v.serialize().into_boxed_slice())
    }

    #[test]
    fn resolved_way_value_roundtrips() {
        let v = ResolvedWayValue::new(
            vec![Some(0), None, Some(u64::MAX - 1)],
            vec![42],
            vec![(1, 2), (3, 4)],
        );
        let back = roundtrip(&v);
        assert_eq!(back.node_idxs, v.node_idxs);
        assert_eq!(back.missing_node_ids, v.missing_node_ids);
        assert_eq!(back.key_vals, v.key_vals);

        let empty = roundtrip(&ResolvedWayValue::new(vec![], vec![], vec![]));
        assert!(empty.node_idxs.is_empty() && empty.key_vals.is_empty());
    }

    #[test]
    fn resolved_node_value_roundtrips() {
        for v in [
            ResolvedNodeValue::new(Some(7), Some((-180_000_000, 90_000_000))),
            ResolvedNodeValue::new(None, Some((0, 0))),
            ResolvedNodeValue::new(Some(7), None),
            ResolvedNodeValue::new(None, None),
        ] {
            assert_eq!(roundtrip(&v), v);
        }
    }

    #[test]
    fn way_keys_roundtrip_and_sort_by_leading_field() {
        let a = WayNodeRefKey::new(5, 900, 3);
        let b = WayNodeRefKey::new(6, 1, 0);
        assert_eq!(WayNodeRefKey::from(a.serialize().into_boxed_slice()), a);
        assert!(a.serialize() < b.serialize());

        let c = WayPosKey::new(10, 2);
        let d = WayPosKey::new(10, 11);
        assert_eq!(WayPosKey::from(c.serialize().into_boxed_slice()), c);
        assert!(c.serialize() < d.serialize());
    }
}
