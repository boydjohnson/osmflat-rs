use std::collections::BTreeSet;

use crate::processing::{
    storage::{EmptyValue, OrdinalKey, OsmIdxValue, RefKey},
    TempDataCodec,
};

/// RocksDB column family holding the encoded relations, keyed by spatial order.
pub const RELATIONS: &str = "relations";
/// RocksDB column family holding the relations' string references, in the same
/// order as [`RELATIONS`].
pub const RELATIONS_STRING_REFS: &str = "relations_string_refs";

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

/// Forward index for the sort-merge join in the relation-member ordering
/// pass, keyed by referenced node id -- merge-joined against `NodeIdToIdx`
/// the same way `WayRefByNodeTDC` is joined for way-refs. See relation.rs.
pub struct RelationNodeMemberRefTDC;

impl TempDataCodec for RelationNodeMemberRefTDC {
    type Key = RefKey;
    type Value = EmptyValue;

    const NAME: &'static str = "RELATION_NODE_MEMBER_REF";
}

/// Forward index keyed by referenced way id, merge-joined against
/// `WayIdToIdx`. See relation.rs.
pub struct RelationWayMemberRefTDC;

impl TempDataCodec for RelationWayMemberRefTDC {
    type Key = RefKey;
    type Value = EmptyValue;

    const NAME: &'static str = "RELATION_WAY_MEMBER_REF";
}

/// Resolved `ordinal -> idx` output of both merge-joins above, keyed by each
/// member's position in the flat `relation_members` structure being built.
/// Node- and way-member ordinals are disjoint subsets of the same ordinal
/// space (assigned once per relation-member regardless of type), so both
/// merge-joins can safely write into this one CF. Relation-type members
/// never get an entry here -- they're resolved directly from the in-memory
/// `relation_id_to_idx` map instead, since that needs no disk I/O at all.
pub struct RelationMemberResolvedTDC;

impl TempDataCodec for RelationMemberResolvedTDC {
    type Key = OrdinalKey;
    type Value = OsmIdxValue;

    const NAME: &'static str = "RELATION_MEMBER_RESOLVED_BY_ORDINAL";
}
