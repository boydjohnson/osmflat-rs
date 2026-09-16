use std::collections::BTreeSet;

use crate::{
    osmpbf,
    processing::{
        storage::{EmptyValue, OrdinalKey, OsmIdxValue, RefKey},
        TempDataCodec,
    },
};
use ahash::AHashMap;
use prost::Message;

/// RocksDB column family holding the encoded relations, keyed by spatial order.
/// Each value is produced by [`encode_relation`].
pub const RELATIONS: &str = "relations";

/// Encode a relation for the [`RELATIONS`] column family, together with the
/// global string-table ids it references.
///
/// `block_string_refs` maps the PBF block's string-table indices to global
/// string ids. Storing that whole mapping per relation (as this used to do) is
/// catastrophically wasteful: a relation block's string table averages ~12K
/// entries, so on a Germany extract 910K relations wrote 81 GiB into RocksDB
/// -- one 175s compaction for what is really a few hundred MB of data.
/// Instead `rel`'s `keys`, `vals` and `roles_sid` are rewritten in place to
/// index a per-relation list holding only the (deduplicated) global ids this
/// relation actually uses, and that list is stored inline ahead of the
/// protobuf bytes:
///
/// ```text
/// [n: u64 BE][n x global string id: u64 BE][prost-encoded Relation]
/// ```
///
/// Global ids are byte offsets into the string table, so they can exceed the
/// protobuf fields' 32-bit width on a planet file; the local indices never do.
pub fn encode_relation(rel: &mut osmpbf::Relation, block_string_refs: &[u64]) -> Vec<u8> {
    let mut local: Vec<u64> = Vec::new();
    let mut lookup: AHashMap<u64, u32> = AHashMap::new();
    let mut remap = |block_idx: usize| -> u32 {
        let global = block_string_refs[block_idx];
        *lookup.entry(global).or_insert_with(|| {
            local.push(global);
            (local.len() - 1) as u32
        })
    };
    for k in &mut rel.keys {
        *k = remap(*k as usize);
    }
    for v in &mut rel.vals {
        *v = remap(*v as usize);
    }
    for r in &mut rel.roles_sid {
        *r = remap(*r as usize) as i32;
    }

    let mut out = Vec::with_capacity(8 + 8 * local.len() + rel.encoded_len());
    out.extend((local.len() as u64).to_be_bytes());
    for s in &local {
        out.extend(s.to_be_bytes());
    }
    rel.encode(&mut out)
        .expect("Vec<u8> has unbounded capacity");
    out
}

/// Inverse of [`encode_relation`]: the relation (with block-local
/// `keys`/`vals`/`roles_sid` indexing the returned list) and its global
/// string ids.
pub fn decode_relation(bytes: &[u8]) -> Result<(osmpbf::Relation, Vec<u64>), prost::DecodeError> {
    let n = u64::from_be_bytes(bytes[..8].try_into().unwrap()) as usize;
    let string_refs = bytes[8..8 + 8 * n]
        .chunks(8)
        .map(|c| u64::from_be_bytes(c.try_into().unwrap()))
        .collect();
    let rel = osmpbf::Relation::decode(&bytes[8 + 8 * n..])?;
    Ok((rel, string_refs))
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
