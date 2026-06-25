use space_time::{xzorder::xz2_sfc::XZ2SFC, zorder::z_curve_2d::ZCurve2D};

use crate::{Node, Osm, Relation, Way};

/// Resolution of the [XZ2SFC] curve used to order ways and relations.
///
/// This value is the single source of truth shared by the `osmflatc`
/// compiler (which writes the archives in this order) and the query
/// functions below (which binary-search assuming this order). Changing it
/// here changes both sides at once; the two must never be configured
/// independently or the spatial ordering would silently desync.
pub const SPATIAL_RESOLUTION: u32 = 18;

/// The curve used to order and query [Node]s by location.
pub fn node_curve() -> ZCurve2D {
    ZCurve2D::default()
}

/// The curve used to order and query [Way]s (and relations) by their
/// minimum bounding box.
pub fn way_curve() -> XZ2SFC {
    XZ2SFC::wgs84(SPATIAL_RESOLUTION)
}

/// Spatial index of a single point. Both the compiler and the query side
/// must compute node indices through this function so they stay identical.
pub fn node_index(curve: &ZCurve2D, lon: f64, lat: f64) -> u64 {
    curve.index(lon, lat)
}

/// Spatial index of a bounding box, used for ways and relations. Both the
/// compiler and the query side must compute indices through this function.
pub fn bbox_index(curve: &XZ2SFC, min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> u64 {
    curve.index(min_x, min_y, max_x, max_y)
}

/// Return [Node]s from the archive that are inside the
/// bounding box.
pub fn find_nodes_by_bounding_box(
    archive: &Osm,
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
) -> impl Iterator<Item = &Node> {
    let curve = node_curve();

    let coord_scale = archive.header().coord_scale();
    let ranges = curve.ranges(xmin, ymin, xmax, ymax, &[]);

    // The last node is a sentinel that only carries the end of the tag range;
    // it has no real location and must be excluded from the ordered search.
    let num_nodes = archive.nodes().len().saturating_sub(1);

    ranges
        .into_iter()
        .flat_map(move |b| {
            let nodes = &archive.nodes()[..num_nodes];
            let lower_index =
                nodes.partition_point(|n| spatial_index_node(&curve, n, coord_scale) < b.lower());
            let upper_relative_index = nodes[lower_index..]
                .partition_point(|n| spatial_index_node(&curve, n, coord_scale) <= b.upper());

            nodes[lower_index..(lower_index + upper_relative_index)].iter()
        })
        .filter(move |n| {
            let lat = n.lat() as f64 / coord_scale as f64;
            let lon = n.lon() as f64 / coord_scale as f64;

            lat >= ymin && lat <= ymax && lon >= xmin && lon <= xmax
        })
}

fn spatial_index_node(curve: &ZCurve2D, node: &Node, coord_scale: i32) -> u64 {
    let lat = node.lat() as f64 / coord_scale as f64;
    let lon = node.lon() as f64 / coord_scale as f64;

    node_index(curve, lon, lat)
}

/// Recompute the minimum bounding box `(min_x, min_y, max_x, max_y)` of the
/// way at `way_idx` from its node references, in degrees. Returns `None` when
/// the way has no resolvable node locations.
///
/// This mirrors the bounding box the compiler computed when it placed the way
/// in spatial order, so binary searching the ways with the index derived from
/// it is valid.
fn way_bounding_box(
    archive: &Osm,
    way_idx: usize,
    coord_scale: f64,
) -> Option<(f64, f64, f64, f64)> {
    let ways = archive.ways();
    // `way_idx` is always < num_ways, so the sentinel guarantees `way_idx + 1`
    // is in bounds and yields the end of this way's node range.
    let begin = ways[way_idx].ref_first_idx() as usize;
    let end = ways[way_idx + 1].ref_first_idx() as usize;

    let nodes_index = archive.nodes_index();
    let nodes = archive.nodes();

    let mut bbox: Option<(f64, f64, f64, f64)> = None;
    for ni in &nodes_index[begin..end] {
        let Some(node_idx) = ni.value() else { continue };
        let node = &nodes[node_idx as usize];
        let x = node.lon() as f64 / coord_scale;
        let y = node.lat() as f64 / coord_scale;
        bbox = Some(match bbox {
            Some((min_x, min_y, max_x, max_y)) => {
                (min_x.min(x), min_y.min(y), max_x.max(x), max_y.max(y))
            }
            None => (x, y, x, y),
        });
    }
    bbox
}

fn spatial_index_way(archive: &Osm, curve: &XZ2SFC, way_idx: usize, coord_scale: f64) -> u64 {
    match way_bounding_box(archive, way_idx, coord_scale) {
        Some((min_x, min_y, max_x, max_y)) => bbox_index(curve, min_x, min_y, max_x, max_y),
        None => 0,
    }
}

/// Partition point over the index range `0..len`, mirroring the semantics of
/// [`slice::partition_point`] but allowing the predicate to look at neighbours
/// (needed because a way's geometry spans the following entry).
fn partition_point_by(len: usize, mut pred: impl FnMut(usize) -> bool) -> usize {
    let mut lo = 0;
    let mut hi = len;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if pred(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Return [Way]s that are in the archive and inside the bounding box.
///
/// #Note: Includes those [Way]s that overlap the bounding box without
///        being fully contained by it.
pub fn find_ways_by_bounding_box(
    archive: &Osm,
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
) -> impl Iterator<Item = &Way> {
    let curve = way_curve();
    let coord_scale = archive.header().coord_scale() as f64;
    // Exclude the trailing sentinel way.
    let num_ways = archive.ways().len().saturating_sub(1);

    curve
        .ranges(xmin, ymin, xmax, ymax, None)
        .into_iter()
        .flat_map(move |b| {
            let lower = partition_point_by(num_ways, |i| {
                spatial_index_way(archive, &curve, i, coord_scale) < b.lower()
            });
            let upper = partition_point_by(num_ways, |i| {
                spatial_index_way(archive, &curve, i, coord_scale) <= b.upper()
            });
            lower..upper
        })
        .filter(move |&i| match way_bounding_box(archive, i, coord_scale) {
            // Exact overlap test to drop the false positives the curve ranges
            // over-select.
            Some((min_x, min_y, max_x, max_y)) => {
                !(max_x < xmin || min_x > xmax || max_y < ymin || min_y > ymax)
            }
            None => false,
        })
        .map(move |i| &archive.ways()[i])
}

fn spatial_index_relation(curve: &XZ2SFC, relation: &Relation, coord_scale: f64) -> u64 {
    bbox_index(
        curve,
        relation.min_lon() as f64 / coord_scale,
        relation.min_lat() as f64 / coord_scale,
        relation.max_lon() as f64 / coord_scale,
        relation.max_lat() as f64 / coord_scale,
    )
}

/// Return [Relation]s in the archive whose bounding box overlaps the query box.
///
/// #Note: Includes those [Relation]s that overlap the bounding box without
///        being fully contained by it.
pub fn find_relations_by_bounding_box(
    archive: &Osm,
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
) -> impl Iterator<Item = &Relation> {
    let curve = way_curve();
    let coord_scale = archive.header().coord_scale() as f64;
    // Exclude the trailing sentinel relation.
    let num_relations = archive.relations().len().saturating_sub(1);

    curve
        .ranges(xmin, ymin, xmax, ymax, None)
        .into_iter()
        .flat_map(move |b| {
            let relations = &archive.relations()[..num_relations];
            let lower = relations
                .partition_point(|r| spatial_index_relation(&curve, r, coord_scale) < b.lower());
            let upper_relative = relations[lower..]
                .partition_point(|r| spatial_index_relation(&curve, r, coord_scale) <= b.upper());

            relations[lower..(lower + upper_relative)].iter()
        })
        .filter(move |r| {
            // Exact overlap test to drop the false positives the curve ranges
            // over-select.
            let min_x = r.min_lon() as f64 / coord_scale;
            let min_y = r.min_lat() as f64 / coord_scale;
            let max_x = r.max_lon() as f64 / coord_scale;
            let max_y = r.max_lat() as f64 / coord_scale;

            !(max_x < xmin || min_x > xmax || max_y < ymin || min_y > ymax)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Header, Node, NodeIndex, Osm, OsmBuilder, Relation, Way};
    use flatdata::MemoryResourceStorage;

    const COORD_SCALE: i32 = 10_000_000;

    fn scale(v: f64) -> i32 {
        (v * COORD_SCALE as f64) as i32
    }

    fn unscale(v: i32) -> f64 {
        v as f64 / COORD_SCALE as f64
    }

    /// Build an in-memory archive laid out exactly the way `osmflatc` does:
    /// nodes ordered by the z-order curve, ways and relations ordered by the
    /// `XZ2SFC` index of their bounding box, each vector terminated by a
    /// sentinel. This lets the query functions binary-search correctly.
    ///
    /// - `node_lonlat`: `(lon, lat)` per node, in degrees.
    /// - `ways`: each way as a list of indices into `node_lonlat`.
    /// - `relation_bboxes`: each relation as `(min_lon, min_lat, max_lon,
    ///   max_lat)`.
    fn build_archive(
        node_lonlat: &[(f64, f64)],
        ways: &[Vec<usize>],
        relation_bboxes: &[(f64, f64, f64, f64)],
    ) -> Osm {
        let storage = MemoryResourceStorage::new("/test");
        let builder = OsmBuilder::new(storage.clone()).unwrap();

        let mut header = Header::new();
        header.set_coord_scale(COORD_SCALE);
        builder.set_header(&header).unwrap();

        // Nodes, ordered by the z-order curve.
        let curve = node_curve();
        let mut order: Vec<usize> = (0..node_lonlat.len()).collect();
        order.sort_by_key(|&i| node_index(&curve, node_lonlat[i].0, node_lonlat[i].1));
        let mut final_idx = vec![0u64; node_lonlat.len()];
        for (pos, &orig) in order.iter().enumerate() {
            final_idx[orig] = pos as u64;
        }
        let mut node_vec: Vec<Node> = order
            .iter()
            .map(|&orig| {
                let (lon, lat) = node_lonlat[orig];
                let mut n = unsafe { Node::new_unchecked() };
                n.set_lon(scale(lon));
                n.set_lat(scale(lat));
                n.set_tag_first_idx(0);
                n
            })
            .collect();
        node_vec.push(unsafe { Node::new_unchecked() }); // sentinel
        builder.set_nodes(&node_vec).unwrap();

        // Ways, ordered by the bounding-box curve.
        let wcurve = way_curve();
        let way_bbox = |w: &[usize]| -> (f64, f64, f64, f64) {
            let mut bb: Option<(f64, f64, f64, f64)> = None;
            for &ni in w {
                let (lon, lat) = node_lonlat[ni];
                bb = Some(match bb {
                    Some((a, b, c, d)) => (a.min(lon), b.min(lat), c.max(lon), d.max(lat)),
                    None => (lon, lat, lon, lat),
                });
            }
            bb.unwrap()
        };
        let mut worder: Vec<usize> = (0..ways.len()).collect();
        worder.sort_by_key(|&i| {
            let (a, b, c, d) = way_bbox(&ways[i]);
            bbox_index(&wcurve, a, b, c, d)
        });
        let mut way_vec: Vec<Way> = Vec::new();
        let mut nodes_index_vec: Vec<NodeIndex> = Vec::new();
        for &wi in &worder {
            let mut w = unsafe { Way::new_unchecked() };
            w.set_tag_first_idx(0);
            w.set_ref_first_idx(nodes_index_vec.len() as u64);
            for &ni in &ways[wi] {
                let mut idx = NodeIndex::new();
                idx.set_value(Some(final_idx[ni]));
                nodes_index_vec.push(idx);
            }
            way_vec.push(w);
        }
        let mut sentinel_way = unsafe { Way::new_unchecked() }; // sentinel terminates the ref range
        sentinel_way.set_tag_first_idx(0);
        sentinel_way.set_ref_first_idx(nodes_index_vec.len() as u64);
        way_vec.push(sentinel_way);
        builder.set_ways(&way_vec).unwrap();
        builder.set_nodes_index(&nodes_index_vec).unwrap();

        // Relations, ordered by the bounding-box curve.
        let mut rorder: Vec<usize> = (0..relation_bboxes.len()).collect();
        rorder.sort_by_key(|&i| {
            let (a, b, c, d) = relation_bboxes[i];
            bbox_index(&wcurve, a, b, c, d)
        });
        let mut rel_vec: Vec<Relation> = rorder
            .iter()
            .map(|&ri| {
                let (a, b, c, d) = relation_bboxes[ri];
                let mut r = unsafe { Relation::new_unchecked() };
                r.set_tag_first_idx(0);
                r.set_min_lon(scale(a));
                r.set_min_lat(scale(b));
                r.set_max_lon(scale(c));
                r.set_max_lat(scale(d));
                r
            })
            .collect();
        rel_vec.push(unsafe { Relation::new_unchecked() }); // sentinel
        builder.set_relations(&rel_vec).unwrap();

        // Remaining resources are required to open the archive but unused here.
        builder.start_relation_members().unwrap().close().unwrap();
        builder.set_tags(&[]).unwrap();
        builder.set_tags_index(&[]).unwrap();
        builder.set_stringtable(b"\0").unwrap();

        Osm::open(storage).unwrap()
    }

    #[test]
    fn finds_nodes_in_box_and_excludes_outside() {
        let nodes = vec![
            (-93.0, 45.0),  // 0 inside
            (-93.1, 45.1),  // 1 inside
            (-100.0, 40.0), // 2 outside (west + south)
            (-80.0, 30.0),  // 3 outside (east + south)
            (-92.5, 44.5),  // 4 inside
        ];
        let archive = build_archive(&nodes, &[], &[]);

        let mut found: Vec<(f64, f64)> =
            find_nodes_by_bounding_box(&archive, -93.5, 44.4, -92.4, 45.5)
                .map(|n| (unscale(n.lon()), unscale(n.lat())))
                .collect();
        found.sort_by(|a, b| a.partial_cmp(b).unwrap());

        assert_eq!(found.len(), 3, "expected 3 nodes inside, got {found:?}");
        let inside = |lon: f64, lat: f64| {
            found
                .iter()
                .any(|&(x, y)| (x - lon).abs() < 1e-6 && (y - lat).abs() < 1e-6)
        };
        assert!(inside(-93.0, 45.0));
        assert!(inside(-93.1, 45.1));
        assert!(inside(-92.5, 44.5));
    }

    #[test]
    fn finds_ways_overlapping_box() {
        let nodes = vec![(-93.0, 45.0), (-93.1, 45.1), (-100.0, 30.0), (-80.0, 40.0)];
        // way 0 is inside the query box; way 1 is far south (no lat overlap).
        let ways = vec![vec![0, 1], vec![2, 3]];
        let archive = build_archive(&nodes, &ways, &[]);

        let found: Vec<usize> = find_ways_by_bounding_box(&archive, -93.5, 44.4, -92.4, 45.5)
            .map(|w| w.refs().count())
            .collect();

        assert_eq!(found.len(), 1, "expected exactly one overlapping way");
        assert_eq!(found[0], 2, "the matching way has two node refs");
    }

    #[test]
    fn finds_relations_overlapping_box() {
        let relations = vec![
            (-93.2, 44.9, -92.9, 45.2),  // overlaps
            (-100.0, 30.0, -80.0, 40.0), // far south, no overlap
        ];
        let archive = build_archive(&[], &[], &relations);

        let found: Vec<(f64, f64, f64, f64)> =
            find_relations_by_bounding_box(&archive, -93.5, 44.4, -92.4, 45.5)
                .map(|r| {
                    (
                        unscale(r.min_lon()),
                        unscale(r.min_lat()),
                        unscale(r.max_lon()),
                        unscale(r.max_lat()),
                    )
                })
                .collect();

        assert_eq!(found.len(), 1, "expected one overlapping relation");
        assert!((found[0].0 - -93.2).abs() < 1e-6);
        assert!((found[0].3 - 45.2).abs() < 1e-6);
    }

    #[test]
    fn empty_box_far_away_finds_nothing() {
        let nodes = vec![(-93.0, 45.0), (-93.1, 45.1)];
        let archive = build_archive(&nodes, &[], &[]);
        assert_eq!(
            find_nodes_by_bounding_box(&archive, 10.0, 10.0, 11.0, 11.0).count(),
            0
        );
    }
}
