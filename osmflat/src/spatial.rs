use space_time::xzorder::xz2_sfc::XZ2SFC;

use crate::{Node, Osm, Way};

const RESOLUTION: u32 = 12;

/// Return [Node]s from the archive that are inside the
/// bounding box.
pub fn find_nodes_by_bounding_box(
    archive: &Osm,
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
) -> impl Iterator<Item = &Node> {
    let curve = XZ2SFC::wgs84(RESOLUTION);

    curve
        .ranges(xmin, ymin, xmax, ymax, None)
        .into_iter()
        .flat_map(move |b| {
            let lower_index = archive
                .nodes()
                .partition_point(|n| spatial_index_node(&curve, n) < b.lower());
            let upper_index = archive.nodes()[lower_index..]
                .partition_point(|n| spatial_index_node(&curve, n) <= b.upper());

            archive.nodes()[lower_index..upper_index].iter()
        })
}

fn spatial_index_node(curve: &XZ2SFC, node: &Node) -> u64 {
    todo!()
}

fn spatial_index_way(curve: &XZ2SFC, way: &Way) -> u64 {
    todo!()
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
    let curve = XZ2SFC::wgs84(RESOLUTION);

    curve
        .ranges(xmin, ymin, xmax, ymax, None)
        .into_iter()
        .flat_map(move |b| {
            let lower_index = archive
                .ways()
                .partition_point(|w| spatial_index_way(&curve, w) < b.lower());
            let upper_index = archive.ways()[lower_index..]
                .partition_point(|w| spatial_index_way(&curve, w) <= b.upper());

            archive.ways()[lower_index..upper_index].iter()
        })
}
