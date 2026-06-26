//! Benchmarks for the bounding-box spatial queries.
//!
//! The focus is `find_ways_by_bounding_box`, which recomputes each candidate
//! way's bounding box from its node references *inside* the binary-search
//! predicate (see `osmflat::find_ways_by_bounding_box`). This benchmark builds
//! a synthetic archive once and measures query latency across query-box sizes,
//! since wider boxes produce more space-filling-curve ranges and therefore more
//! bbox recomputations.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use osmflat::{find_ways_by_bounding_box, test_support::generate_way_archive, Osm};

// A region roughly the size of a US state, so the curve keys spread out.
const LON_MIN: f64 = -97.5;
const LAT_MIN: f64 = 43.0;
const LON_MAX: f64 = -89.5;
const LAT_MAX: f64 = 49.5;

fn build(num_ways: usize, refs_per_way: usize) -> Osm {
    generate_way_archive(
        num_ways,
        refs_per_way,
        LON_MIN,
        LAT_MIN,
        LON_MAX,
        LAT_MAX,
        0xC0FFEE,
    )
}

/// Query boxes centered in the region, as a fraction of the full region span.
/// Larger fractions select more ways and emit more curve ranges.
fn query_box(fraction: f64) -> (f64, f64, f64, f64) {
    let cx = (LON_MIN + LON_MAX) / 2.0;
    let cy = (LAT_MIN + LAT_MAX) / 2.0;
    let hw = (LON_MAX - LON_MIN) / 2.0 * fraction;
    let hh = (LAT_MAX - LAT_MIN) / 2.0 * fraction;
    (cx - hw, cy - hh, cx + hw, cy + hh)
}

fn bench_find_ways(c: &mut Criterion) {
    let num_ways = 100_000;
    let refs_per_way = 10;
    let archive = build(num_ways, refs_per_way);

    let mut group = c.benchmark_group("find_ways_by_bounding_box");
    group.throughput(Throughput::Elements(num_ways as u64));

    for fraction in [0.001, 0.01, 0.1, 0.5] {
        let (xmin, ymin, xmax, ymax) = query_box(fraction);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("box_frac={fraction}")),
            &(xmin, ymin, xmax, ymax),
            |b, &(xmin, ymin, xmax, ymax)| {
                b.iter(|| find_ways_by_bounding_box(&archive, xmin, ymin, xmax, ymax).count())
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_find_ways);
criterion_main!(benches);
