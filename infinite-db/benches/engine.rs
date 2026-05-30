//! Criterion benchmarks for InfiniteDB core paths.
//!
//! Run with `cargo bench`. Covers:
//!   - insert throughput (buffered writes)
//!   - `query_bbox` selectivity vs full scan
//!   - hyperedge traversal depth scaling
//!
//! Each benchmark builds its dataset in a fresh temp directory so runs are
//! isolated and reproducible.

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use infinite_db::infinitedb_core::address::{DimensionVector, SpaceId};
use infinite_db::infinitedb_core::hyperedge::{
    EndpointRef, EndpointRole, Hyperedge, HyperedgeId, HyperedgeKind,
};
use infinite_db::infinitedb_core::space::SpaceConfig;
use infinite_db::infinitedb_core::traversal::TraversalSpec;
use infinite_db::InfiniteDb;
use std::collections::BTreeMap;
use tempfile::TempDir;

fn fresh_db() -> (InfiniteDb, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    (db, dir)
}

/// Spread `n` points across a 2D grid so spatial queries are meaningful.
fn grid_point(i: u32, side: u32) -> DimensionVector {
    DimensionVector::new(vec![i % side, (i / side) % side])
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");
    for &n in &[1_000u32, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                fresh_db,
                |(mut db, _dir)| {
                    let space = SpaceId(1);
                    db.register_space(SpaceConfig::new(space, "bench", 2)).unwrap();
                    for i in 0..n {
                        db.insert(space, grid_point(i, 256), vec![(i & 0xFF) as u8]).unwrap();
                    }
                    db.flush(space).unwrap();
                    db
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// Build a database with `n` flushed records spread over a 256×256 grid.
fn build_populated(n: u32) -> (InfiniteDb, TempDir, SpaceId) {
    let (mut db, dir) = fresh_db();
    let space = SpaceId(1);
    db.register_space(SpaceConfig::new(space, "bench", 2)).unwrap();
    for i in 0..n {
        db.insert(space, grid_point(i, 256), vec![(i & 0xFF) as u8]).unwrap();
        if i % 256 == 255 {
            db.flush(space).unwrap();
        }
    }
    db.flush(space).unwrap();
    (db, dir, space)
}

fn bench_query(c: &mut Criterion) {
    let n = 10_000u32;
    let (mut db, _dir, space) = build_populated(n);

    let mut group = c.benchmark_group("query");
    group.bench_function("full_scan", |b| {
        b.iter(|| {
            let rows = db.query(space, None).unwrap();
            criterion::black_box(rows.len());
        });
    });
    // Small box: a 16×16 corner of the 256×256 grid (Hilbert pruning should win).
    group.bench_function("bbox_selective", |b| {
        b.iter(|| {
            let rows = db
                .query_bbox(
                    space,
                    DimensionVector::new(vec![0, 0]),
                    DimensionVector::new(vec![15, 15]),
                    None,
                )
                .unwrap();
            criterion::black_box(rows.len());
        });
    });
    group.finish();
}

/// Build a linear chain of `depth` hyperedges across distinct spaces.
fn build_chain(depth: u32) -> (InfiniteDb, TempDir, SpaceId, EndpointRef) {
    let (mut db, dir) = fresh_db();
    let edge_space = SpaceId(100);
    db.register_space(SpaceConfig::new(edge_space, "edges", 2)).unwrap();

    let node = |i: u32| EndpointRef {
        role: EndpointRole::new("n"),
        space: SpaceId(1000 + i as u64),
        node: DimensionVector::new(vec![i]),
    };
    for i in 0..depth {
        let edge = Hyperedge {
            id: HyperedgeId(i as u64 + 1),
            kind: HyperedgeKind::new("chain"),
            endpoints: vec![node(i), node(i + 1)],
            weight_milli: None,
            metadata: BTreeMap::new(),
            valid_from: infinite_db::infinitedb_core::address::RevisionId::ZERO,
            valid_to: None,
        };
        db.insert_hyperedge(edge_space, edge).unwrap();
    }
    db.flush(edge_space).unwrap();
    (db, dir, edge_space, node(0))
}

fn bench_hyperedge_bulk(c: &mut Criterion) {
    let mut group = c.benchmark_group("hyperedge_bulk");
    for &n in &[500u32, 2_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("insert_hyperedge", n), &n, |b, &n| {
            b.iter_batched(
                fresh_db,
                |(mut db, _dir)| {
                    let edge_space = SpaceId(50);
                    db.register_space(SpaceConfig::new(edge_space, "edges", 2)).unwrap();
                    for i in 0..n {
                        let edge = Hyperedge {
                            id: HyperedgeId(i as u64 + 1),
                            kind: HyperedgeKind::new("chain"),
                            endpoints: vec![
                                EndpointRef {
                                    role: EndpointRole::new("n"),
                                    space: SpaceId(1000 + i as u64),
                                    node: DimensionVector::new(vec![i, 0]),
                                },
                                EndpointRef {
                                    role: EndpointRole::new("n"),
                                    space: SpaceId(1001 + i as u64),
                                    node: DimensionVector::new(vec![i, 1]),
                                },
                            ],
                            weight_milli: None,
                            metadata: BTreeMap::new(),
                            valid_from: infinite_db::infinitedb_core::address::RevisionId::ZERO,
                            valid_to: None,
                        };
                        db.insert_hyperedge(edge_space, edge).unwrap();
                    }
                    db.flush(edge_space).unwrap();
                },
                BatchSize::SmallInput,
            );
        });
        group.bench_with_input(BenchmarkId::new("insert_hyperedges_bulk", n), &n, |b, &n| {
            b.iter_batched(
                fresh_db,
                |(mut db, _dir)| {
                    let edge_space = SpaceId(50);
                    db.register_space(SpaceConfig::new(edge_space, "edges", 2)).unwrap();
                    let edges: Vec<Hyperedge> = (0..n)
                        .map(|i| Hyperedge {
                            id: HyperedgeId(i as u64 + 1),
                            kind: HyperedgeKind::new("chain"),
                            endpoints: vec![
                                EndpointRef {
                                    role: EndpointRole::new("n"),
                                    space: SpaceId(1000 + i as u64),
                                    node: DimensionVector::new(vec![i, 0]),
                                },
                                EndpointRef {
                                    role: EndpointRole::new("n"),
                                    space: SpaceId(1001 + i as u64),
                                    node: DimensionVector::new(vec![i, 1]),
                                },
                            ],
                            weight_milli: None,
                            metadata: BTreeMap::new(),
                            valid_from: infinite_db::infinitedb_core::address::RevisionId::ZERO,
                            valid_to: None,
                        })
                        .collect();
                    db.insert_hyperedges_bulk(edge_space, edges).unwrap();
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_traversal(c: &mut Criterion) {
    let mut group = c.benchmark_group("traversal");
    for &depth in &[2u32, 4, 8] {
        let (mut db, _dir, edge_space, start) = build_chain(16);
        group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, &depth| {
            b.iter(|| {
                let sub = db
                    .traverse(
                        edge_space,
                        &TraversalSpec {
                            start: start.clone(),
                            max_depth: depth as usize,
                            follow_kinds: None,
                            as_of: None,
                        },
                    )
                    .unwrap();
                criterion::black_box(sub.nodes.len());
            });
        });
    }
    group.finish();
}

fn bench_record_bulk(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_bulk");
    for &n in &[1_000u32, 5_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("insert", n), &n, |b, &n| {
            b.iter_batched(
                fresh_db,
                |(mut db, _dir)| {
                    let space = SpaceId(60);
                    db.register_space(SpaceConfig::new(space, "bench", 2)).unwrap();
                    for i in 0..n {
                        db.insert(space, grid_point(i, 256), vec![(i & 0xFF) as u8])
                            .unwrap();
                    }
                    db.flush(space).unwrap();
                },
                BatchSize::SmallInput,
            );
        });
        group.bench_with_input(BenchmarkId::new("insert_records_bulk", n), &n, |b, &n| {
            b.iter_batched(
                fresh_db,
                |(mut db, _dir)| {
                    let space = SpaceId(60);
                    db.register_space(SpaceConfig::new(space, "bench", 2)).unwrap();
                    let rows: Vec<_> = (0..n)
                        .map(|i| (grid_point(i, 256), vec![(i & 0xFF) as u8]))
                        .collect();
                    db.insert_records_bulk(space, rows).unwrap();
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_insert, bench_query, bench_hyperedge_bulk, bench_record_bulk, bench_traversal);
criterion_main!(benches);
