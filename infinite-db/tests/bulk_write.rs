//! Bulk write sessions for records, hyperedges, and signals.

use infinite_db::infinitedb_core::address::{DimensionVector, RevisionId, SpaceId};
use infinite_db::infinitedb_core::hyperedge::{
    EndpointRef, EndpointRole, Hyperedge, HyperedgeId, HyperedgeKind,
};
use infinite_db::infinitedb_core::signal::{SignalId, SignalKind, SignalSample, SignalScope};
use infinite_db::infinitedb_core::space::SpaceConfig;
use infinite_db::{BulkHyperedgeImportOptions, InfiniteDb};
use tempfile::TempDir;

fn endpoint(space: SpaceId, coords: &[u32]) -> EndpointRef {
    EndpointRef {
        role: EndpointRole::new("n"),
        space,
        node: DimensionVector::new(coords.to_vec()),
    }
}

#[test]
fn bulk_hyperedge_import_matches_single_insert() {
    let dir = TempDir::new().unwrap();
    let edge_space = SpaceId(10);
    let n_space = SpaceId(11);

    let edges: Vec<Hyperedge> = (1u64..=50)
        .map(|i| Hyperedge {
            id: HyperedgeId(i),
            kind: HyperedgeKind::new("step"),
            endpoints: vec![
                endpoint(n_space, &[i as u32, 0]),
                endpoint(n_space, &[i as u32, 1]),
            ],
            weight_milli: None,
            metadata: Default::default(),
            valid_from: RevisionId::ZERO,
            valid_to: None,
        })
        .collect();

    {
        let mut db = InfiniteDb::open(dir.path()).unwrap();
        db.register_space(SpaceConfig::new(edge_space, "edges", 2)).unwrap();
        db.register_space(SpaceConfig::new(n_space, "nodes", 2)).unwrap();
        db.insert_hyperedges_bulk(edge_space, edges.clone()).unwrap();
    }

    let mut db = InfiniteDb::open(dir.path()).unwrap();
    let ep = edges[0].endpoints[0].clone();
    let found = db
        .query_hyperedges_for_endpoint(edge_space, &ep, None)
        .unwrap();
    assert!(found.iter().any(|e| e.id == HyperedgeId(1)));
    assert_eq!(db.query_hyperedges(edge_space, None).unwrap().len(), 50);
}

#[test]
fn bulk_hyperedge_deferred_endpoint_index() {
    let dir = TempDir::new().unwrap();
    let edge_space = SpaceId(20);
    let n_space = SpaceId(21);

    let edge = Hyperedge {
        id: HyperedgeId(1),
        kind: HyperedgeKind::new("step"),
        endpoints: vec![
            endpoint(n_space, &[1, 0]),
            endpoint(n_space, &[1, 1]),
        ],
        weight_milli: None,
        metadata: Default::default(),
        valid_from: RevisionId::ZERO,
        valid_to: None,
    };

    {
        let mut db = InfiniteDb::open(dir.path()).unwrap();
        db.register_space(SpaceConfig::new(edge_space, "edges", 2)).unwrap();
        db.register_space(SpaceConfig::new(n_space, "nodes", 2)).unwrap();
        let opts = BulkHyperedgeImportOptions {
            build_endpoint_index: false,
            ..Default::default()
        };
        let mut import = db
            .begin_hyperedge_import_with_options(edge_space, opts)
            .unwrap();
        import.push(edge.clone()).unwrap();
        import.build_endpoint_index().unwrap();
        import.finish().unwrap();
    }

    let mut db = InfiniteDb::open(dir.path()).unwrap();
    assert_eq!(
        db.query_hyperedges_for_endpoint(edge_space, &edge.endpoints[0], None)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn bulk_records_roundtrip() {
    let dir = TempDir::new().unwrap();
    let space = SpaceId(30);

    {
        let mut db = InfiniteDb::open(dir.path()).unwrap();
        db.register_space(SpaceConfig::new(space, "data", 2)).unwrap();
        let rows: Vec<_> = (0..100u32)
            .map(|i| (DimensionVector::new(vec![i, 0]), vec![i as u8]))
            .collect();
        let result = db.insert_records_bulk(space, rows).unwrap();
        assert_eq!(result.count, 100);
        assert_eq!(result.wal_frames, 100);
    }

    let mut db = InfiniteDb::open(dir.path()).unwrap();
    assert_eq!(db.query(space, None).unwrap().len(), 100);
}

#[test]
fn bulk_record_delete() {
    let dir = TempDir::new().unwrap();
    let space = SpaceId(31);
    let point = DimensionVector::new(vec![7, 8]);

    {
        let mut db = InfiniteDb::open(dir.path()).unwrap();
        db.register_space(SpaceConfig::new(space, "data", 2)).unwrap();
        let mut import = db.begin_record_import(space).unwrap();
        import.push(point.clone(), vec![1, 2, 3]).unwrap();
        import.push_delete(point).unwrap();
        import.finish().unwrap();
    }

    let mut db = InfiniteDb::open(dir.path()).unwrap();
    assert!(db.query(space, None).unwrap().is_empty());
}

#[test]
fn bulk_signals_roundtrip() {
    let dir = TempDir::new().unwrap();
    let space = SpaceId(40);

    let sample = SignalSample {
        signal_id: SignalId(1),
        kind: SignalKind::new("temp"),
        scope: SignalScope {
            parent_prefix: DimensionVector::new(vec![1]),
            total_dims: 2,
        },
        local_coords: vec![5],
        value_milli: 2500,
        source_revision: None,
        constraint: None,
    };

    {
        let mut db = InfiniteDb::open(dir.path()).unwrap();
        db.register_space(SpaceConfig::new(space, "signals", 2)).unwrap();
        db.insert_signals_bulk(space, vec![sample.clone()]).unwrap();
    }

    let mut db = InfiniteDb::open(dir.path()).unwrap();
    let rows = db.query(space, None).unwrap();
    assert_eq!(rows.len(), 1);
}

