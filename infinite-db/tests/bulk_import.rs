//! Bulk hyperedge import and WAL batching.

use infinite_db::infinitedb_core::address::{DimensionVector, RevisionId, SpaceId};
use infinite_db::infinitedb_core::hyperedge::{
    EndpointRef, EndpointRole, Hyperedge, HyperedgeId, HyperedgeKind,
};
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
fn bulk_import_matches_single_insert_queries() {
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
fn bulk_import_deferred_endpoint_index() {
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
        let mut import = db.begin_hyperedge_import_with_options(edge_space, opts);
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
