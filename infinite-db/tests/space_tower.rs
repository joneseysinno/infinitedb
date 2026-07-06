//! SPACE_TOWER integration tests (Waves 1–3).

use infinite_db::infinitedb_core::{
    address::{DimensionVector, SpaceId},
    placement::Placement,
    space::SpaceConfig,
};
use infinite_db::infinitedb_core::hilbert_key::HilbertKey;
use infinite_db::infinitedb_server::api::{handle_request, Request, Response};
use infinite_db::infinitedb_server::session::{AccessLevel, Session, SpaceGrant};
use infinite_db::InfiniteDb;
use tempfile::TempDir;

fn admin_session(spaces: &[SpaceId]) -> Session {
    Session::new(
        infinite_db::infinitedb_core::branch::BranchId::MAIN,
        infinite_db::infinitedb_core::snapshot::SnapshotId(1),
        infinite_db::infinitedb_core::address::RevisionId::legacy(0),
        spaces
            .iter()
            .map(|&s| SpaceGrant {
                space: s,
                level: AccessLevel::Admin,
            })
            .collect(),
    )
}

#[test]
fn wave1_registry_tree_and_catalog() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(SpaceConfig::new(SpaceId(1), "site", 3))
        .unwrap();
    let building_p = Placement::axis_aligned(vec![0, 0, 0], 1, 1, vec![128, 128, 128]);
    db.register_space(
        SpaceConfig::new(SpaceId(2), "building", 3)
            .with_parent(SpaceId(1))
            .with_placement(building_p),
    )
    .unwrap();
    let detail_p = Placement {
        offset: vec![10, 10],
        scale_num: vec![1, 1],
        scale_den: vec![1, 1],
        extent: vec![64, 64],
        fixed_axes: vec![(2, 0)],
    };
    db.register_space(
        SpaceConfig::new(SpaceId(3), "detail", 2)
            .with_parent(SpaceId(2))
            .with_placement(detail_p),
    )
    .unwrap();

    assert_eq!(db.list_children(SpaceId(1)), vec![SpaceId(2)]);
    let subtree = db.get_subtree(SpaceId(1));
    assert_eq!(subtree.len(), 3);
    assert_eq!(subtree[0].0, SpaceId(1));
    assert_eq!(subtree[1].0, SpaceId(2));

    let session = admin_session(&[SpaceId(1), SpaceId(2), SpaceId(3)]);
    match handle_request(
        &db,
        &session,
        Request::ListChildren { parent: SpaceId(1) },
    ) {
        Response::SpaceConfigs(configs) => assert_eq!(configs.len(), 1),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn wave2_precision_ceiling_rejects_33_bits() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let err = db.register_space(
        SpaceConfig::new(SpaceId(1), "big", 2).with_bits_per_dim(33),
    );
    assert!(err.is_err());
}

#[test]
fn wave2_coordinate_overflow_rejected_before_write() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(SpaceConfig::new(SpaceId(1), "s", 1).with_bits_per_dim(4))
        .unwrap();
    let err = db.insert(SpaceId(1), DimensionVector::new(vec![20]), vec![1]);
    assert!(err.is_err());
}

#[test]
fn wave3_register_or_get_racing_id() {
    let dir = TempDir::new().unwrap();
    let db = std::sync::Arc::new(InfiniteDb::open(dir.path()).unwrap());
    let cfg = SpaceConfig::new(SpaceId(100), "race", 2);
    let mut handles = vec![];
    for _ in 0..4 {
        let db = std::sync::Arc::clone(&db);
        let cfg = cfg.clone();
        handles.push(std::thread::spawn(move || db.register_or_get_space(cfg).unwrap()));
    }
    let ids: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert!(ids.iter().all(|&id| id == ids[0]));
}

#[test]
fn wave3_density_tracks_writes() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(SpaceConfig::new(SpaceId(1), "s", 2)).unwrap();
    db.insert(SpaceId(1), DimensionVector::new(vec![0, 0]), vec![1])
        .unwrap();
    db.insert(SpaceId(1), DimensionVector::new(vec![128, 128]), vec![2])
        .unwrap();
    let d = db.space_density(SpaceId(1));
    assert_eq!(d.record_count, 2);
    assert!(d.max_occupied_depth > 0);
}

/// T10 descent conformance fixture — catalog-only BVH descent contract.
#[test]
fn wave3_descent_conformance_fixture() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    // site (3D) → building (3D) → detail (2D in 3D)
    db.register_space(SpaceConfig::new(SpaceId(1), "site", 3))
        .unwrap();
    db.register_space(
        SpaceConfig::new(SpaceId(2), "west", 3)
            .with_parent(SpaceId(1))
            .with_placement(Placement::axis_aligned(
                vec![0, 0, 0],
                1,
                1,
                vec![128, 128, 128],
            )),
    )
    .unwrap();
    db.register_space(
        SpaceConfig::new(SpaceId(3), "east", 3)
            .with_parent(SpaceId(1))
            .with_placement(Placement::axis_aligned(
                vec![128, 0, 0],
                1,
                1,
                vec![127, 128, 128],
            )),
    )
    .unwrap();
    db.register_space(
        SpaceConfig::new(SpaceId(4), "sheet", 2)
            .with_parent(SpaceId(2))
            .with_placement(Placement {
                offset: vec![10, 10],
                scale_num: vec![1, 1],
                scale_den: vec![1, 1],
                extent: vec![64, 64],
                fixed_axes: vec![(2, 0)],
            }),
    )
    .unwrap();

    db.insert(SpaceId(4), DimensionVector::new(vec![5, 5]), b"a".to_vec())
        .unwrap();
    db.insert(SpaceId(3), DimensionVector::new(vec![64, 64, 64]), b"b".to_vec())
        .unwrap();
    db.sync().unwrap();

    // Descent: bbox in site frame touching west wing only → detail child, not east.
    let west_children = db.list_children(SpaceId(2));
    assert_eq!(west_children, vec![SpaceId(4)]);

    let subtree = db.get_subtree(SpaceId(1));
    assert_eq!(subtree.len(), 4);

    // Boundary-jump sanity: spatially adjacent points may be curve-distant — both queryable.
    let west = db.query(SpaceId(4), None).unwrap();
    let east = db.query(SpaceId(3), None).unwrap();
    assert_eq!(west.len(), 1);
    assert_eq!(east.len(), 1);
    let k_w = west[0].hilbert_key.get().unwrap_or(HilbertKey(0));
    let k_e = east[0].hilbert_key.get().unwrap_or(HilbertKey(0));
    assert_ne!(k_w, k_e);
}

/// T12 — cross-space flow vector composed at site ancestor.
#[test]
fn wave4_cross_space_flow_vector() {
    use infinite_db::infinitedb_core::hyperedge::{
        Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
        HyperedgeKind,
    };
    use std::collections::BTreeMap;

    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let edge_space = SpaceId(100);
    db.register_space(SpaceConfig::new(edge_space, "edges", 2))
        .unwrap();
    db.register_space(SpaceConfig::new(SpaceId(1), "site", 2))
        .unwrap();
    db.register_space(
        SpaceConfig::new(SpaceId(2), "west", 2)
            .with_parent(SpaceId(1))
            .with_placement(Placement::axis_aligned(vec![0, 0], 1, 1, vec![128, 128])),
    )
    .unwrap();
    db.register_space(
        SpaceConfig::new(SpaceId(3), "east", 2)
            .with_parent(SpaceId(1))
            .with_placement(Placement::axis_aligned(vec![64, 0], 1, 1, vec![64, 128])),
    )
    .unwrap();

    fn ep(space: SpaceId, x: u32, y: u32, pol: EndpointPolarity) -> EndpointRef {
        EndpointRef::new(
            EndpointRole::new("n"),
            space,
            DimensionVector::new(vec![x, y]),
        )
        .with_polarity(pol)
    }

    let edge = Hyperedge {
        id: HyperedgeId(1),
        kind: HyperedgeKind::new("flow"),
        endpoints: vec![
            ep(SpaceId(2), 10, 5, EndpointPolarity::Tail),
            ep(SpaceId(3), 5, 5, EndpointPolarity::Head),
        ],
        weight_milli: None,
        metadata: BTreeMap::new(),
        valid_from: infinite_db::infinitedb_core::address::RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Directed,
        authoring_frame: None,
        computation: None,
    };
    db.insert_hyperedge(edge_space, edge).unwrap();
    db.sync().unwrap();

    let rec = db
        .query_flow_vector_for_edge(edge_space, HyperedgeId(1), None)
        .unwrap()
        .expect("cross-space vector indexed");
    assert_eq!(rec.vector.space, SpaceId(1));
    assert_eq!(rec.vector.delta, vec![59, 0]);
}

/// Same-space flow vectors unchanged (T12 regression).
#[test]
fn wave4_same_space_flow_vector_regression() {
    use infinite_db::infinitedb_core::hyperedge::{
        Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
        HyperedgeKind,
    };
    use std::collections::BTreeMap;

    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let edge_space = SpaceId(10);
    let entity = SpaceId(20);
    db.register_space(SpaceConfig::new(edge_space, "edges", 2))
        .unwrap();
    db.register_space(SpaceConfig::new(entity, "entity", 2))
        .unwrap();

    let edge = Hyperedge {
        id: HyperedgeId(7),
        kind: HyperedgeKind::new("flow"),
        endpoints: vec![
            EndpointRef::new(
                EndpointRole::new("n"),
                entity,
                DimensionVector::new(vec![0, 0]),
            )
            .with_polarity(EndpointPolarity::Tail),
            EndpointRef::new(
                EndpointRole::new("n"),
                entity,
                DimensionVector::new(vec![0, 8]),
            )
            .with_polarity(EndpointPolarity::Head),
        ],
        weight_milli: None,
        metadata: BTreeMap::new(),
        valid_from: infinite_db::infinitedb_core::address::RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Directed,
        authoring_frame: None,
        computation: None,
    };
    db.insert_hyperedge(edge_space, edge).unwrap();
    db.sync().unwrap();
    let rec = db
        .query_flow_vector_for_edge(edge_space, HyperedgeId(7), None)
        .unwrap()
        .unwrap();
    assert_eq!(rec.vector.space, entity);
    assert_eq!(rec.vector.delta, vec![0, 8]);
}

/// Disjoint forests produce no flow vector (T12).
#[test]
fn wave4_no_common_ancestor_no_vector() {
    use infinite_db::infinitedb_core::hyperedge::{
        Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
        HyperedgeKind,
    };
    use std::collections::BTreeMap;

    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let edge_space = SpaceId(10);
    db.register_space(SpaceConfig::new(edge_space, "edges", 2))
        .unwrap();
    db.register_space(SpaceConfig::new(SpaceId(1), "a", 2))
        .unwrap();
    db.register_space(SpaceConfig::new(SpaceId(2), "b", 2))
        .unwrap();

    let edge = Hyperedge {
        id: HyperedgeId(99),
        kind: HyperedgeKind::new("flow"),
        endpoints: vec![
            EndpointRef::new(
                EndpointRole::new("n"),
                SpaceId(1),
                DimensionVector::new(vec![0, 0]),
            )
            .with_polarity(EndpointPolarity::Tail),
            EndpointRef::new(
                EndpointRole::new("n"),
                SpaceId(2),
                DimensionVector::new(vec![1, 0]),
            )
            .with_polarity(EndpointPolarity::Head),
        ],
        weight_milli: None,
        metadata: BTreeMap::new(),
        valid_from: infinite_db::infinitedb_core::address::RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Directed,
        authoring_frame: None,
        computation: None,
    };
    db.insert_hyperedge(edge_space, edge).unwrap();
    db.sync().unwrap();
    assert!(
        db.query_flow_vector_for_edge(edge_space, HyperedgeId(99), None)
            .unwrap()
            .is_none()
    );
}
