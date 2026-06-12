//! Wave E — typed API surface, ApiError::Busy, error retention, arbiter id reservation.

use infinite_db::{
    DerivationBackpressurePolicy, EngineError, InfiniteDb, OpenOptions,
    RESERVED_ARBITER_ID_THRESHOLD,
};
use infinite_db::infinitedb_core::{
    address::{DimensionVector, RevisionId, SpaceId},
    hyperedge::{
        Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
        HyperedgeKind,
    },
    judgment::ArbiterId,
    space::{ErrorRetentionPolicy, SpaceConfig},
};
use tempfile::TempDir;

fn directed_edge(id: u64, tail: EndpointRef, head: EndpointRef) -> Hyperedge {
    Hyperedge {
        id: HyperedgeId(id),
        kind: HyperedgeKind::new("flow"),
        endpoints: vec![
            tail.with_polarity(EndpointPolarity::Tail),
            head.with_polarity(EndpointPolarity::Head),
        ],
        weight_milli: None,
        metadata: Default::default(),
        valid_from: RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Directed,
        authoring_frame: None,
        computation: None,
    }
}

fn node(space: SpaceId, x: u32) -> EndpointRef {
    EndpointRef::new(
        EndpointRole::new("n"),
        space,
        DimensionVector::new(vec![x, 0]),
    )
}

fn open_edges() -> (InfiniteDb, TempDir, SpaceId) {
    let dir = TempDir::new().unwrap();
    let edge_space = SpaceId(10);
    let db = OpenOptions::default().open(dir.path()).unwrap();
    db.register_space(SpaceConfig::new(edge_space, "edges", 2)).unwrap();
    (db, dir, edge_space)
}

#[test]
fn reserved_arbiter_id_rejected() {
    let (db, _dir, _edge_space) = open_edges();
    let err = db
        .register_arbiter_stream(ArbiterId(3), "low", 2)
        .unwrap_err();
    assert!(matches!(err, EngineError::ReservedArbiterId(3)));
    assert_eq!(RESERVED_ARBITER_ID_THRESHOLD, 10);
}

#[test]
fn try_insert_hyperedge_surfaces_derivation_backpressure() {
    let dir = TempDir::new().unwrap();
    let edge_space = SpaceId(10);
    let db = OpenOptions {
        derivation: DerivationBackpressurePolicy {
            max_pending_tasks: 10_000,
            max_derivation_lag: 1,
            max_worker_threads: 1,
        },
        ..OpenOptions::default()
    }
    .open(dir.path())
    .unwrap();
    db.register_space(SpaceConfig::new(edge_space, "edges", 2))
        .unwrap();
    let entity = SpaceId(1);
    let s1 = db.open_session();
    db.insert_hyperedge_with_session(
        &s1,
        edge_space,
        directed_edge(1, node(entity, 1), node(entity, 2)),
    )
    .unwrap();
    db.insert_hyperedge_with_session(
        &s1,
        edge_space,
        directed_edge(2, node(entity, 3), node(entity, 4)),
    )
    .unwrap();
    let err = db
        .insert_hyperedge_with_session(
            &s1,
            edge_space,
            directed_edge(3, node(entity, 5), node(entity, 6)),
        )
        .unwrap_err();
    assert!(matches!(err, EngineError::DerivationBackpressure { .. }));
    assert!(err.retry_hint_ms().is_some());
}

#[test]
fn purge_resolved_errors_respects_retention_policy() {
    let dir = TempDir::new().unwrap();
    let edge_space = SpaceId(10);
    let db = OpenOptions::default().open(dir.path()).unwrap();
    db.register_space(
        SpaceConfig::new(edge_space, "edges", 2)
            .with_error_retention(ErrorRetentionPolicy::keep_latest_resolved(1)),
    )
    .unwrap();
    use infinite_db::infinitedb_core::error_record::{
        ErrorKind, OperationErrorRecord, OperationRevisionRange,
    };

    let rev_a = db
        .insert(edge_space, DimensionVector::new(vec![90, 0]), vec![1])
        .unwrap();
    let rev_b = db
        .insert(edge_space, DimensionVector::new(vec![91, 0]), vec![2])
        .unwrap();
    db.sync().unwrap();
    for rev in [rev_a, rev_b] {
        db.persist_operation_errors(
            edge_space,
            OperationErrorRecord {
                kind: ErrorKind::ImportValidation,
                revision_range: OperationRevisionRange::new(rev, rev),
                source_space: edge_space,
                entries: vec![],
            },
        )
        .unwrap();
        db.sync().unwrap();
    }
    assert_eq!(
        db.query_operation_errors(edge_space, None, None)
            .unwrap()
            .len(),
        2
    );
    db.resolve_operation_error(edge_space, rev_a).unwrap();
    db.resolve_operation_error(edge_space, rev_b).unwrap();
    db.sync().unwrap();
    let purged = db.purge_resolved_errors(edge_space).unwrap();
    assert_eq!(purged, 1);
    assert_eq!(db.purge_resolved_errors(edge_space).unwrap(), 0);
    db.sync().unwrap();
}
