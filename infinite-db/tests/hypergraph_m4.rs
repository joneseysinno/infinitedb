//! Milestone 4 — derivation bus, delta-merge reads, bulk import.

use std::collections::BTreeMap;

use infinite_db::{
    DerivationBackpressurePolicy, ImportBudget, InfiniteDb, OpenOptions, QueryOptions,
};
use infinite_db::infinitedb_core::{
    address::{DimensionVector, RevisionId, SpaceId},
    hlc::SessionId,
    hyperedge::{
        Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
        HyperedgeKind,
    },
    query::DirectionFilter,
    space::SpaceConfig,
};
use tempfile::TempDir;

fn open_db() -> (InfiniteDb, TempDir, SpaceId) {
    let dir = TempDir::new().unwrap();
    let db = OpenOptions::default().open(dir.path()).unwrap();
    let edge_space = SpaceId(10);
    db.register_space(SpaceConfig::new(edge_space, "edges", 2))
        .unwrap();
    (db, dir, edge_space)
}

fn node(space: SpaceId, x: u32) -> EndpointRef {
    EndpointRef::new(
        EndpointRole::new("n"),
        space,
        DimensionVector::new(vec![x, 0]),
    )
}

fn directed_edge(id: u64, tail: EndpointRef, head: EndpointRef) -> Hyperedge {
    Hyperedge {
        id: HyperedgeId(id),
        kind: HyperedgeKind::new("flow"),
        endpoints: vec![
            tail.with_polarity(EndpointPolarity::Tail),
            head.with_polarity(EndpointPolarity::Head),
        ],
        weight_milli: None,
        metadata: BTreeMap::new(),
        valid_from: RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Directed,
        authoring_frame: None,
        computation: None,
    }
}

#[test]
fn derivation_bus_processes_on_sync() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let hub = node(entity, 1);
    let edge = directed_edge(1, hub.clone(), node(entity, 2));
    db.insert_hyperedge(edge_space, edge).unwrap();
    db.sync().unwrap();
    assert!(db.derivation_stats().events_processed >= 1);
    let edges = db
        .query_hyperedges_for_endpoint_directed(edge_space, &hub, None, DirectionFilter::Any)
        .unwrap();
    assert_eq!(edges.len(), 1);
}

#[test]
fn index_only_matches_full_query_after_sync() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let hub = node(entity, 3);
    for id in 1u64..=3 {
        let e = directed_edge(id, hub.clone(), node(entity, id as u32 + 10));
        db.insert_hyperedge(edge_space, e).unwrap();
    }
    db.sync().unwrap();
    let full = db
        .query_hyperedges_for_endpoint_directed(edge_space, &hub, None, DirectionFilter::Any)
        .unwrap();
    let index_only = db
        .query_hyperedges_for_endpoint_directed_with_options(
            edge_space,
            &hub,
            None,
            DirectionFilter::Any,
            QueryOptions::index_only(),
        )
        .unwrap();
    assert_eq!(full.len(), 3);
    assert_eq!(index_only.len(), 3);
}

#[test]
fn delete_tombstone_ordering_on_bus() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let hub = node(entity, 7);
    let edge = directed_edge(99, hub.clone(), node(entity, 8));
    db.insert_hyperedge(edge_space, edge.clone()).unwrap();
    db.sync().unwrap();
    db.delete_hyperedge(edge_space, HyperedgeId(99)).unwrap();
    db.sync().unwrap();
    assert!(
        db.query_hyperedges_for_endpoint(edge_space, &hub, None)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn bulk_import_applicative_errors_and_commit() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let mut session = db
        .begin_hyperedge_import(edge_space, ImportBudget::default())
        .unwrap();
    let good = directed_edge(1, node(entity, 1), node(entity, 2));
    let bad = Hyperedge {
        id: HyperedgeId(2),
        kind: HyperedgeKind::new("bad"),
        endpoints: vec![node(entity, 3)],
        weight_milli: None,
        metadata: BTreeMap::new(),
        valid_from: RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Undirected,
        authoring_frame: None,
        computation: None,
    };
    session.push(good).unwrap();
    session.push(bad).unwrap();
    let result = db.commit_hyperedge_import(session).unwrap();
    assert!(!result.aborted);
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.admitted.first(), result.admitted.last());
    db.sync().unwrap();
    assert_eq!(db.query_hyperedges(edge_space, None).unwrap().len(), 1);
}

#[test]
fn bulk_import_over_budget_aborts() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let mut session = db
        .begin_hyperedge_import(
            edge_space,
            ImportBudget {
                max_errors: 0,
                sample_every: 1,
            },
        )
        .unwrap();
    let bad = Hyperedge {
        id: HyperedgeId(1),
        kind: HyperedgeKind::new("bad"),
        endpoints: vec![node(entity, 1)],
        weight_milli: None,
        metadata: BTreeMap::new(),
        valid_from: RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Undirected,
        authoring_frame: None,
        computation: None,
    };
    session.push(bad).unwrap();
    let result = db.commit_hyperedge_import(session).unwrap();
    assert!(result.aborted);
    assert!(db.query_hyperedges(edge_space, None).unwrap().is_empty());
}

#[test]
fn delete_not_visible_after_delta_merge_before_derivation() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let hub = node(entity, 99);
    let insert_rev = db
        .insert_hyperedge(edge_space, directed_edge(1, hub.clone(), node(entity, 100)))
        .unwrap();
    db.sync().unwrap();
    let delete_rev = db.delete_hyperedge(edge_space, HyperedgeId(1)).unwrap();
    let delete_session = SessionId(delete_rev.session());
    let wm_before = db.endpoint_index_watermark_vector();
    assert!(
        wm_before.get(delete_session).unwrap_or(RevisionId::ZERO) < delete_rev,
        "watermark must not advance past un-derived delete"
    );
    assert!(
        db.derivation_stats().outstanding_derivations > 0,
        "delete event must remain outstanding until derivation retires it"
    );
    let index_only = db
        .query_hyperedges_for_endpoint_directed_with_options(
            edge_space,
            &hub,
            None,
            DirectionFilter::Any,
            QueryOptions::index_only(),
        )
        .unwrap();
    assert_eq!(index_only.len(), 1, "stale index may still list the edge");
    let _ = insert_rev;
    db.sync().unwrap();
    assert_eq!(db.derivation_stats().outstanding_derivations, 0);
    assert_eq!(
        db.endpoint_index_watermark_vector()
            .get(delete_session)
            .unwrap(),
        delete_rev
    );
    assert!(
        db.query_hyperedges_for_endpoint_directed(edge_space, &hub, None, DirectionFilter::Any)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn delete_recovery_on_reopen_rederives_tombstone() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    let edge_space = SpaceId(10);
    let entity = SpaceId(1);
    let hub = node(entity, 88);
    {
        let db = OpenOptions::default().open(path).unwrap();
        db.register_space(SpaceConfig::new(edge_space, "edges", 2))
            .unwrap();
        db.insert_hyperedge(edge_space, directed_edge(2, hub.clone(), node(entity, 89)))
            .unwrap();
        db.sync().unwrap();
        db.delete_hyperedge(edge_space, HyperedgeId(2)).unwrap();
        db.sync().unwrap();
    }
    {
        let db = OpenOptions::default().open(path).unwrap();
        let edges = db
            .query_hyperedges_for_endpoint_directed(edge_space, &hub, None, DirectionFilter::Any)
            .unwrap();
        assert!(edges.is_empty());
    }
}

#[test]
fn crash_recovery_reopen_rederives_index() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    let edge_space = SpaceId(10);
    let entity = SpaceId(1);
    let hub = node(entity, 42);
    {
        let db = OpenOptions::default().open(path).unwrap();
        db.register_space(SpaceConfig::new(edge_space, "edges", 2))
            .unwrap();
        let edge = directed_edge(7, hub.clone(), node(entity, 43));
        db.insert_hyperedge(edge_space, edge).unwrap();
        db.sync().unwrap();
    }
    {
        let db = OpenOptions::default().open(path).unwrap();
        let edges = db
            .query_hyperedges_for_endpoint_directed(edge_space, &hub, None, DirectionFilter::Any)
            .unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].id, HyperedgeId(7));
    }
}

#[test]
fn backpressure_rejects_writes_when_pending_flooded() {
    let dir = TempDir::new().unwrap();
    let db = OpenOptions {
        derivation: DerivationBackpressurePolicy {
            max_pending_tasks: 1,
            max_derivation_lag: u64::MAX,
            max_worker_threads: 1,
        },
        ..OpenOptions::default()
    }
    .open(dir.path())
    .unwrap();
    let edge_space = SpaceId(10);
    db.register_space(SpaceConfig::new(edge_space, "edges", 2))
        .unwrap();
    let entity = SpaceId(1);
    let e1 = directed_edge(1, node(entity, 1), node(entity, 2));
    let e2 = directed_edge(2, node(entity, 3), node(entity, 4));
    db.insert_hyperedge(edge_space, e1).unwrap();
    let err = db.insert_hyperedge(edge_space, e2).unwrap_err();
    assert!(err.to_string().contains("backpressure") || db.derivation_stats().pending_tasks > 0);
    db.sync().unwrap();
}
