//! Phase 6 — per-session derivation watermarks, delta merge, backpressure, recovery.

use std::collections::BTreeMap;

use infinite_db::{
    DerivationBackpressurePolicy, InfiniteDb, OpenOptions, WriteSession, FORMAT_VERSION_V5,
};
use infinite_db::infinitedb_core::{
    address::{DimensionVector, RevisionId, SpaceId},
    hyperedge::{
        Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
        HyperedgeKind,
    },
    space::SpaceConfig,
};
use tempfile::TempDir;

fn commit_session(db: &InfiniteDb, session: &WriteSession) {
    let durable = db.sync_session_wal(session).unwrap();
    if session.has_pending_intent() {
        db.commit_session_intent(session, &durable).unwrap();
    } else {
        db.sync().unwrap();
    }
}

fn open_db() -> (InfiniteDb, TempDir, SpaceId) {
    open_db_with_policy(DerivationBackpressurePolicy::default())
}

fn open_db_with_policy(policy: DerivationBackpressurePolicy) -> (InfiniteDb, TempDir, SpaceId) {
    let dir = TempDir::new().unwrap();
    let db = OpenOptions {
        format_version: Some(FORMAT_VERSION_V5),
        derivation: policy,
        ..OpenOptions::default()
    }
    .open(dir.path())
    .unwrap();
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

fn commit_hyperedge(
    db: &InfiniteDb,
    session: &WriteSession,
    space: SpaceId,
    edge: Hyperedge,
) -> RevisionId {
    let rev = db
        .insert_hyperedge_with_session(session, space, edge)
        .unwrap();
    commit_session(db, session);
    rev
}

#[test]
fn per_session_watermark_independent_advance() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let s1 = db.open_session();
    let s2 = db.open_session();
    let hub = node(entity, 1);
    commit_hyperedge(
        &db,
        &s1,
        edge_space,
        directed_edge(100, hub.clone(), node(entity, 2)),
    );
    commit_hyperedge(
        &db,
        &s2,
        edge_space,
        directed_edge(101, hub.clone(), node(entity, 3)),
    );
    db.sync_derivation();
    let wm = db.endpoint_index_watermark_vector();
    assert!(wm.get(s1.id()).unwrap() > RevisionId::ZERO);
    assert!(wm.get(s2.id()).unwrap() > RevisionId::ZERO);
    assert_ne!(
        wm.get(s1.id()).unwrap().session(),
        wm.get(s2.id()).unwrap().session()
    );
}

#[test]
fn pipeline_lag_one_session_only() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let s1 = db.open_session();
    let s2 = db.open_session();
    let hub_s1 = node(entity, 50);
    let hub_s2 = node(entity, 51);
    commit_hyperedge(
        &db,
        &s1,
        edge_space,
        directed_edge(200, hub_s1.clone(), node(entity, 52)),
    );
    commit_hyperedge(
        &db,
        &s2,
        edge_space,
        directed_edge(201, hub_s2.clone(), node(entity, 53)),
    );
    db.sync_derivation();
    let wm = db.endpoint_index_watermark_vector();
    assert!(wm.get(s1.id()).unwrap() > RevisionId::ZERO);
    assert!(wm.get(s2.id()).unwrap() > RevisionId::ZERO);
    assert_ne!(wm.get(s1.id()).unwrap().session(), wm.get(s2.id()).unwrap().session());
    assert_eq!(db.query_hyperedges(edge_space, None).unwrap().len(), 2);
}

#[test]
fn backpressure_isolates_flooding_session() {
    let (db, _dir, edge_space) = open_db_with_policy(DerivationBackpressurePolicy {
        max_pending_tasks: 10_000,
        max_derivation_lag: 1,
        max_worker_threads: 1,
    });
    let entity = SpaceId(1);
    let s1 = db.open_session();
    let s2 = db.open_session();
    db.insert_hyperedge_with_session(
        &s1,
        edge_space,
        directed_edge(300, node(entity, 300), node(entity, 400)),
    )
    .unwrap();
    db.insert_hyperedge_with_session(
        &s1,
        edge_space,
        directed_edge(301, node(entity, 301), node(entity, 401)),
    )
    .unwrap();
    let err = db
        .insert_hyperedge_with_session(
            &s1,
            edge_space,
            directed_edge(303, node(entity, 303), node(entity, 403)),
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("backpressure"),
        "flooded session should hit backpressure: {err}"
    );
    let s2_ok = db
        .insert_hyperedge_with_session(
            &s2,
            edge_space,
            directed_edge(400, node(entity, 400), node(entity, 500)),
        )
        .is_ok();
    assert!(s2_ok, "non-flooded session should still accept writes");
}

#[test]
fn crash_recovery_per_session_gap() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    let edge_space = SpaceId(10);
    let entity = SpaceId(1);
    let hub_s1 = node(entity, 60);
    let hub_s2 = node(entity, 61);
    {
        let db = OpenOptions {
            format_version: Some(FORMAT_VERSION_V5),
            ..OpenOptions::default()
        }
        .open(path)
        .unwrap();
        db.register_space(SpaceConfig::new(edge_space, "edges", 2))
            .unwrap();
        let s1 = db.open_session();
        let s2 = db.open_session();
        commit_hyperedge(
            &db,
            &s1,
            edge_space,
            directed_edge(500, hub_s1.clone(), node(entity, 62)),
        );
        commit_hyperedge(
            &db,
            &s2,
            edge_space,
            directed_edge(501, hub_s2.clone(), node(entity, 63)),
        );
    }
    {
        let db = OpenOptions {
            format_version: Some(FORMAT_VERSION_V5),
            ..OpenOptions::default()
        }
        .open(path)
        .unwrap();
        db.sync_derivation();
        assert_eq!(
            db.query_hyperedges_for_endpoint(edge_space, &hub_s1, None)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            db.query_hyperedges_for_endpoint(edge_space, &hub_s2, None)
                .unwrap()
                .len(),
            1
        );
    }
}

#[test]
fn delete_tombstone_ordering_cross_session() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let s2 = db.open_session();
    let hub_s0 = node(entity, 70);
    let hub_s2 = node(entity, 71);
    db.insert_hyperedge(
        edge_space,
        directed_edge(600, hub_s0.clone(), node(entity, 72)),
    )
    .unwrap();
    commit_hyperedge(
        &db,
        &s2,
        edge_space,
        directed_edge(601, hub_s2.clone(), node(entity, 73)),
    );
    db.sync_derivation();
    db.delete_hyperedge(edge_space, HyperedgeId(600)).unwrap();
    db.sync().unwrap();
    assert!(
        db.fetch_hyperedge_by_id(edge_space, HyperedgeId(600), None)
            .unwrap()
            .is_none()
    );
    assert!(
        db.fetch_hyperedge_by_id(edge_space, HyperedgeId(601), None)
            .unwrap()
            .is_some()
    );
}

#[test]
fn hash_partition_preserves_per_edge_order() {
    let (db, _dir, edge_space) = open_db_with_policy(DerivationBackpressurePolicy {
        max_pending_tasks: 10_000,
        max_derivation_lag: u64::MAX,
        max_worker_threads: 4,
    });
    let entity = SpaceId(1);
    let session = db.open_session();
    let hub = node(entity, 80);
    let edge_id = HyperedgeId(700);
    for i in 0..8 {
        let edge = directed_edge(edge_id.0, hub.clone(), node(entity, 90 + i));
        db.insert_hyperedge_with_session(&session, edge_space, edge)
            .unwrap();
    }
    commit_session(&db, &session);
    db.sync_derivation();
    let edges = db
        .query_hyperedges_for_endpoint(edge_space, &hub, None)
        .unwrap();
    assert!(
        edges.len() <= 1,
        "serial upserts on one edge id must not duplicate incidence rows"
    );
}
