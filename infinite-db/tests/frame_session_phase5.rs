//! Peer track Phase 5 — session-scoped frame admission and version-vector pins.

use std::collections::BTreeMap;
use std::thread;
use std::time::Duration;

use infinite_db::{
    AssertionScope, FrameQuery, FrameQueryOptions, FrameRegisterRequest, InfiniteDb, OpenOptions,
    WriteSession, FORMAT_VERSION_V5,
};
use infinite_db::infinitedb_core::{
    address::{DimensionVector, RevisionId, SpaceId},
    frame_query::FrameVersionPin,
    hyperedge::{
        Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
        HyperedgeKind,
    },
    judgment::{SubjectIdentity, SubjectKind, SubjectPin},
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
    let dir = TempDir::new().unwrap();
    let db = OpenOptions {
        format_version: Some(FORMAT_VERSION_V5),
        ..OpenOptions::default()
    }
    .open(dir.path())
    .unwrap();
    let edge_space = SpaceId(10);
    db.register_space(infinite_db::infinitedb_core::space::SpaceConfig::new(
        edge_space, "edges", 2,
    ))
    .unwrap();
    (db, dir, edge_space)
}

fn node(space: SpaceId, x: u32, y: u32) -> EndpointRef {
    EndpointRef::new(
        EndpointRole::new("n"),
        space,
        DimensionVector::new(vec![x, y]),
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

fn edge_bbox(id: u64) -> (DimensionVector, DimensionVector) {
    let p = Hyperedge::storage_point(HyperedgeId(id));
    (p.clone(), p)
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
fn session_scope_admits_only_listed_sessions() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let s1 = db.open_session();
    let s2 = db.open_session();
    let edge = directed_edge(700, node(entity, 1, 0), node(entity, 2, 0));
    commit_hyperedge(&db, &s1, edge_space, edge);
    let edge2 = directed_edge(701, node(entity, 3, 0), node(entity, 4, 0));
    commit_hyperedge(&db, &s2, edge_space, edge2);
    db.sync_derivation();

    let frame = db
        .register_frame(FrameRegisterRequest {
            id: None,
            name: "s1-only".into(),
            assertion_scope: AssertionScope::Session(vec![s1.id()]),
            judgment_overlay: vec![],
            default_as_of: None,
        })
        .unwrap();
    let (min, max) = (
        DimensionVector::new(vec![0, 0]),
        DimensionVector::new(vec![u32::MAX, u32::MAX]),
    );
    let results = db
        .query_hyperedges_in_frame(FrameQuery {
            frame_id: frame.id,
            testimony_space: edge_space,
            min,
            max,
            as_of: None,
            version_vector: None,
            options: FrameQueryOptions::default(),
        })
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].edge.id, HyperedgeId(700));
    assert_eq!(
        results[0].edge.valid_from.session(),
        s1.id().0,
        "visible edge must be session s1"
    );
}

#[test]
fn cross_session_supersession_by_authorship_not_arrival() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let s_a = db.open_session();
    let s_b = db.open_session();

    let edge_a = directed_edge(800, node(entity, 10, 0), node(entity, 11, 0));
    let rev_a = db
        .insert_hyperedge_with_session(&s_a, edge_space, edge_a)
        .unwrap();

    thread::sleep(Duration::from_millis(5));

    let edge_b = directed_edge(800, node(entity, 20, 0), node(entity, 21, 0));
    let rev_b = commit_hyperedge(&db, &s_b, edge_space, edge_b);
    commit_session(&db, &s_a);

    assert!(
        rev_b > rev_a,
        "session B must author later than A for this test"
    );

    let frame = db
        .register_frame(FrameRegisterRequest {
            id: None,
            name: "both-sessions".into(),
            assertion_scope: AssertionScope::Session(vec![s_a.id(), s_b.id()]),
            judgment_overlay: vec![],
            default_as_of: None,
        })
        .unwrap();
    let (min, max) = edge_bbox(800);
    let results = db
        .query_hyperedges_in_frame(FrameQuery {
            frame_id: frame.id,
            testimony_space: edge_space,
            min,
            max,
            as_of: None,
            version_vector: None,
            options: FrameQueryOptions::default(),
        })
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].edge.valid_from, rev_b,
        "later-authored revision wins regardless of commit order"
    );
    assert_eq!(results[0].edge.endpoints[0].node, node(entity, 20, 0).node);
}

#[test]
fn select_contested_surfaces_conflict_under_session_scope() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let session = db.open_session();
    let _rev = commit_hyperedge(
        &db,
        &session,
        edge_space,
        directed_edge(810, node(entity, 1, 0), node(entity, 2, 0)),
    );
    db.sync().unwrap();
    db.sync_derivation();
    assert!(
        db.fetch_hyperedge_by_id(edge_space, HyperedgeId(810), None)
            .unwrap()
            .is_some()
    );

    let frame = db
        .register_frame(FrameRegisterRequest {
            id: None,
            name: "contested".into(),
            assertion_scope: AssertionScope::Session(vec![session.id()]),
            judgment_overlay: vec![],
            default_as_of: None,
        })
        .unwrap();

    let (min, max) = (
        DimensionVector::new(vec![0, 0]),
        DimensionVector::new(vec![u32::MAX, u32::MAX]),
    );
    let results = db
        .query_hyperedges_in_frame(FrameQuery {
            frame_id: frame.id,
            testimony_space: edge_space,
            min: min.clone(),
            max: max.clone(),
            as_of: None,
            version_vector: None,
            options: FrameQueryOptions::default(),
        })
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].edge.valid_from.session(), session.id().0);
}

#[test]
fn version_vector_pin_excludes_later_fast_session_writes() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let slow = db.open_session();
    let fast = db.open_session();

    let rev_slow = commit_hyperedge(
        &db,
        &slow,
        edge_space,
        directed_edge(900, node(entity, 1, 0), node(entity, 2, 0)),
    );
    let rev_fast1 = commit_hyperedge(
        &db,
        &fast,
        edge_space,
        directed_edge(901, node(entity, 3, 0), node(entity, 4, 0)),
    );
    let _rev_fast2 = commit_hyperedge(
        &db,
        &fast,
        edge_space,
        directed_edge(902, node(entity, 5, 0), node(entity, 6, 0)),
    );

    let mut pin: FrameVersionPin = BTreeMap::new();
    pin.insert(slow.id(), rev_slow);
    pin.insert(fast.id(), rev_fast1);

    let frame = db
        .register_frame(FrameRegisterRequest {
            id: None,
            name: "vector-pin".into(),
            assertion_scope: AssertionScope::Session(vec![slow.id(), fast.id()]),
            judgment_overlay: vec![],
            default_as_of: None,
        })
        .unwrap();

    let results_pinned = db
        .query_hyperedges_in_frame(FrameQuery {
            frame_id: frame.id,
            testimony_space: edge_space,
            min: DimensionVector::new(vec![0, 0]),
            max: DimensionVector::new(vec![u32::MAX, u32::MAX]),
            as_of: None,
            version_vector: Some(pin),
            options: FrameQueryOptions::default(),
        })
        .unwrap();
    let ids_pinned: Vec<_> = results_pinned.iter().map(|e| e.edge.id.0).collect();
    assert!(ids_pinned.contains(&900));
    assert!(ids_pinned.contains(&901));
    assert!(
        !ids_pinned.contains(&902),
        "vector pin at rev_fast1 must hide edge 902 authored later on fast session"
    );

    let results_head = db
        .query_hyperedges_in_frame(FrameQuery {
            frame_id: frame.id,
            testimony_space: edge_space,
            min: DimensionVector::new(vec![0, 0]),
            max: DimensionVector::new(vec![u32::MAX, u32::MAX]),
            as_of: None,
            version_vector: None,
            options: FrameQueryOptions::default(),
        })
        .unwrap();
    let ids_head: Vec<_> = results_head.iter().map(|e| e.edge.id.0).collect();
    assert!(ids_head.contains(&902));
}

#[test]
fn retrospective_session_trajectory_by_as_of() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let session = db.open_session();
    let rev1 = commit_hyperedge(
        &db,
        &session,
        edge_space,
        directed_edge(950, node(entity, 1, 0), node(entity, 2, 0)),
    );
    thread::sleep(Duration::from_millis(5));
    let rev2 = commit_hyperedge(
        &db,
        &session,
        edge_space,
        directed_edge(950, node(entity, 9, 0), node(entity, 10, 0)),
    );
    db.sync_derivation();

    let frame = db
        .register_frame(FrameRegisterRequest {
            id: None,
            name: "session-retrospective".into(),
            assertion_scope: AssertionScope::Session(vec![session.id()]),
            judgment_overlay: vec![],
            default_as_of: None,
        })
        .unwrap();
    let (min, max) = (
        DimensionVector::new(vec![0, 0]),
        DimensionVector::new(vec![u32::MAX, u32::MAX]),
    );
    let at_first = db
        .query_hyperedges_in_frame(FrameQuery {
            frame_id: frame.id,
            testimony_space: edge_space,
            min: min.clone(),
            max: max.clone(),
            as_of: Some(rev1),
            version_vector: None,
            options: FrameQueryOptions::default(),
        })
        .unwrap();
    assert_eq!(at_first.len(), 1);
    assert_eq!(at_first[0].edge.endpoints[0].node, node(entity, 1, 0).node);

    let at_head = db
        .query_hyperedges_in_frame(FrameQuery {
            frame_id: frame.id,
            testimony_space: edge_space,
            min,
            max,
            as_of: Some(rev2),
            version_vector: None,
            options: FrameQueryOptions::default(),
        })
        .unwrap();
    assert_eq!(at_head.len(), 1);
    assert_eq!(at_head[0].edge.valid_from, rev2);
    assert!(rev2 > rev1);
}

#[test]
fn session_scope_persists_after_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    let edge_space = SpaceId(10);
    let session_id;
    {
        let db = OpenOptions {
            format_version: Some(FORMAT_VERSION_V5),
            ..OpenOptions::default()
        }
        .open(path)
        .unwrap();
        db.register_space(infinite_db::infinitedb_core::space::SpaceConfig::new(
            edge_space, "edges", 2,
        ))
        .unwrap();
        let session = db.open_session();
        session_id = session.id();
        let entity = SpaceId(1);
        commit_hyperedge(
            &db,
            &session,
            edge_space,
            directed_edge(960, node(entity, 1, 0), node(entity, 2, 0)),
        );
        db.register_frame(FrameRegisterRequest {
            id: None,
            name: "persist".into(),
            assertion_scope: AssertionScope::Session(vec![session_id]),
            judgment_overlay: vec![],
            default_as_of: None,
        })
        .unwrap();
        db.sync().unwrap();
    }
    {
        let db = OpenOptions {
            format_version: Some(FORMAT_VERSION_V5),
            ..OpenOptions::default()
        }
        .open(path)
        .unwrap();
        let frames = db.list_frames();
        assert_eq!(frames.len(), 1);
        assert!(matches!(
            frames[0].assertion_scope,
            AssertionScope::Session(_)
        ));
        let (min, max) = edge_bbox(960);
        let results = db
            .query_hyperedges_in_frame(FrameQuery {
                frame_id: frames[0].id,
                testimony_space: edge_space,
                min,
                max,
                as_of: None,
                version_vector: None,
                options: FrameQueryOptions::default(),
            })
            .unwrap();
        assert_eq!(results.len(), 1);
    }
}

#[test]
fn empty_session_scope_rejected() {
    let (db, _dir, edge_space) = open_db();
    let err = db
        .register_frame(FrameRegisterRequest {
            id: None,
            name: "empty".into(),
            assertion_scope: AssertionScope::Session(vec![]),
            judgment_overlay: vec![],
            default_as_of: None,
        })
        .unwrap_err();
    assert!(matches!(
        err,
        infinite_db::EngineError::InvalidFrame(_)
    ));
    let _ = edge_space;
}

#[test]
fn cross_session_staleness_when_upstream_superseded() {
    use infinite_db::FreshnessStatus;

    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let s_upstream = db.open_session();
    let s_derived = db.open_session();

    let rev_u = commit_hyperedge(
        &db,
        &s_upstream,
        edge_space,
        directed_edge(970, node(entity, 0, 0), node(entity, 1, 0)),
    );
    let mut derived = directed_edge(971, node(entity, 2, 0), node(entity, 3, 0));
    derived.computation = Some(infinite_db::infinitedb_core::computation::ComputationProvenance {
        inputs: vec![SubjectPin {
            kind: SubjectKind::Hyperedge,
            space: edge_space,
            identity: SubjectIdentity::Hyperedge(HyperedgeId(970)),
            subject_revision: rev_u,
        }],
    });
    commit_hyperedge(&db, &s_derived, edge_space, derived);
    db.sync_derivation();

    let fresh = db
        .check_hyperedge_freshness(edge_space, HyperedgeId(971), None)
        .unwrap();
    assert!(
        fresh.is_fresh,
        "expected fresh before upstream supersession: {:?}",
        fresh
    );

    thread::sleep(Duration::from_millis(5));
    commit_hyperedge(
        &db,
        &s_upstream,
        edge_space,
        directed_edge(970, node(entity, 9, 0), node(entity, 1, 0)),
    );
    db.sync_derivation();

    let stale = db
        .check_hyperedge_freshness(edge_space, HyperedgeId(971), None)
        .unwrap();
    assert!(!stale.is_fresh);
    assert_eq!(stale.inputs[0].status, FreshnessStatus::Stale);
}
