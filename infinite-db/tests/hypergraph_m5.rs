//! Milestone 5 — provenance, judgments, per-space error records.

use std::collections::BTreeMap;

use infinite_db::{ArbiterId, EngineError, FrameId, ImportBudget, InfiniteDb, OpenOptions};
use infinite_db::infinitedb_core::{
    address::{DimensionVector, RevisionId, SpaceId},
    hyperedge::{
        Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
        HyperedgeKind,
    },
    judgment::{
        JudgmentId, JudgmentRecord, JudgmentValidationError, JudgmentVerdict, SubjectIdentity,
        SubjectKind, SubjectPin,
    },
    provenance::AuthoringFrameProvenance,
};
use tempfile::TempDir;

fn open_db() -> (InfiniteDb, TempDir, SpaceId) {
    let dir = TempDir::new().unwrap();
    let db = OpenOptions::default().open(dir.path()).unwrap();
    let edge_space = SpaceId(10);
    db.register_space(infinite_db::infinitedb_core::space::SpaceConfig::new(
        edge_space, "edges", 2,
    ))
    .unwrap();
    (db, dir, edge_space)
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

fn node(space: SpaceId, x: u32) -> EndpointRef {
    EndpointRef::new(
        EndpointRole::new("n"),
        space,
        DimensionVector::new(vec![x, 0]),
    )
}

#[test]
fn v3_provenance_roundtrip_after_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    let edge_space = SpaceId(10);
    let entity = SpaceId(1);
    let hub = node(entity, 1);
    {
        let db = OpenOptions::default().open(path).unwrap();
        db.register_space(infinite_db::infinitedb_core::space::SpaceConfig::new(
            edge_space, "edges", 2,
        ))
        .unwrap();
        let mut edge = directed_edge(1, hub, node(entity, 2));
        edge.authoring_frame = Some(AuthoringFrameProvenance {
            frame_id: FrameId(42),
            as_of: RevisionId::legacy(1),
        });
        db.insert_hyperedge(edge_space, edge).unwrap();
        db.sync().unwrap();
    }
    {
        let db = OpenOptions::default().open(path).unwrap();
        let edges = db.query_hyperedges(edge_space, None).unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].authoring_frame.as_ref().unwrap().frame_id, FrameId(42));
    }
}

#[test]
fn judgment_pin_rejects_wrong_revision() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let hub = node(entity, 5);
    let edge = directed_edge(9, hub.clone(), node(entity, 6));
    let rev = db.insert_hyperedge(edge_space, edge).unwrap();
    db.sync().unwrap();
    db.register_arbiter_stream(ArbiterId(1), "validator", 2)
        .unwrap();
    let bad_pin = SubjectPin {
        kind: SubjectKind::Hyperedge,
        space: edge_space,
        identity: SubjectIdentity::Hyperedge(HyperedgeId(9)),
        subject_revision: RevisionId::legacy(rev.legacy_sequence() + 100),
    };
    let record = JudgmentRecord {
        id: JudgmentId(1),
        arbiter: ArbiterId(1),
        subject: bad_pin,
        verdict: JudgmentVerdict::Pass,
        rationale: None,
        authoring_frame: None,
    };
    let err = db.assert_judgment(ArbiterId(1), record).unwrap_err();
    assert!(matches!(
        err,
        EngineError::InvalidJudgment(JudgmentValidationError::SubjectNotFound { .. })
            | EngineError::InvalidJudgment(JudgmentValidationError::SubjectRevisionMismatch { .. })
    ));
}

#[test]
fn judgment_region_query_finds_colocated_record() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let hub = node(entity, 7);
    let edge = directed_edge(3, hub.clone(), node(entity, 8));
    let rev = db.insert_hyperedge(edge_space, edge).unwrap();
    db.sync().unwrap();
    db.register_arbiter_stream(ArbiterId(2), "code", 2).unwrap();
    let pin = SubjectPin {
        kind: SubjectKind::Hyperedge,
        space: edge_space,
        identity: SubjectIdentity::Hyperedge(HyperedgeId(3)),
        subject_revision: rev,
    };
    db.assert_judgment(
        ArbiterId(2),
        JudgmentRecord {
            id: JudgmentId(10),
            arbiter: ArbiterId(2),
            subject: pin,
            verdict: JudgmentVerdict::Pass,
            rationale: None,
            authoring_frame: None,
        },
    )
    .unwrap();
    db.sync().unwrap();
    let storage = Hyperedge::storage_point(HyperedgeId(3));
    let found = db
        .query_judgments_in_region(
            ArbiterId(2),
            storage.clone(),
            storage,
            None,
        )
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].id, JudgmentId(10));
}

#[test]
fn import_errors_persist_in_companion_error_space() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let mut session = db
        .begin_hyperedge_import(edge_space, ImportBudget::default())
        .unwrap();
    let bad = Hyperedge {
        id: HyperedgeId(2),
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
    assert_eq!(result.errors.len(), 1);
    db.sync().unwrap();
    let errors = db.query_operation_errors(edge_space, None, None).unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].source_space, edge_space);
}

#[test]
fn per_space_error_isolation() {
    let (db, _dir, edge_space_a) = open_db();
    let edge_space_b = SpaceId(11);
    db.register_space(infinite_db::infinitedb_core::space::SpaceConfig::new(
        edge_space_b, "edges_b", 2,
    ))
    .unwrap();
    let mut session = db
        .begin_hyperedge_import(edge_space_a, ImportBudget::default())
        .unwrap();
    session
        .push(Hyperedge {
            id: HyperedgeId(1),
            kind: HyperedgeKind::new("bad"),
            endpoints: vec![node(SpaceId(1), 1)],
            weight_milli: None,
            metadata: BTreeMap::new(),
            valid_from: RevisionId::ZERO,
            valid_to: None,
            directionality: Directionality::Undirected,
            authoring_frame: None,
        computation: None,
        })
        .unwrap();
    db.commit_hyperedge_import(session).unwrap();
    db.sync().unwrap();
    assert!(db.query_operation_errors(edge_space_a, None, None).unwrap().len() >= 1);
    assert!(db.query_operation_errors(edge_space_b, None, None).unwrap().is_empty());
}

#[test]
fn resolve_operation_error_tombstones_record() {
    let (db, _dir, edge_space) = open_db();
    let mut session = db
        .begin_hyperedge_import(
            edge_space,
            ImportBudget {
                max_errors: 0,
                sample_every: 1,
            },
        )
        .unwrap();
    session
        .push(Hyperedge {
            id: HyperedgeId(1),
            kind: HyperedgeKind::new("bad"),
            endpoints: vec![node(SpaceId(1), 1)],
            weight_milli: None,
            metadata: BTreeMap::new(),
            valid_from: RevisionId::ZERO,
            valid_to: None,
            directionality: Directionality::Undirected,
            authoring_frame: None,
        computation: None,
        })
        .unwrap();
    db.commit_hyperedge_import(session).unwrap();
    db.sync().unwrap();
    let errors = db.query_operation_errors(edge_space, None, None).unwrap();
    assert!(!errors.is_empty());
    let range_start = errors[0].revision_range.first;
    db.resolve_operation_error(edge_space, range_start).unwrap();
    db.sync().unwrap();
    assert!(db
        .query_operation_errors(edge_space, None, None)
        .unwrap()
        .is_empty());
}
