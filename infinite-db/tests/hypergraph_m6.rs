//! Milestone 6 — frame-resolution query API.

use std::collections::BTreeMap;

use infinite_db::{
    ArbiterId, AssertionScope, EngineError, FrameId, FrameQuery, FrameQueryOptions,
    FrameRegisterRequest, FrameTraversalSpec, InfiniteDb, JudgmentOverlayLayer, OpenOptions,
    OverlayPolicy, StalenessDiagnosis, VerdictFilter,
};
use infinite_db::infinitedb_core::{
    address::{DimensionVector, RevisionId, SpaceId},
    hyperedge::{
        Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
        HyperedgeKind,
    },
    judgment::{
        JudgmentId, JudgmentRecord, JudgmentVerdict, SubjectIdentity, SubjectKind, SubjectPin,
    },
    provenance::{AuthoringFrameProvenance, FrameId as ProvFrameId},
    traversal::{TraversalDirection, TraversalSpec},
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

fn edge_bbox(id: u64) -> (DimensionVector, DimensionVector) {
    let p = Hyperedge::storage_point(HyperedgeId(id));
    (p.clone(), p)
}

#[test]
fn frame_register_roundtrip_after_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    let edge_space = SpaceId(10);
    {
        let db = OpenOptions::default().open(path).unwrap();
        db.register_space(infinite_db::infinitedb_core::space::SpaceConfig::new(
            edge_space, "edges", 2,
        ))
        .unwrap();
        let def = db
            .register_frame(FrameRegisterRequest {
                id: Some(FrameId(7)),
                name: "buildable".into(),
                assertion_scope: AssertionScope::Spaces(vec![edge_space]),
                judgment_overlay: vec![],
                default_as_of: None,
            })
            .unwrap();
        assert_eq!(def.id, FrameId(7));
        db.sync().unwrap();
    }
    {
        let db = OpenOptions::default().open(path).unwrap();
        let def = db.get_frame(FrameId(7)).unwrap();
        assert_eq!(def.name, "buildable");
        assert_eq!(db.list_frames().len(), 1);
    }
}

#[test]
fn suppress_policy_removes_condemned_edges() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let edge = directed_edge(500, node(entity, 1), node(entity, 2));
    let rev = db.insert_hyperedge(edge_space, edge).unwrap();
    db.sync().unwrap();
    db.register_arbiter_stream(ArbiterId(11), "code", 2).unwrap();
    let pin = SubjectPin {
        kind: SubjectKind::Hyperedge,
        space: edge_space,
        identity: SubjectIdentity::Hyperedge(HyperedgeId(500)),
        subject_revision: rev,
    };
    db.assert_judgment(
        ArbiterId(11),
        JudgmentRecord {
            id: JudgmentId(1),
            arbiter: ArbiterId(11),
            subject: pin,
            verdict: JudgmentVerdict::Fail,
            rationale: None,
            authoring_frame: None,
        },
    )
    .unwrap();
    db.sync().unwrap();
    let frame = db
        .register_frame(FrameRegisterRequest {
            id: None,
            name: "construction".into(),
            assertion_scope: AssertionScope::Spaces(vec![edge_space]),
            judgment_overlay: vec![JudgmentOverlayLayer {
                arbiter: ArbiterId(11),
                policy: OverlayPolicy::Suppress,
                verdict_filter: VerdictFilter::Any,
            }],
            default_as_of: None,
        })
        .unwrap();
    let (min, max) = edge_bbox(500);
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
    assert!(results.is_empty());
    assert_eq!(db.query_hyperedges(edge_space, None).unwrap().len(), 1);
}

#[test]
fn annotate_policy_keeps_edges_with_judgments() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let edge = directed_edge(301, node(entity, 3), node(entity, 4));
    let rev = db.insert_hyperedge(edge_space, edge).unwrap();
    db.sync().unwrap();
    db.register_arbiter_stream(ArbiterId(18), "annotator", 2).unwrap();
    let pin = SubjectPin {
        kind: SubjectKind::Hyperedge,
        space: edge_space,
        identity: SubjectIdentity::Hyperedge(HyperedgeId(301)),
        subject_revision: rev,
    };
    db.assert_judgment(
        ArbiterId(18),
        JudgmentRecord {
            id: JudgmentId(2),
            arbiter: ArbiterId(18),
            subject: pin,
            verdict: JudgmentVerdict::Annotate,
            rationale: Some("note".into()),
            authoring_frame: None,
        },
    )
    .unwrap();
    db.sync().unwrap();
    let frame = db
        .register_frame(FrameRegisterRequest {
            id: None,
            name: "annotate".into(),
            assertion_scope: AssertionScope::Spaces(vec![edge_space]),
            judgment_overlay: vec![JudgmentOverlayLayer {
                arbiter: ArbiterId(18),
                policy: OverlayPolicy::Annotate,
                verdict_filter: VerdictFilter::Any,
            }],
            default_as_of: None,
        })
        .unwrap();
    let (min, max) = edge_bbox(301);
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
    assert_eq!(results[0].judgments.len(), 1);
}

#[test]
fn select_contested_surfaces_conflict_judgments() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let edge = directed_edge(800, node(entity, 10), node(entity, 11));
    let rev = db.insert_hyperedge(edge_space, edge).unwrap();
    db.sync().unwrap();
    db.register_arbiter_stream(ArbiterId(14), "review", 2).unwrap();
    let pin = SubjectPin {
        kind: SubjectKind::Hyperedge,
        space: edge_space,
        identity: SubjectIdentity::Hyperedge(HyperedgeId(800)),
        subject_revision: rev,
    };
    db.assert_judgment(
        ArbiterId(14),
        JudgmentRecord {
            id: JudgmentId(3),
            arbiter: ArbiterId(14),
            subject: pin,
            verdict: JudgmentVerdict::Conflict,
            rationale: None,
            authoring_frame: None,
        },
    )
    .unwrap();
    db.sync().unwrap();
    let frame = db
        .register_frame(FrameRegisterRequest {
            id: None,
            name: "review".into(),
            assertion_scope: AssertionScope::Spaces(vec![edge_space]),
            judgment_overlay: vec![JudgmentOverlayLayer {
                arbiter: ArbiterId(14),
                policy: OverlayPolicy::SelectContested,
                verdict_filter: VerdictFilter::Any,
            }],
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
    assert!(results[0]
        .judgments
        .iter()
        .any(|j| matches!(j.record.verdict, JudgmentVerdict::Conflict)));
}

#[test]
fn provenance_diagnosis_in_frame_query() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let frame_id = FrameId(99);
    db.register_frame(FrameRegisterRequest {
        id: Some(frame_id),
        name: "diag".into(),
        assertion_scope: AssertionScope::Spaces(vec![edge_space]),
        judgment_overlay: vec![],
        default_as_of: Some(RevisionId::legacy(100)),
    })
    .unwrap();
    let mut edge = directed_edge(1200, node(entity, 20), node(entity, 21));
    edge.authoring_frame = Some(AuthoringFrameProvenance {
        frame_id: ProvFrameId(99),
        as_of: RevisionId::legacy(1),
    });
    db.insert_hyperedge(edge_space, edge).unwrap();
    db.sync().unwrap();
    let (min, max) = edge_bbox(1200);
    let results = db
        .query_hyperedges_in_frame(FrameQuery {
            frame_id,
            testimony_space: edge_space,
            min,
            max,
            as_of: Some(RevisionId::legacy(100)),
            version_vector: None,
            options: FrameQueryOptions {
                include_diagnosis: true,
                ..Default::default()
            },
        })
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].diagnosis,
        Some(StalenessDiagnosis::StaleFrame)
    );
}

#[test]
fn performance_contract_scan_count_bounded() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    for i in 0..20u64 {
        let e = directed_edge(2000 + i, node(entity, i as u32), node(entity, i as u32 + 1));
        db.insert_hyperedge(edge_space, e).unwrap();
    }
    db.sync().unwrap();
    db.register_arbiter_stream(ArbiterId(15), "a1", 2).unwrap();
    db.register_arbiter_stream(ArbiterId(16), "a2", 2).unwrap();
    let frame = db
        .register_frame(FrameRegisterRequest {
            id: None,
            name: "perf".into(),
            assertion_scope: AssertionScope::Spaces(vec![edge_space]),
            judgment_overlay: vec![
                JudgmentOverlayLayer {
                    arbiter: ArbiterId(15),
                    policy: OverlayPolicy::Annotate,
                    verdict_filter: VerdictFilter::Any,
                },
                JudgmentOverlayLayer {
                    arbiter: ArbiterId(16),
                    policy: OverlayPolicy::Annotate,
                    verdict_filter: VerdictFilter::Any,
                },
            ],
            default_as_of: None,
        })
        .unwrap();
    let (min, max) = edge_bbox(2000);
    db.reset_query_plan_stats();
    let _ = db
        .query_bbox(edge_space, min.clone(), max.clone(), None)
        .unwrap();
    let base_scans = db.query_plan_stats().interval_scans;
    db.reset_query_plan_stats();
    let _ = db
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
    let frame_scans = db.query_plan_stats().interval_scans;
    let admitted = 1u32;
    let arbiters = 2u32;
    let k = 2u32;
    assert!(
        frame_scans <= base_scans + k * (admitted + arbiters),
        "frame_scans={frame_scans} base={base_scans}"
    );
}

#[test]
fn traverse_in_frame_skips_suppressed_edges() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let start = node(entity, 100);
    let mid = node(entity, 101);
    let end = node(entity, 102);
    let e1 = directed_edge(4000, start.clone(), mid.clone());
    let e2 = directed_edge(4001, mid.clone(), end.clone());
    let rev1 = db.insert_hyperedge(edge_space, e1).unwrap();
    db.insert_hyperedge(edge_space, e2).unwrap();
    db.sync().unwrap();
    db.register_arbiter_stream(ArbiterId(17), "gate", 2).unwrap();
    db.assert_judgment(
        ArbiterId(17),
        JudgmentRecord {
            id: JudgmentId(4),
            arbiter: ArbiterId(17),
            subject: SubjectPin {
                kind: SubjectKind::Hyperedge,
                space: edge_space,
                identity: SubjectIdentity::Hyperedge(HyperedgeId(4000)),
                subject_revision: rev1,
            },
            verdict: JudgmentVerdict::Fail,
            rationale: None,
            authoring_frame: None,
        },
    )
    .unwrap();
    db.sync().unwrap();
    let frame = db
        .register_frame(FrameRegisterRequest {
            id: None,
            name: "traverse".into(),
            assertion_scope: AssertionScope::Spaces(vec![edge_space]),
            judgment_overlay: vec![JudgmentOverlayLayer {
                arbiter: ArbiterId(17),
                policy: OverlayPolicy::Suppress,
                verdict_filter: VerdictFilter::Any,
            }],
            default_as_of: None,
        })
        .unwrap();
    db.sync_derivation();
    let result = db
        .traverse_in_frame(&FrameTraversalSpec {
            frame_id: frame.id,
            base: TraversalSpec {
                start: start.clone(),
                edge_space,
                max_depth: 3,
                direction: TraversalDirection::Forward,
                mode: Default::default(),
                follow_kinds: None,
                as_of: None,
            },
            as_of: None,
            version_vector: None,
            options: FrameQueryOptions::default(),
        })
        .unwrap();
    assert!(!result
        .traversal
        .edges
        .iter()
        .any(|e| e.id == HyperedgeId(4001)));
}

#[test]
fn retrospective_frame_lists_all_judgments() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let edge = directed_edge(300, node(entity, 50), node(entity, 51));
    let rev = db.insert_hyperedge(edge_space, edge).unwrap();
    db.sync().unwrap();
    db.register_arbiter_stream(ArbiterId(19), "history", 2).unwrap();
    db.assert_judgment(
        ArbiterId(19),
        JudgmentRecord {
            id: JudgmentId(5),
            arbiter: ArbiterId(19),
            subject: SubjectPin {
                kind: SubjectKind::Hyperedge,
                space: edge_space,
                identity: SubjectIdentity::Hyperedge(HyperedgeId(300)),
                subject_revision: rev,
            },
            verdict: JudgmentVerdict::Pass,
            rationale: None,
            authoring_frame: None,
        },
    )
    .unwrap();
    db.sync().unwrap();
    let frame = db
        .register_frame(FrameRegisterRequest {
            id: None,
            name: "retrospective".into(),
            assertion_scope: AssertionScope::Spaces(vec![edge_space]),
            judgment_overlay: vec![JudgmentOverlayLayer {
                arbiter: ArbiterId(19),
                policy: OverlayPolicy::Annotate,
                verdict_filter: VerdictFilter::Any,
            }],
            default_as_of: None,
        })
        .unwrap();
    let (min, max) = edge_bbox(300);
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
    assert_eq!(results[0].judgments.len(), 1);
}

#[test]
fn duplicate_frame_name_rejected() {
    let (db, _dir, edge_space) = open_db();
    db.register_frame(FrameRegisterRequest {
        id: None,
        name: "dup".into(),
        assertion_scope: AssertionScope::Spaces(vec![edge_space]),
        judgment_overlay: vec![],
        default_as_of: None,
    })
    .unwrap();
    let err = db
        .register_frame(FrameRegisterRequest {
            id: None,
            name: "dup".into(),
            assertion_scope: AssertionScope::Spaces(vec![edge_space]),
            judgment_overlay: vec![],
            default_as_of: None,
        })
        .unwrap_err();
    assert!(matches!(err, EngineError::FrameExists(_)));
}
