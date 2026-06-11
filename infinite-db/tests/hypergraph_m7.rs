//! Milestone 7 — flow-vector lane and staleness closures.

use std::collections::BTreeMap;

use infinite_db::{FreshnessStatus, InfiniteDb, OpenOptions, QueryOptions};
use infinite_db::infinitedb_core::{
    address::{DimensionVector, RevisionId, SpaceId},
    computation::ComputationProvenance as CoreComputation,
    flow_vector::{quantize_direction, FlowVectorQuantization},
    hyperedge::{
        Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
        HyperedgeKind,
    },
    judgment::{SubjectIdentity, SubjectKind, SubjectPin},
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

#[test]
fn v4_computation_roundtrip_after_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    let edge_space = SpaceId(10);
    let entity = SpaceId(1);
    let rev_u;
    {
        let db = OpenOptions::default().open(path).unwrap();
        db.register_space(infinite_db::infinitedb_core::space::SpaceConfig::new(
            edge_space, "edges", 2,
        ))
        .unwrap();
        let upstream = directed_edge(1, node(entity, 0, 0), node(entity, 1, 0));
        rev_u = db.insert_hyperedge(edge_space, upstream).unwrap();
        db.sync().unwrap();
        let mut derived = directed_edge(2, node(entity, 2, 0), node(entity, 3, 0));
        derived.computation = Some(CoreComputation {
            inputs: vec![SubjectPin {
                kind: SubjectKind::Hyperedge,
                space: edge_space,
                identity: SubjectIdentity::Hyperedge(HyperedgeId(1)),
                subject_revision: rev_u,
            }],
        });
        db.insert_hyperedge(edge_space, derived).unwrap();
        db.sync().unwrap();
    }
    {
        let db = OpenOptions::default().open(path).unwrap();
        let edge = db
            .fetch_hyperedge_by_id(edge_space, HyperedgeId(2), None)
            .unwrap()
            .unwrap();
        assert!(edge.computation.is_some());
        assert_eq!(edge.computation.as_ref().unwrap().inputs.len(), 1);
    }
}

#[test]
fn flow_vector_derivation_directed_only() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let directed = directed_edge(10, node(entity, 0, 0), node(entity, 5, 10));
    db.insert_hyperedge(edge_space, directed).unwrap();
    let undirected = Hyperedge {
        directionality: Directionality::Undirected,
        endpoints: vec![
            node(entity, 1, 0),
            node(entity, 2, 0),
        ],
        ..directed_edge(11, node(entity, 1, 0), node(entity, 2, 0))
    };
    db.insert_hyperedge(edge_space, undirected).unwrap();
    db.sync().unwrap();
    assert!(
        db.query_flow_vector_for_edge(edge_space, HyperedgeId(10), None)
            .unwrap()
            .is_some()
    );
    assert!(
        db.query_flow_vector_for_edge(edge_space, HyperedgeId(11), None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn quantized_direction_region_scan() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let up = directed_edge(20, node(entity, 0, 0), node(entity, 0, 20));
    let flat = directed_edge(21, node(entity, 0, 0), node(entity, 20, 0));
    db.insert_hyperedge(edge_space, up).unwrap();
    db.insert_hyperedge(edge_space, flat).unwrap();
    db.sync().unwrap();
    let q = FlowVectorQuantization::default();
    let up_q = quantize_direction(&[0, 20], &q);
    let min = up_q.clone();
    let max = up_q.clone();
    let hits = db
        .query_flow_vectors_in_region(min, max, None, QueryOptions::default())
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].edge.id, HyperedgeId(20));
}

#[test]
fn pipeline_lag_delta_merge_matches_full() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let edge = directed_edge(30, node(entity, 1, 0), node(entity, 1, 15));
    db.insert_hyperedge(edge_space, edge).unwrap();
    let q = FlowVectorQuantization::default();
    let dir = quantize_direction(&[0, 15], &q);
    let lagged = db
        .query_flow_vectors_in_region(dir.clone(), dir.clone(), None, QueryOptions::index_only())
        .unwrap();
    assert!(lagged.is_empty());
    let merged = db
        .query_flow_vectors_in_region(dir.clone(), dir, None, QueryOptions::default())
        .unwrap();
    assert_eq!(merged.len(), 1);
    db.sync().unwrap();
    let after = db
        .query_flow_vectors_in_region(
            quantize_direction(&[0, 15], &q),
            quantize_direction(&[0, 15], &q),
            None,
            QueryOptions::index_only(),
        )
        .unwrap();
    assert_eq!(after.len(), 1);
}

#[test]
fn backward_freshness_stale_when_input_superseded() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let rev_u = db
        .insert_hyperedge(
            edge_space,
            directed_edge(40, node(entity, 0, 0), node(entity, 1, 0)),
        )
        .unwrap();
    db.sync().unwrap();
    let mut derived = directed_edge(41, node(entity, 2, 0), node(entity, 3, 0));
    derived.computation = Some(CoreComputation {
        inputs: vec![SubjectPin {
            kind: SubjectKind::Hyperedge,
            space: edge_space,
            identity: SubjectIdentity::Hyperedge(HyperedgeId(40)),
            subject_revision: rev_u,
        }],
    });
    db.insert_hyperedge(edge_space, derived).unwrap();
    db.sync().unwrap();
    let fresh = db
        .check_hyperedge_freshness(edge_space, HyperedgeId(41), None)
        .unwrap();
    assert!(fresh.is_fresh);
    db.insert_hyperedge(
        edge_space,
        directed_edge(40, node(entity, 0, 0), node(entity, 9, 0)),
    )
    .unwrap();
    db.sync().unwrap();
    let stale = db
        .check_hyperedge_freshness(edge_space, HyperedgeId(41), None)
        .unwrap();
    assert!(!stale.is_fresh);
    assert_eq!(stale.inputs[0].status, FreshnessStatus::Stale);
}

#[test]
fn forward_closure_marks_downstream_stale() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let rev_u = db
        .insert_hyperedge(
            edge_space,
            directed_edge(50, node(entity, 0, 0), node(entity, 1, 0)),
        )
        .unwrap();
    db.sync().unwrap();
    let pin = SubjectPin {
        kind: SubjectKind::Hyperedge,
        space: edge_space,
        identity: SubjectIdentity::Hyperedge(HyperedgeId(50)),
        subject_revision: rev_u,
    };
    let mut derived = directed_edge(51, node(entity, 1, 0), node(entity, 2, 0));
    derived.computation = Some(CoreComputation {
        inputs: vec![pin.clone()],
    });
    db.insert_hyperedge(edge_space, derived).unwrap();
    db.sync().unwrap();
    db.insert_hyperedge(
        edge_space,
        directed_edge(50, node(entity, 9, 0), node(entity, 1, 0)),
    )
    .unwrap();
    db.sync().unwrap();
    let targets = db
        .query_stale_downstream(pin, edge_space, 3, None)
        .unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].edge.id, HyperedgeId(51));
    assert!(!targets[0].stale_inputs.is_empty());
}

#[test]
fn anomaly_forbidden_direction_bbox() {
    let (db, _dir, edge_space) = open_db();
    let entity = SpaceId(1);
    let normal = directed_edge(60, node(entity, 0, 10), node(entity, 0, 0));
    let anomaly = directed_edge(61, node(entity, 0, 0), node(entity, 0, 25));
    db.insert_hyperedge(edge_space, normal).unwrap();
    db.insert_hyperedge(edge_space, anomaly).unwrap();
    db.sync().unwrap();
    let q = FlowVectorQuantization::default();
    let up_q = quantize_direction(&[0, 25], &q);
    let forbidden = db
        .query_flow_vectors_in_region(up_q.clone(), up_q, None, QueryOptions::default())
        .unwrap();
    assert_eq!(forbidden.len(), 1);
    assert_eq!(forbidden[0].edge.id, HyperedgeId(61));
}

#[test]
fn bus_idempotency_reopen_rebuilds_flow_index() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    let edge_space = SpaceId(10);
    let entity = SpaceId(1);
    {
        let db = OpenOptions::default().open(path).unwrap();
        db.register_space(infinite_db::infinitedb_core::space::SpaceConfig::new(
            edge_space, "edges", 2,
        ))
        .unwrap();
        db.insert_hyperedge(
            edge_space,
            directed_edge(70, node(entity, 0, 0), node(entity, 0, 8)),
        )
        .unwrap();
        db.sync().unwrap();
    }
    {
        let db = OpenOptions::default().open(path).unwrap();
        db.sync_derivation();
        db.sync().unwrap();
        let rec = db
            .query_flow_vector_for_edge(edge_space, HyperedgeId(70), None)
            .unwrap();
        assert!(rec.is_some());
        assert_eq!(rec.unwrap().vector.delta, vec![0, 8]);
    }
}
