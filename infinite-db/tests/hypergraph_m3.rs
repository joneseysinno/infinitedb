//! Milestone 3 hypergraph integration tests — directional traversal.

use std::collections::BTreeMap;

use infinite_db::InfiniteDb;
use infinite_db::infinitedb_core::{
    endpoint_index::{
        ENDPOINT_INDEX_BITS_PER_DIM, ENDPOINT_INDEX_DIMS, ENDPOINT_INDEX_SPACE,
    },
    hyperedge::{
        Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
        HyperedgeKind,
    },
    space::{EndpointIndexLayout, SpaceConfig},
    traversal::{
        endpoints_equal, TraversalArrival, TraversalDirection, TraversalMode, TraversalSpec,
    },
};
use infinite_db::infinitedb_core::address::{DimensionVector, RevisionId, SpaceId};
use tempfile::TempDir;

fn open_m2_db() -> (InfiniteDb, TempDir, SpaceId) {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let edge_space = SpaceId(10);
    db.register_space(SpaceConfig::new(edge_space, "edges", 2))
        .unwrap();
    db.register_space(
        SpaceConfig::new(
            ENDPOINT_INDEX_SPACE,
            "__endpoint_index__",
            ENDPOINT_INDEX_DIMS,
        )
        .with_bits_per_dim(ENDPOINT_INDEX_BITS_PER_DIM)
        .with_endpoint_index_layout(EndpointIndexLayout::V2PolarityDim),
    )
    .unwrap();
    (db, dir, edge_space)
}

fn open_v1_index_db() -> (InfiniteDb, TempDir, SpaceId) {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let edge_space = SpaceId(10);
    db.register_space(SpaceConfig::new(edge_space, "edges", 2))
        .unwrap();
    db.register_space(
        SpaceConfig::new(
            ENDPOINT_INDEX_SPACE,
            "__endpoint_index__",
            ENDPOINT_INDEX_DIMS,
        )
        .with_bits_per_dim(ENDPOINT_INDEX_BITS_PER_DIM)
        .with_endpoint_index_layout(EndpointIndexLayout::V1Symmetric),
    )
    .unwrap();
    (db, dir, edge_space)
}

fn entity_space() -> SpaceId {
    SpaceId(1)
}

fn node(x: u32) -> EndpointRef {
    EndpointRef::new(
        EndpointRole::new("n"),
        entity_space(),
        DimensionVector::new(vec![x, 0]),
    )
}

fn directed_edge(id: u64, kind: &str, tails: Vec<EndpointRef>, heads: Vec<EndpointRef>) -> Hyperedge {
    let mut endpoints = Vec::new();
    for t in tails {
        endpoints.push(t.with_polarity(EndpointPolarity::Tail));
    }
    for h in heads {
        endpoints.push(h.with_polarity(EndpointPolarity::Head));
    }
    Hyperedge {
        id: HyperedgeId(id),
        kind: HyperedgeKind::new(kind),
        endpoints,
        weight_milli: None,
        metadata: BTreeMap::new(),
        valid_from: RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Directed,
        authoring_frame: None,
        computation: None,
    }
}

fn undirected_edge(id: u64, endpoints: Vec<EndpointRef>) -> Hyperedge {
    Hyperedge {
        id: HyperedgeId(id),
        kind: HyperedgeKind::new("link"),
        endpoints,
        weight_milli: None,
        metadata: BTreeMap::new(),
        valid_from: RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Undirected,
        authoring_frame: None,
        computation: None,
    }
}

fn level_of(
    result: &infinite_db::infinitedb_core::traversal::TraversalResult,
    ep: &EndpointRef,
) -> Option<usize> {
    result.level_of(ep)
}

fn insert_chain(db: &InfiniteDb, edge_space: SpaceId, nodes: &[u32]) {
    for w in nodes.windows(2) {
        let id = w[0] as u64 * 100 + w[1] as u64;
        let edge = directed_edge(
            id,
            "flow",
            vec![node(w[0])],
            vec![node(w[1])],
        );
        db.insert_hyperedge(edge_space, edge).unwrap();
    }
    db.sync().unwrap();
}

#[test]
fn forward_chain_levels() {
    let (db, _dir, edge_space) = open_m2_db();
    insert_chain(&db, edge_space, &[1, 2, 3]);

    let spec = TraversalSpec {
        start: node(1),
        edge_space,
        max_depth: 10,
        direction: TraversalDirection::Forward,
        mode: TraversalMode::Reachability,
        follow_kinds: None,
        as_of: None,
    };
    let result = db.traverse_hypergraph(&spec).unwrap();
    assert_eq!(level_of(&result, &node(1)), Some(0));
    assert_eq!(level_of(&result, &node(2)), Some(1));
    assert_eq!(level_of(&result, &node(3)), Some(2));
}

#[test]
fn backward_chain_levels() {
    let (db, _dir, edge_space) = open_m2_db();
    insert_chain(&db, edge_space, &[1, 2, 3]);

    let spec = TraversalSpec {
        start: node(3),
        edge_space,
        max_depth: 10,
        direction: TraversalDirection::Backward,
        mode: TraversalMode::Reachability,
        follow_kinds: None,
        as_of: None,
    };
    let result = db.traverse_hypergraph(&spec).unwrap();
    assert_eq!(level_of(&result, &node(3)), Some(0));
    assert_eq!(level_of(&result, &node(2)), Some(1));
    assert_eq!(level_of(&result, &node(1)), Some(2));
}

#[test]
fn both_mode_symmetric_undirected() {
    let (db, _dir, edge_space) = open_m2_db();
    let a = node(1);
    let b = node(2);
    let c = node(3);
    db.insert_hyperedge(
        edge_space,
        undirected_edge(1, vec![a.clone(), b.clone()]),
    )
    .unwrap();
    db.insert_hyperedge(edge_space, undirected_edge(2, vec![b, c.clone()]))
        .unwrap();
    db.sync().unwrap();

    let spec = TraversalSpec {
        start: a,
        edge_space,
        max_depth: 10,
        direction: TraversalDirection::Both,
        mode: TraversalMode::Reachability,
        follow_kinds: None,
        as_of: None,
    };
    let result = db.traverse_hypergraph(&spec).unwrap();
    assert_eq!(level_of(&result, &node(1)), Some(0));
    assert_eq!(level_of(&result, &node(2)), Some(1));
    assert_eq!(level_of(&result, &c), Some(2));
}

#[test]
fn co_tail_reported_not_expanded() {
    let (db, _dir, edge_space) = open_m2_db();
    let t1 = node(1);
    let t2 = node(2);
    let h = node(3);
    db.insert_hyperedge(
        edge_space,
        directed_edge(1, "flow", vec![t1.clone(), t2.clone()], vec![h.clone()]),
    )
    .unwrap();
    db.sync().unwrap();

    let spec = TraversalSpec {
        start: t1.clone(),
        edge_space,
        max_depth: 10,
        direction: TraversalDirection::Forward,
        mode: TraversalMode::Reachability,
        follow_kinds: None,
        as_of: None,
    };
    let result = db.traverse_hypergraph(&spec).unwrap();

    let t2_node = result
        .nodes
        .iter()
        .find(|n| endpoints_equal(&n.endpoint, &t2))
        .expect("co-tail reported");
    assert_eq!(t2_node.arrival, TraversalArrival::CoCause);
    assert_eq!(t2_node.level, 0);

    assert_eq!(level_of(&result, &h), Some(1));

    // T2 was not expanded — no outgoing walk from T2 unless separately reached.
    let t2_outgoing = TraversalSpec {
        start: t2,
        edge_space,
        max_depth: 1,
        direction: TraversalDirection::Forward,
        mode: TraversalMode::Reachability,
        follow_kinds: None,
        as_of: None,
    };
    let from_t2 = db.traverse_hypergraph(&t2_outgoing).unwrap();
    assert_eq!(level_of(&from_t2, &h), Some(1));
}

#[test]
fn b_connectivity_conjunctive() {
    let (db, _dir, edge_space) = open_m2_db();
    let t1 = node(1);
    let t2 = node(2);
    let h = node(3);
    let conj = directed_edge(1, "load", vec![t1.clone(), t2.clone()], vec![h.clone()]);

    db.insert_hyperedge(edge_space, conj).unwrap();
    db.sync().unwrap();

    let reach_spec = TraversalSpec {
        start: t1.clone(),
        edge_space,
        max_depth: 10,
        direction: TraversalDirection::Forward,
        mode: TraversalMode::Reachability,
        follow_kinds: None,
        as_of: None,
    };
    let reach = db.traverse_hypergraph(&reach_spec).unwrap();
    assert_eq!(level_of(&reach, &h), Some(1));

    let bconn_spec = TraversalSpec {
        start: t1,
        edge_space,
        max_depth: 10,
        direction: TraversalDirection::Forward,
        mode: TraversalMode::BConnectivity,
        follow_kinds: None,
        as_of: None,
    };
    let bconn = db.traverse_hypergraph(&bconn_spec).unwrap();
    assert!(level_of(&bconn, &h).is_none());
}

#[test]
fn kind_filter() {
    let (db, _dir, edge_space) = open_m2_db();
    let a = node(1);
    let b = node(2);
    db.insert_hyperedge(
        edge_space,
        directed_edge(1, "flow", vec![a.clone()], vec![b.clone()]),
    )
    .unwrap();
    db.insert_hyperedge(
        edge_space,
        directed_edge(2, "other", vec![b.clone()], vec![node(3)]),
    )
    .unwrap();
    db.sync().unwrap();

    let spec = TraversalSpec {
        start: a,
        edge_space,
        max_depth: 10,
        direction: TraversalDirection::Forward,
        mode: TraversalMode::Reachability,
        follow_kinds: Some(vec![HyperedgeKind::new("flow")]),
        as_of: None,
    };
    let result = db.traverse_hypergraph(&spec).unwrap();
    assert_eq!(result.edges.len(), 1);
    assert_eq!(result.edges[0].kind.as_str(), "flow");
    assert_eq!(level_of(&result, &b), Some(1));
    assert!(level_of(&result, &node(3)).is_none());
}

#[test]
fn max_depth_truncation() {
    let (db, _dir, edge_space) = open_m2_db();
    insert_chain(&db, edge_space, &[1, 2, 3, 4]);

    let spec = TraversalSpec {
        start: node(1),
        edge_space,
        max_depth: 1,
        direction: TraversalDirection::Forward,
        mode: TraversalMode::Reachability,
        follow_kinds: None,
        as_of: None,
    };
    let result = db.traverse_hypergraph(&spec).unwrap();
    assert_eq!(level_of(&result, &node(1)), Some(0));
    assert_eq!(level_of(&result, &node(2)), Some(1));
    assert!(level_of(&result, &node(3)).is_none());
}

#[test]
fn acyclic_dag_and_cycle() {
    let (db, _dir, edge_space) = open_m2_db();
    let a = node(1);
    let b = node(2);
    let c = node(3);
    db.insert_hyperedge(
        edge_space,
        directed_edge(1, "flow", vec![a.clone()], vec![b.clone()]),
    )
    .unwrap();
    db.insert_hyperedge(
        edge_space,
        directed_edge(2, "flow", vec![b.clone()], vec![c.clone()]),
    )
    .unwrap();
    db.sync().unwrap();
    assert!(db.check_hypergraph_acyclic(edge_space, &[], None).unwrap());

    db.insert_hyperedge(
        edge_space,
        directed_edge(3, "flow", vec![c.clone()], vec![a.clone()]),
    )
    .unwrap();
    db.sync().unwrap();
    assert!(!db.check_hypergraph_acyclic(edge_space, &[], None).unwrap());
}

#[test]
fn v1_index_layout_traversal() {
    let (db, _dir, edge_space) = open_v1_index_db();
    insert_chain(&db, edge_space, &[10, 20, 30]);

    let spec = TraversalSpec {
        start: node(10),
        edge_space,
        max_depth: 10,
        direction: TraversalDirection::Forward,
        mode: TraversalMode::Reachability,
        follow_kinds: None,
        as_of: None,
    };
    let result = db.traverse_hypergraph(&spec).unwrap();
    assert_eq!(level_of(&result, &node(30)), Some(2));
}

#[test]
fn inactive_edge_excluded() {
    let (db, _dir, edge_space) = open_m2_db();
    let a = node(1);
    let b = node(2);
    let mut edge = directed_edge(1, "flow", vec![a.clone()], vec![b.clone()]);
    edge.valid_to = Some(RevisionId::legacy(0));
    db.insert_hyperedge(edge_space, edge).unwrap();
    db.sync().unwrap();

    let spec = TraversalSpec {
        start: a,
        edge_space,
        max_depth: 10,
        direction: TraversalDirection::Forward,
        mode: TraversalMode::Reachability,
        follow_kinds: None,
        as_of: None,
    };
    let result = db.traverse_hypergraph(&spec).unwrap();
    assert!(result.edges.is_empty());
    assert_eq!(result.nodes.len(), 1);
}
