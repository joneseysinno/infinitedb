//! Milestone 2 hypergraph integration tests — polarity-dimension endpoint index.

use std::collections::BTreeMap;

use infinite_db::InfiniteDb;
use infinite_db::infinitedb_core::{
    endpoint_index::{
        endpoint_index_point_v2, index_record_layout, polarity_coord,
        ENDPOINT_INDEX_BITS_PER_DIM, ENDPOINT_INDEX_DIMS, ENDPOINT_INDEX_SPACE,
    },
    hyperedge::{
        Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
        HyperedgeKind,
    },
    query::DirectionFilter,
    space::{EndpointIndexLayout, SpaceConfig},
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
    assert_eq!(
        db.endpoint_index_layout(),
        EndpointIndexLayout::V2PolarityDim
    );
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
fn v2_index_point_layout_integration() {
    let ep = node(SpaceId(1), 42).with_polarity(EndpointPolarity::Head);
    let prefix_len = 4;
    let point = endpoint_index_point_v2(&ep, HyperedgeId(0x0102_0304_0506_0708));
    assert_eq!(point.coords[prefix_len], polarity_coord(EndpointPolarity::Head));
    assert_eq!(point.coords[prefix_len + 1], 0x0506_0708);
    assert_eq!(point.coords[prefix_len + 2], 0x0102_0304);
}

#[test]
fn directional_incidence_index_resident_hub() {
    let (db, _dir, edge_space) = open_m2_db();
    let entity_space = SpaceId(1);
    let hub = node(entity_space, 200);

    for id in 1u64..=120 {
        let other = node(entity_space, (id + 500) as u32);
        let edge = if id % 3 == 0 {
            directed_edge(id, other, hub.clone())
        } else {
            directed_edge(id, hub.clone(), other)
        };
        db.insert_hyperedge(edge_space, edge).unwrap();
    }
    db.sync().unwrap();

    let outgoing = db
        .count_incident_edges_for_endpoint_directed(
            &hub,
            None,
            DirectionFilter::Outgoing,
        )
        .unwrap();
    let incoming = db
        .count_incident_edges_for_endpoint_directed(
            &hub,
            None,
            DirectionFilter::Incoming,
        )
        .unwrap();
    assert_eq!(outgoing, 80);
    assert_eq!(incoming, 40);

    let edges_out = db
        .query_hyperedges_for_endpoint_directed(
            edge_space,
            &hub,
            None,
            DirectionFilter::Outgoing,
        )
        .unwrap();
    assert_eq!(edges_out.len(), 80);
}

#[test]
fn symmetric_incidence_matches_v1_semantics() {
    let (db, _dir, edge_space) = open_m2_db();
    let entity_space = SpaceId(1);
    let shared = node(entity_space, 9);

    for id in 1u64..=4 {
        let edge = Hyperedge {
            id: HyperedgeId(id),
            kind: HyperedgeKind::new("link"),
            endpoints: vec![shared.clone(), node(entity_space, (id + 20) as u32)],
            weight_milli: None,
            metadata: BTreeMap::new(),
            valid_from: RevisionId::ZERO,
            valid_to: None,
            directionality: Directionality::Undirected,
            authoring_frame: None,
        computation: None,
        };
        db.insert_hyperedge(edge_space, edge).unwrap();
    }
    db.sync().unwrap();

    let incident = db
        .query_hyperedges_for_endpoint(edge_space, &shared, None)
        .unwrap();
    assert_eq!(incident.len(), 4);
    assert_eq!(
        db.count_incident_edges_for_endpoint(&shared, None).unwrap(),
        4
    );
}

#[test]
fn delete_tombstones_polarity_specific_rows() {
    let (db, _dir, edge_space) = open_m2_db();
    let entity_space = SpaceId(1);
    let hub = node(entity_space, 50);
    let edge = directed_edge(1, hub.clone(), node(entity_space, 60));
    db.insert_hyperedge(edge_space, edge).unwrap();
    db.sync().unwrap();

    let index_before = db.query(ENDPOINT_INDEX_SPACE, None).unwrap();
    let live_before = index_before.iter().filter(|r| !r.tombstone).count();
    assert_eq!(live_before, 2);

    db.delete_hyperedge(edge_space, HyperedgeId(1)).unwrap();
    db.sync().unwrap();

    let index_after = db.query(ENDPOINT_INDEX_SPACE, None).unwrap();
    let live_after = index_after.iter().filter(|r| !r.tombstone).count();
    assert_eq!(live_after, 0);
    assert!(db.query_hyperedges(edge_space, None).unwrap().is_empty());
}

#[test]
fn dual_layout_mixed_era() {
    let (db, _dir, edge_space) = open_v1_index_db();
    let entity_space = SpaceId(1);
    let hub = node(entity_space, 300);

    db.insert_hyperedge(
        edge_space,
        directed_edge(1, hub.clone(), node(entity_space, 301)),
    )
    .unwrap();
    db.sync().unwrap();

    db.upgrade_endpoint_index_layout().unwrap();
    db.insert_hyperedge(
        edge_space,
        directed_edge(2, hub.clone(), node(entity_space, 302)),
    )
    .unwrap();
    db.sync().unwrap();

    let outgoing = db
        .query_hyperedges_for_endpoint_directed(
            edge_space,
            &hub,
            None,
            DirectionFilter::Outgoing,
        )
        .unwrap();
    assert_eq!(outgoing.len(), 2);

    let index_records = db.query(ENDPOINT_INDEX_SPACE, None).unwrap();
    let v1_rows = index_records
        .iter()
        .filter(|r| !r.tombstone && index_record_layout(&r.data) == EndpointIndexLayout::V1Symmetric)
        .count();
    let v2_rows = index_records
        .iter()
        .filter(|r| !r.tombstone && index_record_layout(&r.data) == EndpointIndexLayout::V2PolarityDim)
        .count();
    assert_eq!(v1_rows, 2);
    assert_eq!(v2_rows, 2);
}

#[test]
fn compaction_lazy_rewrite_migrates_v1_rows() {
    let (db, _dir, edge_space) = open_v1_index_db();
    let entity_space = SpaceId(1);
    let hub = node(entity_space, 400);

    db.insert_hyperedge(
        edge_space,
        directed_edge(10, hub.clone(), node(entity_space, 401)),
    )
    .unwrap();
    db.sync().unwrap();

    db.upgrade_endpoint_index_layout().unwrap();
    db.compact_endpoint_index(&[edge_space]).unwrap();
    db.sync().unwrap();

    let index_records = db.query(ENDPOINT_INDEX_SPACE, None).unwrap();
    let live_v1 = index_records
        .iter()
        .filter(|r| !r.tombstone && index_record_layout(&r.data) == EndpointIndexLayout::V1Symmetric)
        .count();
    let live_v2 = index_records
        .iter()
        .filter(|r| !r.tombstone && index_record_layout(&r.data) == EndpointIndexLayout::V2PolarityDim)
        .count();
    assert_eq!(live_v1, 0);
    assert_eq!(live_v2, 2);

    assert!(
        index_records
            .iter()
            .all(|r| index_record_layout(&r.data) != EndpointIndexLayout::V1Symmetric || r.tombstone),
        "no live V1-layout index rows should remain after rewrite"
    );
}

#[test]
fn degree_count_matches_incidence_without_extra_fetch() {
    let (db, _dir, edge_space) = open_m2_db();
    let entity_space = SpaceId(1);
    let hub = node(entity_space, 77);
    for id in 1u64..=5 {
        db.insert_hyperedge(
            edge_space,
            directed_edge(id, hub.clone(), node(entity_space, (id + 10) as u32)),
        )
        .unwrap();
    }
    db.sync().unwrap();

    let count = db
        .count_incident_edges_for_endpoint_directed(
            &hub,
            None,
            DirectionFilter::Outgoing,
        )
        .unwrap();
    let edges = db
        .query_hyperedges_for_endpoint_directed(
            edge_space,
            &hub,
            None,
            DirectionFilter::Outgoing,
        )
        .unwrap();
    assert_eq!(count, edges.len());
    assert_eq!(count, 5);
}

#[test]
fn upgrade_endpoint_index_layout_persists() {
    let (db, dir, _edge_space) = open_v1_index_db();
    db.upgrade_endpoint_index_layout().unwrap();
    drop(db);

    let db2 = InfiniteDb::open(dir.path()).unwrap();
    assert_eq!(
        db2.endpoint_index_layout(),
        EndpointIndexLayout::V2PolarityDim
    );
}
