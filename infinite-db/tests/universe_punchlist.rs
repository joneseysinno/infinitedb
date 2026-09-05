//! Remaining UNIVERSE_PLAN_PUNCHLIST P0/P1/P2 coverage.

use std::collections::{BTreeMap, BTreeSet};

use infinite_db::infinitedb_core::address::{DimensionVector, RevisionId, SpaceId};
use infinite_db::infinitedb_core::ephemeris::{graze_trace_nexus_id, WandererId};
use infinite_db::infinitedb_core::hyperedge::{
    Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
    HyperedgeKind,
};
use infinite_db::infinitedb_core::placement::Placement;
use infinite_db::infinitedb_core::space::SpaceConfig;
use infinite_db::infinitedb_core::universe::{
    GRAZE_WEIGHT_MILLI, detect_constellations, UniverseEdge, UniverseGraphView, ContainerRef,
};
use infinite_db::{InfiniteDb, NexusTransferIntent, NexusTransferPhase};
use tempfile::TempDir;

fn assert_stable<T: std::fmt::Debug>(n: usize, mut build: impl FnMut() -> T) {
    let mut seen = BTreeSet::new();
    for _ in 0..n {
        seen.insert(format!("{:?}", build()));
    }
    assert_eq!(seen.len(), 1, "unstable across rebuilds: {seen:#?}");
}

fn data_space(id: u64, dims: usize) -> SpaceConfig {
    SpaceConfig::new(SpaceId(id), format!("s{id}"), dims)
}

fn node(space: SpaceId, x: u32, y: u32) -> EndpointRef {
    EndpointRef::new(
        EndpointRole::new("n"),
        space,
        DimensionVector::new(vec![x, y]),
    )
}

fn edge(id: u64, _space: SpaceId, a: EndpointRef, b: EndpointRef) -> Hyperedge {
    Hyperedge {
        id: HyperedgeId(id),
        kind: HyperedgeKind::new("link"),
        endpoints: vec![
            a.with_polarity(EndpointPolarity::Neutral),
            b.with_polarity(EndpointPolarity::Neutral),
        ],
        weight_milli: None,
        metadata: BTreeMap::new(),
        valid_from: RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Undirected,
        authoring_frame: None,
        computation: None,
    }
}

#[test]
fn placement_edge_order_is_stable_across_instances() {
    let mut seen = BTreeSet::new();
    for _ in 0..12 {
        let dir = TempDir::new().unwrap();
        let db = InfiniteDb::open(dir.path()).unwrap();
        db.register_space(SpaceConfig::new(SpaceId(1), "root", 2))
            .unwrap();
        for i in 2..=12u64 {
            db.register_space(
                SpaceConfig::new(SpaceId(i), format!("c{i}"), 2)
                    .with_parent(SpaceId(1))
                    .with_placement(Placement::axis_aligned(vec![0, 0], 1, 1, vec![64, 64])),
            )
            .unwrap();
        }
        db.sync().unwrap();
        let v = db.universe_graph_view(None).unwrap();
        let order: Vec<_> = v
            .edges
            .iter()
            .filter(|e| e.projected)
            .map(|e| format!("{:?}", e.endpoints))
            .collect();
        seen.insert(order.join(","));
    }
    assert_eq!(seen.len(), 1, "placement edge order varied");
}

#[test]
fn graze_nexus_ids_distinct_for_transposed_pairs() {
    let region = (
        DimensionVector::new(vec![0, 0]),
        DimensionVector::new(vec![1, 1]),
    );
    let a = graze_trace_nexus_id(WandererId(1), SpaceId(2), RevisionId::legacy(9), &region);
    let b = graze_trace_nexus_id(WandererId(2), SpaceId(1), RevisionId::legacy(9), &region);
    assert_ne!(a, b);
}

#[test]
fn inv_eph_unclustered_zero_weight_graze() {
    let view = UniverseGraphView {
        nodes: vec![
            ContainerRef::Space(SpaceId(1)),
            ContainerRef::Space(SpaceId(2)),
        ],
        edges: vec![UniverseEdge {
            kind: "graze".into(),
            endpoints: vec![
                ContainerRef::Space(SpaceId(1)),
                ContainerRef::Space(SpaceId(2)),
            ],
            weight_milli: Some(GRAZE_WEIGHT_MILLI),
            valid_from: RevisionId::ZERO,
            valid_to: None,
            projected: false,
            nexus_id: Some(1),
        }],
    };
    let clusters = detect_constellations(&view, RevisionId::ZERO)
        .known()
        .unwrap();
    assert_eq!(clusters.len(), 2, "zero-weight graze must not cluster: {clusters:?}");
}

#[test]
fn thousand_hyperedges_on_one_endpoint_all_survive() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(data_space(1, 2)).unwrap();
    db.register_space(data_space(2, 2).with_bits_per_dim(32))
        .unwrap();
    let ep = node(SpaceId(1), 1, 1);
    for i in 1..=1000u64 {
        let other = node(SpaceId(1), 2, 2);
        db.insert_hyperedge(SpaceId(2), edge(i, SpaceId(2), ep.clone(), other))
            .unwrap();
    }
    db.sync().unwrap();
    db.sync_derivation();
    let found = db
        .query_hyperedges_for_endpoint(SpaceId(2), &ep, None)
        .unwrap();
    assert_eq!(found.len(), 1000);
}

#[test]
fn hyperedge_ids_72_and_200_both_survive() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(data_space(1, 2)).unwrap();
    let ep = node(SpaceId(1), 0, 0);
    db.insert_hyperedge(
        SpaceId(1),
        edge(72, SpaceId(1), ep.clone(), node(SpaceId(1), 1, 0)),
    )
    .unwrap();
    db.insert_hyperedge(
        SpaceId(1),
        edge(200, SpaceId(1), ep.clone(), node(SpaceId(1), 2, 0)),
    )
    .unwrap();
    db.sync().unwrap();
    db.sync_derivation();
    let found = db
        .query_hyperedges_for_endpoint(SpaceId(1), &ep, None)
        .unwrap();
    let ids: BTreeSet<_> = found.iter().map(|e| e.id.0).collect();
    assert!(ids.contains(&72) && ids.contains(&200), "{ids:?}");
}

#[test]
fn coords_72_and_200_have_disjoint_incidence() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(data_space(1, 2)).unwrap();
    let a = node(SpaceId(1), 72, 0);
    let b = node(SpaceId(1), 200, 0);
    db.insert_hyperedge(SpaceId(1), edge(1, SpaceId(1), a.clone(), node(SpaceId(1), 0, 1)))
        .unwrap();
    db.insert_hyperedge(SpaceId(1), edge(2, SpaceId(1), b.clone(), node(SpaceId(1), 0, 2)))
        .unwrap();
    db.sync().unwrap();
    db.sync_derivation();
    let ia = db.query_hyperedges_for_endpoint(SpaceId(1), &a, None).unwrap();
    let ib = db.query_hyperedges_for_endpoint(SpaceId(1), &b, None).unwrap();
    assert_eq!(ia.len(), 1);
    assert_eq!(ib.len(), 1);
    assert_ne!(ia[0].id, ib[0].id);
}

#[test]
fn transfer_resumes_from_injected_copying_phase() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(data_space(1, 2)).unwrap();
    db.register_space(data_space(2, 2)).unwrap();
    for i in 0..4u32 {
        db.insert(SpaceId(1), DimensionVector::new(vec![i, 0]), vec![i as u8])
            .unwrap();
    }
    db.sync().unwrap();
    let mut records = db.query(SpaceId(1), None).unwrap();
    records.retain(|r| !r.tombstone);
    records.sort_by(|a, b| a.address.point.coords.cmp(&b.address.point.coords));
    let mut intent = NexusTransferIntent::new(7, SpaceId(1), SpaceId(2));
    intent.phase = NexusTransferPhase::Copying;
    intent.captured_addresses = records.iter().map(|r| r.address.clone()).collect();
    intent.copy_cursor = 0;
    drop(db);

    let bytes = bincode::encode_to_vec(
        &std::collections::BTreeMap::from([(7u64, intent)]),
        bincode::config::standard(),
    )
    .unwrap();
    std::fs::write(dir.path().join("meta").join("nexus_transfers.bin"), bytes).unwrap();

    let db = InfiniteDb::open(dir.path()).unwrap();
    let status = db.nexus_transfer_status(7).unwrap();
    assert_eq!(status.phase, NexusTransferPhase::Complete);
    let target = db.query(SpaceId(2), None).unwrap();
    assert_eq!(target.iter().filter(|r| !r.tombstone).count(), 4);
}

#[test]
fn universe_graph_rebuilds_are_byte_stable() {
    assert_stable(8, || {
        let dir = TempDir::new().unwrap();
        let db = InfiniteDb::open(dir.path()).unwrap();
        db.register_space(data_space(1, 2)).unwrap();
        db.register_space(
            data_space(2, 2)
                .with_parent(SpaceId(1))
                .with_placement(Placement::axis_aligned(vec![0, 0], 1, 1, vec![64, 64])),
        )
        .unwrap();
        db.sync().unwrap();
        db.universe_graph_view(None).unwrap()
    });
}

#[test]
fn ephemeris_frame_queries_testimony() {
    use infinite_db::infinitedb_core::ephemeris::{EphemerisEntry, EphemerisKind, EPHEMERIS_SPACE};
    use infinite_db::{AssertionScope, FrameQuery, FrameRegisterRequest};

    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(data_space(1, 2)).unwrap();
    let entry = EphemerisEntry {
        wanderer: WandererId(3),
        kind: EphemerisKind::Observed,
        anchor: SpaceId(1),
        position: DimensionVector::new(vec![4, 5]),
        stamp: RevisionId::ZERO,
        graze_trace: false,
    };
    db.append_ephemeris(entry, HyperedgeId(9)).unwrap();
    db.sync().unwrap();
    let frame = db
        .register_frame(FrameRegisterRequest {
            id: None,
            name: "ephemeris".into(),
            assertion_scope: AssertionScope::Spaces(vec![EPHEMERIS_SPACE]),
            judgment_overlay: vec![],
            default_as_of: None,
        })
        .unwrap();
    let q = FrameQuery {
        frame_id: frame.id,
        testimony_space: EPHEMERIS_SPACE,
        min: DimensionVector::new(vec![0, 0]),
        max: DimensionVector::new(vec![u32::MAX, u32::MAX]),
        as_of: None,
        version_vector: None,
        options: Default::default(),
    };
    let rows = db.query_hyperedges_in_frame(q).unwrap();
    assert!(!rows.is_empty());
}
