//! Integration tests for UNIVERSE_PLAN waves 1–4.

use infinite_db::infinitedb_core::address::{DimensionVector, RevisionId, SpaceId};
use infinite_db::infinitedb_core::hyperedge::{Directionality, EndpointPolarity};
use infinite_db::infinitedb_core::nexus::{NexusEdge, NexusEndpoint, NexusId, NexusKind};
use infinite_db::infinitedb_core::placement::Placement;
use infinite_db::infinitedb_core::space::SpaceConfig;
use infinite_db::infinitedb_core::universe::ContainerRef;
use infinite_db::infinitedb_core::void::VoidOr;
use infinite_db::infinitedb_core::ephemeris::{
    EphemerisEntry, EphemerisKind, WandererId,
};
use infinite_db::infinitedb_core::hyperedge::HyperedgeId;
use infinite_db::InfiniteDb;
use tempfile::TempDir;

fn data_space(id: u64, dims: usize) -> SpaceConfig {
    SpaceConfig::new(SpaceId(id), format!("s{id}"), dims)
}

#[test]
fn inv_uni_projected_placement_edges() {
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
    let view = db.universe_graph_view(None).unwrap();
    assert!(view.edges.iter().any(|e| e.projected && e.kind == "placement"));
}

#[test]
fn nexus_write_visible_in_graph() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(data_space(1, 2)).unwrap();
    db.register_space(data_space(2, 2)).unwrap();
    let edge = NexusEdge {
        id: NexusId(10),
        kind: NexusKind::new("mirror"),
        endpoints: vec![
            NexusEndpoint {
                container: ContainerRef::Space(SpaceId(1)),
                region: None,
                polarity: EndpointPolarity::Neutral,
            },
            NexusEndpoint {
                container: ContainerRef::Space(SpaceId(2)),
                region: None,
                polarity: EndpointPolarity::Neutral,
            },
        ],
        weight_milli: Some(100),
        metadata: Default::default(),
        valid_from: RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Undirected,
    };
    db.write_nexus(edge).unwrap();
    db.sync().unwrap();
    let view = db.universe_graph_view(None).unwrap();
    assert!(view.edges.iter().any(|e| !e.projected && e.nexus_id == Some(10)));
}

#[test]
fn inv_uni_void_empty_universe() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(data_space(1, 2)).unwrap();
    db.sync().unwrap();
    let centers = db.universe_center(None).unwrap();
    assert!(matches!(centers, VoidOr::Known(_)));
    let void_dir = TempDir::new().unwrap();
    let empty = InfiniteDb::open(void_dir.path()).unwrap();
    let void_centers = empty.universe_center(None).unwrap();
    assert!(void_centers.is_void());
}

#[test]
fn ephemeris_append_and_presence() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(data_space(1, 2)).unwrap();
    let entry = EphemerisEntry {
        wanderer: WandererId(5),
        kind: EphemerisKind::Observed,
        anchor: SpaceId(1),
        position: DimensionVector::new(vec![3, 4]),
        stamp: RevisionId::ZERO,
        graze_trace: false,
    };
    db.append_ephemeris(entry, HyperedgeId(100)).unwrap();
    db.sync().unwrap();
    let presence = db.wanderer_presence_at(WandererId(5), None).unwrap();
    assert!(presence.known().is_some());
    assert!(db.wanderer_presence_at(WandererId(99), None).unwrap().is_void());
}
