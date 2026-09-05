//! Additional invariant tests (UNIVERSE_PLAN_PUNCHLIST P2).

use infinite_db::infinitedb_core::address::{DimensionVector, RevisionId, SpaceId};
use infinite_db::infinitedb_core::ephemeris::{EphemerisEntry, EphemerisKind, WandererId};
use infinite_db::infinitedb_core::hyperedge::{Directionality, EndpointPolarity, HyperedgeId};
use infinite_db::infinitedb_core::nexus::{
    ConstellationPin, NexusEdge, NexusEndpoint, NexusId, NexusKind,
};
use infinite_db::infinitedb_core::space::SpaceConfig;
use infinite_db::infinitedb_core::universe::{
    ContainerRef, ConstellationId, UniverseRatioError, mean_eccentricity,
};
use infinite_db::EngineError;
use infinite_db::InfiniteDb;
use tempfile::TempDir;

fn data_space(id: u64, dims: usize) -> SpaceConfig {
    SpaceConfig::new(SpaceId(id), format!("s{id}"), dims)
}

#[test]
fn inv_nex_endpoint_exists_blocks_space_removal() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(data_space(1, 2)).unwrap();
    db.register_space(data_space(2, 2)).unwrap();
    let edge = NexusEdge {
        id: NexusId(5),
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
        weight_milli: Some(10),
        metadata: Default::default(),
        valid_from: RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Undirected,
    };
    db.write_nexus(edge).unwrap();
    db.sync().unwrap();
    let err = db.remove_space(SpaceId(1)).unwrap_err();
    assert!(matches!(
        err,
        EngineError::RegistrySpace(
            infinite_db::infinitedb_core::space::SpaceError::NexusReferenced(_)
        )
    ));
}

#[test]
fn inv_nex_append_only_tombstone_as_of() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(data_space(1, 2)).unwrap();
    db.register_space(data_space(2, 2)).unwrap();
    let edge = NexusEdge {
        id: NexusId(7),
        kind: NexusKind::new("link"),
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
        weight_milli: None,
        metadata: Default::default(),
        valid_from: RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Undirected,
    };
    let rev = db.write_nexus(edge).unwrap();
    db.sync().unwrap();
    db.delete_nexus(NexusId(7)).unwrap();
    db.sync().unwrap();
    let steady = db.universe_graph_view(None).unwrap();
    assert!(!steady.edges.iter().any(|e| e.nexus_id == Some(7)));
    let historical = db.universe_graph_view(Some(rev)).unwrap();
    assert!(historical.edges.iter().any(|e| e.nexus_id == Some(7)));
}

#[test]
fn inv_uni_void_ratio_statistics() {
    let dir = TempDir::new().unwrap();
    let empty = InfiniteDb::open(dir.path()).unwrap();
    let view = empty.universe_graph_view(None).unwrap();
    assert_eq!(
        mean_eccentricity(&view, RevisionId::ZERO),
        Err(UniverseRatioError::MemberVoid)
    );
    let dir2 = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir2.path()).unwrap();
    db.register_space(data_space(1, 2)).unwrap();
    db.sync().unwrap();
    let view = db.universe_graph_view(None).unwrap();
    assert_eq!(
        mean_eccentricity(&view, RevisionId::ZERO),
        Err(UniverseRatioError::UndefinedSingleton)
    );
}

#[test]
fn pin_roundtrip_embedded() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(data_space(1, 2)).unwrap();
    db.register_space(data_space(2, 2)).unwrap();
    let pin = ConstellationPin {
        id: ConstellationId(42),
        name: "pair".into(),
        members: vec![
            ContainerRef::Space(SpaceId(1)),
            ContainerRef::Space(SpaceId(2)),
        ],
    };
    db.pin_constellation(pin.clone(), NexusId(100)).unwrap();
    db.sync().unwrap();
    let pins = db.pinned_constellations(None).unwrap();
    assert!(pins.iter().any(|p| p.id == ConstellationId(42)));
}

#[test]
fn inv_epi_append_only_observe_after_projected() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    db.register_space(data_space(1, 2)).unwrap();
    let projected = EphemerisEntry {
        wanderer: WandererId(1),
        kind: EphemerisKind::Projected {
            predicted_for: RevisionId::legacy(100),
        },
        anchor: SpaceId(1),
        position: DimensionVector::new(vec![1, 1]),
        stamp: RevisionId::ZERO,
        graze_trace: false,
    };
    let rev_p = db.append_ephemeris(projected, HyperedgeId(1)).unwrap();
    let observed = EphemerisEntry {
        wanderer: WandererId(1),
        kind: EphemerisKind::Observed,
        anchor: SpaceId(1),
        position: DimensionVector::new(vec![1, 1]),
        stamp: RevisionId::ZERO,
        graze_trace: false,
    };
    db.append_ephemeris(observed, HyperedgeId(2)).unwrap();
    db.sync().unwrap();
    let traj = db.ephemeris_of(WandererId(1), None).unwrap();
    assert_eq!(traj.len(), 2);
    let at_p = db.ephemeris_of(WandererId(1), Some(rev_p)).unwrap();
    assert_eq!(at_p.len(), 1);
    assert!(matches!(at_p[0].kind, EphemerisKind::Projected { .. }));
}
