//! Milestone 1 hypergraph integration tests on CRCW `InfiniteDb`.

use std::collections::BTreeMap;

use infinite_db::InfiniteDb;
use infinite_db::infinitedb_core::{
    adapter::AdapterEndpoint,
    hyperedge::{
        Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
        HyperedgeKind,
    },
    hyperedge_codec::{decode_hyperedge, encode_hyperedge, encode_hyperedge_v1_fixture, HyperedgeV1Fixture, EndpointRefV1Fixture},
    kind_catalog::{
        DirectionalityPolicy, KindCatalog, KindDefinition, UnknownKindPolicy,
    },
    query::DirectionFilter,
    space::SpaceConfig,
};
use infinite_db::infinitedb_core::address::{DimensionVector, RevisionId, SpaceId};
use tempfile::TempDir;

fn open_with_edge_space() -> (InfiniteDb, TempDir, SpaceId) {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
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

#[test]
fn hyperedge_insert_query_and_delete() {
    let (db, _dir, edge_space) = open_with_edge_space();
    let entity_space = SpaceId(1);
    let edge = Hyperedge {
        id: HyperedgeId(42),
        kind: HyperedgeKind::new("beam.bears_on"),
        endpoints: vec![
            node(entity_space, 0).with_polarity(EndpointPolarity::Tail),
            node(entity_space, 1).with_polarity(EndpointPolarity::Head),
        ],
        weight_milli: None,
        metadata: BTreeMap::new(),
        valid_from: RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Directed,
        authoring_frame: None,
        computation: None,
    };
    db.insert_hyperedge(edge_space, edge.clone()).unwrap();
    db.sync().unwrap();

    let found = db
        .query_hyperedges_by_kind(edge_space, "beam.bears_on", None)
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].id, HyperedgeId(42));

    db.delete_hyperedge(edge_space, HyperedgeId(42)).unwrap();
    db.sync().unwrap();
    assert!(db.query_hyperedges(edge_space, None).unwrap().is_empty());
}

#[test]
fn endpoint_index_returns_incident_edges_only() {
    let (db, _dir, edge_space) = open_with_edge_space();
    let entity_space = SpaceId(1);
    let shared = node(entity_space, 5);

    for (id, kind) in [(1u64, "link"), (2, "other")] {
        let edge = Hyperedge {
            id: HyperedgeId(id),
            kind: HyperedgeKind::new(kind),
            endpoints: vec![shared.clone(), node(entity_space, id as u32 + 10)],
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
    assert_eq!(incident.len(), 2);
    assert!(incident.iter().any(|e| e.id == HyperedgeId(1)));
    assert!(incident.iter().any(|e| e.id == HyperedgeId(2)));
}

#[test]
fn directed_hub_post_filter() {
    let (db, _dir, edge_space) = open_with_edge_space();
    let entity_space = SpaceId(1);
    let hub = node(entity_space, 100);

    for (id, pol) in [
        (1u64, EndpointPolarity::Tail),
        (2, EndpointPolarity::Tail),
        (3, EndpointPolarity::Head),
    ] {
        let other = node(entity_space, (id + 50) as u32);
        let (tail, head) = if pol == EndpointPolarity::Tail {
            (hub.clone(), other)
        } else {
            (other, hub.clone())
        };
        let edge = Hyperedge {
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
        };
        db.insert_hyperedge(edge_space, edge).unwrap();
    }
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

    let incoming = db
        .query_hyperedges_for_endpoint_directed(
            edge_space,
            &hub,
            None,
            DirectionFilter::Incoming,
        )
        .unwrap();
    assert_eq!(incoming.len(), 1);

    let any = db
        .query_hyperedges_for_endpoint_directed(edge_space, &hub, None, DirectionFilter::Any)
        .unwrap();
    assert_eq!(any.len(), 3);
}

#[test]
fn typed_insert_enforces_oblig_directed() {
    let (db, _dir, edge_space) = open_with_edge_space();
    let mut catalog = KindCatalog::new(UnknownKindPolicy::RejectUnknown);
    catalog.register_edge_kind(
        KindDefinition::new("beam.bears_on")
            .with_directionality(DirectionalityPolicy::ObligateDirected),
    );
    catalog.register_endpoint_role(KindDefinition::new("support"));
    catalog.register_endpoint_role(KindDefinition::new("beam"));

    enum BeamKind {
        BearsOn,
    }
    impl infinite_db::infinitedb_core::adapter::KindLabel for BeamKind {
        fn label(&self) -> &str {
            "beam.bears_on"
        }
    }

    let eps = vec![
        AdapterEndpoint::new("support", SpaceId(1), DimensionVector::new(vec![0, 0]))
            .with_polarity(EndpointPolarity::Tail),
        AdapterEndpoint::new("beam", SpaceId(1), DimensionVector::new(vec![1, 0]))
            .with_polarity(EndpointPolarity::Head),
    ];

    db.insert_hyperedge_typed(
        edge_space,
        HyperedgeId(900),
        BeamKind::BearsOn,
        eps,
        Directionality::Directed,
        None,
        BTreeMap::new(),
        None,
        Some(&catalog),
    )
    .unwrap();

    let err = db
        .insert_hyperedge_typed(
            edge_space,
            HyperedgeId(901),
            BeamKind::BearsOn,
            vec![
                AdapterEndpoint::new("support", SpaceId(1), DimensionVector::new(vec![0, 0])),
                AdapterEndpoint::new("beam", SpaceId(1), DimensionVector::new(vec![2, 0])),
            ],
            Directionality::Undirected,
            None,
            BTreeMap::new(),
            None,
            Some(&catalog),
        )
        .unwrap_err();
    assert!(err.to_string().contains("directionality mismatch"));
}

#[test]
fn v1_payload_decodes_as_undirected_after_insert_via_codec() {
    let (db, _dir, edge_space) = open_with_edge_space();
    let fixture = HyperedgeV1Fixture {
        id: HyperedgeId(77),
        kind: HyperedgeKind::new("legacy"),
        endpoints: vec![
            EndpointRefV1Fixture {
                role: EndpointRole::new("a"),
                space: SpaceId(1),
                node: DimensionVector::new(vec![0, 0]),
            },
            EndpointRefV1Fixture {
                role: EndpointRole::new("b"),
                space: SpaceId(1),
                node: DimensionVector::new(vec![1, 0]),
            },
        ],
        weight_milli: None,
        metadata: BTreeMap::new(),
        valid_from: RevisionId::ZERO,
        valid_to: None,
    };
    let v1_bytes = encode_hyperedge_v1_fixture(&fixture);
    let decoded = decode_hyperedge(&v1_bytes).unwrap();
    assert_eq!(decoded.directionality, Directionality::Undirected);

    db.insert(
        edge_space,
        Hyperedge::storage_point(HyperedgeId(77)),
        v1_bytes,
    )
    .unwrap();
    db.sync().unwrap();

    let fetched = db
        .fetch_hyperedge_by_id(edge_space, HyperedgeId(77), None)
        .unwrap()
        .unwrap();
    assert_eq!(fetched.directionality, Directionality::Undirected);
    let v2 = encode_hyperedge(&fetched).unwrap();
    assert_eq!(v2[0], infinite_db::infinitedb_core::hyperedge_codec::HYPEREDGE_PAYLOAD_V2_TAG);
}

#[test]
fn directed_edge_missing_tail_rejected() {
    let (db, _dir, edge_space) = open_with_edge_space();
    let edge = Hyperedge {
        id: HyperedgeId(1),
        kind: HyperedgeKind::new("bad"),
        endpoints: vec![
            node(SpaceId(1), 0).with_polarity(EndpointPolarity::Head),
            node(SpaceId(1), 1).with_polarity(EndpointPolarity::Head),
        ],
        weight_milli: None,
        metadata: BTreeMap::new(),
        valid_from: RevisionId::ZERO,
        valid_to: None,
        directionality: Directionality::Directed,
        authoring_frame: None,
        computation: None,
    };
    assert!(db.insert_hyperedge(edge_space, edge).is_err());
}
