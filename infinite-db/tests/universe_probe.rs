//! P0 probe tests from UNIVERSE_PLAN_PUNCHLIST Appendix B.

use infinite_db::infinitedb_core::address::{RevisionId, SpaceId};
use infinite_db::infinitedb_core::universe::*;
use infinite_db::infinitedb_core::void::VoidOr;

fn sp(i: u64) -> ContainerRef {
    ContainerRef::Space(SpaceId(i))
}

fn edge(a: u64, b: u64, w: i64) -> UniverseEdge {
    UniverseEdge {
        kind: "nexus".into(),
        endpoints: vec![sp(a), sp(b)],
        weight_milli: Some(w),
        valid_from: RevisionId::ZERO,
        valid_to: None,
        projected: false,
        nexus_id: Some(a * 100 + b),
    }
}

#[test]
fn path_of_five_is_one_constellation() {
    let view = UniverseGraphView {
        nodes: vec![sp(1), sp(2), sp(3), sp(4), sp(5)],
        edges: vec![edge(1, 2, 1), edge(2, 3, 1), edge(3, 4, 1), edge(4, 5, 1)],
    };
    if let VoidOr::Known(c) = detect_constellations(&view, RevisionId::ZERO) {
        assert_eq!(c.len(), 1, "a connected path must not split: {c:?}");
    } else {
        panic!("void");
    }
}

#[test]
fn detection_respects_edge_weights() {
    let view = UniverseGraphView {
        nodes: vec![sp(1), sp(2), sp(3), sp(4)],
        edges: vec![edge(1, 2, 1000), edge(3, 4, 1000), edge(2, 3, 1)],
    };
    if let VoidOr::Known(c) = detect_constellations(&view, RevisionId::ZERO) {
        assert_eq!(c.len(), 2, "a weak bridge must not fuse two dense clusters: {c:?}");
    } else {
        panic!("void");
    }
}

#[test]
fn contract_keeps_every_crossing_edge() {
    let view = UniverseGraphView {
        nodes: vec![sp(1), sp(2), sp(3), sp(4)],
        edges: vec![edge(1, 2, 10), edge(1, 3, 5), edge(1, 4, 5)],
    };
    let c = contract(&view, &[sp(1), sp(2)], ContainerRef::Constellation(ConstellationId(9)));
    assert_eq!(
        c.edges.len(),
        2,
        "supernode->3 and supernode->4 must both survive: {:?}",
        c.edges
    );
}

#[test]
fn center_periphery_is_byte_stable() {
    let view = UniverseGraphView {
        nodes: vec![sp(1), sp(2), sp(3), sp(4), sp(5)],
        edges: vec![edge(1, 2, 1), edge(2, 3, 1), edge(3, 4, 1), edge(4, 5, 1)],
    };
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..200 {
        seen.insert(format!("{:?}", center_and_periphery(&view, RevisionId::ZERO)));
    }
    assert_eq!(seen.len(), 1, "center/periphery varied across runs: {seen:#?}");
}

#[test]
fn sorted_edges_include_endpoints_in_key() {
    let a = UniverseEdge {
        kind: "placement".into(),
        endpoints: vec![sp(1), sp(2)],
        weight_milli: Some(1000),
        valid_from: RevisionId::ZERO,
        valid_to: None,
        projected: true,
        nexus_id: None,
    };
    let b = UniverseEdge {
        kind: "placement".into(),
        endpoints: vec![sp(1), sp(3)],
        weight_milli: Some(1000),
        valid_from: RevisionId::ZERO,
        valid_to: None,
        projected: true,
        nexus_id: None,
    };
    let view = UniverseGraphView {
        nodes: vec![sp(1), sp(2), sp(3)],
        edges: vec![b.clone(), a.clone()],
    }
    .sorted();
    assert_eq!(view.edges[0].endpoints, a.endpoints);
    assert_eq!(view.edges[1].endpoints, b.endpoints);
}
