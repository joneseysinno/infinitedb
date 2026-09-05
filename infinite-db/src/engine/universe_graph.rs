//! Universe graph assembly from registry + Nexus assertions (U3).

use crate::infinitedb_core::{
    address::RevisionId,
    nexus::{ConstellationPin, NEXUS_KIND_CONSTELLATION_PIN},
    nexus_codec::decode_nexus,
    space::SpaceRegistry,
    universe::{
        ContainerRef, UniverseEdge, UniverseGraphView, DEFAULT_PLACEMENT_WEIGHT_MILLI,
        PLACEMENT_EDGE_KIND, is_universe_member,
    },
};

/// Build the ambient universe graph at `as_of`.
pub fn assemble_universe_graph(
    registry: &SpaceRegistry,
    nexus_records: &[(RevisionId, Vec<u8>, bool)],
    as_of: RevisionId,
) -> UniverseGraphView {
    let mut nodes: Vec<ContainerRef> = registry
        .space_ids()
        .into_iter()
        .filter_map(|id| {
            let cfg = registry.get(id)?;
            if is_universe_member(id, cfg) {
                Some(ContainerRef::Space(id))
            } else {
                None
            }
        })
        .collect();

    let mut pinned: Vec<ConstellationPin> = Vec::new();
    let mut edges: Vec<UniverseEdge> = Vec::new();

    for (child_id, child_cfg) in registry.space_ids().into_iter().filter_map(|id| {
        registry.get(id).map(|c| (id, c))
    }) {
        if !is_universe_member(child_id, child_cfg) {
            continue;
        }
        if let Some(parent_id) = child_cfg.parent {
            if registry.get(parent_id).is_some() {
                edges.push(UniverseEdge {
                    kind: PLACEMENT_EDGE_KIND.into(),
                    endpoints: vec![
                        ContainerRef::Space(child_id),
                        ContainerRef::Space(parent_id),
                    ],
                    weight_milli: Some(DEFAULT_PLACEMENT_WEIGHT_MILLI),
                    valid_from: RevisionId::ZERO,
                    valid_to: None,
                    projected: true,
                    nexus_id: None,
                });
            }
        }
    }

    for (rev, data, tombstone) in nexus_records {
        if *rev > as_of || *tombstone {
            continue;
        }
        if let Ok(edge) = decode_nexus(data) {
            if !edge.is_active_at(as_of) {
                continue;
            }
            if edge.kind.as_str() == NEXUS_KIND_CONSTELLATION_PIN {
                if let Some(hex) = edge.metadata.get("pin_payload") {
                    if let Some(pin) =
                        crate::engine::nexus::decode_pin_payload(hex)
                    {
                        pinned.push(pin);
                    }
                }
                continue;
            }
            let endpoints: Vec<ContainerRef> = edge
                .endpoints
                .iter()
                .map(|ep| ep.container.clone())
                .collect();
            edges.push(UniverseEdge {
                kind: edge.kind.0.clone(),
                endpoints,
                weight_milli: edge.weight_milli,
                valid_from: edge.valid_from,
                valid_to: edge.valid_to,
                projected: false,
                nexus_id: Some(edge.id.0),
            });
        }
    }

    for pin in pinned {
        let cref = ContainerRef::Constellation(pin.id);
        if !nodes.contains(&cref) {
            nodes.push(cref);
        }
    }

    UniverseGraphView { nodes, edges }.sorted()
}
