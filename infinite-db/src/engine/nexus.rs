//! Nexus write/read helpers.

use std::io;

use crate::infinitedb_core::{
    address::{RevisionId, SpaceId},
    nexus::{ConstellationPin, NexusEdge, NexusId, NexusValidationError, NEXUS_KIND_CONSTELLATION_PIN},
    nexus_codec::{decode_nexus, encode_nexus},
    space::SpaceConfig,
    universe::{ContainerRef, ConstellationId, NEXUS_SPACE},
};

use super::hypergraph::HypergraphWriteRow;

pub fn nexus_space_config() -> SpaceConfig {
    SpaceConfig::new(NEXUS_SPACE, "__nexus__", 2)
        .with_bits_per_dim(32)
        .without_error_space()
}

pub fn prepare_nexus_write(edge: &NexusEdge) -> io::Result<HypergraphWriteRow> {
    let data = encode_nexus(edge)?;
    Ok(HypergraphWriteRow::new_row(
        NEXUS_SPACE,
        NexusEdge::storage_point(edge.id),
        data,
        false,
    ))
}

pub fn prepare_nexus_tombstone(id: NexusId) -> HypergraphWriteRow {
    HypergraphWriteRow::new_row(NEXUS_SPACE, NexusEdge::storage_point(id), vec![], true)
}

pub fn decode_nexus_record(data: &[u8]) -> io::Result<NexusEdge> {
    decode_nexus(data)
}

pub fn encode_pin_payload(pin: &ConstellationPin) -> String {
    let bytes = bincode::encode_to_vec(pin, bincode::config::standard())
        .unwrap_or_default();
    // store as lossy utf8 for metadata map — use base64-free hex
    bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

pub fn decode_pin_payload(hex: &str) -> Option<ConstellationPin> {
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .filter_map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect();
    bincode::decode_from_slice::<ConstellationPin, _>(&bytes, bincode::config::standard())
        .ok()
        .map(|(p, _)| p)
}

pub fn nexus_edge_from_pin(pin: ConstellationPin, id: NexusId, valid_from: RevisionId) -> NexusEdge {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert("pin_payload".into(), encode_pin_payload(&pin));
    NexusEdge {
        id,
        kind: crate::infinitedb_core::nexus::NexusKind::new(NEXUS_KIND_CONSTELLATION_PIN),
        endpoints: pin.members.iter().map(|m| {
            crate::infinitedb_core::nexus::NexusEndpoint {
                container: m.clone(),
                region: None,
                polarity: crate::infinitedb_core::hyperedge::EndpointPolarity::Neutral,
            }
        }).collect(),
        weight_milli: None,
        metadata,
        valid_from,
        valid_to: None,
        directionality: crate::infinitedb_core::hyperedge::Directionality::Undirected,
    }
}

pub fn pin_from_edge(edge: &NexusEdge) -> Option<ConstellationPin> {
    edge.metadata
        .get("pin_payload")
        .and_then(|hex| decode_pin_payload(hex))
}

pub fn space_referenced_in_nexus(
    space: SpaceId,
    history: &[(RevisionId, Vec<u8>, bool)],
    as_of: RevisionId,
) -> bool {
    for (rev, data, tombstone) in history {
        if *tombstone || *rev > as_of {
            continue;
        }
        if let Ok(edge) = decode_nexus(data) {
            if !edge.is_active_at(as_of) {
                continue;
            }
            if edge.kind.as_str() == NEXUS_KIND_CONSTELLATION_PIN {
                if let Some(pin) = pin_from_edge(&edge) {
                    if pin
                        .members
                        .iter()
                        .any(|m| matches!(m, ContainerRef::Space(s) if *s == space))
                    {
                        return true;
                    }
                }
                continue;
            }
            for ep in &edge.endpoints {
                if matches!(&ep.container, ContainerRef::Space(s) if *s == space) {
                    return true;
                }
            }
        }
    }
    false
}

pub fn constellation_referenced_in_nexus(
    constellation: ConstellationId,
    history: &[(RevisionId, Vec<u8>, bool)],
    as_of: RevisionId,
) -> bool {
    for (rev, data, tombstone) in history {
        if *tombstone || *rev > as_of {
            continue;
        }
        if let Ok(edge) = decode_nexus(data) {
            if !edge.is_active_at(as_of) {
                continue;
            }
            if edge.kind.as_str() == NEXUS_KIND_CONSTELLATION_PIN {
                continue;
            }
            for ep in &edge.endpoints {
                if matches!(
                    &ep.container,
                    ContainerRef::Constellation(id) if *id == constellation
                ) {
                    return true;
                }
            }
        }
    }
    false
}

pub fn validate_nexus_endpoints(
    edge: &NexusEdge,
    member_check: impl Fn(&ContainerRef) -> bool,
) -> Result<(), NexusValidationError> {
    edge.validate()?;
    for ep in &edge.endpoints {
        if !member_check(&ep.container) {
            return Err(NexusValidationError::EndpointNotFound);
        }
    }
    Ok(())
}
