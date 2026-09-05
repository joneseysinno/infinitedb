//! Wanderer identity and Ephemeris testimony encoding (D-E2, D-E3).

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::address::{DimensionVector, RevisionId, SpaceId};
use super::placement::{
    bbox_to_child, extent_in_parent, nearest_common_ancestor, placement_path_to_ancestor,
    point_to_ancestor_space,
};
use super::space::SpaceRegistry;
use super::universe::is_universe_member;
use super::hyperedge::{
    Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
    HyperedgeKind,
};
use super::universe::WANDERER_REGISTRY_SPACE;

pub use super::universe::EPHEMERIS_SPACE;

pub const EPHEMERIS_KIND_OBSERVED: &str = "ephemeris.observed";
pub const EPHEMERIS_KIND_PROJECTED: &str = "ephemeris.projected";
pub const META_STAMP_MS: &str = "stamp_ms";
pub const META_PREDICTED_FOR: &str = "predicted_for";
pub const META_GRAZE_TRACE: &str = "graze_trace";

/// Caller-allocated wanderer identity (D-E3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct WandererId(pub u64);

/// D-E1 discriminant on ephemeris entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub enum EphemerisKind {
    Observed,
    Projected { predicted_for: RevisionId },
}

/// Typed ephemeris entry API over hyperedge testimony.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct EphemerisEntry {
    pub wanderer: WandererId,
    pub kind: EphemerisKind,
    pub anchor: SpaceId,
    pub position: DimensionVector,
    pub stamp: RevisionId,
    pub graze_trace: bool,
}

/// One ambient-frame graze against a member space extent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Graze {
    pub space: SpaceId,
    pub region: (DimensionVector, DimensionVector),
}

pub fn graze_trace_nexus_id(
    wanderer: WandererId,
    space: SpaceId,
    stamp: RevisionId,
    region: &(DimensionVector, DimensionVector),
) -> crate::infinitedb_core::nexus::NexusId {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&wanderer.0.to_le_bytes());
    bytes.extend_from_slice(&space.0.to_le_bytes());
    bytes.extend_from_slice(&stamp.legacy_sequence().to_le_bytes());
    bytes.extend_from_slice(&stamp.physical_ms().to_le_bytes());
    bytes.extend_from_slice(&stamp.session().to_le_bytes());
    bytes.extend_from_slice(&stamp.sequence().to_le_bytes());
    bytes.extend_from_slice(&(stamp.logical() as u32).to_le_bytes());
    encode_dim_vec(&mut bytes, &region.0);
    encode_dim_vec(&mut bytes, &region.1);
    let hash = blake3::hash(&bytes);
    let mut out = [0u8; 8];
    out.copy_from_slice(&hash.as_bytes()[..8]);
    crate::infinitedb_core::nexus::NexusId(u64::from_le_bytes(out))
}

fn encode_dim_vec(bytes: &mut Vec<u8>, v: &DimensionVector) {
    bytes.extend_from_slice(&(v.coords.len() as u32).to_le_bytes());
    for c in &v.coords {
        bytes.extend_from_slice(&c.to_le_bytes());
    }
}

pub fn wanderer_registry_space_config() -> super::space::SpaceConfig {
    super::space::SpaceConfig::new(WANDERER_REGISTRY_SPACE, "__wanderer_registry__", 2)
        .with_bits_per_dim(32)
        .without_error_space()
}

pub fn ephemeris_space_config() -> super::space::SpaceConfig {
    super::space::SpaceConfig::new(EPHEMERIS_SPACE, "__ephemeris__", 2)
        .with_bits_per_dim(32)
        .without_error_space()
}

pub fn wanderer_identity_point(id: WandererId) -> DimensionVector {
    Hyperedge::storage_point(HyperedgeId(id.0))
}

pub fn ephemeris_entry_to_hyperedge(entry: &EphemerisEntry, edge_id: HyperedgeId) -> Hyperedge {
    let kind = match &entry.kind {
        EphemerisKind::Observed => EPHEMERIS_KIND_OBSERVED,
        EphemerisKind::Projected { .. } => EPHEMERIS_KIND_PROJECTED,
    };
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert(
        META_STAMP_MS.into(),
        entry.stamp.physical_ms().to_string(),
    );
    if let EphemerisKind::Projected { predicted_for } = &entry.kind {
        metadata.insert(
            META_PREDICTED_FOR.into(),
            predicted_for.legacy_sequence().to_string(),
        );
    }
    if entry.graze_trace {
        metadata.insert(META_GRAZE_TRACE.into(), "1".into());
    }
    Hyperedge {
        id: edge_id,
        kind: HyperedgeKind::new(kind),
        endpoints: vec![
            EndpointRef::new(
                EndpointRole::new("wanderer"),
                WANDERER_REGISTRY_SPACE,
                wanderer_identity_point(entry.wanderer),
            )
            .with_polarity(EndpointPolarity::Tail),
            EndpointRef::new(
                EndpointRole::new("position"),
                entry.anchor,
                entry.position.clone(),
            )
            .with_polarity(EndpointPolarity::Head),
        ],
        weight_milli: None,
        metadata,
        valid_from: entry.stamp,
        valid_to: None,
        directionality: Directionality::Directed,
        authoring_frame: None,
        computation: None,
    }
}

/// Map a point from `from` space coordinates into `to` space coordinates via the tower.
fn map_point_between_spaces(
    registry: &SpaceRegistry,
    from: SpaceId,
    to: SpaceId,
    point: &[u32],
) -> Option<Vec<u32>> {
    if from == to {
        return Some(point.to_vec());
    }
    let ancestor = nearest_common_ancestor(registry, from, to)?;
    let in_ancestor = if from == ancestor {
        Some(point.to_vec())
    } else {
        point_to_ancestor_space(registry, from, ancestor, point).ok()
    }?;
    if to == ancestor {
        return Some(in_ancestor);
    }
    let path = placement_path_to_ancestor(registry, to, ancestor).ok()?;
    let mut current = in_ancestor;
    for placement in path.iter().rev() {
        let lo = current.clone();
        let (min, max) = bbox_to_child(placement, &lo, &lo)?;
        if min != max {
            return None;
        }
        current = min;
    }
    Some(current)
}

fn point_in_box(point: &[u32], min: &[i64], max: &[i64]) -> bool {
    point
        .iter()
        .enumerate()
        .all(|(i, &p)| {
            let pi = i64::from(p);
            pi >= min.get(i).copied().unwrap_or(i64::MIN)
                && pi <= max.get(i).copied().unwrap_or(i64::MAX)
        })
}

/// Pure graze geometry: ambient position vs member placement extents (D-E4).
pub fn grazes(entry: &EphemerisEntry, registry: &SpaceRegistry) -> Vec<Graze> {
    let anchor = entry.anchor;
    let point = &entry.position.coords;
    let mut out = Vec::new();
    for member_id in registry.space_ids() {
        let config = match registry.get(member_id) {
            Some(c) => c,
            None => continue,
        };
        if !is_universe_member(member_id, config) || member_id == anchor {
            continue;
        }
        let (Some(parent), Some(placement)) = (config.parent, config.placement.as_ref()) else {
            continue;
        };
        let point_in_parent = match map_point_between_spaces(registry, anchor, parent, point) {
            Some(p) => p,
            None => continue,
        };
        let (min_i, max_i) = extent_in_parent(placement);
        if point_in_box(&point_in_parent, &min_i, &max_i) {
            let region_min: Vec<u32> = min_i.iter().map(|&v| v as u32).collect();
            let region_max: Vec<u32> = max_i.iter().map(|&v| v as u32).collect();
            out.push(Graze {
                space: member_id,
                region: (
                    DimensionVector::new(region_min),
                    DimensionVector::new(region_max),
                ),
            });
        }
    }
    out.sort_by_key(|g| g.space.0);
    out
}

pub fn hyperedge_to_ephemeris_entry(edge: &Hyperedge) -> Option<EphemerisEntry> {
    let kind_str = edge.kind.as_str();
    let kind = if kind_str == EPHEMERIS_KIND_OBSERVED {
        EphemerisKind::Observed
    } else if kind_str == EPHEMERIS_KIND_PROJECTED {
        let predicted_for = edge
            .metadata
            .get(META_PREDICTED_FOR)
            .and_then(|s| s.parse::<u64>().ok())
            .map(RevisionId::legacy)
            .unwrap_or(edge.valid_from);
        EphemerisKind::Projected {
            predicted_for,
        }
    } else {
        return None;
    };
    let tail = edge.tail_endpoints().next()?;
    let head = edge.head_endpoints().next()?;
  if tail.space != WANDERER_REGISTRY_SPACE {
        return None;
    }
    let wanderer = Hyperedge::id_from_storage_point(&tail.node)
        .map(|id| WandererId(id.0))?;
    let graze_trace = edge.metadata.get(META_GRAZE_TRACE).map(|v| v == "1").unwrap_or(false);
    Some(EphemerisEntry {
        wanderer,
        kind,
        anchor: head.space,
        position: head.node.clone(),
        stamp: edge.valid_from,
        graze_trace,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::address::{RevisionId, SpaceId};
    use crate::infinitedb_core::placement::Placement;
    use crate::infinitedb_core::space::{SpaceConfig, SpaceRegistry};

    #[test]
    fn entry_edge_roundtrip_observed() {
        let entry = EphemerisEntry {
            wanderer: WandererId(7),
            kind: EphemerisKind::Observed,
            anchor: SpaceId(2),
            position: DimensionVector::new(vec![10, 20]),
            stamp: RevisionId::legacy(5),
            graze_trace: false,
        };
        let edge = ephemeris_entry_to_hyperedge(&entry, HyperedgeId(99));
        let back = hyperedge_to_ephemeris_entry(&edge).unwrap();
        assert_eq!(back.wanderer, WandererId(7));
        assert_eq!(back.kind, EphemerisKind::Observed);
    }

    #[test]
    fn projected_decodes_predicted_for() {
        let entry = EphemerisEntry {
            wanderer: WandererId(1),
            kind: EphemerisKind::Projected {
                predicted_for: RevisionId::legacy(100),
            },
            anchor: SpaceId(3),
            position: DimensionVector::new(vec![1, 1]),
            stamp: RevisionId::legacy(50),
            graze_trace: false,
        };
        let edge = ephemeris_entry_to_hyperedge(&entry, HyperedgeId(1));
        let back = hyperedge_to_ephemeris_entry(&edge).unwrap();
        assert_eq!(
            back.kind,
            EphemerisKind::Projected {
                predicted_for: RevisionId::legacy(100)
            }
        );
    }

    #[test]
    fn grazes_child_placement_in_parent() {
        let mut registry = SpaceRegistry::new();
        registry
            .register(SpaceConfig::new(SpaceId(1), "root", 2))
            .unwrap();
        registry
            .register(
                SpaceConfig::new(SpaceId(2), "child", 2)
                    .with_parent(SpaceId(1))
                    .with_placement(Placement::axis_aligned(vec![0, 0], 1, 1, vec![4, 4])),
            )
            .unwrap();
        let entry = EphemerisEntry {
            wanderer: WandererId(1),
            kind: EphemerisKind::Observed,
            anchor: SpaceId(1),
            position: DimensionVector::new(vec![1, 1]),
            stamp: RevisionId::legacy(1),
            graze_trace: false,
        };
        let hits = grazes(&entry, &registry);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].space, SpaceId(2));
    }
}
