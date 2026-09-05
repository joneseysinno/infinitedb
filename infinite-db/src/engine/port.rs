//! Port-to-constellation import (D-U12, U18).

use std::collections::BTreeMap;

use bincode::{Decode, Encode};

use crate::infinitedb_core::address::{DimensionVector, SpaceId};
use crate::infinitedb_core::nexus::{ConstellationPin, NexusEdge};
use crate::infinitedb_core::space::SpaceConfig;
use crate::infinitedb_core::universe::{ConstellationId, ContainerRef};

/// Self-contained export bundle for universe porting.
#[derive(Debug, Clone, Encode, Decode)]
pub struct UniversePortBundle {
    /// Space configs with caller-assigned ids cleared (matched by name on import).
    pub spaces: Vec<SpaceConfig>,
    pub nexus_edges: Vec<NexusEdge>,
    /// Optional record payloads keyed by original space name.
    pub records_by_name: BTreeMap<String, Vec<(DimensionVector, Vec<u8>)>>,
    pub bundle_hash: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct PortUniverseOptions {
    pub constellation_name: String,
    pub constellation_id: ConstellationId,
    pub pin_nexus_id: u64,
}

impl Default for PortUniverseOptions {
    fn default() -> Self {
        Self {
            constellation_name: "imported".into(),
            constellation_id: ConstellationId(1),
            pin_nexus_id: 1,
        }
    }
}

/// Deterministic bundle hash for idempotent re-port (INV-REGISTER-IDEMPOTENT).
pub fn bundle_hash(bundle: &UniversePortBundle) -> u64 {
    let bytes = bincode::encode_to_vec(bundle, bincode::config::standard()).unwrap_or_default();
    let mut hash = 0xcbf29ce484222325u64;
    for b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Remap container refs using name→new id map.
pub fn remap_nexus_edges(
    edges: &[NexusEdge],
    name_to_id: &BTreeMap<String, SpaceId>,
    id_to_name: &BTreeMap<SpaceId, String>,
) -> Vec<NexusEdge> {
    edges
        .iter()
        .map(|edge| {
            let mut remapped = edge.clone();
            for ep in &mut remapped.endpoints {
                if let ContainerRef::Space(old) = &ep.container {
                    if let Some(name) = id_to_name.get(old) {
                        if let Some(new_id) = name_to_id.get(name) {
                            ep.container = ContainerRef::Space(*new_id);
                        }
                    }
                }
            }
            remapped
        })
        .collect()
}

pub fn pin_for_import(
    members: Vec<ContainerRef>,
    options: &PortUniverseOptions,
) -> ConstellationPin {
    ConstellationPin {
        id: options.constellation_id,
        name: options.constellation_name.clone(),
        members,
    }
}
