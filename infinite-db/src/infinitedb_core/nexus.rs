//! Container-granularity Nexus edges (D-U4, D-U8).

use std::collections::BTreeMap;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::address::{DimensionVector, RevisionId};
use super::hyperedge::{Directionality, EndpointPolarity};
use super::universe::{ContainerRef, ConstellationId};

pub const NEXUS_KIND_PLACEMENT: &str = "placement";
pub const NEXUS_KIND_GRAZE: &str = "graze";
pub const NEXUS_KIND_CONSTELLATION_PIN: &str = "constellation.pin";

/// Stable Nexus identifier (2D storage packing like hyperedges).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct NexusId(pub u64);

/// Open string kind label for Nexus edges.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct NexusKind(pub String);

impl NexusKind {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One container endpoint with optional region.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct NexusEndpoint {
    pub container: ContainerRef,
    pub region: Option<(DimensionVector, DimensionVector)>,
    pub polarity: EndpointPolarity,
}

/// Container-granularity relationship edge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct NexusEdge {
    pub id: NexusId,
    pub kind: NexusKind,
    pub endpoints: Vec<NexusEndpoint>,
    pub weight_milli: Option<i64>,
    pub metadata: BTreeMap<String, String>,
    pub valid_from: RevisionId,
    pub valid_to: Option<RevisionId>,
    pub directionality: Directionality,
}

/// Payload for a pinned constellation assertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct ConstellationPin {
    pub id: ConstellationId,
    pub name: String,
    pub members: Vec<ContainerRef>,
}

impl NexusEdge {
    pub fn storage_point(id: NexusId) -> DimensionVector {
        DimensionVector::new(vec![
            (id.0 >> 32) as u32,
            (id.0 & 0xFFFF_FFFF) as u32,
        ])
    }

    pub fn id_from_storage_point(point: &DimensionVector) -> Option<NexusId> {
        if point.coords.len() != 2 {
            return None;
        }
        Some(NexusId(
            ((point.coords[0] as u64) << 32) | (point.coords[1] as u64),
        ))
    }

    pub fn is_active_at(&self, revision: RevisionId) -> bool {
        revision >= self.valid_from
            && self.valid_to.map(|to| revision <= to).unwrap_or(true)
    }

    pub fn validate(&self) -> Result<(), NexusValidationError> {
        if self.kind.0.trim().is_empty() {
            return Err(NexusValidationError::EmptyKind);
        }
        if self.kind.as_str() == NEXUS_KIND_PLACEMENT {
            return Err(NexusValidationError::ReservedKind(NEXUS_KIND_PLACEMENT));
        }
        if self.kind.as_str() == NEXUS_KIND_CONSTELLATION_PIN {
            return Ok(());
        }
        if self.endpoints.len() < 2 {
            return Err(NexusValidationError::TooFewEndpoints);
        }
        if let Some(valid_to) = self.valid_to {
            if valid_to < self.valid_from {
                return Err(NexusValidationError::InvalidValidityWindow {
                    valid_from: self.valid_from,
                    valid_to,
                });
            }
        }
        for ep in &self.endpoints {
            if let Some((min, max)) = &ep.region {
                if min.dims() != max.dims() {
                    return Err(NexusValidationError::RegionDimMismatch);
                }
                for (a, b) in min.coords.iter().zip(max.coords.iter()) {
                    if *a > *b {
                        return Err(NexusValidationError::InvalidRegion);
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NexusValidationError {
    TooFewEndpoints,
    EmptyKind,
    ReservedKind(&'static str),
    InvalidValidityWindow {
        valid_from: RevisionId,
        valid_to: RevisionId,
    },
    RegionDimMismatch,
    InvalidRegion,
    EndpointNotFound,
}

impl std::fmt::Display for NexusValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NexusValidationError::TooFewEndpoints => write!(f, "nexus needs at least two endpoints"),
            NexusValidationError::EmptyKind => write!(f, "nexus kind cannot be empty"),
            NexusValidationError::ReservedKind(k) => write!(f, "reserved nexus kind {k} cannot be written"),
            NexusValidationError::InvalidValidityWindow { .. } => write!(f, "invalid validity window"),
            NexusValidationError::RegionDimMismatch => write!(f, "region min/max dimension mismatch"),
            NexusValidationError::InvalidRegion => write!(f, "region min exceeds max"),
            NexusValidationError::EndpointNotFound => write!(f, "nexus endpoint not registered"),
        }
    }
}

impl std::error::Error for NexusValidationError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::address::{RevisionId, SpaceId};

    #[test]
    fn placement_kind_rejected_on_write() {
        let edge = NexusEdge {
            id: NexusId(1),
            kind: NexusKind::new(NEXUS_KIND_PLACEMENT),
            endpoints: vec![],
            weight_milli: None,
            metadata: BTreeMap::new(),
            valid_from: RevisionId::ZERO,
            valid_to: None,
            directionality: Directionality::Undirected,
        };
        assert!(matches!(
            edge.validate(),
            Err(NexusValidationError::ReservedKind(_))
        ));
    }

    #[test]
    fn active_window_matches_hyperedge_semantics() {
        let nexus = NexusEdge {
            id: NexusId(2),
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
            metadata: BTreeMap::new(),
            valid_from: RevisionId::legacy(5),
            valid_to: Some(RevisionId::legacy(10)),
            directionality: Directionality::Undirected,
        };
        let hyper = crate::infinitedb_core::hyperedge::Hyperedge {
            id: crate::infinitedb_core::hyperedge::HyperedgeId(1),
            kind: crate::infinitedb_core::hyperedge::HyperedgeKind::new("x"),
            endpoints: vec![],
            weight_milli: None,
            metadata: BTreeMap::new(),
            valid_from: RevisionId::legacy(5),
            valid_to: Some(RevisionId::legacy(10)),
            directionality: Directionality::Undirected,
            authoring_frame: None,
            computation: None,
        };
        for rev in 4..=11 {
            let r = RevisionId::legacy(rev);
            assert_eq!(nexus.is_active_at(r), hyper.is_active_at(r));
        }
    }
}
