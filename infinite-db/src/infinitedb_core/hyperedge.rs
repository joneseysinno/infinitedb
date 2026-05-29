use std::collections::BTreeMap;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use super::address::{DimensionVector, RevisionId, SpaceId};

/// Stable identifier for a hyperedge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct HyperedgeId(pub u64);

/// User-defined relationship kind label.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct HyperedgeKind(pub String);

impl HyperedgeKind {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// User-defined endpoint role label.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct EndpointRole(pub String);

impl EndpointRole {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One participant in a hyperedge, pointing to an entity address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct EndpointRef {
    pub role: EndpointRole,
    pub space: SpaceId,
    pub node: DimensionVector,
}

/// N-ary relationship linking endpoints with optional metadata.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct Hyperedge {
    pub id: HyperedgeId,
    pub kind: HyperedgeKind,
    pub endpoints: Vec<EndpointRef>,
    pub weight_milli: Option<i64>,
    pub metadata: BTreeMap<String, String>,
    pub valid_from: RevisionId,
    pub valid_to: Option<RevisionId>,
}

impl Hyperedge {
    /// Validate common hyperedge invariants.
    pub fn validate(&self) -> Result<(), HyperedgeValidationError> {
        if self.endpoints.len() < 2 {
            return Err(HyperedgeValidationError::TooFewEndpoints);
        }
        if self.kind.0.trim().is_empty() {
            return Err(HyperedgeValidationError::EmptyKind);
        }
        if self.endpoints.iter().any(|ep| ep.role.0.trim().is_empty()) {
            return Err(HyperedgeValidationError::EmptyEndpointRole);
        }
        if let Some(valid_to) = self.valid_to {
            if valid_to < self.valid_from {
                return Err(HyperedgeValidationError::InvalidValidityWindow {
                    valid_from: self.valid_from,
                    valid_to,
                });
            }
        }
        Ok(())
    }

    /// Returns true when this edge is active at the given revision.
    pub fn is_active_at(&self, revision: RevisionId) -> bool {
        revision >= self.valid_from
            && self.valid_to.map(|to| revision <= to).unwrap_or(true)
    }

    /// Compute the centroid of the endpoints sharing the most common space.
    ///
    /// Returns `(space, centroid_coords)` for the modal space, or `None` when no
    /// single space holds at least two endpoints (a purely cross-space edge has
    /// no shared coordinate frame, so there is no meaningful centroid). The
    /// centroid is the per-dimension integer mean over the shared dimensions.
    pub fn endpoint_centroid(&self) -> Option<(SpaceId, Vec<u32>)> {
        use std::collections::HashMap;
        let mut groups: HashMap<u64, Vec<&EndpointRef>> = HashMap::new();
        for ep in &self.endpoints {
            groups.entry(ep.space.0).or_default().push(ep);
        }
        // Modal space: most endpoints, ties broken by smallest space id.
        let (space_id, eps) = groups
            .into_iter()
            .max_by(|a, b| a.1.len().cmp(&b.1.len()).then(b.0.cmp(&a.0)))?;
        if eps.len() < 2 {
            return None;
        }
        let dims = eps.iter().map(|e| e.node.coords.len()).min().unwrap_or(0);
        if dims == 0 {
            return None;
        }
        let mut sums = vec![0u64; dims];
        for ep in &eps {
            for (d, slot) in sums.iter_mut().enumerate() {
                *slot += ep.node.coords[d] as u64;
            }
        }
        let n = eps.len() as u64;
        let centroid = sums.into_iter().map(|s| (s / n) as u32).collect();
        Some((SpaceId(space_id), centroid))
    }
}

#[derive(Debug)]
pub enum HyperedgeValidationError {
    TooFewEndpoints,
    EmptyKind,
    EmptyEndpointRole,
    InvalidValidityWindow {
        valid_from: RevisionId,
        valid_to: RevisionId,
    },
}
