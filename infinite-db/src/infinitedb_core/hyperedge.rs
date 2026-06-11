use std::collections::BTreeMap;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use super::address::{DimensionVector, RevisionId, SpaceId};
use super::computation::ComputationProvenance;
use super::flow_vector::FlowVector;
use super::provenance::AuthoringFrameProvenance;

/// Stable identifier for a hyperedge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct HyperedgeId(pub u64);

/// Structural endpoint polarity — orthogonal to semantic [`EndpointRole`].
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, Encode, Decode,
)]
pub enum EndpointPolarity {
    Tail,
    Head,
    /// Annotative participant on a directed edge, or the only polarity on undirected edges.
    #[default]
    Neutral,
}

/// Edge-level directionality mode.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, Encode, Decode,
)]
pub enum Directionality {
    #[default]
    Undirected,
    Directed,
}

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
    #[serde(default)]
    pub polarity: EndpointPolarity,
}

impl EndpointRef {
    /// Create an endpoint with neutral polarity (legacy-compatible default).
    pub fn new(role: EndpointRole, space: SpaceId, node: DimensionVector) -> Self {
        Self {
            role,
            space,
            node,
            polarity: EndpointPolarity::Neutral,
        }
    }

    /// Set structural polarity (builder-style).
    pub fn with_polarity(mut self, polarity: EndpointPolarity) -> Self {
        self.polarity = polarity;
        self
    }
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
    #[serde(default)]
    pub directionality: Directionality,
    /// Authoring-frame provenance (M5).
    #[serde(default)]
    pub authoring_frame: Option<AuthoringFrameProvenance>,
    /// Structured computation input lineage (M7).
    #[serde(default)]
    pub computation: Option<ComputationProvenance>,
}

impl Hyperedge {
    pub fn is_directed(&self) -> bool {
        self.directionality == Directionality::Directed
    }

    pub fn tail_endpoints(&self) -> impl Iterator<Item = &EndpointRef> {
        self.endpoints
            .iter()
            .filter(|ep| ep.polarity == EndpointPolarity::Tail)
    }

    pub fn head_endpoints(&self) -> impl Iterator<Item = &EndpointRef> {
        self.endpoints
            .iter()
            .filter(|ep| ep.polarity == EndpointPolarity::Head)
    }

    pub fn neutral_endpoints(&self) -> impl Iterator<Item = &EndpointRef> {
        self.endpoints
            .iter()
            .filter(|ep| ep.polarity == EndpointPolarity::Neutral)
    }

    pub fn tail_count(&self) -> usize {
        self.endpoints
            .iter()
            .filter(|ep| ep.polarity == EndpointPolarity::Tail)
            .count()
    }

    pub fn head_count(&self) -> usize {
        self.endpoints
            .iter()
            .filter(|ep| ep.polarity == EndpointPolarity::Head)
            .count()
    }

    pub fn neutral_count(&self) -> usize {
        self.endpoints
            .iter()
            .filter(|ep| ep.polarity == EndpointPolarity::Neutral)
            .count()
    }

    /// Storage point for a hyperedge assertion keyed by id (2D packing).
    pub fn storage_point(id: HyperedgeId) -> DimensionVector {
        DimensionVector::new(vec![
            (id.0 >> 32) as u32,
            (id.0 & 0xFFFF_FFFF) as u32,
        ])
    }

    /// Inverse of [`storage_point`] for 2D assertion addresses.
    pub fn id_from_storage_point(point: &DimensionVector) -> Option<HyperedgeId> {
        if point.coords.len() != 2 {
            return None;
        }
        Some(HyperedgeId(
            ((point.coords[0] as u64) << 32) | (point.coords[1] as u64),
        ))
    }

    /// Whether neutral endpoints are permitted on directed edges.
    pub const NEUTRAL_ON_DIRECTED: NeutralEndpointPolicy = NeutralEndpointPolicy::Permit;

    /// Validate hyperedge invariants including directionality matching rules.
    ///
    /// | directionality | tails | heads | neutrals | result |
    /// |---|---|---|---|---|
    /// | Undirected | 0 | 0 | ≥2 | OK |
    /// | Undirected | >0 | any | any | Error |
    /// | Directed | ≥1 | ≥1 | any* | OK |
    /// | Directed | 0 | any | any | Error |
    /// | Directed | any | 0 | any | Error |
    ///
    /// *Neutrals on directed edges depend on [`NeutralEndpointPolicy`].
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

        match self.directionality {
            Directionality::Undirected => {
                if self.endpoints.iter().any(|ep| ep.polarity != EndpointPolarity::Neutral) {
                    return Err(HyperedgeValidationError::PolarityOnUndirectedEdge);
                }
            }
            Directionality::Directed => {
                let tails = self.tail_count();
                let heads = self.head_count();
                if tails == 0 {
                    return Err(HyperedgeValidationError::DirectedEdgeMissingTail {
                        tail_count: tails,
                        head_count: heads,
                    });
                }
                if heads == 0 {
                    return Err(HyperedgeValidationError::DirectedEdgeMissingHead {
                        tail_count: tails,
                        head_count: heads,
                    });
                }
                if Self::NEUTRAL_ON_DIRECTED == NeutralEndpointPolicy::Reject
                    && self.neutral_count() > 0
                {
                    return Err(HyperedgeValidationError::NeutralOnDirectedEdge);
                }
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
    pub fn endpoint_centroid(&self) -> Option<(SpaceId, Vec<u32>)> {
        centroid_of_endpoints(self.endpoints.iter())
    }

    /// Centroid of tail endpoints in their shared physical space (M7).
    pub fn tail_centroid(&self) -> Option<(SpaceId, DimensionVector)> {
        centroid_of_endpoints(self.tail_endpoints()).map(|(s, c)| (s, DimensionVector::new(c)))
    }

    /// Centroid of head endpoints in their shared physical space (M7).
    pub fn head_centroid(&self) -> Option<(SpaceId, DimensionVector)> {
        centroid_of_endpoints(self.head_endpoints()).map(|(s, c)| (s, DimensionVector::new(c)))
    }

    /// Flow vector = head centroid − tail centroid when directed and same-space (M7).
    pub fn flow_vector(&self) -> Option<FlowVector> {
        if !self.is_directed() {
            return None;
        }
        let (tail_space, tail) = self.tail_centroid()?;
        let (head_space, head) = self.head_centroid()?;
        if tail_space != head_space || tail.coords.len() != head.coords.len() {
            return None;
        }
        let delta: Vec<i32> = head
            .coords
            .iter()
            .zip(tail.coords.iter())
            .map(|(&h, &t)| h as i32 - t as i32)
            .collect();
        Some(FlowVector {
            space: tail_space,
            delta,
        })
    }
}

fn centroid_of_endpoints<'a>(
    endpoints: impl Iterator<Item = &'a EndpointRef>,
) -> Option<(SpaceId, Vec<u32>)> {
    let eps: Vec<_> = endpoints.collect();
    if eps.is_empty() {
        return None;
    }
    let space = eps[0].space;
    if !eps.iter().all(|ep| ep.space == space) {
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
    Some((space, centroid))
}

/// Policy for neutral endpoints on directed edges (decision D1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NeutralEndpointPolicy {
    Permit,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HyperedgeValidationError {
    TooFewEndpoints,
    EmptyKind,
    EmptyEndpointRole,
    InvalidValidityWindow {
        valid_from: RevisionId,
        valid_to: RevisionId,
    },
    PolarityOnUndirectedEdge,
    DirectedEdgeMissingTail {
        tail_count: usize,
        head_count: usize,
    },
    DirectedEdgeMissingHead {
        tail_count: usize,
        head_count: usize,
    },
    NeutralOnDirectedEdge,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ep(polarity: EndpointPolarity, coords: u32) -> EndpointRef {
        EndpointRef::new(
            EndpointRole::new("r"),
            SpaceId(1),
            DimensionVector::new(vec![coords, 0]),
        )
        .with_polarity(polarity)
    }

    fn base_edge(directionality: Directionality, endpoints: Vec<EndpointRef>) -> Hyperedge {
        Hyperedge {
            id: HyperedgeId(1),
            kind: HyperedgeKind::new("link"),
            endpoints,
            weight_milli: None,
            metadata: BTreeMap::new(),
            valid_from: RevisionId::ZERO,
            valid_to: None,
            directionality,
            authoring_frame: None,
            computation: None,
        }
    }

    #[test]
    fn undirected_all_neutral_ok() {
        let edge = base_edge(
            Directionality::Undirected,
            vec![ep(EndpointPolarity::Neutral, 0), ep(EndpointPolarity::Neutral, 1)],
        );
        edge.validate().unwrap();
    }

    #[test]
    fn undirected_with_polarity_err() {
        let edge = base_edge(
            Directionality::Undirected,
            vec![ep(EndpointPolarity::Tail, 0), ep(EndpointPolarity::Neutral, 1)],
        );
        assert_eq!(
            edge.validate().unwrap_err(),
            HyperedgeValidationError::PolarityOnUndirectedEdge
        );
    }

    #[test]
    fn directed_requires_tail_and_head() {
        let edge = base_edge(
            Directionality::Directed,
            vec![ep(EndpointPolarity::Tail, 0), ep(EndpointPolarity::Head, 1)],
        );
        edge.validate().unwrap();
    }

    #[test]
    fn directed_missing_tail_err() {
        let edge = base_edge(
            Directionality::Directed,
            vec![ep(EndpointPolarity::Head, 0), ep(EndpointPolarity::Head, 1)],
        );
        assert!(matches!(
            edge.validate().unwrap_err(),
            HyperedgeValidationError::DirectedEdgeMissingTail { .. }
        ));
    }

    #[test]
    fn storage_point_splits_id() {
        let pt = Hyperedge::storage_point(HyperedgeId(0xABCD_EF12_3456_7890));
        assert_eq!(pt.coords, vec![0xABCD_EF12, 0x3456_7890]);
    }
}
