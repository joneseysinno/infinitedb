//! Flow-vector derivation rows and delta-merge helpers (M7 + T12 cross-space).

use crate::engine::hypergraph::HypergraphWriteRow;
use crate::infinitedb_core::{
    flow_vector::{quantize_direction, FlowVector, FlowVectorQuantization},
    flow_vector_index::{
        decode_flow_vector_index_payload, encode_flow_vector_index_payload,
        encode_flow_vector_index_payload_v2, flow_vector_index_point, magnitude_bucket,
        FLOW_VECTOR_INDEX_SPACE,
    },
    hyperedge::Hyperedge,
    placement::{nearest_common_ancestor, point_to_ancestor_space},
    space::SpaceRegistry,
};

const DEFAULT_QUANTIZATION: FlowVectorQuantization = FlowVectorQuantization {
    bits_per_axis: crate::infinitedb_core::flow_vector::DEFAULT_BITS_PER_AXIS,
};

pub fn default_flow_vector_quantization() -> FlowVectorQuantization {
    DEFAULT_QUANTIZATION
}

/// Resolve flow vector: same-space (M7) or composed at nearest common ancestor (T12).
pub fn resolve_flow_vector(edge: &Hyperedge, registry: &SpaceRegistry) -> Option<FlowVector> {
    if let Some(v) = edge.flow_vector() {
        return Some(v);
    }
    if !edge.is_directed() {
        return None;
    }
    let (tail_space, tail) = edge.tail_centroid()?;
    let (head_space, head) = edge.head_centroid()?;
    if tail_space == head_space {
        return None;
    }
    let ancestor = nearest_common_ancestor(registry, tail_space, head_space)?;
    let tail_anc =
        point_to_ancestor_space(registry, tail_space, ancestor, &tail.coords).ok()?;
    let head_anc =
        point_to_ancestor_space(registry, head_space, ancestor, &head.coords).ok()?;
    if tail_anc.len() != head_anc.len() {
        return None;
    }
    let delta: Vec<i32> = head_anc
        .iter()
        .zip(tail_anc.iter())
        .map(|(&h, &t)| h as i32 - t as i32)
        .collect();
    Some(FlowVector {
        space: ancestor,
        delta,
    })
}

pub fn prepare_flow_vector_derivation(
    edge: &Hyperedge,
    registry: &SpaceRegistry,
    quantization: FlowVectorQuantization,
) -> Vec<HypergraphWriteRow> {
    let Some(vector) = resolve_flow_vector(edge, registry) else {
        return Vec::new();
    };
    let cross = edge.flow_vector().is_none();
    let quantized = quantize_direction(&vector.delta, &quantization);
    let mag = magnitude_bucket(&vector.delta, &quantization);
    let data = if cross {
        encode_flow_vector_index_payload_v2(edge.id, vector.space, Some(mag))
    } else {
        encode_flow_vector_index_payload(edge.id, Some(mag))
    };
    vec![HypergraphWriteRow {
        space: FLOW_VECTOR_INDEX_SPACE,
        point: flow_vector_index_point(&quantized, edge.id),
        data,
        tombstone: false,
        structural: false,
    }]
}

pub fn prepare_flow_vector_tombstones(
    edge: &Hyperedge,
    registry: &SpaceRegistry,
    quantization: FlowVectorQuantization,
) -> Vec<HypergraphWriteRow> {
    let Some(vector) = resolve_flow_vector(edge, registry) else {
        return Vec::new();
    };
    let quantized = quantize_direction(&vector.delta, &quantization);
    vec![HypergraphWriteRow {
        space: FLOW_VECTOR_INDEX_SPACE,
        point: flow_vector_index_point(&quantized, edge.id),
        data: Vec::new(),
        tombstone: true,
        structural: false,
    }]
}

pub fn edge_id_from_flow_vector_index_record(
    coords: &[u32],
    data: &[u8],
) -> Option<crate::infinitedb_core::hyperedge::HyperedgeId> {
    decode_flow_vector_index_payload(data)
        .map(|(id, _, _)| id)
        .or_else(|| {
            crate::infinitedb_core::flow_vector_index::hyperedge_id_from_index_coords(coords)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::{
        address::{DimensionVector, SpaceId},
        hyperedge::{
            Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
            HyperedgeKind,
        },
        placement::Placement,
        space::SpaceConfig,
    };
    use std::collections::BTreeMap;

    fn node(space: SpaceId, x: u32, y: u32, pol: EndpointPolarity) -> EndpointRef {
        EndpointRef::new(
            EndpointRole::new("n"),
            space,
            DimensionVector::new(vec![x, y]),
        )
        .with_polarity(pol)
    }

    #[test]
    fn cross_space_vector_at_common_ancestor() {
        let mut reg = SpaceRegistry::new();
        reg.register(SpaceConfig::new(SpaceId(1), "site", 2)).unwrap();
        reg.register(
            SpaceConfig::new(SpaceId(2), "west", 2)
                .with_parent(SpaceId(1))
                .with_placement(Placement::axis_aligned(vec![0, 0], 1, 1, vec![128, 128])),
        )
        .unwrap();
        reg.register(
            SpaceConfig::new(SpaceId(3), "east", 2)
                .with_parent(SpaceId(1))
                .with_placement(Placement::axis_aligned(vec![64, 0], 1, 1, vec![64, 128])),
        )
        .unwrap();
        let edge = Hyperedge {
            id: HyperedgeId(1),
            kind: HyperedgeKind::new("flow"),
            endpoints: vec![
                node(SpaceId(2), 10, 5, EndpointPolarity::Tail),
                node(SpaceId(3), 5, 5, EndpointPolarity::Head),
            ],
            weight_milli: None,
            metadata: BTreeMap::new(),
            valid_from: crate::infinitedb_core::address::RevisionId::ZERO,
            valid_to: None,
            directionality: Directionality::Directed,
            authoring_frame: None,
            computation: None,
        };
        let v = resolve_flow_vector(&edge, &reg).unwrap();
        assert_eq!(v.space, SpaceId(1));
        assert_eq!(v.delta, vec![59, 0]);
    }

    #[test]
    fn no_common_ancestor_yields_none() {
        let mut reg = SpaceRegistry::new();
        reg.register(SpaceConfig::new(SpaceId(1), "a", 2)).unwrap();
        reg.register(SpaceConfig::new(SpaceId(2), "b", 2)).unwrap();
        let edge = Hyperedge {
            id: HyperedgeId(2),
            kind: HyperedgeKind::new("flow"),
            endpoints: vec![
                node(SpaceId(1), 0, 0, EndpointPolarity::Tail),
                node(SpaceId(2), 1, 0, EndpointPolarity::Head),
            ],
            weight_milli: None,
            metadata: BTreeMap::new(),
            valid_from: crate::infinitedb_core::address::RevisionId::ZERO,
            valid_to: None,
            directionality: Directionality::Directed,
            authoring_frame: None,
            computation: None,
        };
        assert!(resolve_flow_vector(&edge, &reg).is_none());
    }
}
