//! Flow-vector derivation rows and delta-merge helpers (M7).

use crate::engine::hypergraph::HypergraphWriteRow;
use crate::infinitedb_core::{
    flow_vector::{quantize_direction, FlowVectorQuantization},
    flow_vector_index::{
        decode_flow_vector_index_payload, encode_flow_vector_index_payload,
        flow_vector_index_point, magnitude_bucket, FLOW_VECTOR_INDEX_SPACE,
    },
    hyperedge::Hyperedge,
};

const DEFAULT_QUANTIZATION: FlowVectorQuantization = FlowVectorQuantization {
    bits_per_axis: crate::infinitedb_core::flow_vector::DEFAULT_BITS_PER_AXIS,
};

pub fn default_flow_vector_quantization() -> FlowVectorQuantization {
    DEFAULT_QUANTIZATION
}

pub fn prepare_flow_vector_derivation(
    edge: &Hyperedge,
    quantization: FlowVectorQuantization,
) -> Vec<HypergraphWriteRow> {
    let Some(vector) = edge.flow_vector() else {
        return Vec::new();
    };
    let quantized = quantize_direction(&vector.delta, &quantization);
    let mag = magnitude_bucket(&vector.delta, &quantization);
    vec![HypergraphWriteRow {
        space: FLOW_VECTOR_INDEX_SPACE,
        point: flow_vector_index_point(&quantized, edge.id),
        data: encode_flow_vector_index_payload(edge.id, Some(mag)),
        tombstone: false,
    }]
}

pub fn prepare_flow_vector_tombstones(
    edge: &Hyperedge,
    quantization: FlowVectorQuantization,
) -> Vec<HypergraphWriteRow> {
    let Some(vector) = edge.flow_vector() else {
        return Vec::new();
    };
    let quantized = quantize_direction(&vector.delta, &quantization);
    vec![HypergraphWriteRow {
        space: FLOW_VECTOR_INDEX_SPACE,
        point: flow_vector_index_point(&quantized, edge.id),
        data: Vec::new(),
        tombstone: true,
    }]
}

pub fn edge_id_from_flow_vector_index_record(
    coords: &[u32],
    data: &[u8],
) -> Option<crate::infinitedb_core::hyperedge::HyperedgeId> {
    decode_flow_vector_index_payload(data)
        .map(|(id, _)| id)
        .or_else(|| {
            crate::infinitedb_core::flow_vector_index::hyperedge_id_from_index_coords(coords)
        })
}
