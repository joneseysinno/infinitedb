//! Spatial index for quantized flow-vector directions (M7).

use super::{
    address::{DimensionVector, SpaceId},
    flow_vector::{FlowVectorQuantization, QuantizedDirection},
    hyperedge::HyperedgeId,
    space::SpaceConfig,
};

/// Reserved space for flow-vector direction index entries.
pub const FLOW_VECTOR_INDEX_SPACE: SpaceId = SpaceId(u64::MAX - 3);

pub const FLOW_VECTOR_INDEX_DIMS: usize = 6;
pub const FLOW_VECTOR_INDEX_BITS_PER_DIM: u32 = 7;

/// Space config for the reserved flow-vector index.
pub fn flow_vector_index_space_config() -> SpaceConfig {
    SpaceConfig::new(
        FLOW_VECTOR_INDEX_SPACE,
        "__flow_vector_index__",
        FLOW_VECTOR_INDEX_DIMS,
    )
    .with_bits_per_dim(FLOW_VECTOR_INDEX_BITS_PER_DIM)
    .without_error_space()
}

/// Index point: quantized direction prefix + hyperedge id suffix.
pub fn flow_vector_index_point(
    quantized: &QuantizedDirection,
    edge_id: HyperedgeId,
) -> DimensionVector {
    let mut coords = quantized.coords.clone();
    while coords.len() < 4 {
        coords.push(0);
    }
    coords.push((edge_id.0 & 0xFFFF_FFFF) as u32);
    coords.push(((edge_id.0 >> 32) & 0xFFFF_FFFF) as u32);
    DimensionVector::new(coords)
}

/// Decode hyperedge id packed in the trailing index coordinates.
pub fn hyperedge_id_from_index_coords(coords: &[u32]) -> Option<HyperedgeId> {
    if coords.len() < 2 {
        return None;
    }
    let lo = coords[coords.len() - 2] as u64;
    let hi = coords[coords.len() - 1] as u64;
    Some(HyperedgeId((hi << 32) | lo))
}

/// Payload tag for cross-space flow vectors (composed at common ancestor, T12).
pub const FLOW_VECTOR_PAYLOAD_V2_TAG: u8 = 0xF2;

/// Encode index payload (hyperedge id + optional magnitude bucket) — same-space V1.
pub fn encode_flow_vector_index_payload(edge_id: HyperedgeId, magnitude_bucket: Option<u32>) -> Vec<u8> {
    let mut out = edge_id.0.to_le_bytes().to_vec();
    if let Some(m) = magnitude_bucket {
        out.extend_from_slice(&m.to_le_bytes());
    }
    out
}

/// Encode cross-space flow vector payload (V2): tag + edge id + ancestor + magnitude.
pub fn encode_flow_vector_index_payload_v2(
    edge_id: HyperedgeId,
    ancestor: SpaceId,
    magnitude_bucket: Option<u32>,
) -> Vec<u8> {
    let mut out = vec![FLOW_VECTOR_PAYLOAD_V2_TAG];
    out.extend_from_slice(&edge_id.0.to_le_bytes());
    out.extend_from_slice(&ancestor.0.to_le_bytes());
    if let Some(m) = magnitude_bucket {
        out.extend_from_slice(&m.to_le_bytes());
    }
    out
}

/// Decoded payload: edge id, optional magnitude, optional ancestor (V2 cross-space).
pub fn decode_flow_vector_index_payload(
    data: &[u8],
) -> Option<(HyperedgeId, Option<u32>, Option<SpaceId>)> {
    if data.is_empty() {
        return None;
    }
    if data[0] == FLOW_VECTOR_PAYLOAD_V2_TAG {
        if data.len() < 17 {
            return None;
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&data[1..9]);
        let id = HyperedgeId(u64::from_le_bytes(buf));
        buf.copy_from_slice(&data[9..17]);
        let ancestor = SpaceId(u64::from_le_bytes(buf));
        let magnitude = if data.len() >= 21 {
            let mut mb = [0u8; 4];
            mb.copy_from_slice(&data[17..21]);
            Some(u32::from_le_bytes(mb))
        } else {
            None
        };
        return Some((id, magnitude, Some(ancestor)));
    }
    if data.len() < 8 {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[..8]);
    let id = HyperedgeId(u64::from_le_bytes(buf));
    let magnitude = if data.len() >= 12 {
        let mut mb = [0u8; 4];
        mb.copy_from_slice(&data[8..12]);
        Some(u32::from_le_bytes(mb))
    } else {
        None
    };
    Some((id, magnitude, None))
}

/// Magnitude bucket from delta length (optional index metadata).
pub fn magnitude_bucket(delta: &[i32], q: &FlowVectorQuantization) -> u32 {
    let sum_sq: u64 = delta.iter().map(|&d| (d as i64).unsigned_abs()).sum();
    let max_level = (1u32 << q.bits_per_axis).saturating_sub(1) as u64;
    (sum_sq.min(max_level.saturating_mul(16))).min(u32::MAX as u64) as u32
}

/// Whether quantized direction coords fall within an inclusive bbox prefix.
pub fn direction_in_region(
    coords: &[u32],
    min_dir: &QuantizedDirection,
    max_dir: &QuantizedDirection,
) -> bool {
    let len = min_dir.coords.len().max(max_dir.coords.len());
    for i in 0..len {
        let c = coords.get(i).copied().unwrap_or(0);
        let lo = min_dir.coords.get(i).copied().unwrap_or(0);
        let hi = max_dir.coords.get(i).copied().unwrap_or(u32::MAX);
        if c < lo || c > hi {
            return false;
        }
    }
    true
}

/// Pad direction bbox for Hilbert range scans (suffix dims cover full edge-id range).
pub fn pad_flow_vector_index_bbox(
    min_dir: QuantizedDirection,
    max_dir: QuantizedDirection,
) -> (DimensionVector, DimensionVector) {
    let dim_max = (1u32 << FLOW_VECTOR_INDEX_BITS_PER_DIM) - 1;
    let mut min_coords = min_dir.coords;
    let mut max_coords = max_dir.coords;
    while min_coords.len() < 4 {
        min_coords.push(0);
        max_coords.push(dim_max);
    }
    min_coords.push(0);
    min_coords.push(0);
    max_coords.push(dim_max);
    max_coords.push(dim_max);
    (
        DimensionVector::new(min_coords),
        DimensionVector::new(max_coords),
    )
}
