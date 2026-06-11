//! Flow-vector primitives — directed edge displacement and quantized direction (M7).

use serde::{Deserialize, Serialize};

use super::address::SpaceId;

/// Default quantization granularity (design §7 open question).
pub const DEFAULT_BITS_PER_AXIS: u32 = 3;

/// Signed displacement from tail centroid to head centroid in a shared physical space.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowVector {
    pub space: SpaceId,
    /// Per-axis `head - tail` in the dominant physical coordinate space.
    pub delta: Vec<i32>,
}

/// Hilbert-indexable direction bucket per axis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuantizedDirection {
    pub coords: Vec<u32>,
}

/// Quantization policy for direction indexing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowVectorQuantization {
    pub bits_per_axis: u32,
}

impl Default for FlowVectorQuantization {
    fn default() -> Self {
        Self {
            bits_per_axis: DEFAULT_BITS_PER_AXIS,
        }
    }
}

/// Resolved flow vector for a live hyperedge assertion.
#[derive(Debug, Clone)]
pub struct FlowVectorRecord {
    pub edge: super::hyperedge::Hyperedge,
    pub vector: FlowVector,
    pub quantized: QuantizedDirection,
}

/// Bucket signed axis delta into a non-negative index coordinate.
pub fn quantize_axis(delta: i32, bits_per_axis: u32) -> u32 {
    let max_level = (1u32 << bits_per_axis).saturating_sub(1);
    if delta == 0 {
        return 0;
    }
    let sign = if delta > 0 { 1u32 } else { 0u32 };
    let mag = delta.unsigned_abs().min(max_level as u32);
    1 + sign * (max_level + 1) + mag
}

/// Quantize a flow-vector delta for spatial index keys.
pub fn quantize_direction(delta: &[i32], q: &FlowVectorQuantization) -> QuantizedDirection {
    QuantizedDirection {
        coords: delta
            .iter()
            .map(|&d| quantize_axis(d, q.bits_per_axis))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_directions_distinct_buckets_at_default_granularity() {
        let q = FlowVectorQuantization::default();
        let up = quantize_direction(&[0, 10], &q);
        let down = quantize_direction(&[0, -10], &q);
        let flat = quantize_direction(&[5, 0], &q);
        assert_ne!(up.coords, down.coords);
        assert_ne!(up.coords, flat.coords);
        assert_ne!(down.coords, flat.coords);
    }

    #[test]
    fn zero_delta_maps_to_zero_buckets() {
        let q = FlowVectorQuantization::default();
        let z = quantize_direction(&[0, 0], &q);
        assert_eq!(z.coords, vec![0, 0]);
    }
}
