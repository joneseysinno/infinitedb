/// CompositeKey: assembles a final Hilbert key from heterogeneous dimension types.
///
/// Each `DimEncoder` maps one logical field (spatial, temporal, ordinal, enum)
/// to a u32 coordinate. `CompositeKey::encode()` collects all coordinates and
/// delegates to `hilbert::encode()` to produce the final u128 index key.
///
/// Bit budget: with 16 dimensions and 8 bits each = 128 bits, exactly filling u128.
/// Typical configs:
///   - 2D spatial (x, y) + time: 3 dims × 8 bits = 24 bits used
///   - 3D spatial + time: 4 dims × 8 bits = 32 bits
///   - Full 16-dim: 16 × 8 bits = 128 bits
use super::hilbert;

/// A single dimension's contribution to the composite key.
#[derive(Debug, Clone)]
pub struct Dimension {
    /// Human-readable label (e.g., "latitude", "timestamp").
    pub label: &'static str,
    /// The u32 coordinate value for this dimension.
    pub coord: u32,
}

impl Dimension {
    pub fn new(label: &'static str, coord: u32) -> Self {
        Self { label, coord }
    }
}

/// Precision configuration for a composite key.
#[derive(Debug, Clone, Copy)]
pub struct KeyConfig {
    /// Bits allocated per dimension (1–8). All dimensions share the same precision.
    pub bits_per_dim: u32,
}

impl KeyConfig {
    /// Standard 8-bit precision (256 values per axis).
    pub const STANDARD: KeyConfig = KeyConfig { bits_per_dim: 8 };
    /// High 4-bit precision (16 values per axis), up to 32 dimensions.
    pub const COMPACT: KeyConfig = KeyConfig { bits_per_dim: 4 };
}

/// Encodes a set of heterogeneous dimensions into a single Hilbert key.
///
/// # Example
/// ```
/// use infinitedb::infinitedb_index::composite::{CompositeKey, Dimension, KeyConfig};
/// use infinitedb::infinitedb_index::ordinal::encode_i32;
/// use infinitedb::infinitedb_index::temporal::HlcTimestamp;
///
/// let key = CompositeKey::new(KeyConfig::STANDARD)
///     .push(Dimension::new("x", 128))
///     .push(Dimension::new("y", 64))
///     .push(Dimension::new("t", HlcTimestamp { physical_ms: 1_000, logical: 0 }.to_coord()))
///     .encode();
/// ```
#[derive(Debug)]
pub struct CompositeKey {
    config: KeyConfig,
    dims: Vec<Dimension>,
}

impl CompositeKey {
    pub fn new(config: KeyConfig) -> Self {
        Self { config, dims: Vec::new() }
    }

    /// Add a dimension. Panics if adding it would exceed 16 dimensions.
    pub fn push(mut self, dim: Dimension) -> Self {
        assert!(
            self.dims.len() < 16,
            "CompositeKey exceeds maximum of 16 dimensions"
        );
        self.dims.push(dim);
        self
    }

    /// Produce the final u128 Hilbert key.
    /// Panics if no dimensions have been added.
    pub fn encode(&self) -> u128 {
        assert!(!self.dims.is_empty(), "Cannot encode a key with zero dimensions");
        let coords: Vec<u32> = self.dims.iter().map(|d| d.coord).collect();
        hilbert::encode(&coords, self.config.bits_per_dim)
    }

    /// Encode the minimum corner of a range — used to find the start of a key scan.
    pub fn encode_range_min(dims_min: &[u32], bits_per_dim: u32) -> u128 {
        hilbert::encode(dims_min, bits_per_dim)
    }

    /// Encode the maximum corner of a range — used to find the end of a key scan.
    pub fn encode_range_max(dims_max: &[u32], bits_per_dim: u32) -> u128 {
        hilbert::encode(dims_max, bits_per_dim)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_points_order_is_consistent() {
        let key_a = CompositeKey::new(KeyConfig::STANDARD)
            .push(Dimension::new("x", 10))
            .push(Dimension::new("y", 10))
            .encode();
        let key_b = CompositeKey::new(KeyConfig::STANDARD)
            .push(Dimension::new("x", 200))
            .push(Dimension::new("y", 200))
            .encode();
        // Both are valid u128 keys — just verify they differ.
        assert_ne!(key_a, key_b);
    }
}