//! Hilbert range sharding within a space (format v4).

use crate::infinitedb_core::{
    address::{DimensionVector, SpaceId},
    hilbert_key::HilbertKey,
    space::SpaceRegistry,
};

use super::query::space_key;

/// Hilbert shard membership for pruning (distinct from [`ShardKey`] routing).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardRef {
    pub shard_id: u32,
    pub shard_bits: u32,
}

impl ShardRef {
    pub fn new(shard_id: u32, shard_bits: u32) -> Self {
        Self { shard_id, shard_bits }
    }

    /// True when `key` belongs to this shard.
    pub fn contains_key(self, key: HilbertKey) -> bool {
        hilbert_shard_id(key.raw(), self.shard_bits) == self.shard_id
    }
}

/// Default Hilbert shard bits when a space config omits an explicit value.
pub const DEFAULT_SHARD_BITS: u32 = 4;

/// Number of Hilbert shards for the given `shard_bits` (`2^shard_bits`, capped at 65536).
pub fn shard_count(shard_bits: u32) -> u32 {
    1u32.checked_shl(shard_bits.min(16)).unwrap_or(1)
}

/// Map a Hilbert key to a shard id: `key >> (128 - shard_bits)`.
pub fn hilbert_shard_id(key: u128, shard_bits: u32) -> u32 {
    if shard_bits == 0 {
        return 0;
    }
    let shift = 128u32.saturating_sub(shard_bits.min(16));
    (key >> shift) as u32
}

/// Resolve the Hilbert shard for a record coordinate in `space`.
pub fn shard_for_point(
    spaces: &SpaceRegistry,
    space: SpaceId,
    point: &DimensionVector,
    shard_bits: u32,
) -> u32 {
    let key = space_key(spaces, space, point);
    hilbert_shard_id(key, shard_bits)
}

/// Composite key for `(space, hilbert_shard_id)` coordinator and live-tail maps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardKey {
    pub space_id: SpaceId,
    pub shard_id: u32,
}

impl ShardKey {
    pub fn new(space_id: SpaceId, shard_id: u32) -> Self {
        Self { space_id, shard_id }
    }
}