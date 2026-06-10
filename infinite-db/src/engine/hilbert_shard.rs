//! Hilbert range sharding within a space (format v4).

use crate::infinitedb_core::{
    address::{DimensionVector, SpaceId},
    space::SpaceRegistry,
};

use super::query::space_key;

/// Default `SpaceConfig::shard_bits` (16 intra-space I/O shards).
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

/// Pack `(space_id, shard_id)` into a single map key.
pub fn pack_shard_key(space_id: u64, shard_id: u32) -> u64 {
    (space_id << 16) | (shard_id as u64)
}

/// Unpack a packed shard key.
pub fn unpack_shard_key(key: u64) -> (u64, u32) {
    (key >> 16, (key & 0xFFFF) as u32)
}
