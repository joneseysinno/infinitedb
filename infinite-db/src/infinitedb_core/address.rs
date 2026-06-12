use bincode::{
    de::{BorrowDecoder, Decoder},
    enc::Encoder,
    error::{DecodeError, EncodeError},
    BorrowDecode, Decode, Encode,
};
use serde::{Deserialize, Serialize};

use super::hlc::HlcStamp;

/// Identifies a logical space (a named dataset/dimension space).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct SpaceId(pub u64);

/// Hybrid logical revision — 128-bit stamp (peer track Phase 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RevisionId(HlcStamp);

impl RevisionId {
    /// The initial revision before any writes have occurred.
    pub const ZERO: RevisionId = RevisionId(HlcStamp::ZERO);

    /// Embed a dense pre-HLC revision counter (D-P4).
    pub const fn legacy(sequence: u64) -> Self {
        RevisionId(HlcStamp::legacy(sequence))
    }

    /// Construct from a full HLC stamp.
    pub const fn from_stamp(stamp: HlcStamp) -> Self {
        RevisionId(stamp)
    }

    pub fn stamp(self) -> HlcStamp {
        self.0
    }

    /// Dense u64 view for the global session-0 stream (Phase 1 allocator).
    pub fn legacy_sequence(self) -> u64 {
        self.0.legacy_sequence()
    }

    pub fn session(self) -> u32 {
        self.0.session
    }

    pub fn sequence(self) -> u32 {
        self.0.sequence
    }

    pub fn physical_ms(self) -> u64 {
        self.0.physical_ms
    }

    pub fn logical(self) -> u16 {
        self.0.logical
    }

    pub fn is_global_legacy(self) -> bool {
        self.0.is_legacy_embedded()
    }

    /// Next revision in the global session-0 allocator (Phase 1).
    pub fn next_global(self) -> RevisionId {
        debug_assert!(self.is_global_legacy());
        RevisionId::legacy(self.legacy_sequence().saturating_add(1))
    }

    /// Predecessor in global session-0 order (`ZERO` maps to itself).
    pub fn predecessor(self) -> RevisionId {
        if self.is_global_legacy() {
            RevisionId::legacy(self.legacy_sequence().saturating_sub(1))
        } else {
            RevisionId(HlcStamp {
                sequence: self.0.sequence.saturating_sub(1),
                ..self.0
            })
        }
    }

    /// Gap in global sequence space (Phase 1 lag / range math only).
    pub fn global_sequence_gap(self, earlier: RevisionId) -> u64 {
        debug_assert!(self.is_global_legacy() && earlier.is_global_legacy());
        self.legacy_sequence()
            .saturating_sub(earlier.legacy_sequence())
    }
}

impl PartialOrd for RevisionId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RevisionId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl Serialize for RevisionId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.legacy_sequence().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RevisionId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let seq = u64::deserialize(deserializer)?;
        Ok(RevisionId::legacy(seq))
    }
}

// Phase 1 wire compat: global-stream revisions encode as legacy u64 (D-P4).
// v5 u128 wire is handled at block boundaries via [`revision_codec`] and session WAL frames.

impl Encode for RevisionId {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        self.legacy_sequence().encode(encoder)
    }
}

impl<Context> Decode<Context> for RevisionId {
    fn decode<D: Decoder<Context = Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let seq = u64::decode(decoder)?;
        Ok(RevisionId::legacy(seq))
    }
}

impl<'de, Context> BorrowDecode<'de, Context> for RevisionId {
    fn borrow_decode<D: BorrowDecoder<'de, Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        let seq = u64::borrow_decode(decoder)?;
        Ok(RevisionId::legacy(seq))
    }
}

/// An N-dimensional coordinate in unsigned integer space (max 16 dims).
/// `u32` coordinates are used so Hilbert encoding can operate on them directly.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct DimensionVector {
    pub coords: Vec<u32>,
}

impl DimensionVector {
    /// Construct a dimension vector. Supports up to 16 dimensions.
    pub fn new(coords: Vec<u32>) -> Self {
        assert!(
            coords.len() <= 16,
            "DimensionVector exceeds maximum of 16 dimensions"
        );
        Self { coords }
    }

    /// Number of dimensions in this coordinate.
    pub fn dims(&self) -> usize {
        self.coords.len()
    }

    /// Return the coordinate value at index `idx`.
    pub fn coord(&self, idx: usize) -> u32 {
        self.coords[idx]
    }

    /// Returns true if this point is within [min, max] inclusive on every axis.
    pub fn within(&self, min: &DimensionVector, max: &DimensionVector) -> bool {
        assert_eq!(self.dims(), min.dims());
        assert_eq!(self.dims(), max.dims());
        self.coords
            .iter()
            .zip(min.coords.iter().zip(max.coords.iter()))
            .all(|(&v, (&lo, &hi))| v >= lo && v <= hi)
    }
}

/// The primary key for a record: which space + where in that space.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct Address {
    pub space: SpaceId,
    pub point: DimensionVector,
}

impl Address {
    /// Construct a new address from its space and coordinate point.
    pub fn new(space: SpaceId, point: DimensionVector) -> Self {
        Self { space, point }
    }
}

#[cfg(test)]
mod revision_bincode_tests {
    use super::*;
    use bincode::{config::standard, decode_from_slice, encode_to_vec};

    #[test]
    fn legacy_revision_bincode_roundtrip() {
        let rev = RevisionId::legacy(1);
        let bytes = encode_to_vec(rev, standard()).unwrap();
        let (decoded, consumed) = decode_from_slice::<RevisionId, _>(&bytes, standard()).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, rev);
    }
}
