//! Hybrid Logical Clock stamp — 128-bit revision composite (peer track Phase 1).
//!
//! Layout (D-P1): 48-bit physical ms | 16-bit logical | 32-bit session | 32-bit sequence.

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// Reserved session for the pre-HLC global allocation stream (D-P4).
pub const GLOBAL_SESSION: u32 = 0;

/// Legacy-epoch physical ceiling — embedded stamps stay below real wall-clock HLC era.
pub const LEGACY_PHYSICAL_CEILING: u64 = 31_536_000_000; // ~year 2970, well above spill range

/// Asserting stream identity (D-P2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct SessionId(pub u32);

/// 128-bit HLC stamp — lexicographic order is total order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct HlcStamp {
    pub physical_ms: u64,
    pub logical: u16,
    pub session: u32,
    pub sequence: u32,
}

impl PartialOrd for HlcStamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HlcStamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.physical_ms
            .cmp(&other.physical_ms)
            .then(self.logical.cmp(&other.logical))
            .then(self.session.cmp(&other.session))
            .then(self.sequence.cmp(&other.sequence))
    }
}

impl HlcStamp {
    pub const ZERO: Self = Self {
        physical_ms: 0,
        logical: 0,
        session: GLOBAL_SESSION,
        sequence: 0,
    };

    /// Embed a dense pre-HLC `u64` revision (D-P4, lossless via physical spill).
    pub const fn legacy(sequence: u64) -> Self {
        Self {
            physical_ms: sequence >> 32,
            logical: 0,
            session: GLOBAL_SESSION,
            sequence: sequence as u32,
        }
    }

    /// Global-stream stamp from the monotonic allocator (Phase 1).
    pub fn global_sequence(sequence: u32) -> Self {
        Self {
            physical_ms: 0,
            logical: 0,
            session: GLOBAL_SESSION,
            sequence,
        }
    }

    pub fn is_legacy_embedded(self) -> bool {
        self.logical == 0
            && self.session == GLOBAL_SESSION
            && self.physical_ms < LEGACY_PHYSICAL_CEILING
    }

    pub fn legacy_sequence(self) -> u64 {
        (self.physical_ms << 32) | (self.sequence as u64)
    }

    /// Pack into a single `u128` (codec membrane only).
    pub fn pack(self) -> u128 {
        let physical = (self.physical_ms & 0x0000_FFFF_FFFF_FFFF) as u128;
        let logical = self.logical as u128;
        let session = self.session as u128;
        let sequence = self.sequence as u128;
        (physical << 80) | (logical << 64) | (session << 32) | sequence
    }

    pub fn unpack(packed: u128) -> Self {
        Self {
            physical_ms: ((packed >> 80) & 0x0000_FFFF_FFFF_FFFF) as u64,
            logical: ((packed >> 64) & 0xFFFF) as u16,
            session: ((packed >> 32) & 0xFFFF_FFFF) as u32,
            sequence: (packed & 0xFFFF_FFFF) as u32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_orders_before_hlc_era() {
        let legacy = HlcStamp::legacy(100);
        let hlc = HlcStamp {
            physical_ms: 1,
            logical: 0,
            session: 1,
            sequence: 0,
        };
        assert!(legacy < hlc);
    }

    #[test]
    fn pack_roundtrip() {
        let s = HlcStamp {
            physical_ms: 1_700_000_000_000,
            logical: 3,
            session: 42,
            sequence: 7,
        };
        assert_eq!(HlcStamp::unpack(s.pack()), s);
    }

    #[test]
    fn global_sequence_monotone() {
        let a = HlcStamp::global_sequence(1);
        let b = HlcStamp::global_sequence(2);
        assert!(a < b);
    }
}
