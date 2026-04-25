/// Hybrid Logical Clock (HLC) timestamp for use as a temporal dimension.
///
/// An HLC combines a physical time component (milliseconds) with a logical
/// counter. This ensures:
///   - Timestamps are always monotonically increasing across restarts.
///   - Two events on the same node never share a timestamp.
///   - Distributed events compare correctly even with clock skew.
///
/// The HLC is encoded as a u64 (48-bit physical ms | 16-bit logical counter),
/// which is then used as a u32 coordinate via `to_coord()` for Hilbert indexing.
/// For storage and sync the full u64 is used.

/// A Hybrid Logical Clock timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HlcTimestamp {
    /// Physical component: milliseconds since Unix epoch (48 bits used).
    pub physical_ms: u64,
    /// Logical counter: disambiguates events at the same physical millisecond.
    pub logical: u16,
}

impl HlcTimestamp {
    /// Zero-valued timestamp used for initialization.
    pub const ZERO: HlcTimestamp = HlcTimestamp { physical_ms: 0, logical: 0 };

    /// Pack into a u64 for storage: high 48 bits = physical, low 16 = logical.
    pub fn to_u64(self) -> u64 {
        (self.physical_ms << 16) | self.logical as u64
    }

    /// Unpack from a stored u64.
    pub fn from_u64(v: u64) -> Self {
        Self {
            physical_ms: v >> 16,
            logical: (v & 0xFFFF) as u16,
        }
    }

    /// Encode to a u32 coordinate for Hilbert indexing.
    /// Truncates to the high 32 bits of the packed u64 (millisecond granularity).
    pub fn to_coord(self) -> u32 {
        (self.to_u64() >> 32) as u32
    }
}

/// Stateful HLC clock — call `tick()` before every write.
pub struct HlcClock {
    last: HlcTimestamp,
}

impl HlcClock {
    /// Create a new clock starting at [`HlcTimestamp::ZERO`].
    pub fn new() -> Self {
        Self { last: HlcTimestamp::ZERO }
    }

    /// Advance the clock using the current wall time (ms since epoch).
    /// The caller supplies `now_ms` so this remains testable without system calls.
    pub fn tick(&mut self, now_ms: u64) -> HlcTimestamp {
        if now_ms > self.last.physical_ms {
            self.last = HlcTimestamp { physical_ms: now_ms, logical: 0 };
        } else {
            // Clock skew or same ms: increment logical counter.
            self.last = HlcTimestamp {
                physical_ms: self.last.physical_ms,
                logical: self.last.logical.saturating_add(1),
            };
        }
        self.last
    }

    /// Receive a timestamp from a remote node and advance to stay ahead.
    pub fn receive(&mut self, remote: HlcTimestamp, now_ms: u64) -> HlcTimestamp {
        let max_physical = self.last.physical_ms.max(remote.physical_ms).max(now_ms);
        self.last = if max_physical == self.last.physical_ms
            && max_physical == remote.physical_ms
        {
            HlcTimestamp {
                physical_ms: max_physical,
                logical: self.last.logical.max(remote.logical).saturating_add(1),
            }
        } else if max_physical == self.last.physical_ms {
            HlcTimestamp {
                physical_ms: max_physical,
                logical: self.last.logical.saturating_add(1),
            }
        } else if max_physical == remote.physical_ms {
            HlcTimestamp {
                physical_ms: max_physical,
                logical: remote.logical.saturating_add(1),
            }
        } else {
            HlcTimestamp { physical_ms: max_physical, logical: 0 }
        };
        self.last
    }
}

impl Default for HlcClock {
    /// Create a new clock with zero timestamp.
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_is_monotonic() {
        let mut clk = HlcClock::new();
        let t1 = clk.tick(1000);
        let t2 = clk.tick(1000); // same ms
        let t3 = clk.tick(2000);
        assert!(t1 < t2);
        assert!(t2 < t3);
    }

    #[test]
    fn roundtrip_u64() {
        let ts = HlcTimestamp { physical_ms: 1_700_000_000_000, logical: 42 };
        assert_eq!(HlcTimestamp::from_u64(ts.to_u64()), ts);
    }
}