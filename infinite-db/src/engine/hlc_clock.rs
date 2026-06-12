//! Per-session Hybrid Logical Clock (peer track Phase 2, D-P3).

use std::time::{SystemTime, UNIX_EPOCH};

use crate::infinitedb_core::hlc::{HlcStamp, SessionId};

/// Default tolerated forward clock skew (milliseconds).
pub const DEFAULT_MAX_DRIFT_MS: u64 = 5 * 60 * 1000;

/// Contention-free clock state for one asserting session.
#[derive(Debug, Clone)]
pub struct HlcClock {
    session: SessionId,
    last_physical_ms: u64,
    last_logical: u16,
    sequence: u32,
    max_drift_ms: u64,
}

impl HlcClock {
    pub fn new(session: SessionId) -> Self {
        Self {
            session,
            last_physical_ms: 0,
            last_logical: 0,
            sequence: 0,
            max_drift_ms: DEFAULT_MAX_DRIFT_MS,
        }
    }

    /// Issue the next strictly-increasing stamp for this session.
    pub fn stamp(&mut self) -> HlcStamp {
        let physical = wall_clock_ms();
        self.advance_hlc(physical);
        self.sequence = self.sequence.wrapping_add(1);
        HlcStamp {
            physical_ms: self.last_physical_ms,
            logical: self.last_logical,
            session: self.session.0,
            sequence: self.sequence,
        }
    }

    /// Issue `count` contiguous stamps (sequence component advances each time).
    pub fn stamp_n(&mut self, count: u64) -> Vec<HlcStamp> {
        let mut out = Vec::with_capacity(count as usize);
        for _ in 0..count {
            out.push(self.stamp());
        }
        out
    }

    fn advance_hlc(&mut self, physical: u64) {
        let max_allowed = self.last_physical_ms.saturating_add(self.max_drift_ms);
        let physical = physical.min(max_allowed);
        if physical > self.last_physical_ms {
            self.last_physical_ms = physical;
            self.last_logical = 0;
        } else {
            self.last_logical = self.last_logical.wrapping_add(1);
        }
    }
}

fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    impl HlcClock {
        fn with_max_drift_ms(mut self, max_drift_ms: u64) -> Self {
            self.max_drift_ms = max_drift_ms;
            self
        }
    }

    #[test]
    fn stamps_monotone_under_fixed_physical() {
        let mut clock = HlcClock::new(SessionId(7)).with_max_drift_ms(u64::MAX);
        clock.last_physical_ms = 1_000;
        clock.last_logical = 0;
        let a = clock.stamp();
        let b = clock.stamp();
        assert!(a < b);
        assert_eq!(a.session, 7);
        assert_eq!(b.session, 7);
    }

    #[test]
    fn backward_physical_bumps_logical() {
        let mut clock = HlcClock::new(SessionId(1)).with_max_drift_ms(0);
        clock.last_physical_ms = 5_000;
        clock.last_logical = 2;
        clock.sequence = 1;
        let s = clock.stamp();
        assert_eq!(s.physical_ms, 5_000);
        assert_eq!(s.logical, 3);
        assert_eq!(s.sequence, 2);
    }
}
