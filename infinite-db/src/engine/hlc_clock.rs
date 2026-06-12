//! Per-session Hybrid Logical Clock (peer track Phase 2, D-P3; hardening Wave D).

use std::time::{SystemTime, UNIX_EPOCH};

use crate::infinitedb_core::hlc::{HlcStamp, SessionId, LEGACY_PHYSICAL_CEILING};

/// Default tolerated backward clock skew (milliseconds) before refusing to stamp.
pub const DEFAULT_MAX_DRIFT_MS: u64 = 5 * 60 * 1000;

/// Clock refused to issue a stamp (hardening D-P3 / T17).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClockSkewError {
    /// `last_physical` exceeds wall by more than the drift bound — suspected poison.
    PoisonedPhysical { wall_ms: u64, last_physical_ms: u64 },
    /// Wall clock precedes epoch or falls in the legacy embedding region.
    PreEpochWall { wall_ms: u64 },
}

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

    /// Absorb an observed remote stamp (standard HLC `receive`, hardening T16).
    #[allow(dead_code)]
    pub fn receive(&mut self, observed: HlcStamp) {
        if observed.session != self.session.0 {
            self.advance_hlc(observed.physical_ms);
            if observed.physical_ms == self.last_physical_ms && observed.logical >= self.last_logical {
                self.last_logical = observed.logical.saturating_add(1);
            }
        } else {
            self.advance_hlc(observed.physical_ms);
            if observed.physical_ms == self.last_physical_ms {
                self.last_logical = self.last_logical.max(observed.logical).saturating_add(1);
            }
        }
    }

    /// Issue the next strictly-increasing stamp for this session.
    pub fn stamp(&mut self) -> Result<HlcStamp, ClockSkewError> {
        let physical = wall_clock_ms()?;
        self.validate_wall(physical)?;
        self.advance_hlc(physical);
        self.sequence = self.sequence.wrapping_add(1);
        Ok(HlcStamp {
            physical_ms: self.last_physical_ms,
            logical: self.last_logical,
            session: self.session.0,
            sequence: self.sequence,
        })
    }

    /// Issue `count` contiguous stamps (sequence component advances each time).
    pub fn stamp_n(&mut self, count: u64) -> Result<Vec<HlcStamp>, ClockSkewError> {
        let mut out = Vec::with_capacity(count as usize);
        for _ in 0..count {
            out.push(self.stamp()?);
        }
        Ok(out)
    }

    fn advance_hlc(&mut self, physical: u64) {
        if physical > self.last_physical_ms {
            self.last_physical_ms = physical;
            self.last_logical = 0;
        } else {
            let next_logical = self.last_logical.wrapping_add(1);
            if next_logical == 0 {
                self.last_physical_ms = self.last_physical_ms.saturating_add(1);
                self.last_logical = 0;
            } else {
                self.last_logical = next_logical;
            }
        }
    }

    fn validate_wall(&self, wall_ms: u64) -> Result<(), ClockSkewError> {
        if wall_ms < LEGACY_PHYSICAL_CEILING {
            return Err(ClockSkewError::PreEpochWall { wall_ms });
        }
        if self.last_physical_ms > wall_ms.saturating_add(self.max_drift_ms) {
            return Err(ClockSkewError::PoisonedPhysical {
                wall_ms,
                last_physical_ms: self.last_physical_ms,
            });
        }
        Ok(())
    }
}

fn wall_clock_ms() -> Result<u64, ClockSkewError> {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    if ms < LEGACY_PHYSICAL_CEILING {
        return Err(ClockSkewError::PreEpochWall { wall_ms: ms });
    }
    Ok(ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    impl HlcClock {
        fn with_max_drift_ms(mut self, max_drift_ms: u64) -> Self {
            self.max_drift_ms = max_drift_ms;
            self
        }

        fn stamp_with_wall(&mut self, wall_ms: u64) -> Result<HlcStamp, ClockSkewError> {
            self.validate_wall(wall_ms)?;
            self.advance_hlc(wall_ms);
            self.sequence = self.sequence.wrapping_add(1);
            Ok(HlcStamp {
                physical_ms: self.last_physical_ms,
                logical: self.last_logical,
                session: self.session.0,
                sequence: self.sequence,
            })
        }
    }

    const TEST_WALL: u64 = LEGACY_PHYSICAL_CEILING + 1_000_000_000;

    #[test]
    fn stamps_monotone_under_fixed_physical() {
        let mut clock = HlcClock::new(SessionId(7)).with_max_drift_ms(u64::MAX);
        clock.last_physical_ms = TEST_WALL;
        clock.last_logical = 0;
        let a = clock.stamp_with_wall(TEST_WALL).unwrap();
        let b = clock.stamp_with_wall(TEST_WALL).unwrap();
        assert!(a < b);
        assert_eq!(a.session, 7);
        assert_eq!(b.session, 7);
    }

    #[test]
    fn backward_physical_bumps_logical() {
        let wall = TEST_WALL + 4_000_000_000;
        let mut clock = HlcClock::new(SessionId(1)).with_max_drift_ms(u64::MAX);
        clock.last_physical_ms = wall;
        clock.last_logical = 2;
        clock.sequence = 1;
        let s = clock.stamp_with_wall(wall).unwrap();
        assert_eq!(s.physical_ms, wall);
        assert_eq!(s.logical, 3);
        assert_eq!(s.sequence, 2);
    }

    #[test]
    fn forward_jump_trusted() {
        let mut clock = HlcClock::new(SessionId(1)).with_max_drift_ms(u64::MAX);
        clock.last_physical_ms = TEST_WALL;
        let s = clock.stamp_with_wall(TEST_WALL + 7_200_000).unwrap();
        assert_eq!(s.physical_ms, TEST_WALL + 7_200_000);
    }

    #[test]
    fn logical_exhaustion_bumps_physical() {
        let wall = TEST_WALL + 4_000_000_000;
        let mut clock = HlcClock::new(SessionId(1)).with_max_drift_ms(u64::MAX);
        clock.last_physical_ms = wall;
        clock.last_logical = u16::MAX;
        clock.sequence = 1;
        let s = clock.stamp_with_wall(wall).unwrap();
        assert_eq!(s.physical_ms, wall + 1);
        assert_eq!(s.logical, 0);
    }

    #[test]
    fn receive_advances_past_remote() {
        let mut clock = HlcClock::new(SessionId(2)).with_max_drift_ms(u64::MAX);
        clock.last_physical_ms = TEST_WALL;
        let remote = TEST_WALL + 1_000_000_000;
        clock.receive(HlcStamp {
            physical_ms: remote,
            logical: 5,
            session: 99,
            sequence: 1,
        });
        let s = clock.stamp_with_wall(TEST_WALL + 500_000_000).unwrap();
        assert!(s.physical_ms >= remote);
    }
}
