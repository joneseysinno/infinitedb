//! Void algebra — the typed absence primitive (D-V1–D-V3).
//!
//! Three laws (arithmetic analogy):
//! - **Identity:** merging void into any region leaves it unchanged (`x ∪ void = x`).
//! - **Annihilator:** cross-container joins against void yield void (`x ∩ void = void`).
//! - **Undefined division:** ratio-shaped values over void are undefined, never silently zero.
//!
//! [`Presence`] names storage absence at an address (Void / Tombstoned / Present).
//! [`VoidOr`] propagates derived-computation absence through pipelines.

use bincode::{Decode, Encode};

use super::address::RevisionId;
use super::block::Record;

/// Three-tier point presence at a revision (D-V2, D-V4).
#[derive(Debug, Clone, Encode, Decode)]
pub enum Presence {
    /// Address never written.
    Void,
    /// Written then logically deleted; carries the deleting revision.
    Tombstoned { last: RevisionId },
    /// Live record at the pinned revision.
    Present(Record),
}

/// Derived-computation absence propagator (D-V6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum VoidOr<T> {
    Void,
    Known(T),
}

impl<T> VoidOr<T> {
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> VoidOr<U> {
        match self {
            VoidOr::Void => VoidOr::Void,
            VoidOr::Known(v) => VoidOr::Known(f(v)),
        }
    }

    pub fn and_then<U, F: FnOnce(T) -> VoidOr<U>>(self, f: F) -> VoidOr<U> {
        match self {
            VoidOr::Void => VoidOr::Void,
            VoidOr::Known(v) => f(v),
        }
    }

    /// Propagates `Void` if either operand is `Void` (INV-VOID-PROPAGATE).
    pub fn zip_with<U, V, F: FnOnce(T, U) -> V>(self, other: VoidOr<U>, f: F) -> VoidOr<V> {
        match (self, other) {
            (VoidOr::Known(a), VoidOr::Known(b)) => VoidOr::Known(f(a, b)),
            _ => VoidOr::Void,
        }
    }

    pub fn is_void(&self) -> bool {
        matches!(self, VoidOr::Void)
    }

    pub fn known(self) -> Option<T> {
        match self {
            VoidOr::Known(v) => Some(v),
            VoidOr::Void => None,
        }
    }
}

/// Classify point presence from revision history at one address (D-V5).
///
/// `records` must be tombstone-inclusive history for a single address, already
/// filtered to the revision ceiling.
pub fn classify_presence(records: &[Record]) -> Presence {
    let latest = records.iter().max_by_key(|r| r.revision);
    match latest {
        None => Presence::Void,
        Some(r) if r.tombstone => Presence::Tombstoned { last: r.revision },
        Some(r) => Presence::Present(r.clone()),
    }
}

/// Container-level void predicate — polymorphic over addressable containers (D-V3).
pub trait VoidState {
    /// `true` when the container has never held data (no revision history).
    ///
    /// A container that was written then fully tombstoned is **not** void — it is
    /// emptied history (D-V2).
    fn is_void(&self) -> bool;
}

/// Steady-state read view of one space's revision history.
#[derive(Debug, Clone, Copy)]
pub struct SpaceHistoryView<'a> {
    /// Full revision history including tombstones.
    pub history: &'a [Record],
}

impl VoidState for SpaceHistoryView<'_> {
    fn is_void(&self) -> bool {
        self.history.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::address::{Address, DimensionVector, SpaceId};

    fn record(rev: u64, tombstone: bool) -> Record {
        Record {
            address: Address::new(SpaceId(1), DimensionVector::new(vec![0, 0])),
            revision: RevisionId::legacy(rev),
            data: if tombstone { vec![] } else { vec![1] },
            tombstone,
            hilbert_key: Default::default(),
        }
    }

    #[test]
    fn zip_with_propagates_void_both_sides() {
        assert!(matches!(
            VoidOr::Known(1).zip_with(VoidOr::<i32>::Void, |a, b| a + b),
            VoidOr::Void
        ));
        assert!(matches!(
            VoidOr::<i32>::Void.zip_with(VoidOr::Known(2), |a, b| a + b),
            VoidOr::Void
        ));
        assert_eq!(
            VoidOr::Known(1).zip_with(VoidOr::Known(2), |a, b| a + b),
            VoidOr::Known(3)
        );
    }

    #[test]
    fn map_preserves_void() {
        assert!(matches!(VoidOr::<i32>::Void.map(|x| x + 1), VoidOr::Void));
        assert_eq!(VoidOr::Known(2).map(|x| x + 1), VoidOr::Known(3));
    }

    #[test]
    fn classify_presence_three_states() {
        assert!(matches!(classify_presence(&[]), Presence::Void));
        let live = classify_presence(&[record(1, false)]);
        assert!(matches!(live, Presence::Present(_)));
        let tomb = classify_presence(&[record(1, false), record(2, true)]);
        assert!(matches!(tomb, Presence::Tombstoned { .. }));
    }

    #[test]
    fn presence_exhaustive_match_enforced() {
        fn tag(p: Presence) -> &'static str {
            match p {
                Presence::Void => "void",
                Presence::Tombstoned { .. } => "tombstoned",
                Presence::Present(_) => "present",
            }
        }
        assert_eq!(tag(Presence::Void), "void");
    }

    #[test]
    fn space_history_view_void_vs_emptied() {
        assert!(SpaceHistoryView { history: &[] }.is_void());
        assert!(!SpaceHistoryView {
            history: &[record(1, true)]
        }
        .is_void());
    }
}
