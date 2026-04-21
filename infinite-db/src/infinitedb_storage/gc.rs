//! Garbage collection: tombstone pruning and version lifecycle management.
//!
//! GC has two jobs:
//!
//! 1. **Tombstone pruning**: once a tombstoned record's revision is older than
//!    the retention horizon, it can be dropped entirely from compacted output.
//!
//! 2. **Block deletion**: after compaction writes new blocks and they are
//!    durably recorded in the WAL, the old (superseded) block files can be
//!    deleted from disk via the `BlockStore`.
//!
//! GC never deletes blocks that are still referenced by any live snapshot.

use std::collections::HashSet;
use crate::infinitedb_core::{
    address::RevisionId,
    block::{BlockId, Record},
    snapshot::Snapshot,
};

/// Policy controlling what GC is allowed to delete.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Revisions strictly older than this horizon may have their tombstones pruned.
    /// Set to `RevisionId::ZERO` to disable tombstone pruning.
    pub tombstone_horizon: RevisionId,
    /// Revisions strictly older than this horizon may be dropped in non-history mode.
    /// Set to `RevisionId::ZERO` to keep everything.
    pub version_horizon: RevisionId,
}

impl RetentionPolicy {
    /// Keep all history forever (safe default).
    pub fn keep_all() -> Self {
        Self {
            tombstone_horizon: RevisionId::ZERO,
            version_horizon: RevisionId::ZERO,
        }
    }
}

/// Filter a list of records according to the retention policy.
/// Called by the compaction layer before assembling output blocks.
pub fn apply_retention(records: Vec<Record>, policy: &RetentionPolicy) -> Vec<Record> {
    records
        .into_iter()
        .filter(|r| {
            // Drop tombstones older than the horizon.
            if r.tombstone && policy.tombstone_horizon > RevisionId::ZERO {
                return r.revision >= policy.tombstone_horizon;
            }
            true
        })
        .collect()
}

/// Determine which block IDs from `superseded` are safe to delete.
///
/// A block is safe to delete only if it is not referenced by any live snapshot.
pub fn safe_to_delete(
    superseded: &[BlockId],
    live_snapshots: &[Snapshot],
) -> Vec<BlockId> {
    // Collect all block IDs still referenced by any snapshot.
    let referenced: HashSet<BlockId> = live_snapshots
        .iter()
        .flat_map(|s| s.blocks.values().copied())
        .collect();

    superseded
        .iter()
        .filter(|id| !referenced.contains(id))
        .copied()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::{
        address::{Address, DimensionVector, RevisionId, SpaceId},
        block::Record,
    };

    fn make_record(rev: u64, tombstone: bool) -> Record {
        Record {
            address: Address::new(SpaceId(1), DimensionVector::new(vec![0, 0])),
            revision: RevisionId(rev),
            data: vec![],
            tombstone,
        }
    }

    #[test]
    fn prunes_old_tombstones() {
        let records = vec![
            make_record(1, true),  // tombstone, old
            make_record(5, true),  // tombstone, recent
            make_record(3, false), // live record
        ];
        let policy = RetentionPolicy {
            tombstone_horizon: RevisionId(5),
            version_horizon: RevisionId::ZERO,
        };
        let kept = apply_retention(records, &policy);
        // Only tombstone at rev 5 and the live record survive.
        assert_eq!(kept.len(), 2);
        assert!(kept.iter().all(|r| !r.tombstone || r.revision.0 >= 5));
    }

    #[test]
    fn keeps_all_when_horizon_is_zero() {
        let records = vec![make_record(1, true), make_record(2, false)];
        let policy = RetentionPolicy::keep_all();
        let kept = apply_retention(records, &policy);
        assert_eq!(kept.len(), 2);
    }
}
