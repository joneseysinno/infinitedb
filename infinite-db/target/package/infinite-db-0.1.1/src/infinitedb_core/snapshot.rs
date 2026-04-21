use bincode::{Decode, Encode};
use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};
use super::address::{Address, RevisionId, SpaceId};
use super::block::BlockId;

/// Stable identifier for a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct SnapshotId(pub u64);

/// A consistent, point-in-time view of a space.
/// The snapshot does not copy record data — it references block IDs that
/// were live at `revision`. The storage layer resolves block IDs to data.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct Snapshot {
    pub id: SnapshotId,
    pub space: SpaceId,
    /// The logical clock value this snapshot represents.
    pub revision: RevisionId,
    /// The parent snapshot this was derived from (None for the root).
    pub parent: Option<SnapshotId>,
    /// Ordered map of block IDs visible at this revision.
    /// Key is the block's minimum Hilbert address for range routing.
    pub blocks: BTreeMap<u128, BlockId>,
}

impl Snapshot {
    /// Create the initial empty snapshot for a space.
    pub fn root(id: SnapshotId, space: SpaceId) -> Self {
        Self {
            id,
            space,
            revision: RevisionId::ZERO,
            parent: None,
            blocks: BTreeMap::new(),
        }
    }

    /// Returns the set of block IDs present in `self` but not in `other`.
    /// Used by the sync layer to compute what needs to be transferred.
    pub fn diff_blocks(&self, other: &Snapshot) -> Vec<BlockId> {
        self.blocks
            .values()
            .filter(|id| !other.blocks.values().any(|o| o == *id))
            .copied()
            .collect()
    }

    /// Returns true if this snapshot contains blocks covering the given address.
    pub fn may_contain(&self, _address: &Address) -> bool {
        // Full spatial filtering is done via Hilbert keys in the index layer.
        // At this level we return a conservative true; the storage layer narrows it.
        !self.blocks.is_empty()
    }
}