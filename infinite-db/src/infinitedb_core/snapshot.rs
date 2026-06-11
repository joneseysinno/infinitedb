use bincode::{Decode, Encode};
use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};
use super::address::{Address, RevisionId, SpaceId};
use super::block::BlockId;
use super::hilbert_key::HilbertKey;

/// Stable identifier for a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct SnapshotId(pub u64);

/// An entry in a snapshot's block index.
///
/// The map key is the block's minimum Hilbert address; this entry carries the
/// block ID plus its maximum Hilbert address so range queries can prune to
/// blocks whose `[min_key, max_key]` interval overlaps the query interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct BlockIndexEntry {
    /// The block this entry points at.
    pub block_id: BlockId,
    /// The Hilbert key of the block's last (highest) record.
    pub max_key: HilbertKey,
}

/// Immutable snapshot value minted at meaningful boundaries (branch creation,
/// merge completion, sync negotiation).
///
/// The per-space entry in [`SnapshotStore`] is a *mutable head index* updated
/// in place as shards seal blocks; immutable [`Snapshot`] clones capture fixed
/// state for branches and replication. The snapshot does not copy record data —
/// it references block IDs that were live at `revision`.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct Snapshot {
    pub id: SnapshotId,
    pub space: SpaceId,
    /// The logical clock value this snapshot represents.
    pub revision: RevisionId,
    /// The parent snapshot this was derived from (None for the root).
    pub parent: Option<SnapshotId>,
    /// Ordered map of blocks visible at this revision.
    /// Key is the block's minimum Hilbert address for range routing; the value
    /// carries the block ID and its maximum Hilbert address.
    pub blocks: BTreeMap<HilbertKey, BlockIndexEntry>,
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
            .filter(|e| !other.blocks.values().any(|o| o.block_id == e.block_id))
            .map(|e| e.block_id)
            .collect()
    }

    /// Returns true if this snapshot contains blocks covering the given address.
    pub fn may_contain(&self, _address: &Address) -> bool {
        // Full spatial filtering is done via Hilbert keys in the index layer.
        // At this level we return a conservative true; the storage layer narrows it.
        !self.blocks.is_empty()
    }
}