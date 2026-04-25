//! Delta protocol: compute and apply the minimal diff between two snapshots.
//!
//! Sync flow between a local node (L) and a remote node (R):
//!   1. L sends its `MerkleTree` root to R.
//!   2. If roots match, done.
//!   3. R calls `diff_leaves()` to find which block hashes differ.
//!   4. R sends a `Delta` containing only the differing blocks.
//!   5. L calls `apply()` to merge the delta into its snapshot.
//!
//! This means only changed blocks are transferred, regardless of total db size.

use bincode::{Decode, Encode};
use crate::infinitedb_core::{
    address::RevisionId,
    block::{Block, BlockId},
    snapshot::{Snapshot, SnapshotId},
};

/// A set of blocks to be applied to bring a snapshot up to date.
#[derive(Debug, Encode, Decode)]
pub struct Delta {
    /// The snapshot this delta was computed from (the remote's head).
    pub source_snapshot: SnapshotId,
    /// The snapshot this delta targets (what the receiver should end up with).
    pub target_snapshot: SnapshotId,
    /// Blocks present in the remote but absent or stale in the receiver.
    pub added_blocks: Vec<Block>,
    /// Block IDs that the receiver should remove (superseded on remote).
    pub removed_block_ids: Vec<BlockId>,
    /// The revision at which this delta was produced.
    pub at_revision: RevisionId,
}

impl Delta {
    /// Compute the delta between two snapshots.
    ///
    /// `source` is what the remote has; `target` is what the local node has.
    /// Returns the blocks the local node needs to add and which to remove.
    pub fn compute(source: &Snapshot, target: &Snapshot, source_blocks: Vec<Block>) -> Self {
        // Blocks in source but not in target → need to be added.
        let added_blocks: Vec<Block> = source_blocks
            .into_iter()
            .filter(|b| !target.blocks.values().any(|id| *id == b.id))
            .collect();

        // Block IDs in target but not in source → need to be removed.
        let removed_block_ids: Vec<BlockId> = target
            .blocks
            .values()
            .filter(|id| !source.blocks.values().any(|s_id| s_id == *id))
            .copied()
            .collect();

        Delta {
            source_snapshot: source.id,
            target_snapshot: target.id,
            added_blocks,
            removed_block_ids,
            at_revision: source.revision,
        }
    }

    /// Apply this delta to `snapshot`, producing an updated snapshot.
    ///
    /// The caller is responsible for writing `added_blocks` to the `BlockStore`
    /// and deleting `removed_block_ids` via GC after the new snapshot is durable.
    pub fn apply(&self, snapshot: &Snapshot) -> Snapshot {
        use std::collections::BTreeMap;

        // Start from a clone of the current snapshot.
        let mut blocks: BTreeMap<u128, BlockId> = snapshot.blocks.clone();

        // Remove blocks that the remote no longer has.
        blocks.retain(|_, id| !self.removed_block_ids.contains(id));

        // Add blocks from the remote (keyed by their minimum Hilbert address).
        // We use the block's ID as a stand-in key here; the index layer would
        // derive the real Hilbert key from the block's first record.
        for block in &self.added_blocks {
            blocks.insert(block.id.0 as u128, block.id);
        }

        Snapshot {
            id: self.target_snapshot,
            space: snapshot.space,
            revision: self.at_revision,
            parent: Some(snapshot.id),
            blocks,
        }
    }

    /// Return `true` when this delta has no changes.
    pub fn is_empty(&self) -> bool {
        self.added_blocks.is_empty() && self.removed_block_ids.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use crate::infinitedb_core::{
        address::{RevisionId, SpaceId},
        block::{Block, BlockId},
        snapshot::{Snapshot, SnapshotId},
    };

    fn empty_snapshot(id: u64) -> Snapshot {
        Snapshot {
            id: SnapshotId(id),
            space: SpaceId(1),
            revision: RevisionId(id),
            parent: None,
            blocks: BTreeMap::new(),
        }
    }

    fn make_block(id: u64) -> Block {
        Block {
            id: BlockId(id),
            space: SpaceId(1),
            records: vec![],
            min_revision: RevisionId::ZERO,
            max_revision: RevisionId::ZERO,
            checksum: [0u8; 32],
        }
    }

    #[test]
    fn delta_adds_new_blocks() {
        let mut source = empty_snapshot(2);
        source.blocks.insert(10, BlockId(10));
        let target = empty_snapshot(1);

        let delta = Delta::compute(&source, &target, vec![make_block(10)]);
        assert_eq!(delta.added_blocks.len(), 1);
        assert!(delta.removed_block_ids.is_empty());

        let updated = delta.apply(&target);
        assert!(updated.blocks.values().any(|id| *id == BlockId(10)));
    }

    #[test]
    fn empty_delta_when_in_sync() {
        let mut source = empty_snapshot(1);
        source.blocks.insert(5, BlockId(5));
        let mut target = empty_snapshot(1);
        target.blocks.insert(5, BlockId(5));

        let delta = Delta::compute(&source, &target, vec![]);
        assert!(delta.is_empty());
    }
}
