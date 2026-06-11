//! In-memory live tail published by the I/O thread after fsync.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::infinitedb_core::{
    address::RevisionId,
    block::Record,
    hilbert_key::HilbertKey,
    record_identity::RecordIdentityKey,
    snapshot::BlockIndexEntry,
};

use super::shard_view::{ShardView, ShardViewHandle, TailChunk};

/// Records visible to readers before hot segment seal.
///
/// Internally pairs sealed block index entries with the live tail in a single
/// [`ShardViewHandle`] so readers observe a consistent view.
pub struct LiveTailView {
    view: ShardViewHandle,
}

impl LiveTailView {
    pub fn new() -> Self {
        Self {
            view: ShardViewHandle::new(),
        }
    }

    /// Load the atomic shard view for consistent reads.
    pub fn load_view(&self) -> arc_swap::Guard<Arc<ShardView>> {
        self.view.load()
    }

    /// Load shared tail chunks (zero-copy read path).
    pub fn load(&self) -> Arc<Vec<TailChunk>> {
        self.view.load_tail_chunks()
    }

    /// Replace the visible tail (called from the I/O thread on recovery).
    pub fn publish(&self, records: Vec<Record>) {
        self.view.publish_tail(records);
    }

    /// Append one durable record to the published tail.
    pub fn append(&self, record: Record) {
        self.view.append(record);
    }

    /// Publish a group of durable records in one chunk.
    pub fn extend_chunk(&self, records: Vec<Record>) {
        self.view.extend_chunk(records);
    }

    /// Publish a new sealed block and truncated tail atomically.
    pub fn seal(
        &self,
        block_min_key: HilbertKey,
        entry: BlockIndexEntry,
        sealed: &HashSet<RecordIdentityKey>,
    ) {
        self.view.seal(block_min_key, entry, sealed);
    }

    /// Initialize sealed blocks from recovery.
    pub fn init_blocks(&self, blocks: BTreeMap<HilbertKey, BlockIndexEntry>) {
        self.view.init_blocks(blocks);
    }

    /// Explicit clone of all tail records (prefer [`load`] for zero-copy read paths).
    pub fn snapshot(&self) -> Vec<Record> {
        self.load_view().tail_vec()
    }

    pub fn captured_at(&self) -> RevisionId {
        self.view.captured_at()
    }
}

impl Default for LiveTailView {
    fn default() -> Self {
        Self::new()
    }
}
