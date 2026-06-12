//! Atomic per-shard read view pairing sealed blocks and live tail.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::query::record_identity_key;
use arc_swap::ArcSwap;

use crate::infinitedb_core::{
    address::RevisionId,
    block::Record,
    hilbert_key::HilbertKey,
    record_identity::RecordIdentityKey,
    snapshot::BlockIndexEntry,
    space::SpaceRegistry,
};

/// Immutable chunk of tail records published in one group commit.
pub type TailChunk = Arc<Vec<Record>>;

/// Consistent snapshot of one shard's sealed blocks and live tail.
#[derive(Debug, Clone)]
pub struct ShardView {
    /// Sealed blocks contributed by this shard (Hilbert min key → index entry).
    pub blocks: Arc<BTreeMap<HilbertKey, BlockIndexEntry>>,
    /// Append-only chunks of records not yet sealed into a block.
    pub tail_chunks: Arc<Vec<TailChunk>>,
}

impl ShardView {
    /// Empty view with no blocks or tail records.
    pub fn empty() -> Self {
        Self {
            blocks: Arc::new(BTreeMap::new()),
            tail_chunks: Arc::new(Vec::new()),
        }
    }

    /// Iterate all tail records without flattening into a new vector.
    pub fn tail_iter(&self) -> impl Iterator<Item = &Record> {
        self.tail_chunks.iter().flat_map(|chunk| chunk.iter())
    }

    /// Total number of records in the live tail.
    pub fn tail_len(&self) -> usize {
        self.tail_chunks.iter().map(|c| c.len()).sum()
    }

    /// Collect tail records into a new vector (explicit clone when ownership is needed).
    pub fn tail_vec(&self) -> Vec<Record> {
        self.tail_iter().cloned().collect()
    }
}

/// Writer-owned handle; readers call [`ShardViewHandle::load`] once per query.
pub struct ShardViewHandle {
    view: ArcSwap<ShardView>,
    captured_at: AtomicU64,
}

impl ShardViewHandle {
    /// Create an empty shard view.
    pub fn new() -> Self {
        Self {
            view: ArcSwap::from_pointee(ShardView::empty()),
            captured_at: AtomicU64::new(0),
        }
    }

    /// Load the current consistent view (call once per shard per query).
    pub fn load(&self) -> arc_swap::Guard<Arc<ShardView>> {
        self.view.load()
    }

    /// Load tail chunks shared with the writer (zero-copy read path).
    pub fn load_tail_chunks(&self) -> Arc<Vec<TailChunk>> {
        Arc::clone(&self.view.load().tail_chunks)
    }

    /// Replace the entire view atomically.
    pub fn publish(&self, view: ShardView) {
        let max_rev = view
            .tail_iter()
            .map(|r| r.revision.legacy_sequence())
            .max()
            .unwrap_or(0);
        self.view.store(Arc::new(view));
        self.captured_at.store(max_rev, Ordering::Release);
    }

    /// Append one durable record as a single-record chunk.
    pub fn append(&self, record: Record) {
        self.extend_chunk(vec![record]);
    }

    /// Publish a group of durable records as one immutable chunk.
    pub fn extend_chunk(&self, records: Vec<Record>) {
        if records.is_empty() {
            return;
        }
        let current = self.view.load();
        let mut chunks = current.tail_chunks.as_ref().clone();
        chunks.push(Arc::new(records));
        let max_rev = chunks
            .iter()
            .flat_map(|c| c.iter())
            .map(|r| r.revision.legacy_sequence())
            .max()
            .unwrap_or(0);
        self.view.store(Arc::new(ShardView {
            blocks: Arc::clone(&current.blocks),
            tail_chunks: Arc::new(chunks),
        }));
        self.captured_at.store(max_rev, Ordering::Release);
    }

    /// Publish a new block and truncated tail in one swap.
    pub fn seal(
        &self,
        block_min_key: HilbertKey,
        entry: BlockIndexEntry,
        sealed: &HashSet<RecordIdentityKey>,
        spaces: &SpaceRegistry,
    ) {
        let current = self.view.load();
        let mut blocks = current.blocks.as_ref().clone();
        blocks.insert(block_min_key, entry);
        let remainder: Vec<Record> = current
            .tail_iter()
            .filter(|r| !sealed.contains(&record_identity_key(spaces, r)))
            .cloned()
            .collect();
        let chunks = if remainder.is_empty() {
            Vec::new()
        } else {
            vec![Arc::new(remainder)]
        };
        let max_rev = chunks
            .iter()
            .flat_map(|c| c.iter())
            .map(|r| r.revision.legacy_sequence())
            .max()
            .unwrap_or(0);
        self.view.store(Arc::new(ShardView {
            blocks: Arc::new(blocks),
            tail_chunks: Arc::new(chunks),
        }));
        self.captured_at.store(max_rev, Ordering::Release);
    }

    /// Replace tail with a single chunk (recovery bootstrap).
    pub fn publish_tail(&self, tail: Vec<Record>) {
        let current = self.view.load();
        let chunks = if tail.is_empty() {
            Vec::new()
        } else {
            vec![Arc::new(tail)]
        };
        self.publish(ShardView {
            blocks: Arc::clone(&current.blocks),
            tail_chunks: Arc::new(chunks),
        });
    }

    /// Initialize blocks from recovery (tail unchanged).
    pub fn init_blocks(&self, blocks: BTreeMap<HilbertKey, BlockIndexEntry>) {
        let current = self.view.load();
        self.publish(ShardView {
            blocks: Arc::new(blocks),
            tail_chunks: Arc::clone(&current.tail_chunks),
        });
    }

    /// Highest revision in the published tail.
    pub fn captured_at(&self) -> RevisionId {
        RevisionId::legacy(self.captured_at.load(Ordering::Acquire))
    }
}

impl Default for ShardViewHandle {
    fn default() -> Self {
        Self::new()
    }
}
