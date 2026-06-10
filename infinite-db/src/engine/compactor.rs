//! Shard-local compaction triggered after seal.

use std::collections::BTreeMap;
use std::io;

use parking_lot::RwLock;

use crate::infinitedb_core::{
    address::SpaceId,
    snapshot::{BlockIndexEntry, SnapshotId},
    space::SpaceRegistry,
};
use crate::infinitedb_storage::{
    compaction::{compact, CompactionConfig},
    nvme::{compute_checksum, BlockStore},
};

use super::hilbert_shard::hilbert_shard_id;
use super::live_tail::LiveTailView;
use super::snapshot_store::SnapshotStore;

/// Compact small blocks in a shard after seal when block count exceeds threshold.
pub fn maybe_compact_after_seal(
    store: &BlockStore,
    snapshots: &SnapshotStore,
    live_tail: &LiveTailView,
    spaces: &RwLock<SpaceRegistry>,
    next_block_id: &std::sync::atomic::AtomicU64,
    space: SpaceId,
    shard_filter: Option<(u32, u32)>,
) -> io::Result<()> {
    let view = live_tail.load_view();
    let candidates: Vec<(u128, BlockIndexEntry)> = view
        .blocks
        .iter()
        .filter(|(min_key, _)| match shard_filter {
            None => true,
            Some((shard_id, shard_bits)) => hilbert_shard_id(**min_key, shard_bits) == shard_id,
        })
        .map(|(k, e)| (*k, *e))
        .collect();

    const TIER_THRESHOLD: usize = 8;
    if candidates.len() < TIER_THRESHOLD {
        return Ok(());
    }

    let mut input_blocks = Vec::new();
    for (_, entry) in &candidates {
        input_blocks.push(store.read_block(entry.block_id)?);
    }

    let snap_id = snapshots
        .get(space)
        .map(|s| s.id)
        .unwrap_or(SnapshotId(1));

    let result = compact(
        input_blocks,
        &CompactionConfig {
            max_records_per_block: 4096,
            retain_history: false,
        },
        snap_id,
        || {
            crate::infinitedb_core::block::BlockId(
                next_block_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            )
        },
    );

    if result.new_blocks.is_empty() {
        return Ok(());
    }

    let superseded: std::collections::HashSet<_> =
        result.superseded.iter().map(|b| b.0).collect();

    for mut block in result.new_blocks {
        block.checksum = compute_checksum(&block)?;
        store.write_block(&block)?;
        let min_key = block.records.first().map(|r| r.hilbert_key).unwrap_or(0);
        let max_key = block.records.last().map(|r| r.hilbert_key).unwrap_or(min_key);
        let entry = BlockIndexEntry {
            block_id: block.id,
            max_key,
        };
        snapshots.update(space, |snap| {
            snap.blocks
                .retain(|_, e| !superseded.contains(&e.block_id.0));
            snap.blocks.insert(min_key, entry);
        });
        let mut blocks: BTreeMap<u128, BlockIndexEntry> = live_tail
            .load_view()
            .blocks
            .as_ref()
            .clone();
        blocks.retain(|_, e| !superseded.contains(&e.block_id.0));
        blocks.insert(min_key, entry);
        live_tail.init_blocks(blocks);
    }

    let _ = spaces;
    Ok(())
}
