//! Shard-local compaction triggered after seal.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::infinitedb_core::{
    address::SpaceId,
    block::BlockId,
    endpoint_index::ENDPOINT_INDEX_SPACE,
    hilbert_key::HilbertKey,
    snapshot::{BlockIndexEntry, SnapshotId},
    space::{CompactionPolicy, SpaceRegistry},
};

use super::endpoint_index_migrate::expand_endpoint_index_records_for_compaction;
use crate::infinitedb_storage::{
    compaction::{compact, CompactionConfig},
    gc::RetentionPolicy,
    nvme::{compute_checksum, BlockStore},
};

use super::block_gc::{gc_superseded_blocks, live_snapshots_for_gc};
use super::branch_overlay::BranchOverlayStore;
use super::hilbert_shard::ShardRef;
use super::live_tail::LiveTailView;
use super::query::prepare_records_for_seal;
use super::snapshot_store::SnapshotStore;

/// Sentinel id for folding live-tail records into compaction input (never a real block file).
const EPHEMERAL_TAIL_BLOCK: BlockId = BlockId(u64::MAX);

/// Per-space compaction policy overrides for manual `compact_with` invocations.
pub type CompactionPolicyOverrides = Arc<Mutex<std::collections::HashMap<SpaceId, CompactionPolicy>>>;

/// Resolve compaction settings from space config and optional override.
pub fn resolve_compaction_settings(
    spaces: &RwLock<SpaceRegistry>,
    overrides: Option<&CompactionPolicyOverrides>,
    space: SpaceId,
) -> (CompactionConfig, Option<RetentionPolicy>) {
    let policy = overrides
        .and_then(|o| o.lock().get(&space).cloned())
        .or_else(|| {
            spaces
                .read()
                .get(space)
                .map(|c| c.compaction_policy.clone())
        })
        .unwrap_or_default();
    policy_to_settings(&policy)
}

/// Map a [`CompactionPolicy`] to compaction config and optional retention filter.
pub fn policy_to_settings(policy: &CompactionPolicy) -> (CompactionConfig, Option<RetentionPolicy>) {
    match policy {
        CompactionPolicy::KeepAll => (
            CompactionConfig {
                retain_history: true,
                ..CompactionConfig::default()
            },
            None,
        ),
        CompactionPolicy::LatestOnly => (
            CompactionConfig {
                retain_history: false,
                ..CompactionConfig::default()
            },
            None,
        ),
        CompactionPolicy::RetentionWindow {
            version_horizon,
            tombstone_horizon,
        } => (
            CompactionConfig {
                retain_history: true,
                ..CompactionConfig::default()
            },
            Some(RetentionPolicy {
                version_horizon: *version_horizon,
                tombstone_horizon: *tombstone_horizon,
            }),
        ),
    }
}

/// Compact blocks in a shard when at least `min_blocks` candidates are present.
pub fn compact_space_now(
    store: &BlockStore,
    snapshots: &SnapshotStore,
    live_tail: &LiveTailView,
    spaces: &RwLock<SpaceRegistry>,
    next_block_id: &std::sync::atomic::AtomicU64,
    space: SpaceId,
    shard_filter: Option<ShardRef>,
    policy_overrides: Option<&CompactionPolicyOverrides>,
    branch_overlays: Option<&BranchOverlayStore>,
    min_blocks: usize,
) -> io::Result<()> {
    let view = live_tail.load_view();
    let tail_records: Vec<_> = view.tail_vec();
    let candidates: Vec<(HilbertKey, BlockIndexEntry)> = view
        .blocks
        .iter()
        .filter(|(min_key, _)| match shard_filter {
            None => true,
            Some(shard) => shard.contains_key(**min_key),
        })
        .map(|(k, e)| (*k, *e))
        .collect();

    let source_count = candidates.len() + usize::from(!tail_records.is_empty());
    if source_count < min_blocks {
        return Ok(());
    }

    let mut input_blocks = Vec::new();
    for (_, entry) in &candidates {
        input_blocks.push(store.read_block(entry.block_id)?);
    }

    if !tail_records.is_empty() {
        let min_rev = tail_records
            .iter()
            .map(|r| r.revision)
            .min()
            .unwrap_or(crate::infinitedb_core::address::RevisionId::ZERO);
        let max_rev = tail_records
            .iter()
            .map(|r| r.revision)
            .max()
            .unwrap_or(min_rev);
        input_blocks.push(crate::infinitedb_core::block::Block {
            id: EPHEMERAL_TAIL_BLOCK,
            space,
            records: tail_records.clone(),
            min_revision: min_rev,
            max_revision: max_rev,
            checksum: crate::infinitedb_core::checksum::Checksum::ZERO,
        });
    }

    if space == ENDPOINT_INDEX_SPACE {
        let registry = spaces.read();
        for block in &mut input_blocks {
            let records = std::mem::take(&mut block.records);
            block.records = expand_endpoint_index_records_for_compaction(
                records,
                &registry,
                &|_| None,
            );
        }
    }

    let snap_id = snapshots
        .get(space)
        .map(|s| s.id)
        .unwrap_or(SnapshotId(1));

    let (config, retention) =
        resolve_compaction_settings(spaces, policy_overrides, space);

    let result = compact(
        input_blocks,
        &config,
        retention.as_ref(),
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

    let superseded: std::collections::HashSet<_> = result
        .superseded
        .iter()
        .filter(|b| b.0 != EPHEMERAL_TAIL_BLOCK.0)
        .map(|b| b.0)
        .collect();

    let space_registry = spaces.read();
    for mut block in result.new_blocks {
        prepare_records_for_seal(&space_registry, &mut block.records);
        block.checksum = compute_checksum(&block)?;
        store.write_block(&block)?;
        let min_key = block
            .records
            .first()
            .and_then(|r| r.hilbert_key.get())
            .unwrap_or(HilbertKey::ZERO);
        let max_key = block
            .records
            .last()
            .and_then(|r| r.hilbert_key.get())
            .unwrap_or(min_key);
        let entry = BlockIndexEntry {
            block_id: block.id,
            max_key,
        };
        snapshots.update(space, |snap| {
            snap.blocks
                .retain(|_, e| !superseded.contains(&e.block_id.0));
            snap.blocks.insert(min_key, entry);
        });
        let mut blocks: BTreeMap<HilbertKey, BlockIndexEntry> = live_tail
            .load_view()
            .blocks
            .as_ref()
            .clone();
        blocks.retain(|_, e| !superseded.contains(&e.block_id.0));
        blocks.insert(min_key, entry);
        live_tail.init_blocks(blocks);
    }

    let deletable: Vec<BlockId> = result
        .superseded
        .iter()
        .filter(|b| b.0 != EPHEMERAL_TAIL_BLOCK.0)
        .copied()
        .collect();
    let live = live_snapshots_for_gc(snapshots, branch_overlays);
    let _ = gc_superseded_blocks(store, &deletable, &live)?;

    if !tail_records.is_empty() {
        live_tail.publish(Vec::new());
    }

    Ok(())
}

/// Compact small blocks in a shard after seal when this shard exceeds the threshold.
///
/// Space-wide compaction (so LatestOnly can rewrite a 1-block shard that still
/// holds superseded revisions) is driven from [`HilbertCoordinator::flush_space`].
pub fn maybe_compact_after_seal(
    store: &BlockStore,
    snapshots: &SnapshotStore,
    live_tail: &LiveTailView,
    spaces: &RwLock<SpaceRegistry>,
    next_block_id: &std::sync::atomic::AtomicU64,
    space: SpaceId,
    shard_filter: Option<ShardRef>,
    policy_overrides: Option<&CompactionPolicyOverrides>,
    branch_overlays: Option<&BranchOverlayStore>,
) -> io::Result<()> {
    const TIER_THRESHOLD: usize = 8;
    compact_space_now(
        store,
        snapshots,
        live_tail,
        spaces,
        next_block_id,
        space,
        shard_filter,
        policy_overrides,
        branch_overlays,
        TIER_THRESHOLD,
    )
}
