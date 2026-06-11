//! Post-compaction block-file garbage collection.

use std::io;

use crate::infinitedb_core::{block::BlockId, snapshot::Snapshot};
use crate::infinitedb_storage::{gc::safe_to_delete, nvme::BlockStore};

use super::branch_overlay::BranchOverlayStore;
use super::snapshot_store::SnapshotStore;

/// Collect snapshot heads that must pin block files during GC.
pub fn live_snapshots_for_gc(
    snapshots: &SnapshotStore,
    branch_overlays: Option<&BranchOverlayStore>,
) -> Vec<Snapshot> {
    let mut live: Vec<Snapshot> = snapshots
        .all()
        .into_values()
        .map(|snap| (*snap).clone())
        .collect();
    if let Some(overlays) = branch_overlays {
        live.extend(overlays.branch_base_snapshots());
    }
    live
}

/// Delete superseded block files that are unreferenced by any live snapshot head.
pub fn gc_superseded_blocks(
    store: &BlockStore,
    superseded: &[BlockId],
    live_snapshots: &[Snapshot],
) -> io::Result<usize> {
    let deletable = safe_to_delete(superseded, live_snapshots);
    for id in &deletable {
        store.delete_block(*id)?;
    }
    Ok(deletable.len())
}
