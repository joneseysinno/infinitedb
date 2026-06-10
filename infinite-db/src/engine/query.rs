//! Concurrent query execution over sealed blocks + live tail.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::infinitedb_core::{
    address::{DimensionVector, RevisionId, SpaceId},
    block::{BlockId, Record},
    space::SpaceRegistry,
};
use crate::infinitedb_index::composite::KeyConfig;
use crate::infinitedb_index::key::{hilbert_key_for, hilbert_key_standard};
use crate::infinitedb_storage::nvme::BlockStore;

use crate::infinitedb_core::branch::BranchId;

use super::branch_overlay::BranchOverlayStore;
use super::hilbert_live_tails::HilbertLiveTails;
use super::live_tail::LiveTailView;
use super::snapshot_store::SnapshotStore;
use super::space_live_tails::SpaceLiveTails;

pub fn space_key(spaces: &SpaceRegistry, space: SpaceId, point: &DimensionVector) -> u128 {
    match spaces.get(space) {
        Some(config) => hilbert_key_for(point, KeyConfig {
            bits_per_dim: config.bits_per_dim,
        }),
        None => hilbert_key_standard(point),
    }
}

fn live_tail_for_space(
    space: SpaceId,
    live_tail: Option<&LiveTailView>,
    space_live_tails: Option<&SpaceLiveTails>,
    hilbert_live_tails: Option<&HilbertLiveTails>,
) -> Vec<Record> {
    if let Some(hilbert) = hilbert_live_tails {
        let mut records = Vec::new();
        for tail in hilbert.tails_for_space(space.0) {
            records.extend(tail.snapshot());
        }
        return records
            .into_iter()
            .filter(|r| r.address.space == space)
            .collect();
    }
    if let Some(tails) = space_live_tails {
        return tails
            .get(space.0)
            .map(|t| t.snapshot())
            .unwrap_or_default();
    }
    live_tail
        .map(|t| t.snapshot())
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.address.space == space)
        .collect()
}

pub fn query_inner(
    store: &BlockStore,
    snapshots: &SnapshotStore,
    live_tail: Option<&LiveTailView>,
    space_live_tails: Option<&SpaceLiveTails>,
    spaces: &SpaceRegistry,
    revision: &AtomicU64,
    space: SpaceId,
    key_range: Option<(u128, u128)>,
    as_of: Option<RevisionId>,
    include_tombstones: bool,
    hilbert_live_tails: Option<&HilbertLiveTails>,
    branch_overlays: Option<&BranchOverlayStore>,
    branch_id: Option<BranchId>,
) -> std::io::Result<Vec<Record>> {
    let rev_ceiling = as_of.unwrap_or_else(|| RevisionId(revision.load(Ordering::Acquire)));

    let mut tail = live_tail_for_space(space, live_tail, space_live_tails, hilbert_live_tails);
    if let (Some(overlays), Some(branch)) = (branch_overlays, branch_id) {
        if branch != BranchId::MAIN {
            tail.extend(overlays.live_records(branch, space));
        }
    }
    let mut tombstoned: HashSet<Vec<u32>> = tail
        .iter()
        .filter(|r| r.address.space == space && r.tombstone && r.revision <= rev_ceiling)
        .map(|r| r.address.point.coords.clone())
        .collect();

    let mut results: Vec<Record> = Vec::new();

    if let Some(snapshot) = snapshots.get(space) {
        let block_ids: Vec<BlockId> = match key_range {
            None => snapshot.blocks.values().map(|e| e.block_id).collect(),
            Some((lo, hi)) => snapshot
                .blocks
                .iter()
                .filter(|(min_key, entry)| **min_key <= hi && entry.max_key >= lo)
                .map(|(_, entry)| entry.block_id)
                .collect(),
        };

        for block_id in &block_ids {
            let block = store.read_block_shared(*block_id)?;
            for record in &block.records {
                if record.address.space == space
                    && record.tombstone
                    && record.revision <= rev_ceiling
                {
                    tombstoned.insert(record.address.point.coords.clone());
                }
            }
        }

        for block_id in block_ids {
            let block = store.read_block_shared(block_id)?;
            for record in block.records.iter() {
                if record.revision > rev_ceiling {
                    continue;
                }
                if !include_tombstones && record.tombstone {
                    continue;
                }
                if let Some((lo, hi)) = key_range {
                    if lo == hi {
                        let k = space_key(spaces, space, &record.address.point);
                        if k != lo {
                            continue;
                        }
                    }
                }
                results.push(record.clone());
            }
        }

        if !include_tombstones {
            results.retain(|r| !tombstoned.contains(&r.address.point.coords));
        }
    }

    for record in tail {
        let visible = record.address.space == space
            && record.revision <= rev_ceiling
            && (include_tombstones || !record.tombstone)
            && (include_tombstones || !tombstoned.contains(&record.address.point.coords));
        if visible {
            if let Some((lo, hi)) = key_range {
                let k = space_key(spaces, space, &record.address.point);
                if k < lo || k > hi {
                    continue;
                }
            }
            results.push(record);
        }
    }

    Ok(results)
}

pub fn query_bbox(
    store: &BlockStore,
    snapshots: &SnapshotStore,
    live_tail: Option<&LiveTailView>,
    space_live_tails: Option<&SpaceLiveTails>,
    spaces: &SpaceRegistry,
    revision: &AtomicU64,
    space: SpaceId,
    min: DimensionVector,
    max: DimensionVector,
    as_of: Option<RevisionId>,
    hilbert_live_tails: Option<&HilbertLiveTails>,
    branch_overlays: Option<&BranchOverlayStore>,
    branch_id: Option<BranchId>,
) -> std::io::Result<Vec<Record>> {
    assert_eq!(min.dims(), max.dims(), "min and max must have equal dimensions");
    let k_min = space_key(spaces, space, &min);
    let k_max = space_key(spaces, space, &max);
    let (lo, hi) = if k_min <= k_max {
        (k_min, k_max)
    } else {
        (k_max, k_min)
    };
    let mut results = query_inner(
        store,
        snapshots,
        live_tail,
        space_live_tails,
        spaces,
        revision,
        space,
        Some((lo, hi)),
        as_of,
        false,
        hilbert_live_tails,
        branch_overlays,
        branch_id,
    )?;
    results.retain(|r| r.address.point.within(&min, &max));
    Ok(results)
}

pub fn snapshots_map_for_persist(snapshots: &SnapshotStore) -> BTreeMap<u64, crate::infinitedb_core::snapshot::Snapshot> {
    snapshots
        .all()
        .into_iter()
        .map(|(k, v)| (k, (*v).clone()))
        .collect()
}
