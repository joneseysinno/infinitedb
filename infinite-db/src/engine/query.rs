//! Concurrent query execution over sealed blocks + live tail.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU32, Ordering};

use crate::infinitedb_core::{
    address::{DimensionVector, RevisionId, SpaceId},
    block::Record,
    hilbert_key::{CachedHilbertKey, HilbertKey},
    record_identity::{AddressKey, RecordIdentityKey},
    snapshot::BlockIndexEntry,
    space::SpaceRegistry,
};
use crate::infinitedb_index::composite::KeyConfig;
use crate::infinitedb_index::key::{hilbert_key_for, hilbert_key_standard};
use crate::infinitedb_index::range_decompose::{
    block_overlaps_intervals, decompose_bbox, key_in_intervals, KeyInterval,
};

use super::hilbert_shard::{hilbert_shard_id, ShardRef};
use crate::infinitedb_storage::nvme::BlockStore;

use crate::infinitedb_core::branch::BranchId;

use super::branch_overlay::BranchOverlayStore;
use super::watermark::RevisionWatermark;
use super::hilbert_live_tails::HilbertLiveTails;
use super::live_tail::LiveTailView;
use super::snapshot_store::SnapshotStore;
use super::space_live_tails::SpaceLiveTails;

/// Return the cached Hilbert key on `record`, or compute it when unset (legacy blocks).
pub fn record_hilbert_key(spaces: &SpaceRegistry, record: &Record) -> HilbertKey {
    if let Some(k) = record.hilbert_key.get() {
        k
    } else {
        HilbertKey(space_key(spaces, record.address.space, &record.address.point))
    }
}

pub fn space_key(spaces: &SpaceRegistry, space: SpaceId, point: &DimensionVector) -> u128 {
    match spaces.get(space) {
        Some(config) => hilbert_key_for(point, KeyConfig {
            bits_per_dim: config.bits_per_dim,
        }),
        None => hilbert_key_standard(point),
    }
}

/// Address-only identity for latest-wins grouping.
pub fn address_key(spaces: &SpaceRegistry, record: &Record) -> AddressKey {
    AddressKey::from_hilbert(record, record_hilbert_key(spaces, record))
}

/// Full record identity for seal deduplication.
pub fn record_identity_key(spaces: &SpaceRegistry, record: &Record) -> RecordIdentityKey {
    RecordIdentityKey::from_hilbert(record, record_hilbert_key(spaces, record))
}

/// Ensure `record` carries a cached Hilbert key, computing from coordinates when unset.
pub fn ensure_record_hilbert_key(spaces: &SpaceRegistry, record: &mut Record) -> HilbertKey {
    if let Some(k) = record.hilbert_key.get() {
        return k;
    }
    let k = HilbertKey(space_key(spaces, record.address.space, &record.address.point));
    record.hilbert_key = CachedHilbertKey::set(k);
    k
}

/// Resolve Hilbert keys and sort records for block seal.
pub fn prepare_records_for_seal(spaces: &SpaceRegistry, records: &mut [Record]) {
    for record in records.iter_mut() {
        ensure_record_hilbert_key(spaces, record);
    }
    records.sort_by_key(|r| (r.hilbert_key.get().unwrap(), r.revision));
}

fn live_tail_for_space(
    spaces: &SpaceRegistry,
    space: SpaceId,
    live_tail: Option<&LiveTailView>,
    space_live_tails: Option<&SpaceLiveTails>,
    hilbert_live_tails: Option<&HilbertLiveTails>,
    shard_filter: Option<ShardRef>,
) -> Vec<Record> {
    if let Some(hilbert) = hilbert_live_tails {
        let views = hilbert.views_for_space(space);
        if !views.is_empty() {
            let mut records = Vec::new();
            for view in views {
                if let Some(shard) = shard_filter {
                    let has_records = view.tail_iter().any(|r| {
                        shard.contains_key(record_hilbert_key(spaces, r))
                    });
                    let has_blocks = view.blocks.iter().any(|(min_key, _)| {
                        shard.contains_key(*min_key)
                    });
                    if !has_records && !has_blocks {
                        continue;
                    }
                }
                records.extend(view.tail_iter().cloned());
            }
            return records
                .into_iter()
                .filter(|r| r.address.space == space)
                .collect();
        }
    }
    if let Some(tails) = space_live_tails {
        return tails
            .get(space)
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

#[derive(Clone, Copy)]
enum KeyFilter<'a> {
    All,
    Single(HilbertKey, HilbertKey),
    Intervals(&'a [KeyInterval]),
}

fn block_entries_from_snapshot(
    snapshot: &crate::infinitedb_core::snapshot::Snapshot,
    key_filter: KeyFilter<'_>,
) -> Vec<(HilbertKey, BlockIndexEntry)> {
    let overlaps = |min_key: HilbertKey, max_key: HilbertKey| match key_filter {
        KeyFilter::All => true,
        KeyFilter::Single(lo, hi) => min_key <= hi && max_key >= lo,
        KeyFilter::Intervals(intervals) => block_overlaps_intervals(min_key, max_key, intervals),
    };
    snapshot
        .blocks
        .iter()
        .filter(|(min_key, entry)| overlaps(**min_key, entry.max_key))
        .map(|(k, e)| (*k, e.clone()))
        .collect()
}

fn block_entries_for_space(
    spaces: &SpaceRegistry,
    space: SpaceId,
    snapshots: &SnapshotStore,
    key_filter: KeyFilter<'_>,
    live_tail: Option<&LiveTailView>,
    space_live_tails: Option<&SpaceLiveTails>,
    hilbert_live_tails: Option<&HilbertLiveTails>,
    shard_filter: Option<ShardRef>,
) -> Vec<(HilbertKey, BlockIndexEntry)> {
    let overlaps = |min_key: HilbertKey, max_key: HilbertKey| match key_filter {
        KeyFilter::All => true,
        KeyFilter::Single(lo, hi) => min_key <= hi && max_key >= lo,
        KeyFilter::Intervals(intervals) => block_overlaps_intervals(min_key, max_key, intervals),
    };

    if let Some(hilbert) = hilbert_live_tails {
        let views = hilbert.views_for_space(space);
        if !views.is_empty() {
            let mut entries = Vec::new();
            for view in views {
                if let Some(shard) = shard_filter {
                    let shard_match = view.blocks.iter().any(|(min_key, _)| {
                        shard.contains_key(*min_key)
                    }) || view.tail_iter().any(|r| {
                        shard.contains_key(record_hilbert_key(spaces, r))
                    });
                    if !shard_match {
                        continue;
                    }
                }
                for (min_key, entry) in view.blocks.iter() {
                    if overlaps(*min_key, entry.max_key) {
                        entries.push((*min_key, entry.clone()));
                    }
                }
            }
            return entries;
        }
    }
    if let Some(tails) = space_live_tails {
        if let Some(tail) = tails.get(space) {
            let view = tail.load_view();
            return view
                .blocks
                .iter()
                .filter(|(min_key, entry)| overlaps(**min_key, entry.max_key))
                .map(|(k, e)| (*k, e.clone()))
                .collect();
        }
    }
    let _ = live_tail;
    snapshots
        .get(space)
        .map(|snapshot| {
            snapshot
                .blocks
                .iter()
                .filter(|(min_key, entry)| overlaps(**min_key, entry.max_key))
                .map(|(k, e)| (*k, e.clone()))
                .collect()
        })
        .unwrap_or_default()
}

fn record_matches_filter(
    spaces: &SpaceRegistry,
    record: &Record,
    key_filter: KeyFilter<'_>,
) -> bool {
    match key_filter {
        KeyFilter::All => true,
        KeyFilter::Single(lo, hi) => {
            let k = record_hilbert_key(spaces, record);
            if lo == hi {
                k == lo
            } else {
                k >= lo && k <= hi
            }
        }
        KeyFilter::Intervals(intervals) => {
            key_in_intervals(record_hilbert_key(spaces, record), intervals)
        }
    }
}

/// Query plan instrumentation for frame performance-contract tests (M6).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryPlanStats {
    pub interval_scans: u32,
}

static QUERY_INTERVAL_SCANS: AtomicU32 = AtomicU32::new(0);

/// Reset the global interval-scan counter (tests).
pub fn reset_query_plan_stats() {
    QUERY_INTERVAL_SCANS.store(0, Ordering::Relaxed);
}

/// Read accumulated interval-scan count since last reset.
pub fn query_plan_stats() -> QueryPlanStats {
    QueryPlanStats {
        interval_scans: QUERY_INTERVAL_SCANS.load(Ordering::Relaxed),
    }
}

fn record_interval_scan(count: u32) {
    QUERY_INTERVAL_SCANS.fetch_add(count, Ordering::Relaxed);
}

/// Apply latest-wins visibility per address identity.
///
/// An address is visible iff its highest revision at or below `rev_ceiling` is a
/// live record; that record is the one returned. When `include_tombstones` is true,
/// all candidate records are returned unchanged (full revision history).
pub(crate) fn resolve_visibility(
    spaces: &SpaceRegistry,
    candidates: Vec<Record>,
    rev_ceiling: RevisionId,
    include_tombstones: bool,
) -> Vec<Record> {
    if include_tombstones {
        return candidates;
    }

    let mut latest: HashMap<AddressKey, Record> = HashMap::new();
    for record in candidates {
        if record.revision > rev_ceiling {
            continue;
        }
        let key = address_key(spaces, &record);
        let replace = match latest.get(&key) {
            None => true,
            Some(existing) => record.revision > existing.revision,
        };
        if replace {
            latest.insert(key, record);
        }
    }

    latest
        .into_values()
        .filter(|r| !r.tombstone)
        .collect()
}

/// Query sealed blocks and live tail(s) for `space`.
///
/// Visibility rule: an address is visible iff its highest revision ≤ the ceiling
/// is a live record; exactly one record per address is returned unless
/// `include_tombstones` is set (which preserves full revision history).
pub fn query_inner(
    store: &BlockStore,
    snapshots: &SnapshotStore,
    live_tail: Option<&LiveTailView>,
    space_live_tails: Option<&SpaceLiveTails>,
    spaces: &SpaceRegistry,
    watermark: &RevisionWatermark,
    space: SpaceId,
    key_range: Option<(u128, u128)>,
    as_of: Option<RevisionId>,
    include_tombstones: bool,
    hilbert_live_tails: Option<&HilbertLiveTails>,
    branch_overlays: Option<&BranchOverlayStore>,
    branch_id: Option<BranchId>,
) -> std::io::Result<Vec<Record>> {
    let rev_ceiling = as_of.unwrap_or_else(|| watermark.allocated());

    let key_filter = match key_range {
        None => KeyFilter::All,
        Some((lo, hi)) => KeyFilter::Single(HilbertKey(lo), HilbertKey(hi)),
    };

    let on_branch = branch_id.is_some_and(|b| b != BranchId::MAIN);

    let tail = if on_branch {
        match (branch_overlays, branch_id) {
            (Some(overlays), Some(branch)) => overlays.live_records(branch, space),
            _ => Vec::new(),
        }
    } else {
        live_tail_for_space(
            spaces,
            space,
            live_tail,
            space_live_tails,
            hilbert_live_tails,
            None,
        )
    };

    let mut candidates: Vec<Record> = Vec::new();

    let block_entries = if let (Some(overlays), Some(branch)) = (branch_overlays, branch_id) {
        if branch != BranchId::MAIN {
            let mut entries = overlays
                .base_snapshot(branch, space)
                .map(|base| block_entries_from_snapshot(base.as_ref(), key_filter))
                .unwrap_or_default();
            for (min_key, entry) in overlays.sealed_blocks(branch, space) {
                let overlaps = match key_filter {
                    KeyFilter::All => true,
                    KeyFilter::Single(lo, hi) => min_key <= hi && entry.max_key >= lo,
                    KeyFilter::Intervals(intervals) => {
                        block_overlaps_intervals(min_key, entry.max_key, intervals)
                    }
                };
                if overlaps {
                    entries.push((min_key, entry));
                }
            }
            entries
        } else {
            block_entries_for_space(
                spaces,
                space,
                snapshots,
                key_filter,
                live_tail,
                space_live_tails,
                hilbert_live_tails,
                None,
            )
        }
    } else {
        block_entries_for_space(
            spaces,
            space,
            snapshots,
            key_filter,
            live_tail,
            space_live_tails,
            hilbert_live_tails,
            None,
        )
    };
    for (_, entry) in block_entries {
        let block = store.read_block_shared(entry.block_id)?;
        for record in block.records.iter() {
            if record.address.space != space || record.revision > rev_ceiling {
                continue;
            }
            if !record_matches_filter(spaces, record, key_filter) {
                continue;
            }
            candidates.push(record.clone());
        }
    }

    for record in tail {
        if record.address.space != space || record.revision > rev_ceiling {
            continue;
        }
        if !record_matches_filter(spaces, &record, key_filter) {
            continue;
        }
        candidates.push(record);
    }

    Ok(resolve_visibility(
        spaces,
        candidates,
        rev_ceiling,
        include_tombstones,
    ))
}

pub fn query_bbox(
    store: &BlockStore,
    snapshots: &SnapshotStore,
    live_tail: Option<&LiveTailView>,
    space_live_tails: Option<&SpaceLiveTails>,
    spaces: &SpaceRegistry,
    watermark: &RevisionWatermark,
    space: SpaceId,
    min: DimensionVector,
    max: DimensionVector,
    as_of: Option<RevisionId>,
    hilbert_live_tails: Option<&HilbertLiveTails>,
    branch_overlays: Option<&BranchOverlayStore>,
    branch_id: Option<BranchId>,
) -> std::io::Result<Vec<Record>> {
    assert_eq!(min.dims(), max.dims(), "min and max must have equal dimensions");
    let bits = spaces
        .get(space)
        .map(|c| c.bits_per_dim)
        .unwrap_or(8);
    let shard_bits = Some(ShardRef::shard_bits_for_space(spaces, space));
    let intervals = decompose_bbox(&min, &max, bits);
    let _ = intervals;
    record_interval_scan(1);
    let rev_ceiling = as_of.unwrap_or_else(|| watermark.allocated());

    let shard_filter = shard_bits.map(|sb| {
        let mut shard_ids = std::collections::BTreeSet::new();
        for interval in &intervals {
            shard_ids.insert(hilbert_shard_id(interval.lo.raw(), sb));
            shard_ids.insert(hilbert_shard_id(interval.hi.raw(), sb));
        }
        shard_ids
    });

    let on_branch = branch_id.is_some_and(|b| b != BranchId::MAIN);
    let key_filter = KeyFilter::Intervals(&intervals);

    let tail = if on_branch {
        match (branch_overlays, branch_id) {
            (Some(overlays), Some(branch)) => overlays.live_records(branch, space),
            _ => Vec::new(),
        }
    } else {
        live_tail_for_space(
            spaces,
            space,
            live_tail,
            space_live_tails,
            hilbert_live_tails,
            None,
        )
    };

    let mut candidates = Vec::new();
    let block_entries = if let (Some(overlays), Some(branch)) = (branch_overlays, branch_id) {
        if branch != BranchId::MAIN {
            let mut entries = overlays
                .base_snapshot(branch, space)
                .map(|base| block_entries_from_snapshot(base.as_ref(), key_filter))
                .unwrap_or_default();
            for (min_key, entry) in overlays.sealed_blocks(branch, space) {
                if block_overlaps_intervals(min_key, entry.max_key, &intervals) {
                    entries.push((min_key, entry));
                }
            }
            entries
        } else {
            block_entries_for_space(
                spaces,
                space,
                snapshots,
                key_filter,
                live_tail,
                space_live_tails,
                hilbert_live_tails,
                shard_bits.map(|sb| {
                    let first = intervals
                        .first()
                        .map(|i| hilbert_shard_id(i.lo.raw(), sb))
                        .unwrap_or(0);
                    ShardRef::new(first, sb)
                }),
            )
        }
    } else {
        block_entries_for_space(
            spaces,
            space,
            snapshots,
            key_filter,
            live_tail,
            space_live_tails,
            hilbert_live_tails,
            shard_bits.map(|sb| {
                let first = intervals
                    .first()
                    .map(|i| hilbert_shard_id(i.lo.raw(), sb))
                    .unwrap_or(0);
                ShardRef::new(first, sb)
            }),
        )
    };
    for (_, entry) in block_entries {
        let block = store.read_block_shared(entry.block_id)?;
        for record in block.records.iter() {
            if record.address.space != space || record.revision > rev_ceiling {
                continue;
            }
            if !record_matches_filter(spaces, record, KeyFilter::Intervals(&intervals)) {
                continue;
            }
            candidates.push(record.clone());
        }
    }
    for record in tail {
        if record.address.space != space || record.revision > rev_ceiling {
            continue;
        }
        if let Some(ref shards) = shard_filter {
            let sb = ShardRef::shard_bits_for_space(spaces, space);
            let sid = hilbert_shard_id(record_hilbert_key(spaces, &record).raw(), sb);
            if !shards.contains(&sid) {
                continue;
            }
        }
        if !record_matches_filter(spaces, &record, KeyFilter::Intervals(&intervals)) {
            continue;
        }
        candidates.push(record);
    }

    let mut results = resolve_visibility(spaces, candidates, rev_ceiling, false);
    results.retain(|r| r.address.point.within(&min, &max));
    Ok(results)
}

pub fn snapshots_map_for_persist(snapshots: &SnapshotStore) -> BTreeMap<u64, crate::infinitedb_core::snapshot::Snapshot> {
    snapshots
        .all()
        .into_iter()
        .map(|(k, v)| (k.0, (*v).clone()))
        .collect()
}
