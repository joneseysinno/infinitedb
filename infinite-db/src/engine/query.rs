//! Concurrent query execution over sealed blocks + live tail.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::infinitedb_core::{
    address::{DimensionVector, RevisionId, SpaceId},
    block::Record,
    snapshot::BlockIndexEntry,
    space::SpaceRegistry,
};
use crate::infinitedb_index::composite::KeyConfig;
use crate::infinitedb_index::key::{hilbert_key_for, hilbert_key_standard};
use crate::infinitedb_index::range_decompose::{
    block_overlaps_intervals, decompose_bbox, key_in_intervals, KeyInterval,
};

use super::hilbert_shard::hilbert_shard_id;
use crate::infinitedb_storage::nvme::BlockStore;

use crate::infinitedb_core::branch::BranchId;

use super::branch_overlay::BranchOverlayStore;
use super::hilbert_live_tails::HilbertLiveTails;
use super::live_tail::LiveTailView;
use super::snapshot_store::SnapshotStore;
use super::space_live_tails::SpaceLiveTails;

/// Return the cached Hilbert key on `record`, or compute it when unset (legacy blocks).
pub fn record_hilbert_key(spaces: &SpaceRegistry, record: &Record) -> u128 {
    if record.hilbert_key != 0 {
        record.hilbert_key
    } else {
        space_key(spaces, record.address.space, &record.address.point)
    }
}

fn record_hilbert_key_uncached(record: &Record) -> u128 {
    if record.hilbert_key != 0 {
        record.hilbert_key
    } else {
        hilbert_key_standard(&record.address.point)
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

fn live_tail_for_space(
    space: SpaceId,
    live_tail: Option<&LiveTailView>,
    space_live_tails: Option<&SpaceLiveTails>,
    hilbert_live_tails: Option<&HilbertLiveTails>,
    shard_filter: Option<(u32, u32)>,
) -> Vec<Record> {
    if let Some(hilbert) = hilbert_live_tails {
        let views = hilbert.views_for_space(space.0);
        if !views.is_empty() {
            let mut records = Vec::new();
            for view in views {
                if let Some((shard_id, shard_bits)) = shard_filter {
                    let has_records = view.tail_iter().any(|r| {
                        hilbert_shard_id(record_hilbert_key_uncached(r), shard_bits) == shard_id
                    });
                    let has_blocks = view.blocks.iter().any(|(min_key, _)| {
                        hilbert_shard_id(*min_key, shard_bits) == shard_id
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

#[derive(Clone, Copy)]
enum KeyFilter<'a> {
    All,
    Single(u128, u128),
    Intervals(&'a [KeyInterval]),
}

fn block_entries_for_space(
    space: SpaceId,
    snapshots: &SnapshotStore,
    key_filter: KeyFilter<'_>,
    live_tail: Option<&LiveTailView>,
    space_live_tails: Option<&SpaceLiveTails>,
    hilbert_live_tails: Option<&HilbertLiveTails>,
    shard_filter: Option<(u32, u32)>,
) -> Vec<(u128, BlockIndexEntry)> {
    let overlaps = |min_key: u128, max_key: u128| match key_filter {
        KeyFilter::All => true,
        KeyFilter::Single(lo, hi) => min_key <= hi && max_key >= lo,
        KeyFilter::Intervals(intervals) => block_overlaps_intervals(min_key, max_key, intervals),
    };

    if let Some(hilbert) = hilbert_live_tails {
        let views = hilbert.views_for_space(space.0);
        if !views.is_empty() {
            let mut entries = Vec::new();
            for view in views {
                if let Some((shard_id, shard_bits)) = shard_filter {
                    let shard_match = view.blocks.iter().any(|(min_key, _)| {
                        hilbert_shard_id(*min_key, shard_bits) == shard_id
                    }) || view.tail_iter().any(|r| {
                        hilbert_shard_id(record_hilbert_key_uncached(r), shard_bits) == shard_id
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
        if let Some(tail) = tails.get(space.0) {
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

/// Apply latest-wins visibility per address coordinate.
///
/// An address is visible iff its highest revision at or below `rev_ceiling` is a
/// live record; that record is the one returned. When `include_tombstones` is true,
/// all candidate records are returned unchanged (full revision history).
fn resolve_visibility(
    candidates: Vec<Record>,
    rev_ceiling: RevisionId,
    include_tombstones: bool,
) -> Vec<Record> {
    if include_tombstones {
        return candidates;
    }

    let mut latest: HashMap<Vec<u32>, Record> = HashMap::new();
    for record in candidates {
        if record.revision > rev_ceiling {
            continue;
        }
        let coords = record.address.point.coords.clone();
        let replace = match latest.get(&coords) {
            None => true,
            Some(existing) => record.revision > existing.revision,
        };
        if replace {
            latest.insert(coords, record);
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

    let key_filter = match key_range {
        None => KeyFilter::All,
        Some((lo, hi)) => KeyFilter::Single(lo, hi),
    };

    let mut tail = live_tail_for_space(
        space,
        live_tail,
        space_live_tails,
        hilbert_live_tails,
        None,
    );
    if let (Some(overlays), Some(branch)) = (branch_overlays, branch_id) {
        if branch != BranchId::MAIN {
            tail.extend(overlays.live_records(branch, space));
        }
    }

    let mut candidates: Vec<Record> = Vec::new();

    let block_entries = block_entries_for_space(
        space,
        snapshots,
        key_filter,
        live_tail,
        space_live_tails,
        hilbert_live_tails,
        None,
    );
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

    Ok(resolve_visibility(candidates, rev_ceiling, include_tombstones))
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
    let bits = spaces
        .get(space)
        .map(|c| c.bits_per_dim)
        .unwrap_or(8);
    let shard_bits = spaces.get(space).map(|c| c.shard_bits);
    let intervals = decompose_bbox(&min, &max, bits);
    let rev_ceiling = as_of.unwrap_or_else(|| RevisionId(revision.load(Ordering::Acquire)));

    let shard_filter = shard_bits.map(|sb| {
        let mut shard_ids = std::collections::BTreeSet::new();
        for interval in &intervals {
            shard_ids.insert(hilbert_shard_id(interval.lo, sb));
            shard_ids.insert(hilbert_shard_id(interval.hi, sb));
        }
        shard_ids
    });

    let mut tail = live_tail_for_space(
        space,
        live_tail,
        space_live_tails,
        hilbert_live_tails,
        None,
    );
    if let (Some(overlays), Some(branch)) = (branch_overlays, branch_id) {
        if branch != BranchId::MAIN {
            tail.extend(overlays.live_records(branch, space));
        }
    }

    let mut candidates = Vec::new();
    let block_entries = block_entries_for_space(
        space,
        snapshots,
        KeyFilter::Intervals(&intervals),
        live_tail,
        space_live_tails,
        hilbert_live_tails,
        shard_bits.map(|sb| {
            let first = intervals
                .first()
                .map(|i| hilbert_shard_id(i.lo, sb))
                .unwrap_or(0);
            (first, sb)
        }),
    );
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
            let sb = spaces.get(space).map(|c| c.shard_bits).unwrap_or(4);
            let sid = hilbert_shard_id(record_hilbert_key(spaces, &record), sb);
            if !shards.contains(&sid) {
                continue;
            }
        }
        if !record_matches_filter(spaces, &record, KeyFilter::Intervals(&intervals)) {
            continue;
        }
        candidates.push(record);
    }

    let mut results = resolve_visibility(candidates, rev_ceiling, false);
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
