//! Branch merge execution.

use std::collections::{HashMap, HashSet};
use std::io;
use crate::infinitedb_core::{
    address::{Address, SpaceId},
    block::Record,
    branch::{BranchId, BranchRegistry},
    merge::{MergeConflict, MergeResult, MergeStrategy},
};

use super::branch_overlay::BranchOverlayStore;
use super::query::query_inner;
use super::hilbert_live_tails::HilbertLiveTails;
use super::live_tail::LiveTailView;
use super::snapshot_store::SnapshotStore;
use crate::infinitedb_core::space::SpaceRegistry;
use crate::infinitedb_storage::nvme::BlockStore;
use super::session::SessionWatermarks;
type Resolver<'a> = Option<&'a (dyn Fn(MergeConflict) -> Record + Send + Sync)>;

fn latest_per_address(records: Vec<Record>) -> HashMap<Address, Record> {
    let mut map: HashMap<Address, Record> = HashMap::new();
    for record in records {
        map.entry(record.address.clone())
            .and_modify(|existing| {
                if record.revision > existing.revision {
                    *existing = record.clone();
                }
            })
            .or_insert(record);
    }
    map
}

fn records_equivalent(a: &Record, b: &Record) -> bool {
    a.tombstone == b.tombstone && a.data == b.data
}

fn pick_winner(
    conflict: &MergeConflict,
    strategy: MergeStrategy,
    resolver: Resolver<'_>,
) -> Result<Option<Record>, MergeConflict> {
    match strategy {
        MergeStrategy::PreferTarget => Ok(conflict.target.clone()),
        MergeStrategy::PreferSource => Ok(conflict.source.clone()),
        MergeStrategy::PreferHigherRevision => {
            let target_rev = conflict
                .target
                .as_ref()
                .map(|r| r.revision.legacy_sequence())
                .unwrap_or(0);
            let source_rev = conflict
                .source
                .as_ref()
                .map(|r| r.revision.legacy_sequence())
                .unwrap_or(0);
            if source_rev >= target_rev {
                Ok(conflict.source.clone())
            } else {
                Ok(conflict.target.clone())
            }
        }
        MergeStrategy::Interactive => {
            if let Some(resolve) = resolver {
                Ok(Some(resolve(conflict.clone())))
            } else {
                Err(conflict.clone())
            }
        }
    }
}

/// Three-way merge of `source` into `target`, reading overlay state from `branch_overlays`.
pub fn merge_branches(
    store: &BlockStore,
    snapshots: &SnapshotStore,
    live_tail: Option<&LiveTailView>,
    hilbert_tails: Option<&HilbertLiveTails>,
    branch_overlays: &BranchOverlayStore,
    spaces: &SpaceRegistry,
    watermark: &SessionWatermarks,
    branches: &BranchRegistry,
    target: BranchId,
    source: BranchId,
    strategy: MergeStrategy,
    resolver: Resolver<'_>,
) -> io::Result<MergeResult> {
    let source_branch = branches
        .get(source)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "source branch not found"))?;
    let forked_at = source_branch.forked_at;

    let source_live = branch_overlays.all_live_records(source);
    let source_by_addr = latest_per_address(source_live);

    let touched_spaces: HashSet<SpaceId> =
        source_by_addr.keys().map(|a| a.space).collect();

    let mut base_by_addr: HashMap<Address, Record> = HashMap::new();
    let mut target_by_addr: HashMap<Address, Record> = HashMap::new();

    for space in &touched_spaces.clone() {
        let base_records = query_inner(
            store,
            snapshots,
            live_tail,
            spaces,
            watermark,
            *space,
            None,
            Some(forked_at),
            None,
            true,
            hilbert_tails,
            None,
            None,
        )?;
        for record in base_records {
            if !record.tombstone {
                base_by_addr.insert(record.address.clone(), record);
            }
        }

        let target_records = query_inner(
            store,
            snapshots,
            live_tail,
            spaces,
            watermark,
            *space,
            None,
            None,
            None,
            true,
            hilbert_tails,
            if target == BranchId::MAIN {
                None
            } else {
                Some(branch_overlays)
            },
            if target == BranchId::MAIN {
                None
            } else {
                Some(target)
            },
        )?;
        for record in target_records {
            if !record.tombstone {
                target_by_addr.insert(record.address.clone(), record);
            }
        }
    }

    let mut conflicts = Vec::new();
    let mut to_apply: Vec<Record> = Vec::new();
    let mut auto_resolved = 0usize;

    for (address, source_record) in &source_by_addr {
        let base = base_by_addr.get(address).cloned();
        let target_record = target_by_addr.get(address).cloned();

        let source_changed = match (&base, source_record) {
            (None, r) if r.tombstone => false,
            (Some(b), s) => !records_equivalent(b, s),
            (None, _) => true,
        };
        if !source_changed {
            continue;
        }

        let target_changed = match (&base, &target_record) {
            (None, None) => false,
            (Some(b), Some(t)) => !records_equivalent(b, t),
            (None, Some(_)) => true,
            (Some(_), None) => true,
        };

        let diverged = match (&target_record, source_record) {
            (Some(t), s) => !records_equivalent(t, s),
            (None, s) if !s.tombstone => true,
            _ => false,
        };

        if target_changed && diverged {
            let conflict = MergeConflict {
                address: address.clone(),
                base: base.clone(),
                target: target_record.clone(),
                source: Some(source_record.clone()),
            };
            match pick_winner(&conflict, strategy, resolver) {
                Ok(Some(winner)) => {
                    auto_resolved += 1;
                    to_apply.push(winner);
                }
                Ok(None) => {}
                Err(c) => conflicts.push(c),
            }
        } else {
            to_apply.push(source_record.clone());
        }
    }

    if strategy == MergeStrategy::Interactive && !conflicts.is_empty() {
        return Ok(MergeResult {
            merged_records: 0,
            conflicts,
            auto_resolved,
            applied_records: Vec::new(),
        });
    }

    Ok(MergeResult {
        merged_records: to_apply.len(),
        conflicts,
        auto_resolved,
        applied_records: to_apply,
    })
}
