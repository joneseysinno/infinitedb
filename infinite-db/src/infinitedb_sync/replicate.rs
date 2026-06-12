//! Branch-aware replication helpers for [`crate::InfiniteDb`].

use std::collections::HashMap;

use bincode::{config::standard, encode_to_vec};

use crate::engine::error::EngineError;
use crate::engine::query::address_key;
use crate::infinitedb_core::{
    address::{DimensionVector, RevisionId, SpaceId},
    block::Record,
    branch::BranchId,
    merge::MergeStrategy,
    record_identity::AddressKey,
    space::SpaceRegistry,
};
use crate::InfiniteDb;

use super::merkle::{self, MerkleTree};

/// Negotiation payload exchanged before overlay transfer.
#[derive(Debug, Clone)]
pub struct BranchSyncState {
    pub branch: BranchId,
    pub merkle_root: [u8; 32],
    pub revision: RevisionId,
}

fn latest_per_address(spaces: &SpaceRegistry, mut records: Vec<Record>) -> Vec<Record> {
    let mut map: HashMap<AddressKey, Record> = HashMap::new();
    for record in records.drain(..) {
        let key = address_key(spaces, &record);
        map.entry(key)
            .and_modify(|existing| {
                if record.revision > existing.revision {
                    *existing = record.clone();
                }
            })
            .or_insert(record);
    }
    let mut latest: Vec<Record> = map.into_values().collect();
    latest.sort_by_key(|r| (r.address.point.coords.clone(), r.revision.legacy_sequence()));
    latest
}

/// Build a Merkle tree over latest visible records on `branch` in `space`.
pub fn snapshot_merkle(
    db: &InfiniteDb,
    space: SpaceId,
    branch: BranchId,
) -> Result<MerkleTree, EngineError> {
    let spaces = db.spaces.read();
    let records = latest_per_address(&spaces, db.query_on_branch(branch, space, None)?);
    let mut leaves = Vec::with_capacity(records.len());
    for record in &records {
        let encoded = encode_to_vec(
            (&record.address, &record.data, record.tombstone),
            standard(),
        )
        .map_err(|e| EngineError::Other {
            message: e.to_string(),
        })?;
        leaves.push(merkle::hash_record(&encoded));
    }
    Ok(MerkleTree::build(&leaves))
}

/// Current branch sync state for wire exchange.
pub fn branch_sync_state(
    db: &InfiniteDb,
    space: SpaceId,
    branch: BranchId,
) -> Result<BranchSyncState, EngineError> {
    Ok(BranchSyncState {
        branch,
        merkle_root: snapshot_merkle(db, space, branch)?.root(),
        revision: db.revision(),
    })
}

/// Copy overlay records from `remote` branch into a new branch on `local`.
pub fn import_branch_overlay(
    local: &InfiniteDb,
    remote: &InfiniteDb,
    remote_branch: BranchId,
    name: &str,
) -> Result<BranchId, String> {
    remote.sync().map_err(|e| e.to_string())?;
    let branch = local.create_branch(name, BranchId::MAIN)?;
    let records = remote.branch_overlays.all_live_records(remote_branch);
    local
        .apply_records_on_branch(branch, records)
        .map_err(|e| e.to_string())?;
    Ok(branch)
}

/// Fast-forward `local` main with records present on `remote` main (no conflict handling).
pub fn converge_main_records(
    local: &InfiniteDb,
    remote: &InfiniteDb,
    space: SpaceId,
) -> Result<(), EngineError> {
    remote.sync()?;
    let remote_records = remote.query(space, None)?;
    let local_records = local.query(space, None)?;
    let mut local_latest: std::collections::HashMap<Vec<u32>, Record> =
        std::collections::HashMap::new();
    for r in local_records {
        let key = r.address.point.coords.clone();
        local_latest
            .entry(key)
            .and_modify(|e| {
                if r.revision > e.revision {
                    *e = r.clone();
                }
            })
            .or_insert(r);
    }
    let mut rows: Vec<(DimensionVector, Vec<u8>)> = Vec::new();
    for record in remote_records {
        let coords = record.address.point.coords.clone();
        let insert = match local_latest.get(&coords) {
            None => true,
            Some(existing) => record.revision > existing.revision,
        };
        if insert && !record.tombstone {
            rows.push((DimensionVector::new(coords), record.data));
        }
    }
    if !rows.is_empty() {
        local.insert_many(space, rows)?;
    }
    local.sync()
}

/// Import a remote feature branch and merge it into local `main`.
pub fn converge_with_branch_merge(
    local: &InfiniteDb,
    remote: &InfiniteDb,
    space: SpaceId,
    remote_branch: BranchId,
    strategy: MergeStrategy,
) -> Result<(), EngineError> {
    converge_main_records(local, remote, space)?;
    let imported = import_branch_overlay(local, remote, remote_branch, "sync-import")
        .map_err(|e| EngineError::Other { message: e })?;
    let result = local.merge_branch(BranchId::MAIN, imported, strategy, None)?;
    if !result.conflicts.is_empty() {
        return Err(EngineError::Other {
            message: format!("sync merge left {} conflicts", result.conflicts.len()),
        });
    }
    local.flush(space)?;
    local.sync()
}
