//! Branch merge types and conflict resolution strategies.

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::address::Address;
use super::block::Record;
use super::branch::BranchId;

/// How to resolve an address-level conflict during [`crate::InfiniteDb::merge_branch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub enum MergeStrategy {
    /// Keep the target branch's version on conflict.
    PreferTarget,
    /// Take the source branch's version on conflict.
    PreferSource,
    /// Highest revision wins.
    PreferHigherRevision,
    /// Return conflicts to the caller without applying them.
    Interactive,
}

/// One address where source and target diverged from the merge base.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct MergeConflict {
    pub address: Address,
    pub base: Option<Record>,
    pub target: Option<Record>,
    pub source: Option<Record>,
}

/// Outcome of a branch merge operation.
#[derive(Debug, Clone, Encode, Decode)]
pub struct MergeResult {
    pub merged_records: usize,
    pub conflicts: Vec<MergeConflict>,
    pub auto_resolved: usize,
    /// Records selected for application to the target branch.
    pub applied_records: Vec<Record>,
}

/// Audit entry persisted under `spaces/<id>/merge_log.bin` (format v4).
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct MergeLogEntry {
    pub target: BranchId,
    pub source: BranchId,
    pub merged_records: usize,
    pub conflict_count: usize,
    pub at_revision: u64,
}
