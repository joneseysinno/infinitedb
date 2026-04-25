use std::collections::HashMap;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use super::address::RevisionId;
use super::snapshot::SnapshotId;

/// Stable identifier for a branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct BranchId(pub u64);

/// A named, mutable pointer to a snapshot.
/// The `main` branch is the canonical truth. Other branches represent
/// offline or experimental divergences that can be merged back.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Branch {
    pub id: BranchId,
    pub name: String,
    /// The snapshot this branch currently points to.
    pub head: SnapshotId,
    /// The branch this was forked from, if any.
    pub parent: Option<BranchId>,
    /// The revision at which this branch was created.
    pub forked_at: RevisionId,
}

/// Registry of all branches in the database.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BranchRegistry {
    branches: HashMap<BranchId, Branch>,
    names: HashMap<String, BranchId>,
}

impl BranchRegistry {
    /// Create an empty branch registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a new branch definition.
    ///
    /// Returns an error when the branch name already exists.
    pub fn insert(&mut self, branch: Branch) -> Result<(), BranchError> {
        if self.names.contains_key(&branch.name) {
            return Err(BranchError::DuplicateName(branch.name.clone()));
        }
        self.names.insert(branch.name.clone(), branch.id);
        self.branches.insert(branch.id, branch);
        Ok(())
    }

    /// Look up a branch by ID.
    pub fn get(&self, id: BranchId) -> Option<&Branch> {
        self.branches.get(&id)
    }

    /// Look up a branch by name.
    pub fn get_by_name(&self, name: &str) -> Option<&Branch> {
        self.names.get(name).and_then(|id| self.branches.get(id))
    }

    /// Advance a branch's head to a new snapshot.
    pub fn advance(
        &mut self,
        id: BranchId,
        new_head: SnapshotId,
    ) -> Result<(), BranchError> {
        self.branches
            .get_mut(&id)
            .map(|b| b.head = new_head)
            .ok_or(BranchError::NotFound(id))
    }
}

/// Errors returned by branch registry operations.
#[derive(Debug)]
pub enum BranchError {
    /// A branch with the same name already exists.
    DuplicateName(String),
    /// The requested branch ID was not found.
    NotFound(BranchId),
}