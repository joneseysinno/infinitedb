//! Assertion events fed to the derivation bus.

use crate::infinitedb_core::{
    address::{RevisionId, SpaceId},
    branch::BranchId,
    hyperedge::Hyperedge,
};

/// Operation on a hyperedge assertion stream.
#[derive(Debug, Clone)]
pub enum AssertionOp {
    Upsert(Hyperedge),
    Delete(Hyperedge),
}

impl AssertionOp {
    pub fn edge_id(&self) -> crate::infinitedb_core::hyperedge::HyperedgeId {
        match self {
            AssertionOp::Upsert(e) | AssertionOp::Delete(e) => e.id,
        }
    }
}

/// One committed assertion the bus derives index geometry from.
#[derive(Debug, Clone)]
pub struct AssertionEvent {
    pub edge_space: SpaceId,
    pub op: AssertionOp,
    pub source_revision: RevisionId,
    pub branch: BranchId,
}

impl AssertionEvent {
    pub fn upsert(
        edge_space: SpaceId,
        edge: Hyperedge,
        source_revision: RevisionId,
        branch: BranchId,
    ) -> Self {
        Self {
            edge_space,
            op: AssertionOp::Upsert(edge),
            source_revision,
            branch,
        }
    }

    pub fn delete(
        edge_space: SpaceId,
        edge: Hyperedge,
        source_revision: RevisionId,
        branch: BranchId,
    ) -> Self {
        Self {
            edge_space,
            op: AssertionOp::Delete(edge),
            source_revision,
            branch,
        }
    }

    pub fn edge_id(&self) -> crate::infinitedb_core::hyperedge::HyperedgeId {
        self.op.edge_id()
    }
}
