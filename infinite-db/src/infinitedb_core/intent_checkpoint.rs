//! Intent checkpoint descriptors (peer track Phase 4).

use bincode::{Decode, Encode};

use super::address::RevisionId;
use super::error_record::OperationRevisionRange;

/// Operation kind tag carried on an intent checkpoint frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum IntentOperationKind {
    General,
    Insert,
    HypergraphWrite,
    Custom(u32),
}

/// Marks that all WAL frames since the previous checkpoint form one logical operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct IntentCheckpoint {
    pub first_revision: RevisionId,
    pub last_revision: RevisionId,
    pub kind: IntentOperationKind,
}

impl IntentCheckpoint {
    pub fn new(
        first_revision: RevisionId,
        last_revision: RevisionId,
        kind: IntentOperationKind,
    ) -> Self {
        Self {
            first_revision,
            last_revision,
            kind,
        }
    }

    pub fn revision_range(&self) -> OperationRevisionRange {
        OperationRevisionRange::new(self.first_revision, self.last_revision)
    }
}
