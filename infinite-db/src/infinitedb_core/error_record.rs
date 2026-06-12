//! Operation-level error records — queryable MVCC failure provenance (M5).

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::address::{DimensionVector, RevisionId, SpaceId};

/// Inclusive revision span for an operation (import session, merge attempt, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct OperationRevisionRange {
    pub first: RevisionId,
    pub last: RevisionId,
}

impl OperationRevisionRange {
    pub fn empty() -> Self {
        Self {
            first: RevisionId::ZERO,
            last: RevisionId::ZERO,
        }
    }

    pub fn single(rev: RevisionId) -> Self {
        Self {
            first: rev,
            last: rev,
        }
    }

    pub fn new(first: RevisionId, last: RevisionId) -> Self {
        Self { first, last }
    }

    pub fn contains(&self, rev: RevisionId) -> bool {
        rev >= self.first && rev <= self.last
    }
}

/// Closed sum of operation-level failure kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Encode, Decode)]
pub enum ErrorKind {
    ImportValidation,
    ImportBudgetExceeded,
    MergeUnresolved,
    /// Durable session intent interrupted before checkpoint (D-P5).
    InterruptedSessionIntent,
    /// Address overlap detected at an intent checkpoint boundary (Phase 4).
    CheckpointCollision,
    Custom(u32),
}

/// One structured entry inside an operation error record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct ErrorRecordEntry {
    pub index: usize,
    pub item_id: u64,
    pub class: ErrorKind,
    pub message: String,
}

/// Operation-level failure persisted in a companion error space.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct OperationErrorRecord {
    pub kind: ErrorKind,
    pub revision_range: OperationRevisionRange,
    pub source_space: SpaceId,
    pub entries: Vec<ErrorRecordEntry>,
}

/// Storage point for an error record keyed by operation revision range start.
pub fn error_storage_point(range_start: RevisionId) -> DimensionVector {
    DimensionVector::new(vec![
        (range_start.legacy_sequence() >> 32) as u32,
        (range_start.legacy_sequence() & 0xFFFF_FFFF) as u32,
    ])
}

/// Inverse of [`error_storage_point`].
pub fn revision_from_error_point(point: &DimensionVector) -> Option<RevisionId> {
    if point.coords.len() != 2 {
        return None;
    }
    Some(RevisionId::legacy(
        ((point.coords[0] as u64) << 32) | (point.coords[1] as u64),
    ))
}
