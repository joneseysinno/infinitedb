//! Computation provenance — structured input lineage on hyperedges (M7).

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::judgment::SubjectPin;

/// Structured input pins for a derived hyperedge assertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct ComputationProvenance {
    pub inputs: Vec<SubjectPin>,
}

/// Validation failures for computation provenance on write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComputationValidationError {
    EmptyInputs,
    InputNotFound {
        index: usize,
        space: super::address::SpaceId,
        revision: super::address::RevisionId,
    },
    InputRevisionMismatch {
        index: usize,
        expected: super::address::RevisionId,
        observed: super::address::RevisionId,
    },
    InvalidInputPin {
        index: usize,
        message: String,
    },
}

impl std::fmt::Display for ComputationValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComputationValidationError::EmptyInputs => write!(f, "computation inputs cannot be empty"),
            ComputationValidationError::InputNotFound {
                index,
                space,
                revision,
            } => write!(
                f,
                "computation input {index} not found at space {space:?} revision {revision:?}"
            ),
            ComputationValidationError::InputRevisionMismatch {
                index,
                expected,
                observed,
            } => write!(
                f,
                "computation input {index} revision mismatch: expected {expected:?}, observed {observed:?}"
            ),
            ComputationValidationError::InvalidInputPin { index, message } => {
                write!(f, "computation input {index}: {message}")
            }
        }
    }
}
