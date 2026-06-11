//! Authoring-frame provenance carried on testimony assertions (M5).

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::address::RevisionId;

/// Opaque frame identifier until M6 named durable frames land.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Encode, Decode,
)]
pub struct FrameId(pub u64);

/// Frame the author was reading when an assertion was made.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct AuthoringFrameProvenance {
    pub frame_id: FrameId,
    pub as_of: RevisionId,
}

/// Validation failures for authoring-frame provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProvenanceError {
    AsOfExceedsCommit { as_of: RevisionId, commit_ceiling: RevisionId },
}

impl std::fmt::Display for ProvenanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProvenanceError::AsOfExceedsCommit {
                as_of,
                commit_ceiling,
            } => write!(
                f,
                "authoring frame as_of {as_of:?} exceeds commit ceiling {commit_ceiling:?}"
            ),
        }
    }
}
