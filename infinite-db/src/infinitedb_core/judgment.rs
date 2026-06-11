//! Judgment record conventions — arbiter testimony pinned to subjects (M5).

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::{
    address::{Address, DimensionVector, RevisionId, SpaceId},
    hyperedge::HyperedgeId,
    provenance::AuthoringFrameProvenance,
};

/// Arbiter stream identity.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Encode, Decode,
)]
pub struct ArbiterId(pub u64);

/// Judgment assertion identity within an arbiter stream.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Encode, Decode,
)]
pub struct JudgmentId(pub u64);

/// Subject category for judgment pinning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub enum SubjectKind {
    Hyperedge,
    Node,
    Signal,
}

/// Stable subject identity for a judgment.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub enum SubjectIdentity {
    Hyperedge(HyperedgeId),
    Address(Address),
}

/// Exact subject revision a judgment evaluates.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct SubjectPin {
    pub kind: SubjectKind,
    pub space: SpaceId,
    pub identity: SubjectIdentity,
    pub subject_revision: RevisionId,
}

/// Arbiter verdict on a pinned subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub enum JudgmentVerdict {
    Pass,
    Fail,
    Conflict,
    Annotate,
    Custom(String),
}

/// Judgment assertion stored in an arbiter stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct JudgmentRecord {
    pub id: JudgmentId,
    pub arbiter: ArbiterId,
    pub subject: SubjectPin,
    pub verdict: JudgmentVerdict,
    pub rationale: Option<String>,
    pub authoring_frame: Option<AuthoringFrameProvenance>,
}

/// Registered arbiter testimony stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct ArbiterStream {
    pub id: ArbiterId,
    pub assertion_space: SpaceId,
}

/// Validation failures for judgment assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JudgmentValidationError {
    SubjectNotFound { space: SpaceId, revision: RevisionId },
    SubjectRevisionMismatch {
        expected: RevisionId,
        observed: RevisionId,
    },
    UnknownArbiter(ArbiterId),
    InvalidSubjectPin(String),
}

impl std::fmt::Display for JudgmentValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JudgmentValidationError::SubjectNotFound { space, revision } => {
                write!(f, "subject not found in space {space:?} at revision {revision:?}")
            }
            JudgmentValidationError::SubjectRevisionMismatch { expected, observed } => write!(
                f,
                "subject revision mismatch: expected {expected:?}, observed {observed:?}"
            ),
            JudgmentValidationError::UnknownArbiter(id) => {
                write!(f, "unknown arbiter stream {:?}", id.0)
            }
            JudgmentValidationError::InvalidSubjectPin(msg) => write!(f, "{msg}"),
        }
    }
}

/// Storage point for a judgment assertion in an arbiter space.
pub fn judgment_storage_point(id: JudgmentId) -> DimensionVector {
    DimensionVector::new(vec![
        (id.0 >> 32) as u32,
        (id.0 & 0xFFFF_FFFF) as u32,
    ])
}

pub fn judgment_id_from_storage_point(point: &DimensionVector) -> Option<JudgmentId> {
    if point.coords.len() != 2 {
        return None;
    }
    Some(JudgmentId(
        ((point.coords[0] as u64) << 32) | (point.coords[1] as u64),
    ))
}
