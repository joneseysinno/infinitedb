//! Engine-layer error algebra — embeds storage errors losslessly.

use crate::infinitedb_core::{
    address::SpaceId,
    branch::BranchId,
    computation::ComputationValidationError,
    error_kind_catalog::ErrorKindCatalogError,
    frame::FrameValidationError,
    hyperedge::HyperedgeValidationError,
    judgment::JudgmentValidationError,
    provenance::{FrameId, ProvenanceError},
    space::SpaceError,
};
use crate::infinitedb_storage::error::StorageError;

/// Logical consistency failures above the storage layer.
#[derive(Debug, Clone)]
pub enum EngineError {
    Storage(StorageError),
    SpaceNotFound(SpaceId),
    BranchNotFound(BranchId),
    BranchExists(String),
    InvalidSpaceConfig {
        message: String,
    },
    RegistrySpace(SpaceError),
    RegistryBranch(crate::infinitedb_core::branch::BranchError),
    InvalidHyperedge(HyperedgeValidationError),
    DerivationBackpressure {
        pending_tasks: usize,
        derivation_lag: u64,
    },
    WatermarkViolation {
        message: String,
    },
    EndpointIndexMissing,
    ErrorSpaceMissing(SpaceId),
    ErrorRecordEncode {
        message: String,
    },
    ErrorRecordDecode {
        message: String,
    },
    InvalidJudgment(JudgmentValidationError),
    InvalidProvenance(ProvenanceError),
    ErrorKindCatalog(ErrorKindCatalogError),
    ArbiterStreamExists(u64),
    ArbiterStreamNotFound(u64),
    FrameExists(String),
    FrameNotFound(FrameId),
    InvalidFrame(FrameValidationError),
    InvalidComputation(ComputationValidationError),
    Other {
        message: String,
    },
}

impl EngineError {
    pub fn is_retryable(&self) -> bool {
        match self {
            EngineError::Storage(e) => e.is_retryable(),
            EngineError::DerivationBackpressure { .. } => true,
            _ => false,
        }
    }

    pub fn is_caller_correctable(&self) -> bool {
        matches!(
            self,
            EngineError::InvalidHyperedge(_)
                | EngineError::InvalidSpaceConfig { .. }
                | EngineError::RegistrySpace(SpaceError::DuplicateId(_))
                | EngineError::RegistrySpace(SpaceError::DuplicateName(_))
                | EngineError::RegistryBranch(
                    crate::infinitedb_core::branch::BranchError::DuplicateName(_)
                )
                | EngineError::BranchNotFound(_)
                | EngineError::SpaceNotFound(_)
                | EngineError::ErrorSpaceMissing(_)
                | EngineError::InvalidJudgment(_)
                | EngineError::InvalidProvenance(_)
                | EngineError::ArbiterStreamNotFound(_)
                | EngineError::ArbiterStreamExists(_)
                | EngineError::FrameExists(_)
                | EngineError::FrameNotFound(_)
                | EngineError::InvalidFrame(_)
                | EngineError::InvalidComputation(_)
        )
    }

    pub fn is_fatal(&self) -> bool {
        match self {
            EngineError::Storage(e) => e.is_fatal(),
            _ => false,
        }
    }
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::Storage(e) => write!(f, "{e}"),
            EngineError::SpaceNotFound(id) => write!(f, "space {:?} not found", id),
            EngineError::BranchNotFound(id) => write!(f, "branch {:?} not found", id),
            EngineError::BranchExists(name) => write!(f, "branch name already taken: {name}"),
            EngineError::InvalidSpaceConfig { message } => write!(f, "{message}"),
            EngineError::RegistrySpace(e) => write!(f, "{e:?}"),
            EngineError::RegistryBranch(e) => write!(f, "{e:?}"),
            EngineError::InvalidHyperedge(e) => write!(f, "{e:?}"),
            EngineError::DerivationBackpressure {
                pending_tasks,
                derivation_lag,
            } => write!(
                f,
                "derivation backpressure: {pending_tasks} pending tasks, lag {derivation_lag}"
            ),
            EngineError::WatermarkViolation { message } => write!(f, "watermark: {message}"),
            EngineError::EndpointIndexMissing => write!(f, "endpoint index space missing"),
            EngineError::ErrorSpaceMissing(id) => write!(f, "error space missing for {:?}", id),
            EngineError::ErrorRecordEncode { message } => write!(f, "error record encode: {message}"),
            EngineError::ErrorRecordDecode { message } => write!(f, "error record decode: {message}"),
            EngineError::InvalidJudgment(e) => write!(f, "{e}"),
            EngineError::InvalidProvenance(e) => write!(f, "{e}"),
            EngineError::ErrorKindCatalog(e) => write!(f, "{e:?}"),
            EngineError::ArbiterStreamExists(id) => write!(f, "arbiter stream {} exists", id),
            EngineError::ArbiterStreamNotFound(id) => write!(f, "arbiter stream {} not found", id),
            EngineError::FrameExists(name) => write!(f, "frame name already taken: {name}"),
            EngineError::FrameNotFound(id) => write!(f, "frame {:?} not found", id),
            EngineError::InvalidFrame(e) => write!(f, "{e}"),
            EngineError::InvalidComputation(e) => write!(f, "{e}"),
            EngineError::Other { message } => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<StorageError> for EngineError {
    fn from(err: StorageError) -> Self {
        EngineError::Storage(err)
    }
}

impl From<std::io::Error> for EngineError {
    fn from(err: std::io::Error) -> Self {
        EngineError::Storage(StorageError::from(err))
    }
}

impl From<SpaceError> for EngineError {
    fn from(err: SpaceError) -> Self {
        EngineError::RegistrySpace(err)
    }
}

impl From<crate::infinitedb_core::branch::BranchError> for EngineError {
    fn from(err: crate::infinitedb_core::branch::BranchError) -> Self {
        EngineError::RegistryBranch(err)
    }
}

impl From<HyperedgeValidationError> for EngineError {
    fn from(err: HyperedgeValidationError) -> Self {
        EngineError::InvalidHyperedge(err)
    }
}

impl From<ProvenanceError> for EngineError {
    fn from(err: ProvenanceError) -> Self {
        EngineError::InvalidProvenance(err)
    }
}

impl From<JudgmentValidationError> for EngineError {
    fn from(err: JudgmentValidationError) -> Self {
        EngineError::InvalidJudgment(err)
    }
}

impl From<ErrorKindCatalogError> for EngineError {
    fn from(err: ErrorKindCatalogError) -> Self {
        EngineError::ErrorKindCatalog(err)
    }
}

impl From<FrameValidationError> for EngineError {
    fn from(err: FrameValidationError) -> Self {
        EngineError::InvalidFrame(err)
    }
}

impl From<ComputationValidationError> for EngineError {
    fn from(err: ComputationValidationError) -> Self {
        EngineError::InvalidComputation(err)
    }
}

/// Map engine errors to `io::Error` for APIs that still return `io::Result`.
pub fn engine_to_io(err: EngineError) -> std::io::Error {
    let kind = if err.is_caller_correctable() {
        std::io::ErrorKind::InvalidInput
    } else if err.is_retryable() {
        std::io::ErrorKind::WouldBlock
    } else {
        std::io::ErrorKind::Other
    };
    std::io::Error::new(kind, err.to_string())
}
