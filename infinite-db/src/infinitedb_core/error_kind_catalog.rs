//! Error kind catalog for forward-compatible operation error records (M5).

use super::error_record::ErrorKind;

/// Policy when an unknown custom error kind id is encountered on decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UnknownErrorKindPolicy {
    #[default]
    AllowUnknown,
    RejectUnknown,
}

/// Registry of known operation error kinds.
#[derive(Debug, Clone, Default)]
pub struct ErrorKindCatalog {
    policy: UnknownErrorKindPolicy,
}

impl ErrorKindCatalog {
    pub fn new(policy: UnknownErrorKindPolicy) -> Self {
        Self { policy }
    }

    pub fn validate_kind(&self, kind: &ErrorKind) -> Result<(), ErrorKindCatalogError> {
        match kind {
            ErrorKind::ImportValidation
            | ErrorKind::ImportBudgetExceeded
            | ErrorKind::MergeUnresolved
            | ErrorKind::InterruptedSessionIntent
            | ErrorKind::CheckpointCollision => Ok(()),
            ErrorKind::Custom(id) => {
                if *id > 10_000 {
                    match self.policy {
                        UnknownErrorKindPolicy::AllowUnknown => Ok(()),
                        UnknownErrorKindPolicy::RejectUnknown => {
                            Err(ErrorKindCatalogError::UnknownCustomKind(*id))
                        }
                    }
                } else {
                    Ok(())
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKindCatalogError {
    UnknownCustomKind(u32),
}
