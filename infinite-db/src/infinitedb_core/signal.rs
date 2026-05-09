use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use super::address::{DimensionVector, RevisionId};

/// Stable identifier for a signal family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct SignalId(pub u64);

/// User-defined signal kind label.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct SignalKind(pub String);

impl SignalKind {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Parent-scoped signal domain: fixes a prefix in hyperspace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct SignalScope {
    pub parent_prefix: DimensionVector,
    pub total_dims: usize,
}

/// Optional runtime limits for signal values.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct SignalConstraint {
    pub min_milli: Option<i64>,
    pub max_milli: Option<i64>,
    pub clamp_on_write: bool,
}

/// One measured or computed signal value at a local coordinate.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct SignalSample {
    pub signal_id: SignalId,
    pub kind: SignalKind,
    pub scope: SignalScope,
    pub local_coords: Vec<u32>,
    pub value_milli: i64,
    pub source_revision: Option<RevisionId>,
    pub constraint: Option<SignalConstraint>,
}

impl SignalScope {
    /// Full dimensional address for this scope + local coordinates.
    pub fn address_coords(&self, local_coords: &[u32]) -> Result<Vec<u32>, SignalValidationError> {
        if self.parent_prefix.dims() + local_coords.len() != self.total_dims {
            return Err(SignalValidationError::CoordinateDimensionMismatch {
                expected_total: self.total_dims,
                parent_dims: self.parent_prefix.dims(),
                local_dims: local_coords.len(),
            });
        }
        let mut coords = self.parent_prefix.coords.clone();
        coords.extend_from_slice(local_coords);
        Ok(coords)
    }
}

impl SignalSample {
    /// Validate signal sample consistency.
    pub fn validate(&self) -> Result<(), SignalValidationError> {
        let _ = self.scope.address_coords(&self.local_coords)?;
        if self.kind.0.trim().is_empty() {
            return Err(SignalValidationError::EmptyKind);
        }
        if let Some(c) = &self.constraint {
            if let (Some(min), Some(max)) = (c.min_milli, c.max_milli) {
                if min > max {
                    return Err(SignalValidationError::InvalidConstraintRange { min, max });
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum SignalValidationError {
    EmptyKind,
    CoordinateDimensionMismatch {
        expected_total: usize,
        parent_dims: usize,
        local_dims: usize,
    },
    InvalidConstraintRange {
        min: i64,
        max: i64,
    },
}
