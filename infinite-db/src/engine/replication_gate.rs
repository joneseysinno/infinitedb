//! Session WAL replication gate policy (hardening D-P8).

/// When replication confirmation is required before WAL retirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationGatePolicy {
    /// Replication target configured — gates must be marked explicitly.
    Required,
    /// Embedded-only: auto-certify through the sealed revision.
    NotApplicable,
}

impl Default for ReplicationGatePolicy {
    fn default() -> Self {
        Self::NotApplicable
    }
}
