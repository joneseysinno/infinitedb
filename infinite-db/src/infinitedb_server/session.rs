//! Session: authenticated connection state for a single client.
//!
//! A session is created when a client connects and tracks:
//!   - Which spaces the client is authorised to access.
//!   - The active branch the client is reading/writing against.
//!   - The revision clock at the time the session was opened (snapshot isolation).

use bincode::{Decode, Encode};
use crate::infinitedb_core::{
    address::{RevisionId, SpaceId},
    branch::BranchId,
    snapshot::SnapshotId,
};

/// Opaque session token — 16 random bytes, base16-encoded for logging.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(pub [u8; 16]);

impl SessionId {
    /// Generate a new random session ID.
    pub fn new_random() -> Self {
        // Use the OS CSPRNG via getrandom.
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).expect("getrandom failed");
        Self(bytes)
    }

    /// Return the session ID as lower-case hexadecimal.
    pub fn as_hex(&self) -> String {
        self.0.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

/// Access level granted to a session for a particular space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum AccessLevel {
    /// Read-only: queries and snapshot exports only.
    ReadOnly,
    /// Read-write: writes, tombstones, branch operations.
    ReadWrite,
    /// Admin: includes compaction triggers and space management.
    Admin,
}

/// Per-space authorisation entry.
#[derive(Debug, Clone)]
pub struct SpaceGrant {
    pub space: SpaceId,
    pub level: AccessLevel,
}

/// Live session state.
#[derive(Debug)]
pub struct Session {
    pub id: SessionId,
    /// The branch this session is operating against.
    pub branch: BranchId,
    /// The snapshot pinned at session open (provides consistent reads).
    pub pinned_snapshot: SnapshotId,
    /// Revision at session open — used to scope `as_of` queries.
    pub opened_at: RevisionId,
    /// Spaces this session may access.
    grants: Vec<SpaceGrant>,
}

impl Session {
    /// Create a new session with branch/snapshot context and space grants.
    pub fn new(
        branch: BranchId,
        pinned_snapshot: SnapshotId,
        opened_at: RevisionId,
        grants: Vec<SpaceGrant>,
    ) -> Self {
        Self {
            id: SessionId::new_random(),
            branch,
            pinned_snapshot,
            opened_at,
            grants,
        }
    }

    /// Return the access level for `space`, or `None` if not authorised.
    pub fn access(&self, space: SpaceId) -> Option<AccessLevel> {
        self.grants
            .iter()
            .find(|g| g.space == space)
            .map(|g| g.level)
    }

    /// Return `true` if the session may write to `space`.
    pub fn can_write(&self, space: SpaceId) -> bool {
        matches!(
            self.access(space),
            Some(AccessLevel::ReadWrite) | Some(AccessLevel::Admin)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::address::{RevisionId, SpaceId};
    use crate::infinitedb_core::branch::BranchId;
    use crate::infinitedb_core::snapshot::SnapshotId;

    fn make_session(level: AccessLevel) -> Session {
        Session::new(
            BranchId(1),
            SnapshotId(1),
            RevisionId(0),
            vec![SpaceGrant { space: SpaceId(1), level }],
        )
    }

    #[test]
    fn read_only_cannot_write() {
        let s = make_session(AccessLevel::ReadOnly);
        assert!(!s.can_write(SpaceId(1)));
    }

    #[test]
    fn read_write_can_write() {
        let s = make_session(AccessLevel::ReadWrite);
        assert!(s.can_write(SpaceId(1)));
    }

    #[test]
    fn no_grant_returns_none() {
        let s = make_session(AccessLevel::ReadOnly);
        assert!(s.access(SpaceId(99)).is_none());
    }
}
