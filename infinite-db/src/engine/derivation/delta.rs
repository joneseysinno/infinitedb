//! Per-session derivation delta merge helpers (Phase 6).

use crate::engine::session::VersionVector;
use crate::infinitedb_core::address::RevisionId;
use crate::infinitedb_core::hlc::SessionId;
use crate::infinitedb_core::block::Record;

/// Whether an assertion record belongs in the derivation delta for a subscriber.
pub fn record_in_derivation_delta(
    record: &Record,
    derivation_wm: &VersionVector,
    rev_ceiling: RevisionId,
    admitted_sessions: Option<&[SessionId]>,
) -> bool {
    let session = SessionId(record.revision.session());
    if let Some(filter) = admitted_sessions {
        if !filter.contains(&session) {
            return false;
        }
    }
    let wm = derivation_wm
        .get(session)
        .unwrap_or(RevisionId::ZERO);
    if record.revision <= wm || record.revision > rev_ceiling {
        return false;
    }
    true
}
