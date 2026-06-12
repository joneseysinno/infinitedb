//! Intent checkpoint recovery and grouping (peer track Phase 4).

pub use crate::infinitedb_core::intent_checkpoint::IntentCheckpoint;

use crate::infinitedb_core::address::RevisionId;
use crate::infinitedb_storage::session_wal::SessionWalFrame;
use crate::infinitedb_storage::wal::WalEntry;

/// One checkpointed, committed intent group recovered from a session WAL.
#[derive(Debug, Clone)]
pub struct IntentGroup {
    pub checkpoint: IntentCheckpoint,
    pub entries: Vec<WalEntry>,
}

/// Split durable session-WAL frames into committed intent groups and a trailing uncommitted tail.
///
/// An [`IntentCheckpoint`] frame closes the group of data frames immediately preceding it.
/// Any data frames after the last checkpoint (or the entire log when no checkpoint exists)
/// are durable-but-uncommitted per D-P5.
pub fn split_session_wal_frames(frames: Vec<SessionWalFrame>) -> (Vec<IntentGroup>, Vec<WalEntry>) {
    let mut groups = Vec::new();
    let mut pending = Vec::new();
    for frame in frames {
        match frame {
            SessionWalFrame::Data(entry) => pending.push(entry),
            SessionWalFrame::Intent(checkpoint) => {
                if !pending.is_empty() {
                    groups.push(IntentGroup {
                        checkpoint,
                        entries: pending,
                    });
                    pending = Vec::new();
                }
            }
        }
    }
    (groups, pending)
}

pub fn wal_entry_revision(entry: &WalEntry) -> Option<RevisionId> {
    match entry {
        WalEntry::Write { revision, .. } | WalEntry::Tombstone { revision, .. } => Some(*revision),
        WalEntry::Checkpoint { revision } => Some(*revision),
        WalEntry::BlockSealed { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::address::{Address, DimensionVector, SpaceId};
    use crate::infinitedb_core::hlc::HlcStamp;
    use crate::infinitedb_core::intent_checkpoint::IntentOperationKind;

    fn write(session: u32, seq: u32) -> WalEntry {
        WalEntry::Write {
            address: Address::new(SpaceId(1), DimensionVector::new(vec![session, seq])),
            revision: RevisionId::from_stamp(HlcStamp {
                physical_ms: 1,
                logical: 0,
                session,
                sequence: seq,
            }),
            data: vec![seq as u8],
        }
    }

    #[test]
    fn split_committed_group_and_uncommitted_tail() {
        let r1 = wal_entry_revision(&write(1, 1)).unwrap();
        let r2 = wal_entry_revision(&write(1, 2)).unwrap();
        let cp = IntentCheckpoint::new(r1, r2, IntentOperationKind::Insert);
        let frames = vec![
            SessionWalFrame::Data(write(1, 1)),
            SessionWalFrame::Data(write(1, 2)),
            SessionWalFrame::Intent(cp),
            SessionWalFrame::Data(write(1, 3)),
        ];
        let (groups, tail) = split_session_wal_frames(frames);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].entries.len(), 2);
        assert_eq!(tail.len(), 1);
    }

    #[test]
    fn all_durable_without_checkpoint_is_uncommitted() {
        let frames = vec![
            SessionWalFrame::Data(write(2, 1)),
            SessionWalFrame::Data(write(2, 2)),
        ];
        let (groups, tail) = split_session_wal_frames(frames);
        assert!(groups.is_empty());
        assert_eq!(tail.len(), 2);
    }
}
