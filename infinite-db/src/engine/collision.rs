//! Intent-checkpoint collision evaluation (hardening Wave C).

use crate::infinitedb_core::{
    address::{Address, RevisionId},
    hlc::SessionId,
    intent_checkpoint::IntentCheckpoint,
};

/// Structured collision at an intent checkpoint — returned as value, not veto.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollisionEvaluation {
    pub address: Address,
    pub sessions: Vec<SessionId>,
    pub revisions: Vec<RevisionId>,
}

impl CollisionEvaluation {
    pub fn new(address: Address, sessions: Vec<SessionId>, revisions: Vec<RevisionId>) -> Self {
        Self {
            address,
            sessions,
            revisions,
        }
    }
}

/// Successful intent commit — collisions travel in the Ok channel (error algebra rule 3).
#[derive(Debug, Clone)]
pub struct IntentCommitOutcome {
    pub checkpoint: IntentCheckpoint,
    pub collisions: Vec<CollisionEvaluation>,
}
