//! Named durable frame definitions — three-axis truth query configuration (M6).

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::{
    address::{RevisionId, SpaceId},
    branch::BranchId,
    endpoint_index::ENDPOINT_INDEX_SPACE,
    hlc::SessionId,
    judgment::ArbiterId,
    flow_vector_index::FLOW_VECTOR_INDEX_SPACE,
    judgment_index::JUDGMENT_INDEX_SPACE,
    provenance::FrameId,
};
use std::collections::{BTreeMap, HashSet};

/// Assertion scope: which testimony streams are admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub enum AssertionScope {
    /// Union of testimony from these hyperedge spaces on `MAIN`.
    Spaces(Vec<SpaceId>),
    /// Read testimony via branch overlay for a given hyperedge space at query time.
    Branches(Vec<BranchId>),
    /// Admit testimony authored by these asserting sessions (Phase 5 / §5.5).
    Session(Vec<SessionId>),
    /// Union of multiple scopes.
    Union(Vec<AssertionScope>),
}

/// Effect policy for a consulted arbiter judgment layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub enum OverlayPolicy {
    /// Remove assertions condemned by this arbiter layer.
    Suppress,
    /// Keep assertions; attach matching judgments.
    Annotate,
    /// Keep only contested assertions (conflict judgments or cross-source disagreement).
    SelectContested,
}

/// Optional verdict gate for an overlay layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub enum VerdictFilter {
    Any,
    Pass,
    Fail,
    Conflict,
    Annotate,
}

impl Default for VerdictFilter {
    fn default() -> Self {
        Self::Any
    }
}

/// One arbiter stream consulted with a policy during frame resolution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct JudgmentOverlayLayer {
    pub arbiter: ArbiterId,
    pub policy: OverlayPolicy,
    #[serde(default)]
    pub verdict_filter: VerdictFilter,
}

/// Named durable frame — stable application reference to a three-axis epistemic position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct FrameDefinition {
    pub id: FrameId,
    pub name: String,
    pub assertion_scope: AssertionScope,
    pub judgment_overlay: Vec<JudgmentOverlayLayer>,
    pub default_as_of: Option<RevisionId>,
}

/// Request to register a frame; `id` may be explicit for provenance compatibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameRegisterRequest {
    pub id: Option<FrameId>,
    pub name: String,
    pub assertion_scope: AssertionScope,
    pub judgment_overlay: Vec<JudgmentOverlayLayer>,
    pub default_as_of: Option<RevisionId>,
}

/// Admission source for testimony reads during frame resolution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TestimonySource {
    pub space: SpaceId,
    pub branch: Option<BranchId>,
    /// When set, only records whose revision session component is listed are admitted.
    pub sessions: Option<Vec<SessionId>>,
}

/// Frame validation failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameValidationError {
    DuplicateName(String),
    DuplicateId(FrameId),
    SpaceNotRegistered(SpaceId),
    BranchNotFound(BranchId),
    ArbiterNotRegistered(ArbiterId),
    ReservedSpace(SpaceId),
    EmptyName,
    EmptyScope,
    SessionNotRegistered(SessionId),
}

impl std::fmt::Display for FrameValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameValidationError::DuplicateName(n) => write!(f, "frame name already taken: {n}"),
            FrameValidationError::DuplicateId(id) => write!(f, "frame id {:?} already exists", id),
            FrameValidationError::SpaceNotRegistered(id) => {
                write!(f, "space {:?} not registered", id)
            }
            FrameValidationError::BranchNotFound(id) => write!(f, "branch {:?} not found", id),
            FrameValidationError::ArbiterNotRegistered(id) => {
                write!(f, "arbiter stream {} not registered", id.0)
            }
            FrameValidationError::ReservedSpace(id) => write!(f, "reserved space {:?} cannot be testimony", id),
            FrameValidationError::EmptyName => write!(f, "frame name cannot be empty"),
            FrameValidationError::EmptyScope => write!(f, "assertion scope cannot be empty"),
            FrameValidationError::SessionNotRegistered(id) => {
                write!(f, "session {:?} not registered", id.0)
            }
        }
    }
}

/// Whether a space may hold hyperedge testimony in frame queries.
pub fn is_testimony_space(space: SpaceId) -> bool {
    space != ENDPOINT_INDEX_SPACE
        && space != JUDGMENT_INDEX_SPACE
        && space != FLOW_VECTOR_INDEX_SPACE
        && space.0 < 0xA000_0000_0000_0000
}

/// Whether a record's session component is admitted by a testimony source filter.
pub fn record_admitted_by_source(
    record_session: u32,
    source: &TestimonySource,
) -> bool {
    source
        .sessions
        .as_ref()
        .map_or(true, |sessions| sessions.iter().any(|s| s.0 == record_session))
}

fn merge_session_filters(
    a: Option<HashSet<SessionId>>,
    b: Option<HashSet<SessionId>>,
) -> Option<HashSet<SessionId>> {
    match (a, b) {
        (None, _) | (_, None) => None,
        (Some(a), Some(b)) => Some(a.union(&b).copied().collect()),
    }
}

/// Flatten assertion scope into merged testimony sources for a hyperedge space.
pub fn merge_admission_specs(
    scope: &AssertionScope,
    testimony_space: SpaceId,
) -> Vec<TestimonySource> {
    let raw = flatten_assertion_scope(scope, testimony_space);
    let mut merged: BTreeMap<(SpaceId, Option<BranchId>), Option<HashSet<SessionId>>> =
        BTreeMap::new();
    for source in raw {
        let key = (source.space, source.branch);
        let sessions = source
            .sessions
            .map(|v| v.into_iter().collect::<HashSet<_>>());
        match merged.get_mut(&key) {
            Some(existing) => {
                *existing = merge_session_filters(existing.clone(), sessions);
            }
            None => {
                merged.insert(key, sessions);
            }
        }
    }
    merged
        .into_iter()
        .map(|((space, branch), sessions)| TestimonySource {
            space,
            branch,
            sessions: sessions.map(|set| {
                let mut v: Vec<SessionId> = set.into_iter().collect();
                v.sort_by_key(|s| s.0);
                v
            }),
        })
        .collect()
}

/// Flatten assertion scope into testimony sources for a specific hyperedge space.
pub fn flatten_assertion_scope(scope: &AssertionScope, testimony_space: SpaceId) -> Vec<TestimonySource> {
    match scope {
        AssertionScope::Spaces(spaces) => spaces
            .iter()
            .filter(|s| **s == testimony_space)
            .map(|_| TestimonySource {
                space: testimony_space,
                branch: None,
                sessions: None,
            })
            .collect(),
        AssertionScope::Branches(branches) => branches
            .iter()
            .map(|b| TestimonySource {
                space: testimony_space,
                branch: Some(*b),
                sessions: None,
            })
            .collect(),
        AssertionScope::Session(sessions) => {
            if sessions.is_empty() {
                vec![]
            } else {
                vec![TestimonySource {
                    space: testimony_space,
                    branch: None,
                    sessions: Some(sessions.clone()),
                }]
            }
        }
        AssertionScope::Union(parts) => parts
            .iter()
            .flat_map(|p| flatten_assertion_scope(p, testimony_space))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::branch::BranchId;

    #[test]
    fn flatten_union_scope() {
        let scope = AssertionScope::Union(vec![
            AssertionScope::Spaces(vec![SpaceId(1)]),
            AssertionScope::Branches(vec![BranchId(2), BranchId(3)]),
        ]);
        let flat = flatten_assertion_scope(&scope, SpaceId(1));
        assert_eq!(flat.len(), 3);
        assert!(flat.iter().any(|s| s.branch.is_none()));
        assert_eq!(flat.iter().filter(|s| s.branch.is_some()).count(), 2);
    }

    #[test]
    fn merge_session_union_combines_filters() {
        use crate::infinitedb_core::hlc::SessionId;
        let scope = AssertionScope::Union(vec![
            AssertionScope::Session(vec![SessionId(1), SessionId(2)]),
            AssertionScope::Session(vec![SessionId(2), SessionId(3)]),
        ]);
        let merged = merge_admission_specs(&scope, SpaceId(10));
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].sessions,
            Some(vec![SessionId(1), SessionId(2), SessionId(3)])
        );
    }

    #[test]
    fn merge_session_with_spaces_drops_filter() {
        use crate::infinitedb_core::hlc::SessionId;
        let scope = AssertionScope::Union(vec![
            AssertionScope::Spaces(vec![SpaceId(10)]),
            AssertionScope::Session(vec![SessionId(1)]),
        ]);
        let merged = merge_admission_specs(&scope, SpaceId(10));
        assert_eq!(merged.len(), 1);
        assert!(merged[0].sessions.is_none());
    }
}
