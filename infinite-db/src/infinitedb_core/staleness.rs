//! Pure staleness diagnosis from authoring provenance and judgments (M5).

use super::{
    hyperedge::Hyperedge,
    judgment::{JudgmentRecord, JudgmentVerdict},
    provenance::{AuthoringFrameProvenance, FrameId},
};

/// Query-time view of a named durable frame (M6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsultedFrame {
    pub frame_id: FrameId,
    pub as_of: super::address::RevisionId,
}

/// Build a consulted frame from a durable definition and optional override.
pub fn consulted_from_frame(
    frame_id: FrameId,
    default_as_of: Option<super::address::RevisionId>,
    as_of_override: Option<super::address::RevisionId>,
    allocated: super::address::RevisionId,
) -> ConsultedFrame {
    ConsultedFrame {
        frame_id,
        as_of: as_of_override.or(default_as_of).unwrap_or(allocated),
    }
}

/// Diagnosis of an assertion relative to consulted frame and judgments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StalenessDiagnosis {
    Consistent,
    StaleFrame,
    WrongFrame,
    GenuineMistake,
    Condemned,
}

/// Diagnose an assertion using authoring provenance and subject-scoped judgments.
pub fn diagnose_assertion(
    edge: &Hyperedge,
    consulted: ConsultedFrame,
    judgments: &[JudgmentRecord],
) -> StalenessDiagnosis {
    for j in judgments {
        if matches!(
            j.verdict,
            JudgmentVerdict::Fail | JudgmentVerdict::Conflict
        ) {
            return StalenessDiagnosis::Condemned;
        }
    }

    let Some(prov) = edge.authoring_frame.as_ref() else {
        return StalenessDiagnosis::Consistent;
    };

    if prov.frame_id != consulted.frame_id {
        return StalenessDiagnosis::WrongFrame;
    }

    if prov.as_of < consulted.as_of {
        return StalenessDiagnosis::StaleFrame;
    }

    if prov.as_of > consulted.as_of {
        return StalenessDiagnosis::GenuineMistake;
    }

    StalenessDiagnosis::Consistent
}

/// Validate provenance against commit ceiling before write.
pub fn validate_authoring_provenance(
    prov: &AuthoringFrameProvenance,
    commit_ceiling: super::address::RevisionId,
) -> Result<(), super::provenance::ProvenanceError> {
    if prov.as_of > commit_ceiling {
        return Err(super::provenance::ProvenanceError::AsOfExceedsCommit {
            as_of: prov.as_of,
            commit_ceiling,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::{
        hyperedge::{
            Directionality, EndpointPolarity, EndpointRef, EndpointRole, Hyperedge, HyperedgeId,
            HyperedgeKind,
        },
        address::{DimensionVector, RevisionId, SpaceId},
        judgment::{ArbiterId, JudgmentId, JudgmentRecord, JudgmentVerdict, SubjectIdentity, SubjectKind, SubjectPin},
        provenance::{AuthoringFrameProvenance, FrameId},
    };
    use std::collections::BTreeMap;

    fn edge_with_frame(frame: FrameId, as_of: RevisionId) -> Hyperedge {
        Hyperedge {
            id: HyperedgeId(1),
            kind: HyperedgeKind::new("e"),
            endpoints: vec![
                EndpointRef::new(EndpointRole::new("a"), SpaceId(1), DimensionVector::new(vec![0, 0]))
                    .with_polarity(EndpointPolarity::Tail),
                EndpointRef::new(EndpointRole::new("b"), SpaceId(1), DimensionVector::new(vec![1, 0]))
                    .with_polarity(EndpointPolarity::Head),
            ],
            weight_milli: None,
            metadata: BTreeMap::new(),
            valid_from: RevisionId::legacy(1),
            valid_to: None,
            directionality: Directionality::Directed,
            authoring_frame: Some(AuthoringFrameProvenance { frame_id: frame, as_of }),
            computation: None,
        }
    }

    #[test]
    fn diagnose_stale_frame() {
        let edge = edge_with_frame(FrameId(1), RevisionId::legacy(5));
        let consulted = ConsultedFrame {
            frame_id: FrameId(1),
            as_of: RevisionId::legacy(10),
        };
        assert_eq!(
            diagnose_assertion(&edge, consulted, &[]),
            StalenessDiagnosis::StaleFrame
        );
    }

    #[test]
    fn diagnose_condemned_by_judgment() {
        let edge = edge_with_frame(FrameId(1), RevisionId::legacy(10));
        let consulted = ConsultedFrame {
            frame_id: FrameId(1),
            as_of: RevisionId::legacy(10),
        };
        let j = JudgmentRecord {
            id: JudgmentId(1),
            arbiter: ArbiterId(1),
            subject: SubjectPin {
                kind: SubjectKind::Hyperedge,
                space: SpaceId(1),
                identity: SubjectIdentity::Hyperedge(HyperedgeId(1)),
                subject_revision: RevisionId::legacy(1),
            },
            verdict: JudgmentVerdict::Fail,
            rationale: None,
            authoring_frame: None,
        };
        assert_eq!(
            diagnose_assertion(&edge, consulted, &[j]),
            StalenessDiagnosis::Condemned
        );
    }
}
