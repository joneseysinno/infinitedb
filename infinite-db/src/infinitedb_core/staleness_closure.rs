//! Structural staleness via computation input pins (M7).
//!
//! [`FreshnessReport`] and [`StaleTarget`] travel in the Ok channel per error-algebra rule 3.

use serde::{Deserialize, Serialize};

use super::{
    address::RevisionId,
    hyperedge::{Hyperedge, HyperedgeId},
    judgment::SubjectPin,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FreshnessStatus {
    Fresh,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputFreshness {
    pub pin: SubjectPin,
    pub observed_revision: RevisionId,
    pub status: FreshnessStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreshnessReport {
    pub edge_id: HyperedgeId,
    pub inputs: Vec<InputFreshness>,
    pub is_fresh: bool,
}

#[derive(Debug, Clone)]
pub struct StaleTarget {
    pub edge: Hyperedge,
    pub stale_inputs: Vec<InputFreshness>,
    pub depth: usize,
}

/// Whether an observed subject revision satisfies a computation input pin.
///
/// Exact equality is required. When the pin was serialized through legacy `u64`
/// revision encoding but the live record carries a full HLC stamp (Phase 5),
/// matching sequence under a legacy-embedded pin still counts as fresh.
fn revisions_match_for_freshness(pinned: RevisionId, observed: RevisionId) -> bool {
    if pinned == observed {
        return true;
    }
    pinned.is_global_legacy()
        && !observed.is_global_legacy()
        && pinned.legacy_sequence() == observed.legacy_sequence()
}

/// Compare pinned input revision against the observed subject revision at `as_of`.
pub fn input_freshness(
    pin: &SubjectPin,
    observed_revision: RevisionId,
) -> InputFreshness {
    let status = if revisions_match_for_freshness(pin.subject_revision, observed_revision) {
        FreshnessStatus::Fresh
    } else {
        FreshnessStatus::Stale
    };
    InputFreshness {
        pin: pin.clone(),
        observed_revision,
        status,
    }
}

/// Backward freshness for a hyperedge with computation provenance.
pub fn check_computation_freshness(
    edge: &Hyperedge,
    fetch_subject_revision: &impl Fn(&SubjectPin) -> Option<RevisionId>,
) -> Option<FreshnessReport> {
    let computation = edge.computation.as_ref()?;
    let mut inputs = Vec::with_capacity(computation.inputs.len());
    let mut is_fresh = true;
    for pin in &computation.inputs {
        let observed = fetch_subject_revision(pin).unwrap_or(RevisionId::ZERO);
        let entry = input_freshness(pin, observed);
        if entry.status == FreshnessStatus::Stale {
            is_fresh = false;
        }
        inputs.push(entry);
    }
    Some(FreshnessReport {
        edge_id: edge.id,
        inputs,
        is_fresh,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::{
        address::{DimensionVector, SpaceId},
        computation::ComputationProvenance,
        hyperedge::{
            Directionality, EndpointPolarity, EndpointRef, EndpointRole, HyperedgeKind,
        },
        judgment::{SubjectIdentity, SubjectKind},
    };
    use std::collections::BTreeMap;

    fn sample_edge(pin_rev: RevisionId, observed: RevisionId) -> (Hyperedge, RevisionId) {
        let pin = SubjectPin {
            kind: SubjectKind::Node,
            space: SpaceId(1),
            identity: SubjectIdentity::Address(super::super::address::Address::new(
                SpaceId(1),
                DimensionVector::new(vec![1, 0]),
            )),
            subject_revision: pin_rev,
        };
        let edge = Hyperedge {
            id: HyperedgeId(9),
            kind: HyperedgeKind::new("derived"),
            endpoints: vec![
                EndpointRef::new(
                    EndpointRole::new("a"),
                    SpaceId(1),
                    DimensionVector::new(vec![0, 0]),
                )
                .with_polarity(EndpointPolarity::Tail),
                EndpointRef::new(
                    EndpointRole::new("b"),
                    SpaceId(1),
                    DimensionVector::new(vec![1, 0]),
                )
                .with_polarity(EndpointPolarity::Head),
            ],
            weight_milli: None,
            metadata: BTreeMap::new(),
            valid_from: RevisionId::legacy(5),
            valid_to: None,
            directionality: Directionality::Directed,
            authoring_frame: None,
            computation: Some(ComputationProvenance {
                inputs: vec![pin],
            }),
        };
        (edge, observed)
    }

    #[test]
    fn fresh_when_revision_matches() {
        let (edge, obs) = sample_edge(RevisionId::legacy(3), RevisionId::legacy(3));
        let report = check_computation_freshness(&edge, &|_| Some(obs)).unwrap();
        assert!(report.is_fresh);
    }

    #[test]
    fn stale_when_revision_superseded() {
        let (edge, _) = sample_edge(RevisionId::legacy(3), RevisionId::legacy(7));
        let report =
            check_computation_freshness(&edge, &|_| Some(RevisionId::legacy(7))).unwrap();
        assert!(!report.is_fresh);
    }

    #[test]
    fn legacy_pin_matches_hlc_observed_sequence() {
        use crate::infinitedb_core::hlc::HlcStamp;
        let (edge, _) = sample_edge(RevisionId::legacy(3), RevisionId::legacy(3));
        let hlc_observed = RevisionId::from_stamp(HlcStamp {
            physical_ms: 0,
            logical: 0,
            session: 2,
            sequence: 3,
        });
        let mut edge = edge;
        edge.computation.as_mut().unwrap().inputs[0].subject_revision = RevisionId::legacy(3);
        let report =
            check_computation_freshness(&edge, &|_| Some(hlc_observed)).unwrap();
        assert!(report.is_fresh);
    }
}
