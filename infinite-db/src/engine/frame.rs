//! Frame-scoped supersession and judgment overlay resolution (M6).

use std::collections::{HashMap, HashSet};

use crate::infinitedb_core::{
    address::RevisionId,
    block::Record,
    frame::{record_admitted_by_source, JudgmentOverlayLayer, OverlayPolicy, TestimonySource, VerdictFilter},
    hlc::SessionId,
    hyperedge::{Hyperedge, HyperedgeId},
    judgment::{JudgmentRecord, JudgmentVerdict, SubjectIdentity, SubjectKind, SubjectPin},
    space::SpaceRegistry,
    staleness::{diagnose_assertion, ConsultedFrame, StalenessDiagnosis},
};

use super::query::{resolve_visibility_with_pin, FrameTimePin};

/// Record tagged with its admission source.
#[derive(Debug, Clone)]
pub struct SourcedRecord {
    pub source: TestimonySource,
    pub record: Record,
}

/// Judgment attached during overlay resolution.
#[derive(Debug, Clone)]
pub struct AttachedJudgment {
    pub layer: usize,
    pub record: JudgmentRecord,
}

/// Hyperedge resolved within a frame query.
#[derive(Debug, Clone)]
pub struct FrameResolvedHyperedge {
    pub edge: Hyperedge,
    pub source: TestimonySource,
    pub judgments: Vec<AttachedJudgment>,
    pub diagnosis: Option<StalenessDiagnosis>,
    pub suppressed: bool,
}

/// Apply latest-wins visibility for frame admission specs (Phase 5).
///
/// Filters candidates by session admission, merges cross-session buckets, and resolves
/// supersession by HLC authorship order under the frame time pin.
pub fn resolve_visibility_per_source<F>(
    spaces: &SpaceRegistry,
    by_source: &[(TestimonySource, Vec<Record>)],
    pin: &FrameTimePin,
    stable_for_session: F,
) -> Vec<SourcedRecord>
where
    F: Fn(SessionId) -> RevisionId,
{
    let mut out = Vec::new();
    for (source, candidates) in by_source {
        let filtered: Vec<Record> = candidates
            .iter()
            .filter(|r| record_admitted_by_source(r.revision.session(), source))
            .cloned()
            .collect();
        let visible = resolve_visibility_with_pin(
            spaces,
            filtered,
            pin,
            false,
            &stable_for_session,
        );
        for record in visible {
            out.push(SourcedRecord {
                source: TestimonySource {
                    space: source.space,
                    branch: source.branch,
                    sessions: Some(vec![SessionId(record.revision.session())]),
                },
                record,
            });
        }
    }
    out
}

fn subject_pin_for_edge(space: crate::infinitedb_core::address::SpaceId, edge: &Hyperedge) -> SubjectPin {
    SubjectPin {
        kind: SubjectKind::Hyperedge,
        space,
        identity: SubjectIdentity::Hyperedge(edge.id),
        subject_revision: edge.valid_from,
    }
}

fn judgment_matches_filter(j: &JudgmentRecord, filter: VerdictFilter) -> bool {
    match filter {
        VerdictFilter::Any => true,
        VerdictFilter::Pass => matches!(j.verdict, JudgmentVerdict::Pass),
        VerdictFilter::Fail => matches!(j.verdict, JudgmentVerdict::Fail),
        VerdictFilter::Conflict => matches!(j.verdict, JudgmentVerdict::Conflict),
        VerdictFilter::Annotate => matches!(j.verdict, JudgmentVerdict::Annotate),
    }
}

fn is_condemning(verdict: &JudgmentVerdict) -> bool {
    matches!(verdict, JudgmentVerdict::Fail | JudgmentVerdict::Conflict)
}

fn layer_judgments_for_pin(
    pin: &SubjectPin,
    layer: &JudgmentOverlayLayer,
    judgments_by_subject: &HashMap<SubjectPin, Vec<JudgmentRecord>>,
) -> Vec<JudgmentRecord> {
    judgments_by_subject
        .get(pin)
        .into_iter()
        .flatten()
        .filter(|j| j.arbiter == layer.arbiter)
        .filter(|j| judgment_matches_filter(j, layer.verdict_filter.clone()))
        .cloned()
        .collect()
}

/// Cross-source disagreement: same hyperedge id from multiple sources.
pub fn contested_ids(edges: &[FrameResolvedHyperedge]) -> HashSet<HyperedgeId> {
    let mut counts: HashMap<HyperedgeId, usize> = HashMap::new();
    for e in edges {
        *counts.entry(e.edge.id).or_insert(0) += 1;
    }
    counts
        .into_iter()
        .filter(|(_, c)| *c > 1)
        .map(|(id, _)| id)
        .collect()
}

/// Apply judgment overlay layers to resolved hyperedges.
pub fn apply_judgment_overlay(
    mut edges: Vec<FrameResolvedHyperedge>,
    layers: &[JudgmentOverlayLayer],
    judgments_by_subject: &HashMap<SubjectPin, Vec<JudgmentRecord>>,
    consulted: ConsultedFrame,
    include_suppressed: bool,
    include_diagnosis: bool,
) -> Vec<FrameResolvedHyperedge> {
    let cross_contested = contested_ids(&edges);
    let select_contested_mode = layers
        .iter()
        .any(|l| l.policy == OverlayPolicy::SelectContested);

    for edge in edges.iter_mut() {
        let pin = subject_pin_for_edge(edge.source.space, &edge.edge);
        for (layer_idx, layer) in layers.iter().enumerate() {
            for j in layer_judgments_for_pin(&pin, layer, judgments_by_subject) {
                edge.judgments.push(AttachedJudgment {
                    layer: layer_idx,
                    record: j,
                });
            }
        }
        edge.suppressed = layers.iter().enumerate().any(|(layer_idx, layer)| {
            layer.policy == OverlayPolicy::Suppress
                && edge
                    .judgments
                    .iter()
                    .filter(|a| a.layer == layer_idx)
                    .any(|a| is_condemning(&a.record.verdict))
        });
        if include_diagnosis {
            let subject_judgments: Vec<JudgmentRecord> = edge
                .judgments
                .iter()
                .map(|a| a.record.clone())
                .collect();
            edge.diagnosis = Some(diagnose_assertion(
                &edge.edge,
                consulted,
                &subject_judgments,
            ));
        }
    }

    edges.retain(|e| include_suppressed || !e.suppressed);

    if select_contested_mode {
        edges.retain(|e| {
            cross_contested.contains(&e.edge.id)
                || layers.iter().enumerate().any(|(layer_idx, layer)| {
                    layer.policy == OverlayPolicy::SelectContested
                        && e.judgments.iter().any(|a| {
                            a.layer == layer_idx
                                && matches!(a.record.verdict, JudgmentVerdict::Conflict)
                        })
                })
        });
    }

    edges
}

/// Traversal result with per-edge frame resolution metadata (M6).
#[derive(Debug, Clone, Default)]
pub struct FrameTraversalResult {
    pub traversal: crate::infinitedb_core::traversal::TraversalResult,
    pub resolved: Vec<FrameResolvedHyperedge>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::{
        address::{DimensionVector, RevisionId, SpaceId},
        frame::{JudgmentOverlayLayer, OverlayPolicy, VerdictFilter},
        hyperedge::{
            Directionality, EndpointPolarity, EndpointRef, EndpointRole, HyperedgeKind,
        },
        judgment::{ArbiterId, JudgmentId},
        provenance::FrameId,
    };
    use std::collections::BTreeMap;

    fn sample_edge(id: u64) -> Hyperedge {
        Hyperedge {
            id: HyperedgeId(id),
            kind: HyperedgeKind::new("t"),
            endpoints: vec![
                EndpointRef::new(
                    EndpointRole::new("n"),
                    SpaceId(1),
                    DimensionVector::new(vec![1, 0]),
                )
                .with_polarity(EndpointPolarity::Tail),
                EndpointRef::new(
                    EndpointRole::new("n"),
                    SpaceId(1),
                    DimensionVector::new(vec![2, 0]),
                )
                .with_polarity(EndpointPolarity::Head),
            ],
            weight_milli: None,
            metadata: BTreeMap::new(),
            valid_from: RevisionId::legacy(1),
            valid_to: None,
            directionality: Directionality::Directed,
            authoring_frame: None,
            computation: None,
        }
    }

    #[test]
    fn suppress_removes_condemned() {
        let edge = sample_edge(1);
        let pin = subject_pin_for_edge(SpaceId(10), &edge);
        let mut edges = vec![FrameResolvedHyperedge {
            edge,
            source: TestimonySource {
                space: SpaceId(10),
                branch: None,
                sessions: None,
            },
            judgments: vec![],
            diagnosis: None,
            suppressed: false,
        }];
        let mut by_subject = HashMap::new();
        by_subject.insert(
            pin.clone(),
            vec![JudgmentRecord {
                id: JudgmentId(1),
                arbiter: ArbiterId(1),
                subject: pin,
                verdict: JudgmentVerdict::Fail,
                rationale: None,
                authoring_frame: None,
            }],
        );
        let layers = vec![JudgmentOverlayLayer {
            arbiter: ArbiterId(1),
            policy: OverlayPolicy::Suppress,
            verdict_filter: VerdictFilter::Any,
        }];
        let out = apply_judgment_overlay(
            std::mem::take(&mut edges),
            &layers,
            &by_subject,
            ConsultedFrame {
                frame_id: FrameId(1),
                as_of: RevisionId::legacy(1),
            },
            false,
            false,
        );
        assert!(out.is_empty());
    }
}
