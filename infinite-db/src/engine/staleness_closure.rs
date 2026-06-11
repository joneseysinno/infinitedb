//! Forward stale closure over hypergraph traversal (M7).

use std::collections::HashSet;
use std::io;

use crate::engine::traversal::run_traversal;
use crate::infinitedb_core::{
    address::RevisionId,
    block::Record,
    hyperedge::{EndpointRef, EndpointRole, Hyperedge, HyperedgeId},
    judgment::{SubjectIdentity, SubjectKind, SubjectPin},
    space::EndpointIndexLayout,
    staleness_closure::{check_computation_freshness, FreshnessStatus, StaleTarget},
    traversal::{TraversalDirection, TraversalSpec},
};

/// Collect downstream edges whose computation inputs are stale after a source change.
pub fn forward_stale_closure(
    changed: &SubjectPin,
    edge_space: crate::infinitedb_core::address::SpaceId,
    max_depth: usize,
    as_of: Option<RevisionId>,
    index_records: &[Record],
    registry_layout: EndpointIndexLayout,
    rev_ceiling: RevisionId,
    fetch_edge: &impl Fn(HyperedgeId) -> Option<Hyperedge>,
    fetch_subject_revision: &impl Fn(&SubjectPin) -> Option<RevisionId>,
    seed_endpoints: &[EndpointRef],
) -> io::Result<Vec<StaleTarget>> {
    let mut targets = Vec::new();
    let mut seen_edges = HashSet::new();

    for start in seed_endpoints {
        let spec = TraversalSpec {
            start: start.clone(),
            edge_space,
            max_depth,
            direction: TraversalDirection::Forward,
            mode: Default::default(),
            follow_kinds: None,
            as_of,
        };
        let traversal = run_traversal(
            &spec,
            index_records,
            registry_layout,
            rev_ceiling,
            |id| fetch_edge(id),
        )?;
        for edge in &traversal.edges {
            if !seen_edges.insert(edge.id) {
                continue;
            }
            let Some(report) =
                check_computation_freshness(edge, fetch_subject_revision)
            else {
                continue;
            };
            if !report.is_fresh {
                let depth = edge
                    .head_endpoints()
                    .filter_map(|ep| traversal.level_of(ep))
                    .max()
                    .unwrap_or(0);
                let stale_inputs: Vec<_> = report
                    .inputs
                    .into_iter()
                    .filter(|i| i.status == FreshnessStatus::Stale)
                    .collect();
                targets.push(StaleTarget {
                    edge: edge.clone(),
                    stale_inputs,
                    depth,
                });
            }
        }
    }

    let _ = changed;
    Ok(targets)
}

/// Endpoints to seed forward traversal from a changed subject pin.
pub fn staleness_seed_endpoints(
    pin: &SubjectPin,
    fetch_edge: &impl Fn(HyperedgeId) -> Option<Hyperedge>,
) -> Vec<EndpointRef> {
    match (&pin.kind, &pin.identity) {
        (SubjectKind::Node, SubjectIdentity::Address(addr)) => {
            vec![EndpointRef::new(
                EndpointRole::new("subject"),
                addr.space,
                addr.point.clone(),
            )]
        }
        (SubjectKind::Hyperedge, SubjectIdentity::Hyperedge(id)) => {
            fetch_edge(*id)
                .map(|edge| {
                    edge.head_endpoints()
                        .map(|ep| ep.clone())
                        .collect()
                })
                .unwrap_or_default()
        }
        _ => Vec::new(),
    }
}
