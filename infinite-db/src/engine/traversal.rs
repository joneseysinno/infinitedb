//! Hypergraph traversal engine (M3) — reachability BFS and B-connectivity.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;

use crate::infinitedb_core::{
    address::{RevisionId, SpaceId},
    block::Record,
    hyperedge::{EndpointRef, Hyperedge, HyperedgeId},
    query::DirectionFilter,
    space::EndpointIndexLayout,
    traversal::{
        edge_passes_filters, endpoint_key, expand_edge_reachability, record_node,
        run_b_connectivity, TraversalArrival, TraversalDirection, TraversalMode, TraversalResult,
        TraversalSpec,
    },
};

use super::hypergraph::{
    filter_edges_by_direction, incident_edge_ids_directed, partition_incident_ids_by_layout,
};

/// Run traversal using preloaded index records and an edge resolver.
pub fn run_traversal(
    spec: &TraversalSpec,
    index_records: &[Record],
    registry_layout: EndpointIndexLayout,
    rev_ceiling: RevisionId,
    fetch_edge: impl Fn(HyperedgeId) -> Option<Hyperedge>,
) -> io::Result<TraversalResult> {
    match spec.mode {
        TraversalMode::Reachability => run_reachability_traversal(
            spec,
            index_records,
            registry_layout,
            rev_ceiling,
            &fetch_edge,
        ),
        TraversalMode::BConnectivity => run_b_connectivity_traversal(
            spec,
            index_records,
            registry_layout,
            rev_ceiling,
            &fetch_edge,
        ),
    }
}

fn run_reachability_traversal(
    spec: &TraversalSpec,
    index_records: &[Record],
    registry_layout: EndpointIndexLayout,
    rev_ceiling: RevisionId,
    fetch_edge: &impl Fn(HyperedgeId) -> Option<Hyperedge>,
) -> io::Result<TraversalResult> {
    let mut result = TraversalResult::default();
    let mut levels: HashMap<crate::infinitedb_core::traversal::EndpointKey, usize> =
        HashMap::new();
    let mut enqueued: HashSet<crate::infinitedb_core::traversal::EndpointKey> = HashSet::new();
    let mut frontier: VecDeque<(EndpointRef, usize)> = VecDeque::new();

    record_node(
        &mut result.nodes,
        &mut levels,
        spec.start.clone(),
        0,
        TraversalArrival::Start,
    );
    enqueued.insert(endpoint_key(&spec.start));
    if spec.max_depth > 0 {
        frontier.push_back((spec.start.clone(), 0));
    }

    while let Some((current, level)) = frontier.pop_front() {
        if level >= spec.max_depth {
            continue;
        }
        let incident =
            incident_edges_for_node(
                &current,
                spec.direction,
                index_records,
                registry_layout,
                rev_ceiling,
                spec.edge_space,
                &spec.follow_kinds,
                fetch_edge,
            )?;
        for edge in incident {
            if !result.edges.iter().any(|e| e.id == edge.id) {
                result.edges.push(edge.clone());
            }
            expand_edge_reachability(
                &edge,
                &current,
                level,
                spec.direction,
                spec.max_depth,
                &mut result.nodes,
                &mut levels,
                &mut frontier,
                &mut enqueued,
            );
        }
    }

    Ok(result)
}

fn run_b_connectivity_traversal(
    spec: &TraversalSpec,
    index_records: &[Record],
    registry_layout: EndpointIndexLayout,
    rev_ceiling: RevisionId,
    fetch_edge: &impl Fn(HyperedgeId) -> Option<Hyperedge>,
) -> io::Result<TraversalResult> {
    // Collect all directed edges reachable from start via index closure (bounded by max_depth
    // in the fixpoint). For B-connectivity we need the candidate edge set — gather edges
    // incident to any node that could participate using a generous reachability pre-pass
    // without depth limit on edge collection, then run pure fixpoint with max_depth.
    let mut edge_map: HashMap<HyperedgeId, Hyperedge> = HashMap::new();
    let mut seen_nodes: HashSet<crate::infinitedb_core::traversal::EndpointKey> =
        HashSet::new();
    let mut queue: VecDeque<EndpointRef> = VecDeque::new();

    seen_nodes.insert(endpoint_key(&spec.start));
    queue.push_back(spec.start.clone());

    while let Some(node) = queue.pop_front() {
        let incident = incident_edges_for_node(
            &node,
            TraversalDirection::Both,
            index_records,
            registry_layout,
            rev_ceiling,
            spec.edge_space,
            &spec.follow_kinds,
            fetch_edge,
        )?;
        for edge in incident {
            if !edge.is_directed() {
                continue;
            }
            edge_map.entry(edge.id).or_insert(edge.clone());
            for ep in edge.endpoints.iter() {
                let key = endpoint_key(ep);
                if seen_nodes.insert(key) {
                    queue.push_back(ep.clone());
                }
            }
        }
    }

    let edges: Vec<Hyperedge> = edge_map.into_values().collect();
    Ok(run_b_connectivity(
        &spec.start,
        &edges,
        spec.max_depth,
        &spec.follow_kinds,
        rev_ceiling,
    ))
}

fn incident_edges_for_node(
    endpoint: &EndpointRef,
    direction: TraversalDirection,
    index_records: &[Record],
    registry_layout: EndpointIndexLayout,
    rev_ceiling: RevisionId,
    edge_space: SpaceId,
    follow_kinds: &Option<Vec<crate::infinitedb_core::hyperedge::HyperedgeKind>>,
    fetch_edge: &impl Fn(HyperedgeId) -> Option<Hyperedge>,
) -> io::Result<Vec<Hyperedge>> {
    let filter = direction.incidence_filter();
    let ids = incident_edge_ids_directed(index_records, endpoint, filter, registry_layout);

    let v1_set: HashSet<HyperedgeId> = if registry_layout == EndpointIndexLayout::V2PolarityDim
        && filter != DirectionFilter::Any
    {
        let (_, v1_ids) = partition_incident_ids_by_layout(index_records, endpoint, &ids);
        v1_ids.into_iter().collect()
    } else {
        HashSet::new()
    };

    let mut edges = Vec::new();
    for id in ids {
        let Some(edge) = fetch_edge(id) else {
            continue;
        };
        if !edge_passes_filters(&edge, follow_kinds, rev_ceiling) {
            continue;
        }
        if !edge
            .endpoints
            .iter()
            .any(|ep| endpoint_key(ep) == endpoint_key(endpoint))
        {
            continue;
        }
        if registry_layout == EndpointIndexLayout::V2PolarityDim && filter != DirectionFilter::Any {
            if v1_set.contains(&edge.id)
                && filter_edges_by_direction(vec![edge.clone()], endpoint, filter).is_empty()
            {
                continue;
            }
        } else if filter != DirectionFilter::Any
            && filter_edges_by_direction(vec![edge.clone()], endpoint, filter).is_empty()
        {
            continue;
        }
        edges.push(edge);
    }
    let _ = edge_space;
    Ok(edges)
}
