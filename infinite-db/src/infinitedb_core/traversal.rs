//! Cross-space hypergraph traversal (M3).
//!
//! Walks hyperedges from a starting endpoint using the reverse endpoint index,
//! with direction policy, arrival-aware expansion, and wave-front level assignments.
//!
//! # Direction modes
//!
//! - [`TraversalDirection::Forward`]: follow tail → head; expand into heads only.
//! - [`TraversalDirection::Backward`]: follow head → tail; expand into tails only.
//! - [`TraversalDirection::Both`]: symmetric BFS (pre-M3 behavior).
//!
//! Undirected edges (all-neutral polarity) participate only in [`TraversalDirection::Both`];
//! forward/backward modes skip them because polarity filters do not match.
//!
//! # Traversal modes
//!
//! [`TraversalMode::Reachability`] (default) is plain BFS. [`TraversalMode::BConnectivity`]
//! implements conjunctive hypergraph semantics: a head activates only when all tails of
//! the connecting edge are reached. B-connectivity has a higher memory and fixpoint cost
//! profile — it is never the default.

use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};

use super::{
    address::{RevisionId, SpaceId},
    frame_query::FrameQueryOptions,
    hyperedge::{
        EndpointRef, Hyperedge, HyperedgeId, HyperedgeKind,
    },
    provenance::FrameId,
    query::DirectionFilter,
};

/// Stable endpoint identity for visited-set and level maps (space + coordinates).
pub type EndpointKey = (SpaceId, Vec<u32>);

/// Direction policy for traversal expansion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TraversalDirection {
    /// Follow tail → head; expand into head endpoints.
    Forward,
    /// Follow head → tail; expand into tail endpoints.
    Backward,
    /// Symmetric expansion (legacy undirected walk).
    #[default]
    Both,
}

impl TraversalDirection {
    /// Incidence filter used at the current node for this direction policy.
    pub fn incidence_filter(self) -> DirectionFilter {
        match self {
            TraversalDirection::Forward => DirectionFilter::Outgoing,
            TraversalDirection::Backward => DirectionFilter::Incoming,
            TraversalDirection::Both => DirectionFilter::Any,
        }
    }
}

/// Traversal semantics — reachability vs conjunctive B-connectivity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TraversalMode {
    /// Plain BFS reachability (default).
    #[default]
    Reachability,
    /// Conjunctive hypergraph activation: heads require all tails reached.
    BConnectivity,
}

/// How a node entered the traversal result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraversalArrival {
    /// The starting node.
    Start,
    /// Enqueued and expanded from (expansion-side endpoint).
    Expanded,
    /// Same-side co-cause reported but not expanded from.
    CoCause,
}

/// Parameters controlling a graph walk from a starting node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraversalSpec {
    /// Node to begin the walk from.
    pub start: EndpointRef,
    /// Space where hyperedge assertions are stored.
    pub edge_space: SpaceId,
    /// Maximum BFS depth (0 = start node only).
    pub max_depth: usize,
    /// Direction policy for expansion.
    #[serde(default)]
    pub direction: TraversalDirection,
    /// Reachability vs B-connectivity semantics.
    #[serde(default)]
    pub mode: TraversalMode,
    /// If set, only follow edges whose kind is in this list.
    pub follow_kinds: Option<Vec<HyperedgeKind>>,
    /// Only consider edges active at or before this revision.
    pub as_of: Option<RevisionId>,
}

/// Frame-aware traversal spec (M6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameTraversalSpec {
    pub frame_id: FrameId,
    pub base: TraversalSpec,
    pub as_of: Option<RevisionId>,
    pub options: FrameQueryOptions,
}

impl TraversalSpec {
    /// Build a minimal spec with symmetric reachability defaults.
    pub fn new(start: EndpointRef, edge_space: SpaceId, max_depth: usize) -> Self {
        Self {
            start,
            edge_space,
            max_depth,
            direction: TraversalDirection::Both,
            mode: TraversalMode::Reachability,
            follow_kinds: None,
            as_of: None,
        }
    }
}

/// One endpoint in a traversal result with wave-front level and arrival metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraversalNode {
    pub endpoint: EndpointRef,
    /// Wave-front stratum (0 = start).
    pub level: usize,
    pub arrival: TraversalArrival,
}

/// Nodes and edges collected during a directional traversal.
#[derive(Debug, Clone, Default)]
pub struct TraversalResult {
    pub nodes: Vec<TraversalNode>,
    pub edges: Vec<Hyperedge>,
}

impl TraversalResult {
    /// Level assigned to an endpoint, if present.
    pub fn level_of(&self, endpoint: &EndpointRef) -> Option<usize> {
        let key = endpoint_key(endpoint);
        self.nodes
            .iter()
            .find(|n| endpoint_key(&n.endpoint) == key)
            .map(|n| n.level)
    }

    /// Collapse to legacy [`Subgraph`] (nodes without level/arrival metadata).
    pub fn subgraph(&self) -> Subgraph {
        let mut sg = Subgraph::default();
        for n in &self.nodes {
            sg.add_node(n.endpoint.clone());
        }
        for e in &self.edges {
            sg.add_edge(e.clone());
        }
        sg
    }
}

/// The nodes and edges collected during a traversal (legacy view).
#[derive(Debug, Clone, Default)]
pub struct Subgraph {
    /// Distinct endpoints visited.
    pub nodes: Vec<EndpointRef>,
    /// Hyperedges traversed (deduplicated by id).
    pub edges: Vec<Hyperedge>,
}

impl Subgraph {
    /// Record an endpoint if not already present (matched by space + coords).
    pub fn add_node(&mut self, ep: EndpointRef) {
        if !self.nodes.iter().any(|n| endpoints_equal(n, &ep)) {
            self.nodes.push(ep);
        }
    }

    /// Record a hyperedge if not already present.
    pub fn add_edge(&mut self, edge: Hyperedge) {
        if !self.edges.iter().any(|e| e.id == edge.id) {
            self.edges.push(edge);
        }
    }
}

/// Endpoint identity key (space + coordinates).
pub fn endpoint_key(ep: &EndpointRef) -> EndpointKey {
    (ep.space, ep.node.coords.clone())
}

/// Whether two endpoints refer to the same node (ignores polarity and role).
pub fn endpoints_equal(a: &EndpointRef, b: &EndpointRef) -> bool {
    endpoint_key(a) == endpoint_key(b)
}

/// Returns true when `edge` passes kind and revision filters.
pub fn edge_passes_filters(
    edge: &Hyperedge,
    follow_kinds: &Option<Vec<HyperedgeKind>>,
    rev_ceiling: RevisionId,
) -> bool {
    if !edge.is_active_at(rev_ceiling) {
        return false;
    }
    if let Some(kinds) = follow_kinds {
        return kinds.iter().any(|k| k == &edge.kind);
    }
    true
}

/// Record or upgrade a node in the result (keeps lowest level).
pub fn record_node(
    nodes: &mut Vec<TraversalNode>,
    levels: &mut HashMap<EndpointKey, usize>,
    endpoint: EndpointRef,
    level: usize,
    arrival: TraversalArrival,
) {
    let key = endpoint_key(&endpoint);
    if let Some(&existing) = levels.get(&key) {
        if level >= existing {
            return;
        }
        levels.insert(key.clone(), level);
        if let Some(slot) = nodes.iter_mut().find(|n| endpoint_key(&n.endpoint) == key) {
            slot.level = level;
            slot.arrival = arrival;
            slot.endpoint = endpoint;
        }
        return;
    }
    levels.insert(key, level);
    nodes.push(TraversalNode {
        endpoint,
        level,
        arrival,
    });
}

/// Other endpoints on the edge (excluding `current`).
pub fn other_endpoints<'a>(
    edge: &'a Hyperedge,
    current: &EndpointRef,
) -> impl Iterator<Item = &'a EndpointRef> {
    edge.endpoints
        .iter()
        .filter(|ep| !endpoints_equal(ep, current))
}

/// Apply arrival-aware expansion for one edge at level `L`; returns endpoints to enqueue.
pub fn expand_edge_reachability(
    edge: &Hyperedge,
    current: &EndpointRef,
    level: usize,
    direction: TraversalDirection,
    max_depth: usize,
    nodes: &mut Vec<TraversalNode>,
    levels: &mut HashMap<EndpointKey, usize>,
    enqueue: &mut VecDeque<(EndpointRef, usize)>,
    enqueued: &mut HashSet<EndpointKey>,
) {
    if direction == TraversalDirection::Both {
        if level >= max_depth {
            return;
        }
        for ep in other_endpoints(edge, current) {
            let next_level = level + 1;
            record_node(
                nodes,
                levels,
                ep.clone(),
                next_level,
                TraversalArrival::Expanded,
            );
            let key = endpoint_key(ep);
            if enqueued.insert(key) {
                enqueue.push_back((ep.clone(), next_level));
            }
        }
        return;
    }

    // Report co-causes on the arrival side (same level, not enqueued).
    let arrival_side: Vec<&EndpointRef> = match direction {
        TraversalDirection::Forward => edge.tail_endpoints().collect(),
        TraversalDirection::Backward => edge.head_endpoints().collect(),
        TraversalDirection::Both => Vec::new(),
    };
    for ep in arrival_side {
        if !endpoints_equal(ep, current) {
            record_node(
                nodes,
                levels,
                ep.clone(),
                level,
                TraversalArrival::CoCause,
            );
        }
    }

    if level >= max_depth {
        return;
    }

    let next_level = level + 1;
    let expansion_side: Vec<&EndpointRef> = match direction {
        TraversalDirection::Forward => edge.head_endpoints().collect(),
        TraversalDirection::Backward => edge.tail_endpoints().collect(),
        TraversalDirection::Both => Vec::new(),
    };
    for ep in expansion_side {
        record_node(
            nodes,
            levels,
            ep.clone(),
            next_level,
            TraversalArrival::Expanded,
        );
        let key = endpoint_key(ep);
        if enqueued.insert(key) {
            enqueue.push_back((ep.clone(), next_level));
        }
    }
}

/// B-connectivity fixpoint over a static edge set (pure, for tests and engine).
pub fn run_b_connectivity(
    start: &EndpointRef,
    edges: &[Hyperedge],
    max_depth: usize,
    follow_kinds: &Option<Vec<HyperedgeKind>>,
    rev_ceiling: RevisionId,
) -> TraversalResult {
    let mut result = TraversalResult::default();
    let mut levels: HashMap<EndpointKey, usize> = HashMap::new();
    let mut activated_edges: HashSet<HyperedgeId> = HashSet::new();

    record_node(
        &mut result.nodes,
        &mut levels,
        start.clone(),
        0,
        TraversalArrival::Start,
    );

    let filtered: Vec<&Hyperedge> = edges
        .iter()
        .filter(|e| {
            e.is_directed()
                && edge_passes_filters(e, follow_kinds, rev_ceiling)
        })
        .collect();

    loop {
        let mut changed = false;
        for edge in &filtered {
            if activated_edges.contains(&edge.id) {
                continue;
            }
            let tails: Vec<&EndpointRef> = edge.tail_endpoints().collect();
            if tails.is_empty() {
                continue;
            }
            let all_tails_reached = tails.iter().all(|t| levels.contains_key(&endpoint_key(t)));
            if !all_tails_reached {
                continue;
            }
            let tail_level = tails
                .iter()
                .filter_map(|t| levels.get(&endpoint_key(t)).copied())
                .max()
                .unwrap_or(0);
            let head_level = tail_level + 1;
            if head_level > max_depth {
                activated_edges.insert(edge.id);
                result.edges.push((*edge).clone());
                continue;
            }
            activated_edges.insert(edge.id);
            if !result.edges.iter().any(|e| e.id == edge.id) {
                result.edges.push((*edge).clone());
            }
            for ep in edge.head_endpoints() {
                let key = endpoint_key(ep);
                if !levels.contains_key(&key) {
                    record_node(
                        &mut result.nodes,
                        &mut levels,
                        ep.clone(),
                        head_level,
                        TraversalArrival::Expanded,
                    );
                    changed = true;
                }
            }
            for ep in edge.tail_endpoints() {
                record_node(
                    &mut result.nodes,
                    &mut levels,
                    ep.clone(),
                    tail_level,
                    TraversalArrival::CoCause,
                );
            }
        }
        if !changed {
            break;
        }
    }

    result
}

/// Per-kind acyclicity via tail→head reduction and Kahn topological sort.
///
/// Undirected edges are ignored. Empty `kinds` means all kinds.
pub fn hypergraph_acyclic_for_kinds(edges: &[Hyperedge], kinds: &[HyperedgeKind]) -> bool {
    let kind_ok = |k: &HyperedgeKind| kinds.is_empty() || kinds.iter().any(|x| x == k);

    let mut adj: HashMap<EndpointKey, HashSet<EndpointKey>> = HashMap::new();
    let mut in_degree: HashMap<EndpointKey, usize> = HashMap::new();
    let mut nodes: HashSet<EndpointKey> = HashSet::new();

    for edge in edges {
        if !edge.is_directed() || !kind_ok(&edge.kind) {
            continue;
        }
        let tails: Vec<EndpointKey> = edge.tail_endpoints().map(endpoint_key).collect();
        let heads: Vec<EndpointKey> = edge.head_endpoints().map(endpoint_key).collect();
        if tails.is_empty() || heads.is_empty() {
            continue;
        }
        for t in &tails {
            nodes.insert(t.clone());
            in_degree.entry(t.clone()).or_insert(0);
        }
        for h in &heads {
            nodes.insert(h.clone());
            in_degree.entry(h.clone()).or_insert(0);
        }
        for t in &tails {
            for h in &heads {
                if t != h && adj.entry(t.clone()).or_default().insert(h.clone()) {
                    *in_degree.entry(h.clone()).or_insert(0) += 1;
                }
            }
        }
    }

    let mut queue: VecDeque<EndpointKey> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(k, _)| k.clone())
        .collect();

    let mut visited = 0usize;
    while let Some(n) = queue.pop_front() {
        visited += 1;
        if let Some(neighbors) = adj.get(&n) {
            for m in neighbors {
                if let Some(deg) = in_degree.get_mut(m) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(m.clone());
                    }
                }
            }
        }
    }

    visited == nodes.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::hyperedge::{Directionality, EndpointPolarity};
    use std::collections::BTreeMap;

    fn node(space: SpaceId, x: u32) -> EndpointRef {
        use crate::infinitedb_core::hyperedge::EndpointRole;
        use crate::infinitedb_core::address::DimensionVector;
        EndpointRef::new(
            EndpointRole::new("n"),
            space,
            DimensionVector::new(vec![x, 0]),
        )
    }

    fn directed(id: u64, tails: Vec<EndpointRef>, heads: Vec<EndpointRef>) -> Hyperedge {
        let mut endpoints = Vec::new();
        for t in tails {
            endpoints.push(t.with_polarity(EndpointPolarity::Tail));
        }
        for h in heads {
            endpoints.push(h.with_polarity(EndpointPolarity::Head));
        }
        Hyperedge {
            id: HyperedgeId(id),
            kind: HyperedgeKind::new("flow"),
            endpoints,
            weight_milli: None,
            metadata: BTreeMap::new(),
            valid_from: RevisionId::ZERO,
            valid_to: None,
            directionality: Directionality::Directed,
            authoring_frame: None,
            computation: None,
        }
    }

    #[test]
    fn b_connectivity_requires_all_tails() {
        let space = SpaceId(1);
        let t1 = node(space, 1);
        let t2 = node(space, 2);
        let h = node(space, 3);
        let edge = directed(1, vec![t1.clone(), t2.clone()], vec![h.clone()]);
        let rev = RevisionId::ZERO;

        let mut nodes = Vec::new();
        let mut levels = HashMap::new();
        let mut enqueue = VecDeque::new();
        let mut enqueued = HashSet::new();
        record_node(
            &mut nodes,
            &mut levels,
            t1.clone(),
            0,
            TraversalArrival::Start,
        );
        enqueued.insert(endpoint_key(&t1));
        expand_edge_reachability(
            &edge,
            &t1,
            0,
            TraversalDirection::Forward,
            10,
            &mut nodes,
            &mut levels,
            &mut enqueue,
            &mut enqueued,
        );
        assert!(
            levels.contains_key(&endpoint_key(&h)),
            "reachability expands to head from one tail"
        );

        let bconn = run_b_connectivity(&t1, &[edge.clone()], 10, &None, rev);
        assert!(
            bconn.level_of(&h).is_none(),
            "B-connectivity needs all tails in the reached set"
        );

        // Path T1 -> T2 lets B-connectivity eventually satisfy both tails of {T1,T2} -> H.
        let path = directed(2, vec![t1.clone()], vec![t2.clone()]);
        let bconn_with_path = run_b_connectivity(&t1, &[edge, path], 10, &None, rev);
        assert_eq!(bconn_with_path.level_of(&h), Some(2));
    }

    #[test]
    fn acyclic_dag_and_cycle() {
        let space = SpaceId(1);
        let a = node(space, 1);
        let b = node(space, 2);
        let c = node(space, 3);
        let dag = vec![
            directed(1, vec![a.clone()], vec![b.clone()]),
            directed(2, vec![b.clone()], vec![c.clone()]),
        ];
        assert!(hypergraph_acyclic_for_kinds(&dag, &[]));

        let cycle = vec![
            directed(1, vec![a.clone()], vec![b.clone()]),
            directed(2, vec![b.clone()], vec![c.clone()]),
            directed(3, vec![c.clone()], vec![a.clone()]),
        ];
        assert!(!hypergraph_acyclic_for_kinds(&cycle, &[]));
    }
}
