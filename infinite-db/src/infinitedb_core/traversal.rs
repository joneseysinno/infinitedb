//! Cross-space hypergraph traversal.
//!
//! Walks hyperedges from a starting endpoint using the reverse endpoint index,
//! accumulating a bounded subgraph for graph-style queries.

use serde::{Deserialize, Serialize};
use super::{
    address::RevisionId,
    hyperedge::{EndpointRef, Hyperedge, HyperedgeKind},
};

/// Parameters controlling a graph walk from a starting node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraversalSpec {
    /// Node to begin the walk from.
    pub start: EndpointRef,
    /// Maximum BFS depth (0 = start node only).
    pub max_depth: usize,
    /// If set, only follow edges whose kind is in this list.
    pub follow_kinds: Option<Vec<HyperedgeKind>>,
    /// Only consider edges active at or before this revision.
    pub as_of: Option<RevisionId>,
}

/// The nodes and edges collected during a traversal.
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
        if !self.nodes.iter().any(|n| n.space == ep.space && n.node.coords == ep.node.coords) {
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
