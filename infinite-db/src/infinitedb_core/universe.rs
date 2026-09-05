//! Universe graph view — container-generic graph over spaces and Nexus edges (D-U1–U3).

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use bincode::{Decode, Encode};

use serde::{Deserialize, Serialize};

use super::address::{RevisionId, SpaceId};
use super::frame::is_testimony_space;
use super::space::SpaceConfig;
use super::void::{VoidOr, VoidState};

/// Reserved infrastructure spaces (not universe members).
pub const NEXUS_SPACE: SpaceId = SpaceId(u64::MAX - 4);
pub const WANDERER_REGISTRY_SPACE: SpaceId = SpaceId(u64::MAX - 5);
/// Frame-admissible ephemeris testimony space (below testimony threshold).
pub const EPHEMERIS_SPACE: SpaceId = SpaceId(0x9000_0000_0000_0001);

pub const PLACEMENT_EDGE_KIND: &str = "placement";
pub const DEFAULT_PLACEMENT_WEIGHT_MILLI: i64 = 1000;

/// Stable id for a pinned constellation supernode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct ConstellationId(pub u64);

/// Reference to a graph node — space or pinned constellation (D-U7).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Encode, Decode)]
pub enum ContainerRef {
    Space(SpaceId),
    Constellation(ConstellationId),
}

impl ContainerRef {
    pub fn sort_key(&self) -> (u8, u64) {
        match self {
            ContainerRef::Space(id) => (0, id.0),
            ContainerRef::Constellation(id) => (1, id.0),
        }
    }
}

/// View-level edge consumed by analytics (projected placement + stored Nexus).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct UniverseEdge {
    pub kind: String,
    pub endpoints: Vec<ContainerRef>,
    pub weight_milli: Option<i64>,
    pub valid_from: RevisionId,
    pub valid_to: Option<RevisionId>,
    /// True when projected from the registry, not stored in `NEXUS_SPACE`.
    pub projected: bool,
    pub nexus_id: Option<u64>,
}

/// Assembled universe graph snapshot (INV-UNI-GENERIC).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct UniverseGraphView {
    pub nodes: Vec<ContainerRef>,
    pub edges: Vec<UniverseEdge>,
}

impl UniverseGraphView {
    pub fn active_edges_at(&self, as_of: RevisionId) -> Vec<&UniverseEdge> {
        self.edges
            .iter()
            .filter(|e| is_active_window(as_of, e.valid_from, e.valid_to))
            .collect()
    }

    /// Members exist but no active edges at `as_of` (D-U13 relation-void).
    pub fn is_relation_void(&self, as_of: RevisionId) -> bool {
        !self.nodes.is_empty() && self.active_edges_at(as_of).is_empty()
    }

    pub fn sorted(self) -> Self {
        let mut nodes = self.nodes;
        nodes.sort_by_key(|n| n.sort_key());
        let mut edges = self.edges;
        edges.sort_by(|a, b| {
            let a_eps = normalized_endpoint_keys(&a.endpoints);
            let b_eps = normalized_endpoint_keys(&b.endpoints);
            (
                a.projected,
                a.kind.as_str(),
                a.nexus_id,
                a_eps,
                a.valid_from,
                a.weight_milli,
            )
                .cmp(&(
                    b.projected,
                    b.kind.as_str(),
                    b.nexus_id,
                    b_eps,
                    b.valid_from,
                    b.weight_milli,
                ))
        });
        Self { nodes, edges }
    }
}

impl VoidState for UniverseGraphView {
    fn is_void(&self) -> bool {
        self.nodes.is_empty()
    }
}

/// Whether a space is a universe member (D-U6).
pub fn is_universe_member(space: SpaceId, config: &SpaceConfig) -> bool {
    if space == NEXUS_SPACE || space == WANDERER_REGISTRY_SPACE || space == EPHEMERIS_SPACE {
        return false;
    }
    if !is_testimony_space(space) {
        return false;
    }
    if config.name.ends_with("_errors") {
        return false;
    }
    if (space.0 & 0xE000_0000_0000_0000) == 0xE000_0000_0000_0000 {
        return false;
    }
    true
}

pub fn is_active_window(at: RevisionId, from: RevisionId, to: Option<RevisionId>) -> bool {
    at >= from && to.map(|t| at <= t).unwrap_or(true)
}

/// Per-component center and periphery (min / max eccentricity).
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ComponentCenters {
    pub members: Vec<ContainerRef>,
    pub centers: Vec<ContainerRef>,
    pub periphery: Vec<ContainerRef>,
}

/// Ratio-shaped statistics over void or singleton input (D-U13).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniverseRatioError {
    MemberVoid,
    UndefinedSingleton,
}

/// Graze-trace weight: present on the graph but ignored by constellation detection
/// (`INV-EPH-UNCLUSTERED`).
pub const GRAZE_WEIGHT_MILLI: i64 = 0;

fn edge_weight_milli(edge: &UniverseEdge) -> i64 {
    edge.weight_milli.unwrap_or(if edge.projected {
        DEFAULT_PLACEMENT_WEIGHT_MILLI
    } else {
        1
    })
}

fn normalized_endpoint_keys(endpoints: &[ContainerRef]) -> Vec<(u8, u64)> {
    let mut keys: Vec<(u8, u64)> = endpoints.iter().map(|e| e.sort_key()).collect();
    keys.sort();
    keys.dedup();
    keys
}

fn sort_container_refs(refs: &mut [ContainerRef]) {
    refs.sort_by_key(|n| n.sort_key());
}

/// Mean eccentricity across all members (undefined on void or single-node universe).
pub fn mean_eccentricity(
    view: &UniverseGraphView,
    as_of: RevisionId,
) -> Result<f64, UniverseRatioError> {
    if view.is_void() {
        return Err(UniverseRatioError::MemberVoid);
    }
    let comps = connected_components(view, as_of);
    if comps.iter().all(|c| c.len() <= 1) {
        return Err(UniverseRatioError::UndefinedSingleton);
    }
    let mut sum = 0u64;
    let mut count = 0u64;
    for members in comps {
        if members.len() <= 1 {
            continue;
        }
        let eccentricity = all_eccentricities(view, &members, as_of);
        for e in eccentricity.values() {
            sum += u64::from(*e);
            count += 1;
        }
    }
    if count == 0 {
        return Err(UniverseRatioError::UndefinedSingleton);
    }
    Ok(sum as f64 / count as f64)
}

/// Active-edge density: edges / possible pairs (undefined on void or singleton).
pub fn edge_set_density(
    view: &UniverseGraphView,
    as_of: RevisionId,
) -> Result<f64, UniverseRatioError> {
    if view.is_void() {
        return Err(UniverseRatioError::MemberVoid);
    }
    let n = view.nodes.len();
    if n <= 1 {
        return Err(UniverseRatioError::UndefinedSingleton);
    }
    let active = view.active_edges_at(as_of).len();
    let pairs = n * (n - 1) / 2;
    Ok(active as f64 / pairs as f64)
}

/// Newman modularity of the detected clustering (undefined on void or singleton).
pub fn modularity(
    view: &UniverseGraphView,
    as_of: RevisionId,
) -> Result<f64, UniverseRatioError> {
    if view.is_void() {
        return Err(UniverseRatioError::MemberVoid);
    }
    if view.nodes.len() <= 1 {
        return Err(UniverseRatioError::UndefinedSingleton);
    }
    let VoidOr::Known(clusters) = detect_constellations(view, as_of) else {
        return Err(UniverseRatioError::MemberVoid);
    };
    let mut community: HashMap<ContainerRef, usize> = HashMap::new();
    for (i, cluster) in clusters.iter().enumerate() {
        for n in cluster {
            community.insert(n.clone(), i);
        }
    }
    let adj = weighted_adjacency(view, as_of);
    let mut two_m: i64 = 0;
    let mut degree: HashMap<ContainerRef, i64> = HashMap::new();
    for (node, neighbors) in &adj {
        let k: i64 = neighbors.iter().map(|(_, w)| *w).sum();
        degree.insert(node.clone(), k);
        two_m += k;
    }
    if two_m <= 0 {
        return Err(UniverseRatioError::UndefinedSingleton);
    }
    let mut q = 0.0f64;
    for (i, neighbors) in &adj {
        let ki = *degree.get(i).unwrap_or(&0);
        let ci = *community.get(i).unwrap_or(&0);
        for (j, w) in neighbors {
            let cj = *community.get(j).unwrap_or(&0);
            if ci != cj {
                continue;
            }
            let kj = *degree.get(j).unwrap_or(&0);
            q += (*w as f64) - (ki as f64) * (kj as f64) / (two_m as f64);
        }
    }
    Ok(q / (two_m as f64))
}

/// Cross-container distance — `Void` when nodes lie in different components (INV-UNI-ANNIHILATOR).
pub fn distance_in_view(
    view: &UniverseGraphView,
    a: &ContainerRef,
    b: &ContainerRef,
    as_of: RevisionId,
) -> VoidOr<u32> {
    let comps = connected_components(view, as_of);
    for comp in &comps {
        if comp.contains(a) && comp.contains(b) {
            return VoidOr::Known(bfs_distance(view, a, b, as_of));
        }
    }
    VoidOr::Void
}

/// Center and periphery per connected component (INV-UNI-DETERMINISTIC).
pub fn center_and_periphery(
    view: &UniverseGraphView,
    as_of: RevisionId,
) -> VoidOr<Vec<ComponentCenters>> {
    if view.is_void() {
        return VoidOr::Void;
    }
    let comps = connected_components(view, as_of);
    let mut out = Vec::new();
    for members in comps {
        let eccentricity = all_eccentricities(view, &members, as_of);
        let min_e = eccentricity.values().min().copied().unwrap_or(0);
        let max_e = eccentricity.values().max().copied().unwrap_or(0);
        let centers: Vec<ContainerRef> = eccentricity
            .iter()
            .filter(|(_, e)| **e == min_e)
            .map(|(n, _)| n.clone())
            .collect();
        let periphery: Vec<ContainerRef> = eccentricity
            .iter()
            .filter(|(_, e)| **e == max_e)
            .map(|(n, _)| n.clone())
            .collect();
        let mut centers = centers;
        let mut periphery = periphery;
        sort_container_refs(&mut centers);
        sort_container_refs(&mut periphery);
        out.push(ComponentCenters {
            members,
            centers,
            periphery,
        });
    }
    out.sort_by_key(|c| c.members.first().map(|m| m.sort_key()).unwrap_or((0, 0)));
    VoidOr::Known(out)
}

/// Weighted label propagation with id-sorted tie-breaks (D-U9).
pub fn detect_constellations(view: &UniverseGraphView, as_of: RevisionId) -> VoidOr<Vec<Vec<ContainerRef>>> {
    if view.is_void() {
        return VoidOr::Void;
    }
    let nodes: Vec<ContainerRef> = view.nodes.clone();
    let mut labels: HashMap<ContainerRef, ContainerRef> = nodes
        .iter()
        .map(|n| (n.clone(), n.clone()))
        .collect();
    let weighted = weighted_adjacency(view, as_of);
    let mut changed = true;
    while changed {
        changed = false;
        for node in &nodes {
            let neighbors: Vec<(ContainerRef, i64)> = weighted
                .get(node)
                .map(|s| s.clone())
                .unwrap_or_default();
            if neighbors.is_empty() {
                continue;
            }
            let max_w = neighbors.iter().map(|(_, w)| *w).max().unwrap_or(0);
            let strongest: Vec<&ContainerRef> = neighbors
                .iter()
                .filter(|(_, w)| *w == max_w)
                .map(|(n, _)| n)
                .collect();
            let best_neighbor = strongest
                .iter()
                .min_by_key(|n| labels.get(n).map(|l| l.sort_key()).unwrap_or(n.sort_key()));
            if let Some(best_neighbor) = best_neighbor {
                let new_label = labels
                    .get(best_neighbor)
                    .cloned()
                    .unwrap_or_else(|| (*best_neighbor).clone());
                if labels.get(node) != Some(&new_label) {
                    labels.insert(node.clone(), new_label);
                    changed = true;
                }
            }
        }
    }
    let mut groups: BTreeMap<(u8, u64), Vec<ContainerRef>> = BTreeMap::new();
    for node in &nodes {
        let label = labels.get(node).cloned().unwrap_or_else(|| node.clone());
        groups.entry(label.sort_key()).or_default().push(node.clone());
    }
    let clusters: Vec<Vec<ContainerRef>> = groups
        .into_values()
        .map(|mut g| {
            g.sort_by_key(|n| n.sort_key());
            g
        })
        .collect();
    VoidOr::Known(clusters)
}

/// Contract a member cluster to a constellation supernode (INV-UNI-ZOOM).
pub fn contract(
    view: &UniverseGraphView,
    cluster: &[ContainerRef],
    supernode: ContainerRef,
) -> UniverseGraphView {
    let cluster_set: BTreeSet<_> = cluster.iter().cloned().collect();
    let mut nodes: Vec<ContainerRef> = view
        .nodes
        .iter()
        .filter(|n| !cluster_set.contains(n))
        .cloned()
        .collect();
    if !nodes.iter().any(|n| n == &supernode) {
        nodes.push(supernode.clone());
    }
    nodes.sort_by_key(|n| n.sort_key());

    let mut edges: Vec<UniverseEdge> = Vec::new();
    let mut super_edges: HashMap<(String, Vec<(u8, u64)>), UniverseEdge> = HashMap::new();

    for edge in &view.edges {
        let mapped: Vec<ContainerRef> = edge
            .endpoints
            .iter()
            .map(|ep| {
                if cluster_set.contains(ep) {
                    supernode.clone()
                } else {
                    ep.clone()
                }
            })
            .collect();
        if mapped.iter().all(|m| m == &supernode) {
            continue;
        }
        let key = (edge.kind.clone(), normalized_endpoint_keys(&mapped));
        let entry = super_edges.entry(key).or_insert_with(|| UniverseEdge {
            kind: edge.kind.clone(),
            endpoints: mapped.clone(),
            weight_milli: edge.weight_milli,
            valid_from: edge.valid_from,
            valid_to: edge.valid_to,
            projected: edge.projected,
            nexus_id: edge.nexus_id,
        });
        if let (Some(w), Some(ew)) = (entry.weight_milli, edge.weight_milli) {
            entry.weight_milli = Some(w.saturating_add(ew));
        }
    }
    edges.extend(super_edges.into_values());
    edges.sort_by(|a, b| {
        let a_eps = normalized_endpoint_keys(&a.endpoints);
        let b_eps = normalized_endpoint_keys(&b.endpoints);
        (a.projected, a.kind.as_str(), a.nexus_id, a_eps)
            .cmp(&(b.projected, b.kind.as_str(), b.nexus_id, b_eps))
    });
    UniverseGraphView { nodes, edges }
}

fn connected_components(view: &UniverseGraphView, as_of: RevisionId) -> Vec<Vec<ContainerRef>> {
    let adj = adjacency(view, as_of);
    let mut seen: BTreeSet<ContainerRef> = BTreeSet::new();
    let mut comps = Vec::new();
    for node in &view.nodes {
        if seen.contains(node) {
            continue;
        }
        let mut comp = Vec::new();
        let mut queue = VecDeque::new();
        queue.push_back(node.clone());
        seen.insert(node.clone());
        while let Some(cur) = queue.pop_front() {
            comp.push(cur.clone());
            for nb in adj.get(&cur).into_iter().flat_map(|s| s.iter()) {
                if !seen.contains(nb) {
                    seen.insert(nb.clone());
                    queue.push_back(nb.clone());
                }
            }
        }
        comp.sort_by_key(|n| n.sort_key());
        comps.push(comp);
    }
    comps.sort_by_key(|c| c.first().map(|n| n.sort_key()).unwrap_or((0, 0)));
    comps
}

fn weighted_adjacency(
    view: &UniverseGraphView,
    as_of: RevisionId,
) -> HashMap<ContainerRef, Vec<(ContainerRef, i64)>> {
    let mut adj: HashMap<ContainerRef, HashMap<ContainerRef, i64>> = HashMap::new();
    for node in &view.nodes {
        adj.entry(node.clone()).or_default();
    }
    for edge in view.active_edges_at(as_of) {
        let w = edge_weight_milli(edge);
        if w <= 0 {
            continue;
        }
        for i in 0..edge.endpoints.len() {
            for j in (i + 1)..edge.endpoints.len() {
                let a = edge.endpoints[i].clone();
                let b = edge.endpoints[j].clone();
                adj.entry(a.clone())
                    .or_default()
                    .entry(b.clone())
                    .and_modify(|e| *e = (*e).max(w))
                    .or_insert(w);
                adj.entry(b)
                    .or_default()
                    .entry(a)
                    .and_modify(|e| *e = (*e).max(w))
                    .or_insert(w);
            }
        }
    }
    adj.into_iter()
        .map(|(k, m)| {
            let v: Vec<(ContainerRef, i64)> = m.into_iter().collect();
            (k, v)
        })
        .collect()
}

fn adjacency(view: &UniverseGraphView, as_of: RevisionId) -> HashMap<ContainerRef, BTreeSet<ContainerRef>> {
    let mut adj: HashMap<ContainerRef, BTreeSet<ContainerRef>> = HashMap::new();
    for node in &view.nodes {
        adj.entry(node.clone()).or_default();
    }
    for edge in view.active_edges_at(as_of) {
        for i in 0..edge.endpoints.len() {
            for j in (i + 1)..edge.endpoints.len() {
                let a = edge.endpoints[i].clone();
                let b = edge.endpoints[j].clone();
                adj.entry(a.clone()).or_default().insert(b.clone());
                adj.entry(b).or_default().insert(a);
            }
        }
    }
    adj
}

fn bfs_distance(view: &UniverseGraphView, from: &ContainerRef, to: &ContainerRef, as_of: RevisionId) -> u32 {
    if from == to {
        return 0;
    }
    let adj = adjacency(view, as_of);
    let mut dist: HashMap<ContainerRef, u32> = HashMap::new();
    let mut queue = VecDeque::new();
    queue.push_back(from.clone());
    dist.insert(from.clone(), 0);
    while let Some(cur) = queue.pop_front() {
        let d = dist[&cur];
        for nb in adj.get(&cur).into_iter().flat_map(|s| s.iter()) {
            if !dist.contains_key(nb) {
                dist.insert(nb.clone(), d + 1);
                if nb == to {
                    return d + 1;
                }
                queue.push_back(nb.clone());
            }
        }
    }
    u32::MAX
}

fn all_eccentricities(
    view: &UniverseGraphView,
    members: &[ContainerRef],
    as_of: RevisionId,
) -> BTreeMap<ContainerRef, u32> {
    let mut out = BTreeMap::new();
    for node in members {
        let mut max_d = 0u32;
        for other in members {
            if node != other {
                let d = bfs_distance(view, node, other, as_of);
                if d != u32::MAX {
                    max_d = max_d.max(d);
                }
            }
        }
        out.insert(node.clone(), max_d);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::address::RevisionId;

    fn space(id: u64) -> ContainerRef {
        ContainerRef::Space(SpaceId(id))
    }

    fn path_graph() -> UniverseGraphView {
        let nodes = vec![space(1), space(2), space(3)];
        let edges = vec![
            UniverseEdge {
                kind: "link".into(),
                endpoints: vec![space(1), space(2)],
                weight_milli: Some(1000),
                valid_from: RevisionId::ZERO,
                valid_to: None,
                projected: false,
                nexus_id: Some(1),
            },
            UniverseEdge {
                kind: "link".into(),
                endpoints: vec![space(2), space(3)],
                weight_milli: Some(1000),
                valid_from: RevisionId::ZERO,
                valid_to: None,
                projected: false,
                nexus_id: Some(2),
            },
        ];
        UniverseGraphView { nodes, edges }
    }

    #[test]
    fn membership_excludes_infrastructure() {
        let data = SpaceConfig::new(SpaceId(10), "data", 2);
        let nexus_cfg = SpaceConfig::new(NEXUS_SPACE, "__nexus__", 2);
        assert!(is_universe_member(SpaceId(10), &data));
        assert!(!is_universe_member(NEXUS_SPACE, &nexus_cfg));
        assert!(!is_universe_member(EPHEMERIS_SPACE, &data));
    }

    #[test]
    fn inv_uni_void_member_and_relation() {
        let empty = UniverseGraphView {
            nodes: vec![],
            edges: vec![],
        };
        assert!(empty.is_void());
        let isolated = UniverseGraphView {
            nodes: vec![space(1)],
            edges: vec![],
        };
        assert!(!isolated.is_void());
        assert!(isolated.is_relation_void(RevisionId::ZERO));
    }

    #[test]
    fn path_center_is_middle() {
        let view = path_graph();
        let centers = center_and_periphery(&view, RevisionId::ZERO).known().unwrap();
        assert_eq!(centers.len(), 1);
        assert!(centers[0].centers.contains(&space(2)));
        assert!(centers[0].periphery.contains(&space(1)));
        assert!(centers[0].periphery.contains(&space(3)));
    }

    #[test]
    fn inv_uni_deterministic() {
        let view = path_graph();
        let a = detect_constellations(&view, RevisionId::ZERO);
        let b = detect_constellations(&view, RevisionId::ZERO);
        assert_eq!(a, b);
    }

    #[test]
    fn cross_component_distance_is_void() {
        let view = UniverseGraphView {
            nodes: vec![space(1), space(2)],
            edges: vec![],
        };
        assert!(distance_in_view(&view, &space(1), &space(2), RevisionId::ZERO).is_void());
    }

    #[test]
    fn inv_uni_zoom_contract() {
        let view = UniverseGraphView {
            nodes: vec![space(1), space(2), space(3), space(4)],
            edges: vec![
                UniverseEdge {
                    kind: "link".into(),
                    endpoints: vec![space(1), space(2)],
                    weight_milli: Some(1000),
                    valid_from: RevisionId::ZERO,
                    valid_to: None,
                    projected: false,
                    nexus_id: Some(1),
                },
                UniverseEdge {
                    kind: "link".into(),
                    endpoints: vec![space(3), space(4)],
                    weight_milli: Some(1000),
                    valid_from: RevisionId::ZERO,
                    valid_to: None,
                    projected: false,
                    nexus_id: Some(2),
                },
            ],
        };
        let cluster = vec![space(1), space(2)];
        let supernode = ContainerRef::Constellation(ConstellationId(1));
        let contracted = contract(&view, &cluster, supernode.clone());
        let outer = detect_constellations(&contracted, RevisionId::ZERO).known().unwrap();
        assert_eq!(outer.len(), 2);
    }

    #[test]
    fn ratio_stats_void_and_singleton() {
        let empty = UniverseGraphView {
            nodes: vec![],
            edges: vec![],
        };
        assert_eq!(
            mean_eccentricity(&empty, RevisionId::ZERO),
            Err(UniverseRatioError::MemberVoid)
        );
        let singleton = UniverseGraphView {
            nodes: vec![space(1)],
            edges: vec![],
        };
        assert_eq!(
            mean_eccentricity(&singleton, RevisionId::ZERO),
            Err(UniverseRatioError::UndefinedSingleton)
        );
        assert_eq!(
            modularity(&empty, RevisionId::ZERO),
            Err(UniverseRatioError::MemberVoid)
        );
        assert_eq!(
            modularity(&singleton, RevisionId::ZERO),
            Err(UniverseRatioError::UndefinedSingleton)
        );
    }

    #[test]
    fn path_of_five_one_cluster() {
        let view = UniverseGraphView {
            nodes: vec![space(1), space(2), space(3), space(4), space(5)],
            edges: vec![
                UniverseEdge {
                    kind: "link".into(),
                    endpoints: vec![space(1), space(2)],
                    weight_milli: Some(1),
                    valid_from: RevisionId::ZERO,
                    valid_to: None,
                    projected: false,
                    nexus_id: Some(1),
                },
                UniverseEdge {
                    kind: "link".into(),
                    endpoints: vec![space(2), space(3)],
                    weight_milli: Some(1),
                    valid_from: RevisionId::ZERO,
                    valid_to: None,
                    projected: false,
                    nexus_id: Some(2),
                },
                UniverseEdge {
                    kind: "link".into(),
                    endpoints: vec![space(3), space(4)],
                    weight_milli: Some(1),
                    valid_from: RevisionId::ZERO,
                    valid_to: None,
                    projected: false,
                    nexus_id: Some(3),
                },
                UniverseEdge {
                    kind: "link".into(),
                    endpoints: vec![space(4), space(5)],
                    weight_milli: Some(1),
                    valid_from: RevisionId::ZERO,
                    valid_to: None,
                    projected: false,
                    nexus_id: Some(4),
                },
            ],
        };
        let clusters = detect_constellations(&view, RevisionId::ZERO).known().unwrap();
        assert_eq!(clusters.len(), 1);
    }
}
