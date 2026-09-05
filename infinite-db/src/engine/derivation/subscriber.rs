//! Derivation bus subscribers.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::engine::flow_vector::{
    default_flow_vector_quantization, prepare_flow_vector_derivation, prepare_flow_vector_tombstones,
};
use crate::engine::hypergraph::{
    prepare_index_derivation, prepare_index_tombstones, registry_index_layout, HypergraphWriteRow,
};
use crate::engine::nexus::prepare_nexus_write;
use crate::infinitedb_core::ephemeris::{
    grazes, hyperedge_to_ephemeris_entry, wanderer_identity_point, EphemerisKind, EPHEMERIS_SPACE,
};
use crate::infinitedb_core::hyperedge::{Directionality, EndpointPolarity};
use crate::infinitedb_core::nexus::{
    NexusEdge, NexusEndpoint, NexusKind, NEXUS_KIND_GRAZE,
};
use crate::infinitedb_core::space::SpaceRegistry;
use crate::infinitedb_core::universe::{ContainerRef, WANDERER_REGISTRY_SPACE};

use super::event::{AssertionEvent, AssertionOp};

/// Produces derived write rows from assertion events.
pub trait DerivationSubscriber: Send + Sync {
    fn derive(&self, event: &AssertionEvent) -> Vec<HypergraphWriteRow>;
}

/// Endpoint reverse-index subscriber (M4).
pub struct EndpointIndexSubscriber {
    spaces: Arc<RwLock<SpaceRegistry>>,
}

impl EndpointIndexSubscriber {
    pub fn new(spaces: Arc<RwLock<SpaceRegistry>>) -> Self {
        Self { spaces }
    }
}

impl DerivationSubscriber for EndpointIndexSubscriber {
    fn derive(&self, event: &AssertionEvent) -> Vec<HypergraphWriteRow> {
        let layout = registry_index_layout(&self.spaces.read());
        let edge = match &event.op {
            AssertionOp::Upsert(e) | AssertionOp::Delete(e) => e,
        };
        match &event.op {
            AssertionOp::Upsert(_) => prepare_index_derivation(edge, layout, &self.spaces.read()),
            AssertionOp::Delete(_) => prepare_index_tombstones(edge, layout, &self.spaces.read()),
        }
    }
}

/// Flow-vector direction index subscriber (M7 + T12 cross-space).
pub struct FlowVectorSubscriber {
    spaces: Arc<RwLock<SpaceRegistry>>,
}

impl FlowVectorSubscriber {
    pub fn new(spaces: Arc<RwLock<SpaceRegistry>>) -> Self {
        Self { spaces }
    }
}

impl DerivationSubscriber for FlowVectorSubscriber {
    fn derive(&self, event: &AssertionEvent) -> Vec<HypergraphWriteRow> {
        let q = default_flow_vector_quantization();
        let registry = self.spaces.read();
        let edge = match &event.op {
            AssertionOp::Upsert(e) | AssertionOp::Delete(e) => e,
        };
        match &event.op {
            AssertionOp::Upsert(_) => prepare_flow_vector_derivation(edge, &registry, q),
            AssertionOp::Delete(_) => prepare_flow_vector_tombstones(edge, &registry, q),
        }
    }
}

/// Edge locator subscriber — watermark-only in M4 (locator is `Hyperedge::storage_point`).
pub struct EdgeLocatorSubscriber;

impl DerivationSubscriber for EdgeLocatorSubscriber {
    fn derive(&self, _event: &AssertionEvent) -> Vec<HypergraphWriteRow> {
        Vec::new()
    }
}

/// Opt-in graze Nexus trace subscriber (D-E4).
pub struct EphemerisGrazeSubscriber {
    spaces: Arc<RwLock<SpaceRegistry>>,
}

impl EphemerisGrazeSubscriber {
    pub fn new(spaces: Arc<RwLock<SpaceRegistry>>) -> Self {
        Self { spaces }
    }
}

impl DerivationSubscriber for EphemerisGrazeSubscriber {
    fn derive(&self, event: &AssertionEvent) -> Vec<HypergraphWriteRow> {
        if event.edge_space != EPHEMERIS_SPACE {
            return Vec::new();
        }
        let edge = match &event.op {
            AssertionOp::Upsert(e) => e,
            AssertionOp::Delete(_) => return Vec::new(),
        };
        let entry = match hyperedge_to_ephemeris_entry(edge) {
            Some(e) => e,
            None => return Vec::new(),
        };
        if matches!(entry.kind, EphemerisKind::Projected { .. }) || !entry.graze_trace {
            return Vec::new();
        }
        let registry = self.spaces.read();
        let grazes_list = grazes(&entry, &registry);
        let mut rows = Vec::new();
        for graze in grazes_list {
            let nexus_id = crate::infinitedb_core::ephemeris::graze_trace_nexus_id(
                entry.wanderer,
                graze.space,
                entry.stamp,
                &graze.region,
            );
            let wanderer_pt = wanderer_identity_point(entry.wanderer);
            let nexus = NexusEdge {
                id: nexus_id,
                kind: NexusKind::new(NEXUS_KIND_GRAZE),
                endpoints: vec![
                    NexusEndpoint {
                        container: ContainerRef::Space(WANDERER_REGISTRY_SPACE),
                        region: Some((wanderer_pt.clone(), wanderer_pt)),
                        polarity: EndpointPolarity::Tail,
                    },
                    NexusEndpoint {
                        container: ContainerRef::Space(graze.space),
                        region: Some(graze.region),
                        polarity: EndpointPolarity::Head,
                    },
                ],
                weight_milli: Some(crate::infinitedb_core::universe::GRAZE_WEIGHT_MILLI),
                metadata: Default::default(),
                valid_from: entry.stamp,
                valid_to: Some(entry.stamp),
                directionality: Directionality::Directed,
            };
            if let Ok(row) = prepare_nexus_write(&nexus) {
                rows.push(row);
            }
        }
        rows
    }
}

/// Collect rows from all subscribers (deduped by address is handled by MVCC).
pub fn derive_all(
    subscribers: &[Box<dyn DerivationSubscriber>],
    event: &AssertionEvent,
) -> Vec<HypergraphWriteRow> {
    let mut rows = Vec::new();
    for sub in subscribers {
        rows.extend(sub.derive(event));
    }
    rows
}
