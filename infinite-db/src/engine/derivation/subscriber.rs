//! Derivation bus subscribers.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::engine::flow_vector::{
    default_flow_vector_quantization, prepare_flow_vector_derivation, prepare_flow_vector_tombstones,
};
use crate::engine::hypergraph::{
    prepare_index_derivation, prepare_index_tombstones, registry_index_layout, HypergraphWriteRow,
};
use crate::infinitedb_core::space::SpaceRegistry;

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
            AssertionOp::Upsert(_) => prepare_index_derivation(edge, layout),
            AssertionOp::Delete(_) => prepare_index_tombstones(edge, layout),
        }
    }
}

/// Flow-vector direction index subscriber (M7).
pub struct FlowVectorSubscriber;

impl DerivationSubscriber for FlowVectorSubscriber {
    fn derive(&self, event: &AssertionEvent) -> Vec<HypergraphWriteRow> {
        let q = default_flow_vector_quantization();
        let edge = match &event.op {
            AssertionOp::Upsert(e) | AssertionOp::Delete(e) => e,
        };
        match &event.op {
            AssertionOp::Upsert(_) => prepare_flow_vector_derivation(edge, q),
            AssertionOp::Delete(_) => prepare_flow_vector_tombstones(edge, q),
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
