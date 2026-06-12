//! Background derivation bus: fan-out, parallel derive, hash-partitioned workers.

mod backpressure;
mod delta;
mod event;
mod subscriber;
mod watermark;

pub use backpressure::{DerivationBackpressurePolicy, DerivationStats};
pub use delta::record_in_derivation_delta;
pub use event::AssertionEvent;
pub use subscriber::{
    derive_all, DerivationSubscriber, EdgeLocatorSubscriber, EndpointIndexSubscriber,
    FlowVectorSubscriber,
};
pub use watermark::{FailedDerivation, WatermarkRegistry};

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{unbounded, Sender};
use parking_lot::Mutex;

use crate::engine::error::EngineError;
use crate::engine::hypergraph::HypergraphWriteRow;
use crate::engine::session::VersionVector;
use crate::infinitedb_core::address::RevisionId;
use crate::infinitedb_core::hlc::SessionId;
use crate::infinitedb_core::hyperedge::HyperedgeId;

/// Apply derived rows to storage at the assertion's source revision.
pub trait DerivationSink: Send + Sync {
    fn apply_derived_rows(
        &self,
        rows: Vec<HypergraphWriteRow>,
        source_revision: RevisionId,
    ) -> Result<(), EngineError>;
}

struct BusState {
    subscribers: Vec<Box<dyn DerivationSubscriber>>,
    watermarks: Arc<WatermarkRegistry>,
    sink: Arc<dyn DerivationSink>,
    stats: Arc<Mutex<DerivationStats>>,
}

impl BusState {
    fn process_event(&self, event: AssertionEvent) -> Result<(), EngineError> {
        let session = SessionId(event.source_revision.session());
        if event.branch != crate::infinitedb_core::branch::BranchId::MAIN {
            self.watermarks
                .retire_for_session(session, event.source_revision);
            return Ok(());
        }
        let _ = event.edge_space;
        let rows = derive_all(&self.subscribers, &event);
        let row_count = rows.len();
        if !rows.is_empty() {
            self.sink
                .apply_derived_rows(rows, event.source_revision)?;
            self.stats.lock().derived_rows_written += row_count as u64;
        }
        self.watermarks
            .retire_for_session(session, event.source_revision);
        self.stats.lock().events_processed += 1;
        Ok(())
    }

    fn record_failure(&self, event: &AssertionEvent, error: EngineError) {
        let session = SessionId(event.source_revision.session());
        self.watermarks.record_failure_for_session(
            session,
            event.source_revision,
            error.to_string(),
        );
        self.stats.lock().derivation_failures += 1;
    }
}

/// Stable 64-bit mix for hash-partitioned worker routing (deterministic across runs).
fn worker_index_for_edge(edge_id: HyperedgeId, worker_count: usize) -> usize {
    let mut x = edge_id.0;
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^= x >> 33;
    (x as usize) % worker_count.max(1)
}

/// Parallel derivation bus with hash-partitioned per-edge ordering.
pub struct DerivationBus {
    txs: Mutex<Option<Vec<Sender<AssertionEvent>>>>,
    watermarks: Arc<WatermarkRegistry>,
    stats: Arc<Mutex<DerivationStats>>,
    pending: Arc<AtomicU64>,
    policy: DerivationBackpressurePolicy,
    workers: Mutex<Vec<JoinHandle<()>>>,
    shutdown: Arc<AtomicBool>,
    worker_count: usize,
}

impl DerivationBus {
    pub fn new(
        policy: DerivationBackpressurePolicy,
        watermarks: Arc<WatermarkRegistry>,
        subscribers: Vec<Box<dyn DerivationSubscriber>>,
        sink: Arc<dyn DerivationSink>,
    ) -> Self {
        let worker_count = policy.max_worker_threads.max(1);
        let mut txs: Vec<Sender<AssertionEvent>> = Vec::with_capacity(worker_count);
        let mut rxs = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let (tx, rx) = unbounded();
            txs.push(tx);
            rxs.push(rx);
        }

        let stats = Arc::new(Mutex::new(DerivationStats::default()));
        let pending = Arc::new(AtomicU64::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));

        let shared = Arc::new(BusState {
            subscribers,
            watermarks: Arc::clone(&watermarks),
            sink,
            stats: Arc::clone(&stats),
        });

        let mut workers = Vec::with_capacity(worker_count);

        for rx in rxs {
            let shared = Arc::clone(&shared);
            let shutdown = Arc::clone(&shutdown);
            let pending_counter = Arc::clone(&pending);
            workers.push(thread::spawn(move || {
                let _shutdown = shutdown;
                while let Ok(event) = rx.recv() {
                    if let Err(e) = shared.process_event(event.clone()) {
                        shared.record_failure(&event, e);
                    }
                    pending_counter.fetch_sub(1, Ordering::AcqRel);
                }
            }));
        }

        Self {
            txs: Mutex::new(Some(txs)),
            watermarks,
            stats,
            pending,
            policy,
            workers: Mutex::new(workers),
            shutdown,
            worker_count,
        }
    }

    pub fn stats(&self) -> DerivationStats {
        let mut s = self.stats.lock().clone();
        s.pending_tasks = self.pending.load(Ordering::Acquire) as usize;
        s.outstanding_derivations = self.watermarks.total_outstanding();
        s
    }

    pub fn failed_derivations(&self) -> Vec<FailedDerivation> {
        self.watermarks.failed_derivations()
    }

    pub fn endpoint_index_watermark(&self) -> RevisionId {
        self.watermarks
            .get("endpoint_index")
            .unwrap_or(RevisionId::ZERO)
    }

    pub fn endpoint_index_watermark_vector(&self) -> VersionVector {
        self.watermarks
            .get_vector("endpoint_index")
            .unwrap_or_default()
    }

    pub fn flow_vector_index_watermark(&self) -> RevisionId {
        self.watermarks
            .get("flow_vector_index")
            .unwrap_or(RevisionId::ZERO)
    }

    pub fn flow_vector_index_watermark_vector(&self) -> VersionVector {
        self.watermarks
            .get_vector("flow_vector_index")
            .unwrap_or_default()
    }

    pub fn min_watermark_vector(&self) -> VersionVector {
        self.watermarks.min_vector()
    }

    pub fn check_backpressure(
        &self,
        submitting_session: SessionId,
        allocated_revision: RevisionId,
    ) -> Result<(), EngineError> {
        let pending = self.pending.load(Ordering::Acquire) as usize;
        let lag = self
            .watermarks
            .lag_for_session(submitting_session, allocated_revision);
        if pending >= self.policy.max_pending_tasks || lag > self.policy.max_derivation_lag {
            self.stats.lock().backpressure_rejections += 1;
            return Err(EngineError::DerivationBackpressure {
                pending_tasks: pending,
                derivation_lag: lag,
            });
        }
        Ok(())
    }

    pub fn submit(&self, event: AssertionEvent) -> Result<(), EngineError> {
        let session = SessionId(event.source_revision.session());
        self.watermarks
            .register_for_session(session, event.source_revision);
        self.pending.fetch_add(1, Ordering::AcqRel);
        let edge_id = event.edge_id();
        let worker_idx = worker_index_for_edge(edge_id, self.worker_count);
        let guard = self.txs.lock();
        let Some(txs) = guard.as_ref() else {
            self.pending.fetch_sub(1, Ordering::AcqRel);
            return Err(EngineError::Other {
                message: "derivation bus channel closed".into(),
            });
        };
        if txs[worker_idx].send(event).is_err() {
            self.pending.fetch_sub(1, Ordering::AcqRel);
            return Err(EngineError::Other {
                message: "derivation bus channel closed".into(),
            });
        }
        Ok(())
    }

    /// Block until all queued events are processed.
    pub fn flush(&self) {
        while self.pending.load(Ordering::Acquire) > 0 {
            thread::yield_now();
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.txs.lock().take();
        self.flush();
        let handles = self.workers.lock().drain(..).collect::<Vec<_>>();
        for h in handles {
            let _ = h.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_partition_is_stable() {
        let id = HyperedgeId(42);
        assert_eq!(
            worker_index_for_edge(id, 8),
            worker_index_for_edge(id, 8)
        );
        assert!(worker_index_for_edge(id, 8) < 8);
    }
}
