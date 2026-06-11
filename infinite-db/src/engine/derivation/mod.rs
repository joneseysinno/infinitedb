//! Background derivation bus: fan-out, parallel derive, shard routing.

mod backpressure;
mod event;
mod subscriber;
mod watermark;

pub use backpressure::{DerivationBackpressurePolicy, DerivationStats};
pub use event::AssertionEvent;
pub use subscriber::{
    derive_all, DerivationSubscriber, EdgeLocatorSubscriber, EndpointIndexSubscriber,
    FlowVectorSubscriber,
};
pub use watermark::WatermarkRegistry;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;
use parking_lot::Mutex;

use crate::engine::error::EngineError;
use crate::engine::hypergraph::HypergraphWriteRow;
use crate::infinitedb_core::address::RevisionId;
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
        if event.branch != crate::infinitedb_core::branch::BranchId::MAIN {
            self.watermarks.advance_all(event.source_revision);
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
        self.watermarks.advance_all(event.source_revision);
        self.stats.lock().events_processed += 1;
        Ok(())
    }
}

/// Parallel derivation bus with per-edge ordering.
pub struct DerivationBus {
    tx: Mutex<Option<Sender<AssertionEvent>>>,
    watermarks: Arc<WatermarkRegistry>,
    stats: Arc<Mutex<DerivationStats>>,
    pending: Arc<AtomicU64>,
    policy: DerivationBackpressurePolicy,
    workers: Mutex<Vec<JoinHandle<()>>>,
    shutdown: Arc<AtomicBool>,
}

impl DerivationBus {
    pub fn new(
        policy: DerivationBackpressurePolicy,
        watermarks: Arc<WatermarkRegistry>,
        subscribers: Vec<Box<dyn DerivationSubscriber>>,
        sink: Arc<dyn DerivationSink>,
    ) -> Self {
        let (tx, rx) = unbounded();
        let stats = Arc::new(Mutex::new(DerivationStats::default()));
        let pending = Arc::new(AtomicU64::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let edge_locks: Arc<DashMap<HyperedgeId, Arc<Mutex<()>>>> = Arc::new(DashMap::new());

        let shared = Arc::new(BusState {
            subscribers,
            watermarks: Arc::clone(&watermarks),
            sink,
            stats: Arc::clone(&stats),
        });

        let worker_count = policy.max_worker_threads.max(1);
        let mut workers = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let rx: Receiver<AssertionEvent> = rx.clone();
            let shared = Arc::clone(&shared);
            let edge_locks = Arc::clone(&edge_locks);
            let shutdown = Arc::clone(&shutdown);
            let pending_counter = Arc::clone(&pending);
            workers.push(thread::spawn(move || {
                while let Ok(event) = rx.recv() {
                    if shutdown.load(Ordering::Acquire) {
                        break;
                    }
                    let edge_id = event.edge_id();
                    let lock = edge_locks
                        .entry(edge_id)
                        .or_insert_with(|| Arc::new(Mutex::new(())))
                        .clone();
                    let _guard = lock.lock();
                    if let Err(e) = shared.process_event(event) {
                        eprintln!("derivation bus error: {e}");
                    }
                    pending_counter.fetch_sub(1, Ordering::AcqRel);
                }
            }));
        }

        Self {
            tx: Mutex::new(Some(tx)),
            watermarks,
            stats,
            pending,
            policy,
            workers: Mutex::new(workers),
            shutdown,
        }
    }

    pub fn stats(&self) -> DerivationStats {
        let mut s = self.stats.lock().clone();
        s.pending_tasks = self.pending.load(Ordering::Acquire) as usize;
        s
    }

    pub fn endpoint_index_watermark(&self) -> RevisionId {
        self.watermarks
            .get("endpoint_index")
            .unwrap_or(RevisionId::ZERO)
    }

    pub fn flow_vector_index_watermark(&self) -> RevisionId {
        self.watermarks
            .get("flow_vector_index")
            .unwrap_or(RevisionId::ZERO)
    }

    pub fn min_watermark(&self) -> RevisionId {
        self.watermarks.min_watermark()
    }

    pub fn check_backpressure(&self, allocated_revision: RevisionId) -> Result<(), EngineError> {
        let pending = self.pending.load(Ordering::Acquire) as usize;
        let lag = allocated_revision
            .0
            .saturating_sub(self.watermarks.min_watermark().0);
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
        self.pending.fetch_add(1, Ordering::AcqRel);
        let guard = self.tx.lock();
        let Some(tx) = guard.as_ref() else {
            self.pending.fetch_sub(1, Ordering::AcqRel);
            return Err(EngineError::Other {
                message: "derivation bus channel closed".into(),
            });
        };
        if tx.send(event).is_err() {
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
        self.tx.lock().take();
        self.flush();
        let handles = self.workers.lock().drain(..).collect::<Vec<_>>();
        for h in handles {
            let _ = h.join();
        }
    }
}
