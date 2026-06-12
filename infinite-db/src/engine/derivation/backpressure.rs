//! Derivation bus backpressure policy.

/// Thresholds governing derivation lag and write throttling.
#[derive(Debug, Clone)]
pub struct DerivationBackpressurePolicy {
    /// Reject new assertion writes when pending bus events exceed this count.
    pub max_pending_tasks: usize,
    /// Reject when `allocated_revision - min_subscriber_watermark` exceeds this.
    pub max_derivation_lag: u64,
    /// Worker pool size cap when widening under delta pressure.
    pub max_worker_threads: usize,
}

impl Default for DerivationBackpressurePolicy {
    fn default() -> Self {
        Self {
            max_pending_tasks: 10_000,
            max_derivation_lag: 50_000,
            max_worker_threads: 8,
        }
    }
}

/// Runtime counters for observability and backpressure decisions.
#[derive(Debug, Clone, Default)]
pub struct DerivationStats {
    pub pending_tasks: usize,
    pub events_processed: u64,
    pub derived_rows_written: u64,
    pub backpressure_rejections: u64,
    pub derivation_failures: u64,
    pub outstanding_derivations: usize,
}
