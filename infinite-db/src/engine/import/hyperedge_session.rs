//! Hyperedge bulk import session with applicative validation.

use crate::engine::error::EngineError;
use crate::engine::watermark::RevisionRange;
use crate::infinitedb_core::{
    address::SpaceId,
    hyperedge::{Hyperedge, HyperedgeValidationError},
};

/// Error classification for import diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ImportErrorClass {
    Validation,
    Catalog,
    Other,
}

/// One import validation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportErrorEntry {
    pub index: usize,
    pub edge_id: u64,
    pub class: ImportErrorClass,
    pub message: String,
}

/// Monoid of import errors — merged logs sort by (index, class) for determinism.
#[derive(Debug, Clone, Default)]
pub struct ImportErrorLog {
    entries: Vec<ImportErrorEntry>,
}

impl ImportErrorLog {
    pub fn push(&mut self, entry: ImportErrorEntry) {
        self.entries.push(entry);
    }

    pub fn merge(mut self, other: Self) -> Self {
        self.entries.extend(other.entries);
        self.entries.sort_by(|a, b| {
            (a.index, a.class, &a.edge_id).cmp(&(b.index, b.class, &b.edge_id))
        });
        self
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[ImportErrorEntry] {
        &self.entries
    }

    pub fn error_rate(&self, records_seen: u64) -> f64 {
        if records_seen == 0 {
            0.0
        } else {
            self.entries.len() as f64 / records_seen as f64
        }
    }
}

/// Shard-invariant error budget (errors per N records).
#[derive(Debug, Clone, Copy)]
pub struct ImportBudget {
    /// Maximum allowed error rate = max_errors / sample_every records.
    pub max_errors: usize,
    pub sample_every: u64,
}

impl Default for ImportBudget {
    fn default() -> Self {
        Self {
            max_errors: 100,
            sample_every: 1000,
        }
    }
}

impl ImportBudget {
    pub fn is_over_budget(&self, log: &ImportErrorLog, records_seen: u64) -> bool {
        if records_seen == 0 {
            return false;
        }
        let allowed = (records_seen / self.sample_every) as usize * self.max_errors
            + self.max_errors;
        log.len() > allowed
    }
}

/// Result of a committed import session.
#[derive(Debug, Clone)]
pub struct HyperedgeImportResult {
    pub admitted: RevisionRange,
    pub errors: ImportErrorLog,
    pub aborted: bool,
}

/// Queued hyperedge awaiting commit.
pub(crate) struct QueuedEdge {
    pub edge: Hyperedge,
}

/// In-memory import session — validates applicatively, commits atomically.
pub struct HyperedgeImportSession {
    pub(crate) space: SpaceId,
    budget: ImportBudget,
    queued: Vec<QueuedEdge>,
    errors: ImportErrorLog,
    records_seen: u64,
    aborted: bool,
}

impl HyperedgeImportSession {
    pub(crate) fn new(space: SpaceId, budget: ImportBudget) -> Self {
        Self {
            space,
            budget,
            queued: Vec::new(),
            errors: ImportErrorLog::default(),
            records_seen: 0,
            aborted: false,
        }
    }

    pub fn space(&self) -> SpaceId {
        self.space
    }

    pub fn errors(&self) -> &ImportErrorLog {
        &self.errors
    }

    pub fn push(&mut self, edge: Hyperedge) -> Result<(), EngineError> {
        if self.aborted {
            return Err(EngineError::Other {
                message: "import session aborted".into(),
            });
        }
        let index = self.queued.len();
        self.records_seen += 1;
        if let Err(e) = edge.validate() {
            self.record_validation_error(index, edge.id.0, e);
            return Ok(());
        }
        self.queued.push(QueuedEdge { edge });
        if self.budget.is_over_budget(&self.errors, self.records_seen) {
            self.aborted = true;
        }
        Ok(())
    }

    fn record_validation_error(
        &mut self,
        index: usize,
        edge_id: u64,
        err: HyperedgeValidationError,
    ) {
        self.errors.push(ImportErrorEntry {
            index,
            edge_id,
            class: ImportErrorClass::Validation,
            message: format!("{err:?}"),
        });
    }

    pub fn is_aborted(&self) -> bool {
        self.aborted
    }

    pub fn is_over_budget(&self) -> bool {
        self.budget.is_over_budget(&self.errors, self.records_seen)
    }

    pub fn records_seen(&self) -> u64 {
        self.records_seen
    }

    pub(crate) fn take_queued(&mut self) -> Vec<QueuedEdge> {
        std::mem::take(&mut self.queued)
    }

    pub(crate) fn take_errors(self) -> ImportErrorLog {
        self.errors
    }
}
