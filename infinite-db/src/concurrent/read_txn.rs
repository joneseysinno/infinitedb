//! Point-in-time read transaction.
//!
//! Pin a concurrent read view at per-session stable ceilings so queries do not
//! observe in-flight writes. Obtain one from [`InfiniteDb::read`].
//!
//! Repeatable read: no admitted session's stable component moves under the
//! pinned vector for the lifetime of this transaction.
//!
//! ```no_run
//! use infinite_db::InfiniteDb;
//! use infinite_db::infinitedb_core::address::SpaceId;
//! use infinite_db::EngineError;
//!
//! # fn example(db: &InfiniteDb) -> Result<(), EngineError> {
//! let txn = db.read();
//! let rows = txn.query(SpaceId(1))?;
//! # Ok(())
//! # }
//! ```

use crate::engine::error::EngineError;
use crate::engine::query::{query_inner, space_key};
use crate::engine::session::VersionVector;
use crate::infinitedb_core::{
    address::{DimensionVector, RevisionId, SpaceId},
    block::Record,
    branch::BranchId,
    query::Query,
};
use crate::InfiniteDb;

/// How this read view resolves revision ceilings.
#[derive(Debug, Clone)]
enum ReadVisibility {
    /// Per-session pin captured at open (D-P6).
    Vector(VersionVector),
    /// Explicit scalar ceiling — meaningful within one epoch/stream only.
    Scalar(RevisionId),
}

/// Concurrent read view pinned at per-session stable revisions.
pub struct ReadTxn<'a> {
    db: &'a InfiniteDb,
    branch: BranchId,
    visibility: ReadVisibility,
    /// Snapshot at open — always available for inspection.
    captured_vector: VersionVector,
}

impl<'a> ReadTxn<'a> {
    /// Pin reads at each admitted session's stable revision on `main`.
    ///
    /// Queries through this view are repeatable: the visible record set cannot
    /// change while the transaction is held.
    pub fn new(db: &'a InfiniteDb) -> Self {
        let captured_vector = db.capture_version_vector();
        Self {
            db,
            branch: BranchId::MAIN,
            visibility: ReadVisibility::Vector(captured_vector.clone()),
            captured_vector,
        }
    }

    /// Per-session stable ceilings captured at transaction open.
    pub fn version_vector(&self) -> &VersionVector {
        &self.captured_vector
    }

    /// Read through a branch overlay instead of `main`.
    pub fn on_branch(mut self, branch: BranchId) -> Self {
        self.branch = branch;
        self
    }

    /// Switch to explicit scalar ceiling mode (within-one-stream caveat applies).
    pub fn as_of(mut self, rev: RevisionId) -> Self {
        self.visibility = ReadVisibility::Scalar(rev);
        self
    }

    fn pinned_vector(&self) -> Option<&VersionVector> {
        match &self.visibility {
            ReadVisibility::Vector(v) => Some(v),
            ReadVisibility::Scalar(_) => None,
        }
    }

    fn scalar_as_of(&self) -> Option<RevisionId> {
        match &self.visibility {
            ReadVisibility::Vector(_) => None,
            ReadVisibility::Scalar(r) => Some(*r),
        }
    }

    /// Query all live records in `space` within this read view.
    pub fn query(&self, space: SpaceId) -> Result<Vec<Record>, EngineError> {
        self.db.query_on_branch_pinned(
            self.branch,
            space,
            self.scalar_as_of(),
            self.pinned_vector(),
        )
    }

    /// Bounding-box query within this read view.
    pub fn query_bbox(
        &self,
        space: SpaceId,
        min: DimensionVector,
        max: DimensionVector,
    ) -> Result<Vec<Record>, EngineError> {
        self.db.query_bbox_on_branch_pinned(
            self.branch,
            space,
            min,
            max,
            self.scalar_as_of(),
            self.pinned_vector(),
        )
    }

    /// Execute a [`Query`] descriptor against this read view.
    pub fn execute(&self, q: &Query) -> Result<Vec<Record>, EngineError> {
        let as_of = q.as_of.or(self.scalar_as_of());
        let pinned = self.pinned_vector();
        let spaces = self.db.spaces.read();
        let key_range = match q.key_range {
            Some(kr) => Some(kr),
            None => q.range.as_ref().map(|r| {
                let ka = space_key(&spaces, q.space, &r.min);
                let kb = space_key(&spaces, q.space, &r.max);
                if ka <= kb {
                    (ka, kb)
                } else {
                    (kb, ka)
                }
            }),
        };

        let ctx = self.db.query_ctx();
        let branch_id = if self.branch == BranchId::MAIN {
            None
        } else {
            Some(self.branch)
        };
        let mut results = query_inner(
            &self.db.store,
            &self.db.snapshots,
            None,
            &spaces,
            &self.db.session_watermarks,
            q.space,
            key_range,
            as_of,
            pinned,
            q.include_tombstones,
            Some(ctx.hilbert_tails),
            Some(&self.db.branch_overlays),
            branch_id,
        )
        .map_err(EngineError::from)?;

        if let Some(range) = &q.range {
            results.retain(|r| r.address.point.within(&range.min, &range.max));
        }

        Ok(results)
    }
}
