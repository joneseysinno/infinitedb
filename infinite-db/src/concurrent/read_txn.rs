//! Point-in-time read transaction.
//!
//! Pin a concurrent read view at a revision ceiling so queries do not observe
//! in-flight writes. Obtain one from [`InfiniteDb::read`].
//!
//! ```no_run
//! use infinite_db::InfiniteDb;
//! use infinite_db::infinitedb_core::address::SpaceId;
//!
//! # fn example(db: &InfiniteDb) -> std::io::Result<()> {
//! let txn = db.read();
//! let rows = txn.query(SpaceId(1))?;
//! # Ok(())
//! # }
//! ```

use crate::engine::query::{query_inner, space_key};
use crate::infinitedb_core::{
    address::{DimensionVector, RevisionId, SpaceId},
    block::Record,
    branch::BranchId,
    query::Query,
};
use crate::InfiniteDb;

/// Concurrent read view pinned at a revision ceiling.
pub struct ReadTxn<'a> {
    db: &'a InfiniteDb,
    branch: BranchId,
    as_of: Option<RevisionId>,
}

impl<'a> ReadTxn<'a> {
    /// Pin reads at the current database revision on `main`.
    pub fn new(db: &'a InfiniteDb) -> Self {
        let as_of = Some(RevisionId(db.revision()));
        Self {
            db,
            branch: BranchId::MAIN,
            as_of,
        }
    }

    /// Read through a branch overlay instead of `main`.
    pub fn on_branch(mut self, branch: BranchId) -> Self {
        self.branch = branch;
        self
    }

    /// Override the revision ceiling for this read view.
    pub fn as_of(mut self, rev: RevisionId) -> Self {
        self.as_of = Some(rev);
        self
    }

    /// Query all live records in `space` within this read view.
    pub fn query(&self, space: SpaceId) -> std::io::Result<Vec<Record>> {
        self.db.query_on_branch(self.branch, space, self.as_of)
    }

    /// Bounding-box query within this read view.
    pub fn query_bbox(
        &self,
        space: SpaceId,
        min: DimensionVector,
        max: DimensionVector,
    ) -> std::io::Result<Vec<Record>> {
        self.db
            .query_bbox_on_branch(self.branch, space, min, max, self.as_of)
    }

    /// Execute a [`Query`] descriptor against this read view.
    ///
    /// When `q.range` is set, an exact per-record coordinate filter is applied
    /// so results match [`InfiniteDb::query_bbox`].
    pub fn execute(&self, q: &Query) -> std::io::Result<Vec<Record>> {
        let as_of = q.as_of.or(self.as_of);
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
            ctx.live_tail,
            ctx.space_tails,
            &spaces,
            &self.db.revision,
            q.space,
            key_range,
            as_of,
            q.include_tombstones,
            ctx.hilbert_tails,
            Some(&self.db.branch_overlays),
            branch_id,
        )?;

        if let Some(range) = &q.range {
            results.retain(|r| r.address.point.within(&range.min, &range.max));
        }

        Ok(results)
    }
}
