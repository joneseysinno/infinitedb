//! Bulk hyperedge import for translation-scale loads (e.g. STEP geometry).

use std::collections::BTreeSet;

use bincode::{config::standard, encode_to_vec};

use crate::infinitedb_core::{
    address::{Address, RevisionId, SpaceId},
    block::Record,
    endpoint_index::{
        encode_hyperedge_id, endpoint_index_point, edge_endpoints, ENDPOINT_INDEX_SPACE,
    },
    hyperedge::Hyperedge,
};
use crate::infinitedb_storage::wal::{WalDurability, WalEntry};

use super::{InfiniteDb, HYPEREDGE_LOCATOR_SPACE};

/// Default WAL sync interval during bulk import (frames between fsyncs).
pub const DEFAULT_BULK_SYNC_EVERY: usize = 4096;

/// Default in-memory buffer size before auto-flush during bulk import.
pub const DEFAULT_BULK_FLUSH_THRESHOLD: usize = 8192;

/// Tuning for a bulk hyperedge import session.
#[derive(Debug, Clone)]
pub struct BulkHyperedgeImportOptions {
    pub sync_every: usize,
    pub flush_threshold: usize,
    pub build_endpoint_index: bool,
}

impl Default for BulkHyperedgeImportOptions {
    fn default() -> Self {
        Self {
            sync_every: DEFAULT_BULK_SYNC_EVERY,
            flush_threshold: DEFAULT_BULK_FLUSH_THRESHOLD,
            build_endpoint_index: true,
        }
    }
}

/// Statistics returned after a successful bulk import.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkImportResult {
    pub count: usize,
    pub records_written: usize,
    pub first_revision: Option<RevisionId>,
    pub last_revision: Option<RevisionId>,
}

/// Active bulk import session.
pub struct BulkHyperedgeImport<'a> {
    pub(super) db: &'a mut InfiniteDb,
    pub(super) space: SpaceId,
    pub(super) options: BulkHyperedgeImportOptions,
    pub(super) saved_durability: WalDurability,
    pub(super) saved_flush_threshold: usize,
    pub(super) touched_spaces: BTreeSet<u64>,
    pub(super) count: usize,
    pub(super) records_written: usize,
    pub(super) first_revision: Option<RevisionId>,
    pub(super) last_revision: Option<RevisionId>,
    pub(super) pending_index_edges: Vec<Hyperedge>,
    finished: bool,
}

impl<'a> BulkHyperedgeImport<'a> {
    pub fn space(&self) -> SpaceId {
        self.space
    }

    pub fn push(&mut self, mut edge: Hyperedge) -> std::io::Result<RevisionId> {
        edge.validate().map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{:?}", e))
        })?;
        let (rev, n, touched) =
            self.db
                .push_hyperedge_bulk(self.space, &mut edge, self.options.build_endpoint_index)?;
        for id in touched {
            self.touched_spaces.insert(id);
        }
        if !self.options.build_endpoint_index {
            self.pending_index_edges.push(edge);
        }
        self.count += 1;
        self.records_written += n;
        self.first_revision = Some(self.first_revision.unwrap_or(rev));
        self.last_revision = Some(rev);
        Ok(rev)
    }

    pub fn build_endpoint_index(&mut self) -> std::io::Result<usize> {
        let edges = std::mem::take(&mut self.pending_index_edges);
        let mut index_rows = 0usize;
        for edge in &edges {
            index_rows += self.db.index_hyperedge_endpoints_buffered(edge)?;
            self.touched_spaces.insert(ENDPOINT_INDEX_SPACE.0);
        }
        self.records_written += index_rows;
        Ok(index_rows)
    }

    pub fn finish(mut self) -> std::io::Result<BulkImportResult> {
        if !self.pending_index_edges.is_empty() {
            self.build_endpoint_index()?;
        }
        self.db
            .sync_wal()
            .and_then(|_| self.db.finalize_bulk_flush(&self.touched_spaces))
            .and_then(|_| {
                self.db.restore_after_bulk_import(
                    self.saved_durability,
                    self.saved_flush_threshold,
                )
            })?;
        self.finished = true;
        Ok(BulkImportResult {
            count: self.count,
            records_written: self.records_written,
            first_revision: self.first_revision,
            last_revision: self.last_revision,
        })
    }
}

impl Drop for BulkHyperedgeImport<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.db.restore_after_bulk_import(
                self.saved_durability,
                self.saved_flush_threshold,
            );
        }
    }
}

pub(super) struct PreparedHyperedgeWrites {
    pub rows: Vec<(WalEntry, Record)>,
}

impl InfiniteDb {
    pub fn begin_hyperedge_import(
        &mut self,
        space: SpaceId,
    ) -> BulkHyperedgeImport<'_> {
        self.begin_hyperedge_import_with_options(space, BulkHyperedgeImportOptions::default())
    }

    pub fn begin_hyperedge_import_with_options(
        &mut self,
        space: SpaceId,
        options: BulkHyperedgeImportOptions,
    ) -> BulkHyperedgeImport<'_> {
        let saved_durability = self.wal.durability();
        let saved_flush_threshold = self.flush_threshold;
        self.wal.set_durability(WalDurability::Buffered {
            sync_every: options.sync_every.max(1),
        });
        self.flush_threshold = options.flush_threshold;
        self.defer_auto_flush = true;
        BulkHyperedgeImport {
            db: self,
            space,
            options,
            saved_durability,
            saved_flush_threshold,
            touched_spaces: BTreeSet::new(),
            count: 0,
            records_written: 0,
            first_revision: None,
            last_revision: None,
            pending_index_edges: Vec::new(),
            finished: false,
        }
    }

    pub fn insert_hyperedges_bulk<I>(
        &mut self,
        space: SpaceId,
        edges: I,
    ) -> std::io::Result<BulkImportResult>
    where
        I: IntoIterator<Item = Hyperedge>,
    {
        let mut import = self.begin_hyperedge_import(space);
        for edge in edges {
            import.push(edge)?;
        }
        import.finish()
    }

    pub fn insert_hyperedges_bulk_with_options<I>(
        &mut self,
        space: SpaceId,
        edges: I,
        options: BulkHyperedgeImportOptions,
    ) -> std::io::Result<BulkImportResult>
    where
        I: IntoIterator<Item = Hyperedge>,
    {
        let mut import = self.begin_hyperedge_import_with_options(space, options);
        for edge in edges {
            import.push(edge)?;
        }
        import.finish()
    }

    /// Force the WAL buffer to disk (strict durability escape hatch).
    pub fn sync_wal(&mut self) -> std::io::Result<()> {
        self.wal.sync()
    }

    pub(super) fn prepare_hyperedge_writes(
        &self,
        space: SpaceId,
        edge: &Hyperedge,
        build_endpoint_index: bool,
    ) -> std::io::Result<PreparedHyperedgeWrites> {
        let (point, is_centroid) = self.edge_storage_point(space, edge);
        let data = encode_to_vec(edge, standard())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let rev = self.next_revision();
        let address = Address::new(space, point.clone());
        let mut rows = vec![(
            WalEntry::Write {
                address: address.clone(),
                revision: rev,
                data: data.clone(),
            },
            Record {
                address,
                revision: rev,
                data,
                tombstone: false,
            },
        )];

        if is_centroid {
            let locator_data = encode_to_vec(&point, standard())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            let loc_point = super::locator_point(space, edge.id);
            let loc_addr = Address::new(HYPEREDGE_LOCATOR_SPACE, loc_point);
            let loc_rev = self.next_revision();
            rows.push((
                WalEntry::Write {
                    address: loc_addr.clone(),
                    revision: loc_rev,
                    data: locator_data.clone(),
                },
                Record {
                    address: loc_addr,
                    revision: loc_rev,
                    data: locator_data,
                    tombstone: false,
                },
            ));
        }

        if build_endpoint_index {
            for ep in edge_endpoints(edge) {
                let idx_point = endpoint_index_point(ep, edge.id);
                let idx_data = encode_hyperedge_id(edge.id);
                let idx_addr = Address::new(ENDPOINT_INDEX_SPACE, idx_point);
                let idx_rev = self.next_revision();
                rows.push((
                    WalEntry::Write {
                        address: idx_addr.clone(),
                        revision: idx_rev,
                        data: idx_data.clone(),
                    },
                    Record {
                        address: idx_addr,
                        revision: idx_rev,
                        data: idx_data,
                        tombstone: false,
                    },
                ));
            }
        }

        Ok(PreparedHyperedgeWrites { rows })
    }

    pub(super) fn push_hyperedge_bulk(
        &mut self,
        space: SpaceId,
        edge: &mut Hyperedge,
        build_endpoint_index: bool,
    ) -> std::io::Result<(RevisionId, usize, Vec<u64>)> {
        if build_endpoint_index {
            self.ensure_endpoint_index_space()?;
        }
        let (_, is_centroid) = self.edge_storage_point(space, edge);
        if is_centroid {
            self.ensure_locator_space()?;
        }
        let prepared = self.prepare_hyperedge_writes(space, edge, build_endpoint_index)?;
        let n = prepared.rows.len();
        let mut touched = vec![space.0];
        if is_centroid {
            touched.push(HYPEREDGE_LOCATOR_SPACE.0);
        }
        if build_endpoint_index {
            touched.push(ENDPOINT_INDEX_SPACE.0);
        }
        let rev = self.apply_prepared_writes_buffered(prepared)?;
        edge.valid_from = rev;
        #[cfg(feature = "sync")]
        {
            let edge_clone = edge.clone();
            self.enqueue_sync(crate::infinitedb_sync::transport::SyncOperation::WriteHyperedge {
                space,
                edge: edge_clone,
                revision: rev,
            })?;
        }
        Ok((rev, n, touched))
    }

    pub(super) fn apply_prepared_writes_buffered(
        &mut self,
        prepared: PreparedHyperedgeWrites,
    ) -> std::io::Result<RevisionId> {
        let rev = prepared.rows[0].1.revision;
        for (entry, record) in prepared.rows {
            self.wal.append_frame(&entry)?;
            self.buffer.push(record);
        }
        let every = self.wal.durability().sync_every();
        if self.wal.pending_frames() >= every {
            self.wal.sync()?;
        }
        Ok(rev)
    }

    pub(super) fn apply_prepared_writes_strict(
        &mut self,
        prepared: PreparedHyperedgeWrites,
    ) -> std::io::Result<RevisionId> {
        let rev = prepared.rows[0].1.revision;
        for (entry, record) in prepared.rows {
            self.wal.append(&entry)?;
            self.buffer.push(record);
        }
        Ok(rev)
    }

    pub(super) fn index_hyperedge_endpoints_buffered(
        &mut self,
        edge: &Hyperedge,
    ) -> std::io::Result<usize> {
        self.ensure_endpoint_index_space()?;
        let mut n = 0usize;
        for ep in edge_endpoints(edge) {
            let point = endpoint_index_point(ep, edge.id);
            let data = encode_hyperedge_id(edge.id);
            let address = Address::new(ENDPOINT_INDEX_SPACE, point);
            let rev = self.next_revision();
            self.wal.append_frame(&WalEntry::Write {
                address: address.clone(),
                revision: rev,
                data: data.clone(),
            })?;
            self.buffer.push(Record {
                address,
                revision: rev,
                data,
                tombstone: false,
            });
            n += 1;
        }
        let every = self.wal.durability().sync_every();
        if self.wal.pending_frames() >= every {
            self.wal.sync()?;
        }
        Ok(n)
    }

    pub(super) fn finalize_bulk_flush(
        &mut self,
        touched_spaces: &BTreeSet<u64>,
    ) -> std::io::Result<()> {
        for &space_id in touched_spaces {
            self.flush(SpaceId(space_id))?;
        }
        Ok(())
    }

    pub(super) fn restore_after_bulk_import(
        &mut self,
        durability: WalDurability,
        flush_threshold: usize,
    ) -> std::io::Result<()> {
        self.wal.set_durability(durability);
        self.flush_threshold = flush_threshold;
        self.defer_auto_flush = false;
        Ok(())
    }
}
