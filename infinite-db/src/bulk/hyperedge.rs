//! Bulk hyperedge import and delete.

use bincode::{config::standard, encode_to_vec};

use crate::infinitedb_core::{
    address::{Address, RevisionId, SpaceId},
    block::Record,
    endpoint_index::{
        encode_hyperedge_id, endpoint_index_point, edge_endpoints, ENDPOINT_INDEX_SPACE,
    },
    hyperedge::{Hyperedge, HyperedgeId},
};
use crate::infinitedb_storage::wal::WalEntry;

use super::session::{
    BulkImportResult, BulkSessionCore, BulkWriteOptions, BulkWriteResult, DEFAULT_BULK_FLUSH_THRESHOLD,
    DEFAULT_BULK_SYNC_EVERY,
};
use super::super::{hyperedge_point, locator_point, InfiniteDb, HYPEREDGE_LOCATOR_SPACE};

/// Tuning for bulk hyperedge import.
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

impl BulkHyperedgeImportOptions {
    pub fn write_options(&self) -> BulkWriteOptions {
        BulkWriteOptions {
            sync_every: self.sync_every,
            flush_threshold: self.flush_threshold,
        }
    }
}

/// Bulk hyperedge insert/delete session.
///
/// Only one bulk session per [`InfiniteDb`] at a time. Call [`Self::finish`] before drop.
pub struct BulkHyperedgeImport<'a> {
    session: BulkSessionCore<'a>,
    space: SpaceId,
    build_endpoint_index: bool,
    pending_index_edges: Vec<Hyperedge>,
}

impl<'a> BulkHyperedgeImport<'a> {
    pub fn space(&self) -> SpaceId {
        self.space
    }

    pub fn push(&mut self, mut edge: Hyperedge) -> std::io::Result<RevisionId> {
        edge.validate().map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{:?}", e))
        })?;
        let rev = push_hyperedge_bulk_session(
            &mut self.session,
            self.space,
            &mut edge,
            self.build_endpoint_index,
        )?;
        if !self.build_endpoint_index {
            self.pending_index_edges.push(edge);
        }
        Ok(rev)
    }

    pub fn push_delete(&mut self, id: HyperedgeId) -> std::io::Result<RevisionId> {
        delete_hyperedge_bulk_session(&mut self.session, self.space, id)
    }

    pub fn build_endpoint_index(&mut self) -> std::io::Result<usize> {
        let edges = std::mem::take(&mut self.pending_index_edges);
        let mut index_rows = 0usize;
        for edge in &edges {
            index_rows += index_hyperedge_endpoints_session(&mut self.session, edge)?;
            self.session.touch_space(ENDPOINT_INDEX_SPACE);
        }
        Ok(index_rows)
    }

    pub fn finish(mut self) -> std::io::Result<BulkImportResult> {
        if !self.pending_index_edges.is_empty() {
            self.build_endpoint_index()?;
        }
        self.session.finish()
    }
}

fn push_hyperedge_bulk_session(
    session: &mut BulkSessionCore<'_>,
    space: SpaceId,
    edge: &mut Hyperedge,
    build_endpoint_index: bool,
) -> std::io::Result<RevisionId> {
    let db = &mut *session.db;
    if build_endpoint_index {
        db.ensure_endpoint_index_space()?;
    }
    let (_, is_centroid) = db.edge_storage_point(space, edge);
    if is_centroid {
        db.ensure_locator_space()?;
    }
    let rows = db.prepare_hyperedge_writes(space, edge, build_endpoint_index)?;
    session.touch_space(space);
    if is_centroid {
        session.touch_space(HYPEREDGE_LOCATOR_SPACE);
    }
    if build_endpoint_index {
        session.touch_space(ENDPOINT_INDEX_SPACE);
    }
    let rev = session.push_rows(rows)?;
    edge.valid_from = rev;
    #[cfg(feature = "sync")]
    session.defer_sync(crate::infinitedb_sync::transport::SyncOperation::WriteHyperedge {
        space,
        edge: edge.clone(),
        revision: rev,
    });
    Ok(rev)
}

fn delete_hyperedge_bulk_session(
    session: &mut BulkSessionCore<'_>,
    space: SpaceId,
    id: HyperedgeId,
) -> std::io::Result<RevisionId> {
    let (rows, touch) = {
        let db = &mut *session.db;
        let edge = db.fetch_hyperedge_by_id(space, id, None)?;
        let point = match &edge {
            Some(e) => db.edge_storage_point(space, e).0,
            None => hyperedge_point(id),
        };
        let mut rows = Vec::new();
        let mut touch = vec![space];
        if let Some(e) = &edge {
            db.ensure_endpoint_index_space()?;
            for ep in edge_endpoints(e) {
                let idx_point = endpoint_index_point(ep, e.id);
                let rev = db.next_revision();
                let address = Address::new(ENDPOINT_INDEX_SPACE, idx_point);
                rows.push((
                    WalEntry::Tombstone {
                        address: address.clone(),
                        revision: rev,
                    },
                    Record {
                        address,
                        revision: rev,
                        data: vec![],
                        tombstone: true,
                    },
                ));
            }
            touch.push(ENDPOINT_INDEX_SPACE);
        }
        let main_rev = db.next_revision();
        let main_addr = Address::new(space, point);
        rows.push((
            WalEntry::Tombstone {
                address: main_addr.clone(),
                revision: main_rev,
            },
            Record {
                address: main_addr,
                revision: main_rev,
                data: vec![],
                tombstone: true,
            },
        ));
        if edge.is_some() && db.uses_centroid_keying(space) {
            db.ensure_locator_space()?;
            let loc_rev = db.next_revision();
            let loc_point = locator_point(space, id);
            let loc_addr = Address::new(HYPEREDGE_LOCATOR_SPACE, loc_point);
            rows.push((
                WalEntry::Tombstone {
                    address: loc_addr.clone(),
                    revision: loc_rev,
                },
                Record {
                    address: loc_addr,
                    revision: loc_rev,
                    data: vec![],
                    tombstone: true,
                },
            ));
            touch.push(HYPEREDGE_LOCATOR_SPACE);
        }
        (rows, touch)
    };
    let n = rows.len();
    let rev = session.push_rows_only(rows)?;
    session.record_operation(rev, n);
    for s in touch {
        session.touch_space(s);
    }
    #[cfg(feature = "sync")]
    session.defer_sync(crate::infinitedb_sync::transport::SyncOperation::DeleteHyperedge {
        space,
        edge_id: id,
        revision: rev,
    });
    Ok(rev)
}

fn index_hyperedge_endpoints_session(
    session: &mut BulkSessionCore<'_>,
    edge: &Hyperedge,
) -> std::io::Result<usize> {
    let db = &mut *session.db;
    db.ensure_endpoint_index_space()?;
    let mut rows = Vec::new();
    for ep in edge_endpoints(edge) {
        let idx_point = endpoint_index_point(ep, edge.id);
        let idx_data = encode_hyperedge_id(edge.id);
        let idx_addr = Address::new(ENDPOINT_INDEX_SPACE, idx_point);
        let idx_rev = db.next_revision();
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
    if rows.is_empty() {
        return Ok(0);
    }
    let n = rows.len();
    session.push_rows(rows)?;
    Ok(n)
}

impl InfiniteDb {
    pub fn begin_hyperedge_import(
        &mut self,
        space: SpaceId,
    ) -> std::io::Result<BulkHyperedgeImport<'_>> {
        self.begin_hyperedge_import_with_options(space, BulkHyperedgeImportOptions::default())
    }

    pub fn begin_hyperedge_import_with_options(
        &mut self,
        space: SpaceId,
        options: BulkHyperedgeImportOptions,
    ) -> std::io::Result<BulkHyperedgeImport<'_>> {
        let build_endpoint_index = options.build_endpoint_index;
        let session = BulkSessionCore::begin(self, options.write_options())?;
        Ok(BulkHyperedgeImport {
            session,
            space,
            build_endpoint_index,
            pending_index_edges: Vec::new(),
        })
    }

    pub fn insert_hyperedges_bulk<I>(
        &mut self,
        space: SpaceId,
        edges: I,
    ) -> std::io::Result<BulkWriteResult>
    where
        I: IntoIterator<Item = Hyperedge>,
    {
        let mut import = self.begin_hyperedge_import(space)?;
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
    ) -> std::io::Result<BulkWriteResult>
    where
        I: IntoIterator<Item = Hyperedge>,
    {
        let mut import = self.begin_hyperedge_import_with_options(space, options)?;
        for edge in edges {
            import.push(edge)?;
        }
        import.finish()
    }

    pub fn delete_hyperedges_bulk<I>(
        &mut self,
        space: SpaceId,
        ids: I,
    ) -> std::io::Result<BulkWriteResult>
    where
        I: IntoIterator<Item = HyperedgeId>,
    {
        let mut import = self.begin_hyperedge_import(space)?;
        for id in ids {
            import.push_delete(id)?;
        }
        import.finish()
    }

    pub(crate) fn prepare_hyperedge_writes(
        &self,
        space: SpaceId,
        edge: &Hyperedge,
        build_endpoint_index: bool,
    ) -> std::io::Result<Vec<(WalEntry, Record)>> {
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
            let loc_point = locator_point(space, edge.id);
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

        Ok(rows)
    }

}
