//! Bulk record import for translation-scale loads.

use crate::infinitedb_core::address::{DimensionVector, RevisionId, SpaceId};

use super::session::{BulkSessionCore, BulkWriteOptions, BulkWriteResult};
use super::super::LegacyDb;

/// Bulk insert/delete session for raw records in one space.
///
/// Only one bulk session may be active per [`LegacyDb`] at a time.
/// Parallelize parsing upstream; use a single thread for `push`.
pub struct BulkRecordImport<'a> {
    session: BulkSessionCore<'a>,
    space: SpaceId,
}

impl<'a> BulkRecordImport<'a> {
    pub fn space(&self) -> SpaceId {
        self.space
    }

    pub fn push(&mut self, point: DimensionVector, data: Vec<u8>) -> std::io::Result<RevisionId> {
        self.session.push_write(self.space, point, data)
    }

    pub fn push_delete(&mut self, point: DimensionVector) -> std::io::Result<RevisionId> {
        self.session.push_tombstone(self.space, point)
    }

    pub fn finish(self) -> std::io::Result<BulkWriteResult> {
        self.session.finish()
    }
}

impl LegacyDb {
    /// Begin a buffered bulk record import into `space`.
    pub fn begin_record_import(&mut self, space: SpaceId) -> std::io::Result<BulkRecordImport<'_>> {
        self.begin_record_import_with_options(space, BulkWriteOptions::default())
    }

    /// Begin bulk record import with explicit WAL/flush tuning.
    pub fn begin_record_import_with_options(
        &mut self,
        space: SpaceId,
        options: BulkWriteOptions,
    ) -> std::io::Result<BulkRecordImport<'_>> {
        Ok(BulkRecordImport {
            session: BulkSessionCore::begin(self, options)?,
            space,
        })
    }

    /// Import many records in one bulk session.
    pub fn insert_records_bulk<I>(
        &mut self,
        space: SpaceId,
        rows: I,
    ) -> std::io::Result<BulkWriteResult>
    where
        I: IntoIterator<Item = (DimensionVector, Vec<u8>)>,
    {
        let mut import = self.begin_record_import(space)?;
        for (point, data) in rows {
            import.push(point, data)?;
        }
        import.finish()
    }

    /// Tombstone many record addresses in one bulk session.
    pub fn delete_records_bulk<I>(
        &mut self,
        space: SpaceId,
        points: I,
    ) -> std::io::Result<BulkWriteResult>
    where
        I: IntoIterator<Item = DimensionVector>,
    {
        let mut import = self.begin_record_import(space)?;
        for point in points {
            import.push_delete(point)?;
        }
        import.finish()
    }
}
