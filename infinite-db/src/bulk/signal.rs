//! Bulk signal sample import.

use bincode::{config::standard, encode_to_vec};

use crate::infinitedb_core::{
    address::{DimensionVector, RevisionId, SpaceId},
    signal::SignalSample,
};

use super::session::{BulkSessionCore, BulkWriteOptions, BulkWriteResult};
use super::super::InfiniteDb;

/// Bulk insert/delete session for [`SignalSample`] values in one space.
pub struct BulkSignalImport<'a> {
    session: BulkSessionCore<'a>,
    space: SpaceId,
}

impl<'a> BulkSignalImport<'a> {
    pub fn space(&self) -> SpaceId {
        self.space
    }

    pub fn push(&mut self, sample: SignalSample) -> std::io::Result<RevisionId> {
        sample.validate().map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{:?}", e))
        })?;
        let full_coords = sample
            .scope
            .address_coords(&sample.local_coords)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{:?}", e)))?;
        if let Some(cfg) = self.session.db.spaces.get(self.space) {
            if cfg.dims != full_coords.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "signal coordinates do not match space dimensions",
                ));
            }
        }
        let data = encode_to_vec(&sample, standard())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let rev = self
            .session
            .push_write(self.space, DimensionVector::new(full_coords), data)?;
        #[cfg(feature = "sync")]
        {
            let sample_clone = sample;
            self.session.defer_sync(crate::infinitedb_sync::transport::SyncOperation::WriteSignal {
                space: self.space,
                sample: sample_clone,
                revision: rev,
            });
        }
        Ok(rev)
    }

    /// Logical delete: tombstone at the sample's resolved address.
    pub fn push_delete(&mut self, sample: SignalSample) -> std::io::Result<RevisionId> {
        sample.validate().map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{:?}", e))
        })?;
        let full_coords = sample
            .scope
            .address_coords(&sample.local_coords)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{:?}", e)))?;
        self.session
            .push_tombstone(self.space, DimensionVector::new(full_coords))
    }

    pub fn finish(self) -> std::io::Result<BulkWriteResult> {
        self.session.finish()
    }
}

impl InfiniteDb {
    /// Begin a buffered bulk signal import into `space`.
    pub fn begin_signal_import(&mut self, space: SpaceId) -> std::io::Result<BulkSignalImport<'_>> {
        self.begin_signal_import_with_options(space, BulkWriteOptions::default())
    }

    pub fn begin_signal_import_with_options(
        &mut self,
        space: SpaceId,
        options: BulkWriteOptions,
    ) -> std::io::Result<BulkSignalImport<'_>> {
        Ok(BulkSignalImport {
            session: BulkSessionCore::begin(self, options)?,
            space,
        })
    }

    pub fn insert_signals_bulk<I>(
        &mut self,
        space: SpaceId,
        samples: I,
    ) -> std::io::Result<BulkWriteResult>
    where
        I: IntoIterator<Item = SignalSample>,
    {
        let mut import = self.begin_signal_import(space)?;
        for sample in samples {
            import.push(sample)?;
        }
        import.finish()
    }

    pub fn delete_signals_bulk<I>(
        &mut self,
        space: SpaceId,
        samples: I,
    ) -> std::io::Result<BulkWriteResult>
    where
        I: IntoIterator<Item = SignalSample>,
    {
        let mut import = self.begin_signal_import(space)?;
        for sample in samples {
            import.push_delete(sample)?;
        }
        import.finish()
    }
}
