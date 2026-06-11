//! On-disk format version gate.

use std::io;
use std::path::Path;

/// Networked multi-master cluster metadata (`meta/cluster.bin`).
pub const FORMAT_VERSION_V5: u32 = 5;

/// Hilbert shards + branch overlays (`spaces/<id>/shards/<shard>/hot.seg`).
pub const FORMAT_VERSION_V4: u32 = 4;

/// Per-space I/O threads (`spaces/<id>/hot.seg` + staging WAL).
pub const FORMAT_VERSION_V3: u32 = 3;

/// Single global I/O thread (`hot/<id>.seg` at db root).
pub const FORMAT_VERSION_V2: u32 = 2;

/// Legacy single-threaded layout (`wal.log` at db root).
pub const FORMAT_VERSION_V1: u32 = 1;

/// Persisted format marker written to `meta/format_version.bin`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormatVersion(pub u32);

impl FormatVersion {
    /// Intra-space Hilbert sharding + branch merge (Phase C).
    pub fn v4() -> Self {
        Self(FORMAT_VERSION_V4)
    }

    /// Per-space parallel writes (Phase B).
    pub fn v3() -> Self {
        Self(FORMAT_VERSION_V3)
    }

    /// Single I/O thread layout (Phase A).
    pub fn v2() -> Self {
        Self(FORMAT_VERSION_V2)
    }

    /// Original on-disk format v1 WAL layout (opening no longer supported).
    pub fn v1() -> Self {
        Self(FORMAT_VERSION_V1)
    }

    /// Read `meta/format_version.bin` when present.
    pub fn read_from_meta(meta_dir: &Path) -> io::Result<Option<Self>> {
        let path = meta_dir.join("format_version.bin");
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(path)?;
        if bytes.len() != 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "format_version.bin must be 4 bytes",
            ));
        }
        Ok(Some(Self(u32::from_le_bytes(bytes.try_into().unwrap()))))
    }

    /// Write `meta/format_version.bin`.
    pub fn write_to_meta(&self, meta_dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(meta_dir)?;
        let path = meta_dir.join("format_version.bin");
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, self.0.to_le_bytes())?;
        std::fs::rename(&tmp, path)
    }
}
