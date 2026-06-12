//! On-disk format version gate.

use std::io;
use std::path::Path;

/// HLC revision wire + Hilbert shards + branch overlays (current).
pub const FORMAT_VERSION_V5: u32 = 5;

/// Hilbert shards + branch overlays.
pub const FORMAT_VERSION_V4: u32 = 4;

/// Persisted format marker written to `meta/format_version.bin`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormatVersion(pub u32);

impl FormatVersion {
    pub fn v4() -> Self {
        Self(FORMAT_VERSION_V4)
    }

    pub fn v5() -> Self {
        Self(FORMAT_VERSION_V5)
    }

    pub fn is_supported(self) -> bool {
        matches!(self.0, FORMAT_VERSION_V4 | FORMAT_VERSION_V5)
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
