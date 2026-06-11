//! Storage-layer error algebra.

use std::io;
use std::path::PathBuf;

/// Physical durability failures: segment IO, corruption, fsync, WAL frames.
#[derive(Debug, Clone)]
pub enum StorageError {
    /// Transient or persistent IO failure.
    Io {
        kind: io::ErrorKind,
        message: String,
        path: Option<PathBuf>,
    },
    /// Block or WAL checksum / decode failure.
    Corruption {
        message: String,
        path: Option<PathBuf>,
    },
    /// WAL frame encoding or recovery error.
    WalFrame {
        message: String,
    },
}

impl StorageError {
    pub fn from_io(err: io::Error, path: Option<PathBuf>) -> Self {
        StorageError::Io {
            kind: err.kind(),
            message: err.to_string(),
            path,
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            StorageError::Io {
                kind: io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock,
                ..
            }
        )
    }

    pub fn is_caller_correctable(&self) -> bool {
        false
    }

    pub fn is_fatal(&self) -> bool {
        matches!(self, StorageError::Corruption { .. })
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Io { message, path, .. } => {
                if let Some(p) = path {
                    write!(f, "io error at {}: {message}", p.display())
                } else {
                    write!(f, "io error: {message}")
                }
            }
            StorageError::Corruption { message, path } => {
                if let Some(p) = path {
                    write!(f, "corruption at {}: {message}", p.display())
                } else {
                    write!(f, "corruption: {message}")
                }
            }
            StorageError::WalFrame { message } => write!(f, "wal frame error: {message}"),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<io::Error> for StorageError {
    fn from(err: io::Error) -> Self {
        StorageError::from_io(err, None)
    }
}

impl From<StorageError> for io::Error {
    fn from(err: StorageError) -> Self {
        let kind = match &err {
            StorageError::Io { kind, .. } => *kind,
            StorageError::Corruption { .. } => io::ErrorKind::InvalidData,
            StorageError::WalFrame { .. } => io::ErrorKind::InvalidData,
        };
        io::Error::new(kind, err.to_string())
    }
}
