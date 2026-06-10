//! Cluster metadata (`meta/cluster.bin`, format v5 additive).

use std::io;
use std::path::Path;

use bincode::{config::standard, decode_from_slice, encode_to_vec, Decode, Encode};

/// Persisted node identity and sync vector (Phase D).
#[derive(Debug, Clone, Default, Encode, Decode)]
pub struct ClusterMeta {
    pub node_id: u64,
    pub peers: Vec<String>,
    /// Last applied replication revision per remote peer id.
    pub sync_vector: Vec<(u64, u64)>,
}

impl ClusterMeta {
    pub fn read_from_meta(meta_dir: &Path) -> io::Result<Option<Self>> {
        let path = meta_dir.join("cluster.bin");
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(path)?;
        let (meta, _) = decode_from_slice(&bytes, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(Some(meta))
    }

    pub fn write_to_meta(&self, meta_dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(meta_dir)?;
        let path = meta_dir.join("cluster.bin");
        let tmp = path.with_extension("tmp");
        let bytes = encode_to_vec(self, standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, path)
    }
}
