//! Named on-disk counter block for database metadata.

use std::io;

use bincode::{config::standard, decode_from_slice, Decode, Encode};

/// Revision and allocation counters persisted in `counters.bin`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct PersistedCounters {
    /// Highest allocated revision id.
    pub revision: u64,
    /// Next block id to assign.
    pub next_block: u64,
    /// Next snapshot id to assign.
    pub next_snapshot: u64,
    /// Next branch id to assign.
    pub next_branch: u64,
    /// Next asserting `SessionId` to mint (D-P2).
    pub next_session: u32,
}

impl PersistedCounters {
    /// Create a counter snapshot from runtime values.
    pub fn new(
        revision: u64,
        next_block: u64,
        next_snapshot: u64,
        next_branch: u64,
        next_session: u32,
    ) -> Self {
        Self {
            revision,
            next_block,
            next_snapshot,
            next_branch,
            next_session,
        }
    }
}

/// Decode `counters.bin`, migrating legacy positional layouts.
pub fn decode_counters(bytes: &[u8]) -> io::Result<PersistedCounters> {
    if let Ok((c, _)) = decode_from_slice::<PersistedCounters, _>(bytes, standard()) {
        return Ok(c);
    }
    if let Ok((c, _)) = decode_from_slice::<[u64; 4], _>(bytes, standard()) {
        return Ok(PersistedCounters::new(c[0], c[1], c[2], c[3], 1));
    }
    let (c, _): ([u64; 3], _) = decode_from_slice(bytes, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(PersistedCounters::new(c[0], c[1], c[2], 2, 1))
}
