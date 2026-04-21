use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use super::address::{Address, RevisionId, SpaceId};

/// Stable identifier for a block on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct BlockId(pub u64);

/// A single record: a spatial address, a revision, raw serialized data,
/// and a tombstone flag (true = this record has been logically deleted).
/// Records are immutable once written — updates append new revisions.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct Record {
    pub address: Address,
    pub revision: RevisionId,
    /// Serialized payload (bincode-encoded). Empty when `tombstone` is true.
    pub data: Vec<u8>,
    /// Marks this revision as a logical deletion.
    pub tombstone: bool,
}

/// An immutable, sorted collection of records sharing a contiguous Hilbert
/// key range. Once written, a block is never mutated — compaction produces
/// replacement blocks.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct Block {
    pub id: BlockId,
    pub space: SpaceId,
    /// Records sorted by (address.point, revision) ascending.
    pub records: Vec<Record>,
    /// Revision range covered — used to skip blocks during history queries.
    pub min_revision: RevisionId,
    pub max_revision: RevisionId,
    /// Blake3 checksum of the serialized records for integrity verification.
    pub checksum: [u8; 32],
}

impl Block {
    /// Returns true if this block contains any live (non-tombstoned) records
    /// at or before the given revision.
    pub fn has_live_records_at(&self, rev: RevisionId) -> bool {
        self.records
            .iter()
            .any(|r| r.revision <= rev && !r.tombstone)
    }

    /// Returns the latest record at the given address at or before `rev`,
    /// if one exists and is not a tombstone.
    pub fn get_at(&self, address: &Address, rev: RevisionId) -> Option<&Record> {
        self.records
            .iter()
            .filter(|r| &r.address == address && r.revision <= rev)
            .max_by_key(|r| r.revision)
            .filter(|r| !r.tombstone)
    }
}

/// A named grouping of blocks within a space — analogous to a table.
/// A Relation does not own blocks; the storage layer maps block IDs to files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    pub name: String,
    pub space: SpaceId,
}