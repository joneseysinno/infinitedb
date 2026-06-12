//! Versioned block wire decode — legacy u64 revisions vs v5 HLC (Phase 1).

use std::io;

use bincode::{config::standard, decode_from_slice, Encode, Decode};
use serde::{Deserialize, Serialize};

use super::{
    address::{Address, RevisionId, SpaceId},
    block::{Block, BlockId, Record},
    checksum::Checksum,
    hilbert_key::CachedHilbertKey,
    revision_codec::RevisionWireFormat,
};

/// Pre-v5 block shape (revision field was bare u64).
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
struct LegacyRecord {
    pub address: Address,
    pub revision: u64,
    pub data: Vec<u8>,
    pub tombstone: bool,
    #[serde(default)]
    pub hilbert_key: CachedHilbertKey,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
struct LegacyBlock {
    pub id: BlockId,
    pub space: SpaceId,
    pub records: Vec<LegacyRecord>,
    pub min_revision: u64,
    pub max_revision: u64,
    pub checksum: Checksum,
}

fn lift_record(r: LegacyRecord) -> Record {
    Record {
        address: r.address,
        revision: RevisionId::legacy(r.revision),
        data: r.data,
        tombstone: r.tombstone,
        hilbert_key: r.hilbert_key,
    }
}

fn lift_block(b: LegacyBlock) -> Block {
    Block {
        id: b.id,
        space: b.space,
        records: b.records.into_iter().map(lift_record).collect(),
        min_revision: RevisionId::legacy(b.min_revision),
        max_revision: RevisionId::legacy(b.max_revision),
        checksum: b.checksum,
    }
}

/// Decode a sealed block, embedding legacy u64 revisions when needed.
pub fn decode_block(bytes: &[u8], wire: RevisionWireFormat) -> Result<Block, io::Error> {
    match wire {
        RevisionWireFormat::HlcU128 => match decode_from_slice::<Block, _>(bytes, standard()) {
            Ok((block, _)) => Ok(block),
            Err(_) => {
                let (legacy, _) = decode_from_slice::<LegacyBlock, _>(bytes, standard())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(lift_block(legacy))
            }
        },
        RevisionWireFormat::LegacyU64 => {
            let (legacy, _) = decode_from_slice::<LegacyBlock, _>(bytes, standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(lift_block(legacy))
        }
    }
}
