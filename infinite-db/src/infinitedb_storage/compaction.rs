//! Compaction: merge small or fragmented blocks into larger ones.
//!
//! Compaction is triggered when:
//!   - A block count threshold is exceeded within a space.
//!   - A block's live record ratio drops below a minimum fill factor.
//!
//! Strategy: collect all candidate blocks, merge their records,
//! sort by (Address, RevisionId), deduplicate keeping only the latest
//! revision per address (unless history retention is required), and
//! write out new sealed blocks. The old block IDs are then handed to
//! the GC for deletion.

use std::collections::BTreeMap;
use crate::infinitedb_core::{
    address::RevisionId,
    block::{Block, BlockId, Record},
    snapshot::SnapshotId,
    address::SpaceId,
};

/// Configuration for a compaction run.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Maximum number of records per output block.
    pub max_records_per_block: usize,
    /// If true, keep all revisions (history mode). If false, only the latest
    /// live revision per address is retained.
    pub retain_history: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            max_records_per_block: 4096,
            retain_history: true,
        }
    }
}

/// Result of a compaction run.
pub struct CompactionResult {
    /// Newly produced blocks (not yet written to disk).
    pub new_blocks: Vec<Block>,
    /// Block IDs that were consumed and can be deleted after the new blocks
    /// are durably written.
    pub superseded: Vec<BlockId>,
}

/// Merge a set of blocks into compacted output blocks.
///
/// `next_block_id` is a closure that vends the next unused `BlockId`.
/// `snapshot` is the snapshot these blocks belong to (stored in output blocks).
pub fn compact<F>(
    input_blocks: Vec<Block>,
    config: &CompactionConfig,
    _snapshot: SnapshotId,
    mut next_block_id: F,
) -> CompactionResult
where
    F: FnMut() -> BlockId,
{
    let superseded: Vec<BlockId> = input_blocks.iter().map(|b| b.id).collect();
    let space = input_blocks
        .first()
        .map(|b| b.space)
        .unwrap_or(SpaceId(0));

    // Collect and sort all records.
    let mut all: Vec<Record> = input_blocks
        .into_iter()
        .flat_map(|b| b.records.into_iter())
        .collect();

    // Sort by address then revision ascending.
    all.sort_by(|a, b| {
        a.address
            .point
            .coords
            .cmp(&b.address.point.coords)
            .then_with(|| a.revision.cmp(&b.revision))
    });

    // Optionally deduplicate: keep only the latest revision per address.
    let records: Vec<Record> = if config.retain_history {
        all
    } else {
        let mut map: BTreeMap<Vec<u32>, Record> = BTreeMap::new();
        for rec in all {
            map.insert(rec.address.point.coords.clone(), rec);
        }
        map.into_values().collect()
    };

    // Chunk into output blocks.
    let new_blocks = records
        .chunks(config.max_records_per_block)
        .map(|chunk| {
            let chunk = chunk.to_vec();
            let min_rev = chunk.iter().map(|r| r.revision).min().unwrap_or(RevisionId::ZERO);
            let max_rev = chunk.iter().map(|r| r.revision).max().unwrap_or(RevisionId::ZERO);
            Block {
                id: next_block_id(),
                space,
                records: chunk,
                min_revision: min_rev,
                max_revision: max_rev,
                checksum: [0u8; 32], // computed by nvme::compute_checksum before writing
            }
        })
        .collect();

    CompactionResult { new_blocks, superseded }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::{
        address::{Address, DimensionVector, RevisionId, SpaceId},
        block::Record,
        snapshot::SnapshotId,
    };

    fn make_record(x: u32, rev: u64, tombstone: bool) -> Record {
        Record {
            address: Address::new(SpaceId(1), DimensionVector::new(vec![x, 0])),
            revision: RevisionId(rev),
            data: vec![],
            tombstone,
        }
    }

    fn make_block(id: u64, records: Vec<Record>) -> Block {
        Block {
            id: BlockId(id),
            space: SpaceId(1),
            min_revision: RevisionId(0),
            max_revision: RevisionId(99),
            records,
            checksum: [0u8; 32],
        }
    }

    #[test]
    fn compacts_two_blocks_into_one() {
        let blocks = vec![
            make_block(1, vec![make_record(1, 1, false), make_record(2, 1, false)]),
            make_block(2, vec![make_record(3, 1, false), make_record(4, 1, false)]),
        ];
        let mut next_id = 10u64;
        let result = compact(
            blocks,
            &CompactionConfig::default(),
            SnapshotId(1),
            || { let id = BlockId(next_id); next_id += 1; id },
        );
        assert_eq!(result.superseded, vec![BlockId(1), BlockId(2)]);
        assert_eq!(result.new_blocks.len(), 1);
        assert_eq!(result.new_blocks[0].records.len(), 4);
    }

    #[test]
    fn dedup_keeps_latest_revision() {
        let blocks = vec![make_block(
            1,
            vec![make_record(1, 1, false), make_record(1, 2, false)],
        )];
        let config = CompactionConfig { retain_history: false, ..Default::default() };
        let mut next_id = 10u64;
        let result = compact(
            blocks,
            &config,
            SnapshotId(1),
            || { let id = BlockId(next_id); next_id += 1; id },
        );
        assert_eq!(result.new_blocks[0].records.len(), 1);
        assert_eq!(result.new_blocks[0].records[0].revision, RevisionId(2));
    }
}
