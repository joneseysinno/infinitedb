//! Restart and durability matrix.
//!
//! These tests open a database, mutate it, drop the handle, and reopen from the
//! same directory to assert that committed state survives a process restart.

use infinite_db::infinitedb_core::address::{DimensionVector, SpaceId};
use infinite_db::infinitedb_core::branch::BranchId;
use infinite_db::infinitedb_core::space::SpaceConfig;
use infinite_db::infinitedb_storage::compaction::CompactionConfig;
use infinite_db::InfiniteDb;
use tempfile::TempDir;

fn space(id: u64, dims: usize) -> SpaceConfig {
    SpaceConfig::new(SpaceId(id), format!("space_{id}"), dims)
}

#[test]
fn unflushed_writes_replay_from_wal() {
    let dir = TempDir::new().unwrap();
    let space_id = SpaceId(1);
    {
        let mut db = InfiniteDb::open(dir.path()).unwrap();
        db.register_space(space(1, 2)).unwrap();
        db.insert(space_id, DimensionVector::new(vec![3, 4]), vec![7]).unwrap();
        // No flush: the record lives only in the WAL + buffer.
    }
    let mut db = InfiniteDb::open(dir.path()).unwrap();
    let results = db.query(space_id, None).unwrap();
    assert_eq!(results.len(), 1, "WAL replay must restore unflushed writes");
    assert_eq!(results[0].data, vec![7]);
}

#[test]
fn flushed_blocks_visible_after_reopen() {
    let dir = TempDir::new().unwrap();
    let space_id = SpaceId(1);
    {
        let mut db = InfiniteDb::open(dir.path()).unwrap();
        db.register_space(space(1, 2)).unwrap();
        db.insert(space_id, DimensionVector::new(vec![5, 5]), vec![42]).unwrap();
        db.insert(space_id, DimensionVector::new(vec![200, 30]), vec![43]).unwrap();
        db.flush(space_id).unwrap();
    }
    // Reopen: the sealed block must remain reachable via the persisted snapshot.
    let mut db = InfiniteDb::open(dir.path()).unwrap();
    let mut results = db.query(space_id, None).unwrap();
    results.sort_by_key(|r| r.data.clone());
    assert_eq!(results.len(), 2, "flushed blocks must survive restart");
    assert_eq!(results[0].data, vec![42]);
    assert_eq!(results[1].data, vec![43]);
}

#[test]
fn branches_persist_across_restart() {
    let dir = TempDir::new().unwrap();
    let created;
    {
        let mut db = InfiniteDb::open(dir.path()).unwrap();
        created = db.create_branch("feature", BranchId(1)).unwrap();
        assert_ne!(created, BranchId(1));
    }
    let db = InfiniteDb::open(dir.path()).unwrap();
    assert_eq!(
        db.branch_id("feature"),
        Some(created),
        "user branch must survive restart"
    );
    assert!(db.branch_id("main").is_some(), "main branch always present");
}

#[test]
fn branch_counter_does_not_collide_after_restart() {
    let dir = TempDir::new().unwrap();
    let first;
    {
        let mut db = InfiniteDb::open(dir.path()).unwrap();
        first = db.create_branch("a", BranchId(1)).unwrap();
    }
    let mut db = InfiniteDb::open(dir.path()).unwrap();
    let second = db.create_branch("b", BranchId(1)).unwrap();
    assert_ne!(
        first, second,
        "branch IDs must remain unique after the counter is reloaded"
    );
}

#[test]
fn compacted_data_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let space_id = SpaceId(2);
    {
        let mut db = InfiniteDb::open(dir.path()).unwrap();
        db.register_space(space(2, 1)).unwrap();
        for i in 0..3u32 {
            db.insert(space_id, DimensionVector::new(vec![i]), vec![i as u8])
                .unwrap();
            db.flush(space_id).unwrap();
        }
        db.compact_space(
            space_id,
            &CompactionConfig {
                max_records_per_block: 16,
                retain_history: true,
            },
        )
        .unwrap();
    }
    let mut db = InfiniteDb::open(dir.path()).unwrap();
    assert_eq!(db.query(space_id, None).unwrap().len(), 3);
}
