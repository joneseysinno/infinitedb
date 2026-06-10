//! Restart and durability matrix for CRCW [`InfiniteDb`] (format v4).

use infinite_db::infinitedb_core::address::{DimensionVector, SpaceId};
use infinite_db::infinitedb_core::branch::BranchId;
use infinite_db::infinitedb_core::space::SpaceConfig;
use infinite_db::{InfiniteDb, OpenOptions, FORMAT_VERSION_V4};
use tempfile::TempDir;

fn space(id: u64, dims: usize) -> SpaceConfig {
    SpaceConfig::new(SpaceId(id), format!("space_{id}"), dims).with_shard_bits(2)
}

fn open_v4(dir: &TempDir) -> InfiniteDb {
    OpenOptions {
        format_version: Some(FORMAT_VERSION_V4),
        ..OpenOptions::default()
    }
    .open(dir.path())
    .unwrap()
}

#[test]
fn writes_visible_after_sync_and_reopen() {
    let dir = TempDir::new().unwrap();
    let space_id = SpaceId(1);
    {
        let db = open_v4(&dir);
        db.register_space(space(1, 2)).unwrap();
        db.insert(space_id, DimensionVector::new(vec![3, 4]), vec![7])
            .unwrap();
        db.sync().unwrap();
    }
    let db = open_v4(&dir);
    let results = db.query(space_id, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].data, vec![7]);
}

#[test]
fn flushed_data_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let space_id = SpaceId(1);
    {
        let db = open_v4(&dir);
        db.register_space(space(1, 2)).unwrap();
        db.insert(space_id, DimensionVector::new(vec![5, 5]), vec![42])
            .unwrap();
        db.insert(space_id, DimensionVector::new(vec![200, 30]), vec![43])
            .unwrap();
        db.flush(space_id).unwrap();
        db.sync().unwrap();
    }
    let db = open_v4(&dir);
    let mut results = db.query(space_id, None).unwrap();
    results.sort_by_key(|r| r.data.clone());
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].data, vec![42]);
    assert_eq!(results[1].data, vec![43]);
}

#[test]
fn branches_persist_across_restart() {
    let dir = TempDir::new().unwrap();
    let created;
    {
        let db = open_v4(&dir);
        db.register_space(space(1, 2)).unwrap();
        created = db.create_branch("feature", BranchId::MAIN).unwrap();
        assert_ne!(created, BranchId::MAIN);
        db.sync().unwrap();
    }
    let db = open_v4(&dir);
    assert_eq!(db.branch_id("feature"), Some(created));
    assert_eq!(db.branch_id("main"), Some(BranchId::MAIN));
}

#[test]
fn branch_counter_unique_after_restart() {
    let dir = TempDir::new().unwrap();
    let first;
    {
        let db = open_v4(&dir);
        db.register_space(space(1, 2)).unwrap();
        first = db.create_branch("a", BranchId::MAIN).unwrap();
        db.sync().unwrap();
    }
    let db = open_v4(&dir);
    let second = db.create_branch("b", BranchId::MAIN).unwrap();
    assert_ne!(first, second);
}
