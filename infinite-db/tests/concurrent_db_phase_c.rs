//! Phase C: Hilbert shards, branch overlays, and merge_branch.

use std::sync::Arc;
use std::thread;

use infinite_db::infinitedb_core::address::{DimensionVector, SpaceId};
use infinite_db::infinitedb_core::branch::BranchId;
use infinite_db::infinitedb_core::merge::MergeStrategy;
use infinite_db::infinitedb_core::space::SpaceConfig;
use infinite_db::{InfiniteDb, OpenOptions, FORMAT_VERSION_V4};
use tempfile::TempDir;

fn open_v4(dir: &TempDir) -> InfiniteDb {
    OpenOptions {
        format_version: Some(FORMAT_VERSION_V4),
        ..OpenOptions::default()
    }
    .open(dir.path())
    .unwrap()
}

fn space(id: u64, dims: usize) -> SpaceConfig {
    SpaceConfig::new(SpaceId(id), format!("space_{id}"), dims).with_shard_bits(4)
}

#[test]
fn v4_hilbert_shards_spawn_on_register() {
    let dir = TempDir::new().unwrap();
    let db = open_v4(&dir);
    db.register_space(space(1, 2)).unwrap();
    assert_eq!(db.format_version(), 4);
    assert!(db.space_shard_count() >= 16);
    let shard_dir = dir.path().join("spaces/1/shards/0");
    assert!(shard_dir.join("hot.seg").exists() || shard_dir.join("wal/staging.log").exists());
}

#[test]
fn parallel_branch_writes_no_overlap() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(open_v4(&dir));
    db.register_space(space(1, 2)).unwrap();
    let branch_a = db.create_branch("feature-a", BranchId::MAIN).unwrap();
    let branch_b = db.create_branch("feature-b", BranchId::MAIN).unwrap();

    let db_a = Arc::clone(&db);
    let ha = thread::spawn(move || {
        for i in 0..16 {
            db_a.insert_on_branch(
                branch_a,
                SpaceId(1),
                DimensionVector::new(vec![i, 0]),
                vec![1],
            )
            .unwrap();
        }
    });

    let db_b = Arc::clone(&db);
    let hb = thread::spawn(move || {
        for i in 0..16 {
            db_b.insert_on_branch(
                branch_b,
                SpaceId(1),
                DimensionVector::new(vec![i, 100]),
                vec![2],
            )
            .unwrap();
        }
    });

    ha.join().unwrap();
    hb.join().unwrap();

    assert_eq!(db.query_on_branch(branch_a, SpaceId(1), None).unwrap().len(), 16);
    assert_eq!(db.query_on_branch(branch_b, SpaceId(1), None).unwrap().len(), 16);
    assert_eq!(db.query(SpaceId(1), None).unwrap().len(), 0);
}

#[test]
fn merge_fast_forward_non_conflicting() {
    let dir = TempDir::new().unwrap();
    let db = open_v4(&dir);
    db.register_space(space(1, 2)).unwrap();
    let branch = db.create_branch("feature", BranchId::MAIN).unwrap();
    db.insert_on_branch(branch, SpaceId(1), DimensionVector::new(vec![1, 1]), vec![42])
        .unwrap();
    db.sync().unwrap();

    let result = db
        .merge_branch(BranchId::MAIN, branch, MergeStrategy::PreferSource, None)
        .unwrap();
    assert_eq!(result.merged_records, 1);
    assert!(result.conflicts.is_empty());
    db.sync().unwrap();

    let main = db.query(SpaceId(1), None).unwrap();
    let latest = main.iter().max_by_key(|r| r.revision.0).unwrap();
    assert_eq!(latest.data, vec![42]);
}

#[test]
fn merge_conflict_detection() {
    let dir = TempDir::new().unwrap();
    let db = open_v4(&dir);
    db.register_space(space(1, 2)).unwrap();
    let point = DimensionVector::new(vec![5, 5]);
    db.insert(SpaceId(1), point.clone(), vec![1]).unwrap();
    db.sync().unwrap();

    let branch = db.create_branch("feature", BranchId::MAIN).unwrap();
    db.insert_on_branch(branch, SpaceId(1), point.clone(), vec![2])
        .unwrap();
    db.insert(SpaceId(1), point.clone(), vec![3]).unwrap();
    db.sync().unwrap();

    let result = db
        .merge_branch(BranchId::MAIN, branch, MergeStrategy::Interactive, None)
        .unwrap();
    assert_eq!(result.conflicts.len(), 1);
    assert_eq!(result.merged_records, 0);
}

#[test]
fn merge_prefer_higher_revision() {
    let dir = TempDir::new().unwrap();
    let db = open_v4(&dir);
    db.register_space(space(1, 2)).unwrap();
    let point = DimensionVector::new(vec![7, 7]);
    db.insert(SpaceId(1), point.clone(), vec![1]).unwrap();
    db.sync().unwrap();

    let branch = db.create_branch("feature", BranchId::MAIN).unwrap();
    db.insert_on_branch(branch, SpaceId(1), point.clone(), vec![9]).unwrap();
    db.sync().unwrap();

    let result = db
        .merge_branch(
            BranchId::MAIN,
            branch,
            MergeStrategy::PreferHigherRevision,
            None,
        )
        .unwrap();
    assert!(result.conflicts.is_empty());
    db.sync().unwrap();
    let main = db.query(SpaceId(1), None).unwrap();
    let latest = main.iter().max_by_key(|r| r.revision.0).unwrap();
    assert_eq!(latest.data, vec![9]);
}

#[test]
fn shard_parallel_same_branch() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(open_v4(&dir));
    db.register_space(space(1, 2)).unwrap();

    let handles: Vec<_> = (0..16)
        .map(|i| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                db.insert(
                    SpaceId(1),
                    DimensionVector::new(vec![i * 17, i * 3]),
                    vec![i as u8],
                )
                .unwrap();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    db.sync().unwrap();
    assert_eq!(db.query(SpaceId(1), None).unwrap().len(), 16);
}

#[test]
fn query_bbox_on_branch_sees_overlay_not_main() {
    let dir = TempDir::new().unwrap();
    let db = open_v4(&dir);
    db.register_space(space(1, 2)).unwrap();
    let branch = db.create_branch("feature", BranchId::MAIN).unwrap();

    db.insert_on_branch(
        branch,
        SpaceId(1),
        DimensionVector::new(vec![3, 4]),
        vec![99],
    )
    .unwrap();

    let min = DimensionVector::new(vec![3, 4]);
    let max = DimensionVector::new(vec![3, 4]);
    assert!(db.query_bbox(SpaceId(1), min.clone(), max.clone(), None).unwrap().is_empty());
    let on_branch = db
        .query_bbox_on_branch(branch, SpaceId(1), min, max, None)
        .unwrap();
    assert_eq!(on_branch.len(), 1);
    assert_eq!(on_branch[0].data, vec![99]);
    assert_eq!(db.branch_id("feature"), Some(branch));
}
