//! CRCW correctness tests (visibility, read consistency).

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use infinite_db::infinitedb_core::address::{DimensionVector, RevisionId, SpaceId};
use infinite_db::infinitedb_core::branch::BranchId;
use infinite_db::infinitedb_core::query::Query;
use infinite_db::infinitedb_core::snapshot::SnapshotId;
use infinite_db::infinitedb_core::space::SpaceConfig;
use infinite_db::{InfiniteDb, OpenOptions};
use tempfile::TempDir;

fn space(id: u64, dims: usize) -> SpaceConfig {
    SpaceConfig::new(SpaceId(id), format!("space_{id}"), dims)
}

#[test]
fn tombstone_resurrection() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let space_id = SpaceId(1);
    db.register_space(space(1, 2)).unwrap();
    let point = DimensionVector::new(vec![10, 20]);

    db.insert(space_id, point.clone(), vec![1]).unwrap();
    db.sync().unwrap();

    db.delete(space_id, point.clone()).unwrap();
    db.sync().unwrap();

    db.insert(space_id, point.clone(), vec![99]).unwrap();
    db.sync().unwrap();

    let results = db.query(space_id, None).unwrap();
    assert_eq!(results.len(), 1, "re-insert after delete must be visible");
    assert_eq!(results[0].data, vec![99]);
    assert!(!results[0].tombstone);
}

#[test]
fn tombstone_resurrection_across_seal_boundary() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let space_id = SpaceId(1);
    db.register_space(space(1, 2)).unwrap();
    let point = DimensionVector::new(vec![5, 5]);

    db.insert(space_id, point.clone(), vec![1]).unwrap();
    db.sync().unwrap();
    db.flush(space_id).unwrap();

    db.delete(space_id, point.clone()).unwrap();
    db.sync().unwrap();
    db.flush(space_id).unwrap();

    db.insert(space_id, point.clone(), vec![77]).unwrap();
    db.sync().unwrap();

    let results = db.query(space_id, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].data, vec![77]);
}

#[test]
fn latest_wins() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let space_id = SpaceId(1);
    db.register_space(space(1, 2)).unwrap();
    let point = DimensionVector::new(vec![1, 1]);

    let rev1 = db.insert(space_id, point.clone(), vec![10]).unwrap();
    db.sync().unwrap();
    let rev2 = db.insert(space_id, point.clone(), vec![20]).unwrap();
    db.sync().unwrap();
    let rev3 = db.insert(space_id, point.clone(), vec![30]).unwrap();
    db.sync().unwrap();

    let results = db.query(space_id, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].revision, rev3);
    assert_eq!(results[0].data, vec![30]);

    let at_rev1 = db.query(space_id, Some(rev1)).unwrap();
    assert_eq!(at_rev1.len(), 1);
    assert_eq!(at_rev1[0].data, vec![10]);

    let between = db.query(space_id, Some(RevisionId(rev2.0))).unwrap();
    assert_eq!(between.len(), 1);
    assert_eq!(between[0].data, vec![20]);
    assert!(rev1.0 < rev2.0 && rev2.0 < rev3.0);
}

#[test]
fn include_tombstones_regression() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let space_id = SpaceId(1);
    db.register_space(space(1, 2)).unwrap();
    let point = DimensionVector::new(vec![3, 3]);

    db.insert(space_id, point.clone(), vec![1]).unwrap();
    db.sync().unwrap();
    db.delete(space_id, point.clone()).unwrap();
    db.sync().unwrap();

    let default = db.query(space_id, None).unwrap();
    assert_eq!(default.len(), 0, "tombstoned address hidden by default");

    let with_tombstones = db
        .read()
        .execute(&Query::new(space_id, SnapshotId(1)).include_tombstones())
        .unwrap();
    assert!(
        with_tombstones.iter().any(|r| r.tombstone),
        "include_tombstones surfaces the tombstone"
    );
    assert!(
        with_tombstones.len() >= 2,
        "include_tombstones preserves revision history"
    );
}

#[test]
fn seal_window_no_duplicate_records_under_load() {
    let dir = TempDir::new().unwrap();
    let mut options = OpenOptions::default();
    options.io_thread.hot_segment_seal_threshold = 8;
    let db = options.open(dir.path()).unwrap();
    let space_id = SpaceId(1);
    db.register_space(space(1, 2)).unwrap();

    let query_iterations = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let db_read: Arc<InfiniteDb> = Arc::new(db);
    let iter_read = Arc::clone(&query_iterations);
    let stop_read = Arc::clone(&stop);
    let db_reader = Arc::clone(&db_read);
    let reader = thread::spawn(move || {
        while !stop_read.load(Ordering::Relaxed) {
            if let Ok(results) = db_reader.query(space_id, None) {
                let mut pairs = HashSet::new();
                for r in &results {
                    let key = (r.address.point.coords.clone(), r.revision.0);
                    assert!(pairs.insert(key), "duplicate (address, revision) in query");
                }
                iter_read.fetch_add(1, Ordering::Relaxed);
            }
            thread::sleep(Duration::from_millis(1));
        }
    });

    let db_write = Arc::clone(&db_read);
    let writer = thread::spawn(move || {
        for i in 0..200u32 {
            db_write
                .insert(
                    space_id,
                    DimensionVector::new(vec![i % 50, i / 50]),
                    vec![i as u8],
                )
                .unwrap();
            if i % 16 == 0 {
                let _ = db_write.flush(space_id);
            }
        }
        db_write.sync().unwrap();
    });

    writer.join().unwrap();
    stop.store(true, Ordering::Relaxed);
    reader.join().unwrap();
    assert!(query_iterations.load(Ordering::Relaxed) > 0);

    let final_results = db_read.query(space_id, None).unwrap();
    assert_eq!(final_results.len(), 200);
}

#[test]
fn read_txn_repeatable_under_concurrent_writes() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(InfiniteDb::open(dir.path()).unwrap());
    let space_id = SpaceId(1);
    db.register_space(space(1, 2)).unwrap();
    db.insert(space_id, DimensionVector::new(vec![0, 0]), vec![1])
        .unwrap();
    db.sync().unwrap();

    let txn = db.read();
    let first = txn.query(space_id).unwrap();

    let db_write = Arc::clone(&db);
    let writer = thread::spawn(move || {
        for i in 1..20u32 {
            db_write
                .insert(space_id, DimensionVector::new(vec![i, 0]), vec![i as u8])
                .unwrap();
        }
        db_write.sync().unwrap();
    });
    thread::sleep(Duration::from_millis(50));
    let second = txn.query(space_id).unwrap();
    writer.join().unwrap();
    let third = txn.query(space_id).unwrap();

    assert_eq!(first.len(), second.len());
    assert_eq!(second.len(), third.len());
    for (a, b) in first.iter().zip(second.iter()) {
        assert_eq!(a.revision, b.revision);
        assert_eq!(a.data, b.data);
    }
}

#[test]
fn stable_revision_lag_and_catch_up() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let space_id = SpaceId(1);
    db.register_space(space(1, 2)).unwrap();

    assert!(db.stable_revision() <= db.revision());

    db.insert(space_id, DimensionVector::new(vec![1, 1]), vec![1])
        .unwrap();
    let allocated = db.revision();
    assert!(db.stable_revision() < allocated);

    db.sync().unwrap();
    assert_eq!(db.stable_revision(), db.revision());
}

#[test]
fn large_space_id_register_and_write() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let space_id = SpaceId(u64::MAX - 1);
    db.register_space(SpaceConfig::new(space_id, "endpoint", 2))
        .unwrap();
    db.insert(space_id, DimensionVector::new(vec![1, 2]), vec![7])
        .unwrap();
    db.sync().unwrap();
    let results = db.query(space_id, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].data, vec![7]);
}

#[test]
fn branch_overlay_keys_do_not_collide() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let space_a = SpaceId(1u64 << 32);
    let space_b = SpaceId(0);
    db.register_space(SpaceConfig::new(space_a, "a", 2))
        .unwrap();
    db.register_space(SpaceConfig::new(space_b, "b", 2))
        .unwrap();

    let branch1 = db.create_branch("b1", BranchId::MAIN).unwrap();
    let branch2 = db.create_branch("b2", BranchId::MAIN).unwrap();

    db.insert_on_branch(branch1, space_a, DimensionVector::new(vec![1, 1]), vec![11])
        .unwrap();
    db.insert_on_branch(branch2, space_b, DimensionVector::new(vec![2, 2]), vec![22])
        .unwrap();
    db.sync().unwrap();

    let r1 = db.query_on_branch(branch1, space_a, None).unwrap();
    let r2 = db.query_on_branch(branch2, space_b, None).unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);
    assert_eq!(r1[0].data, vec![11]);
    assert_eq!(r2[0].data, vec![22]);
}

#[test]
fn insert_many_round_trip() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let space_id = SpaceId(1);
    db.register_space(space(1, 2)).unwrap();

    let rows: Vec<_> = (0..100)
        .map(|i| (DimensionVector::new(vec![i, i % 7]), vec![i as u8]))
        .collect();
    let (first, last) = db.insert_many(space_id, rows).unwrap();
    assert!(first.0 <= last.0);
    db.sync().unwrap();

    let results = db.query(space_id, None).unwrap();
    assert_eq!(results.len(), 100);
}

#[test]
fn lazy_shard_provisioning() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    for i in 0..5 {
        db.register_space(space(i + 1, 2)).unwrap();
    }
    assert_eq!(db.space_shard_count(), 0);

    db.insert(SpaceId(1), DimensionVector::new(vec![1, 1]), vec![1])
        .unwrap();
    db.sync().unwrap();
    assert!(db.space_shard_count() >= 1);
}

#[test]
fn branch_overlay_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let space_id = SpaceId(1);
    let branch;
    {
        let db = InfiniteDb::open(dir.path()).unwrap();
        db.register_space(space(1, 2)).unwrap();
        branch = db.create_branch("feature", BranchId::MAIN).unwrap();
        db.insert_on_branch(branch, space_id, DimensionVector::new(vec![3, 3]), vec![99])
            .unwrap();
        db.sync().unwrap();
    }
    let db = InfiniteDb::open(dir.path()).unwrap();
    let results = db.query_on_branch(branch, space_id, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].data, vec![99]);
}

#[test]
fn sync_barrier_publishes_enqueued_writes() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let space_id = SpaceId(1);
    db.register_space(space(1, 2)).unwrap();
    for i in 0..50 {
        db.insert(space_id, DimensionVector::new(vec![i, 0]), vec![i as u8])
            .unwrap();
    }
    db.sync().unwrap();
    let results = db.query(space_id, None).unwrap();
    assert_eq!(results.len(), 50);
}
