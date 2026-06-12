//! CRCW correctness tests (visibility, read consistency).

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

static SYNC_INJECT_LOCK: Mutex<()> = Mutex::new(());

use infinite_db::infinitedb_core::address::{DimensionVector, RevisionId, SpaceId};
use infinite_db::infinitedb_core::branch::BranchId;
use infinite_db::infinitedb_core::query::Query;
use infinite_db::infinitedb_core::snapshot::SnapshotId;
use infinite_db::infinitedb_core::space::{CompactionPolicy, SpaceConfig};
use infinite_db::{InfiniteDb, OpenOptions, FORMAT_VERSION_V4};
use tempfile::TempDir;

fn space(id: u64, dims: usize) -> SpaceConfig {
    SpaceConfig::new(SpaceId(id), format!("space_{id}"), dims)
}

fn reset_sync_fail_inject() {
    use infinite_db::infinitedb_storage::hot_segment::{
        TEST_FAIL_SYNC_ARMED, TEST_FAIL_SYNC_GROUP,
    };
    use std::sync::atomic::Ordering;
    TEST_FAIL_SYNC_GROUP.store(false, Ordering::SeqCst);
    TEST_FAIL_SYNC_ARMED.store(false, Ordering::SeqCst);
}

fn arm_sync_fail_inject() {
    use infinite_db::infinitedb_storage::hot_segment::TEST_FAIL_SYNC_ARMED;
    use std::sync::atomic::Ordering;
    TEST_FAIL_SYNC_ARMED.store(true, Ordering::SeqCst);
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

    let between = db.query(space_id, Some(rev2)).unwrap();
    assert_eq!(between.len(), 1);
    assert_eq!(between[0].data, vec![20]);
    assert!(rev1 < rev2 && rev2 < rev3);
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
                    let key = (r.address.point.coords.clone(), r.revision.legacy_sequence());
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
fn stable_advances_past_failed_group_commit() {
    use infinite_db::infinitedb_storage::hot_segment::TEST_FAIL_SYNC_GROUP;
    use std::sync::atomic::Ordering;

    let _inject_guard = SYNC_INJECT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_sync_fail_inject();
    arm_sync_fail_inject();
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let space_id = SpaceId(1);
    db.register_space(space(1, 2)).unwrap();

    let stable_before = db.stable_revision();
    TEST_FAIL_SYNC_GROUP.store(true, Ordering::SeqCst);
    db.insert(space_id, DimensionVector::new(vec![1, 1]), vec![1])
        .unwrap();
    db.sync().unwrap_err();

    let stable_after = db.stable_revision();
    assert!(
        stable_after >= stable_before,
        "stable must not remain pinned below a failed revision"
    );

    let failed = db.failed_revisions();
    assert!(!failed.is_empty(), "failed revision should be recorded");
    let failed_again = db.failed_revisions();
    assert_eq!(failed, failed_again, "observation must not drain evidence");

    reset_sync_fail_inject();
    db.insert(space_id, DimensionVector::new(vec![2, 2]), vec![2])
        .unwrap();
    db.sync().unwrap();
    let results = db.query(space_id, None).unwrap();
    assert!(results.iter().any(|r| r.data == vec![2]));
}

#[test]
fn failed_group_not_resurrected_on_reopen() {
    use infinite_db::infinitedb_storage::hot_segment::TEST_FAIL_SYNC_GROUP;
    use std::sync::atomic::Ordering;

    let _inject_guard = SYNC_INJECT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_sync_fail_inject();
    arm_sync_fail_inject();
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let space_id = SpaceId(1);
    let point = DimensionVector::new(vec![9, 9]);

    {
        let db = InfiniteDb::open(&path).unwrap();
        db.register_space(space(1, 2)).unwrap();
        TEST_FAIL_SYNC_GROUP.store(true, Ordering::SeqCst);
        db.insert(space_id, point.clone(), vec![99]).unwrap();
        db.sync().unwrap_err();
        assert!(
            !db
                .query(space_id, None)
                .unwrap()
                .iter()
                .any(|r| r.data == vec![99]),
            "failed write must not appear in same session"
        );
    }

    reset_sync_fail_inject();
    let db = InfiniteDb::open(&path).unwrap();
    assert!(
        !db
            .query(space_id, None)
            .unwrap()
            .iter()
            .any(|r| r.data == vec![99]),
        "failed write must not resurrect on reopen"
    );
}

#[test]
fn query_bbox_respects_space_hilbert_precision_in_shard_prune() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let space_id = SpaceId(1);
    db.register_space(space(1, 2).with_bits_per_dim(4))
        .unwrap();
    let point = DimensionVector::new(vec![3, 5]);
    db.insert(space_id, point.clone(), vec![7]).unwrap();
    db.sync().unwrap();

    let min = DimensionVector::new(vec![3, 5]);
    let max = DimensionVector::new(vec![3, 5]);
    let results = db.query_bbox(space_id, min, max, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].data, vec![7]);
}

#[test]
fn allocation_registration_not_visible_to_stable() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(InfiniteDb::open(dir.path()).unwrap());
    db.register_space(space(1, 2)).unwrap();

    let range = db.allocate_revisions(1);
    let rev = range.nth(0);
    assert!(
        db.stable_revision() < rev,
        "stable must not reach an allocated-but-unretired revision"
    );

    let db2 = Arc::clone(&db);
    let writer = thread::spawn(move || {
        for _ in 0..300 {
            let range = db2.allocate_revisions(1);
            let rev = range.nth(0);
            assert!(
                db2.stable_revision() < rev,
                "stable must lag behind unretired allocation {rev:?}"
            );
            thread::yield_now();
        }
    });

    for _ in 0..10_000 {
        let stable = db.stable_revision();
        let allocated = db.revision();
        assert!(
            stable <= allocated,
            "stable {stable:?} must not exceed allocated {allocated:?}"
        );
        thread::yield_now();
    }
    writer.join().unwrap();
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
fn endpoint_space_multi_shard_writes_isolated() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let endpoint = SpaceId(u64::MAX - 1);
    let other = SpaceId(u64::MAX - 2);
    db.register_space(
        SpaceConfig::new(endpoint, "endpoint", 2).with_shard_bits(4),
    )
    .unwrap();
    db.register_space(space(other.0, 2)).unwrap();

    for shard in 0..4u32 {
        let point = DimensionVector::new(vec![shard * 1000, shard]);
        db.insert(endpoint, point, vec![shard as u8]).unwrap();
    }
    db.insert(other, DimensionVector::new(vec![1, 1]), vec![99]).unwrap();
    db.sync().unwrap();

    let endpoint_rows = db.query(endpoint, None).unwrap();
    assert_eq!(endpoint_rows.len(), 4);
    let other_rows = db.query(other, None).unwrap();
    assert_eq!(other_rows.len(), 1);
    assert_eq!(other_rows[0].data, vec![99]);
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
    assert!(first <= last);
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
fn as_of_survives_auto_compaction_with_keep_all() {
    let _inject_guard = SYNC_INJECT_LOCK.lock().unwrap();
    reset_sync_fail_inject();
    let dir = TempDir::new().unwrap();
    let mut opts = OpenOptions::default();
    opts.io_thread.hot_segment_seal_bytes = 64;
    let db = opts.open(dir.path()).unwrap();
    let space_id = SpaceId(1);
    db.register_space(
        space(1, 2).with_compaction_policy(CompactionPolicy::KeepAll),
    )
    .unwrap();

    // One block per distinct coordinate (block index keys on hilbert min_key).
    let mut rev1 = RevisionId::legacy(0);
    for i in 0..10u64 {
        let point = DimensionVector::new(vec![i as u32, 0]);
        let rev = db.insert(space_id, point, vec![i as u8]).unwrap();
        if i == 0 {
            rev1 = rev;
        }
        db.sync().unwrap();
        db.flush(space_id).unwrap();
    }

    let at_first = db.query(space_id, Some(rev1)).unwrap();
    assert_eq!(at_first.len(), 1);
    assert_eq!(at_first[0].data, vec![0]);

    let latest = db.query(space_id, None).unwrap();
    assert_eq!(latest.len(), 10);
}

#[test]
fn latest_only_compaction_drops_history() {
    let dir = TempDir::new().unwrap();
    let mut opts = OpenOptions::default();
    opts.io_thread.hot_segment_seal_bytes = 64;
    let db = opts.open(dir.path()).unwrap();
    let space_id = SpaceId(1);
    db.register_space(
        space(1, 2).with_compaction_policy(CompactionPolicy::LatestOnly),
    )
    .unwrap();
    let point = DimensionVector::new(vec![7, 7]);

    let rev1 = db.insert(space_id, point.clone(), vec![1]).unwrap();
    db.insert(space_id, point.clone(), vec![2]).unwrap();
    db.sync().unwrap();
    db.flush(space_id).unwrap();

    // Distinct coordinates so multiple blocks accumulate before compaction.
    for i in 2..10u64 {
        let p = DimensionVector::new(vec![100 + i as u32, 0]);
        db.insert(space_id, p, vec![i as u8]).unwrap();
        db.sync().unwrap();
        db.flush(space_id).unwrap();
    }

    let at_first = db.query(space_id, Some(rev1)).unwrap();
    assert!(at_first.is_empty(), "latest-only compaction drops old revisions");
}

#[test]
fn branch_base_blocks_survive_main_compaction() {
    let dir = TempDir::new().unwrap();
    let mut opts = OpenOptions::default();
    opts.format_version = Some(FORMAT_VERSION_V4);
    opts.io_thread.hot_segment_seal_bytes = 64;
    let db = opts.open(dir.path()).unwrap();
    let space_id = SpaceId(1);
    db.register_space(
        space(1, 2).with_compaction_policy(CompactionPolicy::LatestOnly),
    )
    .unwrap();

    for i in 0..10u64 {
        let point = DimensionVector::new(vec![i as u32, 0]);
        db.insert(space_id, point, vec![i as u8]).unwrap();
        db.sync().unwrap();
        db.flush(space_id).unwrap();
    }

    let branch = db.create_branch("feature", BranchId::MAIN).unwrap();
    let fork_view = db.query_on_branch(branch, space_id, None).unwrap();
    assert_eq!(fork_view.len(), 10);

    for i in 0..8u64 {
        let point = DimensionVector::new(vec![100 + i as u32, 0]);
        db.insert(space_id, point, vec![200 + i as u8]).unwrap();
        db.sync().unwrap();
        db.flush(space_id).unwrap();
    }

    let main_latest = db.query(space_id, None).unwrap();
    assert_eq!(main_latest.len(), 18);

    let branch_after = db.query_on_branch(branch, space_id, None).unwrap();
    assert_eq!(branch_after.len(), 10);
    let mut fork_data: Vec<u8> = fork_view.iter().map(|r| r.data[0]).collect();
    let mut branch_data: Vec<u8> = branch_after.iter().map(|r| r.data[0]).collect();
    fork_data.sort();
    branch_data.sort();
    assert_eq!(fork_data, branch_data);

    let reopened = InfiniteDb::open(dir.path()).unwrap();
    let branch_reopen = reopened.query_on_branch(branch, space_id, None).unwrap();
    assert_eq!(branch_reopen.len(), 10);
}

#[test]
fn compaction_removes_superseded_blocks_without_branches() {
    let dir = TempDir::new().unwrap();
    let mut opts = OpenOptions::default();
    opts.format_version = Some(FORMAT_VERSION_V4);
    opts.io_thread.hot_segment_seal_bytes = 64;
    let db = opts.open(dir.path()).unwrap();
    let space_id = SpaceId(1);
    db.register_space(
        space(1, 2).with_compaction_policy(CompactionPolicy::LatestOnly),
    )
    .unwrap();

    for i in 0..10u64 {
        let point = DimensionVector::new(vec![i as u32, 0]);
        db.insert(space_id, point, vec![i as u8]).unwrap();
        db.sync().unwrap();
        db.flush(space_id).unwrap();
    }

    let blocks_before: HashSet<String> = std::fs::read_dir(dir.path().join("blocks"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    for i in 0..8u64 {
        let point = DimensionVector::new(vec![100 + i as u32, 0]);
        db.insert(space_id, point, vec![200 + i as u8]).unwrap();
        db.sync().unwrap();
        db.flush(space_id).unwrap();
    }

    let blocks_after: HashSet<String> = std::fs::read_dir(dir.path().join("blocks"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    let removed: Vec<_> = blocks_before.difference(&blocks_after).collect();
    assert!(
        !removed.is_empty(),
        "compaction should delete unreferenced superseded block files"
    );

    let reopened = InfiniteDb::open(dir.path()).unwrap();
    let results = reopened.query(space_id, None).unwrap();
    assert_eq!(results.len(), 18);
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

#[test]
fn origin_point_hilbert_key_cached_on_insert() {
    use infinite_db::infinitedb_core::hilbert_key::HilbertKey;
    use infinite_db::infinitedb_index::key::hilbert_key_standard;

    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let space_id = SpaceId(1);
    db.register_space(space(1, 2)).unwrap();
    let origin = DimensionVector::new(vec![0, 0]);

    db.insert(space_id, origin.clone(), vec![1]).unwrap();
    db.sync().unwrap();

    let results = db.query(space_id, None).unwrap();
    assert_eq!(results.len(), 1);
    assert!(!results[0].hilbert_key.is_unset());
    let expected = HilbertKey(hilbert_key_standard(&origin));
    assert_eq!(results[0].hilbert_key.get(), Some(expected));
}
