//! Phase B: per-space I/O threads and parallel writes across spaces.

use std::sync::Arc;
use std::thread;

use infinite_db::infinitedb_core::address::{DimensionVector, SpaceId};
use infinite_db::infinitedb_core::branch::BranchId;
use infinite_db::infinitedb_core::space::SpaceConfig;
use infinite_db::{InfiniteDb, OpenOptions, FORMAT_VERSION_V3};
use tempfile::TempDir;

fn open_v3(dir: &tempfile::TempDir) -> InfiniteDb {
    OpenOptions {
        format_version: Some(FORMAT_VERSION_V3),
        ..OpenOptions::default()
    }
    .open(dir.path())
    .unwrap()
}

fn space(id: u64, dims: usize) -> SpaceConfig {
    SpaceConfig::new(SpaceId(id), format!("space_{id}"), dims)
}

#[test]
fn lazy_space_io_threads_spawn_on_first_write() {
    let dir = TempDir::new().unwrap();
    let db = open_v3(&dir);
    db.register_space(space(1, 2)).unwrap();
    db.register_space(space(2, 2)).unwrap();
    assert_eq!(db.space_shard_count(), 0);

    db.insert(SpaceId(1), DimensionVector::new(vec![1, 1]), vec![1])
        .unwrap();
    db.sync().unwrap();
    assert_eq!(db.space_shard_count(), 1);

    db.insert(SpaceId(2), DimensionVector::new(vec![2, 2]), vec![2])
        .unwrap();
    db.sync().unwrap();
    assert_eq!(db.space_shard_count(), 2);
}

#[test]
fn parallel_writes_across_spaces() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(open_v3(&dir));
    db.register_space(space(1, 2)).unwrap();
    db.register_space(space(2, 2)).unwrap();
    db.register_space(space(3, 2)).unwrap();
    db.register_space(space(4, 2)).unwrap();

    let handles: Vec<_> = (1..=4u64)
        .map(|space_n| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                let space_id = SpaceId(space_n);
                for i in 0..32 {
                    db.insert(space_id, DimensionVector::new(vec![i, space_n as u32]), vec![i as u8])
                        .unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    db.sync().unwrap();

    for space_n in 1..=4 {
        let results = db.query(SpaceId(space_n), None).unwrap();
        assert_eq!(results.len(), 32, "space {space_n}");
    }
}

#[test]
fn enqueue_batch_cross_space() {
    let dir = TempDir::new().unwrap();
    let db = open_v3(&dir);
    db.register_space(space(10, 2)).unwrap();
    db.register_space(space(20, 2)).unwrap();

    use infinite_db::infinitedb_core::address::Address;
    use infinite_db::WriteJob;
    use infinite_db::infinitedb_storage::wal::WalEntry;

    let (rev1, rev2) = db.allocate_revisions(2);

    let jobs = vec![
        WriteJob {
            branch_id: BranchId::MAIN,
            revision: rev1,
            entry: WalEntry::Write {
                address: Address::new(SpaceId(10), DimensionVector::new(vec![1, 1])),
                revision: rev1,
                data: vec![1],
            },
            hilbert_key: infinite_db::infinitedb_core::hilbert_key::HilbertKey::ZERO,
        },
        WriteJob {
            branch_id: BranchId::MAIN,
            revision: rev2,
            entry: WalEntry::Write {
                address: Address::new(SpaceId(20), DimensionVector::new(vec![2, 2])),
                revision: rev2,
                data: vec![2],
            },
            hilbert_key: infinite_db::infinitedb_core::hilbert_key::HilbertKey::ZERO,
        },
    ];

    db.enqueue_batch(jobs).unwrap();
    db.sync().unwrap();

    assert_eq!(db.query(SpaceId(10), None).unwrap().len(), 1);
    assert_eq!(db.query(SpaceId(20), None).unwrap().len(), 1);
}

#[test]
fn v3_per_space_paths_after_write() {
    let dir = TempDir::new().unwrap();
    let db = open_v3(&dir);
    let space_id = SpaceId(5);
    db.register_space(space(5, 2)).unwrap();
    db.insert(space_id, DimensionVector::new(vec![1, 2]), vec![9]).unwrap();
    db.sync().unwrap();

    let space_dir = dir.path().join("spaces").join("5");
    assert!(space_dir.join("hot.seg").exists() || space_dir.join("wal/staging.log").exists());
}

#[test]
fn v3_durability_after_reopen() {
    let dir = TempDir::new().unwrap();
    let space_id = SpaceId(7);
    {
        let db = open_v3(&dir);
        db.register_space(space(7, 2)).unwrap();
        db.insert(space_id, DimensionVector::new(vec![3, 3]), vec![99]).unwrap();
        db.sync().unwrap();
        db.flush(space_id).unwrap();
    }

    let db = open_v3(&dir);
    assert_eq!(db.format_version(), 3);
    let results = db.query(space_id, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].data, vec![99]);
}
