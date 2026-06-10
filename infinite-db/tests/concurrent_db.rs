//! Phase A: InfiniteDb fire-and-forget writes + concurrent reads.

use std::sync::Arc;
use std::thread;

use infinite_db::infinitedb_core::address::{DimensionVector, SpaceId};
use infinite_db::infinitedb_core::space::SpaceConfig;
use infinite_db::InfiniteDb;
use tempfile::TempDir;

fn space(id: u64, dims: usize) -> SpaceConfig {
    SpaceConfig::new(SpaceId(id), format!("space_{id}"), dims)
}

#[test]
fn fire_and_forget_insert_visible_after_sync() {
    let dir = TempDir::new().unwrap();
    let db = InfiniteDb::open(dir.path()).unwrap();
    let space_id = SpaceId(1);
    db.register_space(space(1, 2)).unwrap();

    db.insert(space_id, DimensionVector::new(vec![1, 2]), vec![42])
        .unwrap();
    db.sync().unwrap();

    let results = db.query(space_id, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].data, vec![42]);
}

#[test]
fn concurrent_enqueues_from_multiple_threads() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(InfiniteDb::open(dir.path()).unwrap());
    db.register_space(space(1, 2)).unwrap();
    let space_id = SpaceId(1);

    let handles: Vec<_> = (0..8)
        .map(|i| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for j in 0..16 {
                    db.insert(
                        space_id,
                        DimensionVector::new(vec![i, j]),
                        vec![i as u8, j as u8],
                    )
                    .unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    db.sync().unwrap();

    let results = db.query(space_id, None).unwrap();
    assert_eq!(results.len(), 128);
}

#[test]
fn concurrent_reads_while_writing() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(InfiniteDb::open(dir.path()).unwrap());
    db.register_space(space(1, 2)).unwrap();
    let space_id = SpaceId(1);

    db.sync().unwrap();
    let read_rev = db.revision();

    let db_write = Arc::clone(&db);
    let writer = thread::spawn(move || {
        for i in 0..50 {
            db_write
                .insert(space_id, DimensionVector::new(vec![i, 0]), vec![i as u8])
                .unwrap();
        }
        db_write.sync().unwrap();
    });

    let db_read = Arc::clone(&db);
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let db = Arc::clone(&db_read);
            thread::spawn(move || {
                for _ in 0..20 {
                    let results = db
                        .query(space_id, Some(infinite_db::infinitedb_core::address::RevisionId(read_rev)))
                        .unwrap();
                    assert!(results.is_empty());
                }
            })
        })
        .collect();

    writer.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }

    let all = db.query(space_id, None).unwrap();
    assert_eq!(all.len(), 50);
}

#[test]
fn v4_format_written_on_create() {
    let dir = TempDir::new().unwrap();
    let _db = InfiniteDb::open(dir.path()).unwrap();
    let bytes = std::fs::read(dir.path().join("meta").join("format_version.bin")).unwrap();
    assert_eq!(u32::from_le_bytes(bytes.try_into().unwrap()), 4);
    assert!(dir.path().join("spaces").is_dir());
}

#[test]
fn durability_after_reopen() {
    let dir = TempDir::new().unwrap();
    let space_id = SpaceId(1);
    {
        let db = InfiniteDb::open(dir.path()).unwrap();
        db.register_space(space(1, 2)).unwrap();
        db.insert(space_id, DimensionVector::new(vec![9, 9]), vec![7])
            .unwrap();
        db.sync().unwrap();
        db.flush(space_id).unwrap();
    }

    let db = InfiniteDb::open(dir.path()).unwrap();
    let results = db.query(space_id, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].data, vec![7]);
}
