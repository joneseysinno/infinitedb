//! Minimal embedded database: open, register a space, insert, sync, query.

use infinite_db::infinitedb_core::address::{DimensionVector, SpaceId};
use infinite_db::infinitedb_core::space::SpaceConfig;
use infinite_db::InfiniteDb;

fn main() -> std::io::Result<()> {
    let dir = std::env::temp_dir().join("infinite-db-quickstart");
    let db = InfiniteDb::open(&dir)?;

    db.register_space(SpaceConfig::new(SpaceId(1), "demo", 2))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    db.insert(SpaceId(1), DimensionVector::new(vec![0, 0]), vec![1, 2, 3])?;
    db.sync()?;

    let rows = db.query(SpaceId(1), None)?;
    println!("{} record(s) at {:?}", rows.len(), dir);
    Ok(())
}
