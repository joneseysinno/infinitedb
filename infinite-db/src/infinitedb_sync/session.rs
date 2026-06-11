//! Block-level sync session driver.
//!
//! For CRCW [`crate::InfiniteDb`], use [`crate::infinitedb_sync::replicate::converge_main_records`].

#[cfg(test)]
mod tests {
    use crate::infinitedb_core::address::{DimensionVector, SpaceId};
    use crate::infinitedb_core::branch::BranchId;
    use crate::infinitedb_core::space::SpaceConfig;
    use crate::infinitedb_sync::replicate::{converge_main_records, snapshot_merkle};
    use crate::InfiniteDb;
    use tempfile::TempDir;

    #[test]
    fn converge_brings_diverged_crcw_databases_to_same_merkle_root() {
        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();
        let space = SpaceId(1);

        let a = InfiniteDb::open(dir_a.path()).unwrap();
        let b = InfiniteDb::open(dir_b.path()).unwrap();
        a.register_space(SpaceConfig::new(space, "s", 2)).unwrap();
        b.register_space(SpaceConfig::new(space, "s", 2)).unwrap();

        b.insert(space, DimensionVector::new(vec![2, 2]), vec![2])
            .unwrap();
        b.sync().unwrap();

        let root_b = snapshot_merkle(&b, space, BranchId::MAIN).unwrap().root();
        assert!(a.query(space, None).unwrap().is_empty());

        converge_main_records(&a, &b, space).unwrap();
        assert_eq!(
            snapshot_merkle(&a, space, BranchId::MAIN).unwrap().root(),
            root_b
        );
    }
}
