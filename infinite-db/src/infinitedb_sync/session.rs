//! Block-level sync session driver.
//!
//! v1 Merkle/delta wire protocol (`run_sync_session_*`, `converge_spaces`) requires
//! the `legacy-v1` feature. For CRCW [`crate::InfiniteDb`], use
//! [`crate::infinitedb_sync::replicate::converge_main_records`].

use std::io::{self, Read, Write};

use crate::infinitedb_core::address::SpaceId;
use crate::infinitedb_sync::{
    delta::Delta,
    serial::{read_message, write_message, SyncMessage},
};

#[cfg(feature = "legacy-v1")]
use crate::infinitedb_core::{block::Block, snapshot::Snapshot};
#[cfg(feature = "legacy-v1")]
use crate::legacy_v1::LegacyDb;

/// Run a sync session as the initiator (sends our root first, receives delta to apply).
#[cfg(feature = "legacy-v1")]
pub fn run_sync_session_initiator<R: Read, W: Write>(
    local: &mut LegacyDb,
    space: SpaceId,
    reader: &mut R,
    writer: &mut W,
) -> io::Result<()> {
    let local_tree = local.snapshot_merkle(space)?;
    write_message(writer, &SyncMessage::MerkleRoot { root: local_tree.root() })?;

    let remote_root = match read_message(reader)? {
        SyncMessage::MerkleRoot { root } => root,
        SyncMessage::Error { message } => {
            return Err(io::Error::new(io::ErrorKind::Other, message));
        }
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected MerkleRoot, got {other:?}"),
            ));
        }
    };

    if local_tree.root() == remote_root {
        return Ok(());
    }

    let remote_tree = match read_message(reader)? {
        SyncMessage::MerkleTree(t) => t,
        SyncMessage::Error { message } => {
            return Err(io::Error::new(io::ErrorKind::Other, message));
        }
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected MerkleTree, got {other:?}"),
            ));
        }
    };

    let _diffs = local_tree.diff_leaves(&remote_tree);

    let delta = match read_message(reader)? {
        SyncMessage::Delta(d) => d,
        SyncMessage::Error { message } => {
            return Err(io::Error::new(io::ErrorKind::Other, message));
        }
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected Delta, got {other:?}"),
            ));
        }
    };

    local.apply_delta(space, &delta)?;
    write_message(
        writer,
        &SyncMessage::Ack {
            applied_revision: local.revision(),
        },
    )?;
    Ok(())
}

/// Run a sync session as the responder (reads initiator root, may send tree + delta).
#[cfg(feature = "legacy-v1")]
pub fn run_sync_session_responder<R: Read, W: Write>(
    local: &mut LegacyDb,
    space: SpaceId,
    reader: &mut R,
    writer: &mut W,
    supply_delta: impl FnOnce(&Snapshot) -> io::Result<Delta>,
) -> io::Result<()> {
    let local_tree = local.snapshot_merkle(space)?;

    let initiator_root = match read_message(reader)? {
        SyncMessage::MerkleRoot { root } => root,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected MerkleRoot, got {other:?}"),
            ));
        }
    };

    write_message(writer, &SyncMessage::MerkleRoot { root: local_tree.root() })?;

    if initiator_root == local_tree.root() {
        return Ok(());
    }

    write_message(writer, &SyncMessage::MerkleTree(local_tree.clone()))?;

    let local_snap = local
        .snapshot_for_space(space)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no snapshot"))?;
    let delta = supply_delta(&local_snap)?;

    write_message(writer, &SyncMessage::Delta(delta))?;

    match read_message(reader)? {
        SyncMessage::Ack { .. } => Ok(()),
        SyncMessage::Error { message } => Err(io::Error::new(io::ErrorKind::Other, message)),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected Ack, got {other:?}"),
        )),
    }
}

/// In-memory convergence helper for v1 databases (no wire I/O). Requires `legacy-v1`.
#[cfg(feature = "legacy-v1")]
pub fn converge_spaces(
    local: &mut LegacyDb,
    remote: &mut LegacyDb,
    space: SpaceId,
) -> io::Result<()> {
    let remote_snap = remote
        .snapshot_for_space(space)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "remote snapshot"))?;
    let local_snap = local
        .snapshot_for_space(space)
        .unwrap_or_else(|| Snapshot::root(remote_snap.id, space));

    let remote_blocks: Vec<Block> = remote_snap
        .blocks
        .values()
        .map(|e| remote.read_block(e.block_id))
        .collect::<io::Result<_>>()?;

    let delta = Delta::compute(&remote_snap, &local_snap, remote_blocks);
    if delta.is_empty() {
        return Ok(());
    }
    local.apply_delta(space, &delta)
}

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
