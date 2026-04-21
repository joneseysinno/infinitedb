//! Snapshot wire encoding for sync transport.
//!
//! `serial` handles the boundary between in-memory snapshots and the bytes
//! that travel over the network or get written to a sync-exchange file.
//!
//! Encoding: bincode (little-endian, fixed-int). Each message is framed as:
//!   [4 bytes: message type tag] [8 bytes: payload length] [N bytes: payload]
//!
//! This framing allows a receiver to skip unknown message types for
//! forward compatibility.

use std::io::{self, Read, Write};
use bincode::{config::standard, decode_from_slice, encode_to_vec, Decode, Encode};
use crate::infinitedb_core::snapshot::Snapshot;
use crate::infinitedb_sync::{
    delta::Delta,
    merkle::MerkleTree,
};

/// Message types exchanged during a sync session.
#[derive(Debug, Encode, Decode)]
pub enum SyncMessage {
    /// Initiator sends its Merkle root to begin negotiation.
    MerkleRoot { root: [u8; 32] },
    /// Responder sends its own tree when roots differ, for leaf-level diffing.
    MerkleTree(MerkleTree),
    /// Initiator sends the computed delta.
    Delta(Delta),
    /// Receiver acknowledges successful application of a delta.
    Ack { applied_revision: u64 },
    /// Either side signals an error.
    Error { message: String },
}

// ---------------------------------------------------------------------------
// Framed write / read
// ---------------------------------------------------------------------------

/// Write a `SyncMessage` to any `Write` sink with length framing.
pub fn write_message<W: Write>(sink: &mut W, msg: &SyncMessage) -> io::Result<()> {
    let payload = encode_to_vec(msg, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let len = payload.len() as u64;
    sink.write_all(&len.to_le_bytes())?;
    sink.write_all(&payload)
}

/// Read a `SyncMessage` from any `Read` source with length framing.
pub fn read_message<R: Read>(src: &mut R) -> io::Result<SyncMessage> {
    let mut len_buf = [0u8; 8];
    src.read_exact(&mut len_buf)?;
    let len = u64::from_le_bytes(len_buf) as usize;
    // Guard against absurdly large lengths (e.g. corrupt stream).
    if len > 256 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SyncMessage payload exceeds 256 MiB limit",
        ));
    }
    let mut payload = vec![0u8; len];
    src.read_exact(&mut payload)?;
    let (msg, _) = decode_from_slice::<SyncMessage, _>(&payload, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(msg)
}

/// Serialise a `Snapshot` header for exchange (no block data included).
pub fn encode_snapshot(snapshot: &Snapshot) -> io::Result<Vec<u8>> {
    encode_to_vec(snapshot, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

/// Deserialise a `Snapshot` header received from a remote node.
pub fn decode_snapshot(bytes: &[u8]) -> io::Result<Snapshot> {
    let (snap, _) = decode_from_slice::<Snapshot, _>(bytes, standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(snap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn roundtrip_merkle_root_message() {
        let msg = SyncMessage::MerkleRoot { root: [7u8; 32] };
        let mut buf = Vec::new();
        write_message(&mut buf, &msg).unwrap();
        let mut cursor = Cursor::new(buf);
        let decoded = read_message(&mut cursor).unwrap();
        assert!(matches!(decoded, SyncMessage::MerkleRoot { root } if root == [7u8; 32]));
    }

    #[test]
    fn roundtrip_ack_message() {
        let msg = SyncMessage::Ack { applied_revision: 42 };
        let mut buf = Vec::new();
        write_message(&mut buf, &msg).unwrap();
        let mut cursor = Cursor::new(buf);
        let decoded = read_message(&mut cursor).unwrap();
        assert!(matches!(decoded, SyncMessage::Ack { applied_revision: 42 }));
    }
}
