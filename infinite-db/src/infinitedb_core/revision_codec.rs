//! Versioned on-wire revision encoding (peer track Phase 1).
//!
//! V4 and earlier: bare `u64` legacy sequence (bincode-compatible).
//! V5: optional tagged `u128` packed HLC for new sealed blocks.

use std::io;

use super::{address::RevisionId, hlc::HlcStamp};

/// Minimum database format version that may write tagged HLC revisions in new blocks.
pub const FORMAT_VERSION_HLC_REVISION: u32 = 5;

/// Wire tag for packed HLC revision (v5+ blocks).
pub const REVISION_WIRE_HLC_TAG: u8 = 0xD1;

/// Whether revisions on disk use bare u64 or tagged u128.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevisionWireFormat {
    /// Pre-v5 databases and sealed blocks.
    LegacyU64,
    /// Format v5+ packed HLC in newly sealed blocks.
    HlcU128,
}

impl RevisionWireFormat {
    pub fn from_format_version(version: u32) -> Self {
        if version >= FORMAT_VERSION_HLC_REVISION {
            Self::HlcU128
        } else {
            Self::LegacyU64
        }
    }
}

/// Encode a revision for explicit wire contexts (block migration, tests).
pub fn encode_revision_hlc(rev: RevisionId) -> Vec<u8> {
    let mut out = vec![REVISION_WIRE_HLC_TAG];
    out.extend_from_slice(&rev.stamp().pack().to_le_bytes());
    out
}

/// Decode tagged HLC revision bytes.
pub fn decode_revision_hlc(bytes: &[u8]) -> Result<RevisionId, io::Error> {
    if bytes.first() != Some(&REVISION_WIRE_HLC_TAG) || bytes.len() < 17 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid HLC revision wire",
        ));
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&bytes[1..17]);
    Ok(RevisionId::from_stamp(HlcStamp::unpack(u128::from_le_bytes(buf))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hlc_wire_roundtrip() {
        let rev = RevisionId::from_stamp(HlcStamp {
            physical_ms: 99,
            logical: 1,
            session: 2,
            sequence: 3,
        });
        let bytes = encode_revision_hlc(rev);
        assert_eq!(decode_revision_hlc(&bytes).unwrap(), rev);
    }
}
