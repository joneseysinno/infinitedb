//! Blake3 block checksum newtype.

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// Blake3 digest of a block's serialized records.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode,
)]
pub struct Checksum(pub [u8; 32]);

impl Checksum {
    pub const ZERO: Self = Self([0u8; 32]);

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}
