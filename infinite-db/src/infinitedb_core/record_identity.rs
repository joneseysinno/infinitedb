//! Record and address identity keys for grouping without coordinate clones.

use std::hash::Hash;

use super::address::{RevisionId, SpaceId};
use super::block::Record;
use super::hilbert_key::HilbertKey;

/// Address identity within a space (Hilbert key when cached, else coordinates).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AddressIdentity {
    Hilbert(HilbertKey),
    Coords(Vec<u32>),
}

/// Full record identity (address + revision) for seal deduplication.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecordIdentityKey {
    pub space: SpaceId,
    pub address: AddressIdentity,
    pub revision: RevisionId,
}

/// Address-only key for latest-wins visibility grouping.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AddressKey {
    pub space: SpaceId,
    pub address: AddressIdentity,
}

impl AddressKey {
    pub fn from_hilbert(record: &Record, hilbert: HilbertKey) -> Self {
        Self {
            space: record.address.space,
            address: AddressIdentity::Hilbert(hilbert),
        }
    }
}

impl RecordIdentityKey {
    pub fn from_hilbert(record: &Record, hilbert: HilbertKey) -> Self {
        Self {
            space: record.address.space,
            address: AddressIdentity::Hilbert(hilbert),
            revision: record.revision,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::address::{Address, DimensionVector};
    use crate::infinitedb_core::hilbert_key::CachedHilbertKey;

    #[test]
    fn cached_and_unset_keys_group_equally() {
        let space = SpaceId(1);
        let point = DimensionVector::new(vec![3, 7]);
        let hilbert = HilbertKey(42);
        let cached = Record {
            address: Address::new(space, point.clone()),
            revision: RevisionId(1),
            data: vec![1],
            tombstone: false,
            hilbert_key: CachedHilbertKey::set(hilbert),
        };
        let unset = Record {
            hilbert_key: CachedHilbertKey::UNSET,
            ..cached.clone()
        };
        assert_eq!(
            AddressKey::from_hilbert(&cached, hilbert),
            AddressKey::from_hilbert(&unset, hilbert),
        );
    }
}
