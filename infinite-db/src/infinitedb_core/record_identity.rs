//! Record and address identity keys for grouping without coordinate clones.

use std::hash::Hash;

use super::address::{RevisionId, SpaceId};
use super::block::Record;
use super::hilbert_key::{CachedHilbertKey, HilbertKey};

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
    pub fn from_record(record: &Record) -> Self {
        Self {
            space: record.address.space,
            address: address_identity_from_record(record),
        }
    }
}

impl RecordIdentityKey {
    pub fn from_record(record: &Record) -> Self {
        Self {
            space: record.address.space,
            address: address_identity_from_record(record),
            revision: record.revision,
        }
    }
}

/// Canonical address identity: coordinates are authoritative; Hilbert is routing cache.
fn address_identity_from_record(record: &Record) -> AddressIdentity {
    address_identity_from_cached(
        record.address.space,
        record.hilbert_key,
        &record.address.point.coords,
    )
}

fn address_identity_from_cached(
    _space: SpaceId,
    _key: CachedHilbertKey,
    coords: &[u32],
) -> AddressIdentity {
    AddressIdentity::Coords(coords.to_vec())
}
