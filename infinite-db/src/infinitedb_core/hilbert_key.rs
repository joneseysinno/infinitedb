//! Hilbert curve keys and optional cached key state on records.

use bincode::{
    Decode, Encode,
    de::{BorrowDecode, BorrowDecoder, Decoder},
    enc::Encoder,
    error::{DecodeError, EncodeError},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A computed Hilbert curve key for spatial ordering and range routing.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Encode, Decode,
)]
pub struct HilbertKey(pub(crate) u128);

impl HilbertKey {
    pub const ZERO: Self = Self(0);

    pub(crate) fn from_raw(raw: u128) -> Self {
        Self(raw)
    }

    pub fn raw(self) -> u128 {
        self.0
    }
}

/// Optional Hilbert key cached on a record (unset = compute from coordinates).
///
/// On disk this serializes as `u128` with `0` meaning unset (legacy compatibility).
/// Key `0` is both the origin cell's address and the unset sentinel; consumers must
/// treat unset as recompute, which yields the identical key — benign by construction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct CachedHilbertKey(Option<HilbertKey>);

impl CachedHilbertKey {
    pub const UNSET: Self = Self(None);

    pub fn set(key: HilbertKey) -> Self {
        Self(Some(key))
    }

    pub fn get(self) -> Option<HilbertKey> {
        self.0
    }

    pub fn is_unset(self) -> bool {
        self.0.is_none()
    }

    pub fn from_encoded(raw: u128) -> Self {
        if raw == 0 {
            Self::UNSET
        } else {
            Self(Some(HilbertKey(raw)))
        }
    }

    fn encode_u128(self) -> u128 {
        self.0.map(|k| k.0).unwrap_or(0)
    }
}

impl Serialize for CachedHilbertKey {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.encode_u128().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CachedHilbertKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = u128::deserialize(deserializer)?;
        Ok(Self::from_encoded(raw))
    }
}

impl Encode for CachedHilbertKey {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        self.encode_u128().encode(encoder)
    }
}

impl<Context> Decode<Context> for CachedHilbertKey {
    fn decode<D: Decoder<Context = Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let raw = u128::decode(decoder)?;
        Ok(Self::from_encoded(raw))
    }
}

impl<'de, Context> BorrowDecode<'de, Context> for CachedHilbertKey {
    fn borrow_decode<D: BorrowDecoder<'de, Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        let raw = u128::borrow_decode(decoder)?;
        Ok(Self::from_encoded(raw))
    }
}

#[cfg(test)]
mod inv_key_construction {
    //! INV-KEY-CONSTRUCTION: bare `HilbertKey(_)` / `from_raw` stay crate-internal.

    #[test]
    fn curve_address_is_public_bridge() {
        use crate::infinitedb_core::address::DimensionVector;
        use crate::infinitedb_core::hilbert_key::HilbertKey;
        use crate::infinitedb_index::{composite::KeyConfig, CurveAddress};

        let key: HilbertKey =
            CurveAddress::from_point(&DimensionVector::new(vec![1, 2]), KeyConfig { bits_per_dim: 8 })
                .into();
        assert!(key.raw() > 0);
    }
}
