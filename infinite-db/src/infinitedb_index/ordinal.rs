/// Encodes ordered (scalar) values — integers, floats, enums — into a u32
/// coordinate suitable for use as a Hilbert dimension.
///
/// All encodings preserve sort order: encode(a) < encode(b) iff a < b.

/// Encode a signed 32-bit integer into an unsigned u32 coordinate.
/// Maps i32::MIN → 0, 0 → 2^31, i32::MAX → u32::MAX.
pub fn encode_i32(v: i32) -> u32 {
    (v as i64 + i32::MAX as i64 + 1) as u32
}

/// Decode a coordinate produced by [`encode_i32`].
pub fn decode_i32(v: u32) -> i32 {
    (v as i64 - i32::MAX as i64 - 1) as i32
}

/// Encode an f32 into a u32 that preserves sort order.
/// NaN is not permitted.
pub fn encode_f32(v: f32) -> u32 {
    assert!(!v.is_nan(), "NaN cannot be used as a spatial coordinate");
    let bits = v.to_bits();
    // Flip sign bit; if negative, flip all bits to restore order.
    if bits >> 31 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000
    }
}

/// Decode a coordinate produced by [`encode_f32`].
pub fn decode_f32(v: u32) -> f32 {
    let bits = if v >> 31 != 0 {
        v ^ 0x8000_0000
    } else {
        !v
    };
    f32::from_bits(bits)
}

/// Encode an enum variant (any type that converts to u32) as a coordinate.
/// Variants must be contiguous and fit within u32.
pub fn encode_enum(variant: u32) -> u32 {
    variant
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn i32_order_preserved() {
        assert!(encode_i32(-100) < encode_i32(0));
        assert!(encode_i32(0) < encode_i32(100));
        assert_eq!(decode_i32(encode_i32(-42)), -42);
        assert_eq!(decode_i32(encode_i32(42)), 42);
    }

    #[test]
    fn f32_order_preserved() {
        assert!(encode_f32(-1.0) < encode_f32(0.0));
        assert!(encode_f32(0.0) < encode_f32(1.0));
        assert_eq!(decode_f32(encode_f32(3.14_f32)), 3.14_f32);
    }
}