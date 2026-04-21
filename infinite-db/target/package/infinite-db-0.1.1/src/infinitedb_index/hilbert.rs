/// N-dimensional Hilbert curve encoding using Skilling's algorithm (2004).
/// Reference: "Programming the Hilbert Curve", John Skilling, AIP Conf. Proc. 707.
///
/// The algorithm works in two stages:
///   Encode: coordinates → transposed Hilbert bits → compact u128 key
///   Decode: compact u128 key → transposed Hilbert bits → coordinates
///
/// The "transposed" form stores Hilbert index bits interleaved across n
/// coordinate slots. `compact`/`decompact` convert between transposed and u128.
///
/// Bit budget: dims × bits_per_dim ≤ 128. Typical use: 8 bits/dim, max 16 dims.

/// N-dimensional Hilbert curve encoding using Skilling's algorithm.
/// Maps a `DimensionVector` to a single `u128` index key.
/// Coordinates are truncated to `bits_per_dim` bits (max 8 for u128 with 16 dims).
///
/// Key property: spatially close points map to numerically close keys,
/// so spatial range queries become contiguous key scans.

/// Encode an N-dimensional point into a 1D Hilbert index.
///
/// - `coords`: unsigned coordinates, one per dimension.
/// - `bits_per_dim`: precision bits per coordinate (1–8). Total key bits = dims × bits_per_dim ≤ 128.
pub fn encode(coords: &[u32], bits_per_dim: u32) -> u128 {
    let n = coords.len();
    assert!(n > 0, "At least one dimension required");
    assert!(
        n as u32 * bits_per_dim <= 128,
        "dims * bits_per_dim must be ≤ 128"
    );
    let b = bits_per_dim;
    let mask = (1u64 << b) - 1;
    let mut x: Vec<u64> = coords.iter().map(|&c| (c as u64) & mask).collect();
    axes_to_transpose(&mut x, b, n);
    compact(&x, n, b)
}

/// Decode a 1D Hilbert key back into an N-dimensional point.
/// Must use the same `dims` and `bits_per_dim` as `encode`.
pub fn decode(key: u128, dims: usize, bits_per_dim: u32) -> Vec<u32> {
    assert!(dims > 0);
    assert!(dims as u32 * bits_per_dim <= 128);
    let mut x = decompact(key, dims, bits_per_dim);
    transpose_to_axes(&mut x, bits_per_dim, dims);
    x.into_iter().map(|v| v as u32).collect()
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Skilling's AxesToTranspose: converts Cartesian coordinates to the
/// "transposed" representation of the Hilbert index, in-place.
fn axes_to_transpose(x: &mut Vec<u64>, b: u32, n: usize) {
    let m = 1u64 << (b - 1);

    // Inverse undo: Q from M down to 2.
    let mut q = m;
    while q > 1 {
        let p = q - 1;
        for i in 0..n {
            if x[i] & q != 0 {
                x[0] ^= p;
            } else {
                let t = (x[0] ^ x[i]) & p;
                x[0] ^= t;
                x[i] ^= t;
            }
        }
        q >>= 1;
    }

    // Gray encode.
    for i in 1..n {
        x[i] ^= x[i - 1];
    }
    let mut t = 0u64;
    let mut q2 = m;
    while q2 > 1 {
        if x[n - 1] & q2 != 0 {
            t ^= q2 - 1;
        }
        q2 >>= 1;
    }
    for xi in x.iter_mut() {
        *xi ^= t;
    }
}

/// Skilling's TransposeToAxes: converts the transposed Hilbert representation
/// back to Cartesian coordinates, in-place.
fn transpose_to_axes(x: &mut Vec<u64>, b: u32, n: usize) {
    // Gray decode.
    let t = x[n - 1] >> 1;
    for i in (1..n).rev() {
        x[i] ^= x[i - 1];
    }
    x[0] ^= t;

    // Undo excess work: P from 2 up to (but not including) 2^b.
    let mut p = 2u64;
    let limit = 1u64 << b;
    while p != limit {
        let q = p - 1;
        for i in (0..n).rev() {
            if x[i] & p != 0 {
                x[0] ^= q;
            } else {
                let t2 = (x[0] ^ x[i]) & q;
                x[0] ^= t2;
                x[i] ^= t2;
            }
        }
        p <<= 1;
    }
}

/// Compact the transposed form (n × b bits) into a single u128.
/// Hilbert bit at MSB-position k → X[k % n] bit (b-1 - k/n).
fn compact(x: &[u64], n: usize, b: u32) -> u128 {
    let total = b as usize * n;
    let mut h: u128 = 0;
    for p in 0..total {
        let q = total - 1 - p;          // position from MSB
        let axis = q % n;
        let bit_in_axis = b as usize - 1 - q / n;
        if x[axis] & (1u64 << bit_in_axis) != 0 {
            h |= 1u128 << p;
        }
    }
    h
}

/// Inverse of `compact`: expand u128 key back to n × b transposed form.
fn decompact(key: u128, n: usize, b: u32) -> Vec<u64> {
    let total = b as usize * n;
    let mut x = vec![0u64; n];
    for p in 0..total {
        let q = total - 1 - p;
        let axis = q % n;
        let bit_in_axis = b as usize - 1 - q / n;
        if key & (1u128 << p) != 0 {
            x[axis] |= 1u64 << bit_in_axis;
        }
    }
    x
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip_2d() {
        let pts = vec![
            vec![0u32, 0],
            vec![1, 0],
            vec![0, 1],
            vec![3, 3],
            vec![7, 5],
        ];
        for pt in &pts {
            let key = encode(pt, 4);
            let decoded = decode(key, 2, 4);
            assert_eq!(*pt, decoded, "roundtrip failed for {:?}", pt);
        }
    }

    #[test]
    fn encode_decode_roundtrip_3d() {
        let pt = vec![3u32, 1, 2];
        let key = encode(&pt, 4);
        let decoded = decode(key, 3, 4);
        assert_eq!(pt, decoded);
    }
}