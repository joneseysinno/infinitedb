//! Merkle tree for snapshot verification and sync.
//!
//! Each leaf is the blake3 hash of a serialised `Record`.
//! Internal nodes are blake3 hashes of their two children concatenated.
//! The root hash uniquely identifies the full dataset at a snapshot;
//! two nodes can compare roots to determine whether they are in sync
//! without transferring any record data.
//!
//! Tree structure:
//!   - Leaves are ordered by Hilbert key (u128), matching block order.
//!   - The tree is padded to the next power of two with zero hashes.
//!   - Depth is ceil(log2(leaf_count)).

use blake3::Hasher;
use bincode::{Decode, Encode};

/// A 32-byte blake3 hash.
pub type Hash = [u8; 32];

const ZERO_HASH: Hash = [0u8; 32];

/// A complete Merkle tree over a sorted sequence of leaf hashes.
#[derive(Debug, Clone, Encode, Decode)]
pub struct MerkleTree {
    /// All nodes stored in a flat array, level-order (BFS).
    /// Index 0 = root. Children of node i: 2i+1 (left), 2i+2 (right).
    nodes: Vec<Hash>,
    /// Number of real leaves (before padding).
    pub leaf_count: usize,
}

impl MerkleTree {
    /// Build a Merkle tree from an ordered slice of leaf hashes.
    pub fn build(leaves: &[Hash]) -> Self {
        if leaves.is_empty() {
            return Self { nodes: vec![ZERO_HASH], leaf_count: 0 };
        }
        // Pad to next power of two.
        let size = leaves.len().next_power_of_two();
        let mut level: Vec<Hash> = leaves.to_vec();
        level.resize(size, ZERO_HASH);

        // Total nodes in a complete binary tree of `size` leaves.
        let total = 2 * size - 1;
        let mut nodes = vec![ZERO_HASH; total];
        // Fill leaves (right-most level).
        let leaf_start = size - 1;
        for (i, h) in level.iter().enumerate() {
            nodes[leaf_start + i] = *h;
        }
        // Build internal nodes bottom-up.
        for i in (0..leaf_start).rev() {
            nodes[i] = hash_pair(&nodes[2 * i + 1], &nodes[2 * i + 2]);
        }
        Self { nodes, leaf_count: leaves.len() }
    }

    /// The root hash representing the entire dataset.
    pub fn root(&self) -> Hash {
        self.nodes[0]
    }

    /// Returns the indices of leaves that differ between `self` and `other`.
    /// Both trees must have been built from the same number of leaves.
    pub fn diff_leaves(&self, other: &MerkleTree) -> Vec<usize> {
        let mut diffs = Vec::new();
        self.diff_recursive(other, 0, &mut diffs);
        diffs
    }

    fn diff_recursive(&self, other: &MerkleTree, idx: usize, out: &mut Vec<usize>) {
        if idx >= self.nodes.len() || idx >= other.nodes.len() {
            return;
        }
        if self.nodes[idx] == other.nodes[idx] {
            return; // Subtrees are identical — skip entirely.
        }
        let leaf_start = (self.nodes.len() + 1) / 2 - 1;
        if idx >= leaf_start {
            // This is a leaf.
            let leaf_idx = idx - leaf_start;
            if leaf_idx < self.leaf_count {
                out.push(leaf_idx);
            }
        } else {
            self.diff_recursive(other, 2 * idx + 1, out);
            self.diff_recursive(other, 2 * idx + 2, out);
        }
    }
}

/// Hash a single record's raw bytes.
pub fn hash_record(data: &[u8]) -> Hash {
    let mut h = Hasher::new();
    h.update(data);
    *h.finalize().as_bytes()
}

fn hash_pair(left: &Hash, right: &Hash) -> Hash {
    let mut h = Hasher::new();
    h.update(left);
    h.update(right);
    *h.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(v: u8) -> Hash {
        let mut h = [0u8; 32];
        h[0] = v;
        h
    }

    #[test]
    fn identical_trees_have_same_root() {
        let leaves = vec![leaf(1), leaf(2), leaf(3)];
        let a = MerkleTree::build(&leaves);
        let b = MerkleTree::build(&leaves);
        assert_eq!(a.root(), b.root());
    }

    #[test]
    fn different_leaf_detected() {
        let a = MerkleTree::build(&[leaf(1), leaf(2), leaf(3)]);
        let mut changed = vec![leaf(1), leaf(9), leaf(3)];
        let b = MerkleTree::build(&changed);
        assert_ne!(a.root(), b.root());
        let diffs = a.diff_leaves(&b);
        assert_eq!(diffs, vec![1]);
    }

    #[test]
    fn empty_tree_root_is_zero() {
        let t = MerkleTree::build(&[]);
        assert_eq!(t.root(), ZERO_HASH);
    }
}
