use std::collections::HashMap;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use super::address::{RevisionId, SpaceId};

/// History retention mode for compaction in a space.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, PartialEq, Eq)]
pub enum CompactionPolicy {
    /// Keep all revisions (default).
    KeepAll,
    /// Drop revisions older than the configured horizons during compaction.
    RetentionWindow {
        version_horizon: RevisionId,
        tombstone_horizon: RevisionId,
    },
    /// Latest revision per address only (destructive).
    LatestOnly,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self::KeepAll
    }
}

/// Configuration for a registered space.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct SpaceConfig {
    pub id: SpaceId,
    /// Human-readable name.
    pub name: String,
    /// Number of dimensions all records in this space must have.
    pub dims: usize,
    /// Hilbert precision (bits per dimension) for this space.
    ///
    /// This is a load-bearing invariant: every block in the space must be keyed
    /// at the same precision or `BTreeMap` ordering breaks. It is fixed when the
    /// space is registered and must satisfy `dims * bits_per_dim <= 128`.
    pub bits_per_dim: u32,
    /// Experimental: key hyperedges in this space by the centroid of their
    /// endpoints (for spatial locality) instead of by `HyperedgeId`.
    ///
    /// When enabled, the database maintains an id→point locator so edges remain
    /// addressable by id. Defaults to `false`; only affects hyperedge spaces.
    pub centroid_keying: bool,
    /// Hilbert range sharding within this space (format v4).
    ///
    /// `shard_id = hilbert_key >> (128 - shard_bits)`. Default `4` → 16 shards.
    /// `0` disables intra-space sharding (single I/O thread per space).
    pub shard_bits: u32,
    /// Compaction history retention for this space.
    #[serde(default)]
    pub compaction_policy: CompactionPolicy,
}

impl SpaceConfig {
    /// Create a space configuration with the standard 8-bit Hilbert precision.
    pub fn new(id: SpaceId, name: impl Into<String>, dims: usize) -> Self {
        Self {
            id,
            name: name.into(),
            dims,
            bits_per_dim: 8,
            centroid_keying: false,
            shard_bits: 4,
            compaction_policy: CompactionPolicy::default(),
        }
    }

    /// Override the Hilbert precision (bits per dimension) for this space.
    pub fn with_bits_per_dim(mut self, bits_per_dim: u32) -> Self {
        self.bits_per_dim = bits_per_dim;
        self
    }

    /// Enable experimental centroid-based hyperedge keying for this space.
    pub fn with_centroid_keying(mut self) -> Self {
        self.centroid_keying = true;
        self
    }

    /// Override Hilbert range shard count (`2^shard_bits` I/O threads per space).
    pub fn with_shard_bits(mut self, shard_bits: u32) -> Self {
        self.shard_bits = shard_bits;
        self
    }

    /// Override compaction history retention for this space.
    pub fn with_compaction_policy(mut self, policy: CompactionPolicy) -> Self {
        self.compaction_policy = policy;
        self
    }
}

/// Registry of all known spaces in the database.
/// Persisted as part of the database metadata block.
#[derive(Debug, Default, Serialize, Deserialize, Encode, Decode)]
pub struct SpaceRegistry {
    spaces: HashMap<SpaceId, SpaceConfig>,
    names: HashMap<String, SpaceId>,
}

impl SpaceRegistry {
    /// Create an empty space registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new space. Returns an error if the name or ID is already taken.
    pub fn register(&mut self, config: SpaceConfig) -> Result<(), SpaceError> {
        if self.spaces.contains_key(&config.id) {
            return Err(SpaceError::DuplicateId(config.id));
        }
        if self.names.contains_key(&config.name) {
            return Err(SpaceError::DuplicateName(config.name));
        }
        self.names.insert(config.name.clone(), config.id);
        self.spaces.insert(config.id, config);
        Ok(())
    }

    /// Look up a space by ID.
    pub fn get(&self, id: SpaceId) -> Option<&SpaceConfig> {
        self.spaces.get(&id)
    }

    /// Look up a space by name.
    pub fn get_by_name(&self, name: &str) -> Option<&SpaceConfig> {
        self.names.get(name).and_then(|id| self.spaces.get(id))
    }

    /// Return all registered space IDs.
    pub fn space_ids(&self) -> Vec<SpaceId> {
        self.spaces.keys().copied().collect()
    }

    /// Remove a space and return its previous configuration, if it existed.
    pub fn remove(&mut self, id: SpaceId) -> Option<SpaceConfig> {
        if let Some(config) = self.spaces.remove(&id) {
            self.names.remove(&config.name);
            Some(config)
        } else {
            None
        }
    }
}

/// Errors returned by space registry operations.
#[derive(Debug)]
pub enum SpaceError {
    /// The provided `SpaceId` is already registered.
    DuplicateId(SpaceId),
    /// The provided space name is already registered.
    DuplicateName(String),
    /// A requested space does not exist.
    NotFound(SpaceId),
}