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

/// Endpoint reverse-index coordinate layout (reserved `ENDPOINT_INDEX_SPACE` only).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, Encode, Decode,
)]
pub enum EndpointIndexLayout {
    /// M1 symmetric layout: no polarity coordinate dimension.
    #[default]
    V1Symmetric,
    /// M2 layout: polarity dimension between endpoint coords and edge-id dimensions.
    V2PolarityDim,
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
    /// Hilbert range sharding within this space (format v4).
    ///
    /// `shard_id = hilbert_key >> (128 - shard_bits)`. Default `4` → 16 shards.
    /// `0` disables intra-space sharding (single I/O thread per space).
    pub shard_bits: u32,
    /// Compaction history retention for this space.
    #[serde(default)]
    pub compaction_policy: CompactionPolicy,
    /// Endpoint index layout version (meaningful only for `ENDPOINT_INDEX_SPACE`).
    #[serde(default)]
    pub endpoint_index_layout: EndpointIndexLayout,
    /// Companion space for operation-level error records (M5).
    #[serde(default)]
    pub error_space: Option<SpaceId>,
    /// When true, do not auto-register a companion error space on registration.
    #[serde(default)]
    pub skip_error_space: bool,
}

impl SpaceConfig {
    /// Create a space configuration with the standard 8-bit Hilbert precision.
    pub fn new(id: SpaceId, name: impl Into<String>, dims: usize) -> Self {
        Self {
            id,
            name: name.into(),
            dims,
            bits_per_dim: 8,
            shard_bits: 4,
            compaction_policy: CompactionPolicy::default(),
            endpoint_index_layout: EndpointIndexLayout::default(),
            error_space: None,
            skip_error_space: false,
        }
    }

    /// Override the Hilbert precision (bits per dimension) for this space.
    pub fn with_bits_per_dim(mut self, bits_per_dim: u32) -> Self {
        self.bits_per_dim = bits_per_dim;
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

    /// Override endpoint index layout (reserved index space).
    pub fn with_endpoint_index_layout(mut self, layout: EndpointIndexLayout) -> Self {
        self.endpoint_index_layout = layout;
        self
    }

    /// Link an existing companion error space.
    pub fn with_error_space(mut self, error_space: SpaceId) -> Self {
        self.error_space = Some(error_space);
        self
    }

    /// Disable auto-registration of a companion error space.
    pub fn without_error_space(mut self) -> Self {
        self.skip_error_space = true;
        self
    }

    /// Companion error space id, if configured.
    pub fn companion_error_space(&self) -> Option<SpaceId> {
        self.error_space
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

    /// Companion error space for a data space, if linked.
    pub fn error_space_for(&self, data_space: SpaceId) -> Option<SpaceId> {
        self.get(data_space)?.error_space
    }

    /// Derive a companion error space id from a data space id.
    pub fn derive_error_space_id(data_space: SpaceId) -> SpaceId {
        SpaceId(data_space.0 ^ 0xE000_0000_0000_0000)
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

    /// Replace an existing space configuration (e.g. endpoint index layout upgrade).
    pub fn update(&mut self, config: SpaceConfig) -> Result<(), SpaceError> {
        let existing = self
            .spaces
            .get(&config.id)
            .ok_or(SpaceError::NotFound(config.id))?;
        if existing.name != config.name && self.names.contains_key(&config.name) {
            return Err(SpaceError::DuplicateName(config.name));
        }
        if existing.name != config.name {
            self.names.remove(&existing.name);
            self.names.insert(config.name.clone(), config.id);
        }
        self.spaces.insert(config.id, config);
        Ok(())
    }
}

/// Errors returned by space registry operations.
#[derive(Debug, Clone)]
pub enum SpaceError {
    /// The provided `SpaceId` is already registered.
    DuplicateId(SpaceId),
    /// The provided space name is already registered.
    DuplicateName(String),
    /// A requested space does not exist.
    NotFound(SpaceId),
}