use std::collections::HashMap;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use super::address::{RevisionId, SpaceId};
use super::placement::{validate_placement, Placement, PlacementError};

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

/// Retention policy for companion `{name}_errors` spaces (resolved records only).
#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode, PartialEq, Eq)]
pub struct ErrorRetentionPolicy {
    /// Maximum tombstone-resolved error records to retain; older resolved pairs are compacted away.
    #[serde(default)]
    pub max_resolved_keep: Option<usize>,
}

impl ErrorRetentionPolicy {
    /// Retain at most `n` most recently resolved error records.
    pub fn keep_latest_resolved(n: usize) -> Self {
        Self {
            max_resolved_keep: Some(n),
        }
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

/// Dyadic cell-center reservation policy (D-T5).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, Encode, Decode,
)]
pub enum CenterReservation {
    /// No center reservation (legacy default).
    #[default]
    Off,
    /// Reserved centers accept only structural writes (D-T6).
    StructuralOnly,
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
    /// Optional retention for resolved records in the companion error space.
    #[serde(default)]
    pub error_retention: Option<ErrorRetentionPolicy>,
    /// Parent space in the tower hierarchy (D-T2).
    #[serde(default)]
    pub parent: Option<SpaceId>,
    /// Placement within the parent coordinate frame (D-T1/D-T3).
    #[serde(default)]
    pub placement: Option<Placement>,
    /// Opt-in dyadic center reservation (D-T5).
    #[serde(default)]
    pub center_reservation: CenterReservation,
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
            error_retention: None,
            parent: None,
            placement: None,
            center_reservation: CenterReservation::default(),
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

    /// Retention policy for resolved records in the companion error space.
    pub fn with_error_retention(mut self, policy: ErrorRetentionPolicy) -> Self {
        self.error_retention = Some(policy);
        self
    }

    /// Disable auto-registration of a companion error space.
    pub fn without_error_space(mut self) -> Self {
        self.skip_error_space = true;
        self
    }

    /// Set parent space (tower hierarchy).
    pub fn with_parent(mut self, parent: SpaceId) -> Self {
        self.parent = Some(parent);
        self
    }

    /// Set placement within the parent frame.
    pub fn with_placement(mut self, placement: Placement) -> Self {
        self.placement = Some(placement);
        self
    }

    /// Set center-reservation policy.
    pub fn with_center_reservation(mut self, policy: CenterReservation) -> Self {
        self.center_reservation = policy;
        self
    }

    /// Companion error space id, if configured.
    pub fn companion_error_space(&self) -> Option<SpaceId> {
        self.error_space
    }

    /// Compare all fields except `id` (INV-REGISTER-IDEMPOTENT).
    pub fn equivalent_to(&self, other: &SpaceConfig) -> bool {
        self.name == other.name
            && self.dims == other.dims
            && self.bits_per_dim == other.bits_per_dim
            && self.shard_bits == other.shard_bits
            && self.compaction_policy == other.compaction_policy
            && self.endpoint_index_layout == other.endpoint_index_layout
            && self.skip_error_space == other.skip_error_space
            && self.error_retention == other.error_retention
            && self.parent == other.parent
            && self.placement == other.placement
            && self.center_reservation == other.center_reservation
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
        self.validate_tree(&config)?;
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

    /// Idempotent register-or-get (INV-REGISTER-IDEMPOTENT).
    pub fn register_or_get(&mut self, config: SpaceConfig) -> Result<SpaceId, SpaceError> {
        if let Some(existing_id) = self.names.get(&config.name).copied() {
            let existing = self
                .spaces
                .get(&existing_id)
                .ok_or(SpaceError::NotFound(existing_id))?;
            if existing.equivalent_to(&config) {
                return Ok(existing_id);
            }
            return Err(SpaceError::ConfigConflict {
                name: config.name,
                existing_id,
            });
        }
        let id = config.id;
        self.register(config)?;
        Ok(id)
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

    /// Direct children of a space (catalog query T3).
    pub fn children_of(&self, parent: SpaceId) -> Vec<SpaceId> {
        let mut children: Vec<SpaceId> = self
            .spaces
            .values()
            .filter(|c| c.parent == Some(parent))
            .map(|c| c.id)
            .collect();
        children.sort_by_key(|id| id.0);
        children
    }

    /// Placement for a registered child space.
    pub fn placement_of(&self, id: SpaceId) -> Option<&Placement> {
        self.spaces.get(&id)?.placement.as_ref()
    }

    /// Depth-first subtree listing (catalog query T3).
    pub fn subtree(&self, root: SpaceId) -> Vec<(SpaceId, SpaceConfig)> {
        let mut out = Vec::new();
        self.subtree_dfs(root, &mut out);
        out
    }

    fn subtree_dfs(&self, id: SpaceId, out: &mut Vec<(SpaceId, SpaceConfig)>) {
        let Some(config) = self.spaces.get(&id).cloned() else {
            return;
        };
        out.push((id, config));
        let mut children = self.children_of(id);
        for child in children.drain(..) {
            self.subtree_dfs(child, out);
        }
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
    pub fn remove(&mut self, id: SpaceId) -> Result<Option<SpaceConfig>, SpaceError> {
        if !self.children_of(id).is_empty() {
            return Err(SpaceError::HasChildren(id));
        }
        if let Some(config) = self.spaces.remove(&id) {
            self.names.remove(&config.name);
            Ok(Some(config))
        } else {
            Ok(None)
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
        self.validate_tree(&config)?;
        if existing.name != config.name {
            self.names.remove(&existing.name);
            self.names.insert(config.name.clone(), config.id);
        }
        self.spaces.insert(config.id, config);
        Ok(())
    }

    fn validate_tree(&self, config: &SpaceConfig) -> Result<(), SpaceError> {
        if let Some(parent_id) = config.parent {
            if parent_id == config.id {
                return Err(SpaceError::Cycle(config.id));
            }
            let parent = self
                .get(parent_id)
                .ok_or(SpaceError::ParentNotFound(parent_id))?;
            validate_placement(
                config.dims,
                parent.dims,
                parent.bits_per_dim,
                config.parent,
                config.placement.as_ref(),
            )
            .map_err(SpaceError::from)?;
            // Acyclicity: walk parent chain.
            let mut walk = parent.parent;
            while let Some(pid) = walk {
                if pid == config.id {
                    return Err(SpaceError::Cycle(config.id));
                }
                walk = self.get(pid).and_then(|c| c.parent);
            }
        } else if config.placement.is_some() {
            return Err(SpaceError::PlacementError(PlacementError::ParentRequired));
        }
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
    /// Parent space does not exist (INV-TREE-PARENT-EXISTS).
    ParentNotFound(SpaceId),
    /// Registration would introduce a cycle (INV-TREE-ACYCLIC).
    Cycle(SpaceId),
    /// Placement validation failed (INV-PLACEMENT-CONTAINED).
    PlacementError(PlacementError),
    /// Cannot remove a space with registered children.
    HasChildren(SpaceId),
    /// Same name, conflicting configuration (INV-REGISTER-IDEMPOTENT).
    ConfigConflict {
        name: String,
        existing_id: SpaceId,
    },
}

impl From<PlacementError> for SpaceError {
    fn from(e: PlacementError) -> Self {
        SpaceError::PlacementError(e)
    }
}

impl std::fmt::Display for SpaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpaceError::DuplicateId(id) => write!(f, "duplicate space id {:?}", id),
            SpaceError::DuplicateName(n) => write!(f, "duplicate space name {n}"),
            SpaceError::NotFound(id) => write!(f, "space {:?} not found", id),
            SpaceError::ParentNotFound(id) => write!(f, "parent space {:?} not found", id),
            SpaceError::Cycle(id) => write!(f, "cycle detected at space {:?}", id),
            SpaceError::PlacementError(e) => write!(f, "placement error: {e:?}"),
            SpaceError::HasChildren(id) => write!(f, "space {:?} has children", id),
            SpaceError::ConfigConflict { name, existing_id } => {
                write!(f, "config conflict for name {name}, existing {:?}", existing_id)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infinitedb_core::placement::Placement;

    #[test]
    fn legacy_config_decode_defaults() {
        let legacy = SpaceConfig::new(SpaceId(1), "legacy", 2);
        let bytes = bincode::encode_to_vec(&legacy, bincode::config::standard()).unwrap();
        let (decoded, _): (SpaceConfig, _) =
            bincode::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert!(decoded.parent.is_none());
        assert!(decoded.placement.is_none());
        assert_eq!(decoded.center_reservation, CenterReservation::Off);
    }

    #[test]
    fn deep_chain_registers() {
        let mut reg = SpaceRegistry::new();
        reg.register(SpaceConfig::new(SpaceId(1), "root", 3)).unwrap();
        let child_p = Placement {
            offset: vec![0, 0],
            scale_num: vec![1, 1],
            scale_den: vec![1, 1],
            extent: vec![64, 64],
            fixed_axes: vec![(2, 0)],
        };
        reg.register(
            SpaceConfig::new(SpaceId(2), "child", 2)
                .with_parent(SpaceId(1))
                .with_placement(child_p),
        )
        .unwrap();
        assert_eq!(reg.children_of(SpaceId(1)), vec![SpaceId(2)]);
    }

    #[test]
    fn cycle_rejected() {
        let mut reg = SpaceRegistry::new();
        reg.register(SpaceConfig::new(SpaceId(1), "a", 2)).unwrap();
        reg.register(
            SpaceConfig::new(SpaceId(2), "b", 2)
                .with_parent(SpaceId(1))
                .with_placement(Placement::axis_aligned(vec![0, 0], 1, 1, vec![64, 64])),
        )
        .unwrap();
        let err = reg.update(
            SpaceConfig::new(SpaceId(1), "a", 2)
                .with_parent(SpaceId(2))
                .with_placement(Placement::axis_aligned(vec![0, 0], 1, 1, vec![64, 64])),
        );
        assert!(matches!(err, Err(SpaceError::Cycle(_))));
    }

    #[test]
    fn register_or_get_idempotent() {
        let mut reg = SpaceRegistry::new();
        let cfg = SpaceConfig::new(SpaceId(10), "idem", 2);
        assert_eq!(reg.register_or_get(cfg.clone()).unwrap(), SpaceId(10));
        assert_eq!(
            reg.register_or_get(SpaceConfig::new(SpaceId(99), "idem", 2)).unwrap(),
            SpaceId(10)
        );
    }
}
