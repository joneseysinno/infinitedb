use std::collections::HashMap;
use bincode::{
    de::{BorrowDecoder, Decoder},
    enc::Encoder,
    error::{DecodeError, EncodeError},
    BorrowDecode, Decode, Encode,
};
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
    /// Compact key: interned space ordinal + geometry + polarity + truncated `valid_from`.
    /// Identity fields (`SpaceId`, `HyperedgeId`) live in the payload, not the curve key.
    V3CompactKey,
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

    /// Companion `{name}_errors` space: 2D × 32-bit so [`super::error_record::error_storage_point`]
    /// can store a packed revision without overflowing Hilbert precision.
    pub fn new_error_companion(id: SpaceId, name: impl Into<String>) -> Self {
        Self::new(id, name, 2)
            .with_bits_per_dim(32)
            .without_error_space()
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
///
/// `ordinals` / `next_ordinal` are assigned at register time and persisted in a
/// sidecar (`space_ordinals.bin`) so `spaces.bin` stays backward-compatible.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SpaceRegistry {
    spaces: HashMap<SpaceId, SpaceConfig>,
    names: HashMap<String, SpaceId>,
    #[serde(skip)]
    ordinals: HashMap<SpaceId, u32>,
    #[serde(skip)]
    next_ordinal: u32,
}

impl Encode for SpaceRegistry {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        self.spaces.encode(encoder)?;
        self.names.encode(encoder)?;
        Ok(())
    }
}

impl<Context> Decode<Context> for SpaceRegistry {
    fn decode<D: Decoder<Context = Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
        Ok(Self {
            spaces: HashMap::decode(decoder)?,
            names: HashMap::decode(decoder)?,
            ordinals: HashMap::new(),
            next_ordinal: 0,
        })
    }
}

impl<'de, Context> BorrowDecode<'de, Context> for SpaceRegistry {
    fn borrow_decode<D: BorrowDecoder<'de, Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        Ok(Self {
            spaces: HashMap::borrow_decode(decoder)?,
            names: HashMap::borrow_decode(decoder)?,
            ordinals: HashMap::new(),
            next_ordinal: 0,
        })
    }
}

impl SpaceRegistry {
    /// Create an empty space registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new space. Returns an error if the name or ID is already taken.
    pub fn register(&mut self, config: SpaceConfig) -> Result<(), SpaceError> {
        self.validate_tree(&config)?;
        self.validate_index_precision(&config)?;
        if self.spaces.contains_key(&config.id) {
            return Err(SpaceError::DuplicateId(config.id));
        }
        if self.names.contains_key(&config.name) {
            return Err(SpaceError::DuplicateName(config.name));
        }
        let id = config.id;
        self.names.insert(config.name.clone(), id);
        self.spaces.insert(id, config);
        self.assign_ordinal(id)?;
        Ok(())
    }

    /// Dense interned ordinal for index-key packing (stable across restarts once persisted).
    pub fn space_ordinal(&self, id: SpaceId) -> Option<u32> {
        self.ordinals.get(&id).copied()
    }

    /// Snapshot of interned ordinals for sidecar persistence.
    pub fn ordinal_snapshot(&self) -> (HashMap<SpaceId, u32>, u32) {
        (self.ordinals.clone(), self.next_ordinal)
    }

    /// Restore interned ordinals from sidecar persistence.
    pub fn load_ordinals(&mut self, ordinals: HashMap<SpaceId, u32>, next_ordinal: u32) {
        self.ordinals = ordinals;
        self.next_ordinal = next_ordinal;
        let missing: Vec<SpaceId> = self
            .spaces
            .keys()
            .copied()
            .filter(|id| !self.ordinals.contains_key(id))
            .collect();
        for id in missing {
            if let Ok(ord) = self.alloc_ordinal() {
                self.ordinals.insert(id, ord);
            }
        }
    }

    /// Assign ordinals by sorted `SpaceId` (used when no sidecar exists yet).
    pub fn rebuild_ordinals(&mut self) {
        let mut ids: Vec<SpaceId> = self.spaces.keys().copied().collect();
        ids.sort_by_key(|id| id.0);
        self.ordinals.clear();
        for (i, id) in ids.into_iter().enumerate() {
            self.ordinals.insert(id, i as u32);
        }
        self.next_ordinal = self.ordinals.len() as u32;
    }

    fn alloc_ordinal(&mut self) -> Result<u32, SpaceError> {
        let ord = self.next_ordinal;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or(SpaceError::SpaceOrdinalExhausted)?;
        Ok(ord)
    }

    fn assign_ordinal(&mut self, id: SpaceId) -> Result<(), SpaceError> {
        if self.ordinals.contains_key(&id) {
            return Ok(());
        }
        let ord = self.alloc_ordinal()?;
        self.ordinals.insert(id, ord);
        Ok(())
    }

    /// INV-INDEX-PRECISION-DOMINATES: an index space must be at least as precise as
    /// any geometry space it indexes.
    fn validate_index_precision(&self, config: &SpaceConfig) -> Result<(), SpaceError> {
        if config.id == ENDPOINT_INDEX_SPACE_ID {
            let max_indexed = self.max_indexed_bits_per_dim();
            if config.bits_per_dim < max_indexed {
                return Err(SpaceError::IndexPrecisionDominates {
                    index_bits: config.bits_per_dim,
                    required_bits: max_indexed,
                });
            }
            return Ok(());
        }
        if !is_geometry_indexed_space(config.id, config) {
            return Ok(());
        }
        if let Some(index) = self.get(ENDPOINT_INDEX_SPACE_ID) {
            if index.bits_per_dim < config.bits_per_dim {
                return Err(SpaceError::IndexPrecisionDominates {
                    index_bits: index.bits_per_dim,
                    required_bits: config.bits_per_dim,
                });
            }
        }
        Ok(())
    }

    fn max_indexed_bits_per_dim(&self) -> u32 {
        self.spaces
            .values()
            .filter(|c| is_geometry_indexed_space(c.id, c))
            .map(|c| c.bits_per_dim)
            .max()
            .unwrap_or(0)
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

    /// Return all registered space IDs in ascending id order (D-DET).
    pub fn space_ids(&self) -> Vec<SpaceId> {
        let mut ids: Vec<SpaceId> = self.spaces.keys().copied().collect();
        ids.sort_by_key(|id| id.0);
        ids
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
            self.ordinals.remove(&id);
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
    /// Active Nexus endpoint or pin membership references this space (INV-NEX-ENDPOINT-EXISTS).
    NexusReferenced(SpaceId),
    /// Same name, conflicting configuration (INV-REGISTER-IDEMPOTENT).
    ConfigConflict {
        name: String,
        existing_id: SpaceId,
    },
    /// INV-INDEX-PRECISION-DOMINATES: index `bits_per_dim` below an indexed space.
    IndexPrecisionDominates {
        index_bits: u32,
        required_bits: u32,
    },
    /// Interned space ordinal would exceed the index coordinate ceiling.
    SpaceOrdinalExhausted,
}

/// Reserved endpoint-index id (duplicated to avoid a module cycle with `endpoint_index`).
const ENDPOINT_INDEX_SPACE_ID: SpaceId = SpaceId(u64::MAX - 1);

fn is_geometry_indexed_space(id: SpaceId, config: &SpaceConfig) -> bool {
    if config.name.ends_with("_errors") {
        return false;
    }
    match id.0 {
        x if x == u64::MAX - 1
            || x == u64::MAX - 2
            || x == u64::MAX - 3
            || x == u64::MAX - 4
            || x == u64::MAX - 5
            || x == 0x9000_0000_0000_0001 =>
        {
            return false;
        }
        _ => {}
    }
    // Packed-id assertion spaces (2D × 32-bit) key rows by identity, not geometry.
    if config.dims == 2 && config.bits_per_dim >= 32 {
        return false;
    }
    true
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
            SpaceError::NexusReferenced(id) => {
                write!(f, "space {:?} is referenced by an active Nexus edge or pin", id)
            }
            SpaceError::ConfigConflict { name, existing_id } => {
                write!(f, "config conflict for name {name}, existing {:?}", existing_id)
            }
            SpaceError::IndexPrecisionDominates {
                index_bits,
                required_bits,
            } => write!(
                f,
                "index bits_per_dim {index_bits} is below indexed space precision {required_bits}"
            ),
            SpaceError::SpaceOrdinalExhausted => {
                write!(f, "space ordinal space exhausted")
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
    fn space_ids_are_sorted() {
        let mut reg = SpaceRegistry::new();
        reg.register(SpaceConfig::new(SpaceId(30), "c", 2)).unwrap();
        reg.register(SpaceConfig::new(SpaceId(2), "a", 2)).unwrap();
        reg.register(SpaceConfig::new(SpaceId(10), "b", 2)).unwrap();
        assert_eq!(
            reg.space_ids(),
            vec![SpaceId(2), SpaceId(10), SpaceId(30)]
        );
    }

    #[test]
    fn index_precision_dominates_rejects_coarse_index() {
        let mut reg = SpaceRegistry::new();
        reg.register(SpaceConfig::new(SpaceId(1), "data", 2).with_bits_per_dim(8))
            .unwrap();
        let err = reg
            .register(
                SpaceConfig::new(SpaceId(u64::MAX - 1), "__endpoint_index__", 16)
                    .with_bits_per_dim(7)
                    .without_error_space(),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            SpaceError::IndexPrecisionDominates {
                index_bits: 7,
                required_bits: 8
            }
        ));
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
