use std::collections::HashMap;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use super::address::SpaceId;

/// Configuration for a registered space.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct SpaceConfig {
    pub id: SpaceId,
    /// Human-readable name.
    pub name: String,
    /// Number of dimensions all records in this space must have.
    pub dims: usize,
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