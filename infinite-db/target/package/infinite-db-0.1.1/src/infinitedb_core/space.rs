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

    pub fn get(&self, id: SpaceId) -> Option<&SpaceConfig> {
        self.spaces.get(&id)
    }

    pub fn get_by_name(&self, name: &str) -> Option<&SpaceConfig> {
        self.names.get(name).and_then(|id| self.spaces.get(id))
    }

    pub fn remove(&mut self, id: SpaceId) -> Option<SpaceConfig> {
        if let Some(config) = self.spaces.remove(&id) {
            self.names.remove(&config.name);
            Some(config)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub enum SpaceError {
    DuplicateId(SpaceId),
    DuplicateName(String),
    NotFound(SpaceId),
}