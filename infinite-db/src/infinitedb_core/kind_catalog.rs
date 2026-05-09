use std::collections::HashMap;

/// Action to take when an unknown label is encountered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownKindPolicy {
    AllowUnknown,
    WarnUnknown,
    RejectUnknown,
}

/// Metadata for a registered kind/role label.
#[derive(Debug, Clone)]
pub struct KindDefinition {
    pub label: String,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub version: Option<String>,
}

impl KindDefinition {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            description: None,
            owner: None,
            version: None,
        }
    }
}

/// Runtime registry for discoverable edge/signal/role vocabularies.
#[derive(Debug, Clone)]
pub struct KindCatalog {
    policy: UnknownKindPolicy,
    edge_kinds: HashMap<String, KindDefinition>,
    signal_kinds: HashMap<String, KindDefinition>,
    endpoint_roles: HashMap<String, KindDefinition>,
}

impl KindCatalog {
    pub fn new(policy: UnknownKindPolicy) -> Self {
        Self {
            policy,
            edge_kinds: HashMap::new(),
            signal_kinds: HashMap::new(),
            endpoint_roles: HashMap::new(),
        }
    }

    pub fn register_edge_kind(&mut self, def: KindDefinition) {
        self.edge_kinds.insert(def.label.clone(), def);
    }

    pub fn register_signal_kind(&mut self, def: KindDefinition) {
        self.signal_kinds.insert(def.label.clone(), def);
    }

    pub fn register_endpoint_role(&mut self, def: KindDefinition) {
        self.endpoint_roles.insert(def.label.clone(), def);
    }

    pub fn validate_edge_kind(&self, label: &str) -> Result<(), CatalogError> {
        self.validate_known(label, &self.edge_kinds, LabelType::EdgeKind)
    }

    pub fn validate_signal_kind(&self, label: &str) -> Result<(), CatalogError> {
        self.validate_known(label, &self.signal_kinds, LabelType::SignalKind)
    }

    pub fn validate_endpoint_role(&self, label: &str) -> Result<(), CatalogError> {
        self.validate_known(label, &self.endpoint_roles, LabelType::EndpointRole)
    }

    fn validate_known(
        &self,
        label: &str,
        table: &HashMap<String, KindDefinition>,
        ty: LabelType,
    ) -> Result<(), CatalogError> {
        if table.contains_key(label) {
            return Ok(());
        }
        match self.policy {
            UnknownKindPolicy::AllowUnknown => Ok(()),
            UnknownKindPolicy::WarnUnknown => {
                eprintln!("KindCatalog warning: unknown {} '{}'", ty.as_str(), label);
                Ok(())
            }
            UnknownKindPolicy::RejectUnknown => Err(CatalogError::UnknownLabel {
                label_type: ty,
                label: label.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum LabelType {
    EdgeKind,
    SignalKind,
    EndpointRole,
}

impl LabelType {
    fn as_str(self) -> &'static str {
        match self {
            LabelType::EdgeKind => "edge kind",
            LabelType::SignalKind => "signal kind",
            LabelType::EndpointRole => "endpoint role",
        }
    }
}

#[derive(Debug)]
pub enum CatalogError {
    UnknownLabel { label_type: LabelType, label: String },
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::UnknownLabel { label_type, label } => {
                write!(f, "unknown {} '{}'", label_type.as_str(), label)
            }
        }
    }
}

impl std::error::Error for CatalogError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_policy_errors_for_unknown_kind() {
        let catalog = KindCatalog::new(UnknownKindPolicy::RejectUnknown);
        let err = catalog.validate_edge_kind("beam.bears_on").unwrap_err();
        assert!(err.to_string().contains("unknown edge kind"));
    }
}
