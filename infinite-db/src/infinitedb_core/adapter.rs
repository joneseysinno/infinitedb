use super::address::{DimensionVector, SpaceId};
use super::hyperedge::{EndpointRef, EndpointRole, HyperedgeKind};
use super::signal::SignalKind;

/// Trait for upstream domain types that map to kind labels.
pub trait KindLabel {
    fn label(&self) -> &str;
}

/// Trait for upstream domain types that map to endpoint role labels.
pub trait RoleLabel {
    fn role_label(&self) -> &str;
}

/// Trait binding a typed domain model to one concrete database space.
pub trait SpaceBinding {
    const SPACE_ID: SpaceId;
    const DIMS: usize;
    const SPACE_NAME: &'static str = "";
}

impl KindLabel for String {
    fn label(&self) -> &str {
        self.as_str()
    }
}

impl KindLabel for str {
    fn label(&self) -> &str {
        self
    }
}

impl<T: KindLabel + ?Sized> KindLabel for &T {
    fn label(&self) -> &str {
        (*self).label()
    }
}

impl RoleLabel for String {
    fn role_label(&self) -> &str {
        self.as_str()
    }
}

impl RoleLabel for str {
    fn role_label(&self) -> &str {
        self
    }
}

impl<T: RoleLabel + ?Sized> RoleLabel for &T {
    fn role_label(&self) -> &str {
        (*self).role_label()
    }
}

impl<T: KindLabel> From<T> for HyperedgeKind {
    fn from(value: T) -> Self {
        HyperedgeKind::new(value.label())
    }
}

impl<T: KindLabel> From<T> for SignalKind {
    fn from(value: T) -> Self {
        SignalKind::new(value.label())
    }
}

impl<T: RoleLabel> From<T> for EndpointRole {
    fn from(value: T) -> Self {
        EndpointRole::new(value.role_label())
    }
}

/// Adapter endpoint shape that accepts any upstream `RoleLabel` and maps
/// into a core `EndpointRef`.
#[derive(Debug, Clone)]
pub struct AdapterEndpoint {
    pub role: String,
    pub space: SpaceId,
    pub node: DimensionVector,
}

impl AdapterEndpoint {
    pub fn new<R: RoleLabel>(role: R, space: SpaceId, node: DimensionVector) -> Self {
        Self {
            role: role.role_label().to_string(),
            space,
            node,
        }
    }
}

impl From<AdapterEndpoint> for EndpointRef {
    fn from(value: AdapterEndpoint) -> Self {
        EndpointRef {
            role: EndpointRole::new(value.role),
            space: value.space,
            node: value.node,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    enum EdgeKind {
        Supports,
    }

    impl KindLabel for EdgeKind {
        fn label(&self) -> &str {
            match self {
                EdgeKind::Supports => "beam.supports",
            }
        }
    }

    #[test]
    fn kind_label_converts_to_core_kind() {
        let k: HyperedgeKind = EdgeKind::Supports.into();
        assert_eq!(k.as_str(), "beam.supports");
    }
}
