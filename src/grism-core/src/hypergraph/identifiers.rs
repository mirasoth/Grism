//! Type identifiers for graph elements.

use serde::{Deserialize, Serialize};

/// Node identifier.
pub type NodeId = u64;

/// Edge/Hyperedge identifier.
pub type EdgeId = u64;

/// Label for nodes, edges, and hyperedges.
pub type Label = String;

/// Role name for hyperedge endpoints.
pub type Role = String;

/// Property key name.
pub type PropertyKey = String;

/// Generate a new unique node ID.
pub fn new_node_id() -> NodeId {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Generate a new unique edge ID.
pub fn new_edge_id() -> EdgeId {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Standard role name for binary edge source.
pub const ROLE_SOURCE: &str = "source";

/// Standard role name for binary edge target.
pub const ROLE_TARGET: &str = "target";

/// Reference to a graph entity (node or hyperedge).
///
/// This enum allows hyperedges to reference either nodes or other hyperedges,
/// enabling higher-order relations such as:
/// - Provenance and justification chains
/// - Causal and temporal dependencies
/// - Rule application and inference confidence
/// - Planning and execution traces
///
/// # Example
///
/// ```rust
/// use grism_core::EntityRef;
///
/// let node_ref = EntityRef::Node(1);
/// let hyperedge_ref = EntityRef::Hyperedge(2);
///
/// assert!(node_ref.is_node());
/// assert!(hyperedge_ref.is_hyperedge());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntityRef {
    /// Reference to a node.
    Node(NodeId),
    /// Reference to a hyperedge (for hyperedge-to-hyperedge relations).
    Hyperedge(EdgeId),
}

impl EntityRef {
    /// Check if this reference points to a node.
    pub const fn is_node(&self) -> bool {
        matches!(self, Self::Node(_))
    }

    /// Check if this reference points to a hyperedge.
    pub const fn is_hyperedge(&self) -> bool {
        matches!(self, Self::Hyperedge(_))
    }

    /// Get the node ID if this is a node reference.
    pub const fn as_node(&self) -> Option<NodeId> {
        match self {
            Self::Node(id) => Some(*id),
            _ => None,
        }
    }

    /// Get the edge ID if this is a hyperedge reference.
    pub const fn as_hyperedge(&self) -> Option<EdgeId> {
        match self {
            Self::Hyperedge(id) => Some(*id),
            _ => None,
        }
    }
}

impl From<NodeId> for EntityRef {
    fn from(id: NodeId) -> Self {
        Self::Node(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_ref_node() {
        let node_ref = EntityRef::Node(42);
        assert!(node_ref.is_node());
        assert!(!node_ref.is_hyperedge());
        assert_eq!(node_ref.as_node(), Some(42));
        assert_eq!(node_ref.as_hyperedge(), None);
    }

    #[test]
    fn test_entity_ref_hyperedge() {
        let edge_ref = EntityRef::Hyperedge(99);
        assert!(!edge_ref.is_node());
        assert!(edge_ref.is_hyperedge());
        assert_eq!(edge_ref.as_node(), None);
        assert_eq!(edge_ref.as_hyperedge(), Some(99));
    }

    #[test]
    fn test_entity_ref_from_node_id() {
        let node_ref: EntityRef = 42u64.into();
        assert!(node_ref.is_node());
        assert_eq!(node_ref.as_node(), Some(42));
    }

    #[test]
    fn test_role_constants() {
        assert_eq!(ROLE_SOURCE, "source");
        assert_eq!(ROLE_TARGET, "target");
    }
}
