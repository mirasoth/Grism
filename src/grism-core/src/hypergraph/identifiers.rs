//! Type identifiers for graph elements.

use serde::{Deserialize, Serialize};

/// Node identifier.
pub type NodeId = u64;

/// Edge identifier.
pub type EdgeId = u64;

/// Label for nodes, edges, and hyperedges.
pub type Label = String;

/// Role name for hyperedge endpoints.
pub type Role = String;

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

/// Reference to a graph element.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ElementRef {
    /// Reference to a node.
    Node(NodeId),
    /// Reference to an edge.
    Edge(EdgeId),
}

impl From<NodeId> for ElementRef {
    fn from(id: NodeId) -> Self {
        Self::Node(id)
    }
}
