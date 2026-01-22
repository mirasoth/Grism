//! Binary edge representation (property graph compatible).
//!
//! A binary edge is a convenience view over a hyperedge with arity = 2
//! and standard roles {source, target}. It provides a familiar property-graph
//! interface while maintaining full compatibility with the hypergraph model.

use serde::{Deserialize, Serialize};

use super::identifiers::{ROLE_SOURCE, ROLE_TARGET, new_edge_id};
use super::properties::HasProperties;
use super::{EdgeId, Hyperedge, Label, NodeId, PropertyMap};

/// A binary edge in the graph (property graph compatible).
///
/// Binary edges are a projection of hyperedges with arity = 2 and roles
/// {source, target}. They provide a familiar property-graph interface
/// for simple directed relationships.
///
/// # Relationship to Hyperedges
///
/// Per the Grism architecture design:
/// > **Edge ≡ Hyperedge with arity = 2**, conventionally using the roles `source` and `target`.
///
/// Binary edges can be converted to and from hyperedges losslessly.
///
/// # Example
///
/// ```rust
/// use grism_core::Edge;
///
/// let edge = Edge::new("KNOWS", 1, 2);
/// assert_eq!(edge.source, 1);
/// assert_eq!(edge.target, 2);
///
/// // Convert to hyperedge
/// let hyperedge = edge.to_hyperedge();
/// assert!(hyperedge.is_binary());
/// assert!(hyperedge.has_binary_roles());
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    /// Unique edge identifier.
    pub id: EdgeId,
    /// Edge label/type.
    pub label: Label,
    /// Source node ID.
    pub source: NodeId,
    /// Target node ID.
    pub target: NodeId,
    /// Edge properties.
    pub properties: PropertyMap,
}

impl Edge {
    /// Create a new edge with generated ID.
    pub fn new(label: impl Into<Label>, source: NodeId, target: NodeId) -> Self {
        Self {
            id: new_edge_id(),
            label: label.into(),
            source,
            target,
            properties: PropertyMap::new(),
        }
    }

    /// Create a new edge with a specific ID.
    pub fn with_id(id: EdgeId, label: impl Into<Label>, source: NodeId, target: NodeId) -> Self {
        Self {
            id,
            label: label.into(),
            source,
            target,
            properties: PropertyMap::new(),
        }
    }

    /// Add properties to this edge.
    #[must_use]
    pub fn with_properties(mut self, properties: PropertyMap) -> Self {
        self.properties = properties;
        self
    }

    /// Check if this edge has the given label.
    pub fn has_label(&self, label: &str) -> bool {
        self.label == label
    }

    /// Get the endpoints as a tuple (source, target).
    pub const fn endpoints(&self) -> (NodeId, NodeId) {
        (self.source, self.target)
    }

    /// Check if this edge connects two nodes (in either direction).
    pub const fn connects(&self, a: NodeId, b: NodeId) -> bool {
        (self.source == a && self.target == b) || (self.source == b && self.target == a)
    }

    /// Convert this binary edge to a hyperedge.
    ///
    /// Creates a hyperedge with:
    /// - The same ID, label, and properties
    /// - Two role bindings: source -> source node, target -> target node
    ///
    /// This conversion is lossless and can be reversed with `Hyperedge::to_binary_edge()`.
    pub fn to_hyperedge(&self) -> Hyperedge {
        Hyperedge::with_id(self.id, self.label.clone())
            .with_node(self.source, ROLE_SOURCE)
            .with_node(self.target, ROLE_TARGET)
            .with_properties(self.properties.clone())
    }

    /// Create a binary edge from a hyperedge.
    ///
    /// Returns `Some(Edge)` if the hyperedge:
    /// - Has exactly 2 role bindings (arity = 2)
    /// - Has roles {source, target}
    /// - Both bindings point to nodes (not hyperedges)
    ///
    /// Returns `None` otherwise.
    ///
    /// This is equivalent to `Hyperedge::to_binary_edge()`.
    pub fn from_hyperedge(hyperedge: &Hyperedge) -> Option<Self> {
        hyperedge.to_binary_edge()
    }
}

impl HasProperties for Edge {
    fn properties(&self) -> &PropertyMap {
        &self.properties
    }

    fn properties_mut(&mut self) -> &mut PropertyMap {
        &mut self.properties
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[test]
    fn test_edge_creation() {
        let edge = Edge::new("KNOWS", 1, 2);

        assert!(edge.has_label("KNOWS"));
        assert_eq!(edge.source, 1);
        assert_eq!(edge.target, 2);
        assert_eq!(edge.endpoints(), (1, 2));
    }

    #[test]
    fn test_edge_connects() {
        let edge = Edge::new("KNOWS", 1, 2);

        assert!(edge.connects(1, 2));
        assert!(edge.connects(2, 1));
        assert!(!edge.connects(1, 3));
    }

    #[test]
    fn test_edge_to_hyperedge() {
        let mut props = PropertyMap::new();
        props.insert("strength".to_string(), Value::Float64(0.8));

        let edge = Edge::with_id(42, "KNOWS", 1, 2).with_properties(props);
        let hyperedge = edge.to_hyperedge();

        assert_eq!(hyperedge.id, 42);
        assert_eq!(hyperedge.label, "KNOWS");
        assert!(hyperedge.is_binary());
        assert!(hyperedge.has_binary_roles());
        assert!(hyperedge.involves_node(1));
        assert!(hyperedge.involves_node(2));
        assert_eq!(hyperedge.role_of_node(1), Some(&ROLE_SOURCE.to_string()));
        assert_eq!(hyperedge.role_of_node(2), Some(&ROLE_TARGET.to_string()));
        assert_eq!(
            hyperedge.properties.get("strength"),
            Some(&Value::Float64(0.8))
        );
    }

    #[test]
    fn test_edge_from_hyperedge_roundtrip() {
        let original = Edge::new("LIKES", 10, 20);
        let hyperedge = original.to_hyperedge();
        let recovered = Edge::from_hyperedge(&hyperedge).unwrap();

        assert_eq!(recovered.label, original.label);
        assert_eq!(recovered.source, original.source);
        assert_eq!(recovered.target, original.target);
    }

    #[test]
    fn test_edge_from_hyperedge_non_binary() {
        // Create a non-binary hyperedge
        let hyperedge = Hyperedge::new("EVENT")
            .with_node(1, "participant")
            .with_node(2, "participant")
            .with_node(3, "location");

        assert!(Edge::from_hyperedge(&hyperedge).is_none());
    }

    #[test]
    fn test_edge_from_hyperedge_non_standard_roles() {
        // Create a binary hyperedge without source/target roles
        let hyperedge = Hyperedge::new("AUTHORED_BY")
            .with_node(1, "paper")
            .with_node(2, "author");

        assert!(Edge::from_hyperedge(&hyperedge).is_none());
    }
}
