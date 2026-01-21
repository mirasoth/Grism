//! Binary edge representation (property graph compatible).

use serde::{Deserialize, Serialize};

use super::properties::HasProperties;
use super::{EdgeId, Label, NodeId, PropertyMap, identifiers::new_edge_id};

/// A binary edge in the graph (property graph compatible).
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
    pub fn with_properties(mut self, properties: PropertyMap) -> Self {
        self.properties = properties;
        self
    }

    /// Check if this edge has the given label.
    pub fn has_label(&self, label: &str) -> bool {
        self.label == label
    }

    /// Get the endpoints as a tuple (source, target).
    pub fn endpoints(&self) -> (NodeId, NodeId) {
        (self.source, self.target)
    }

    /// Check if this edge connects two nodes (in either direction).
    pub fn connects(&self, a: NodeId, b: NodeId) -> bool {
        (self.source == a && self.target == b) || (self.source == b && self.target == a)
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
}
