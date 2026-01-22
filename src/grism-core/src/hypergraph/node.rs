//! Node representation.

use serde::{Deserialize, Serialize};

use super::properties::HasProperties;
use super::{Label, NodeId, PropertyMap, identifiers::new_node_id};

/// A node in the graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    /// Unique node identifier.
    pub id: NodeId,
    /// Node labels (a node can have multiple labels).
    pub labels: Vec<Label>,
    /// Node properties.
    pub properties: PropertyMap,
}

impl Node {
    /// Create a new node with generated ID.
    pub fn new() -> Self {
        Self {
            id: new_node_id(),
            labels: Vec::new(),
            properties: PropertyMap::new(),
        }
    }

    /// Create a new node with a specific ID.
    pub fn with_id(id: NodeId) -> Self {
        Self {
            id,
            labels: Vec::new(),
            properties: PropertyMap::new(),
        }
    }

    /// Add a label to this node.
    #[must_use]
    pub fn with_label(mut self, label: impl Into<Label>) -> Self {
        self.labels.push(label.into());
        self
    }

    /// Add labels to this node.
    #[must_use]
    pub fn with_labels(mut self, labels: impl IntoIterator<Item = impl Into<Label>>) -> Self {
        self.labels.extend(labels.into_iter().map(Into::into));
        self
    }

    /// Add properties to this node.
    #[must_use]
    pub fn with_properties(mut self, properties: PropertyMap) -> Self {
        self.properties = properties;
        self
    }

    /// Check if this node has a specific label.
    pub fn has_label(&self, label: &str) -> bool {
        self.labels.iter().any(|l| l == label)
    }

    /// Get the primary label (first label, if any).
    pub fn primary_label(&self) -> Option<&str> {
        self.labels.first().map(String::as_str)
    }
}

impl Default for Node {
    fn default() -> Self {
        Self::new()
    }
}

impl HasProperties for Node {
    fn properties(&self) -> &PropertyMap {
        &self.properties
    }

    fn properties_mut(&mut self) -> &mut PropertyMap {
        &mut self.properties
    }
}

#[cfg(test)]
mod tests {
    use crate::types::Value;

    use super::*;

    #[test]
    fn test_node_creation() {
        let node = Node::new().with_label("Person").with_label("Employee");

        assert!(node.has_label("Person"));
        assert!(node.has_label("Employee"));
        assert!(!node.has_label("Animal"));
    }

    #[test]
    fn test_node_properties() {
        let mut node = Node::new();
        node.set_property("name", Value::from("Alice"));
        node.set_property("age", Value::from(30i64));

        assert_eq!(node.get_property("name"), Some(&Value::from("Alice")));
        assert!(node.has_property("age"));
    }
}
