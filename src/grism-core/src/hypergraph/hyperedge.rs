//! N-ary hyperedge representation.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::properties::HasProperties;
use super::{EdgeId, Label, NodeId, PropertyMap, Role, identifiers::new_edge_id};

/// An n-ary hyperedge connecting multiple nodes with roles.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HyperEdge {
    /// Unique hyperedge identifier.
    pub id: EdgeId,
    /// Hyperedge label/type.
    pub label: Label,
    /// Endpoints with their roles.
    pub endpoints: Vec<NodeId>,
    /// Role assignments for endpoints.
    pub roles: HashMap<NodeId, Role>,
    /// Hyperedge properties.
    pub properties: PropertyMap,
}

impl HyperEdge {
    /// Create a new hyperedge with generated ID.
    pub fn new(label: impl Into<Label>) -> Self {
        Self {
            id: new_edge_id(),
            label: label.into(),
            endpoints: Vec::new(),
            roles: HashMap::new(),
            properties: PropertyMap::new(),
        }
    }

    /// Create a new hyperedge with a specific ID.
    pub fn with_id(id: EdgeId, label: impl Into<Label>) -> Self {
        Self {
            id,
            label: label.into(),
            endpoints: Vec::new(),
            roles: HashMap::new(),
            properties: PropertyMap::new(),
        }
    }

    /// Add an endpoint with a role.
    pub fn with_endpoint(mut self, node_id: NodeId, role: impl Into<Role>) -> Self {
        self.endpoints.push(node_id);
        self.roles.insert(node_id, role.into());
        self
    }

    /// Add multiple endpoints.
    pub fn with_endpoints(
        mut self,
        endpoints: impl IntoIterator<Item = (NodeId, impl Into<Role>)>,
    ) -> Self {
        for (node_id, role) in endpoints {
            self.endpoints.push(node_id);
            self.roles.insert(node_id, role.into());
        }
        self
    }

    /// Add properties to this hyperedge.
    pub fn with_properties(mut self, properties: PropertyMap) -> Self {
        self.properties = properties;
        self
    }

    /// Get the arity (number of endpoints).
    pub fn arity(&self) -> usize {
        self.endpoints.len()
    }

    /// Check if this is a binary edge (arity == 2).
    pub fn is_binary(&self) -> bool {
        self.arity() == 2
    }

    /// Get the role of a node in this hyperedge.
    pub fn role_of(&self, node_id: NodeId) -> Option<&Role> {
        self.roles.get(&node_id)
    }

    /// Get all nodes with a specific role.
    pub fn nodes_with_role(&self, role: &str) -> Vec<NodeId> {
        self.roles
            .iter()
            .filter(|(_, r)| r.as_str() == role)
            .map(|(&id, _)| id)
            .collect()
    }

    /// Get all unique roles.
    pub fn roles(&self) -> Vec<&Role> {
        self.roles.values().collect()
    }

    /// Check if this hyperedge involves a specific node.
    pub fn involves(&self, node_id: NodeId) -> bool {
        self.endpoints.contains(&node_id)
    }

    /// Convert to a binary edge if arity is 2.
    pub fn to_binary_edge(&self) -> Option<super::Edge> {
        if self.arity() == 2 {
            Some(super::Edge {
                id: self.id,
                label: self.label.clone(),
                source: self.endpoints[0],
                target: self.endpoints[1],
                properties: self.properties.clone(),
            })
        } else {
            None
        }
    }
}

impl HasProperties for HyperEdge {
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
    fn test_hyperedge_creation() {
        let edge = HyperEdge::new("Event")
            .with_endpoint(1, "participant")
            .with_endpoint(2, "participant")
            .with_endpoint(3, "location");

        assert_eq!(edge.arity(), 3);
        assert!(!edge.is_binary());
        assert!(edge.involves(1));
        assert!(!edge.involves(99));
    }

    #[test]
    fn test_hyperedge_roles() {
        let edge = HyperEdge::new("Meeting")
            .with_endpoint(1, "host")
            .with_endpoint(2, "attendee")
            .with_endpoint(3, "attendee");

        assert_eq!(edge.role_of(1), Some(&"host".to_string()));
        let mut attendees = edge.nodes_with_role("attendee");
        attendees.sort();
        assert_eq!(attendees, vec![2, 3]);
    }

    #[test]
    fn test_binary_conversion() {
        let binary = HyperEdge::new("KNOWS")
            .with_endpoint(1, "source")
            .with_endpoint(2, "target");

        assert!(binary.is_binary());

        let edge = binary.to_binary_edge().unwrap();
        assert_eq!(edge.source, 1);
        assert_eq!(edge.target, 2);
    }
}
