//! Hypergraph container - canonical user-facing abstraction.
//!
//! The Hypergraph represents a logical, executable view over a persistent hypergraph.
//! It provides a stable, expressive, and optimizable foundation for AI-native workloads.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{Edge, EdgeId, Hyperedge, Label, Node, NodeId, PropertyMap, Role};
use crate::schema::{EntityInfo, EntityKind, Schema, SchemaViolation};
use crate::types::Value;

/// A hypergraph container - the canonical user-facing abstraction.
///
/// A Hypergraph represents a logical, executable view over a persistent hypergraph,
/// analogous to how `DataFrame` represents a logical view over tabular data.
///
/// ## Properties
///
/// - **Hypergraph-first** — n-ary relations (hyperedges) are native
/// - **Relation-centric** — operations compile to relational algebra over hyperedges
/// - **View-based** — immutable, lazy, and composable
/// - **Storage-agnostic** — backed by Lance via physical planning, not hard-wired
///
/// ## Example
///
/// ```rust
/// use grism_core::Hypergraph;
///
/// let mut hg = Hypergraph::new();
///
/// // Create nodes
/// let alice = hg.add_node("Person", [("name", "Alice"), ("age", "30")]);
/// let bob = hg.add_node("Person", [("name", "Bob"), ("age", "25")]);
/// let company = hg.add_node("Company", [("name", "Acme")]);
///
/// // Create hyperedges
/// let works_at = hg.add_hyperedge("WORKS_AT")
///     .with_node(alice, "employee")
///     .with_node(company, "employer")
///     .with_properties([("since", "2020")])
///     .build();
///
/// let knows = hg.add_hyperedge("KNOWS")
///     .with_node(alice, "source")
///     .with_node(bob, "target")
///     .with_properties([("strength", "0.8")])
///     .build();
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hypergraph {
    /// Unique identifier for this hypergraph instance
    id: String,

    /// Nodes in the hypergraph
    nodes: HashMap<NodeId, Node>,

    /// Hyperedges in the hypergraph
    hyperedges: HashMap<EdgeId, Hyperedge>,

    /// Schema information
    schema: Schema,

    /// Global properties
    properties: PropertyMap,

    /// Whether to enforce strict schema validation on writes.
    /// When true, all properties must match their declared types.
    #[serde(default)]
    strict_schema: bool,
}

impl Hypergraph {
    /// Create a new empty hypergraph.
    pub fn new() -> Self {
        Self::with_id("default")
    }

    /// Create a new hypergraph with a specific ID.
    pub fn with_id(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            nodes: HashMap::new(),
            hyperedges: HashMap::new(),
            schema: Schema::empty(),
            properties: PropertyMap::new(),
            strict_schema: false,
        }
    }

    /// Enable or disable strict schema validation.
    ///
    /// When strict schema is enabled:
    /// - All properties must match their declared types
    /// - Undeclared properties are violations
    /// - Required properties must be present
    ///
    /// # Example
    ///
    /// ```rust
    /// use grism_core::Hypergraph;
    /// use grism_core::types::DataType;
    ///
    /// let mut hg = Hypergraph::new().with_strict_schema(true);
    /// hg.schema_mut().register_property("Person", "age", DataType::Int64);
    ///
    /// // Now add_node_validated will check properties against schema
    /// ```
    #[must_use]
    pub const fn with_strict_schema(mut self, strict: bool) -> Self {
        self.strict_schema = strict;
        self
    }

    /// Check if strict schema validation is enabled.
    pub const fn is_strict_schema(&self) -> bool {
        self.strict_schema
    }

    /// Set strict schema mode.
    pub const fn set_strict_schema(&mut self, strict: bool) {
        self.strict_schema = strict;
    }

    /// Get the hypergraph ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Get the hypergraph schema.
    pub const fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Get a mutable reference to the hypergraph schema.
    pub const fn schema_mut(&mut self) -> &mut Schema {
        &mut self.schema
    }

    /// Get the number of nodes in the hypergraph.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get the number of hyperedges in the hypergraph.
    pub fn hyperedge_count(&self) -> usize {
        self.hyperedges.len()
    }

    /// Add a node to the hypergraph.
    ///
    /// # Arguments
    /// * `label` - The node label/type
    /// * `properties` - Initial properties as key-value pairs
    ///
    /// # Returns
    /// The ID of the created node
    pub fn add_node<I, K, V>(&mut self, label: impl Into<Label>, properties: I) -> NodeId
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<Value>,
    {
        let node_id = super::identifiers::new_node_id();
        let mut property_map = PropertyMap::new();

        for (key, value) in properties {
            property_map.insert(key.into(), value.into());
        }

        let node = Node {
            id: node_id,
            labels: vec![label.into()],
            properties: property_map,
        };

        // Update schema with node type info
        self.schema.register_entity(EntityInfo {
            name: node.labels.first().unwrap().clone(),
            kind: EntityKind::Node,
            columns: node.properties.keys().cloned().collect(),
            is_alias: false,
        });

        self.nodes.insert(node_id, node);
        node_id
    }

    /// Add a node with schema validation.
    ///
    /// If strict schema mode is enabled, this validates all properties against
    /// the schema before adding the node.
    ///
    /// # Arguments
    /// * `label` - The node label/type
    /// * `properties` - Initial properties as key-value pairs
    ///
    /// # Returns
    /// * `Ok(NodeId)` if the node was added successfully
    /// * `Err(Vec<SchemaViolation>)` if validation failed
    ///
    /// # Example
    ///
    /// ```rust
    /// use grism_core::Hypergraph;
    /// use grism_core::types::DataType;
    ///
    /// let mut hg = Hypergraph::new().with_strict_schema(true);
    /// hg.schema_mut().register_property("Person", "age", DataType::Int64);
    ///
    /// // This will fail because "age" should be Int64
    /// let result = hg.add_node_validated("Person", [("age", "thirty")]);
    /// assert!(result.is_err());
    ///
    /// // This will succeed
    /// let result = hg.add_node_validated("Person", [("age", 30i64)]);
    /// assert!(result.is_ok());
    /// ```
    pub fn add_node_validated<I, K, V>(
        &mut self,
        label: impl Into<Label>,
        properties: I,
    ) -> Result<NodeId, Vec<SchemaViolation>>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<Value>,
    {
        let label = label.into();
        let mut property_map = PropertyMap::new();

        for (key, value) in properties {
            property_map.insert(key.into(), value.into());
        }

        // Validate properties if strict schema is enabled
        if self.strict_schema {
            let violations = self.schema.validate_properties(&label, &property_map, true);
            if !violations.is_empty() {
                return Err(violations);
            }
        }

        let node_id = super::identifiers::new_node_id();
        let node = Node {
            id: node_id,
            labels: vec![label.clone()],
            properties: property_map,
        };

        // Update schema with node type info
        self.schema.register_entity(EntityInfo {
            name: label,
            kind: EntityKind::Node,
            columns: node.properties.keys().cloned().collect(),
            is_alias: false,
        });

        self.nodes.insert(node_id, node);
        Ok(node_id)
    }

    /// Add a hyperedge to the hypergraph.
    ///
    /// # Arguments
    /// * `label` - The hyperedge label/type
    ///
    /// # Returns
    /// A `HyperedgeBuilder` for configuring the hyperedge
    pub fn add_hyperedge(&mut self, label: impl Into<Label>) -> HyperedgeBuilder {
        HyperedgeBuilder {
            hypergraph: self,
            hyperedge: Hyperedge::new(label),
        }
    }

    /// Get a node by ID.
    pub fn get_node(&self, node_id: NodeId) -> Option<&Node> {
        self.nodes.get(&node_id)
    }

    /// Get a hyperedge by ID.
    pub fn get_hyperedge(&self, edge_id: EdgeId) -> Option<&Hyperedge> {
        self.hyperedges.get(&edge_id)
    }

    /// Get all nodes with a specific label.
    pub fn nodes_with_label(&self, label: &str) -> Vec<&Node> {
        self.nodes
            .values()
            .filter(|node| node.has_label(label))
            .collect()
    }

    /// Get all hyperedges with a specific label.
    pub fn hyperedges_with_label(&self, label: &str) -> Vec<&Hyperedge> {
        self.hyperedges
            .values()
            .filter(|edge| edge.label == label)
            .collect()
    }

    /// Get all hyperedges that involve a specific node.
    pub fn hyperedges_involving(&self, node_id: NodeId) -> Vec<&Hyperedge> {
        self.hyperedges
            .values()
            .filter(|edge| edge.involves_node(node_id))
            .collect()
    }

    /// Find hyperedges where a node has a specific role.
    pub fn hyperedges_with_role(&self, node_id: NodeId, role: &str) -> Vec<&Hyperedge> {
        self.hyperedges
            .values()
            .filter(|edge| edge.role_of_node(node_id).is_some_and(|r| r == role))
            .collect()
    }

    /// Get all binary edges (arity = 2 hyperedges).
    pub fn binary_edges(&self) -> Vec<Edge> {
        self.hyperedges
            .values()
            .filter_map(super::hyperedge::Hyperedge::to_binary_edge)
            .collect()
    }

    /// Get binary edges with a specific label.
    pub fn binary_edges_with_label(&self, label: &str) -> Vec<Edge> {
        self.hyperedges
            .values()
            .filter(|edge| edge.label == label && edge.is_binary())
            .filter_map(super::hyperedge::Hyperedge::to_binary_edge)
            .collect()
    }

    /// Get global properties.
    pub const fn properties(&self) -> &PropertyMap {
        &self.properties
    }

    /// Set a global property.
    pub fn set_property(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        self.properties.insert(key.into(), value.into());
    }

    /// Remove a global property.
    pub fn remove_property(&mut self, key: &str) -> Option<Value> {
        self.properties.remove(key)
    }

    /// Validate the hypergraph against its schema.
    ///
    /// This method checks that all data in the hypergraph conforms to the
    /// declared schema. It returns a list of all violations found.
    ///
    /// # Arguments
    /// * `strict` - If true, undeclared properties are also reported as violations
    ///
    /// # Checks Performed
    /// - All node properties match their declared types
    /// - All hyperedge properties match their declared types
    /// - Required properties are present
    /// - Non-nullable properties are not null
    /// - (In strict mode) No undeclared properties exist
    ///
    /// # Example
    ///
    /// ```rust
    /// use grism_core::Hypergraph;
    /// use grism_core::types::DataType;
    ///
    /// let mut hg = Hypergraph::new();
    /// hg.schema_mut().register_property("Person", "age", DataType::Int64);
    ///
    /// // Add a node with correct type
    /// hg.add_node("Person", [("age", 30i64)]);
    ///
    /// // Validate - should pass
    /// let violations = hg.validate_schema(false);
    /// assert!(violations.is_empty());
    /// ```
    pub fn validate_schema(&self, strict: bool) -> Vec<SchemaViolation> {
        let mut violations = Vec::new();

        // Validate all nodes
        for node in self.nodes.values() {
            // Get the primary label for this node
            if let Some(label) = node.labels.first() {
                let node_violations =
                    self.schema
                        .validate_properties(label, &node.properties, strict);
                violations.extend(node_violations);
            }
        }

        // Validate all hyperedges
        for edge in self.hyperedges.values() {
            let edge_violations =
                self.schema
                    .validate_properties(&edge.label, &edge.properties, strict);
            violations.extend(edge_violations);
        }

        violations
    }

    /// Check if the hypergraph data is valid according to its schema.
    ///
    /// This is a convenience method that returns true if there are no violations.
    pub fn is_schema_valid(&self, strict: bool) -> bool {
        self.validate_schema(strict).is_empty()
    }

    /// Create a subgraph containing only nodes and hyperedges that match criteria.
    ///
    /// This is a foundational operation for creating views and filtering.
    /// Note: Only node bindings are considered; hyperedge-to-hyperedge bindings
    /// are not followed in the current implementation.
    pub fn subgraph<F>(&self, predicate: F) -> SubgraphView
    where
        F: Fn(&Node, &Hyperedge) -> bool,
    {
        let mut filtered_nodes = HashMap::new();
        let mut filtered_hyperedges = HashMap::new();

        // Filter hyperedges first
        for (edge_id, edge) in &self.hyperedges {
            let involved_nodes = edge.involved_nodes();

            // Check if all involved nodes satisfy the predicate
            let all_nodes_valid = involved_nodes.iter().all(|&node_id| {
                self.nodes
                    .get(&node_id)
                    .is_some_and(|node| predicate(node, edge))
            });

            if all_nodes_valid {
                filtered_hyperedges.insert(*edge_id, edge.clone());

                // Include all involved nodes
                for node_id in involved_nodes {
                    if let Some(node) = self.nodes.get(&node_id) {
                        filtered_nodes.insert(node_id, node.clone());
                    }
                }
            }
        }

        SubgraphView {
            base: self,
            nodes: filtered_nodes,
            hyperedges: filtered_hyperedges,
        }
    }
}

impl Default for Hypergraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for creating hyperedges with a fluent API.
pub struct HyperedgeBuilder<'a> {
    hypergraph: &'a mut Hypergraph,
    hyperedge: Hyperedge,
}

impl HyperedgeBuilder<'_> {
    /// Add a binding to any entity (node or hyperedge) with a role.
    #[must_use]
    pub fn with_binding(mut self, entity: super::EntityRef, role: impl Into<Role>) -> Self {
        self.hyperedge = self.hyperedge.with_binding(entity, role);
        self
    }

    /// Add a node binding with a role.
    #[must_use]
    pub fn with_node(mut self, node_id: NodeId, role: impl Into<Role>) -> Self {
        self.hyperedge = self.hyperedge.with_node(node_id, role);
        self
    }

    /// Add a hyperedge binding with a role (for meta-relations).
    #[must_use]
    pub fn with_hyperedge(mut self, edge_id: EdgeId, role: impl Into<Role>) -> Self {
        self.hyperedge = self.hyperedge.with_hyperedge(edge_id, role);
        self
    }

    /// Add multiple node bindings.
    #[must_use]
    pub fn with_nodes(
        mut self,
        nodes: impl IntoIterator<Item = (NodeId, impl Into<Role>)>,
    ) -> Self {
        self.hyperedge = self.hyperedge.with_nodes(nodes);
        self
    }

    /// Set properties for the hyperedge.
    #[must_use]
    pub fn with_properties(
        mut self,
        properties: impl IntoIterator<Item = (impl Into<String>, impl Into<Value>)>,
    ) -> Self {
        for (key, value) in properties {
            self.hyperedge.properties.insert(key.into(), value.into());
        }
        self
    }

    /// Build and add the hyperedge to the hypergraph.
    ///
    /// # Returns
    /// The ID of the created hyperedge
    ///
    /// # Panics
    /// Panics if the hyperedge has fewer than 2 role bindings (arity < 2).
    pub fn build(self) -> EdgeId {
        let edge_id = self.hyperedge.id;

        // Validate that hyperedge has at least 2 endpoints (arity ≥ 2)
        assert!(
            self.hyperedge.arity() >= 2,
            "Hyperedges must have arity 2 or more"
        );

        // Update schema with hyperedge type info
        self.hypergraph.schema.register_entity(EntityInfo {
            name: self.hyperedge.label.clone(),
            kind: EntityKind::Hyperedge,
            columns: self.hyperedge.properties.keys().cloned().collect(),
            is_alias: false,
        });

        self.hypergraph.hyperedges.insert(edge_id, self.hyperedge);
        edge_id
    }

    /// Build and add the hyperedge with schema validation.
    ///
    /// If strict schema mode is enabled on the hypergraph, this validates
    /// all properties against the schema before adding the hyperedge.
    ///
    /// # Returns
    /// * `Ok(EdgeId)` if the hyperedge was added successfully
    /// * `Err(Vec<SchemaViolation>)` if validation failed
    ///
    /// # Errors
    /// Returns an error if:
    /// - The hyperedge has fewer than 2 role bindings (arity < 2)
    /// - Strict schema is enabled and properties don't match the schema
    pub fn build_validated(self) -> Result<EdgeId, Vec<SchemaViolation>> {
        let edge_id = self.hyperedge.id;

        // Validate that hyperedge has at least 2 endpoints (arity ≥ 2)
        if self.hyperedge.arity() < 2 {
            return Err(vec![SchemaViolation::MissingEntity {
                name: format!(
                    "Hyperedge '{}' must have arity 2 or more, got {}",
                    self.hyperedge.label,
                    self.hyperedge.arity()
                ),
                kind: EntityKind::Hyperedge,
            }]);
        }

        // Validate properties if strict schema is enabled
        if self.hypergraph.strict_schema {
            let violations = self.hypergraph.schema.validate_properties(
                &self.hyperedge.label,
                &self.hyperedge.properties,
                true,
            );
            if !violations.is_empty() {
                return Err(violations);
            }
        }

        // Update schema with hyperedge type info
        self.hypergraph.schema.register_entity(EntityInfo {
            name: self.hyperedge.label.clone(),
            kind: EntityKind::Hyperedge,
            columns: self.hyperedge.properties.keys().cloned().collect(),
            is_alias: false,
        });

        self.hypergraph.hyperedges.insert(edge_id, self.hyperedge);
        Ok(edge_id)
    }
}

/// A view over a subgraph, created by filtering operations.
///
/// SubgraphViews are immutable and provide read-only access to a subset
/// of nodes and hyperedges from the base hypergraph.
#[derive(Debug, Clone)]
pub struct SubgraphView<'a> {
    base: &'a Hypergraph,
    nodes: HashMap<NodeId, Node>,
    hyperedges: HashMap<EdgeId, Hyperedge>,
}

impl SubgraphView<'_> {
    /// Get the base hypergraph.
    pub const fn base(&self) -> &Hypergraph {
        self.base
    }

    /// Get the number of nodes in this view.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get the number of hyperedges in this view.
    pub fn hyperedge_count(&self) -> usize {
        self.hyperedges.len()
    }

    /// Get a node by ID.
    pub fn get_node(&self, node_id: NodeId) -> Option<&Node> {
        self.nodes.get(&node_id)
    }

    /// Get a hyperedge by ID.
    pub fn get_hyperedge(&self, edge_id: EdgeId) -> Option<&Hyperedge> {
        self.hyperedges.get(&edge_id)
    }

    /// Iterate over all nodes in this view.
    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.values()
    }

    /// Iterate over all hyperedges in this view.
    pub fn hyperedges(&self) -> impl Iterator<Item = &Hyperedge> {
        self.hyperedges.values()
    }

    /// Get all nodes with a specific label in this view.
    pub fn nodes_with_label(&self, label: &str) -> Vec<&Node> {
        self.nodes
            .values()
            .filter(|node| node.has_label(label))
            .collect()
    }

    /// Get all hyperedges with a specific label in this view.
    pub fn hyperedges_with_label(&self, label: &str) -> Vec<&Hyperedge> {
        self.hyperedges
            .values()
            .filter(|edge| edge.label == label)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hypergraph_creation() {
        let hg = Hypergraph::new();
        assert_eq!(hg.id(), "default");
        assert_eq!(hg.node_count(), 0);
        assert_eq!(hg.hyperedge_count(), 0);
    }

    #[test]
    fn test_add_nodes() {
        let mut hg = Hypergraph::new();

        let alice = hg.add_node("Person", vec![("name", "Alice"), ("role", "developer")]);
        let bob = hg.add_node("Person", vec![("name", "Bob"), ("role", "designer")]);

        assert_eq!(hg.node_count(), 2);

        let alice_node = hg.get_node(alice).unwrap();
        assert!(alice_node.has_label("Person"));
        assert_eq!(
            alice_node.properties.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(
            alice_node.properties.get("role"),
            Some(&Value::String("developer".to_string()))
        );
    }

    #[test]
    fn test_add_hyperedges() {
        let mut hg = Hypergraph::new();

        let alice = hg.add_node("Person", [("name", "Alice")]);
        let bob = hg.add_node("Person", [("name", "Bob")]);
        let company = hg.add_node("Company", [("name", "Acme")]);

        // Binary edge
        let knows_id = hg
            .add_hyperedge("KNOWS")
            .with_node(alice, "source")
            .with_node(bob, "target")
            .with_properties([("strength", 0.8)])
            .build();

        // Ternary hyperedge
        let works_at_id = hg
            .add_hyperedge("WORKS_AT")
            .with_node(alice, "employee")
            .with_node(company, "employer")
            .with_node(1, "since") // Using a literal as a placeholder for a date node
            .with_properties([("full_time", true)])
            .build();

        assert_eq!(hg.hyperedge_count(), 2);

        let knows_edge = hg.get_hyperedge(knows_id).unwrap();
        assert_eq!(knows_edge.label, "KNOWS");
        assert!(knows_edge.is_binary());
        assert_eq!(knows_edge.role_of_node(alice), Some(&"source".to_string()));
        assert_eq!(knows_edge.role_of_node(bob), Some(&"target".to_string()));

        let works_at_edge = hg.get_hyperedge(works_at_id).unwrap();
        assert_eq!(works_at_edge.label, "WORKS_AT");
        assert_eq!(works_at_edge.arity(), 3);
        assert!(!works_at_edge.is_binary());
    }

    #[test]
    fn test_query_operations() {
        let mut hg = Hypergraph::new();

        let alice = hg.add_node("Person", [("name", "Alice"), ("role", "developer")]);
        let bob = hg.add_node("Person", [("name", "Bob"), ("role", "designer")]);
        let charlie = hg.add_node("Person", [("name", "Charlie"), ("role", "manager")]);
        let acme = hg.add_node("Company", [("name", "Acme")]);

        let _knows1 = hg
            .add_hyperedge("KNOWS")
            .with_node(alice, "source")
            .with_node(bob, "target")
            .build();

        let _knows2 = hg
            .add_hyperedge("KNOWS")
            .with_node(bob, "source")
            .with_node(charlie, "target")
            .build();

        let _works_at = hg
            .add_hyperedge("WORKS_AT")
            .with_node(alice, "employee")
            .with_node(acme, "employer")
            .build();

        // Test nodes by label
        let people = hg.nodes_with_label("Person");
        assert_eq!(people.len(), 3);

        let companies = hg.nodes_with_label("Company");
        assert_eq!(companies.len(), 1);

        // Test hyperedges by label
        let knows_edges = hg.hyperedges_with_label("KNOWS");
        assert_eq!(knows_edges.len(), 2);

        let works_at_edges = hg.hyperedges_with_label("WORKS_AT");
        assert_eq!(works_at_edges.len(), 1);

        // Test hyperedges involving a node
        let alice_edges = hg.hyperedges_involving(alice);
        assert_eq!(alice_edges.len(), 2);

        let bob_edges = hg.hyperedges_involving(bob);
        assert_eq!(bob_edges.len(), 2);

        // Test hyperedges by role
        let alice_as_source = hg.hyperedges_with_role(alice, "source");
        assert_eq!(alice_as_source.len(), 1);

        let alice_as_employee = hg.hyperedges_with_role(alice, "employee");
        assert_eq!(alice_as_employee.len(), 1);
    }

    #[test]
    fn test_binary_edges() {
        let mut hg = Hypergraph::new();

        let alice = hg.add_node("Person", [("name", "Alice")]);
        let bob = hg.add_node("Person", [("name", "Bob")]);
        let charlie = hg.add_node("Person", [("name", "Charlie")]);

        let _knows1 = hg
            .add_hyperedge("KNOWS")
            .with_node(alice, "source")
            .with_node(bob, "target")
            .build();

        let _triangle = hg
            .add_hyperedge("IN_TRIANGLE")
            .with_node(alice, "vertex1")
            .with_node(bob, "vertex2")
            .with_node(charlie, "vertex3")
            .build();

        let binary_edges = hg.binary_edges();
        assert_eq!(binary_edges.len(), 1);

        let knows_edges = hg.binary_edges_with_label("KNOWS");
        assert_eq!(knows_edges.len(), 1);

        let triangle_edges = hg.binary_edges_with_label("IN_TRIANGLE");
        assert_eq!(triangle_edges.len(), 0); // Not binary
    }

    #[test]
    fn test_subgraph_view() {
        let mut hg = Hypergraph::new();

        let alice = hg.add_node("Person", [("name", "Alice"), ("role", "developer")]);
        let bob = hg.add_node("Person", [("name", "Bob"), ("role", "designer")]);
        let charlie = hg.add_node("Person", [("name", "Charlie"), ("role", "manager")]);
        let acme = hg.add_node("Company", [("name", "Acme")]);

        let _knows1 = hg
            .add_hyperedge("KNOWS")
            .with_node(alice, "source")
            .with_node(bob, "target")
            .build();

        let _knows2 = hg
            .add_hyperedge("KNOWS")
            .with_node(bob, "source")
            .with_node(charlie, "target")
            .build();

        let _works_at = hg
            .add_hyperedge("WORKS_AT")
            .with_node(alice, "employee")
            .with_node(acme, "employer")
            .build();

        // Create a subgraph with only "KNOWS" edges
        let knows_subgraph = hg.subgraph(|_, edge| edge.label == "KNOWS");

        assert_eq!(knows_subgraph.node_count(), 3); // alice, bob, charlie
        assert_eq!(knows_subgraph.hyperedge_count(), 2); // only KNOWS edges

        let knows_edges = knows_subgraph.hyperedges_with_label("KNOWS");
        assert_eq!(knows_edges.len(), 2);

        let works_at_edges = knows_subgraph.hyperedges_with_label("WORKS_AT");
        assert_eq!(works_at_edges.len(), 0);
    }

    #[test]
    #[should_panic(expected = "Hyperedges must have arity 2 or more")]
    fn test_hyperedge_arity_validation() {
        let mut hg = Hypergraph::new();
        let alice = hg.add_node("Person", [("name", "Alice")]);

        // This should panic because hyperedges need at least 2 endpoints
        hg.add_hyperedge("INVALID")
            .with_node(alice, "only_endpoint")
            .build();
    }

    #[test]
    fn test_strict_schema_mode() {
        use crate::types::DataType;

        let mut hg = Hypergraph::new().with_strict_schema(true);

        // Register property schemas
        hg.schema_mut()
            .register_property("Person", "age", DataType::Int64);
        hg.schema_mut()
            .register_property("Person", "name", DataType::String);

        // Valid node - properties match schema (using Value directly for mixed types)
        let result = hg.add_node_validated(
            "Person",
            vec![
                ("age".to_string(), Value::Int64(30)),
                ("name".to_string(), Value::String("Alice".to_string())),
            ],
        );
        assert!(result.is_ok());

        // Invalid node - type mismatch (age should be Int64)
        let result = hg.add_node_validated("Person", [("age", "thirty")]);
        assert!(result.is_err());

        let violations = result.unwrap_err();
        assert!(!violations.is_empty());
    }

    #[test]
    fn test_strict_schema_disabled() {
        let mut hg = Hypergraph::new(); // strict_schema defaults to false

        // Without strict schema, any properties are allowed
        let result = hg.add_node_validated("Person", [("anything", "is allowed")]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_hyperedge_build_validated() {
        use crate::types::DataType;

        let mut hg = Hypergraph::new().with_strict_schema(true);

        // Register property schemas
        hg.schema_mut()
            .register_property("KNOWS", "since", DataType::Int64);

        let alice = hg.add_node("Person", [("name", "Alice")]);
        let bob = hg.add_node("Person", [("name", "Bob")]);

        // Valid hyperedge
        let result = hg
            .add_hyperedge("KNOWS")
            .with_node(alice, "source")
            .with_node(bob, "target")
            .with_properties([("since", 2020i64)])
            .build_validated();
        assert!(result.is_ok());

        // Invalid hyperedge - type mismatch
        let result = hg
            .add_hyperedge("KNOWS")
            .with_node(alice, "source")
            .with_node(bob, "target")
            .with_properties([("since", "recently")])
            .build_validated();
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_mut() {
        use crate::types::DataType;

        let mut hg = Hypergraph::new();

        // Initially no property schemas
        assert!(!hg.schema().has_property_schema("Person", "age"));

        // Register via schema_mut
        hg.schema_mut()
            .register_property("Person", "age", DataType::Int64);

        // Now the schema is registered
        assert!(hg.schema().has_property_schema("Person", "age"));
    }

    #[test]
    fn test_validate_schema_valid() {
        use crate::types::DataType;

        let mut hg = Hypergraph::new();
        hg.schema_mut()
            .register_property("Person", "age", DataType::Int64);
        hg.schema_mut()
            .register_property("Person", "name", DataType::String);

        // Add nodes with correct types
        hg.add_node(
            "Person",
            vec![
                ("age", Value::Int64(30)),
                ("name", Value::String("Alice".into())),
            ],
        );
        hg.add_node(
            "Person",
            vec![
                ("age", Value::Int64(25)),
                ("name", Value::String("Bob".into())),
            ],
        );

        // Validate - should have no violations
        let violations = hg.validate_schema(false);
        assert!(violations.is_empty());
        assert!(hg.is_schema_valid(false));
    }

    #[test]
    fn test_validate_schema_type_mismatch() {
        use crate::schema::SchemaViolation;
        use crate::types::DataType;

        let mut hg = Hypergraph::new();
        hg.schema_mut()
            .register_property("Person", "age", DataType::Int64);

        // Add a node with wrong type (string instead of int)
        hg.add_node("Person", [("age", "thirty")]);

        // Validate - should have type mismatch
        let violations = hg.validate_schema(false);
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            SchemaViolation::TypeMismatch { property, .. } if property == "age"
        ));
        assert!(!hg.is_schema_valid(false));
    }

    #[test]
    fn test_validate_schema_strict_mode() {
        use crate::schema::SchemaViolation;
        use crate::types::DataType;

        let mut hg = Hypergraph::new();
        hg.schema_mut()
            .register_property("Person", "age", DataType::Int64);

        // Add a node with an undeclared property
        hg.add_node(
            "Person",
            vec![
                ("age", Value::Int64(30)),
                ("undeclared", Value::String("value".into())),
            ],
        );

        // Non-strict: no violations for undeclared properties
        let violations = hg.validate_schema(false);
        assert!(violations.is_empty());

        // Strict: undeclared properties are violations
        let violations = hg.validate_schema(true);
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            SchemaViolation::UndeclaredProperty { property, .. } if property == "undeclared"
        ));
    }

    #[test]
    fn test_validate_hyperedge_properties() {
        use crate::schema::SchemaViolation;
        use crate::types::DataType;

        let mut hg = Hypergraph::new();
        hg.schema_mut()
            .register_property("KNOWS", "since", DataType::Int64);

        let alice = hg.add_node("Person", [("name", "Alice")]);
        let bob = hg.add_node("Person", [("name", "Bob")]);

        // Add hyperedge with wrong property type
        hg.add_hyperedge("KNOWS")
            .with_node(alice, "source")
            .with_node(bob, "target")
            .with_properties([("since", "recently")]) // Should be Int64
            .build();

        let violations = hg.validate_schema(false);
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            SchemaViolation::TypeMismatch { entity, property, .. } if entity == "KNOWS" && property == "since"
        ));
    }
}
