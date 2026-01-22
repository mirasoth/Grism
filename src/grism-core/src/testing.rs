//! Testing utilities and helpers for grism-core.
//!
//! This module provides common test patterns, fixtures, and utilities
//! to make testing grism-core components easier and more consistent.

use crate::hypergraph::{EdgeId, Hypergraph, NodeId};
use crate::types::Value;
use std::collections::HashMap;

/// Test fixture builder for creating common hypergraph scenarios.
pub struct HypergraphFixture {
    hypergraph: Hypergraph,
    nodes: HashMap<String, NodeId>,
    edges: HashMap<String, EdgeId>,
}

impl HypergraphFixture {
    /// Create a new empty fixture.
    pub fn new() -> Self {
        Self {
            hypergraph: Hypergraph::new(),
            nodes: HashMap::new(),
            edges: HashMap::new(),
        }
    }

    /// Create a fixture with a simple social network pattern.
    pub fn social_network() -> Self {
        let mut fixture = Self::new();

        // Create people
        let alice = fixture.add_node("Person", "alice", [("name", "Alice"), ("age", "30")]);
        let bob = fixture.add_node("Person", "bob", [("name", "Bob"), ("age", "25")]);
        let charlie = fixture.add_node("Person", "charlie", [("name", "Charlie"), ("age", "35")]);

        // Create company
        let acme = fixture.add_node("Company", "acme", [("name", "Acme Corp")]);

        // Create relationships
        let edge_id = fixture
            .hypergraph
            .add_hyperedge("KNOWS")
            .with_node(alice, "source")
            .with_node(bob, "target")
            .with_properties([("strength", "0.8")])
            .build();
        fixture.edges.insert("alice_knows_bob".to_string(), edge_id);

        let edge_id = fixture
            .hypergraph
            .add_hyperedge("KNOWS")
            .with_node(bob, "source")
            .with_node(charlie, "target")
            .with_properties([("strength", "0.9")])
            .build();
        fixture
            .edges
            .insert("bob_knows_charlie".to_string(), edge_id);

        let edge_id = fixture
            .hypergraph
            .add_hyperedge("WORKS_AT")
            .with_node(alice, "employee")
            .with_node(acme, "employer")
            .with_properties([("since", "2020")])
            .build();
        fixture
            .edges
            .insert("alice_works_at_acme".to_string(), edge_id);

        fixture
    }

    /// Create a fixture with a citation network pattern.
    pub fn citation_network() -> Self {
        let mut fixture = Self::new();

        // Create papers
        let paper1 = fixture.add_node(
            "Paper",
            "paper1",
            [("title", "Foundation of AI"), ("year", "2020")],
        );
        let paper2 = fixture.add_node(
            "Paper",
            "paper2",
            [("title", "Advanced Graph Theory"), ("year", "2021")],
        );
        let paper3 = fixture.add_node(
            "Paper",
            "paper3",
            [("title", "Neural Networks in Practice"), ("year", "2022")],
        );

        // Create authors
        let author1 = fixture.add_node("Author", "author1", [("name", "Dr. Smith")]);
        let author2 = fixture.add_node("Author", "author2", [("name", "Dr. Jones")]);

        // Create citations
        let edge_id = fixture
            .hypergraph
            .add_hyperedge("CITES")
            .with_node(paper2, "citing")
            .with_node(paper1, "cited")
            .build();
        fixture
            .edges
            .insert("paper2_cites_paper1".to_string(), edge_id);

        let edge_id = fixture
            .hypergraph
            .add_hyperedge("CITES")
            .with_node(paper3, "citing")
            .with_node(paper1, "cited")
            .build();
        fixture
            .edges
            .insert("paper3_cites_paper1".to_string(), edge_id);

        let edge_id = fixture
            .hypergraph
            .add_hyperedge("CITES")
            .with_node(paper3, "citing")
            .with_node(paper2, "cited")
            .build();
        fixture
            .edges
            .insert("paper3_cites_paper2".to_string(), edge_id);

        // Create authorships
        let edge_id = fixture
            .hypergraph
            .add_hyperedge("AUTHORED_BY")
            .with_node(paper1, "paper")
            .with_node(author1, "author")
            .build();
        fixture.edges.insert("paper1_authored".to_string(), edge_id);

        let edge_id = fixture
            .hypergraph
            .add_hyperedge("AUTHORED_BY")
            .with_node(paper2, "paper")
            .with_node(author1, "author")
            .build();
        fixture.edges.insert("paper2_authored".to_string(), edge_id);

        let edge_id = fixture
            .hypergraph
            .add_hyperedge("AUTHORED_BY")
            .with_node(paper3, "paper")
            .with_node(author2, "author")
            .build();
        fixture.edges.insert("paper3_authored".to_string(), edge_id);

        fixture
    }

    /// Add a node to the fixture and track it by name.
    pub fn add_node<'a>(
        &mut self,
        label: &str,
        name: &str,
        properties: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> NodeId {
        let node_id = self.hypergraph.add_node(label, properties);
        self.nodes.insert(name.to_string(), node_id);
        node_id
    }

    /// Add a hyperedge to the fixture and track it by name.
    ///
    /// This is a convenience method that builds a simple binary hyperedge.
    /// For more complex hyperedges, use `hypergraph_mut()` directly.
    pub fn add_binary_edge(
        &mut self,
        label: &str,
        name: &str,
        source: NodeId,
        target: NodeId,
    ) -> EdgeId {
        let edge_id = self
            .hypergraph
            .add_hyperedge(label)
            .with_node(source, "source")
            .with_node(target, "target")
            .build();
        self.edges.insert(name.to_string(), edge_id);
        edge_id
    }

    /// Get the hypergraph.
    pub const fn hypergraph(&self) -> &Hypergraph {
        &self.hypergraph
    }

    /// Get the hypergraph mutably.
    pub const fn hypergraph_mut(&mut self) -> &mut Hypergraph {
        &mut self.hypergraph
    }

    /// Track an edge ID by name (for edges created directly on the hypergraph).
    pub fn track_edge(&mut self, name: &str, edge_id: EdgeId) {
        self.edges.insert(name.to_string(), edge_id);
    }

    /// Get a node ID by name.
    pub fn node_id(&self, name: &str) -> Option<NodeId> {
        self.nodes.get(name).copied()
    }

    /// Get an edge ID by name.
    pub fn edge_id(&self, name: &str) -> Option<EdgeId> {
        self.edges.get(name).copied()
    }

    /// Get all node IDs.
    pub fn node_ids(&self) -> impl Iterator<Item = &NodeId> {
        self.nodes.values()
    }

    /// Get all edge IDs.
    pub fn edge_ids(&self) -> impl Iterator<Item = &EdgeId> {
        self.edges.values()
    }
}

impl Default for HypergraphFixture {
    fn default() -> Self {
        Self::new()
    }
}

/// Assertion helpers for testing hypergraphs.
pub struct HypergraphAssertions<'a> {
    hypergraph: &'a Hypergraph,
}

impl<'a> HypergraphAssertions<'a> {
    /// Create new assertions for a hypergraph.
    pub const fn new(hypergraph: &'a Hypergraph) -> Self {
        Self { hypergraph }
    }

    /// Assert that the hypergraph has the expected number of nodes.
    #[must_use]
    pub fn assert_node_count(self, expected: usize) -> Self {
        assert_eq!(
            self.hypergraph.node_count(),
            expected,
            "Expected {} nodes, found {}",
            expected,
            self.hypergraph.node_count()
        );
        self
    }

    /// Assert that the hypergraph has the expected number of hyperedges.
    pub fn assert_hyperedge_count(self, expected: usize) -> Self {
        assert_eq!(
            self.hypergraph.hyperedge_count(),
            expected,
            "Expected {} hyperedges, found {}",
            expected,
            self.hypergraph.hyperedge_count()
        );
        self
    }

    /// Assert that a node with the given label exists.
    pub fn assert_has_node_with_label(self, label: &str) -> Self {
        let nodes = self.hypergraph.nodes_with_label(label);
        assert!(
            !nodes.is_empty(),
            "Expected at least one node with label '{}'",
            label
        );
        self
    }

    /// Assert that a hyperedge with the given label exists.
    pub fn assert_has_hyperedge_with_label(self, label: &str) -> Self {
        let edges = self.hypergraph.hyperedges_with_label(label);
        assert!(
            !edges.is_empty(),
            "Expected at least one hyperedge with label '{}'",
            label
        );
        self
    }

    /// Assert that a node has a specific property.
    pub fn assert_node_has_property(
        self,
        node_id: NodeId,
        property_name: &str,
        expected_value: &Value,
    ) -> Self {
        let node = self.hypergraph.get_node(node_id).unwrap();
        assert_eq!(
            node.properties.get(property_name),
            Some(expected_value),
            "Node {} should have property {} = {:?}",
            node_id,
            property_name,
            expected_value
        );
        self
    }

    /// Assert that a hyperedge involves a specific node.
    pub fn assert_hyperedge_involves(self, edge_id: EdgeId, node_id: NodeId) -> Self {
        let edge = self.hypergraph.get_hyperedge(edge_id).unwrap();
        assert!(
            edge.involves_node(node_id),
            "Hyperedge {} should involve node {}",
            edge_id,
            node_id
        );
        self
    }

    /// Assert that a node has a specific role in a hyperedge.
    pub fn assert_node_has_role(
        self,
        edge_id: EdgeId,
        node_id: NodeId,
        expected_role: &str,
    ) -> Self {
        let edge = self.hypergraph.get_hyperedge(edge_id).unwrap();
        assert_eq!(
            edge.role_of_node(node_id),
            Some(&expected_role.to_string()),
            "Node {} should have role '{}' in hyperedge {}",
            node_id,
            expected_role,
            edge_id
        );
        self
    }
}

/// Macro for creating test properties easily.
#[macro_export]
macro_rules! props {
    ($($key:expr => $val:expr),* $(,)?) => {
        vec![$(($key, $val)),*]
    };
}

/// Macro for asserting test results with better error messages.
#[macro_export]
macro_rules! assert_eq_msg {
    ($left:expr, $right:expr, $msg:expr) => {
        assert_eq!(
            $left, $right,
            "{}: expected {:?}, got {:?}",
            $msg, $right, $left
        );
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixture_social_network() {
        let fixture = HypergraphFixture::social_network();

        // Test basic structure
        let assertions = HypergraphAssertions::new(fixture.hypergraph());
        assertions
            .assert_node_count(4) // 3 people + 1 company
            .assert_hyperedge_count(3) // 2 KNOWS + 1 WORKS_AT
            .assert_has_node_with_label("Person")
            .assert_has_node_with_label("Company")
            .assert_has_hyperedge_with_label("KNOWS")
            .assert_has_hyperedge_with_label("WORKS_AT");

        // Test specific relationships
        let alice = fixture.node_id("alice").unwrap();
        let bob = fixture.node_id("bob").unwrap();
        let alice_knows_bob = fixture.edge_id("alice_knows_bob").unwrap();

        HypergraphAssertions::new(fixture.hypergraph())
            .assert_hyperedge_involves(alice_knows_bob, alice)
            .assert_hyperedge_involves(alice_knows_bob, bob)
            .assert_node_has_role(alice_knows_bob, alice, "source")
            .assert_node_has_role(alice_knows_bob, bob, "target");
    }

    #[test]
    fn test_fixture_citation_network() {
        let fixture = HypergraphFixture::citation_network();

        let assertions = HypergraphAssertions::new(fixture.hypergraph());
        assertions
            .assert_node_count(5) // 3 papers + 2 authors
            .assert_hyperedge_count(6) // 3 CITES + 3 AUTHORED_BY
            .assert_has_node_with_label("Paper")
            .assert_has_node_with_label("Author")
            .assert_has_hyperedge_with_label("CITES")
            .assert_has_hyperedge_with_label("AUTHORED_BY");

        // Test citation structure
        let paper1 = fixture.node_id("paper1").unwrap();
        let paper2 = fixture.node_id("paper2").unwrap();
        let paper2_cites_paper1 = fixture.edge_id("paper2_cites_paper1").unwrap();

        HypergraphAssertions::new(fixture.hypergraph())
            .assert_hyperedge_involves(paper2_cites_paper1, paper1)
            .assert_hyperedge_involves(paper2_cites_paper1, paper2);
    }

    #[test]
    fn test_custom_fixture() {
        let mut fixture = HypergraphFixture::new();

        let task1 = fixture.add_node(
            "Task",
            "task1",
            props!("title" => "Design API", "priority" => "high"),
        );
        let task2 = fixture.add_node(
            "Task",
            "task2",
            props!("title" => "Implement Core", "priority" => "medium"),
        );

        let edge_id = fixture
            .hypergraph_mut()
            .add_hyperedge("DEPENDS_ON")
            .with_node(task2, "dependent")
            .with_node(task1, "dependency")
            .with_properties([("critical", "true")])
            .build();
        fixture.track_edge("task2_depends_task1", edge_id);

        let assertions = HypergraphAssertions::new(fixture.hypergraph());
        assertions
            .assert_node_count(2)
            .assert_hyperedge_count(1)
            .assert_has_node_with_label("Task")
            .assert_has_hyperedge_with_label("DEPENDS_ON");
    }
}
