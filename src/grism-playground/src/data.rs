//! Sample data generation for playground examples.
//!
//! This module provides functions to create sample hypergraph data
//! for testing and demonstrations.

use std::sync::Arc;

use common_error::GrismResult;
use grism_core::hypergraph::{Edge, EntityRef, Hyperedge, Node, PropertyMap};
use grism_core::types::Value;
use grism_storage::{InMemoryStorage, Storage};

/// Create a sample social network hypergraph.
///
/// Creates a simple social network with:
/// - Person nodes with name, age, city properties
/// - KNOWS edges between persons
/// - WORKS_AT hyperedges connecting persons to companies with roles
///
/// # Example
///
/// ```rust,ignore
/// let storage = create_social_network().await?;
/// let persons = storage.get_nodes_by_label("Person").await?;
/// println!("Created {} persons", persons.len());
/// ```
pub async fn create_social_network() -> GrismResult<Arc<InMemoryStorage>> {
    let storage = Arc::new(InMemoryStorage::new());

    // Create Person nodes
    let alice = Node::new()
        .with_label("Person")
        .with_properties(properties![
            "name" => "Alice",
            "age" => 30i64,
            "city" => "San Francisco"
        ]);

    let bob = Node::new()
        .with_label("Person")
        .with_properties(properties![
            "name" => "Bob",
            "age" => 25i64,
            "city" => "New York"
        ]);

    let charlie = Node::new()
        .with_label("Person")
        .with_properties(properties![
            "name" => "Charlie",
            "age" => 35i64,
            "city" => "San Francisco"
        ]);

    let diana = Node::new()
        .with_label("Person")
        .with_properties(properties![
            "name" => "Diana",
            "age" => 28i64,
            "city" => "Seattle"
        ]);

    let eve = Node::new()
        .with_label("Person")
        .with_properties(properties![
            "name" => "Eve",
            "age" => 32i64,
            "city" => "New York"
        ]);

    // Create Company nodes
    let acme = Node::new()
        .with_label("Company")
        .with_properties(properties![
            "name" => "Acme Corp",
            "industry" => "Technology",
            "employees" => 500i64
        ]);

    let widgets = Node::new()
        .with_label("Company")
        .with_properties(properties![
            "name" => "Widgets Inc",
            "industry" => "Manufacturing",
            "employees" => 200i64
        ]);

    // Insert nodes
    let alice_id = storage.insert_node(&alice).await?;
    let bob_id = storage.insert_node(&bob).await?;
    let charlie_id = storage.insert_node(&charlie).await?;
    let diana_id = storage.insert_node(&diana).await?;
    let eve_id = storage.insert_node(&eve).await?;
    let acme_id = storage.insert_node(&acme).await?;
    let widgets_id = storage.insert_node(&widgets).await?;

    // Create KNOWS edges (binary relationships)
    // Edge::new takes (label, source, target)
    let edges = vec![
        Edge::new("KNOWS", alice_id, bob_id),
        Edge::new("KNOWS", alice_id, charlie_id),
        Edge::new("KNOWS", bob_id, diana_id),
        Edge::new("KNOWS", charlie_id, diana_id),
        Edge::new("KNOWS", diana_id, eve_id),
        Edge::new("KNOWS", eve_id, alice_id), // Cycle
    ];

    for edge in &edges {
        storage.insert_edge(edge).await?;
    }

    // Create WORKS_AT hyperedges (n-ary relationships)
    // Hyperedge::with_binding(entity, role) - entity first, then role

    // Alice works at Acme as Engineer, reporting to Charlie
    let works_at_1 = Hyperedge::new("WORKS_AT")
        .with_binding(EntityRef::Node(alice_id), "employee")
        .with_binding(EntityRef::Node(acme_id), "company")
        .with_binding(EntityRef::Node(charlie_id), "manager")
        .with_properties(properties![
            "role" => "Engineer",
            "start_year" => 2020i64
        ]);

    // Bob works at Widgets as Analyst
    let works_at_2 = Hyperedge::new("WORKS_AT")
        .with_binding(EntityRef::Node(bob_id), "employee")
        .with_binding(EntityRef::Node(widgets_id), "company")
        .with_properties(properties![
            "role" => "Analyst",
            "start_year" => 2022i64
        ]);

    // Charlie works at Acme as Manager
    let works_at_3 = Hyperedge::new("WORKS_AT")
        .with_binding(EntityRef::Node(charlie_id), "employee")
        .with_binding(EntityRef::Node(acme_id), "company")
        .with_properties(properties![
            "role" => "Manager",
            "start_year" => 2018i64
        ]);

    // Diana works at Acme as Designer
    let works_at_4 = Hyperedge::new("WORKS_AT")
        .with_binding(EntityRef::Node(diana_id), "employee")
        .with_binding(EntityRef::Node(acme_id), "company")
        .with_binding(EntityRef::Node(charlie_id), "manager")
        .with_properties(properties![
            "role" => "Designer",
            "start_year" => 2021i64
        ]);

    storage.insert_hyperedge(&works_at_1).await?;
    storage.insert_hyperedge(&works_at_2).await?;
    storage.insert_hyperedge(&works_at_3).await?;
    storage.insert_hyperedge(&works_at_4).await?;

    // Create MEETING hyperedge (multi-party relationship)
    let meeting = Hyperedge::new("MEETING")
        .with_binding(EntityRef::Node(charlie_id), "organizer")
        .with_binding(EntityRef::Node(alice_id), "attendee")
        .with_binding(EntityRef::Node(diana_id), "attendee")
        .with_binding(EntityRef::Node(acme_id), "location")
        .with_properties(properties![
            "title" => "Weekly Standup",
            "duration_minutes" => 30i64
        ]);

    storage.insert_hyperedge(&meeting).await?;

    Ok(storage)
}

/// Create a minimal sample hypergraph for basic testing.
///
/// Creates a simple graph with:
/// - 3 nodes (A, B, C)
/// - 2 edges (A→B, B→C)
/// - 1 hyperedge connecting all three
pub async fn create_sample_hypergraph() -> GrismResult<Arc<InMemoryStorage>> {
    let storage = Arc::new(InMemoryStorage::new());

    // Create nodes
    let node_a = Node::new()
        .with_label("Node")
        .with_properties(properties!["name" => "A", "value" => 1i64]);
    let node_b = Node::new()
        .with_label("Node")
        .with_properties(properties!["name" => "B", "value" => 2i64]);
    let node_c = Node::new()
        .with_label("Node")
        .with_properties(properties!["name" => "C", "value" => 3i64]);

    let a_id = storage.insert_node(&node_a).await?;
    let b_id = storage.insert_node(&node_b).await?;
    let c_id = storage.insert_node(&node_c).await?;

    // Create edges
    let edge_ab = Edge::new("CONNECTS", a_id, b_id);
    let edge_bc = Edge::new("CONNECTS", b_id, c_id);

    storage.insert_edge(&edge_ab).await?;
    storage.insert_edge(&edge_bc).await?;

    // Create hyperedge
    let triangle = Hyperedge::new("TRIANGLE")
        .with_binding(EntityRef::Node(a_id), "vertex")
        .with_binding(EntityRef::Node(b_id), "vertex")
        .with_binding(EntityRef::Node(c_id), "vertex")
        .with_properties(properties!["type" => "path"]);

    storage.insert_hyperedge(&triangle).await?;

    Ok(storage)
}

/// Macro for creating property maps inline.
#[macro_export]
macro_rules! properties {
    ($($key:literal => $value:expr),* $(,)?) => {{
        let mut map = grism_core::hypergraph::PropertyMap::new();
        $(
            map.insert($key.to_string(), grism_core::types::Value::from($value));
        )*
        map
    }};
}

pub use properties;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_social_network() {
        let storage = create_social_network().await.unwrap();

        let persons = storage.get_nodes_by_label("Person").await.unwrap();
        assert_eq!(persons.len(), 5);

        let companies = storage.get_nodes_by_label("Company").await.unwrap();
        assert_eq!(companies.len(), 2);

        let edges = storage.get_all_edges().await.unwrap();
        assert_eq!(edges.len(), 6);

        let hyperedges = storage.get_all_hyperedges().await.unwrap();
        assert_eq!(hyperedges.len(), 5);
    }

    #[tokio::test]
    async fn test_create_sample_hypergraph() {
        let storage = create_sample_hypergraph().await.unwrap();

        let nodes = storage.get_all_nodes().await.unwrap();
        assert_eq!(nodes.len(), 3);

        let edges = storage.get_all_edges().await.unwrap();
        assert_eq!(edges.len(), 2);

        let hyperedges = storage.get_all_hyperedges().await.unwrap();
        assert_eq!(hyperedges.len(), 1);
    }
}
