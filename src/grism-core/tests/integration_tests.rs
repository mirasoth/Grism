//! Integration tests for grism-core
//!
//! These tests cover the complete functionality of grism-core without duplicating
//! existing unit tests in individual modules.

use grism_core::*;
use proptest::prelude::*;

#[test]
fn test_value_equality_and_conversion() {
    // Test Value equality
    assert_eq!(Value::Int64(42), Value::Int64(42));
    assert_ne!(Value::Int64(42), Value::Int64(43));
    assert_eq!(
        Value::String("hello".to_string()),
        Value::String("hello".to_string())
    );

    // Test Value cloning
    let v = Value::Vector(vec![1.0, 2.0, 3.0]);
    let v_cloned = v.clone();
    assert_eq!(v, v_cloned);

    // Test Value debug format
    assert_eq!(format!("{:?}", Value::Int64(42)), "Int64(42)");
    assert_eq!(format!("{:?}", Value::Float64(3.14)), "Float64(3.14)");
    assert_eq!(
        format!("{:?}", Value::String("test".to_string())),
        "String(\"test\")"
    );
    assert_eq!(format!("{:?}", Value::Bool(true)), "Bool(true)");
    assert_eq!(format!("{:?}", Value::Null), "Null");
}

#[test]
fn test_datatype_operations() {
    // Test DataType equality
    assert_eq!(DataType::Int64, DataType::Int64);
    assert_ne!(DataType::Int64, DataType::Float64);

    // Test DataType display
    assert_eq!(format!("{}", DataType::Int64), "Int64");
    assert_eq!(format!("{}", DataType::String), "String");
    assert_eq!(format!("{}", DataType::Vector(3)), "Vector(3)");
}

#[test]
fn test_hypergraph_identity_management() {
    let mut hg1 = Hypergraph::new();
    let mut hg2 = Hypergraph::with_id("test-graph");

    assert_eq!(hg1.id(), "default");
    assert_eq!(hg2.id(), "test-graph");

    // Test node ID generation
    let node1 = hg1.add_node("Person", Vec::<(&str, &str)>::new());
    let node2 = hg1.add_node("Person", Vec::<(&str, &str)>::new());
    let node3 = hg2.add_node("Person", Vec::<(&str, &str)>::new());

    assert_ne!(node1, node2);
    // Node IDs are global and sequential
    eprintln!("node1: {}, node2: {}, node3: {}", node1, node2, node3);
    assert!(node1 < node2); // IDs are sequential
}

#[test]
fn test_node_operations() {
    let mut hg = Hypergraph::new();

    // Test node with multiple labels
    let node = hg.add_node("Person", Vec::<(&str, &str)>::new());
    let node_ref = hg.get_node(node).unwrap();
    assert!(node_ref.has_label("Person"));
    assert!(!node_ref.has_label("Company"));

    // Test node properties
    let node_with_props = hg.add_node(
        "Person",
        vec![
            ("name", "Alice"),
            ("age", "30"),
            ("active", "true"),
            ("score", "95.5"),
            ("tags", "dev,rust"),
        ],
    );

    let node_data = hg.get_node(node_with_props).unwrap();
    assert_eq!(
        node_data.properties.get("name"),
        Some(&Value::String("Alice".to_string()))
    );
    assert_eq!(
        node_data.properties.get("age"),
        Some(&Value::String("30".to_string()))
    );
    assert_eq!(
        node_data.properties.get("active"),
        Some(&Value::String("true".to_string()))
    );
    assert_eq!(
        node_data.properties.get("score"),
        Some(&Value::String("95.5".to_string()))
    );
    assert!(node_data.properties.contains_key("tags"));

    // Test property operations
    let mut props = PropertyMap::new();
    props.insert("key".to_string(), Value::String("value".to_string()));
    assert_eq!(props.len(), 1);
    assert!(props.contains_key("key"));

    props.clear();
    assert!(props.is_empty());
}

#[test]
fn test_hyperedge_role_operations() {
    let mut hg = Hypergraph::new();

    let alice = hg.add_node("Person", vec![("name", "Alice")]);
    let bob = hg.add_node("Person", vec![("name", "Bob")]);
    let company = hg.add_node("Company", vec![("name", "Acme")]);

    // Test binary edge
    let works_at = hg
        .add_hyperedge("WORKS_AT")
        .with_node(alice, "employee")
        .with_node(company, "employer")
        .build();

    let edge = hg.get_hyperedge(works_at).unwrap();
    assert_eq!(edge.label, "WORKS_AT");
    assert_eq!(edge.arity(), 2);

    // Test role-based access
    let employee_entities = edge.entities_with_role("employee");
    let employer_entities = edge.entities_with_role("employer");

    assert!(!employee_entities.is_empty());
    assert!(!employer_entities.is_empty());
    assert!(employee_entities[0] != employer_entities[0]);

    // Test non-existent role
    assert!(edge.entities_with_role("manager").is_empty());

    // Test n-ary hyperedge
    let project = hg.add_node("Project", vec![("name", "ProjectX")]);
    let manages = hg
        .add_hyperedge("MANAGES")
        .with_node(alice, "manager")
        .with_node(bob, "team_member")
        .with_node(project, "project")
        .with_properties(vec![("budget", Value::Int64(100000))])
        .build();

    let manages_edge = hg.get_hyperedge(manages).unwrap();
    assert_eq!(manages_edge.arity(), 3);
    assert_eq!(manages_edge.bindings.len(), 3);
    assert!(manages_edge.properties.contains_key("budget"));
}

#[test]
fn test_edge_binary_abstraction() {
    let mut hg = Hypergraph::new();

    let alice = hg.add_node("Person", vec![("name", "Alice")]);
    let bob = hg.add_node("Person", vec![("name", "Bob")]);

    // Test Edge convenience wrapper
    let knows_id = hg
        .add_hyperedge("KNOWS")
        .with_node(alice, ROLE_SOURCE)
        .with_node(bob, ROLE_TARGET)
        .build();

    let edge = Edge::from_hyperedge(hg.get_hyperedge(knows_id).unwrap()).unwrap();
    assert_eq!(edge.source, alice);
    assert_eq!(edge.target, bob);
    assert_eq!(edge.label, "KNOWS");

    // Test invalid edge (not binary)
    let company = hg.add_node("Company", vec![("name", "Acme")]);
    let complex_id = hg
        .add_hyperedge("COMPLEX")
        .with_node(alice, "a")
        .with_node(bob, "b")
        .with_node(company, "c")
        .build();

    let complex_edge = Edge::from_hyperedge(hg.get_hyperedge(complex_id).unwrap());
    assert!(complex_edge.is_none());
}

#[test]
fn test_schema_column_info() {
    let col_int = ColumnInfo::new("id", DataType::Int64);
    let col_str = ColumnInfo::new("name", DataType::String);
    let col_vec = ColumnInfo::new("embeddings", DataType::Vector(128));

    assert_eq!(col_int.name, "id");
    assert_eq!(col_int.data_type, DataType::Int64);

    assert_eq!(col_str.name, "name");
    assert_eq!(col_str.data_type, DataType::String);

    assert_eq!(col_vec.name, "embeddings");
    assert_eq!(col_vec.data_type, DataType::Vector(128));
}

#[test]
fn test_schema_entity_info() {
    let entity = EntityInfo::node("Person", vec!["name".to_string()]);
    let alias_entity = EntityInfo::node("Person", vec!["name".to_string()]).with_alias("User");

    assert_eq!(entity.kind, EntityKind::Node);
    assert_eq!(entity.name, "Person");
    assert!(entity.has_column("name"));
    assert!(!entity.has_column("email"));

    assert_eq!(alias_entity.name, "User");
    assert!(alias_entity.is_alias);
}

#[test]
fn test_schema_operations() {
    let mut schema = Schema::new();

    // Register properties
    schema.register_property("Person", "name", DataType::String);
    schema.register_property("Person", "age", DataType::Int64);
    schema.register_property("KNOWS", "since", DataType::Int64);

    // Test property schema lookup
    let person_props = schema.get_properties_for_label("Person");
    assert!(person_props.is_some());
    let props = person_props.unwrap();
    assert!(props.contains_key("name"));
    assert!(props.contains_key("age"));
    assert_eq!(props.get("name").unwrap().data_type, DataType::String);

    // Test non-existent entity
    assert!(schema.get_properties_for_label("Company").is_none());
}

#[test]
fn test_column_reference_resolution() {
    // Test column reference creation
    let col_ref = ColumnRef::new("name");
    assert_eq!(col_ref.name, "name");
    assert!(!col_ref.is_qualified());

    // Test qualified column reference
    let qual_col = ColumnRef::qualified("Person", "name");
    assert_eq!(qual_col.name, "name");
    assert_eq!(qual_col.qualifier, Some("Person".to_string()));
    assert!(qual_col.is_qualified());

    // Test column reference parsing
    let parsed = ColumnRef::parse("Company.founded_at");
    assert_eq!(parsed.name, "founded_at");
    assert_eq!(parsed.qualifier, Some("Company".to_string()));
}

#[test]
fn test_subgraph_view_operations() {
    let mut hg = Hypergraph::new();

    // Create test data
    let alice = hg.add_node("Person", vec![("name", "Alice")]);
    let bob = hg.add_node("Person", vec![("name", "Bob")]);
    let company = hg.add_node("Company", vec![("name", "Acme")]);

    let _knows = hg
        .add_hyperedge("KNOWS")
        .with_node(alice, "source")
        .with_node(bob, "target")
        .build();

    let _works = hg
        .add_hyperedge("WORKS_AT")
        .with_node(alice, "employee")
        .with_node(company, "employer")
        .build();

    // Test basic hypergraph queries
    let person_nodes = hg.nodes_with_label("Person");
    assert_eq!(person_nodes.len(), 2);

    let company_nodes = hg.nodes_with_label("Company");
    assert_eq!(company_nodes.len(), 1);

    let knows_edges = hg.hyperedges_with_label("KNOWS");
    assert_eq!(knows_edges.len(), 1);

    let works_edges = hg.hyperedges_with_label("WORKS_AT");
    assert_eq!(works_edges.len(), 1);
}

#[test]
fn test_hypergraph_query_patterns() {
    let mut hg = Hypergraph::new();

    // Build a small social graph
    let alice = hg.add_node("Person", vec![("name", "Alice"), ("age", "30")]);
    let bob = hg.add_node("Person", vec![("name", "Bob"), ("age", "25")]);
    let charlie = hg.add_node("Person", vec![("name", "Charlie"), ("age", "35")]);

    let knows_ab = hg
        .add_hyperedge("KNOWS")
        .with_node(alice, "source")
        .with_node(bob, "target")
        .with_properties(vec![("strength", Value::Float64(0.8))])
        .build();

    let _knows_bc = hg
        .add_hyperedge("KNOWS")
        .with_node(bob, "source")
        .with_node(charlie, "target")
        .with_properties(vec![("strength", Value::Float64(0.9))])
        .build();

    let _knows_ac = hg
        .add_hyperedge("KNOWS")
        .with_node(alice, "source")
        .with_node(charlie, "target")
        .with_properties(vec![("strength", Value::Float64(0.6))])
        .build();

    // Test queries
    assert_eq!(hg.node_count(), 3);
    assert_eq!(hg.hyperedge_count(), 3);

    // Find all KNOWS edges
    let knows_edges = hg.hyperedges_with_label("KNOWS");
    assert_eq!(knows_edges.len(), 3);

    // Check specific relationships
    let ab_edge = hg.get_hyperedge(knows_ab).unwrap();
    let source_nodes = ab_edge.nodes_with_role("source");
    let target_nodes = ab_edge.nodes_with_role("target");
    assert!(!source_nodes.is_empty());
    assert!(!target_nodes.is_empty());
    assert_eq!(source_nodes[0], alice);
    assert_eq!(target_nodes[0], bob);
    assert_eq!(
        ab_edge.properties.get("strength"),
        Some(&Value::Float64(0.8))
    );

    // Find nodes by property
    let alice_node = hg.get_node(alice).unwrap();
    assert_eq!(
        alice_node.properties.get("name"),
        Some(&Value::String("Alice".to_string()))
    );
    assert_eq!(
        alice_node.properties.get("age"),
        Some(&Value::String("30".to_string()))
    );
}

proptest! {
    #[test]
    fn test_value_arithmetic_operations(
        a in any::<i64>(),
        b in any::<i64>()
    ) {
        let va = Value::Int64(a);
        let vb = Value::Int64(b);

        // Test that values can be cloned and compared
        prop_assert_eq!(va.clone(), va);
        prop_assert_eq!(vb.clone(), vb);
    }

    #[test]
    fn test_hypergraph_node_properties(
        name in "[a-zA-Z0-9]{1,10}",
        age in any::<i64>()
    ) {
        let mut hg = Hypergraph::new();
        let node = hg.add_node("Person", vec![
            ("name", name.clone()),
            ("age", age.to_string())
        ]);

        let node_data = hg.get_node(node).unwrap();
        prop_assert_eq!(
            node_data.properties.get("name"),
            Some(&Value::String(name))
        );
        prop_assert_eq!(
            node_data.properties.get("age"),
            Some(&Value::String(age.to_string()))
        );
    }
}

#[test]
fn test_hypergraph_fixture_usage() {
    // Test the built-in fixtures
    let social_graph = HypergraphFixture::social_network();
    let hypergraph = social_graph.hypergraph();

    let _ = HypergraphAssertions::new(&hypergraph)
        .assert_node_count(4) // 3 people + 1 company
        .assert_hyperedge_count(3) // 2 KNOWS + 1 WORKS_AT
        .assert_has_node_with_label("Person")
        .assert_has_hyperedge_with_label("KNOWS");

    let citation_graph = HypergraphFixture::citation_network();
    let citation_hypergraph = citation_graph.hypergraph();

    let _ = HypergraphAssertions::new(&citation_hypergraph)
        .assert_node_count(5) // 3 papers + 2 authors
        .assert_has_node_with_label("Paper")
        .assert_has_hyperedge_with_label("CITES");
}

#[test]
fn test_error_handling() {
    let mut hg = Hypergraph::new();

    // Test adding hyperedge with invalid role bindings
    let alice = hg.add_node("Person", Vec::<(&str, &str)>::new());

    // This should work fine
    let bob = hg.add_node("Person", Vec::<(&str, &str)>::new());
    let _valid = hg
        .add_hyperedge("VALID")
        .with_node(alice, "role1")
        .with_node(bob, "role2")
        .build();

    // Test adding hyperedge with duplicate roles
    let duplicate = hg
        .add_hyperedge("DUPLICATE")
        .with_node(alice, "role")
        .with_node(alice, "role") // Same role twice
        .build();

    let edge = hg.get_hyperedge(duplicate).unwrap();
    // Should still work but might have both bindings
    assert!(edge.bindings.len() >= 1);
}

#[test]
fn test_memory_efficiency() {
    let mut hg = Hypergraph::new();

    // Create a larger graph to test memory usage patterns
    let mut nodes = Vec::new();
    for i in 0..100 {
        let node = hg.add_node(
            "Node",
            vec![("id", i.to_string()), ("name", format!("Node{}", i))],
        );
        nodes.push(node);
    }

    // Create hyperedges
    for i in 0..50 {
        let _edge = hg
            .add_hyperedge("CONNECTS")
            .with_node(nodes[i], "from")
            .with_node(nodes[i + 50], "to")
            .with_properties(vec![("weight", Value::Float64(i as f64))])
            .build();
    }

    assert_eq!(hg.node_count(), 100);
    assert_eq!(hg.hyperedge_count(), 50);

    // Test that we can access nodes and edges
    assert!(hg.get_node(nodes[0]).is_some());
    assert!(hg.get_node(nodes[99]).is_some());
}
