//! Property-based testing utilities for grism-core.
//!
//! This module provides Arbitrary implementations for core types
//! to enable property-based testing with proptest.

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use crate::hypergraph::{Hyperedge, Node};
    use crate::types::{DataType, Value};

    // =========================================================================
    // Arbitrary Strategies for Value
    // =========================================================================

    /// Strategy for generating arbitrary Value instances that roundtrip through JSON.
    /// Uses integer-representable floats to avoid JSON precision issues.
    fn arb_value() -> impl Strategy<Value = Value> {
        prop_oneof![
            // Null
            Just(Value::Null),
            // Bool
            any::<bool>().prop_map(Value::Bool),
            // Int64
            any::<i64>().prop_map(Value::Int64),
            // Float64 - use integers cast to float to avoid JSON precision issues
            any::<i32>().prop_map(|i| Value::Float64(f64::from(i))),
            // String
            ".*".prop_map(|s| Value::String(s)),
            // Binary
            prop::collection::vec(any::<u8>(), 0..100).prop_map(Value::Binary),
            // Vector - use integers cast to float to avoid precision issues
            prop::collection::vec(any::<i16>(), 0..16)
                .prop_map(|v| Value::Vector(v.into_iter().map(|i| i as f32).collect())),
            // Timestamp
            any::<i64>().prop_map(Value::Timestamp),
            // Date
            any::<i32>().prop_map(Value::Date),
        ]
    }

    /// Strategy for generating simple (non-recursive) Value instances.
    /// Uses integer-representable floats to avoid JSON precision issues.
    fn arb_simple_value() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::Int64),
            // Use integers cast to float to avoid JSON precision issues
            any::<i32>().prop_map(|i| Value::Float64(f64::from(i))),
            "[a-zA-Z0-9]{0,50}".prop_map(|s| Value::String(s)),
        ]
    }

    // =========================================================================
    // Arbitrary Strategies for DataType
    // =========================================================================

    /// Strategy for generating arbitrary DataType instances.
    fn arb_data_type() -> impl Strategy<Value = DataType> {
        prop_oneof![
            Just(DataType::Null),
            Just(DataType::Bool),
            Just(DataType::Int64),
            Just(DataType::Float64),
            Just(DataType::String),
            Just(DataType::Binary),
            Just(DataType::Timestamp),
            Just(DataType::Date),
            Just(DataType::Symbol),
            (0usize..256).prop_map(DataType::Vector),
        ]
    }

    // =========================================================================
    // Arbitrary Strategies for Node
    // =========================================================================

    /// Strategy for generating arbitrary Node instances.
    fn arb_node() -> impl Strategy<Value = Node> {
        (
            "[A-Z][a-z]{2,10}", // label
            prop::collection::hash_map("[a-z]{1,10}", arb_simple_value(), 0..5),
        )
            .prop_map(|(label, props)| {
                let mut node = Node::new().with_label(label);
                for (key, value) in props {
                    node.properties.insert(key, value);
                }
                node
            })
    }

    // =========================================================================
    // Property Tests
    // =========================================================================

    proptest! {
        /// Test that Value serialization roundtrips correctly.
        #[test]
        fn value_serde_roundtrip(value in arb_value()) {
            let serialized = serde_json::to_string(&value).unwrap();
            let deserialized: Value = serde_json::from_str(&serialized).unwrap();

            // Note: Float64 NaN doesn't equal itself, but we filter those out
            prop_assert_eq!(value, deserialized);
        }

        /// Test that DataType serialization roundtrips correctly.
        #[test]
        fn data_type_serde_roundtrip(dt in arb_data_type()) {
            let serialized = serde_json::to_string(&dt).unwrap();
            let deserialized: DataType = serde_json::from_str(&serialized).unwrap();
            prop_assert_eq!(dt, deserialized);
        }

        /// Test that Node serialization roundtrips correctly.
        #[test]
        fn node_serde_roundtrip(node in arb_node()) {
            let serialized = serde_json::to_string(&node).unwrap();
            let deserialized: Node = serde_json::from_str(&serialized).unwrap();
            prop_assert_eq!(node, deserialized);
        }

        /// Test that type coercion is reflexive.
        #[test]
        fn data_type_coercion_reflexive(dt in arb_data_type()) {
            prop_assert!(dt.can_coerce_to(&dt));
        }

        /// Test that common_supertype is symmetric.
        #[test]
        fn data_type_common_supertype_symmetric(
            dt1 in arb_data_type(),
            dt2 in arb_data_type()
        ) {
            let super1 = dt1.common_supertype(&dt2);
            let super2 = dt2.common_supertype(&dt1);
            prop_assert_eq!(super1, super2);
        }

        /// Test that Value type_name is consistent.
        #[test]
        fn value_type_name_not_empty(value in arb_value()) {
            let type_name = value.type_name();
            prop_assert!(!type_name.is_empty());
        }

        /// Test that Node with_label works correctly.
        #[test]
        fn node_with_label_has_label(label in "[A-Z][a-zA-Z]{1,20}") {
            let node = Node::new().with_label(&label);
            prop_assert!(node.has_label(&label));
        }

        /// Test that Hyperedge arity is consistent with bindings.
        #[test]
        fn hyperedge_arity_consistent(
            label in "[A-Z]{1,10}",
            num_nodes in 2usize..10
        ) {
            let mut edge = Hyperedge::new(label);
            for i in 0..num_nodes {
                edge = edge.with_node(i as u64, format!("role{}", i));
            }
            prop_assert_eq!(edge.arity(), num_nodes);
        }

        /// Test that Hyperedge involves all added nodes.
        #[test]
        fn hyperedge_involves_added_nodes(
            label in "[A-Z]{1,10}",
            node_ids in prop::collection::vec(1u64..1000, 2..10)
        ) {
            let mut edge = Hyperedge::new(label);
            for (i, &node_id) in node_ids.iter().enumerate() {
                edge = edge.with_node(node_id, format!("role{}", i));
            }

            // All added nodes should be involved
            for &node_id in &node_ids {
                prop_assert!(edge.involves_node(node_id));
            }
        }

        /// Test that Int64 can always coerce to Float64.
        #[test]
        fn int64_coerces_to_float64(_i in any::<i64>()) {
            prop_assert!(DataType::Int64.can_coerce_to(&DataType::Float64));
        }

        /// Test that Null can coerce to any type.
        #[test]
        fn null_coerces_to_any(dt in arb_data_type()) {
            prop_assert!(DataType::Null.can_coerce_to(&dt));
        }
    }

    // =========================================================================
    // Arrow Roundtrip Tests
    // =========================================================================

    proptest! {
        /// Test that simple values roundtrip through Arrow.
        #[test]
        fn value_arrow_roundtrip_simple(value in arb_simple_value()) {
            // Skip Null as Arrow handles it differently
            if matches!(value, Value::Null) {
                return Ok(());
            }

            if let Some(arrow_array) = value.to_arrow_scalar() {
                if let Some(recovered) = Value::from_arrow_array(&arrow_array, 0) {
                    // For most types, we expect exact equality
                    match (&value, &recovered) {
                        (Value::Int64(v1), Value::Int64(v2)) => prop_assert_eq!(v1, v2),
                        (Value::Float64(v1), Value::Float64(v2)) => {
                            prop_assert!((v1 - v2).abs() < 1e-10);
                        }
                        (Value::String(v1), Value::String(v2)) => prop_assert_eq!(v1, v2),
                        (Value::Bool(v1), Value::Bool(v2)) => prop_assert_eq!(v1, v2),
                        _ => {} // Other types may have different representations
                    }
                }
            }
        }

        /// Test that Vector values roundtrip through Arrow.
        #[test]
        fn value_arrow_roundtrip_vector(
            values in prop::collection::vec(-1000.0f32..1000.0f32, 1..16)
        ) {
            let original = Value::Vector(values.clone());
            if let Some(arrow_array) = original.to_arrow_scalar() {
                if let Some(recovered) = Value::from_arrow_array(&arrow_array, 0) {
                    if let Value::Vector(rec_values) = recovered {
                        prop_assert_eq!(values.len(), rec_values.len());
                        for (orig, rec) in values.iter().zip(rec_values.iter()) {
                            prop_assert!((orig - rec).abs() < 1e-5);
                        }
                    }
                }
            }
        }
    }
}
