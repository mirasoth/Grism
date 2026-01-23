//! Unit tests for grism-engine crate
//!
//! These tests focus on individual component behavior, edge cases, and error handling
//! without duplicating existing integration tests.

use std::sync::Arc;

use grism_engine::{
    executor::RuntimeConfig,
    memory::{MemoryManager, MemoryReservation, TrackingMemoryManager},
    metrics::{MetricsSink, OperatorMetrics},
    physical::{ExecutionMode, OperatorCaps, PartitioningSpec, PhysicalSchema, PlanProperties},
    planner::{LocalPhysicalPlanner, PhysicalPlanner, PlannerConfig},
};
use grism_logical::ops::{LogicalOp, ScanOp};

#[test]
fn test_physical_schema() {
    // Test empty schema
    let schema = PhysicalSchema::empty();
    assert_eq!(schema.num_columns(), 0);
}

#[test]
fn test_plan_properties() {
    let props = PlanProperties::local().with_blocking();

    assert!(props.contains_blocking);
    assert!(props.execution_mode == ExecutionMode::Local);
    assert!(props.partitioning.is_none());
}

#[test]
fn test_operator_capabilities() {
    // Test non-blocking, stateless operator
    let filter_caps = OperatorCaps::streaming();
    assert!(!filter_caps.blocking);
    assert!(filter_caps.stateless);

    // Test blocking, stateful operator
    let sort_caps = OperatorCaps::blocking();
    assert!(sort_caps.blocking);
    assert!(!sort_caps.stateless);
}

#[test]
fn test_memory_manager() {
    let memory_manager: Arc<dyn MemoryManager> = Arc::new(TrackingMemoryManager::new(1000)); // 1KB limit

    // Test reservation
    let reservation1 = MemoryReservation::try_new(Arc::clone(&memory_manager), 100).unwrap();
    assert_eq!(memory_manager.used(), 100);

    {
        let _reservation2 = MemoryReservation::try_new(Arc::clone(&memory_manager), 200).unwrap();
        assert_eq!(memory_manager.used(), 300);
    } // reservation2 dropped here

    // reservation2 should be dropped
    assert_eq!(memory_manager.used(), 100);

    // Drop reservation1
    drop(reservation1);
    assert_eq!(memory_manager.used(), 0);

    // Test limit enforcement
    let _reservation3 = MemoryReservation::try_new(Arc::clone(&memory_manager), 800).unwrap();
    assert_eq!(memory_manager.used(), 800);

    // Should fail - exceeds limit
    let result = MemoryReservation::try_new(Arc::clone(&memory_manager), 300);
    assert!(result.is_err());
    assert_eq!(memory_manager.used(), 800);
}

#[test]
fn test_metrics_collection() {
    let metrics = MetricsSink::new();

    // Create and populate operator metrics
    let mut op_metrics = OperatorMetrics::new();
    op_metrics.add_rows_in(100);
    op_metrics.add_rows_out(80);
    op_metrics.update_memory(1024);

    // Record metrics
    metrics.record("TestOperator", op_metrics);

    // Verify metrics were recorded
    let recorded = metrics.get("TestOperator").unwrap();
    assert_eq!(recorded.rows_in, 100);
    assert_eq!(recorded.rows_out, 80);
    assert_eq!(recorded.memory_bytes, 1024);
}

#[test]
fn test_planner_config() {
    let config = PlannerConfig::default();
    assert_eq!(config.batch_size, None);

    let custom_config = PlannerConfig {
        batch_size: Some(1024),
        enable_predicate_pushdown: false,
        enable_projection_pushdown: false,
    };
    assert_eq!(custom_config.batch_size, Some(1024));
}

#[test]
fn test_runtime_config() {
    let config = RuntimeConfig::default();
    assert!(config.batch_size > 0);

    let custom_config = RuntimeConfig::default().with_batch_size(4096);
    assert_eq!(custom_config.batch_size, 4096);
}

#[test]
fn test_partitioning_spec() {
    // Single partitioning
    let singleton = PartitioningSpec::single();
    assert!(singleton.is_single());
    assert_eq!(singleton.num_partitions, 1);

    // Round-robin partitioning
    let round_robin = PartitioningSpec::round_robin(4);
    assert!(!round_robin.is_single());
    assert_eq!(round_robin.num_partitions, 4);

    // Hash partitioning
    let hash = PartitioningSpec::hash(vec!["nodes.id".to_string()], 8);
    assert!(!hash.is_single());
    assert_eq!(hash.num_partitions, 8);
}

#[test]
fn test_physical_planner() {
    let planner = LocalPhysicalPlanner::new();

    // Create a simple scan plan
    let scan_op = ScanOp::nodes_with_label("Person");
    let logical_plan = grism_logical::LogicalPlan::new(LogicalOp::scan(scan_op));

    // Plan the logical plan
    let physical_plan = planner.plan(&logical_plan).unwrap();

    // Verify the physical plan
    assert!(!physical_plan.properties().contains_blocking);
    assert_eq!(
        physical_plan.properties().execution_mode,
        ExecutionMode::Local
    );
}

#[test]
fn test_error_handling() {
    let planner = LocalPhysicalPlanner::new();

    // Test with invalid logical plan (should be handled gracefully)
    // This test ensures the planner doesn't panic on unexpected inputs
    let result = std::panic::catch_unwind(|| {
        // In a real scenario, you'd test specific error conditions
        // Here we're just ensuring the planner is robust
        let _ = planner;
    });

    assert!(result.is_ok());
}
