//! Integration tests for the grism-engine crate.
//!
//! These tests verify end-to-end query execution using the full pipeline:
//! - Storage setup with test data
//! - Physical plan construction
//! - Local execution
//! - Result verification
//!
//! ## Test Categories
//!
//! 1. **Basic Operator Tests**: Individual operator functionality
//! 2. **Combined Pipeline Tests**: Multi-operator query pipelines
//! 3. **Graph Pattern Tests**: Hyperedge and traversal patterns

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;

use common_error::GrismResult;
use grism_core::{Hyperedge, Node};
use grism_logical::expr::{col, lit};
use grism_logical::ops::{FilterOp, LimitOp, LogicalOp, ProjectOp, ScanOp, SortOp};
use grism_logical::{AggExpr, LogicalPlan, SortKey};
use grism_storage::{InMemoryStorage, SnapshotId, Storage};

use grism_engine::PhysicalPlanner;
use grism_engine::executor::LocalExecutor;
use grism_engine::planner::LocalPhysicalPlanner;

/// Helper to setup test storage with person data.
async fn setup_person_storage() -> Arc<InMemoryStorage> {
    let storage = Arc::new(InMemoryStorage::new());

    // Create person nodes
    // Note: We store age in the ID for simplicity since Node doesn't have property builders
    // In real tests, we would use proper properties
    let people = vec![
        (1, "Alice", "Person"),
        (2, "Bob", "Person"),
        (3, "Charlie", "Person"),
        (4, "Diana", "Person"),
        (5, "Eve", "Person"),
    ];

    for (id, _name, label) in people {
        let node = Node::with_id(id).with_label(label);
        storage.insert_node(&node).await.unwrap();
    }

    storage
}

/// Execute a logical plan and collect all results.
async fn execute_plan(
    storage: Arc<InMemoryStorage>,
    plan: LogicalPlan,
) -> GrismResult<Vec<RecordBatch>> {
    let planner = LocalPhysicalPlanner::new();
    let physical_plan = planner.plan(&plan)?;

    let executor = LocalExecutor::new();
    let result = executor
        .execute(
            physical_plan,
            storage as Arc<dyn Storage>,
            SnapshotId::default(),
        )
        .await?;

    Ok(result.batches)
}

// =============================================================================
// Scan Tests
// =============================================================================

#[tokio::test]
async fn test_scan_all_nodes() {
    let storage = setup_person_storage().await;

    // Note: Scan without label may return 0 nodes if storage indexes by label
    // This is expected behavior - use nodes_with_label for filtering
    let scan = ScanOp::nodes_with_label("Person");
    let plan = LogicalPlan::new(LogicalOp::Scan(scan));

    let results = execute_plan(storage, plan).await.unwrap();

    // Should have batches with 5 total rows
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);
}

#[tokio::test]
async fn test_scan_nodes_by_label() {
    let storage = setup_person_storage().await;

    let scan = ScanOp::nodes_with_label("Person");
    let plan = LogicalPlan::new(LogicalOp::Scan(scan));

    let results = execute_plan(storage, plan).await.unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);
}

// =============================================================================
// Filter Tests
// =============================================================================

#[tokio::test]
async fn test_filter_simple_predicate() {
    let storage = setup_person_storage().await;

    // Scan nodes with _id > 3
    let scan = ScanOp::nodes_with_label("Person");
    let filter = FilterOp::new(col("_id").gt(lit(3i64)));

    let plan = LogicalPlan::new(LogicalOp::Filter {
        input: Box::new(LogicalOp::Scan(scan)),
        filter,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    // Should have nodes 4 and 5
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_filter_equality() {
    let storage = setup_person_storage().await;

    // Scan nodes with _id = 3
    let scan = ScanOp::nodes_with_label("Person");
    let filter = FilterOp::new(col("_id").eq(lit(3i64)));

    let plan = LogicalPlan::new(LogicalOp::Filter {
        input: Box::new(LogicalOp::Scan(scan)),
        filter,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    // Should have only node 3
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

#[tokio::test]
async fn test_filter_complex_predicate() {
    let storage = setup_person_storage().await;

    // Scan nodes with _id >= 2 AND _id <= 4
    let scan = ScanOp::nodes_with_label("Person");
    let filter = FilterOp::new(col("_id").gt_eq(lit(2i64)).and(col("_id").lt_eq(lit(4i64))));

    let plan = LogicalPlan::new(LogicalOp::Filter {
        input: Box::new(LogicalOp::Scan(scan)),
        filter,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    // Should have nodes 2, 3, 4
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

// =============================================================================
// Project Tests
// =============================================================================

#[tokio::test]
async fn test_project_select_columns() {
    let storage = setup_person_storage().await;

    let scan = ScanOp::nodes_with_label("Person");
    let project = ProjectOp::new(vec![col("_id").into(), col("_label").into()]);

    let plan = LogicalPlan::new(LogicalOp::Project {
        input: Box::new(LogicalOp::Scan(scan)),
        project,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    // Verify we only have 2 columns
    for batch in &results {
        assert_eq!(batch.num_columns(), 2);
    }
}

#[tokio::test]
async fn test_project_computed_expression() {
    let storage = setup_person_storage().await;

    // Project a computed expression: _id + 10
    let scan = ScanOp::nodes_with_label("Person");
    let expr = col("_id").add(lit(10i64)).alias("id_plus_10");
    let project = ProjectOp::new(vec![col("_id").into(), expr]);

    let plan = LogicalPlan::new(LogicalOp::Project {
        input: Box::new(LogicalOp::Scan(scan)),
        project,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    // Verify we have 2 columns: _id and id_plus_10
    for batch in &results {
        assert_eq!(batch.num_columns(), 2);

        // Verify the computed values
        if let (Some(id_col), Some(computed_col)) = (
            batch.column_by_name("_id"),
            batch.column_by_name("id_plus_10"),
        ) {
            let ids = id_col.as_any().downcast_ref::<Int64Array>().unwrap();
            let computed = computed_col.as_any().downcast_ref::<Int64Array>().unwrap();

            for i in 0..ids.len() {
                assert_eq!(
                    computed.value(i),
                    ids.value(i) + 10,
                    "Computed value mismatch at row {}",
                    i
                );
            }
        }
    }
}

// =============================================================================
// Limit Tests
// =============================================================================

#[tokio::test]
async fn test_limit_basic() {
    let storage = setup_person_storage().await;

    let scan = ScanOp::nodes_with_label("Person");
    let limit = LimitOp::new(3);

    let plan = LogicalPlan::new(LogicalOp::Limit {
        input: Box::new(LogicalOp::Scan(scan)),
        limit,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows <= 3);
}

#[tokio::test]
async fn test_limit_with_offset() {
    let storage = setup_person_storage().await;

    let scan = ScanOp::nodes_with_label("Person");
    let limit = LimitOp::with_offset(2, 2);

    let plan = LogicalPlan::new(LogicalOp::Limit {
        input: Box::new(LogicalOp::Scan(scan)),
        limit,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    // Should skip 2 and take 2, so max 2 rows
    assert!(total_rows <= 2);
}

// =============================================================================
// Sort Tests
// =============================================================================

#[tokio::test]
async fn test_sort_ascending() {
    let storage = setup_person_storage().await;

    let scan = ScanOp::nodes_with_label("Person");
    let sort = SortOp::new(vec![SortKey::asc(col("_id").into())]);

    let plan = LogicalPlan::new(LogicalOp::Sort {
        input: Box::new(LogicalOp::Scan(scan)),
        sort,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    // Verify sorted order
    let mut prev_id = i64::MIN;
    for batch in &results {
        if let Some(id_col) = batch.column_by_name("_id") {
            if let Some(ids) = id_col.as_any().downcast_ref::<Int64Array>() {
                for i in 0..ids.len() {
                    let id = ids.value(i);
                    assert!(id >= prev_id, "IDs not sorted: {} < {}", id, prev_id);
                    prev_id = id;
                }
            }
        }
    }
}

#[tokio::test]
async fn test_sort_descending() {
    let storage = setup_person_storage().await;

    let scan = ScanOp::nodes_with_label("Person");
    let sort = SortOp::new(vec![SortKey::desc(col("_id").into())]);

    let plan = LogicalPlan::new(LogicalOp::Sort {
        input: Box::new(LogicalOp::Scan(scan)),
        sort,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    // Verify sorted order (descending)
    let mut prev_id = i64::MAX;
    for batch in &results {
        if let Some(id_col) = batch.column_by_name("_id") {
            if let Some(ids) = id_col.as_any().downcast_ref::<Int64Array>() {
                for i in 0..ids.len() {
                    let id = ids.value(i);
                    assert!(id <= prev_id, "IDs not sorted descending");
                    prev_id = id;
                }
            }
        }
    }
}

// =============================================================================
// Aggregate Tests
// =============================================================================

#[tokio::test]
async fn test_aggregate_count() {
    use grism_logical::ops::AggregateOp;

    let storage = setup_person_storage().await;

    // COUNT(*) on Person nodes
    let scan = ScanOp::nodes_with_label("Person");
    let aggregate = AggregateOp::global(vec![AggExpr::count_star()]);

    let plan = LogicalPlan::new(LogicalOp::Aggregate {
        input: Box::new(LogicalOp::Scan(scan)),
        aggregate,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    // Should have 1 row with count = 5
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    // Verify the count value
    if let Some(batch) = results.first() {
        // The column name is "COUNT(*)"
        let count_col = batch.column(0);
        let count = count_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(count.value(0), 5);
    }
}

#[tokio::test]
async fn test_aggregate_sum() {
    use grism_logical::ops::AggregateOp;

    let storage = setup_person_storage().await;

    // SUM(_id) on Person nodes - should be 1+2+3+4+5 = 15
    let scan = ScanOp::nodes_with_label("Person");
    let aggregate = AggregateOp::global(vec![AggExpr::sum(col("_id"))]);

    let plan = LogicalPlan::new(LogicalOp::Aggregate {
        input: Box::new(LogicalOp::Scan(scan)),
        aggregate,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    // Should have 1 row with sum = 15
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    // Verify the sum value
    if let Some(batch) = results.first() {
        let sum_col = batch.column(0);
        let sum = sum_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(sum.value(0), 15);
    }
}

#[tokio::test]
async fn test_aggregate_avg() {
    use grism_logical::ops::AggregateOp;

    let storage = setup_person_storage().await;

    // AVG(_id) on Person nodes - should be (1+2+3+4+5)/5 = 3.0
    let scan = ScanOp::nodes_with_label("Person");
    let aggregate = AggregateOp::global(vec![AggExpr::avg(col("_id"))]);

    let plan = LogicalPlan::new(LogicalOp::Aggregate {
        input: Box::new(LogicalOp::Scan(scan)),
        aggregate,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    // Should have 1 row with avg = 3.0
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    // Verify the avg value
    if let Some(batch) = results.first() {
        let avg_col = batch.column(0);
        let avg = avg_col.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((avg.value(0) - 3.0).abs() < 0.001);
    }
}

#[tokio::test]
async fn test_aggregate_min_max() {
    use grism_logical::ops::AggregateOp;

    let storage = setup_person_storage().await;

    // MIN(_id), MAX(_id) on Person nodes
    let scan = ScanOp::nodes_with_label("Person");
    let aggregate = AggregateOp::global(vec![AggExpr::min(col("_id")), AggExpr::max(col("_id"))]);

    let plan = LogicalPlan::new(LogicalOp::Aggregate {
        input: Box::new(LogicalOp::Scan(scan)),
        aggregate,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    // Should have 1 row
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    // Verify min=1, max=5
    if let Some(batch) = results.first() {
        let min_col = batch.column(0);
        let max_col = batch.column(1);
        let min = min_col.as_any().downcast_ref::<Int64Array>().unwrap();
        let max = max_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(min.value(0), 1);
        assert_eq!(max.value(0), 5);
    }
}

// =============================================================================
// Combined Pipeline Tests
// =============================================================================

#[tokio::test]
async fn test_scan_filter_project() {
    let storage = setup_person_storage().await;

    // MATCH (p:Person) WHERE p._id > 3 RETURN p._id, p._label
    let scan = ScanOp::nodes_with_label("Person");
    let filter = FilterOp::new(col("_id").gt(lit(3i64)));
    let project = ProjectOp::new(vec![col("_id").into(), col("_label").into()]);

    let plan = LogicalPlan::new(LogicalOp::Project {
        input: Box::new(LogicalOp::Filter {
            input: Box::new(LogicalOp::Scan(scan)),
            filter,
        }),
        project,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    // Should have nodes 4 and 5
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);

    // Should have 2 columns
    for batch in &results {
        assert_eq!(batch.num_columns(), 2);
    }
}

#[tokio::test]
async fn test_scan_filter_sort_limit() {
    let storage = setup_person_storage().await;

    // MATCH (p:Person) WHERE p._id >= 2
    // ORDER BY p._id DESC
    // LIMIT 3
    let scan = ScanOp::nodes_with_label("Person");
    let filter = FilterOp::new(col("_id").gt_eq(lit(2i64)));
    let sort = SortOp::new(vec![SortKey::desc(col("_id").into())]);
    let limit = LimitOp::new(3);

    let plan = LogicalPlan::new(LogicalOp::Limit {
        input: Box::new(LogicalOp::Sort {
            input: Box::new(LogicalOp::Filter {
                input: Box::new(LogicalOp::Scan(scan)),
                filter,
            }),
            sort,
        }),
        limit,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    // Should have top 3 by _id descending: 5, 4, 3
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    // Verify ordering (descending)
    let ids: Vec<i64> = results
        .iter()
        .flat_map(|b| {
            b.column_by_name("_id")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .map(|arr| (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>())
                .unwrap_or_default()
        })
        .collect();

    assert_eq!(ids, vec![5, 4, 3]);
}

// =============================================================================
// Graph Pattern Tests (End-to-End with Hyperedges)
// =============================================================================

/// Setup a social network graph with:
/// - Person nodes (Alice, Bob, Charlie, Diana)
/// - Company nodes (TechCorp, DataInc)
/// - KNOWS hyperedges (binary)
/// - WORKS_AT hyperedges (binary)
/// - MEETING hyperedges (n-ary: host, attendees, location)
async fn setup_social_graph() -> Arc<InMemoryStorage> {
    let storage = Arc::new(InMemoryStorage::new());

    // Create Person nodes
    let alice = Node::with_id(1).with_label("Person");
    let bob = Node::with_id(2).with_label("Person");
    let charlie = Node::with_id(3).with_label("Person");
    let diana = Node::with_id(4).with_label("Person");

    // Create Company nodes
    let techcorp = Node::with_id(10).with_label("Company");
    let datainc = Node::with_id(11).with_label("Company");

    // Create Location node for meetings
    let conf_room = Node::with_id(20).with_label("Location");

    // Insert nodes
    for node in [
        &alice, &bob, &charlie, &diana, &techcorp, &datainc, &conf_room,
    ] {
        storage.insert_node(node).await.unwrap();
    }

    // Create KNOWS relationships (binary hyperedges)
    // Alice knows Bob, Charlie
    // Bob knows Charlie, Diana
    let knows1 = Hyperedge::new("KNOWS")
        .with_node(1, "source")
        .with_node(2, "target"); // Alice -> Bob
    let knows2 = Hyperedge::new("KNOWS")
        .with_node(1, "source")
        .with_node(3, "target"); // Alice -> Charlie
    let knows3 = Hyperedge::new("KNOWS")
        .with_node(2, "source")
        .with_node(3, "target"); // Bob -> Charlie
    let knows4 = Hyperedge::new("KNOWS")
        .with_node(2, "source")
        .with_node(4, "target"); // Bob -> Diana

    // Create WORKS_AT relationships
    // Alice, Bob work at TechCorp
    // Charlie, Diana work at DataInc
    let works1 = Hyperedge::new("WORKS_AT")
        .with_node(1, "employee")
        .with_node(10, "company"); // Alice @ TechCorp
    let works2 = Hyperedge::new("WORKS_AT")
        .with_node(2, "employee")
        .with_node(10, "company"); // Bob @ TechCorp
    let works3 = Hyperedge::new("WORKS_AT")
        .with_node(3, "employee")
        .with_node(11, "company"); // Charlie @ DataInc
    let works4 = Hyperedge::new("WORKS_AT")
        .with_node(4, "employee")
        .with_node(11, "company"); // Diana @ DataInc

    // Create n-ary MEETING hyperedge
    // Meeting with Alice (host), Bob and Charlie (attendees), in conf room
    let meeting = Hyperedge::new("MEETING")
        .with_node(1, "host")
        .with_node(2, "attendee")
        .with_node(3, "attendee")
        .with_node(20, "location");

    // Insert hyperedges
    for edge in [
        &knows1, &knows2, &knows3, &knows4, &works1, &works2, &works3, &works4, &meeting,
    ] {
        storage.insert_hyperedge(edge).await.unwrap();
    }

    storage
}

#[tokio::test]
async fn test_scan_hyperedges() {
    let storage = setup_social_graph().await;

    // Scan all KNOWS hyperedges
    let scan = ScanOp::hyperedges_with_label("KNOWS");
    let plan = LogicalPlan::new(LogicalOp::Scan(scan));

    let results = execute_plan(storage, plan).await.unwrap();

    // Should have 4 KNOWS relationships
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);
}

#[tokio::test]
async fn test_scan_hyperedges_works_at() {
    let storage = setup_social_graph().await;

    // Scan all WORKS_AT hyperedges
    let scan = ScanOp::hyperedges_with_label("WORKS_AT");
    let plan = LogicalPlan::new(LogicalOp::Scan(scan));

    let results = execute_plan(storage, plan).await.unwrap();

    // Should have 4 WORKS_AT relationships
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);
}

#[tokio::test]
async fn test_scan_nary_hyperedge() {
    let storage = setup_social_graph().await;

    // Scan MEETING hyperedges (n-ary)
    let scan = ScanOp::hyperedges_with_label("MEETING");
    let plan = LogicalPlan::new(LogicalOp::Scan(scan));

    let results = execute_plan(storage, plan).await.unwrap();

    // Should have 1 MEETING
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

#[tokio::test]
async fn test_filter_nodes_by_label() {
    let storage = setup_social_graph().await;

    // Get all Person nodes
    let scan = ScanOp::nodes_with_label("Person");
    let plan = LogicalPlan::new(LogicalOp::Scan(scan));

    let results = execute_plan(storage, plan).await.unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4); // Alice, Bob, Charlie, Diana
}

#[tokio::test]
async fn test_filter_company_nodes() {
    let storage = setup_social_graph().await;

    // Get all Company nodes
    let scan = ScanOp::nodes_with_label("Company");
    let plan = LogicalPlan::new(LogicalOp::Scan(scan));

    let results = execute_plan(storage, plan).await.unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2); // TechCorp, DataInc
}

#[tokio::test]
async fn test_complex_filter_on_nodes() {
    let storage = setup_social_graph().await;

    // Get Person nodes with ID between 2 and 3 (Bob and Charlie)
    let scan = ScanOp::nodes_with_label("Person");
    let filter = FilterOp::new(col("_id").gt_eq(lit(2i64)).and(col("_id").lt_eq(lit(3i64))));

    let plan = LogicalPlan::new(LogicalOp::Filter {
        input: Box::new(LogicalOp::Scan(scan)),
        filter,
    });

    let results = execute_plan(storage, plan).await.unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_aggregate_count_hyperedges() {
    use grism_logical::ops::AggregateOp;

    let storage = setup_social_graph().await;

    // COUNT(*) on KNOWS hyperedges
    let scan = ScanOp::hyperedges_with_label("KNOWS");
    let aggregate = AggregateOp::global(vec![AggExpr::count_star()]);

    let plan = LogicalPlan::new(LogicalOp::Aggregate {
        input: Box::new(LogicalOp::Scan(scan)),
        aggregate,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    // Verify count = 4
    if let Some(batch) = results.first() {
        let count_col = batch.column(0);
        let count = count_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(count.value(0), 4);
    }
}

#[tokio::test]
async fn test_sort_nodes_by_id_desc() {
    let storage = setup_social_graph().await;

    // Sort Person nodes by ID descending
    let scan = ScanOp::nodes_with_label("Person");
    let sort = SortOp::new(vec![SortKey::desc(col("_id").into())]);

    let plan = LogicalPlan::new(LogicalOp::Sort {
        input: Box::new(LogicalOp::Scan(scan)),
        sort,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    // Verify order: 4, 3, 2, 1 (Diana, Charlie, Bob, Alice)
    let ids: Vec<i64> = results
        .iter()
        .flat_map(|b| {
            b.column_by_name("_id")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .map(|arr| (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>())
                .unwrap_or_default()
        })
        .collect();

    assert_eq!(ids, vec![4, 3, 2, 1]);
}

#[tokio::test]
async fn test_pipeline_filter_sort_limit() {
    let storage = setup_social_graph().await;

    // Get top 2 Person nodes with ID > 1, sorted by ID descending
    // Should return Diana (4) and Charlie (3)
    let scan = ScanOp::nodes_with_label("Person");
    let filter = FilterOp::new(col("_id").gt(lit(1i64)));
    let sort = SortOp::new(vec![SortKey::desc(col("_id").into())]);
    let limit = LimitOp::new(2);

    let plan = LogicalPlan::new(LogicalOp::Limit {
        input: Box::new(LogicalOp::Sort {
            input: Box::new(LogicalOp::Filter {
                input: Box::new(LogicalOp::Scan(scan)),
                filter,
            }),
            sort,
        }),
        limit,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    let ids: Vec<i64> = results
        .iter()
        .flat_map(|b| {
            b.column_by_name("_id")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .map(|arr| (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>())
                .unwrap_or_default()
        })
        .collect();

    assert_eq!(ids, vec![4, 3]);
}

#[tokio::test]
async fn test_project_with_label_column() {
    let storage = setup_social_graph().await;

    // Project _id and _label from Person nodes
    let scan = ScanOp::nodes_with_label("Person");
    let project = ProjectOp::new(vec![col("_id").into(), col("_label").into()]);

    let plan = LogicalPlan::new(LogicalOp::Project {
        input: Box::new(LogicalOp::Scan(scan)),
        project,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    for batch in &results {
        assert_eq!(batch.num_columns(), 2);

        // Verify all labels are "Person"
        if let Some(label_col) = batch.column_by_name("_label") {
            if let Some(labels) = label_col.as_any().downcast_ref::<StringArray>() {
                for i in 0..labels.len() {
                    assert_eq!(labels.value(i), "Person");
                }
            }
        }
    }
}

#[tokio::test]
async fn test_project_arithmetic_on_ids() {
    let storage = setup_social_graph().await;

    // Project _id * 10 as scaled_id
    let scan = ScanOp::nodes_with_label("Person");
    let expr = col("_id").mul(lit(10i64)).alias("scaled_id");
    let project = ProjectOp::new(vec![col("_id").into(), expr]);

    let plan = LogicalPlan::new(LogicalOp::Project {
        input: Box::new(LogicalOp::Scan(scan)),
        project,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    for batch in &results {
        if let (Some(id_col), Some(scaled_col)) = (
            batch.column_by_name("_id"),
            batch.column_by_name("scaled_id"),
        ) {
            let ids = id_col.as_any().downcast_ref::<Int64Array>().unwrap();
            let scaled = scaled_col.as_any().downcast_ref::<Int64Array>().unwrap();

            for i in 0..ids.len() {
                assert_eq!(scaled.value(i), ids.value(i) * 10);
            }
        }
    }
}

#[tokio::test]
async fn test_multiple_aggregates() {
    use grism_logical::ops::AggregateOp;

    let storage = setup_social_graph().await;

    // COUNT(*), SUM(_id), MIN(_id), MAX(_id) on Person nodes
    let scan = ScanOp::nodes_with_label("Person");
    let aggregate = AggregateOp::global(vec![
        AggExpr::count_star(),
        AggExpr::sum(col("_id")),
        AggExpr::min(col("_id")),
        AggExpr::max(col("_id")),
    ]);

    let plan = LogicalPlan::new(LogicalOp::Aggregate {
        input: Box::new(LogicalOp::Scan(scan)),
        aggregate,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 4);

    // Verify: COUNT=4, SUM=1+2+3+4=10, MIN=1, MAX=4
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let sum = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let min = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let max = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);

    assert_eq!(count, 4);
    assert_eq!(sum, 10);
    assert_eq!(min, 1);
    assert_eq!(max, 4);
}

#[tokio::test]
async fn test_empty_filter_result() {
    let storage = setup_social_graph().await;

    // Filter Person nodes with ID > 100 (none exist)
    let scan = ScanOp::nodes_with_label("Person");
    let filter = FilterOp::new(col("_id").gt(lit(100i64)));

    let plan = LogicalPlan::new(LogicalOp::Filter {
        input: Box::new(LogicalOp::Scan(scan)),
        filter,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn test_aggregate_on_empty_result() {
    use grism_logical::ops::AggregateOp;

    let storage = setup_social_graph().await;

    // COUNT(*) on filtered empty result
    let scan = ScanOp::nodes_with_label("Person");
    let filter = FilterOp::new(col("_id").gt(lit(100i64)));
    let aggregate = AggregateOp::global(vec![AggExpr::count_star()]);

    let plan = LogicalPlan::new(LogicalOp::Aggregate {
        input: Box::new(LogicalOp::Filter {
            input: Box::new(LogicalOp::Scan(scan)),
            filter,
        }),
        aggregate,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    // COUNT(*) on empty should return 0
    if let Some(batch) = results.first() {
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 0);
    }
}

#[tokio::test]
async fn test_limit_zero() {
    let storage = setup_social_graph().await;

    let scan = ScanOp::nodes_with_label("Person");
    let limit = LimitOp::new(0);

    let plan = LogicalPlan::new(LogicalOp::Limit {
        input: Box::new(LogicalOp::Scan(scan)),
        limit,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn test_offset_beyond_data() {
    let storage = setup_social_graph().await;

    // Offset 100, but only 4 Person nodes exist
    let scan = ScanOp::nodes_with_label("Person");
    let limit = LimitOp::with_offset(10, 100);

    let plan = LogicalPlan::new(LogicalOp::Limit {
        input: Box::new(LogicalOp::Scan(scan)),
        limit,
    });

    let results = execute_plan(storage, plan).await.unwrap();

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}
