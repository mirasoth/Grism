# Local Engine Milestone - Completed

**Status**: ✅ Completed  
**Date**: 2026-01-22

## Overview

The `grism-engine` crate provides a production-ready local execution engine for Grism queries.
It implements a pull-based pipeline architecture with Arrow-native data exchange between operators.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         grism-engine                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────┐    ┌──────────────────┐    ┌─────────────────┐   │
│  │ LogicalPlan │───▶│LocalPhysicalPlanner│───▶│  PhysicalPlan   │   │
│  └─────────────┘    └──────────────────┘    └─────────────────┘   │
│                              │                       │              │
│                              │                       ▼              │
│                              │            ┌─────────────────┐       │
│                              │            │  LocalExecutor  │       │
│                              │            └─────────────────┘       │
│                              │                       │              │
│                              ▼                       ▼              │
│                    ┌──────────────────┐   ┌─────────────────┐      │
│                    │ Schema Inference │   │ ExecutionContext│      │
│                    └──────────────────┘   └─────────────────┘      │
│                                                     │               │
│                                                     ▼               │
│                                           ┌─────────────────┐       │
│                                           │  RecordBatch[]  │       │
│                                           └─────────────────┘       │
└─────────────────────────────────────────────────────────────────────┘
```

## Implemented Components

### 1. Physical Operators

| Operator | Description | Status |
|----------|-------------|--------|
| `NodeScanExec` | Scan nodes by label | ✅ |
| `HyperedgeScanExec` | Scan hyperedges by label | ✅ |
| `FilterExec` | Filter rows by predicate | ✅ |
| `ProjectExec` | Project/compute columns | ✅ |
| `LimitExec` | Limit rows with offset | ✅ |
| `SortExec` | Multi-key sorting | ✅ |
| `HashAggregateExec` | Aggregation with GROUP BY | ✅ |
| `AdjacencyExpandExec` | Binary edge traversal | ✅ |
| `RoleExpandExec` | N-ary hyperedge traversal | ✅ |
| `UnionExec` | Union of two inputs | ✅ |
| `RenameExec` | Rename columns | ✅ |
| `EmptyExec` | Empty result | ✅ |

### 2. Expression Evaluator

The `ExprEvaluator` converts `LogicalExpr` to Arrow arrays with support for:

- **Literals**: Int64, Float64, String, Bool, Null
- **Column References**: Direct and qualified (`entity.column`)
- **Binary Operations**: +, -, *, /, %, =, <>, <, <=, >, >=, AND, OR
- **Unary Operations**: NOT, IS NULL, IS NOT NULL, NEG
- **CASE Expressions**: CASE WHEN...THEN...ELSE...END
- **IN Lists**: expr IN (v1, v2, ...)
- **BETWEEN**: expr BETWEEN low AND high

### 3. Aggregation Functions

| Function | Input Types | Output Type |
|----------|-------------|-------------|
| COUNT(*) | Any | Int64 |
| COUNT(col) | Any | Int64 |
| SUM | Int64, Float64 | Same as input |
| AVG | Int64, Float64 | Float64 |
| MIN | Numeric, String, Date, Timestamp | Same as input |
| MAX | Numeric, String, Date, Timestamp | Same as input |

### 4. Schema Inference

The `schema_inference` module provides:

- `infer_expr_type()` - Infer Arrow DataType from LogicalExpr
- `build_project_schema()` - Build schema for projections including computed expressions
- `build_aggregate_schema()` - Build schema for aggregate operations

### 5. Memory Management

- `MemoryManager` trait with pluggable implementations
- `NoopMemoryManager` - No tracking (default)
- `TrackingMemoryManager` - Memory tracking with limits

### 6. Execution Context

`ExecutionContext` provides:
- Storage access
- Memory management
- Cancellation handling via `CancellationHandle`
- Metrics collection via `MetricsSink`

## Test Coverage

### Unit Tests (97 tests)

Each operator has comprehensive unit tests covering:
- Basic functionality
- Edge cases (empty input, null handling)
- Error conditions
- Operator capabilities

### Integration Tests (33 tests)

End-to-end tests covering the full pipeline with real graph patterns:

#### Basic Operator Tests
| Test | Description |
|------|-------------|
| `test_scan_all_nodes` | Scan nodes by label |
| `test_scan_nodes_by_label` | Scan with label filter |
| `test_filter_simple_predicate` | Filter with > operator |
| `test_filter_equality` | Filter with = operator |
| `test_filter_complex_predicate` | Filter with AND |
| `test_project_select_columns` | Column selection |
| `test_project_computed_expression` | Arithmetic expressions |
| `test_limit_basic` | Basic LIMIT |
| `test_limit_with_offset` | LIMIT with OFFSET |
| `test_sort_ascending` | Sort ASC |
| `test_sort_descending` | Sort DESC |
| `test_aggregate_count` | COUNT(*) |
| `test_aggregate_sum` | SUM(col) |
| `test_aggregate_avg` | AVG(col) |
| `test_aggregate_min_max` | MIN/MAX |
| `test_scan_filter_project` | Combined pipeline |
| `test_scan_filter_sort_limit` | Full query pipeline |

#### Graph Pattern Tests (with Hyperedges)
| Test | Description |
|------|-------------|
| `test_scan_hyperedges` | Scan KNOWS hyperedges |
| `test_scan_hyperedges_works_at` | Scan WORKS_AT hyperedges |
| `test_scan_nary_hyperedge` | Scan n-ary MEETING hyperedges |
| `test_filter_nodes_by_label` | Filter Person nodes |
| `test_filter_company_nodes` | Filter Company nodes |
| `test_complex_filter_on_nodes` | Filter with range predicate |
| `test_aggregate_count_hyperedges` | COUNT on hyperedges |
| `test_sort_nodes_by_id_desc` | Sort nodes descending |
| `test_pipeline_filter_sort_limit` | Complex multi-operator pipeline |
| `test_project_with_label_column` | Project with _label column |
| `test_project_arithmetic_on_ids` | Computed columns with multiplication |
| `test_multiple_aggregates` | COUNT, SUM, MIN, MAX together |
| `test_empty_filter_result` | Empty result handling |
| `test_aggregate_on_empty_result` | Aggregate on empty input |
| `test_limit_zero` | LIMIT 0 edge case |
| `test_offset_beyond_data` | OFFSET beyond data size |

## Usage Example

```rust
use grism_engine::{LocalExecutor, LocalPhysicalPlanner, PhysicalPlanner};
use grism_logical::{LogicalPlan, LogicalOp, ScanOp};
use grism_logical::expr::{col, lit};
use grism_storage::{InMemoryStorage, SnapshotId, Storage};

// Build logical plan
let scan = ScanOp::nodes_with_label("Person");
let filter = FilterOp::new(col("age").gt(lit(21i64)));
let plan = LogicalPlan::new(LogicalOp::filter(LogicalOp::scan(scan), filter));

// Convert to physical plan
let planner = LocalPhysicalPlanner::new();
let physical_plan = planner.plan(&plan)?;

// Execute
let executor = LocalExecutor::new();
let result = executor
    .execute(physical_plan, storage, SnapshotId::default())
    .await?;

// Process results
for batch in result.batches {
    println!("Got {} rows", batch.num_rows());
}
```

## Files Changed

### Created

| File | Description |
|------|-------------|
| `src/grism-engine/src/expr/mod.rs` | Expression module |
| `src/grism-engine/src/expr/evaluator.rs` | Expression evaluator |
| `src/grism-engine/src/planner/schema_inference.rs` | Schema inference |
| `src/grism-engine/tests/integration.rs` | Integration tests |

### Modified

| File | Description |
|------|-------------|
| `src/grism-engine/src/lib.rs` | Added expr module, documentation |
| `src/grism-engine/src/planner/mod.rs` | Added schema_inference export |
| `src/grism-engine/src/planner/local.rs` | Use schema inference |
| `src/grism-engine/src/operators/filter.rs` | Full predicate evaluation |
| `src/grism-engine/src/operators/project.rs` | Full expression evaluation |
| `src/grism-engine/src/operators/aggregate.rs` | Accumulator implementation |
| `src/grism-engine/src/operators/sort.rs` | Sorting implementation |
| `src/grism-engine/src/operators/expand.rs` | Role expand implementation |

## Future Work

1. **Lance Storage Integration** - Connect to persistent storage
2. **More Aggregate Functions** - FIRST, LAST, COLLECT, COLLECT_DISTINCT
3. **Performance Optimization** - Batch size tuning, vectorized execution
4. **Index Support** - Use indexes for predicate pushdown
5. **Distributed Bridge** - Connect to Ray-based distributed execution
