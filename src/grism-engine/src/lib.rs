//! Local execution engine for Grism.
//!
//! This crate provides the physical planning and execution layer for Grism.
//! It transforms logical plans into physical plans and executes them locally
//! using Arrow-native, vectorized operators.

// Allow for issues that would require extensive API changes or are intentional design choices
#![allow(clippy::unused_self)] // Some trait impls require self
#![allow(clippy::significant_drop_tightening)] // Drop timing is intentional
#![allow(clippy::match_same_arms)] // Explicit match arms for clarity
#![allow(clippy::option_if_let_else)] // Often clearer than map_or
#![allow(clippy::use_self)] // Explicit type names aid readability
#![allow(clippy::unnecessary_wraps)] // API consistency
#![allow(clippy::struct_excessive_bools)] // Config structs need booleans
#![allow(clippy::missing_const_for_fn)] // Many methods can't be const due to trait bounds
#![allow(clippy::return_self_not_must_use)] // Builder methods don't always need must_use
#![allow(clippy::needless_borrow)] // Explicit borrows aid clarity
#![allow(clippy::should_implement_trait)] // Some methods intentionally don't implement traits
#![allow(clippy::or_fun_call)] // Function calls in or patterns
#![allow(clippy::needless_collect)] // Intermediate collections for clarity
#![allow(clippy::needless_pass_by_value)] // Function signatures for consistency

// Allow for numeric conversions that are intentional
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::trivially_copy_pass_by_ref)]

//!
//! # Architecture
//!
//! The execution engine follows a three-stage pipeline:
//!
//! ```text
//! ┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
//! │  LogicalPlan    │ ──▶ │ LocalPhysical    │ ──▶ │   LocalExecutor  │
//! │  (grism-logical)│     │ Planner          │     │                  │
//! └─────────────────┘     └──────────────────┘     └──────────────────┘
//!                                │                         │
//!                                ▼                         ▼
//!                         PhysicalPlan              Arrow RecordBatch
//! ```
//!
//! # Key Components
//!
//! ## Physical Plan Model ([`physical`])
//!
//! - [`PhysicalPlan`]: Wrapper around root operator with execution properties
//! - [`PhysicalSchema`]: Arrow schema with table qualifiers for column resolution
//! - [`PlanProperties`]: Execution mode, partitioning, blocking info
//!
//! ## Physical Operators ([`operators`])
//!
//! All operators implement the [`PhysicalOperator`] trait with a pull-based API:
//!
//! - **Scan**: [`NodeScanExec`], [`HyperedgeScanExec`] - Read from storage
//! - **Transform**: [`FilterExec`], [`ProjectExec`], [`LimitExec`] - Per-batch transforms
//! - **Blocking**: [`SortExec`], [`HashAggregateExec`] - Require all input first
//! - **Graph**: [`AdjacencyExpandExec`], [`RoleExpandExec`] - Graph traversal
//!
//! ## Expression Evaluation ([`expr`])
//!
//! The [`ExprEvaluator`] converts `LogicalExpr` to Arrow arrays:
//!
//! - Arithmetic: `+`, `-`, `*`, `/`, `%`
//! - Comparison: `=`, `<>`, `<`, `<=`, `>`, `>=`
//! - Logical: `AND`, `OR`, `NOT`, `IS NULL`
//! - Special: `CASE`, `IN`, `BETWEEN`
//!
//! ## Execution ([`executor`])
//!
//! - [`LocalExecutor`]: Pulls batches through operator pipeline
//! - [`ExecutionContext`]: Storage, snapshot, memory, metrics
//! - [`CancellationHandle`]: Cooperative cancellation support
//!
//! ## Resource Management
//!
//! - [`MemoryManager`]: Track and limit memory usage
//! - [`MetricsSink`]: Collect per-operator execution statistics
//!
//! # Example
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use grism_engine::{LocalExecutor, LocalPhysicalPlanner, PhysicalPlanner};
//! use grism_logical::{LogicalPlan, ops::ScanOp};
//!
//! // Build a logical plan
//! let scan = ScanOp::nodes_with_label("Person");
//! let logical_plan = LogicalPlan::new(LogicalOp::Scan(scan));
//!
//! // Convert to physical plan
//! let planner = LocalPhysicalPlanner::new();
//! let physical_plan = planner.plan(&logical_plan)?;
//!
//! // Execute
//! let executor = LocalExecutor::new();
//! let result = executor.execute(physical_plan, storage, snapshot).await?;
//!
//! // Process results
//! for batch in result.batches {
//!     println!("Got {} rows", batch.num_rows());
//! }
//! ```
//!
//! # Operator Capabilities
//!
//! Each operator reports its capabilities via [`OperatorCaps`]:
//!
//! | Operator | Blocking | Stateless | Description |
//! |----------|----------|-----------|-------------|
//! | `NodeScanExec` | No | No | Stream nodes from storage |
//! | `FilterExec` | No | Yes | Apply predicate per batch |
//! | `ProjectExec` | No | Yes | Compute expressions per batch |
//! | `LimitExec` | No | No | Limit output rows |
//! | `SortExec` | **Yes** | No | Sort requires all input |
//! | `HashAggregateExec` | **Yes** | No | Aggregation requires all input |
//! | `AdjacencyExpandExec` | No | No | Graph traversal |
//!
//! [`PhysicalPlan`]: physical::PhysicalPlan
//! [`PhysicalSchema`]: physical::PhysicalSchema
//! [`PlanProperties`]: physical::PlanProperties
//! [`PhysicalOperator`]: operators::PhysicalOperator
//! [`NodeScanExec`]: operators::NodeScanExec
//! [`HyperedgeScanExec`]: operators::HyperedgeScanExec
//! [`FilterExec`]: operators::FilterExec
//! [`ProjectExec`]: operators::ProjectExec
//! [`LimitExec`]: operators::LimitExec
//! [`SortExec`]: operators::SortExec
//! [`HashAggregateExec`]: operators::HashAggregateExec
//! [`AdjacencyExpandExec`]: operators::AdjacencyExpandExec
//! [`RoleExpandExec`]: operators::RoleExpandExec
//! [`ExprEvaluator`]: expr::ExprEvaluator
//! [`LocalExecutor`]: executor::LocalExecutor
//! [`ExecutionContext`]: executor::ExecutionContext
//! [`CancellationHandle`]: executor::CancellationHandle
//! [`MemoryManager`]: memory::MemoryManager
//! [`MetricsSink`]: metrics::MetricsSink
//! [`OperatorCaps`]: physical::OperatorCaps

pub mod executor;
pub mod expr;
pub mod memory;
pub mod metrics;
pub mod operators;
pub mod physical;
pub mod planner;

#[cfg(feature = "python")]
pub mod python;

// Re-export commonly used types
pub use executor::{
    CancellationHandle, ExecutionContext, ExecutionContextExt, ExecutionContextTrait,
    ExecutionResult, LocalExecutor, RuntimeConfig,
};
pub use memory::{MemoryManager, MemoryReservation, NoopMemoryManager, TrackingMemoryManager};
pub use metrics::{ExecutionTimer, MetricsSink, OperatorMetrics};
pub use operators::PhysicalOperator;
pub use physical::{
    ExecutionMode, OperatorCaps, PartitioningSpec, PartitioningStrategy, PhysicalPlan,
    PhysicalSchema, PhysicalSchemaBuilder, PlanProperties,
};
pub use planner::{LocalPhysicalPlanner, PhysicalPlanner, PlannerConfig};
