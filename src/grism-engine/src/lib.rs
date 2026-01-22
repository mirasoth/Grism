//! Local execution engine for Grism.
//!
//! This crate provides the physical planning and execution layer for Grism.
//! It transforms logical plans into physical plans and executes them locally
//! using Arrow-native, vectorized operators.

#![allow(clippy::missing_const_for_fn)] // Builder patterns often can't be const
#![allow(clippy::return_self_not_must_use)] // Builder patterns don't always need must_use
#![allow(clippy::unused_self)] // Some methods need self for trait compatibility
#![allow(clippy::doc_markdown)] // Documentation backticks are sometimes unnecessary
#![allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)] // Closures are sometimes clearer
#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation
)] // Some casts are intentional
#![allow(clippy::needless_lifetimes)] // Lifetimes are sometimes needed for clarity
#![allow(clippy::large_enum_variant)] // Some enum variants are intentionally large
#![allow(clippy::too_many_arguments)] // Some functions need many arguments
#![allow(clippy::uninlined_format_args)] // Format args are sometimes clearer inline
#![allow(clippy::significant_drop_in_scrutinee)] // Some temporaries are needed
#![allow(clippy::struct_field_names, clippy::struct_excessive_bools)] // Field names sometimes match struct name, some structs need many bools
#![allow(clippy::trivially_copy_pass_by_ref)] // Some small types are passed by ref for consistency
#![allow(clippy::unnecessary_wraps)] // Some Result wraps are for API consistency
#![allow(clippy::option_if_let_else)] // if let/else is sometimes clearer than map_or
#![allow(clippy::useless_conversion)] // Some conversions are for type clarity
#![allow(clippy::unnecessary_literal_unwrap, clippy::map_unwrap_or)] // Some unwraps are for clarity
#![allow(clippy::needless_collect)] // Some collects are needed for clarity
#![allow(clippy::into_iter_on_ref, clippy::should_implement_trait)] // Some into_iter on refs are intentional, some methods intentionally don't implement traits
#![allow(clippy::bool_comparison)] // Some bool comparisons are clearer
#![allow(clippy::needless_pass_by_value)] // Some pass-by-value is intentional
#![allow(clippy::option_as_ref_deref)] // Some Option<&T> vs &Option<T> are intentional
#![allow(clippy::format_push_string)] // Some format! + push_str patterns are clearer
#![allow(clippy::match_same_arms)] // Some match arms intentionally have same body
#![allow(clippy::needless_borrow)] // Some borrows are for clarity
#![allow(clippy::use_self)] // Some structure name repetition is clearer
#![allow(clippy::or_fun_call)] // Some function calls in unwrap_or are clearer
#![allow(clippy::significant_drop_tightening)] // Some temporaries with Drop must stay alive
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
    CancellationHandle, ExecutionContext, ExecutionResult, LocalExecutor, RuntimeConfig,
};
pub use memory::{MemoryManager, MemoryReservation, NoopMemoryManager, TrackingMemoryManager};
pub use metrics::{ExecutionTimer, MetricsSink, OperatorMetrics};
pub use operators::PhysicalOperator;
pub use physical::{
    ExecutionMode, OperatorCaps, PartitioningSpec, PartitioningStrategy, PhysicalPlan,
    PhysicalSchema, PhysicalSchemaBuilder, PlanProperties,
};
pub use planner::{LocalPhysicalPlanner, PhysicalPlanner, PlannerConfig};
