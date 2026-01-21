//! Query optimizer for Grism hypergraph database.
//!
//! `grism-optimizer` provides the optimization layer for Grism logical plans.
//! This crate implements the rewrite rules and optimization framework defined
//! in RFC-0006 (Logical Planning & Rewrite Rules).
//!
//! # Overview
//!
//! The optimizer transforms logical plans to improve query performance while
//! preserving semantic equivalence. It applies a set of rewrite rules in
//! fixed-point iteration until no more improvements can be made.
//!
//! # Implemented Rules
//!
//! - **ConstantFolding**: Evaluate constant expressions at plan time
//! - **PredicatePushdown**: Move filters closer to data sources
//! - **ProjectionPruning**: Remove unused columns early
//!
//! # Example
//!
//! ```rust
//! use grism_logical::{LogicalPlan, PlanBuilder};
//! use grism_logical::{ScanOp, FilterOp, ProjectOp};
//! use grism_logical::expr::{col, lit};
//! use grism_optimizer::{Optimizer, optimize};
//!
//! // Build a query plan
//! let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
//!     .project(ProjectOp::columns(["name", "age"]))
//!     .filter(FilterOp::new(col("age").gt(lit(18i64))))
//!     .build();
//!
//! // Optimize the plan
//! let optimized = optimize(plan).unwrap();
//!
//! // The optimizer will push the filter below the projection
//! println!("{}", optimized.plan.explain());
//! ```
//!
//! # Safety Guarantees (RFC-0006)
//!
//! All rewrites in this crate are guaranteed to:
//!
//! 1. Preserve hyperedge identity semantics
//! 2. Preserve column values for all rows
//! 3. Preserve NULL/three-valued logic behavior
//! 4. Preserve determinism classification
//! 5. Not introduce scope/shadowing issues

pub mod rules;

// Re-export commonly used types
pub use rules::{
    ConstantFolding, OptimizationRule, OptimizedPlan, Optimizer, OptimizerConfig,
    PredicatePushdown, ProjectionPruning, RuleTrace, Transformed,
};

use common_error::GrismResult;
use grism_logical::LogicalPlan;

/// Optimize a logical plan using the default optimizer.
///
/// This is a convenience function that creates a default optimizer and
/// applies it to the given plan.
pub fn optimize(plan: LogicalPlan) -> GrismResult<OptimizedPlan> {
    Optimizer::default().optimize(plan)
}

/// Optimize a logical plan with tracing enabled.
///
/// Returns an `OptimizedPlan` with detailed trace information about
/// which rules were applied.
pub fn optimize_with_trace(plan: LogicalPlan) -> GrismResult<OptimizedPlan> {
    let config = OptimizerConfig::default().with_trace(true);
    let optimizer = Optimizer::with_config(
        vec![
            Box::new(ConstantFolding),
            Box::new(PredicatePushdown),
            Box::new(ProjectionPruning),
        ],
        config,
    );
    optimizer.optimize(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_logical::{
        AggExpr, AggregateOp, FilterOp, LimitOp, LogicalOp, PlanBuilder, ProjectOp, ScanOp, col,
        lit,
    };

    #[test]
    fn test_optimize_basic() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("age").gt(lit(18i64))))
            .project(ProjectOp::columns(["name", "city"]))
            .build();

        let result = optimize(plan).unwrap();

        // Should complete without error
        assert!(result.iterations > 0);
    }

    #[test]
    fn test_optimize_with_trace() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .project(ProjectOp::columns(["name", "age"]))
            .filter(FilterOp::new(col("age").gt(lit(18i64))))
            .build();

        let result = optimize_with_trace(plan).unwrap();

        // Should have trace entries
        let trace_output = result.format_trace();
        assert!(trace_output.contains("Optimization completed"));
    }

    #[test]
    fn test_optimize_constant_filter() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(lit(1i64).lt(lit(2i64)))) // Always true
            .build();

        let result = optimize(plan).unwrap();

        // Filter should be removed
        assert!(
            !result
                .plan
                .contains_op(|op| matches!(op, LogicalOp::Filter { .. }))
        );
    }

    #[test]
    fn test_optimize_aggregation() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Order"))
            .aggregate(AggregateOp::group_by(["customer_id"]).with_agg(AggExpr::sum(col("amount"))))
            .limit(LimitOp::new(10))
            .build();

        let result = optimize(plan).unwrap();

        // Should preserve aggregation
        assert!(
            result
                .plan
                .contains_op(|op| matches!(op, LogicalOp::Aggregate { .. }))
        );
    }

    #[test]
    fn test_optimize_complex_plan() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person").with_alias("p"))
            .filter(FilterOp::new(col("active").eq(lit(true))))
            .project(ProjectOp::columns(["name", "age", "city"]))
            .filter(FilterOp::new(col("age").gt(lit(21i64))))
            .project(ProjectOp::columns(["name", "city"]))
            .limit(LimitOp::new(100))
            .build();

        let result = optimize(plan).unwrap();

        // Should optimize successfully
        assert!(result.plan.operator_count() > 0);
    }
}
