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
//! - **`ConstantFolding`**: Evaluate constant expressions at plan time
//! - **`PredicatePushdown`**: Move filters closer to data sources
//! - **`ProjectionPruning`**: Remove unused columns early
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
    ConstantFolding, ExpandReordering, FilterExpandFusion, OptimizationRule, OptimizedPlan,
    Optimizer, OptimizerConfig, PredicatePushdown, ProjectionPruning, RuleTrace, Transformed,
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
            Box::new(FilterExpandFusion),
            Box::new(ExpandReordering),
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
        AggExpr, AggregateOp, ExpandOp, FilterOp, LimitOp, LogicalOp, PlanBuilder, ProjectOp,
        ScanOp, UnionOp, col, lit,
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

    // ============================================================
    // E2E Tests: Multiple Query Chaining Orders
    // Per Phase One Milestone Section 10.1
    // ============================================================

    #[test]
    fn test_chain_filter_project_limit() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("age").gt(lit(18i64))))
            .project(ProjectOp::columns(["name", "city"]))
            .limit(LimitOp::new(10))
            .build();

        let result = optimize(plan).unwrap();
        assert!(result.plan.operator_count() > 0);
    }

    #[test]
    fn test_chain_expand_filter_project() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .expand(
                ExpandOp::binary()
                    .with_edge_label("KNOWS")
                    .with_target_alias("friend"),
            )
            .filter(FilterOp::new(col("friend.age").gt(lit(21i64))))
            .project(ProjectOp::columns(["friend.name"]))
            .build();

        let result = optimize(plan).unwrap();
        assert!(result.plan.operator_count() > 0);
    }

    // ============================================================
    // E2E Tests: Expand Reordering
    // ============================================================

    #[test]
    fn test_expand_reordering_more_selective_first() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .expand(ExpandOp::binary().with_target_alias("other"))
            .expand(
                ExpandOp::binary()
                    .with_edge_label("KNOWS")
                    .with_to_label("Person")
                    .with_target_alias("friend"),
            )
            .build();

        let result = optimize(plan).unwrap();

        // The more selective expand (with KNOWS label) should be on top
        if let LogicalOp::Expand { expand, .. } = result.plan.root() {
            assert_eq!(expand.edge_label, Some("KNOWS".to_string()));
        }
    }

    #[test]
    fn test_expand_reordering_preserves_dependent() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .expand(
                ExpandOp::binary()
                    .with_edge_label("KNOWS")
                    .with_target_alias("friend"),
            )
            .expand(
                ExpandOp::binary()
                    .with_edge_label("WORKS_AT")
                    .with_target_alias("friend"), // Same alias!
            )
            .build();

        let result = optimize(plan).unwrap();

        // Order should be preserved due to alias conflict
        if let LogicalOp::Expand { expand, .. } = result.plan.root() {
            assert_eq!(expand.edge_label, Some("WORKS_AT".to_string()));
        }
    }

    // ============================================================
    // E2E Tests: Constant Folding
    // ============================================================

    #[test]
    fn test_constant_folding_always_false() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(lit(false)))
            .build();

        let result = optimize(plan).unwrap();
        assert!(matches!(result.plan.root(), LogicalOp::Empty));
    }

    // ============================================================
    // E2E Tests: Predicate Pushdown
    // ============================================================

    #[test]
    fn test_predicate_pushdown_through_project() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .project(ProjectOp::columns(["name", "age"]))
            .filter(FilterOp::new(col("age").gt(lit(18i64))))
            .build();

        let result = optimize(plan).unwrap();

        // Filter should be below Project
        if let LogicalOp::Project { input, .. } = result.plan.root() {
            assert!(matches!(input.as_ref(), LogicalOp::Filter { .. }));
        }
    }

    #[test]
    fn test_predicate_pushdown_stops_at_aggregate() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Order"))
            .aggregate(AggregateOp::group_by(["customer_id"]).with_agg(AggExpr::sum(col("amount"))))
            .filter(FilterOp::new(col("customer_id").eq(lit(42i64))))
            .build();

        let result = optimize(plan).unwrap();
        assert!(matches!(result.plan.root(), LogicalOp::Filter { .. }));
    }

    #[test]
    fn test_predicate_pushdown_combines_filters() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("age").gt(lit(18i64))))
            .filter(FilterOp::new(col("active").eq(lit(true))))
            .build();

        let result = optimize(plan).unwrap();

        fn count_filters(op: &LogicalOp) -> usize {
            let self_count = if matches!(op, LogicalOp::Filter { .. }) {
                1
            } else {
                0
            };
            self_count + op.inputs().iter().map(|i| count_filters(i)).sum::<usize>()
        }

        assert_eq!(count_filters(result.plan.root()), 1);
    }

    // ============================================================
    // E2E Tests: Complex Patterns
    // ============================================================

    #[test]
    fn test_complex_graph_traversal() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person").with_alias("p"))
            .expand(
                ExpandOp::binary()
                    .with_edge_label("KNOWS")
                    .with_target_alias("friend"),
            )
            .filter(FilterOp::new(col("friend.age").gt(lit(21i64))))
            .expand(
                ExpandOp::binary()
                    .with_edge_label("WORKS_AT")
                    .with_target_alias("company"),
            )
            .project(ProjectOp::columns([
                "p.name",
                "friend.name",
                "company.name",
            ]))
            .limit(LimitOp::new(100))
            .build();

        let result = optimize(plan).unwrap();
        assert!(result.plan.operator_count() > 0);
    }

    #[test]
    fn test_union_optimization() {
        let left = LogicalOp::project(
            LogicalOp::filter(
                LogicalOp::scan(ScanOp::nodes_with_label("Customer")),
                FilterOp::new(col("active").eq(lit(true))),
            ),
            ProjectOp::columns(["name", "id"]),
        );

        let right = LogicalOp::project(
            LogicalOp::scan(ScanOp::nodes_with_label("Vendor")),
            ProjectOp::columns(["name", "id"]),
        );

        let plan = LogicalPlan::new(LogicalOp::union(left, right, UnionOp::all()));
        let result = optimize(plan).unwrap();

        assert!(
            result
                .plan
                .contains_op(|op| matches!(op, LogicalOp::Union { .. }))
        );
    }

    // ============================================================
    // E2E Tests: Optimizer Properties
    // ============================================================

    #[test]
    fn test_optimizer_determinism() {
        let create_plan = || {
            PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
                .filter(FilterOp::new(col("active").eq(lit(true))))
                .project(ProjectOp::columns(["name", "age"]))
                .build()
        };

        let result1 = optimize(create_plan()).unwrap();
        let result2 = optimize(create_plan()).unwrap();

        assert_eq!(result1.plan.explain(), result2.plan.explain());
        assert_eq!(result1.iterations, result2.iterations);
    }

    #[test]
    fn test_trace_format() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .project(ProjectOp::columns(["name", "age"]))
            .filter(FilterOp::new(col("age").gt(lit(18i64))))
            .build();

        let result = optimize_with_trace(plan).unwrap();
        let trace = result.format_trace();

        assert!(trace.contains("Optimization completed"));
        assert!(trace.contains("iteration"));
        assert!(trace.contains("rule"));
    }

    #[test]
    fn test_empty_plan() {
        let plan = LogicalPlan::new(LogicalOp::Empty);
        let result = optimize(plan).unwrap();
        assert!(matches!(result.plan.root(), LogicalOp::Empty));
    }

    #[test]
    fn test_single_scan() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person")).build();
        let result = optimize(plan).unwrap();
        assert!(matches!(result.plan.root(), LogicalOp::Scan(_)));
    }

    #[test]
    fn test_deeply_nested_filters() {
        let mut plan_op = LogicalOp::scan(ScanOp::nodes_with_label("Person"));

        for _ in 0..10 {
            plan_op = LogicalOp::filter(plan_op, FilterOp::new(col("active").eq(lit(true))));
        }

        let plan = LogicalPlan::new(plan_op);
        let result = optimize(plan).unwrap();

        // Should combine all redundant filters
        assert!(result.rules_applied > 0);
    }
}
