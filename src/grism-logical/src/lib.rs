//! Logical planning layer for Grism hypergraph database.
//!
//! `grism-logical` provides the canonical logical IR and expression system for Grism.
//! This crate implements the semantic layer defined in RFC-0002 (Logical Algebra),
//! RFC-0003 (Expression System), and RFC-0006 (Logical Planning).
//!
//! # Overview
//!
//! The logical layer is responsible for:
//!
//! - **Expression System**: Typed, deterministic expressions for predicates, projections, and aggregations
//! - **Logical Operators**: The canonical operator set (Scan, Expand, Filter, Project, Aggregate, etc.)
//! - **Logical Plan**: A DAG structure representing query semantics
//! - **Plan Building**: Fluent API for constructing plans programmatically
//!
//! # Key Design Principles
//!
//! 1. **Hypergraph-first semantics**: All relations are hyperedges; binary edges are projections
//! 2. **No Join operator**: Relational composition is expressed via `Expand`
//! 3. **Determinism**: All expressions and operators are deterministic unless marked otherwise
//! 4. **Layered naming**: Logical operators are semantic, not algorithmic
//!
//! # Example
//!
//! ```rust
//! use grism_logical::{LogicalPlan, PlanBuilder};
//! use grism_logical::ops::{ScanOp, FilterOp, ProjectOp, LimitOp};
//! use grism_logical::expr::{col, lit};
//!
//! // Build a query: Scan(Person) -> Filter(age > 18) -> Project(name, city) -> Limit(10)
//! let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
//!     .filter(FilterOp::new(col("age").gt(lit(18i64))))
//!     .project(ProjectOp::columns(["name", "city"]))
//!     .limit(LimitOp::new(10))
//!     .build();
//!
//! // Explain the plan
//! println!("{}", plan.explain());
//! ```
//!
//! # Modules
//!
//! - [`expr`]: Expression system (RFC-0003)
//! - [`ops`]: Logical operators (RFC-0002)
//! - [`plan`]: Logical plan structure (RFC-0006)
//!
//! # RFC Compliance
//!
//! This crate implements:
//!
//! - **RFC-0002**: Hypergraph Logical Algebra & Formal Semantics
//! - **RFC-0003**: Expression System & Type Model
//! - **RFC-0006**: Logical Planning & Rewrite Rules (planning phase only)

pub mod expr;
pub mod ops;
mod plan;

// Re-export commonly used types
pub use plan::{LogicalPlan, PlanBuilder};

// Re-export operator types at crate root for convenience
pub use ops::{
    AggregateOp, Direction, ExpandMode, ExpandOp, FilterOp, HopRange, InferMode, InferOp, LimitOp,
    LogicalOp, ProjectOp, RenameOp, ScanKind, ScanOp, SortKey, SortOp, UnionOp,
};

// Re-export expression types at crate root for convenience
pub use expr::{
    AggExpr, AggFunc, BinaryOp, BuiltinFunc, Determinism, FuncCategory, FuncExpr, FuncKind,
    LogicalExpr, NullBehavior, UnaryOp,
};

// Re-export expression convenience functions
pub use expr::{avg, col, count_star, lit, max, min, qualified_col, sum};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_plan() {
        // Scan -> Filter -> Project
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("age").gt_eq(lit(21i64))))
            .project(ProjectOp::columns(["name", "email"]))
            .build();

        let explain = plan.explain();
        assert!(explain.contains("Scan"));
        assert!(explain.contains("Filter"));
        assert!(explain.contains("Project"));
    }

    #[test]
    fn test_aggregation_plan() {
        // Scan -> Aggregate
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Order"))
            .aggregate(
                AggregateOp::group_by(["customer_id"])
                    .with_agg(AggExpr::sum(col("amount")))
                    .with_agg(AggExpr::count_star()),
            )
            .build();

        assert!(plan.contains_op(|op| matches!(op, LogicalOp::Aggregate { .. })));
    }

    #[test]
    fn test_expand_plan() {
        // Scan(Person) -> Expand(KNOWS) -> Filter -> Project
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person").with_alias("p"))
            .expand(
                ExpandOp::binary()
                    .with_edge_label("KNOWS")
                    .with_target_alias("friend"),
            )
            .filter(FilterOp::new(col("friend.age").gt(lit(18i64))))
            .project(ProjectOp::columns(["p.name", "friend.name"]))
            .build();

        assert!(plan.contains_op(|op| matches!(op, LogicalOp::Expand { .. })));
    }

    #[test]
    fn test_expression_builders() {
        let expr = col("price")
            .mul(col("quantity"))
            .sub(col("discount"))
            .alias("net_total");

        assert_eq!(expr.output_name(), "net_total");
        assert!(expr.is_deterministic());
    }

    #[test]
    fn test_union_plan() {
        let left = LogicalOp::scan(ScanOp::nodes_with_label("Customer"));
        let right = LogicalOp::scan(ScanOp::nodes_with_label("Vendor"));
        let union_plan = LogicalPlan::new(LogicalOp::union(left, right, UnionOp::all()));

        assert_eq!(union_plan.root().input_count(), 2);
    }
}
