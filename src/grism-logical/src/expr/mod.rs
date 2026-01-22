//! Expression system for Grism logical planning (RFC-0003 compliant).
//!
//! This module provides the canonical Expression IR for Grism. Expressions are the
//! smallest executable semantic units used in predicates, projections, aggregations,
//! and inference rules.
//!
//! # Design Principles (RFC-0003)
//!
//! 1. **Determinism first**: Expressions must be referentially transparent unless
//!    explicitly marked otherwise.
//!
//! 2. **Language neutrality**: Expressions form a shared IR across Python and Rust.
//!
//! 3. **Static validation**: All type errors SHOULD be detected prior to execution.
//!
//! 4. **Columnar semantics**: Expressions operate over columns, not rows.
//!
//! # Expression Categories
//!
//! - **Literal**: Constant values (`Int64(42)`, `String("hello")`)
//! - **Column reference**: References to columns in scope (`col("name")`)
//! - **Binary**: Two-operand operations (`a + b`, `x > y`)
//! - **Unary**: Single-operand operations (`NOT x`, `IS NULL`)
//! - **Function**: Function calls (`LENGTH(name)`, `CAST(x AS Float64)`)
//! - **Aggregate**: Aggregate functions (`SUM(amount)`, `COUNT(*)`)
//!
//! # Example
//!
//! ```rust
//! use grism_logical::expr::{LogicalExpr, BinaryOp, AggExpr};
//! use grism_core::Value;
//!
//! // Simple comparison
//! let predicate = LogicalExpr::column("age")
//!     .gt(LogicalExpr::literal(18i64));
//!
//! // Complex expression
//! let complex = LogicalExpr::column("price")
//!     .mul_expr(LogicalExpr::column("quantity"))
//!     .alias("total");
//!
//! // Aggregate
//! let sum = LogicalExpr::aggregate(AggExpr::sum(LogicalExpr::column("amount")));
//! ```

mod agg;
mod binary;
mod expression;
mod func;
mod unary;

pub use agg::{AggExpr, AggFunc};
pub use binary::BinaryOp;
pub use expression::LogicalExpr;
pub use func::{BuiltinFunc, Determinism, FuncCategory, FuncExpr, FuncKind, NullBehavior};
pub use unary::UnaryOp;

// Convenience function for creating column expressions
/// Create a column reference expression.
pub fn col(name: impl Into<String>) -> LogicalExpr {
    LogicalExpr::column(name)
}

/// Create a qualified column reference expression.
pub fn qualified_col(qualifier: impl Into<String>, name: impl Into<String>) -> LogicalExpr {
    LogicalExpr::qualified_column(qualifier, name)
}

/// Create a literal expression.
pub fn lit<V: Into<grism_core::Value>>(value: V) -> LogicalExpr {
    LogicalExpr::literal(value)
}

/// Create a COUNT(*) aggregate.
pub fn count_star() -> LogicalExpr {
    LogicalExpr::aggregate(AggExpr::count_star())
}

/// Create a SUM aggregate.
pub fn sum(expr: LogicalExpr) -> LogicalExpr {
    LogicalExpr::aggregate(AggExpr::sum(expr))
}

/// Create a MIN aggregate.
pub fn min(expr: LogicalExpr) -> LogicalExpr {
    LogicalExpr::aggregate(AggExpr::min(expr))
}

/// Create a MAX aggregate.
pub fn max(expr: LogicalExpr) -> LogicalExpr {
    LogicalExpr::aggregate(AggExpr::max(expr))
}

/// Create an AVG aggregate.
pub fn avg(expr: LogicalExpr) -> LogicalExpr {
    LogicalExpr::aggregate(AggExpr::avg(expr))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convenience_functions() {
        let expr = col("age").gt(lit(18i64));
        assert!(expr.is_deterministic());

        let agg = sum(col("amount"));
        assert!(agg.contains_aggregate());
    }
}
