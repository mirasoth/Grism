//! Filter operator for logical planning (RFC-0002 compliant).
//!
//! Filter retains hyperedges where the predicate evaluates to true.

use serde::{Deserialize, Serialize};

use crate::expr::LogicalExpr;

/// Filter operator - restriction of hyperedges by predicate.
///
/// # Semantics (RFC-0002, Section 6.3)
///
/// - Retains hyperedges where predicate evaluates to true
/// - Predicate is a boolean expression over columns
///
/// # Three-Valued Logic
///
/// - TRUE: Row survives
/// - FALSE: Row is filtered out
/// - UNKNOWN (NULL): Treated as FALSE (row is filtered out)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterOp {
    /// The predicate expression (must evaluate to Bool).
    pub predicate: LogicalExpr,
}

impl FilterOp {
    /// Create a new filter operation.
    pub const fn new(predicate: LogicalExpr) -> Self {
        Self { predicate }
    }

    /// Combine this filter with another using AND.
    #[must_use]
    pub fn and(self, other: Self) -> Self {
        Self {
            predicate: self.predicate.and(other.predicate),
        }
    }

    /// Combine this filter with another using OR.
    #[must_use]
    pub fn or(self, other: Self) -> Self {
        Self {
            predicate: self.predicate.or(other.predicate),
        }
    }

    /// Check if this filter's predicate is deterministic.
    pub fn is_deterministic(&self) -> bool {
        self.predicate.is_deterministic()
    }

    /// Get all column references in the predicate.
    pub fn column_refs(&self) -> std::collections::HashSet<String> {
        self.predicate.column_refs()
    }
}

impl std::fmt::Display for FilterOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Filter({})", self.predicate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_creation() {
        let predicate = LogicalExpr::column("age").gt(LogicalExpr::literal(18i64));
        let filter = FilterOp::new(predicate);

        assert!(filter.is_deterministic());
        assert!(filter.column_refs().contains("age"));
    }

    #[test]
    fn test_filter_combination() {
        let f1 = FilterOp::new(LogicalExpr::column("a").gt(LogicalExpr::literal(0i64)));
        let f2 = FilterOp::new(LogicalExpr::column("b").lt(LogicalExpr::literal(100i64)));

        let combined = f1.and(f2);
        let refs = combined.column_refs();

        assert!(refs.contains("a"));
        assert!(refs.contains("b"));
    }

    #[test]
    fn test_filter_display() {
        let filter =
            FilterOp::new(LogicalExpr::column("status").eq(LogicalExpr::literal("active")));

        let display = filter.to_string();
        assert!(display.contains("Filter"));
        assert!(display.contains("status"));
    }
}
