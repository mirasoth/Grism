//! Sort operator for logical planning (RFC-0002 compliant).
//!
//! Sort orders the output rows.

use serde::{Deserialize, Serialize};

use crate::expr::LogicalExpr;

/// Sort key specification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortKey {
    /// Expression to sort by.
    pub expr: LogicalExpr,

    /// Sort direction (ascending if true).
    pub ascending: bool,

    /// Nulls first (if true, NULLs come before non-NULLs).
    pub nulls_first: bool,
}

impl SortKey {
    /// Create a new ascending sort key.
    pub const fn asc(expr: LogicalExpr) -> Self {
        Self {
            expr,
            ascending: true,
            nulls_first: false,
        }
    }

    /// Create a new descending sort key.
    pub const fn desc(expr: LogicalExpr) -> Self {
        Self {
            expr,
            ascending: false,
            nulls_first: true, // Typically NULLs first for DESC
        }
    }

    /// Set nulls first.
    #[must_use]
    pub const fn nulls_first(mut self) -> Self {
        self.nulls_first = true;
        self
    }

    /// Set nulls last.
    #[must_use]
    pub const fn nulls_last(mut self) -> Self {
        self.nulls_first = false;
        self
    }
}

impl std::fmt::Display for SortKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let dir = if self.ascending { "ASC" } else { "DESC" };
        let nulls = if self.nulls_first {
            "NULLS FIRST"
        } else {
            "NULLS LAST"
        };
        write!(f, "{} {} {}", self.expr.output_name(), dir, nulls)
    }
}

/// Sort operator - row ordering.
///
/// # Semantics (RFC-0002, Section 6.9)
///
/// - Purely logical constraint
/// - Defines output ordering
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortOp {
    /// Sort keys in order of precedence.
    pub keys: Vec<SortKey>,
}

impl SortOp {
    /// Create a new sort operation.
    pub const fn new(keys: Vec<SortKey>) -> Self {
        Self { keys }
    }

    /// Create a single-key ascending sort.
    pub fn asc(expr: LogicalExpr) -> Self {
        Self {
            keys: vec![SortKey::asc(expr)],
        }
    }

    /// Create a single-key descending sort.
    pub fn desc(expr: LogicalExpr) -> Self {
        Self {
            keys: vec![SortKey::desc(expr)],
        }
    }

    /// Add a sort key.
    #[must_use]
    pub fn then_by(mut self, key: SortKey) -> Self {
        self.keys.push(key);
        self
    }

    /// Add an ascending sort key.
    #[must_use]
    pub fn then_asc(mut self, expr: LogicalExpr) -> Self {
        self.keys.push(SortKey::asc(expr));
        self
    }

    /// Add a descending sort key.
    #[must_use]
    pub fn then_desc(mut self, expr: LogicalExpr) -> Self {
        self.keys.push(SortKey::desc(expr));
        self
    }

    /// Get all column references from sort keys.
    pub fn column_refs(&self) -> std::collections::HashSet<String> {
        let mut refs = std::collections::HashSet::new();
        for key in &self.keys {
            refs.extend(key.expr.column_refs());
        }
        refs
    }
}

impl std::fmt::Display for SortOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let keys = self
            .keys
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "Sort({})", keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::col;

    #[test]
    fn test_sort_creation() {
        let sort = SortOp::asc(col("name")).then_desc(col("age"));

        assert_eq!(sort.keys.len(), 2);
        assert!(sort.keys[0].ascending);
        assert!(!sort.keys[1].ascending);
    }

    #[test]
    fn test_sort_column_refs() {
        let sort = SortOp::asc(col("a")).then_desc(col("b"));

        let refs = sort.column_refs();
        assert!(refs.contains("a"));
        assert!(refs.contains("b"));
    }

    #[test]
    fn test_sort_display() {
        let sort = SortOp::desc(col("created_at"));
        assert!(sort.to_string().contains("DESC"));
    }
}
