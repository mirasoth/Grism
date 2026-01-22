//! Aggregate operator for logical planning (RFC-0002 compliant).
//!
//! Aggregate groups hyperedges by keys and computes aggregate functions.

use serde::{Deserialize, Serialize};

use crate::expr::{AggExpr, LogicalExpr};

/// Aggregate operator - grouping and aggregation.
///
/// # Semantics (RFC-0002, Section 6.7)
///
/// - Groups hyperedges by keys
/// - Computes aggregate functions
///
/// # Rules
///
/// - Produces new hyperedges (one per group)
/// - Aggregate functions MUST be associative and deterministic
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateOp {
    /// Grouping key expressions.
    pub group_keys: Vec<LogicalExpr>,

    /// Aggregate expressions with optional output aliases.
    pub aggregates: Vec<AggExpr>,
}

impl AggregateOp {
    /// Create a new aggregate operation.
    pub const fn new(group_keys: Vec<LogicalExpr>, aggregates: Vec<AggExpr>) -> Self {
        Self {
            group_keys,
            aggregates,
        }
    }

    /// Create an aggregate with no grouping (global aggregate).
    pub const fn global(aggregates: Vec<AggExpr>) -> Self {
        Self {
            group_keys: Vec::new(),
            aggregates,
        }
    }

    /// Create an aggregate grouping by column names.
    pub fn group_by(columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            group_keys: columns
                .into_iter()
                .map(|c| LogicalExpr::column(c.into()))
                .collect(),
            aggregates: Vec::new(),
        }
    }

    /// Add a grouping key.
    #[must_use]
    pub fn with_key(mut self, key: LogicalExpr) -> Self {
        self.group_keys.push(key);
        self
    }

    /// Add an aggregate expression.
    #[must_use]
    pub fn with_agg(mut self, agg: AggExpr) -> Self {
        self.aggregates.push(agg);
        self
    }

    /// Get the output column names.
    pub fn output_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .group_keys
            .iter()
            .map(LogicalExpr::output_name)
            .collect();
        names.extend(self.aggregates.iter().map(AggExpr::output_name));
        names
    }

    /// Check if this is a global aggregate (no grouping keys).
    pub const fn is_global(&self) -> bool {
        self.group_keys.is_empty()
    }

    /// Get all column references from group keys.
    pub fn group_key_refs(&self) -> std::collections::HashSet<String> {
        let mut refs = std::collections::HashSet::new();
        for key in &self.group_keys {
            refs.extend(key.column_refs());
        }
        refs
    }

    /// Get all column references from aggregate expressions.
    pub fn aggregate_refs(&self) -> std::collections::HashSet<String> {
        let mut refs = std::collections::HashSet::new();
        for agg in &self.aggregates {
            refs.extend(agg.expr.column_refs());
        }
        refs
    }

    /// Get all column references (keys + aggregates).
    pub fn column_refs(&self) -> std::collections::HashSet<String> {
        let mut refs = self.group_key_refs();
        refs.extend(self.aggregate_refs());
        refs
    }
}

impl std::fmt::Display for AggregateOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Aggregate(")?;

        if !self.group_keys.is_empty() {
            let keys = self
                .group_keys
                .iter()
                .map(LogicalExpr::output_name)
                .collect::<Vec<_>>()
                .join(", ");
            write!(f, "keys=[{keys}], ")?;
        }

        let aggs = self
            .aggregates
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "aggs=[{aggs}])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::col;

    #[test]
    fn test_aggregate_creation() {
        let agg = AggregateOp::group_by(["department", "year"])
            .with_agg(AggExpr::sum(col("salary")))
            .with_agg(AggExpr::count(col("id")));

        assert_eq!(agg.group_keys.len(), 2);
        assert_eq!(agg.aggregates.len(), 2);
        assert!(!agg.is_global());
    }

    #[test]
    fn test_global_aggregate() {
        let agg = AggregateOp::global(vec![AggExpr::count_star(), AggExpr::sum(col("amount"))]);

        assert!(agg.is_global());
        assert_eq!(agg.aggregates.len(), 2);
    }

    #[test]
    fn test_aggregate_column_refs() {
        let agg = AggregateOp::group_by(["dept"]).with_agg(AggExpr::sum(col("salary")));

        let refs = agg.column_refs();
        assert!(refs.contains("dept"));
        assert!(refs.contains("salary"));
    }

    #[test]
    fn test_aggregate_display() {
        let agg = AggregateOp::group_by(["dept"])
            .with_agg(AggExpr::sum(col("salary")).with_alias("total"));

        let display = agg.to_string();
        assert!(display.contains("Aggregate"));
        assert!(display.contains("dept"));
        assert!(display.contains("SUM"));
    }
}
