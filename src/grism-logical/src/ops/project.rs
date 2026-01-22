//! Project operator for logical planning (RFC-0002 compliant).
//!
//! Project reduces the column set and computes derived columns.

use serde::{Deserialize, Serialize};

use crate::expr::LogicalExpr;

/// Project operator - column selection and computation.
///
/// # Semantics (RFC-0002, Section 6.4)
///
/// - Reduces column set
/// - Does not alter hyperedge identity
/// - MUST NOT change cardinality
///
/// # Rules
///
/// - Dropping a role removes it from scope
/// - Computed columns can introduce new names via alias
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectOp {
    /// Expressions to project (column refs or computed expressions).
    pub expressions: Vec<LogicalExpr>,
}

impl ProjectOp {
    /// Create a new projection with column names.
    pub fn columns(names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            expressions: names
                .into_iter()
                .map(|n| LogicalExpr::column(n.into()))
                .collect(),
        }
    }

    /// Create a new projection with expressions.
    pub const fn new(expressions: Vec<LogicalExpr>) -> Self {
        Self { expressions }
    }

    /// Add an expression to the projection.
    #[must_use]
    pub fn with_expr(mut self, expr: LogicalExpr) -> Self {
        self.expressions.push(expr);
        self
    }

    /// Add a column to the projection.
    #[must_use]
    pub fn with_column(mut self, name: impl Into<String>) -> Self {
        self.expressions.push(LogicalExpr::column(name.into()));
        self
    }

    /// Add a wildcard to include all columns.
    #[must_use]
    pub fn with_all(mut self) -> Self {
        self.expressions.push(LogicalExpr::Wildcard);
        self
    }

    /// Get the output column names.
    pub fn output_names(&self) -> Vec<String> {
        self.expressions
            .iter()
            .map(LogicalExpr::output_name)
            .collect()
    }

    /// Check if this projection includes a wildcard.
    pub fn has_wildcard(&self) -> bool {
        self.expressions
            .iter()
            .any(|e| matches!(e, LogicalExpr::Wildcard | LogicalExpr::QualifiedWildcard(_)))
    }

    /// Get all column references from projection expressions.
    pub fn column_refs(&self) -> std::collections::HashSet<String> {
        let mut refs = std::collections::HashSet::new();
        for expr in &self.expressions {
            refs.extend(expr.column_refs());
        }
        refs
    }
}

impl std::fmt::Display for ProjectOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cols = self.output_names().join(", ");
        write!(f, "Project({})", cols)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_project_columns() {
        let project = ProjectOp::columns(["name", "age", "city"]);

        assert_eq!(project.expressions.len(), 3);
        assert_eq!(project.output_names(), vec!["name", "age", "city"]);
    }

    #[test]
    fn test_project_expressions() {
        let project = ProjectOp::new(vec![
            LogicalExpr::column("price")
                .mul_expr(LogicalExpr::column("quantity"))
                .alias("total"),
            LogicalExpr::column("name"),
        ]);

        let names = project.output_names();
        assert!(names.contains(&"total".to_string()));
        assert!(names.contains(&"name".to_string()));
    }

    #[test]
    fn test_project_with_wildcard() {
        let project = ProjectOp::columns(["id"]).with_all();

        assert!(project.has_wildcard());
    }

    #[test]
    fn test_project_display() {
        let project = ProjectOp::columns(["a", "b", "c"]);
        assert_eq!(project.to_string(), "Project(a, b, c)");
    }
}
