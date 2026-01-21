//! Project operator for column selection.

use serde::{Deserialize, Serialize};

use crate::expr::LogicalExpr;

use super::LogicalOp;

/// A single projection (expression with optional alias).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Projection {
    /// Expression to project.
    pub expr: LogicalExpr,
    /// Optional output column name.
    pub alias: Option<String>,
}

impl Projection {
    /// Create a new projection.
    pub fn new(expr: LogicalExpr) -> Self {
        Self { expr, alias: None }
    }

    /// Create a projection with an alias.
    pub fn with_alias(expr: LogicalExpr, alias: impl Into<String>) -> Self {
        Self {
            expr,
            alias: Some(alias.into()),
        }
    }
}

/// Project operator - column projection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectOp {
    /// Input operator.
    pub input: Box<LogicalOp>,
    /// Columns to project.
    pub columns: Vec<Projection>,
}

impl ProjectOp {
    /// Create a new project operation.
    pub fn new(input: LogicalOp, columns: Vec<Projection>) -> Self {
        Self {
            input: Box::new(input),
            columns,
        }
    }
}
