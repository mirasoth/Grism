//! Aggregation expressions.

use serde::{Deserialize, Serialize};

use super::LogicalExpr;

/// Aggregation function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggFunc {
    /// Count rows.
    Count,
    /// Sum values.
    Sum,
    /// Average values.
    Avg,
    /// Minimum value.
    Min,
    /// Maximum value.
    Max,
    /// Collect values into an array.
    Collect,
}

impl std::fmt::Display for AggFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Count => write!(f, "COUNT"),
            Self::Sum => write!(f, "SUM"),
            Self::Avg => write!(f, "AVG"),
            Self::Min => write!(f, "MIN"),
            Self::Max => write!(f, "MAX"),
            Self::Collect => write!(f, "COLLECT"),
        }
    }
}

/// Aggregation expression.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggExpr {
    /// Aggregation function.
    pub func: AggFunc,
    /// Expression to aggregate (None for COUNT(*)).
    pub expr: Option<LogicalExpr>,
    /// Output column alias.
    pub alias: Option<String>,
}

impl AggExpr {
    /// Create a new aggregation expression.
    pub fn new(func: AggFunc, expr: Option<LogicalExpr>) -> Self {
        Self {
            func,
            expr,
            alias: None,
        }
    }

    /// Set the output alias.
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Create a COUNT(*) expression.
    pub fn count_all() -> Self {
        Self::new(AggFunc::Count, None)
    }

    /// Create a COUNT(expr) expression.
    pub fn count(expr: LogicalExpr) -> Self {
        Self::new(AggFunc::Count, Some(expr))
    }

    /// Create a SUM expression.
    pub fn sum(expr: LogicalExpr) -> Self {
        Self::new(AggFunc::Sum, Some(expr))
    }

    /// Create an AVG expression.
    pub fn avg(expr: LogicalExpr) -> Self {
        Self::new(AggFunc::Avg, Some(expr))
    }

    /// Create a MIN expression.
    pub fn min(expr: LogicalExpr) -> Self {
        Self::new(AggFunc::Min, Some(expr))
    }

    /// Create a MAX expression.
    pub fn max(expr: LogicalExpr) -> Self {
        Self::new(AggFunc::Max, Some(expr))
    }

    /// Create a COLLECT expression.
    pub fn collect(expr: LogicalExpr) -> Self {
        Self::new(AggFunc::Collect, Some(expr))
    }
}

impl std::fmt::Display for AggExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref expr) = self.expr {
            write!(f, "{}({})", self.func, expr)?;
        } else {
            write!(f, "{}(*)", self.func)?;
        }
        if let Some(ref alias) = self.alias {
            write!(f, " AS {}", alias)?;
        }
        Ok(())
    }
}
