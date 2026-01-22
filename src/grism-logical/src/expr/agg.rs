//! Aggregate functions for logical expressions (RFC-0003 compliant).

use grism_core::DataType;
use serde::{Deserialize, Serialize};

use super::LogicalExpr;

/// Aggregate function types.
///
/// Aggregate functions operate on groups of rows and produce a single result.
/// Per RFC-0003, aggregate functions MUST be associative and deterministic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggFunc {
    /// Count rows (or non-null values)
    Count,
    /// Count distinct values
    CountDistinct,
    /// Sum of values
    Sum,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Average value
    Avg,
    /// First value (deterministic only with ORDER BY)
    First,
    /// Last value (deterministic only with ORDER BY)
    Last,
    /// Collect values into an array
    Collect,
    /// Collect distinct values into an array
    CollectDistinct,
}

impl AggFunc {
    /// Get the result type of this aggregate function given the input type.
    ///
    /// Returns `None` if the operation is not valid for the given type.
    pub fn result_type(&self, input: &DataType) -> Option<DataType> {
        match self {
            // Count always returns Int64
            Self::Count | Self::CountDistinct => Some(DataType::Int64),

            // Sum: numeric types, Int64 promotes to Int64, Float64 stays Float64
            Self::Sum => match input {
                DataType::Int64 => Some(DataType::Int64),
                DataType::Float64 => Some(DataType::Float64),
                _ => None,
            },

            // Min/Max: any orderable type, preserves input type
            Self::Min | Self::Max => match input {
                DataType::Int64
                | DataType::Float64
                | DataType::String
                | DataType::Date
                | DataType::Timestamp => Some(input.clone()),
                _ => None,
            },

            // Avg: numeric types, always returns Float64
            Self::Avg => match input {
                DataType::Int64 | DataType::Float64 => Some(DataType::Float64),
                _ => None,
            },

            // First/Last: preserves input type
            Self::First | Self::Last => Some(input.clone()),

            // Collect: returns array of input type
            Self::Collect | Self::CollectDistinct => Some(DataType::Array(Box::new(input.clone()))),
        }
    }

    /// Get the function name for display.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Count => "COUNT",
            Self::CountDistinct => "COUNT_DISTINCT",
            Self::Sum => "SUM",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Avg => "AVG",
            Self::First => "FIRST",
            Self::Last => "LAST",
            Self::Collect => "COLLECT",
            Self::CollectDistinct => "COLLECT_DISTINCT",
        }
    }

    /// Check if this aggregate is order-dependent.
    pub const fn is_order_dependent(&self) -> bool {
        matches!(self, Self::First | Self::Last)
    }
}

impl std::fmt::Display for AggFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// An aggregate expression with function and input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggExpr {
    /// The aggregate function.
    pub func: AggFunc,
    /// The input expression.
    pub expr: Box<LogicalExpr>,
    /// Whether DISTINCT is applied.
    pub distinct: bool,
    /// Optional alias for the result column.
    pub alias: Option<String>,
}

impl AggExpr {
    /// Create a new aggregate expression.
    pub fn new(func: AggFunc, expr: LogicalExpr) -> Self {
        Self {
            func,
            expr: Box::new(expr),
            distinct: false,
            alias: None,
        }
    }

    /// Set DISTINCT flag.
    #[must_use]
    pub const fn with_distinct(mut self, distinct: bool) -> Self {
        self.distinct = distinct;
        self
    }

    /// Set alias for the result.
    #[must_use]
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Get the result type given the input expression type.
    pub fn result_type(&self, input_type: &DataType) -> Option<DataType> {
        self.func.result_type(input_type)
    }

    /// Get the effective output name.
    pub fn output_name(&self) -> String {
        self.alias.as_ref().map_or_else(
            || format!("{}({})", self.func.name(), self.expr),
            std::clone::Clone::clone,
        )
    }
}

impl std::fmt::Display for AggExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.distinct {
            write!(f, "{}(DISTINCT {})", self.func, self.expr)
        } else {
            write!(f, "{}({})", self.func, self.expr)
        }
    }
}

// Helper functions for creating common aggregates
impl AggExpr {
    /// Create COUNT(*).
    pub fn count_star() -> Self {
        Self::new(AggFunc::Count, LogicalExpr::Wildcard)
    }

    /// Create COUNT(expr).
    pub fn count(expr: LogicalExpr) -> Self {
        Self::new(AggFunc::Count, expr)
    }

    /// Create SUM(expr).
    pub fn sum(expr: LogicalExpr) -> Self {
        Self::new(AggFunc::Sum, expr)
    }

    /// Create MIN(expr).
    pub fn min(expr: LogicalExpr) -> Self {
        Self::new(AggFunc::Min, expr)
    }

    /// Create MAX(expr).
    pub fn max(expr: LogicalExpr) -> Self {
        Self::new(AggFunc::Max, expr)
    }

    /// Create AVG(expr).
    pub fn avg(expr: LogicalExpr) -> Self {
        Self::new(AggFunc::Avg, expr)
    }

    /// Create COLLECT(expr).
    pub fn collect(expr: LogicalExpr) -> Self {
        Self::new(AggFunc::Collect, expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_type() {
        assert_eq!(
            AggFunc::Count.result_type(&DataType::Int64),
            Some(DataType::Int64)
        );
        assert_eq!(
            AggFunc::Count.result_type(&DataType::String),
            Some(DataType::Int64)
        );
    }

    #[test]
    fn test_sum_type() {
        assert_eq!(
            AggFunc::Sum.result_type(&DataType::Int64),
            Some(DataType::Int64)
        );
        assert_eq!(
            AggFunc::Sum.result_type(&DataType::Float64),
            Some(DataType::Float64)
        );
        assert_eq!(AggFunc::Sum.result_type(&DataType::String), None);
    }

    #[test]
    fn test_avg_type() {
        assert_eq!(
            AggFunc::Avg.result_type(&DataType::Int64),
            Some(DataType::Float64)
        );
        assert_eq!(
            AggFunc::Avg.result_type(&DataType::Float64),
            Some(DataType::Float64)
        );
    }

    #[test]
    fn test_collect_type() {
        assert_eq!(
            AggFunc::Collect.result_type(&DataType::String),
            Some(DataType::Array(Box::new(DataType::String)))
        );
    }

    #[test]
    fn test_order_dependency() {
        assert!(AggFunc::First.is_order_dependent());
        assert!(AggFunc::Last.is_order_dependent());
        assert!(!AggFunc::Sum.is_order_dependent());
    }

    #[test]
    fn test_agg_expr_display() {
        let agg = AggExpr::sum(LogicalExpr::Column("amount".to_string()));
        assert_eq!(agg.to_string(), "SUM(amount)");

        let distinct_agg =
            AggExpr::count(LogicalExpr::Column("id".to_string())).with_distinct(true);
        assert_eq!(distinct_agg.to_string(), "COUNT(DISTINCT id)");
    }
}
