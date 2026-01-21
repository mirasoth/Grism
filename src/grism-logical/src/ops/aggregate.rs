//! Aggregate operator for grouping and aggregation.

use serde::{Deserialize, Serialize};

use crate::expr::{AggExpr, LogicalExpr};

use super::LogicalOp;

/// Aggregate operator - grouping and aggregation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateOp {
    /// Input operator.
    pub input: Box<LogicalOp>,
    /// Grouping key expressions.
    pub keys: Vec<LogicalExpr>,
    /// Aggregation expressions.
    pub aggs: Vec<AggExpr>,
}

impl AggregateOp {
    /// Create a new aggregate operation.
    pub fn new(input: LogicalOp, keys: Vec<LogicalExpr>, aggs: Vec<AggExpr>) -> Self {
        Self {
            input: Box::new(input),
            keys,
            aggs,
        }
    }
}
