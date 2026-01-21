//! Filter operator for predicate-based filtering.

use serde::{Deserialize, Serialize};

use crate::expr::LogicalExpr;

use super::LogicalOp;

/// Filter operator - predicate-based row filtering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterOp {
    /// Input operator.
    pub input: Box<LogicalOp>,
    /// Filter predicate (must evaluate to bool).
    pub predicate: LogicalExpr,
}

impl FilterOp {
    /// Create a new filter operation.
    pub fn new(input: LogicalOp, predicate: LogicalExpr) -> Self {
        Self {
            input: Box::new(input),
            predicate,
        }
    }
}
