//! Limit operator for row limiting.

use serde::{Deserialize, Serialize};

use super::LogicalOp;

/// Limit operator - limits number of rows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitOp {
    /// Input operator.
    pub input: Box<LogicalOp>,
    /// Maximum number of rows.
    pub limit: usize,
    /// Optional offset for pagination.
    pub offset: Option<usize>,
}

impl LimitOp {
    /// Create a new limit operation.
    pub fn new(input: LogicalOp, limit: usize) -> Self {
        Self {
            input: Box::new(input),
            limit,
            offset: None,
        }
    }

    /// Add an offset.
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }
}
