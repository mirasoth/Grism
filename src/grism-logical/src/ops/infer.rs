//! Infer operator for rule-based reasoning.

use serde::{Deserialize, Serialize};

use super::LogicalOp;

/// Infer operator - applies inference rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferOp {
    /// Input operator.
    pub input: Box<LogicalOp>,
    /// Identifier for the rule set to apply.
    pub rule_set: String,
    /// Whether to materialize inferred edges.
    pub materialize: bool,
}

impl InferOp {
    /// Create a new infer operation.
    pub fn new(input: LogicalOp, rule_set: impl Into<String>) -> Self {
        Self {
            input: Box::new(input),
            rule_set: rule_set.into(),
            materialize: false,
        }
    }

    /// Enable materialization of inferred edges.
    pub fn with_materialize(mut self, materialize: bool) -> Self {
        self.materialize = materialize;
        self
    }
}
