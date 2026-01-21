//! Logical plan root structure.

use serde::{Deserialize, Serialize};

use grism_core::Schema;

use crate::ops::LogicalOp;

/// Root of a logical query plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalPlan {
    /// Root operator of the plan.
    pub root: LogicalOp,
    /// Output schema (if known).
    pub schema: Option<Schema>,
}

impl LogicalPlan {
    /// Create a new logical plan.
    pub fn new(root: LogicalOp) -> Self {
        Self { root, schema: None }
    }

    /// Create a plan with a known schema.
    pub fn with_schema(root: LogicalOp, schema: Schema) -> Self {
        Self {
            root,
            schema: Some(schema),
        }
    }

    /// Get the root operator.
    pub fn root(&self) -> &LogicalOp {
        &self.root
    }

    /// Get the output schema.
    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }

    /// Display the plan as a tree.
    pub fn explain(&self) -> String {
        self.root.explain(0)
    }
}

impl std::fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.explain())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::{ScanKind, ScanOp};

    #[test]
    fn test_plan_creation() {
        let scan = LogicalOp::Scan(ScanOp {
            kind: ScanKind::Node,
            label: Some("Person".to_string()),
            namespace: None,
        });
        let plan = LogicalPlan::new(scan);

        assert!(plan.schema().is_none());
        let explain = plan.explain();
        assert!(explain.contains("Scan"));
    }
}
