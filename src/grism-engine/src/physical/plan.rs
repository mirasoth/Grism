//! Physical plan structure.

use std::fmt::Write;
use std::sync::Arc;

use crate::operators::PhysicalOperator;
use crate::physical::{PhysicalSchema, PlanProperties};

/// A physical execution plan.
///
/// Physical plans are DAGs of physical operators that can be directly executed.
/// Unlike logical plans, physical plans are fully resolved and backend-specific.
#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    /// The root operator of this plan.
    root: Arc<dyn PhysicalOperator>,
    /// Plan-level properties.
    properties: PlanProperties,
}

impl PhysicalPlan {
    /// Create a new physical plan with the given root operator.
    pub fn new(root: Arc<dyn PhysicalOperator>) -> Self {
        Self {
            root,
            properties: PlanProperties::default(),
        }
    }

    /// Create with custom properties.
    pub fn with_properties(root: Arc<dyn PhysicalOperator>, properties: PlanProperties) -> Self {
        Self { root, properties }
    }

    /// Get the root operator.
    pub fn root(&self) -> &Arc<dyn PhysicalOperator> {
        &self.root
    }

    /// Get a clone of the root operator Arc.
    pub fn root_arc(&self) -> Arc<dyn PhysicalOperator> {
        Arc::clone(&self.root)
    }

    /// Get plan properties.
    pub fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    /// Get output schema from the root operator.
    pub fn schema(&self) -> &PhysicalSchema {
        self.root.schema()
    }

    /// Generate EXPLAIN output.
    pub fn explain(&self) -> String {
        let mut output = String::from("Physical Plan:\n");
        output.push_str(&self.root.explain(1));
        output
    }

    /// Generate verbose EXPLAIN output with schema.
    pub fn explain_verbose(&self) -> String {
        let mut output = self.explain();
        output.push_str("\nOutput Schema:\n");
        let _ = write!(output, "{}", self.schema());
        let _ = writeln!(output, "Execution Mode: {}", self.properties.execution_mode);
        if self.properties.contains_blocking {
            output.push_str("Contains blocking operators: yes\n");
        }
        output
    }

    /// Count the number of operators in the plan.
    pub fn operator_count(&self) -> usize {
        fn count(op: &dyn PhysicalOperator) -> usize {
            1 + op
                .children()
                .iter()
                .map(|c| count(c.as_ref()))
                .sum::<usize>()
        }
        count(self.root.as_ref())
    }

    /// Get the maximum depth of the plan tree.
    pub fn depth(&self) -> usize {
        fn max_depth(op: &dyn PhysicalOperator) -> usize {
            let child_depths: Vec<_> = op
                .children()
                .iter()
                .map(|c| max_depth(c.as_ref()))
                .collect();
            1 + child_depths.into_iter().max().unwrap_or(0)
        }
        max_depth(self.root.as_ref())
    }

    /// Check if the plan contains any blocking operators.
    pub fn has_blocking_operators(&self) -> bool {
        fn check_blocking(op: &dyn PhysicalOperator) -> bool {
            if op.capabilities().blocking {
                return true;
            }
            op.children().iter().any(|c| check_blocking(c.as_ref()))
        }
        check_blocking(self.root.as_ref())
    }
}

impl std::fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.explain())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::EmptyExec;

    #[test]
    fn test_physical_plan_creation() {
        let empty = Arc::new(EmptyExec::new());
        let plan = PhysicalPlan::new(empty);

        assert_eq!(plan.operator_count(), 1);
        assert_eq!(plan.depth(), 1);
        assert!(!plan.has_blocking_operators());
    }

    #[test]
    fn test_physical_plan_explain() {
        let empty = Arc::new(EmptyExec::new());
        let plan = PhysicalPlan::new(empty);

        let explain = plan.explain();
        assert!(explain.contains("Physical Plan"));
        assert!(explain.contains("EmptyExec"));
    }
}
