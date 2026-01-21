//! Main optimizer and rule interface.

use common_error::GrismResult;
use grism_logical::LogicalPlan;

use super::{PredicatePushdown, ProjectionPushdown};

/// Trait for optimization rules.
pub trait OptimizationRule: Send + Sync {
    /// Get the rule name.
    fn name(&self) -> &'static str;

    /// Apply the rule to a plan.
    fn apply(&self, plan: LogicalPlan) -> GrismResult<LogicalPlan>;
}

/// Query optimizer that applies a sequence of rules.
pub struct Optimizer {
    rules: Vec<Box<dyn OptimizationRule>>,
    max_iterations: usize,
}

impl Optimizer {
    /// Create a new optimizer with the given rules.
    pub fn new(rules: Vec<Box<dyn OptimizationRule>>) -> Self {
        Self {
            rules,
            max_iterations: 10,
        }
    }

    /// Set the maximum number of optimization iterations.
    pub fn with_max_iterations(mut self, max: usize) -> Self {
        self.max_iterations = max;
        self
    }

    /// Optimize a logical plan.
    pub fn optimize(&self, mut plan: LogicalPlan) -> GrismResult<LogicalPlan> {
        for _ in 0..self.max_iterations {
            let original = plan.explain();

            for rule in &self.rules {
                plan = rule.apply(plan)?;
            }

            // Check if plan changed
            if plan.explain() == original {
                break;
            }
        }

        Ok(plan)
    }

    /// Get the rules.
    pub fn rules(&self) -> &[Box<dyn OptimizationRule>] {
        &self.rules
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new(vec![
            Box::new(PredicatePushdown),
            Box::new(ProjectionPushdown),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_logical::{LogicalOp, ScanKind, ScanOp};

    #[test]
    fn test_optimizer_creation() {
        let optimizer = Optimizer::default();
        assert_eq!(optimizer.rules().len(), 2);
    }

    #[test]
    fn test_optimize_simple_plan() {
        let scan = LogicalOp::Scan(ScanOp::nodes(Some("Person")));
        let plan = LogicalPlan::new(scan);

        let optimizer = Optimizer::default();
        let result = optimizer.optimize(plan);

        assert!(result.is_ok());
    }
}
