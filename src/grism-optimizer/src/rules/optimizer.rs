//! The main optimizer that applies rules to logical plans (RFC-0006 compliant).
//!
//! The optimizer applies rules in a fixed-point iteration until no more changes occur
//! or a maximum number of iterations is reached.

use common_error::GrismResult;
use grism_logical::LogicalPlan;
use log::debug;

use super::rule::{OptimizationRule, OptimizedPlan, RuleTrace};

/// Configuration for the optimizer.
#[derive(Debug, Clone)]
pub struct OptimizerConfig {
    /// Maximum number of iterations before stopping.
    pub max_iterations: usize,
    /// Whether to enable detailed tracing.
    pub enable_trace: bool,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            max_iterations: 100,
            enable_trace: false,
        }
    }
}

impl OptimizerConfig {
    /// Create a new config with the given max iterations.
    pub fn with_max_iterations(mut self, max: usize) -> Self {
        self.max_iterations = max;
        self
    }

    /// Enable or disable tracing.
    pub fn with_trace(mut self, enable: bool) -> Self {
        self.enable_trace = enable;
        self
    }
}

/// The main optimizer that applies rules to logical plans.
///
/// # Rewrite Ordering (RFC-0006, Section 7)
///
/// Rules are applied in a semantically safe order:
///
/// 1. Validation
/// 2. Constant folding
/// 3. Predicate pushdown
/// 4. Projection pruning
/// 5. Expand rewrites
/// 6. Limit pushdown
///
/// Repeated application is allowed until a fixpoint is reached.
///
/// # Termination (RFC-0006, Section 8)
///
/// - Rewrite process MUST terminate
/// - Cyclic rewrites are forbidden
pub struct Optimizer {
    /// The rules to apply (in order).
    rules: Vec<Box<dyn OptimizationRule>>,
    /// Configuration.
    config: OptimizerConfig,
}

impl Optimizer {
    /// Create a new optimizer with the given rules.
    pub fn new(rules: Vec<Box<dyn OptimizationRule>>) -> Self {
        Self {
            rules,
            config: OptimizerConfig::default(),
        }
    }

    /// Create a new optimizer with custom config.
    pub fn with_config(rules: Vec<Box<dyn OptimizationRule>>, config: OptimizerConfig) -> Self {
        Self { rules, config }
    }

    /// Add a rule to the optimizer.
    pub fn add_rule<R: OptimizationRule + 'static>(&mut self, rule: R) {
        self.rules.push(Box::new(rule));
    }

    /// Optimize a logical plan.
    ///
    /// Applies rules in fixed-point iteration until no changes occur.
    pub fn optimize(&self, plan: LogicalPlan) -> GrismResult<OptimizedPlan> {
        let mut current_plan = plan;
        let mut iterations = 0;
        let mut total_rules_applied = 0;
        let mut trace = Vec::new();

        loop {
            if iterations >= self.config.max_iterations {
                debug!(
                    "Optimizer reached max iterations ({}), stopping",
                    self.config.max_iterations
                );
                break;
            }

            iterations += 1;
            let mut changed_this_iteration = false;

            for rule in &self.rules {
                let before = if self.config.enable_trace {
                    Some(current_plan.explain())
                } else {
                    None
                };

                let result = rule.apply(current_plan)?;

                if result.changed {
                    changed_this_iteration = true;
                    total_rules_applied += 1;

                    debug!("Rule '{}' applied in iteration {}", rule.name(), iterations);

                    if self.config.enable_trace {
                        trace.push(RuleTrace::new(
                            rule.name(),
                            before.unwrap_or_default(),
                            result.plan.explain(),
                            true,
                        ));
                    }
                }

                current_plan = result.plan;
            }

            if !changed_this_iteration {
                debug!("No changes in iteration {}, reached fixpoint", iterations);
                break;
            }
        }

        Ok(OptimizedPlan {
            plan: current_plan,
            iterations,
            rules_applied: total_rules_applied,
            trace,
        })
    }

    /// Optimize with a single pass (no fixpoint iteration).
    pub fn optimize_once(&self, plan: LogicalPlan) -> GrismResult<OptimizedPlan> {
        let mut current_plan = plan;
        let mut rules_applied = 0;
        let mut trace = Vec::new();

        for rule in &self.rules {
            let before = if self.config.enable_trace {
                Some(current_plan.explain())
            } else {
                None
            };

            let result = rule.apply(current_plan)?;

            if result.changed {
                rules_applied += 1;

                if self.config.enable_trace {
                    trace.push(RuleTrace::new(
                        rule.name(),
                        before.unwrap_or_default(),
                        result.plan.explain(),
                        true,
                    ));
                }
            }

            current_plan = result.plan;
        }

        Ok(OptimizedPlan {
            plan: current_plan,
            iterations: 1,
            rules_applied,
            trace,
        })
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        use super::{ConstantFolding, PredicatePushdown, ProjectionPruning};

        // Default optimizer with standard rules per RFC-0006, Section 7
        Self::new(vec![
            Box::new(ConstantFolding),
            Box::new(PredicatePushdown),
            Box::new(ProjectionPruning),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::rule::Transformed;
    use grism_logical::{FilterOp, LogicalOp, PlanBuilder, ScanOp, col, lit};

    struct AddLimitRule;

    impl OptimizationRule for AddLimitRule {
        fn name(&self) -> &'static str {
            "AddLimit"
        }

        fn apply(&self, plan: LogicalPlan) -> GrismResult<Transformed> {
            // Only add limit if not already present
            if matches!(plan.root(), grism_logical::LogicalOp::Limit { .. }) {
                return Ok(Transformed::no(plan));
            }

            Ok(Transformed::yes(LogicalPlan::new(LogicalOp::limit(
                plan.root,
                grism_logical::LimitOp::new(1000),
            ))))
        }
    }

    #[test]
    fn test_optimizer_basic() {
        let optimizer = Optimizer::new(vec![Box::new(AddLimitRule)]);

        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("age").gt(lit(18i64))))
            .build();

        let result = optimizer.optimize(plan).unwrap();

        assert!(result.rules_applied > 0);
        assert!(
            result
                .plan
                .contains_op(|op| matches!(op, LogicalOp::Limit { .. }))
        );
    }

    #[test]
    fn test_optimizer_fixpoint() {
        // Rule that does nothing - should reach fixpoint immediately
        struct NoChangeRule;

        impl OptimizationRule for NoChangeRule {
            fn name(&self) -> &'static str {
                "NoChange"
            }

            fn apply(&self, plan: LogicalPlan) -> GrismResult<Transformed> {
                Ok(Transformed::no(plan))
            }
        }

        let optimizer = Optimizer::new(vec![Box::new(NoChangeRule)]);

        let plan = PlanBuilder::scan(ScanOp::nodes()).build();

        let result = optimizer.optimize(plan).unwrap();

        assert_eq!(result.iterations, 1);
        assert_eq!(result.rules_applied, 0);
    }

    #[test]
    fn test_optimizer_with_trace() {
        let config = OptimizerConfig::default().with_trace(true);
        let optimizer = Optimizer::with_config(vec![Box::new(AddLimitRule)], config);

        let plan = PlanBuilder::scan(ScanOp::nodes()).build();

        let result = optimizer.optimize(plan).unwrap();

        assert!(!result.trace.is_empty());
        assert!(result.trace[0].changed);
    }
}
