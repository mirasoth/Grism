//! Optimization rule trait and framework (RFC-0006 compliant).
//!
//! This module defines the core abstraction for optimization rules and the
//! framework for applying them to logical plans.

use common_error::GrismResult;
use grism_logical::LogicalPlan;

/// A single optimization rule that can transform a logical plan.
///
/// # Safety Conditions (RFC-0006, Section 5.2)
///
/// A rewrite is **legal** if and only if all of the following hold:
///
/// 1. **Hyperedge Preservation**: The multiset of logical hyperedges produced is identical.
/// 2. **Column Semantics Preservation**: Column values are identical for all rows.
/// 3. **NULL Semantics Preservation**: Three-valued logic behavior is unchanged.
/// 4. **Determinism Preservation**: No volatile expressions are reordered across boundaries.
/// 5. **Scope Preservation**: No column or role shadowing is introduced.
pub trait OptimizationRule: Send + Sync {
    /// Get the name of this rule.
    fn name(&self) -> &'static str;

    /// Get a description of what this rule does.
    fn description(&self) -> &'static str {
        "No description available"
    }

    /// Apply this rule to the plan, returning a potentially transformed plan.
    ///
    /// Returns `Ok(plan)` where `plan` may be unchanged if the rule doesn't apply.
    fn apply(&self, plan: LogicalPlan) -> GrismResult<Transformed>;
}

/// The result of applying an optimization rule.
#[derive(Debug, Clone)]
pub struct Transformed {
    /// The (potentially transformed) plan.
    pub plan: LogicalPlan,
    /// Whether the plan was actually changed.
    pub changed: bool,
}

impl Transformed {
    /// Create a new transformed result indicating the plan was changed.
    pub fn yes(plan: LogicalPlan) -> Self {
        Self {
            plan,
            changed: true,
        }
    }

    /// Create a new transformed result indicating the plan was unchanged.
    pub fn no(plan: LogicalPlan) -> Self {
        Self {
            plan,
            changed: false,
        }
    }
}

impl From<LogicalPlan> for Transformed {
    fn from(plan: LogicalPlan) -> Self {
        Self::no(plan)
    }
}

/// A trace entry for a single rule application.
#[derive(Debug, Clone)]
pub struct RuleTrace {
    /// The name of the rule that was applied.
    pub rule_name: String,
    /// The plan before the rule was applied (as explain string).
    pub before: String,
    /// The plan after the rule was applied (as explain string).
    pub after: String,
    /// Whether the rule actually changed the plan.
    pub changed: bool,
}

impl RuleTrace {
    /// Create a new trace entry.
    pub fn new(
        rule_name: impl Into<String>,
        before: impl Into<String>,
        after: impl Into<String>,
        changed: bool,
    ) -> Self {
        Self {
            rule_name: rule_name.into(),
            before: before.into(),
            after: after.into(),
            changed,
        }
    }
}

/// The result of optimization with optional trace information.
#[derive(Debug, Clone)]
pub struct OptimizedPlan {
    /// The final optimized plan.
    pub plan: LogicalPlan,
    /// Number of optimization iterations performed.
    pub iterations: usize,
    /// Number of rules that were applied (changed the plan).
    pub rules_applied: usize,
    /// Detailed trace of rule applications (if tracing was enabled).
    pub trace: Vec<RuleTrace>,
}

impl OptimizedPlan {
    /// Create a new optimized plan result.
    pub fn new(plan: LogicalPlan) -> Self {
        Self {
            plan,
            iterations: 0,
            rules_applied: 0,
            trace: Vec::new(),
        }
    }

    /// Format the trace as a human-readable string.
    pub fn format_trace(&self) -> String {
        let mut output = String::new();
        output.push_str(&format!(
            "Optimization completed in {} iterations, {} rules applied\n",
            self.iterations, self.rules_applied
        ));

        if self.trace.is_empty() {
            output.push_str("  (no trace available)\n");
        } else {
            for (i, entry) in self.trace.iter().filter(|t| t.changed).enumerate() {
                output.push_str(&format!(
                    "\n--- Rule {} applied: {} ---\n",
                    i + 1,
                    entry.rule_name
                ));
                output.push_str("Before:\n");
                output.push_str(&entry.before);
                output.push_str("\nAfter:\n");
                output.push_str(&entry.after);
            }
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_logical::{LogicalOp, ScanOp};

    struct NoOpRule;

    impl OptimizationRule for NoOpRule {
        fn name(&self) -> &'static str {
            "NoOp"
        }

        fn apply(&self, plan: LogicalPlan) -> GrismResult<Transformed> {
            Ok(Transformed::no(plan))
        }
    }

    #[test]
    fn test_transformed() {
        let plan = LogicalPlan::new(LogicalOp::scan(ScanOp::nodes()));

        let unchanged = Transformed::no(plan.clone());
        assert!(!unchanged.changed);

        let changed = Transformed::yes(plan);
        assert!(changed.changed);
    }

    #[test]
    fn test_rule_trace() {
        let trace = RuleTrace::new("TestRule", "before", "after", true);
        assert_eq!(trace.rule_name, "TestRule");
        assert!(trace.changed);
    }
}
