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
    pub const fn yes(plan: LogicalPlan) -> Self {
        Self {
            plan,
            changed: true,
        }
    }

    /// Create a new transformed result indicating the plan was unchanged.
    pub const fn no(plan: LogicalPlan) -> Self {
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
///
/// # Display Format (RFC-0006, Section 8.2)
///
/// ```text
/// Rule: PredicatePushdown
/// Before: Filter → Expand → Scan
/// After: Expand → Filter → Scan
/// ```
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
    /// Brief summary of the plan structure before (operator chain).
    pub before_summary: Option<String>,
    /// Brief summary of the plan structure after (operator chain).
    pub after_summary: Option<String>,
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
            before_summary: None,
            after_summary: None,
        }
    }

    /// Create a trace entry with summaries.
    pub fn with_summaries(
        rule_name: impl Into<String>,
        before: impl Into<String>,
        after: impl Into<String>,
        before_summary: impl Into<String>,
        after_summary: impl Into<String>,
        changed: bool,
    ) -> Self {
        Self {
            rule_name: rule_name.into(),
            before: before.into(),
            after: after.into(),
            changed,
            before_summary: Some(before_summary.into()),
            after_summary: Some(after_summary.into()),
        }
    }

    /// Format this trace entry according to RFC-0006, Section 8.2.
    pub fn format_rfc(&self) -> String {
        use std::fmt::Write;
        let mut output = String::new();

        writeln!(output, "Rule: {}", self.rule_name).unwrap();

        // Use summaries if available, otherwise extract from full explain
        let before_chain = self
            .before_summary
            .as_deref()
            .unwrap_or_else(|| Self::extract_operator_chain(&self.before));
        let after_chain = self
            .after_summary
            .as_deref()
            .unwrap_or_else(|| Self::extract_operator_chain(&self.after));

        writeln!(output, "Before: {before_chain}").unwrap();
        writeln!(output, "After: {after_chain}").unwrap();

        output
    }

    /// Extract operator chain from explain output.
    fn extract_operator_chain(explain: &str) -> &str {
        // Simple extraction: take first line or use full string
        explain.lines().next().unwrap_or(explain).trim()
    }
}

impl std::fmt::Display for RuleTrace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format_rfc())
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
    pub const fn new(plan: LogicalPlan) -> Self {
        Self {
            plan,
            iterations: 0,
            rules_applied: 0,
            trace: Vec::new(),
        }
    }

    /// Format the trace as a human-readable string (RFC-0006, Section 8.2).
    ///
    /// # Output Format
    ///
    /// ```text
    /// Optimization completed in 3 iterations, 2 rules applied
    ///
    /// Rule: PredicatePushdown
    /// Before: Filter → Expand → Scan
    /// After: Expand → Filter → Scan
    ///
    /// Rule: ConstantFolding
    /// Before: Filter(1 < 2) → Scan
    /// After: Scan
    /// ```
    pub fn format_trace(&self) -> String {
        use std::fmt::Write;
        let mut output = String::new();
        let iter_suffix = if self.iterations == 1 { "" } else { "s" };
        let rule_suffix = if self.rules_applied == 1 { "" } else { "s" };
        writeln!(
            output,
            "Optimization completed in {} iteration{}, {} rule{} applied",
            self.iterations, iter_suffix, self.rules_applied, rule_suffix
        )
        .unwrap();

        if self.trace.is_empty() {
            output.push_str("  (no trace available)\n");
        } else {
            let changed_entries: Vec<_> = self.trace.iter().filter(|t| t.changed).collect();

            if changed_entries.is_empty() {
                output.push_str("  (no rules applied)\n");
            } else {
                for entry in changed_entries {
                    output.push('\n');
                    output.push_str(&entry.format_rfc());
                }
            }
        }

        output
    }

    /// Format trace in verbose mode (includes full explain output).
    pub fn format_trace_verbose(&self) -> String {
        use std::fmt::Write;
        let mut output = String::new();
        let iter_suffix = if self.iterations == 1 { "" } else { "s" };
        let rule_suffix = if self.rules_applied == 1 { "" } else { "s" };
        writeln!(
            output,
            "Optimization completed in {} iteration{}, {} rule{} applied",
            self.iterations, iter_suffix, self.rules_applied, rule_suffix
        )
        .unwrap();

        if self.trace.is_empty() {
            output.push_str("  (no trace available)\n");
        } else {
            for (i, entry) in self.trace.iter().filter(|t| t.changed).enumerate() {
                writeln!(output, "\n=== Step {} - {} ===", i + 1, entry.rule_name).unwrap();
                output.push_str("\nBefore:\n");
                output.push_str(&entry.before);
                output.push_str("\nAfter:\n");
                output.push_str(&entry.after);
                output.push('\n');
            }
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_logical::{LogicalOp, ScanOp};

    #[allow(dead_code)]
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
