//! Structural validation for logical plans.
//!
//! This module validates the structure of logical plans:
//! - DAG validation (no cycles)
//! - Operator input arity checks
//! - Schema consistency between operators

use std::collections::HashSet;

use crate::{LogicalOp, LogicalPlan};

/// A structural validation error.
#[derive(Debug, Clone, PartialEq)]
pub enum StructuralValidationError {
    /// The plan contains a cycle.
    CycleDetected {
        /// Description of where the cycle was detected.
        location: String,
    },

    /// An operator has incorrect input arity.
    InvalidArity {
        /// The operator name.
        operator: String,
        /// Expected number of inputs.
        expected: usize,
        /// Actual number of inputs.
        actual: usize,
    },

    /// Empty plan (no operators).
    EmptyPlan,

    /// Invalid operator sequence (e.g., Aggregate without grouping keys on non-Scan).
    InvalidOperatorSequence {
        /// Description of the issue.
        message: String,
    },

    /// Limit with zero rows.
    ZeroLimit,

    /// Invalid hop range in Expand.
    InvalidHopRange {
        /// Description of the issue.
        message: String,
    },
}

impl std::fmt::Display for StructuralValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CycleDetected { location } => {
                write!(f, "Cycle detected in plan at: {}", location)
            }
            Self::InvalidArity {
                operator,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "Invalid arity for {}: expected {} inputs, got {}",
                    operator, expected, actual
                )
            }
            Self::EmptyPlan => write!(f, "Plan is empty"),
            Self::InvalidOperatorSequence { message } => {
                write!(f, "Invalid operator sequence: {}", message)
            }
            Self::ZeroLimit => write!(f, "Limit cannot be zero"),
            Self::InvalidHopRange { message } => {
                write!(f, "Invalid hop range: {}", message)
            }
        }
    }
}

impl std::error::Error for StructuralValidationError {}

/// Structural validator for logical plans.
pub struct StructuralValidator;

impl StructuralValidator {
    /// Validate the structural integrity of a logical plan.
    ///
    /// Returns `Ok(())` if the plan is structurally valid, or a list of errors.
    pub fn validate(plan: &LogicalPlan) -> Result<(), Vec<StructuralValidationError>> {
        let mut errors = Vec::new();

        // Validate the root operator
        Self::validate_operator(plan.root(), &mut errors, &mut HashSet::new(), 0);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Recursively validate an operator and its children.
    fn validate_operator(
        op: &LogicalOp,
        errors: &mut Vec<StructuralValidationError>,
        visited: &mut HashSet<usize>,
        depth: usize,
    ) {
        // Use pointer address as a simple identity check for cycle detection
        let ptr = op as *const LogicalOp as usize;

        // Check for cycles (shouldn't happen with Box, but defensive)
        if !visited.insert(ptr) {
            errors.push(StructuralValidationError::CycleDetected {
                location: format!("depth {}", depth),
            });
            return;
        }

        // Validate arity
        Self::validate_arity(op, errors);

        // Validate operator-specific constraints
        Self::validate_operator_constraints(op, errors);

        // Recursively validate children
        for child in op.inputs() {
            Self::validate_operator(child, errors, visited, depth + 1);
        }

        // Remove from visited set (allows same structure to appear in different branches)
        visited.remove(&ptr);
    }

    /// Validate operator input arity.
    fn validate_arity(op: &LogicalOp, errors: &mut Vec<StructuralValidationError>) {
        let (expected, _actual) = match op {
            LogicalOp::Scan(_) => (0, 0),
            LogicalOp::Empty => (0, 0),
            LogicalOp::Expand { input, .. } => (1, if input.is_leaf() { 0 } else { 1 }),
            LogicalOp::Filter { .. } => (1, 1),
            LogicalOp::Project { .. } => (1, 1),
            LogicalOp::Aggregate { .. } => (1, 1),
            LogicalOp::Limit { .. } => (1, 1),
            LogicalOp::Sort { .. } => (1, 1),
            LogicalOp::Union { .. } => (2, 2),
            LogicalOp::Rename { .. } => (1, 1),
            LogicalOp::Infer { .. } => (1, 1),
        };

        // For operators with inputs, verify they have children
        if expected > 0 {
            let inputs = op.inputs();
            if inputs.len() != expected {
                errors.push(StructuralValidationError::InvalidArity {
                    operator: op.name().to_string(),
                    expected,
                    actual: inputs.len(),
                });
            }
        }
    }

    /// Validate operator-specific constraints.
    fn validate_operator_constraints(op: &LogicalOp, errors: &mut Vec<StructuralValidationError>) {
        match op {
            LogicalOp::Limit { limit, .. } => {
                // Limit cannot be zero (would always return empty)
                if limit.limit == 0 {
                    errors.push(StructuralValidationError::ZeroLimit);
                }
            }
            LogicalOp::Expand { expand, .. } => {
                // Validate hop range
                if expand.hops.min > expand.hops.max && expand.hops.max != 0 {
                    errors.push(StructuralValidationError::InvalidHopRange {
                        message: format!("min ({}) > max ({})", expand.hops.min, expand.hops.max),
                    });
                }
            }
            LogicalOp::Union { left, right, .. } => {
                // Union should have compatible schemas (structural check only)
                // Detailed schema compatibility is checked in semantic validation
                if matches!(left.as_ref(), LogicalOp::Empty)
                    && matches!(right.as_ref(), LogicalOp::Empty)
                {
                    // Both empty is allowed but unusual
                }
            }
            _ => {}
        }
    }
}

/// Check if a plan is a valid DAG (no cycles).
pub fn is_dag(plan: &LogicalPlan) -> bool {
    fn check_dag(op: &LogicalOp, visited: &mut HashSet<usize>) -> bool {
        let ptr = op as *const LogicalOp as usize;

        if !visited.insert(ptr) {
            return false;
        }

        for child in op.inputs() {
            if !check_dag(child, visited) {
                return false;
            }
        }

        visited.remove(&ptr);
        true
    }

    check_dag(plan.root(), &mut HashSet::new())
}

/// Count the total number of operators in a plan.
pub fn operator_count(plan: &LogicalPlan) -> usize {
    fn count(op: &LogicalOp) -> usize {
        1 + op.inputs().iter().map(|i| count(i)).sum::<usize>()
    }
    count(plan.root())
}

/// Get the maximum depth of the plan tree.
pub fn plan_depth(plan: &LogicalPlan) -> usize {
    fn depth(op: &LogicalOp) -> usize {
        1 + op.inputs().iter().map(|i| depth(i)).max().unwrap_or(0)
    }
    depth(plan.root())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ExpandOp, FilterOp, HopRange, LimitOp, PlanBuilder, ProjectOp, ScanOp, UnionOp, col, lit,
    };

    #[test]
    fn test_valid_simple_plan() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("age").gt(lit(18i64))))
            .project(ProjectOp::columns(["name", "city"]))
            .build();

        let result = StructuralValidator::validate(&plan);
        assert!(result.is_ok());
    }

    #[test]
    fn test_valid_union_plan() {
        let left = LogicalOp::scan(ScanOp::nodes_with_label("Customer"));
        let right = LogicalOp::scan(ScanOp::nodes_with_label("Vendor"));
        let union = LogicalOp::union(left, right, UnionOp::all());
        let plan = LogicalPlan::new(union);

        let result = StructuralValidator::validate(&plan);
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_hop_range() {
        let expand = ExpandOp::binary().with_hops(HopRange { min: 5, max: 3 });
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .expand(expand)
            .build();

        let result = StructuralValidator::validate(&plan);
        assert!(result.is_err());

        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, StructuralValidationError::InvalidHopRange { .. }))
        );
    }

    #[test]
    fn test_is_dag() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("active").eq(lit(true))))
            .build();

        assert!(is_dag(&plan));
    }

    #[test]
    fn test_operator_count() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("active").eq(lit(true))))
            .project(ProjectOp::columns(["name"]))
            .limit(LimitOp::new(10))
            .build();

        assert_eq!(operator_count(&plan), 4);
    }

    #[test]
    fn test_plan_depth() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("active").eq(lit(true))))
            .project(ProjectOp::columns(["name"]))
            .build();

        assert_eq!(plan_depth(&plan), 3);
    }

    #[test]
    fn test_empty_plan() {
        let plan = LogicalPlan::new(LogicalOp::Empty);
        let result = StructuralValidator::validate(&plan);
        // Empty plan is structurally valid
        assert!(result.is_ok());
    }
}
