//! Validation layer for Grism logical plans.
//!
//! This module provides structural and semantic validation for logical plans,
//! ensuring they are well-formed and type-consistent.
//!
//! # Validation Categories (RFC-0006, Section 9)
//!
//! ## Structural Validation
//!
//! - DAG validation (no cycles)
//! - Operator input arity checks
//! - Schema consistency between operators
//!
//! ## Semantic Validation
//!
//! - Column reference validity
//! - Expression type checking
//! - Role binding correctness
//!
//! # Example
//!
//! ```rust
//! use grism_logical::{LogicalPlan, PlanBuilder, ScanOp, FilterOp, col, lit};
//! use grism_logical::validation::{PlanValidator, validate_plan};
//!
//! let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
//!     .filter(FilterOp::new(col("age").gt(lit(18i64))))
//!     .build();
//!
//! // Validate the plan
//! match validate_plan(&plan) {
//!     Ok(()) => println!("Plan is valid"),
//!     Err(errors) => println!("Validation errors: {:?}", errors),
//! }
//! ```

mod semantic;
mod structural;

pub use semantic::{SemanticValidationError, SemanticValidator};
pub use structural::{StructuralValidationError, StructuralValidator};

use crate::LogicalPlan;

/// A validation error that can occur during plan validation.
#[derive(Debug, Clone)]
pub enum ValidationError {
    /// Structural validation error.
    Structural(StructuralValidationError),
    /// Semantic validation error.
    Semantic(SemanticValidationError),
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Structural(e) => write!(f, "Structural error: {e}"),
            Self::Semantic(e) => write!(f, "Semantic error: {e}"),
        }
    }
}

impl std::error::Error for ValidationError {}

impl From<StructuralValidationError> for ValidationError {
    fn from(e: StructuralValidationError) -> Self {
        Self::Structural(e)
    }
}

impl From<SemanticValidationError> for ValidationError {
    fn from(e: SemanticValidationError) -> Self {
        Self::Semantic(e)
    }
}

/// Combined plan validator that runs both structural and semantic validation.
#[derive(Debug, Default)]
pub struct PlanValidator {
    /// Skip semantic validation (for partially constructed plans).
    skip_semantic: bool,
}

impl PlanValidator {
    /// Create a new plan validator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Skip semantic validation.
    #[must_use]
    pub const fn skip_semantic(mut self) -> Self {
        self.skip_semantic = true;
        self
    }

    /// Validate a logical plan.
    ///
    /// Returns `Ok(())` if the plan is valid, or a list of validation errors.
    pub fn validate(&self, plan: &LogicalPlan) -> Result<(), Vec<ValidationError>> {
        let mut errors = Vec::new();

        // Run structural validation
        if let Err(structural_errors) = StructuralValidator::validate(plan) {
            errors.extend(
                structural_errors
                    .into_iter()
                    .map(ValidationError::Structural),
            );
        }

        // Run semantic validation if not skipped
        if !self.skip_semantic {
            if let Err(semantic_errors) = SemanticValidator::validate(plan) {
                errors.extend(semantic_errors.into_iter().map(ValidationError::Semantic));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

/// Convenience function to validate a plan with default settings.
pub fn validate_plan(plan: &LogicalPlan) -> Result<(), Vec<ValidationError>> {
    PlanValidator::new().validate(plan)
}

/// Validate only structural aspects of a plan.
pub fn validate_structural(plan: &LogicalPlan) -> Result<(), Vec<StructuralValidationError>> {
    StructuralValidator::validate(plan)
}

/// Validate only semantic aspects of a plan.
pub fn validate_semantic(plan: &LogicalPlan) -> Result<(), Vec<SemanticValidationError>> {
    SemanticValidator::validate(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FilterOp, PlanBuilder, ScanOp, col, lit};

    #[test]
    fn test_validate_simple_plan() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("age").gt(lit(18i64))))
            .build();

        let result = validate_plan(&plan);
        assert!(result.is_ok());
    }

    #[test]
    fn test_plan_validator_skip_semantic() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person")).build();

        let validator = PlanValidator::new().skip_semantic();
        let result = validator.validate(&plan);
        assert!(result.is_ok());
    }
}
