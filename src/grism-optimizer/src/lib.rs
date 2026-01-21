//! Query optimizer for Grism logical plans.
//!
//! Provides rule-based and cost-based optimization for logical plans.

mod rules;

pub use rules::{OptimizationRule, Optimizer};

use common_error::GrismResult;
use grism_logical::LogicalPlan;

/// Optimize a logical plan using the default optimizer.
pub fn optimize(plan: LogicalPlan) -> GrismResult<LogicalPlan> {
    let optimizer = Optimizer::default();
    optimizer.optimize(plan)
}
