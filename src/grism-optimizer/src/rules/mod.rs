//! Optimization rules for query plans.

mod optimizer;
mod predicate_pushdown;
mod projection_pushdown;

pub use optimizer::{OptimizationRule, Optimizer};
pub use predicate_pushdown::PredicatePushdown;
pub use projection_pushdown::ProjectionPushdown;
