//! Physical planning module.
//!
//! Converts logical plans to physical plans for execution.

mod local;
mod schema_inference;

pub use local::{LocalPhysicalPlanner, PhysicalPlanner, PlannerConfig};
pub use schema_inference::{build_aggregate_schema, build_project_schema, infer_expr_type};
