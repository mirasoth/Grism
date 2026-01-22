//! Physical plan structures and utilities.
//!
//! This module contains the physical plan representation, schema, and properties
//! for local execution. Physical plans are fully executable, backend-specific
//! representations of logical plans.

mod plan;
mod properties;
mod schema;

pub use plan::PhysicalPlan;
pub use properties::{
    ExecutionMode, OperatorCaps, PartitioningSpec, PartitioningStrategy, PlanProperties,
};
pub use schema::{PhysicalSchema, PhysicalSchemaBuilder};
