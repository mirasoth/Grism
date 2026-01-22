//! Local execution engine for Grism.
//!
//! Provides vectorized, async execution of logical plans on a single machine.

mod executor;
mod node;
#[cfg(feature = "python")]
pub mod python;
mod result;

pub use executor::{ExecutionConfig, LocalExecutor};
pub use node::ExecNode;
pub use result::{QueryResult, ResultBatch};
