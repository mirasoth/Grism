//! Local execution engine for Grism.
//!
//! Provides vectorized, async execution of logical plans on a single machine.

mod executor;
mod node;
mod result;

pub use executor::{ExecutionConfig, LocalExecutor};
pub use node::ExecNode;
pub use result::{QueryResult, ResultBatch};
