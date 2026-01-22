//! Query execution module.
//!
//! This module provides execution infrastructure for Grism:
//!
//! - [`ExecutionContextTrait`]: Runtime-agnostic context trait
//! - [`ExecutionContext`]: Local execution context implementation
//! - [`LocalExecutor`]: Single-machine executor
//! - [`ExecutionResult`]: Query execution results

mod context;
mod local;
mod result;
mod traits;

pub use context::{CancellationHandle, ExecutionContext, RuntimeConfig};
pub use local::LocalExecutor;
pub use result::ExecutionResult;
pub use traits::{ExecutionContextExt, ExecutionContextTrait};
