//! Query execution module.

mod context;
mod local;
mod result;

pub use context::{CancellationHandle, ExecutionContext, RuntimeConfig};
pub use local::LocalExecutor;
pub use result::ExecutionResult;
