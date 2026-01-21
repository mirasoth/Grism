//! Async runtime utilities for Grism.
//!
//! Provides runtime management and async utilities.

use std::future::Future;

use common_error::{GrismError, GrismResult};
use tokio::runtime::Runtime;

/// Get or create a Tokio runtime for blocking operations.
pub fn get_runtime() -> GrismResult<Runtime> {
    Runtime::new().map_err(|e| GrismError::InternalError(format!("Failed to create runtime: {e}")))
}

/// Block on a future using the default runtime.
pub fn block_on<F: Future>(future: F) -> GrismResult<F::Output> {
    let runtime = get_runtime()?;
    Ok(runtime.block_on(future))
}

/// Spawn a task on the current runtime.
pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(future)
}

/// A handle to a set of spawned tasks.
pub struct JoinSet<T> {
    inner: tokio::task::JoinSet<T>,
}

impl<T: Send + 'static> JoinSet<T> {
    /// Create a new join set.
    pub fn new() -> Self {
        Self {
            inner: tokio::task::JoinSet::new(),
        }
    }

    /// Spawn a task into the set.
    pub fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = T> + Send + 'static,
    {
        self.inner.spawn(future);
    }

    /// Wait for the next task to complete.
    pub async fn join_next(&mut self) -> Option<Result<T, tokio::task::JoinError>> {
        self.inner.join_next().await
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Get the number of tasks in the set.
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<T: Send + 'static> Default for JoinSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
