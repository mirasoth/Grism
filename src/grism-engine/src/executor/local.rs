//! Local single-node executor implementation.

use std::sync::Arc;
use std::time::Instant;

use common_error::{GrismError, GrismResult};
use grism_storage::{SnapshotId, Storage};

use crate::executor::{CancellationHandle, ExecutionContext, ExecutionResult, RuntimeConfig};
use crate::memory::{MemoryManager, TrackingMemoryManager};
use crate::metrics::MetricsSink;
use crate::physical::PhysicalPlan;

/// Local single-node executor.
///
/// Executes physical plans using a pull-based pipeline model.
/// This is the **reference execution backend** for Grism.
#[derive(Debug)]
pub struct LocalExecutor {
    /// Execution configuration.
    config: RuntimeConfig,
}

impl LocalExecutor {
    /// Create a new local executor with default configuration.
    pub fn new() -> Self {
        Self {
            config: RuntimeConfig::default(),
        }
    }

    /// Create with custom configuration.
    pub fn with_config(config: RuntimeConfig) -> Self {
        Self { config }
    }

    /// Create with custom batch size.
    pub fn with_batch_size(batch_size: usize) -> Self {
        Self {
            config: RuntimeConfig::default().with_batch_size(batch_size),
        }
    }

    /// Get the executor configuration.
    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    /// Execute a physical plan.
    pub async fn execute(
        &self,
        plan: PhysicalPlan,
        storage: Arc<dyn Storage>,
        snapshot: SnapshotId,
    ) -> GrismResult<ExecutionResult> {
        self.execute_with_cancellation(plan, storage, snapshot, None)
            .await
    }

    /// Execute a physical plan with cancellation support.
    pub async fn execute_with_cancellation(
        &self,
        plan: PhysicalPlan,
        storage: Arc<dyn Storage>,
        snapshot: SnapshotId,
        cancel_handle: Option<CancellationHandle>,
    ) -> GrismResult<ExecutionResult> {
        // Create memory manager
        let memory: Arc<dyn MemoryManager> = if self.config.memory_limit > 0 {
            Arc::new(TrackingMemoryManager::new(self.config.memory_limit))
        } else {
            Arc::new(TrackingMemoryManager::unlimited())
        };

        // Create metrics sink
        let metrics = MetricsSink::new();

        // Create execution context
        let mut ctx = ExecutionContext::new(storage, snapshot)
            .with_config(self.config.clone())
            .with_memory(memory)
            .with_metrics(metrics.clone());

        // Set up cancellation if provided
        if let Some(handle) = cancel_handle {
            let (new_handle, new_rx) = CancellationHandle::new();
            if handle.is_cancelled() {
                new_handle.cancel();
            }
            ctx = ctx.with_cancellation(new_rx);
        }

        // Get root operator
        let root = plan.root();
        let schema = plan.schema().clone();

        // Open the pipeline
        root.open(&ctx).await?;

        // Collect results
        let mut batches = Vec::new();
        let start = Instant::now();

        loop {
            // Check cancellation
            if ctx.is_cancelled() {
                // Close and return partial results or error
                root.close().await?;
                return Err(GrismError::cancelled("Query execution cancelled"));
            }

            match root.next().await? {
                Some(batch) => {
                    if batch.num_rows() > 0 {
                        batches.push(batch);
                    }
                }
                None => break,
            }
        }

        // Close the pipeline
        root.close().await?;

        let elapsed = start.elapsed();

        Ok(ExecutionResult::new(batches, schema, metrics, elapsed))
    }

    /// Execute synchronously (blocking).
    pub fn execute_sync(
        &self,
        plan: PhysicalPlan,
        storage: Arc<dyn Storage>,
        snapshot: SnapshotId,
    ) -> GrismResult<ExecutionResult> {
        common_runtime::block_on(self.execute(plan, storage, snapshot))?
    }
}

impl Default for LocalExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::EmptyExec;
    use grism_storage::InMemoryStorage;

    #[tokio::test]
    async fn test_execute_empty() {
        let executor = LocalExecutor::new();
        let storage = Arc::new(InMemoryStorage::new());
        let snapshot = SnapshotId::default();

        let plan = PhysicalPlan::new(Arc::new(EmptyExec::new()));
        let result = executor.execute(plan, storage, snapshot).await.unwrap();

        assert!(result.is_empty());
        assert_eq!(result.total_rows(), 0);
    }

    #[test]
    fn test_executor_config() {
        let executor = LocalExecutor::with_batch_size(1024);
        assert_eq!(executor.config().batch_size, 1024);
    }

    #[tokio::test]
    async fn test_execute_with_memory_limit() {
        let config = RuntimeConfig::default().with_memory_limit(1024 * 1024);
        let executor = LocalExecutor::with_config(config);

        let storage = Arc::new(InMemoryStorage::new());
        let snapshot = SnapshotId::default();

        let plan = PhysicalPlan::new(Arc::new(EmptyExec::new()));
        let result = executor.execute(plan, storage, snapshot).await.unwrap();

        assert!(result.is_empty());
    }
}
