//! Execution context for query execution.
//!
//! This module provides the execution context that operators use to access
//! storage, memory management, and other runtime resources.

use std::sync::Arc;

use grism_storage::{SnapshotId, Storage};
use tokio::sync::watch;

use crate::executor::traits::ExecutionContextTrait;
use crate::memory::{MemoryManager, NoopMemoryManager};
use crate::metrics::MetricsSink;

// ============================================================================
// Runtime Configuration
// ============================================================================

/// Runtime configuration for execution.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Batch size for operator processing.
    pub batch_size: usize,
    /// Memory limit in bytes (0 = unlimited).
    pub memory_limit: usize,
    /// Enable metrics collection.
    pub collect_metrics: bool,
    /// Parallelism level (unused in local execution).
    pub parallelism: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            memory_limit: 0,
            collect_metrics: true,
            parallelism: 1,
        }
    }
}

impl RuntimeConfig {
    /// Create config with custom batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Create config with memory limit.
    pub fn with_memory_limit(mut self, limit: usize) -> Self {
        self.memory_limit = limit;
        self
    }

    /// Enable or disable metrics collection.
    pub fn with_metrics(mut self, enabled: bool) -> Self {
        self.collect_metrics = enabled;
        self
    }

    /// Set parallelism level.
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }
}

// ============================================================================
// Local Execution Context
// ============================================================================

/// Local execution context passed to all operators.
///
/// Implements [`ExecutionContextTrait`] for local single-machine execution.
/// The context is **read-only** to operators and shared across the pipeline.
///
/// # Contract (RFC-0008, Section 5.1)
///
/// - Context is shared across all operators in a pipeline
/// - Operators must not modify context state
/// - Context provides cooperative cancellation
#[derive(Clone)]
pub struct ExecutionContext {
    /// Snapshot for consistent reads.
    pub snapshot: SnapshotId,
    /// Storage backend handle.
    pub storage: Arc<dyn Storage>,
    /// Memory manager for accounting.
    pub memory: Arc<dyn MemoryManager>,
    /// Cancellation receiver.
    cancel_rx: watch::Receiver<bool>,
    /// Metrics sink for operator statistics (public for access).
    pub metrics: Option<MetricsSink>,
    /// Runtime configuration.
    pub config: RuntimeConfig,
}

impl std::fmt::Debug for ExecutionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionContext")
            .field("snapshot", &self.snapshot)
            .field("config", &self.config)
            .field("metrics_enabled", &self.metrics.is_some())
            .finish_non_exhaustive()
    }
}

impl ExecutionContext {
    /// Create a new execution context.
    pub fn new(storage: Arc<dyn Storage>, snapshot: SnapshotId) -> Self {
        let (_, cancel_rx) = watch::channel(false);
        Self {
            snapshot,
            storage,
            memory: Arc::new(NoopMemoryManager::new()),
            cancel_rx,
            metrics: Some(MetricsSink::new()),
            config: RuntimeConfig::default(),
        }
    }

    /// Create with custom configuration.
    pub fn with_config(mut self, config: RuntimeConfig) -> Self {
        // If metrics are disabled in config, set metrics to None
        if !config.collect_metrics {
            self.metrics = None;
        }
        self.config = config;
        self
    }

    /// Create with memory manager.
    pub fn with_memory(mut self, memory: Arc<dyn MemoryManager>) -> Self {
        self.memory = memory;
        self
    }

    /// Create with metrics sink.
    pub fn with_metrics(mut self, metrics: MetricsSink) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Disable metrics collection.
    pub fn without_metrics(mut self) -> Self {
        self.metrics = None;
        self
    }

    /// Create with cancellation receiver.
    pub fn with_cancellation(mut self, cancel_rx: watch::Receiver<bool>) -> Self {
        self.cancel_rx = cancel_rx;
        self
    }

    /// Get the runtime configuration.
    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    /// Get the metrics sink (if enabled).
    pub fn metrics(&self) -> Option<&MetricsSink> {
        self.metrics.as_ref()
    }

    /// Record operator metrics.
    pub fn record_metrics(&self, operator_id: &str, metrics: crate::metrics::OperatorMetrics) {
        if let Some(ref sink) = self.metrics {
            sink.record(operator_id, metrics);
        }
    }

    /// Update operator metrics.
    pub fn update_metrics<F>(&self, operator_id: &str, f: F)
    where
        F: FnOnce(&mut crate::metrics::OperatorMetrics),
    {
        if let Some(ref sink) = self.metrics {
            sink.update(operator_id, f);
        }
    }
}

// Implement the runtime-agnostic trait
impl ExecutionContextTrait for ExecutionContext {
    fn storage(&self) -> Arc<dyn Storage> {
        Arc::clone(&self.storage)
    }

    fn snapshot_id(&self) -> SnapshotId {
        self.snapshot
    }

    fn memory_manager(&self) -> Arc<dyn MemoryManager> {
        Arc::clone(&self.memory)
    }

    fn metrics_sink(&self) -> Option<&MetricsSink> {
        self.metrics.as_ref()
    }

    fn is_cancelled(&self) -> bool {
        *self.cancel_rx.borrow()
    }

    fn batch_size(&self) -> usize {
        self.config.batch_size
    }
}

// ============================================================================
// Cancellation Handle
// ============================================================================

/// Handle for cancelling query execution.
///
/// This handle is separate from the execution context and can be used
/// to signal cancellation from outside the query execution pipeline.
#[derive(Debug, Clone)]
pub struct CancellationHandle {
    cancel_tx: watch::Sender<bool>,
}

impl CancellationHandle {
    /// Create a new cancellation handle and receiver.
    pub fn new() -> (Self, watch::Receiver<bool>) {
        let (tx, rx) = watch::channel(false);
        (Self { cancel_tx: tx }, rx)
    }

    /// Cancel the query.
    pub fn cancel(&self) {
        let _ = self.cancel_tx.send(true);
    }

    /// Check if cancelled.
    pub fn is_cancelled(&self) -> bool {
        *self.cancel_tx.borrow()
    }
}

impl Default for CancellationHandle {
    fn default() -> Self {
        Self::new().0
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use grism_storage::InMemoryStorage;

    #[test]
    fn test_runtime_config() {
        let config = RuntimeConfig::default()
            .with_batch_size(4096)
            .with_memory_limit(1024 * 1024);

        assert_eq!(config.batch_size, 4096);
        assert_eq!(config.memory_limit, 1024 * 1024);
    }

    #[test]
    fn test_execution_context() {
        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        assert!(!ctx.is_cancelled());
        assert_eq!(ctx.batch_size(), 8192);
    }

    #[test]
    fn test_context_trait() {
        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        // Test through the trait
        let trait_ctx: &dyn ExecutionContextTrait = &ctx;
        assert!(!trait_ctx.is_cancelled());
        assert_eq!(trait_ctx.batch_size(), 8192);
        assert!(trait_ctx.metrics_sink().is_some());
    }

    #[test]
    fn test_context_without_metrics() {
        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default()).without_metrics();

        assert!(ctx.metrics_sink().is_none());
    }

    #[test]
    fn test_cancellation() {
        let (handle, rx) = CancellationHandle::new();
        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default()).with_cancellation(rx);

        assert!(!ctx.is_cancelled());
        handle.cancel();
        assert!(ctx.is_cancelled());
    }
}
