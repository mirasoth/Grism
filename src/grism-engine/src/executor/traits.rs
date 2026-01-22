//! Runtime-agnostic execution context trait.
//!
//! This module defines the `ExecutionContextTrait` that abstracts the execution
//! context for both local and distributed runtimes. Per RFC-0102, operators
//! are runtime-agnostic and interact with the context through this trait.

use std::sync::Arc;

use grism_storage::{SnapshotId, Storage};

use crate::memory::MemoryManager;
use crate::metrics::MetricsSink;

/// Runtime-agnostic execution context trait.
///
/// Both local and Ray runtimes implement this trait with their specific
/// resource management. Operators use this trait to access storage,
/// memory management, and metrics without knowing the execution environment.
///
/// # Contract (RFC-0102, Section 5.7)
///
/// - Context is read-only to operators
/// - Context provides access to shared resources
/// - Context supports cooperative cancellation
pub trait ExecutionContextTrait: Send + Sync {
    /// Access to storage layer.
    fn storage(&self) -> Arc<dyn Storage>;

    /// Current snapshot for consistent reads.
    fn snapshot_id(&self) -> SnapshotId;

    /// Memory management interface.
    fn memory_manager(&self) -> Arc<dyn MemoryManager>;

    /// Optional metrics collection.
    fn metrics_sink(&self) -> Option<&MetricsSink>;

    /// Check if execution has been cancelled.
    fn is_cancelled(&self) -> bool;

    /// Get the configured batch size.
    fn batch_size(&self) -> usize;

    /// Check if metrics collection is enabled.
    fn metrics_enabled(&self) -> bool {
        self.metrics_sink().is_some()
    }
}

/// Extension trait for `ExecutionContextTrait` with convenience methods.
pub trait ExecutionContextExt: ExecutionContextTrait {
    /// Record operator metrics if enabled.
    fn record_metrics(&self, operator_id: &str, metrics: crate::metrics::OperatorMetrics) {
        if let Some(sink) = self.metrics_sink() {
            sink.record(operator_id, metrics);
        }
    }

    /// Update operator metrics if enabled.
    fn update_metrics<F>(&self, operator_id: &str, f: F)
    where
        F: FnOnce(&mut crate::metrics::OperatorMetrics),
    {
        if let Some(sink) = self.metrics_sink() {
            sink.update(operator_id, f);
        }
    }
}

// Blanket implementation for all types implementing ExecutionContextTrait
impl<T: ExecutionContextTrait> ExecutionContextExt for T {}
