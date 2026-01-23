//! Storage traits and implementations.
//!
//! This module provides storage backends for Grism:
//! - `MemoryStorage`: Arrow-columnar in-memory storage (RFC-0020)
//! - `LanceStorage`: Lance-based persistent storage (RFC-0019)
//!
//! The `Storage` trait follows RFC-0012 specifications.

#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::return_self_not_must_use)]

use std::fmt::Debug;

use async_trait::async_trait;
use grism_logical::LogicalExpr;

use common_error::GrismResult;

use crate::snapshot::SnapshotId;
use crate::stream::RecordBatchStream;
use crate::types::{DatasetId, FragmentMeta, Projection, SnapshotSpec, StorageCaps};

// ============================================================================
// Storage Trait (RFC-0012)
// ============================================================================

/// Storage trait per RFC-0012.
///
/// All storage backends MUST implement this interface. The trait is designed
/// to be execution-agnostic and runtime-neutral.
///
/// # Contract
///
/// - `scan()` returns a pull-based Arrow `RecordBatch` stream
/// - Fragment boundaries are stable for a given `SnapshotId`
/// - Storage is accessed only through `ExecutionContextTrait::storage()`
/// - Storage does not inspect physical plans or operator state
///
/// # Example
///
/// ```rust,ignore
/// let storage: Arc<dyn Storage> = /* ... */;
/// let snapshot = storage.resolve_snapshot(SnapshotSpec::Latest)?;
/// let stream = storage.scan(
///     DatasetId::nodes("Person"),
///     &Projection::all(),
///     None,
///     snapshot,
/// ).await?;
/// ```
#[async_trait]
pub trait Storage: Send + Sync + Debug {
    /// Resolve a snapshot specification to a concrete snapshot ID.
    ///
    /// Per RFC-0012 §6.2, storage MUST NOT implicitly create snapshots.
    fn resolve_snapshot(&self, spec: SnapshotSpec) -> GrismResult<SnapshotId>;

    /// Scan a dataset and return a `RecordBatch` stream.
    ///
    /// Per RFC-0012 §5.1, scans:
    /// - Return pull-based Arrow `RecordBatch` streams
    /// - Respect snapshot isolation
    /// - May apply predicate/projection pushdown based on capabilities
    ///
    /// # Arguments
    ///
    /// * `dataset` - The dataset to scan
    /// * `projection` - Columns to include (empty = all)
    /// * `predicate` - Optional filter predicate for pushdown
    /// * `snapshot` - Snapshot to scan against
    async fn scan(
        &self,
        dataset: DatasetId,
        projection: &Projection,
        predicate: Option<&LogicalExpr>,
        snapshot: SnapshotId,
    ) -> GrismResult<RecordBatchStream>;

    /// Get fragment metadata for a dataset.
    ///
    /// Per RFC-0012 §5.2, fragments:
    /// - Are stable, addressable units of persisted data
    /// - Are immutable within a snapshot
    /// - Are suitable for parallel scanning
    fn fragments(&self, dataset: DatasetId, snapshot: SnapshotId) -> Vec<FragmentMeta>;

    /// Get storage capabilities.
    ///
    /// Per RFC-0012 §5.3, capabilities advertise optional features:
    /// - Predicate pushdown
    /// - Projection pushdown
    /// - Fragment-level pruning
    /// - Object store compatibility
    fn capabilities(&self) -> StorageCaps;

    /// Get the current/latest snapshot ID.
    ///
    /// Returns the most recent snapshot available.
    fn current_snapshot(&self) -> GrismResult<SnapshotId>;

    /// Close the storage, releasing any resources.
    ///
    /// For persistent backends, this may flush pending data.
    async fn close(&self) -> GrismResult<()> {
        Ok(())
    }
}

// ============================================================================
// Writable Storage Extension
// ============================================================================

/// Extension trait for storage backends that support writes.
///
/// This is separate from the core `Storage` trait because not all
/// storage views support mutation (e.g., read-only snapshots).
#[async_trait]
pub trait WritableStorage: Storage {
    /// Write a batch to a dataset.
    ///
    /// Returns the number of rows written.
    async fn write(
        &self,
        dataset: DatasetId,
        batch: arrow::record_batch::RecordBatch,
    ) -> GrismResult<usize>;

    /// Create a new snapshot from current state.
    ///
    /// Returns the new snapshot ID.
    async fn create_snapshot(&self) -> GrismResult<SnapshotId>;

    /// Flush any pending writes to persistent storage.
    async fn flush(&self) -> GrismResult<()>;
}

// ============================================================================
// Storage Statistics
// ============================================================================

/// Storage statistics.
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    /// Total number of nodes across all labels.
    pub node_count: usize,
    /// Total number of hyperedges across all labels.
    pub hyperedge_count: usize,
    /// Number of node labels.
    pub node_label_count: usize,
    /// Number of hyperedge labels.
    pub hyperedge_label_count: usize,
    /// Total storage size in bytes (if applicable).
    pub storage_bytes: Option<usize>,
    /// Number of snapshots.
    pub snapshot_count: usize,
}

/// Extension trait for storage statistics.
pub trait StorageStatsExt: Storage {
    /// Get storage statistics.
    fn stats(&self) -> StorageStats {
        StorageStats::default()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_caps() {
        let memory = StorageCaps::memory();
        assert!(!memory.predicate_pushdown);
        assert!(memory.projection_pushdown);

        let lance = StorageCaps::lance();
        assert!(lance.predicate_pushdown);
        assert!(lance.projection_pushdown);
    }
}
