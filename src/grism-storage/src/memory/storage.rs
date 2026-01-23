//! `MemoryStorage` implementation (RFC-0020).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use grism_logical::LogicalExpr;
use tokio::sync::RwLock;

use common_error::{GrismError, GrismResult};

use crate::snapshot::SnapshotId;
use crate::storage::{Storage, StorageStats, StorageStatsExt, WritableStorage};
use crate::stream::{MemoryBatchStream, ProjectedBatchStream, RecordBatchStream, empty_stream};
use crate::types::{DatasetId, FragmentMeta, Projection, SnapshotSpec, StorageCaps};

use super::stores::{HyperedgeStore, NodeStore};

// ============================================================================
// MemorySnapshot
// ============================================================================

/// A snapshot of in-memory storage state.
#[derive(Debug, Clone)]
struct MemorySnapshot {
    /// Node stores by label.
    nodes: HashMap<String, NodeStore>,
    /// Hyperedge stores by label.
    hyperedges: HashMap<String, HyperedgeStore>,
    /// Snapshot ID (stored for debugging/tracing).
    #[allow(dead_code)]
    id: SnapshotId,
}

impl MemorySnapshot {
    /// Create a new empty snapshot (reserved for future use).
    #[allow(dead_code)]
    fn new(id: SnapshotId) -> Self {
        Self {
            nodes: HashMap::new(),
            hyperedges: HashMap::new(),
            id,
        }
    }

    /// Create a snapshot from current state.
    fn from_state(
        id: SnapshotId,
        nodes: &HashMap<String, NodeStore>,
        hyperedges: &HashMap<String, HyperedgeStore>,
    ) -> Self {
        Self {
            nodes: nodes
                .iter()
                .map(|(k, v)| (k.clone(), v.snapshot()))
                .collect(),
            hyperedges: hyperedges
                .iter()
                .map(|(k, v)| (k.clone(), v.snapshot()))
                .collect(),
            id,
        }
    }
}

// ============================================================================
// MemoryStorage
// ============================================================================

/// In-memory storage backend (RFC-0020).
///
/// Provides a non-persistent, low-latency storage implementation
/// that stores data as Arrow `RecordBatches`.
///
/// # Thread Safety
///
/// `MemoryStorage` is thread-safe and can be shared across async tasks.
/// All operations use interior mutability via `RwLock`.
///
/// # Snapshot Isolation
///
/// Snapshots provide point-in-time views of the data. Each snapshot
/// contains a copy of the data at the time it was created.
///
/// # Example
///
/// ```rust,ignore
/// let storage = MemoryStorage::new();
///
/// // Write data
/// storage.write(DatasetId::nodes("Person"), batch).await?;
///
/// // Create snapshot
/// let snapshot = storage.create_snapshot().await?;
///
/// // Scan data
/// let stream = storage.scan(
///     DatasetId::nodes("Person"),
///     &Projection::all(),
///     None,
///     snapshot,
/// ).await?;
/// ```
#[derive(Debug)]
pub struct MemoryStorage {
    /// Node stores by label (mutable state).
    nodes: RwLock<HashMap<String, NodeStore>>,
    /// Hyperedge stores by label (mutable state).
    hyperedges: RwLock<HashMap<String, HyperedgeStore>>,
    /// Snapshots by ID.
    snapshots: RwLock<HashMap<SnapshotId, MemorySnapshot>>,
    /// Current working snapshot ID.
    current_snapshot: AtomicU64,
    /// Next snapshot ID.
    next_snapshot_id: AtomicU64,
}

impl MemoryStorage {
    /// Create a new empty in-memory storage.
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            hyperedges: RwLock::new(HashMap::new()),
            snapshots: RwLock::new(HashMap::new()),
            current_snapshot: AtomicU64::new(0),
            next_snapshot_id: AtomicU64::new(1),
        }
    }

    /// Get the number of node labels.
    pub async fn node_label_count(&self) -> usize {
        self.nodes.read().await.len()
    }

    /// Get the number of hyperedge labels.
    pub async fn hyperedge_label_count(&self) -> usize {
        self.hyperedges.read().await.len()
    }

    /// Get all node labels.
    pub async fn node_labels(&self) -> Vec<String> {
        self.nodes.read().await.keys().cloned().collect()
    }

    /// Get all hyperedge labels.
    pub async fn hyperedge_labels(&self) -> Vec<String> {
        self.hyperedges.read().await.keys().cloned().collect()
    }

    /// Clear all data (not snapshots).
    pub async fn clear(&self) {
        self.nodes.write().await.clear();
        self.hyperedges.write().await.clear();
    }

    /// Get batches for a node dataset from a snapshot.
    async fn get_node_batches(
        &self,
        label: Option<&str>,
        snapshot: SnapshotId,
    ) -> GrismResult<Vec<RecordBatch>> {
        // Try snapshot first
        let snapshots = self.snapshots.read().await;
        if let Some(snap) = snapshots.get(&snapshot) {
            return Ok(self.collect_node_batches_from_snapshot(snap, label));
        }
        drop(snapshots);

        // Fall back to current state if snapshot 0 (working state)
        if snapshot == 0 {
            let nodes = self.nodes.read().await;
            return Ok(self.collect_node_batches(&nodes, label));
        }

        Err(GrismError::storage(format!(
            "Snapshot {snapshot} not found"
        )))
    }

    /// Get batches for a hyperedge dataset from a snapshot.
    async fn get_hyperedge_batches(
        &self,
        label: Option<&str>,
        snapshot: SnapshotId,
    ) -> GrismResult<Vec<RecordBatch>> {
        // Try snapshot first
        let snapshots = self.snapshots.read().await;
        if let Some(snap) = snapshots.get(&snapshot) {
            return Ok(self.collect_hyperedge_batches_from_snapshot(snap, label));
        }
        drop(snapshots);

        // Fall back to current state if snapshot 0
        if snapshot == 0 {
            let hyperedges = self.hyperedges.read().await;
            return Ok(self.collect_hyperedge_batches(&hyperedges, label));
        }

        Err(GrismError::storage(format!(
            "Snapshot {snapshot} not found"
        )))
    }

    fn collect_node_batches(
        &self,
        nodes: &HashMap<String, NodeStore>,
        label: Option<&str>,
    ) -> Vec<RecordBatch> {
        match label {
            Some(l) => nodes
                .get(l)
                .map_or_else(Vec::new, |store| store.batches().to_vec()),
            None => nodes
                .values()
                .flat_map(|store| store.batches().iter().cloned())
                .collect(),
        }
    }

    fn collect_node_batches_from_snapshot(
        &self,
        snapshot: &MemorySnapshot,
        label: Option<&str>,
    ) -> Vec<RecordBatch> {
        match label {
            Some(l) => snapshot
                .nodes
                .get(l)
                .map_or_else(Vec::new, |store| store.batches().to_vec()),
            None => snapshot
                .nodes
                .values()
                .flat_map(|store| store.batches().iter().cloned())
                .collect(),
        }
    }

    fn collect_hyperedge_batches(
        &self,
        hyperedges: &HashMap<String, HyperedgeStore>,
        label: Option<&str>,
    ) -> Vec<RecordBatch> {
        match label {
            Some(l) => hyperedges
                .get(l)
                .map_or_else(Vec::new, |store| store.batches().to_vec()),
            None => hyperedges
                .values()
                .flat_map(|store| store.batches().iter().cloned())
                .collect(),
        }
    }

    fn collect_hyperedge_batches_from_snapshot(
        &self,
        snapshot: &MemorySnapshot,
        label: Option<&str>,
    ) -> Vec<RecordBatch> {
        match label {
            Some(l) => snapshot
                .hyperedges
                .get(l)
                .map_or_else(Vec::new, |store| store.batches().to_vec()),
            None => snapshot
                .hyperedges
                .values()
                .flat_map(|store| store.batches().iter().cloned())
                .collect(),
        }
    }

    /// Get fragment metadata for nodes.
    fn get_node_fragments(
        &self,
        nodes: &HashMap<String, NodeStore>,
        label: Option<&str>,
    ) -> Vec<FragmentMeta> {
        match label {
            Some(l) => nodes
                .get(l)
                .map_or_else(Vec::new, |store| store.fragments().to_vec()),
            None => nodes
                .values()
                .flat_map(|store| store.fragments().iter().cloned())
                .collect(),
        }
    }

    /// Get fragment metadata for hyperedges.
    fn get_hyperedge_fragments(
        &self,
        hyperedges: &HashMap<String, HyperedgeStore>,
        label: Option<&str>,
    ) -> Vec<FragmentMeta> {
        match label {
            Some(l) => hyperedges
                .get(l)
                .map_or_else(Vec::new, |store| store.fragments().to_vec()),
            None => hyperedges
                .values()
                .flat_map(|store| store.fragments().iter().cloned())
                .collect(),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    fn resolve_snapshot(&self, spec: SnapshotSpec) -> GrismResult<SnapshotId> {
        match spec {
            SnapshotSpec::Latest => {
                let current = self.current_snapshot.load(Ordering::Acquire);
                Ok(current)
            }
            SnapshotSpec::Id(id) => Ok(id),
            SnapshotSpec::Named(_name) => {
                // Named snapshots not yet supported
                Err(GrismError::not_implemented("Named snapshots"))
            }
        }
    }

    async fn scan(
        &self,
        dataset: DatasetId,
        projection: &Projection,
        _predicate: Option<&LogicalExpr>,
        snapshot: SnapshotId,
    ) -> GrismResult<RecordBatchStream> {
        // Note: predicate pushdown is not supported for memory storage (RFC-0020)
        // Predicates are evaluated by the execution engine.

        let batches = match dataset {
            DatasetId::Nodes { ref label } => {
                self.get_node_batches(label.as_deref(), snapshot).await?
            }
            DatasetId::Hyperedges { ref label } => {
                self.get_hyperedge_batches(label.as_deref(), snapshot)
                    .await?
            }
            DatasetId::Adjacency { .. } => {
                // Adjacency datasets not yet implemented
                return Ok(empty_stream());
            }
        };

        if batches.is_empty() {
            return Ok(empty_stream());
        }

        // Create stream
        let stream = MemoryBatchStream::boxed(batches);

        // Apply projection if specified
        if projection.is_all() {
            Ok(stream)
        } else {
            Ok(ProjectedBatchStream::boxed(
                stream,
                projection.columns.clone(),
            ))
        }
    }

    fn fragments(&self, dataset: DatasetId, snapshot: SnapshotId) -> Vec<FragmentMeta> {
        // Use try_read for non-blocking access
        // This is safe because fragments() is typically called when there's no contention

        match dataset {
            DatasetId::Nodes { ref label } => {
                // Try snapshot first
                if let Ok(snapshots) = self.snapshots.try_read()
                    && let Some(snap) = snapshots.get(&snapshot)
                {
                    return self.get_node_fragments(&snap.nodes, label.as_deref());
                }

                // Fall back to current state for snapshot 0
                if snapshot == 0
                    && let Ok(nodes) = self.nodes.try_read()
                {
                    return self.get_node_fragments(&nodes, label.as_deref());
                }

                Vec::new()
            }
            DatasetId::Hyperedges { ref label } => {
                // Try snapshot first
                if let Ok(snapshots) = self.snapshots.try_read()
                    && let Some(snap) = snapshots.get(&snapshot)
                {
                    return self.get_hyperedge_fragments(&snap.hyperedges, label.as_deref());
                }

                // Fall back to current state for snapshot 0
                if snapshot == 0
                    && let Ok(hyperedges) = self.hyperedges.try_read()
                {
                    return self.get_hyperedge_fragments(&hyperedges, label.as_deref());
                }

                Vec::new()
            }
            DatasetId::Adjacency { .. } => Vec::new(),
        }
    }

    fn capabilities(&self) -> StorageCaps {
        StorageCaps::memory()
    }

    fn current_snapshot(&self) -> GrismResult<SnapshotId> {
        Ok(self.current_snapshot.load(Ordering::Acquire))
    }
}

#[async_trait]
impl WritableStorage for MemoryStorage {
    async fn write(&self, dataset: DatasetId, batch: RecordBatch) -> GrismResult<usize> {
        let num_rows = batch.num_rows();

        match dataset {
            DatasetId::Nodes { label } => {
                let label = label.unwrap_or_else(|| "_default".to_string());
                let mut nodes = self.nodes.write().await;
                let store = nodes.entry(label).or_default();
                store.add_batch(batch)?;
            }
            DatasetId::Hyperedges { label } => {
                let label = label.unwrap_or_else(|| "_default".to_string());
                let mut hyperedges = self.hyperedges.write().await;
                let store = hyperedges.entry(label).or_default();
                store.add_batch(batch)?;
            }
            DatasetId::Adjacency { .. } => {
                return Err(GrismError::not_implemented("Adjacency writes"));
            }
        }

        Ok(num_rows)
    }

    async fn create_snapshot(&self) -> GrismResult<SnapshotId> {
        let id = self.next_snapshot_id.fetch_add(1, Ordering::AcqRel);

        let nodes = self.nodes.read().await;
        let hyperedges = self.hyperedges.read().await;

        let snapshot = MemorySnapshot::from_state(id, &nodes, &hyperedges);

        drop(nodes);
        drop(hyperedges);

        let mut snapshots = self.snapshots.write().await;
        snapshots.insert(id, snapshot);

        self.current_snapshot.store(id, Ordering::Release);

        Ok(id)
    }

    async fn flush(&self) -> GrismResult<()> {
        // No-op for memory storage
        Ok(())
    }
}

impl StorageStatsExt for MemoryStorage {
    fn stats(&self) -> StorageStats {
        // Synchronous stats - use try_read where possible
        let node_count = self
            .nodes
            .try_read()
            .map(|n| n.values().map(super::stores::NodeStore::row_count).sum())
            .unwrap_or(0);
        let hyperedge_count = self
            .hyperedges
            .try_read()
            .map(|h| {
                h.values()
                    .map(super::stores::HyperedgeStore::row_count)
                    .sum()
            })
            .unwrap_or(0);
        let node_label_count = self.nodes.try_read().map(|n| n.len()).unwrap_or(0);
        let hyperedge_label_count = self.hyperedges.try_read().map(|h| h.len()).unwrap_or(0);
        let snapshot_count = self.snapshots.try_read().map(|s| s.len()).unwrap_or(0);

        StorageStats {
            node_count,
            hyperedge_count,
            node_label_count,
            hyperedge_label_count,
            storage_bytes: None, // Could calculate from batches
            snapshot_count,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::test_utils::NodeBatchBuilder;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_memory_storage_basic() {
        let storage = MemoryStorage::new();

        // Should start empty
        let snapshot = storage.resolve_snapshot(SnapshotSpec::Latest).unwrap();
        assert_eq!(snapshot, 0);
    }

    #[tokio::test]
    async fn test_write_and_scan() {
        let storage = MemoryStorage::new();

        // Write some nodes
        let mut builder = NodeBatchBuilder::new();
        builder.add(1, Some("Person"));
        builder.add(2, Some("Person"));
        let batch = builder.build().unwrap();

        let rows = storage
            .write(DatasetId::nodes("Person"), batch)
            .await
            .unwrap();
        assert_eq!(rows, 2);

        // Create snapshot
        let snapshot = storage.create_snapshot().await.unwrap();

        // Scan
        let mut stream = storage
            .scan(
                DatasetId::nodes("Person"),
                &Projection::all(),
                None,
                snapshot,
            )
            .await
            .unwrap();

        let result = stream.next().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_snapshot_isolation() {
        let storage = MemoryStorage::new();

        // Write initial data
        let mut builder = NodeBatchBuilder::new();
        builder.add(1, Some("Person"));
        storage
            .write(DatasetId::nodes("Person"), builder.build().unwrap())
            .await
            .unwrap();

        // Create snapshot
        let snapshot1 = storage.create_snapshot().await.unwrap();

        // Write more data
        let mut builder2 = NodeBatchBuilder::new();
        builder2.add(2, Some("Person"));
        storage
            .write(DatasetId::nodes("Person"), builder2.build().unwrap())
            .await
            .unwrap();

        // Create another snapshot
        let snapshot2 = storage.create_snapshot().await.unwrap();

        // Scan snapshot1 - should have 1 row
        let mut stream1 = storage
            .scan(
                DatasetId::nodes("Person"),
                &Projection::all(),
                None,
                snapshot1,
            )
            .await
            .unwrap();
        let result1 = stream1.next().await.unwrap().unwrap();
        assert_eq!(result1.num_rows(), 1);

        // Scan snapshot2 - should have 2 rows
        let mut stream2 = storage
            .scan(
                DatasetId::nodes("Person"),
                &Projection::all(),
                None,
                snapshot2,
            )
            .await
            .unwrap();

        let mut total_rows = 0;
        while let Some(batch) = stream2.next().await {
            total_rows += batch.unwrap().num_rows();
        }
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_projection() {
        let storage = MemoryStorage::new();

        let mut builder = NodeBatchBuilder::new();
        builder.add(1, Some("Person"));
        storage
            .write(DatasetId::nodes("Person"), builder.build().unwrap())
            .await
            .unwrap();

        let snapshot = storage.create_snapshot().await.unwrap();

        // Scan with projection
        let mut stream = storage
            .scan(
                DatasetId::nodes("Person"),
                &Projection::columns(["_id"]),
                None,
                snapshot,
            )
            .await
            .unwrap();

        let result = stream.next().await.unwrap().unwrap();
        assert_eq!(result.num_columns(), 1);
    }

    #[tokio::test]
    async fn test_capabilities() {
        let storage = MemoryStorage::new();
        let caps = storage.capabilities();

        assert!(!caps.predicate_pushdown);
        assert!(caps.projection_pushdown);
        assert!(caps.fragment_pruning);
        assert!(!caps.object_store);
    }

    #[tokio::test]
    async fn test_fragments() {
        let storage = MemoryStorage::new();

        let mut builder = NodeBatchBuilder::new();
        builder.add(1, Some("Person"));
        builder.add(2, Some("Person"));
        storage
            .write(DatasetId::nodes("Person"), builder.build().unwrap())
            .await
            .unwrap();

        let snapshot = storage.create_snapshot().await.unwrap();

        let fragments = storage.fragments(DatasetId::nodes("Person"), snapshot);
        assert_eq!(fragments.len(), 1);
        assert_eq!(fragments[0].row_count, 2);
    }
}
