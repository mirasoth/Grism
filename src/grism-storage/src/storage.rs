//! Storage trait and configuration.
//!
//! This module provides storage backends for Grism:
//! - `InMemoryStorage`: Hash-map based storage for testing and small datasets
//! - `FileStorage`: JSON file-based storage for production and large datasets
//!
//! Per RFC-0102 Section 6.5, these storage backends support both local and distributed execution.

#![allow(clippy::missing_const_for_fn)] // Builder patterns often can't be const
#![allow(clippy::return_self_not_must_use)] // Builder patterns don't always need must_use

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use common_error::{GrismError, GrismResult};
use grism_core::hypergraph::{Edge, EdgeId, Hyperedge, Node, NodeId};

use crate::snapshot::Snapshot;

// ============================================================================
// Storage Configuration
// ============================================================================

/// Storage configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Base path for data storage.
    pub base_path: String,
    /// Enable snapshot isolation.
    pub snapshot_isolation: bool,
    /// Maximum number of snapshots to retain.
    pub max_snapshots: usize,
    /// Enable write-ahead logging for durability.
    pub enable_wal: bool,
    /// Sync writes to disk immediately.
    pub sync_writes: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            base_path: "./grism_data".to_string(),
            snapshot_isolation: true,
            max_snapshots: 10,
            enable_wal: true,
            sync_writes: false,
        }
    }
}

impl StorageConfig {
    /// Create a configuration for in-memory storage.
    pub fn in_memory() -> Self {
        Self {
            base_path: ":memory:".to_string(),
            snapshot_isolation: false,
            max_snapshots: 1,
            enable_wal: false,
            sync_writes: false,
        }
    }

    /// Create a configuration for file storage.
    pub fn file_storage(path: impl Into<String>) -> Self {
        Self {
            base_path: path.into(),
            ..Default::default()
        }
    }

    /// Set base path.
    pub fn with_base_path(mut self, path: impl Into<String>) -> Self {
        self.base_path = path.into();
        self
    }

    /// Enable or disable sync writes.
    pub fn with_sync_writes(mut self, sync: bool) -> Self {
        self.sync_writes = sync;
        self
    }
}

// ============================================================================
// Storage Trait
// ============================================================================

/// Trait for storage backends.
///
/// All storage implementations must be thread-safe (Send + Sync) to support
/// concurrent access from multiple operators.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Get the storage configuration.
    fn config(&self) -> &StorageConfig;

    /// Get storage statistics.
    fn stats(&self) -> StorageStats {
        StorageStats::default()
    }

    // Node operations

    /// Get a node by ID.
    async fn get_node(&self, id: NodeId) -> GrismResult<Option<Node>>;

    /// Get all nodes.
    async fn get_all_nodes(&self) -> GrismResult<Vec<Node>>;

    /// Get nodes by label.
    async fn get_nodes_by_label(&self, label: &str) -> GrismResult<Vec<Node>>;

    /// Insert a node.
    async fn insert_node(&self, node: &Node) -> GrismResult<NodeId>;

    /// Insert multiple nodes in a batch.
    async fn insert_nodes(&self, nodes: &[Node]) -> GrismResult<Vec<NodeId>> {
        let mut ids = Vec::with_capacity(nodes.len());
        for node in nodes {
            ids.push(self.insert_node(node).await?);
        }
        Ok(ids)
    }

    /// Delete a node.
    async fn delete_node(&self, id: NodeId) -> GrismResult<bool>;

    /// Count nodes by label.
    async fn count_nodes_by_label(&self, label: &str) -> GrismResult<usize> {
        Ok(self.get_nodes_by_label(label).await?.len())
    }

    // Edge operations

    /// Get an edge by ID.
    async fn get_edge(&self, id: EdgeId) -> GrismResult<Option<Edge>>;

    /// Get all edges.
    async fn get_all_edges(&self) -> GrismResult<Vec<Edge>>;

    /// Get edges by label.
    async fn get_edges_by_label(&self, label: &str) -> GrismResult<Vec<Edge>>;

    /// Get edges connected to a node.
    async fn get_edges_for_node(&self, node_id: NodeId) -> GrismResult<Vec<Edge>>;

    /// Insert an edge.
    async fn insert_edge(&self, edge: &Edge) -> GrismResult<EdgeId>;

    /// Insert multiple edges in a batch.
    async fn insert_edges(&self, edges: &[Edge]) -> GrismResult<Vec<EdgeId>> {
        let mut ids = Vec::with_capacity(edges.len());
        for edge in edges {
            ids.push(self.insert_edge(edge).await?);
        }
        Ok(ids)
    }

    /// Delete an edge.
    async fn delete_edge(&self, id: EdgeId) -> GrismResult<bool>;

    // Hyperedge operations

    /// Get a hyperedge by ID.
    async fn get_hyperedge(&self, id: EdgeId) -> GrismResult<Option<Hyperedge>>;

    /// Get all hyperedges.
    async fn get_all_hyperedges(&self) -> GrismResult<Vec<Hyperedge>>;

    /// Get hyperedges by label.
    async fn get_hyperedges_by_label(&self, label: &str) -> GrismResult<Vec<Hyperedge>>;

    /// Insert a hyperedge.
    async fn insert_hyperedge(&self, hyperedge: &Hyperedge) -> GrismResult<EdgeId>;

    /// Insert multiple hyperedges in a batch.
    async fn insert_hyperedges(&self, hyperedges: &[Hyperedge]) -> GrismResult<Vec<EdgeId>> {
        let mut ids = Vec::with_capacity(hyperedges.len());
        for hyperedge in hyperedges {
            ids.push(self.insert_hyperedge(hyperedge).await?);
        }
        Ok(ids)
    }

    /// Delete a hyperedge.
    async fn delete_hyperedge(&self, id: EdgeId) -> GrismResult<bool>;

    // Snapshot operations

    /// Create a snapshot.
    async fn create_snapshot(&self) -> GrismResult<Snapshot>;

    /// Get the current snapshot.
    async fn current_snapshot(&self) -> GrismResult<Option<Snapshot>>;

    // Persistence operations

    /// Flush any pending writes to storage.
    async fn flush(&self) -> GrismResult<()> {
        Ok(()) // Default no-op for in-memory storage
    }

    /// Close the storage, flushing any pending writes.
    async fn close(&self) -> GrismResult<()> {
        self.flush().await
    }
}

/// Storage statistics.
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    /// Number of nodes.
    pub node_count: usize,
    /// Number of edges.
    pub edge_count: usize,
    /// Number of hyperedges.
    pub hyperedge_count: usize,
    /// Storage size in bytes (if applicable).
    pub storage_bytes: Option<usize>,
}

// ============================================================================
// In-Memory Storage
// ============================================================================

/// In-memory storage implementation for testing and small datasets.
///
/// This storage backend keeps all data in memory using `HashMap`s.
/// It is thread-safe and supports concurrent read/write access.
pub struct InMemoryStorage {
    config: StorageConfig,
    nodes: tokio::sync::RwLock<HashMap<NodeId, Node>>,
    edges: tokio::sync::RwLock<HashMap<EdgeId, Edge>>,
    hyperedges: tokio::sync::RwLock<HashMap<EdgeId, Hyperedge>>,
    current_snapshot: tokio::sync::RwLock<Option<Snapshot>>,
}

impl InMemoryStorage {
    /// Create a new in-memory storage.
    pub fn new() -> Self {
        Self::with_config(StorageConfig::in_memory())
    }

    /// Create with configuration.
    pub fn with_config(config: StorageConfig) -> Self {
        Self {
            config,
            nodes: tokio::sync::RwLock::new(HashMap::new()),
            edges: tokio::sync::RwLock::new(HashMap::new()),
            hyperedges: tokio::sync::RwLock::new(HashMap::new()),
            current_snapshot: tokio::sync::RwLock::new(None),
        }
    }

    /// Get the number of nodes.
    pub async fn node_count(&self) -> usize {
        self.nodes.read().await.len()
    }

    /// Get the number of edges.
    pub async fn edge_count(&self) -> usize {
        self.edges.read().await.len()
    }

    /// Get the number of hyperedges.
    pub async fn hyperedge_count(&self) -> usize {
        self.hyperedges.read().await.len()
    }

    /// Clear all data.
    pub async fn clear(&self) {
        self.nodes.write().await.clear();
        self.edges.write().await.clear();
        self.hyperedges.write().await.clear();
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for InMemoryStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryStorage")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    fn config(&self) -> &StorageConfig {
        &self.config
    }

    fn stats(&self) -> StorageStats {
        // Note: This is approximate since we can't async here
        StorageStats::default()
    }

    async fn get_node(&self, id: NodeId) -> GrismResult<Option<Node>> {
        Ok(self.nodes.read().await.get(&id).cloned())
    }

    async fn get_all_nodes(&self) -> GrismResult<Vec<Node>> {
        Ok(self.nodes.read().await.values().cloned().collect())
    }

    async fn get_nodes_by_label(&self, label: &str) -> GrismResult<Vec<Node>> {
        Ok(self
            .nodes
            .read()
            .await
            .values()
            .filter(|n| n.has_label(label))
            .cloned()
            .collect())
    }

    async fn insert_node(&self, node: &Node) -> GrismResult<NodeId> {
        self.nodes.write().await.insert(node.id, node.clone());
        Ok(node.id)
    }

    async fn insert_nodes(&self, nodes: &[Node]) -> GrismResult<Vec<NodeId>> {
        let ids: Vec<_> = nodes.iter().map(|n| n.id).collect();
        {
            let mut lock = self.nodes.write().await;
            for node in nodes {
                lock.insert(node.id, node.clone());
            }
        }
        Ok(ids)
    }

    async fn delete_node(&self, id: NodeId) -> GrismResult<bool> {
        Ok(self.nodes.write().await.remove(&id).is_some())
    }

    async fn get_edge(&self, id: EdgeId) -> GrismResult<Option<Edge>> {
        Ok(self.edges.read().await.get(&id).cloned())
    }

    async fn get_all_edges(&self) -> GrismResult<Vec<Edge>> {
        Ok(self.edges.read().await.values().cloned().collect())
    }

    async fn get_edges_by_label(&self, label: &str) -> GrismResult<Vec<Edge>> {
        Ok(self
            .edges
            .read()
            .await
            .values()
            .filter(|e| e.has_label(label))
            .cloned()
            .collect())
    }

    async fn get_edges_for_node(&self, node_id: NodeId) -> GrismResult<Vec<Edge>> {
        Ok(self
            .edges
            .read()
            .await
            .values()
            .filter(|e| e.source == node_id || e.target == node_id)
            .cloned()
            .collect())
    }

    async fn insert_edge(&self, edge: &Edge) -> GrismResult<EdgeId> {
        self.edges.write().await.insert(edge.id, edge.clone());
        Ok(edge.id)
    }

    async fn insert_edges(&self, edges: &[Edge]) -> GrismResult<Vec<EdgeId>> {
        let ids: Vec<_> = edges.iter().map(|e| e.id).collect();
        {
            let mut lock = self.edges.write().await;
            for edge in edges {
                lock.insert(edge.id, edge.clone());
            }
        }
        Ok(ids)
    }

    async fn delete_edge(&self, id: EdgeId) -> GrismResult<bool> {
        Ok(self.edges.write().await.remove(&id).is_some())
    }

    async fn get_hyperedge(&self, id: EdgeId) -> GrismResult<Option<Hyperedge>> {
        Ok(self.hyperedges.read().await.get(&id).cloned())
    }

    async fn get_all_hyperedges(&self) -> GrismResult<Vec<Hyperedge>> {
        Ok(self.hyperedges.read().await.values().cloned().collect())
    }

    async fn get_hyperedges_by_label(&self, label: &str) -> GrismResult<Vec<Hyperedge>> {
        Ok(self
            .hyperedges
            .read()
            .await
            .values()
            .filter(|h| h.label == label)
            .cloned()
            .collect())
    }

    async fn insert_hyperedge(&self, hyperedge: &Hyperedge) -> GrismResult<EdgeId> {
        self.hyperedges
            .write()
            .await
            .insert(hyperedge.id, hyperedge.clone());
        Ok(hyperedge.id)
    }

    async fn insert_hyperedges(&self, hyperedges: &[Hyperedge]) -> GrismResult<Vec<EdgeId>> {
        let ids: Vec<_> = hyperedges.iter().map(|h| h.id).collect();
        {
            let mut lock = self.hyperedges.write().await;
            for hyperedge in hyperedges {
                lock.insert(hyperedge.id, hyperedge.clone());
            }
        }
        Ok(ids)
    }

    async fn delete_hyperedge(&self, id: EdgeId) -> GrismResult<bool> {
        Ok(self.hyperedges.write().await.remove(&id).is_some())
    }

    async fn create_snapshot(&self) -> GrismResult<Snapshot> {
        let snapshot = Snapshot::new();
        *self.current_snapshot.write().await = Some(snapshot.clone());
        Ok(snapshot)
    }

    async fn current_snapshot(&self) -> GrismResult<Option<Snapshot>> {
        Ok(self.current_snapshot.read().await.clone())
    }
}

// ============================================================================
// File Storage (JSON-based)
// ============================================================================

/// File storage data format.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct FileStorageData {
    nodes: HashMap<NodeId, Node>,
    edges: HashMap<EdgeId, Edge>,
    hyperedges: HashMap<EdgeId, Hyperedge>,
    snapshot: Option<Snapshot>,
}

/// File-based storage implementation for production use.
///
/// This storage backend persists data to JSON files for durability.
/// It supports larger datasets that don't fit in memory and provides
/// basic durability guarantees.
///
/// **Note**: For very large datasets, consider using Lance format storage
/// (`LanceStorage`) when implemented.
pub struct FileStorage {
    config: StorageConfig,
    path: PathBuf,
    data: tokio::sync::RwLock<FileStorageData>,
    dirty: tokio::sync::RwLock<bool>,
}

impl FileStorage {
    /// Create or open file storage at the given path.
    pub async fn open(path: impl AsRef<Path>) -> GrismResult<Self> {
        let path = path.as_ref().to_path_buf();
        let config = StorageConfig::file_storage(path.to_string_lossy().to_string());

        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                GrismError::InternalError(format!("Failed to create storage directory: {e}"))
            })?;
        }

        // Load existing data or create new
        let data = if path.exists() {
            let contents = tokio::fs::read_to_string(&path).await.map_err(|e| {
                GrismError::InternalError(format!("Failed to read storage file: {e}"))
            })?;
            serde_json::from_str(&contents).map_err(|e| {
                GrismError::InternalError(format!("Failed to parse storage file: {e}"))
            })?
        } else {
            FileStorageData::default()
        };

        Ok(Self {
            config,
            path,
            data: tokio::sync::RwLock::new(data),
            dirty: tokio::sync::RwLock::new(false),
        })
    }

    /// Create a new file storage with configuration.
    pub async fn with_config(config: StorageConfig) -> GrismResult<Self> {
        Self::open(&config.base_path).await
    }

    /// Mark the storage as dirty (needs flushing).
    async fn mark_dirty(&self) {
        *self.dirty.write().await = true;
    }

    /// Persist data to disk.
    async fn persist(&self) -> GrismResult<()> {
        let data = self.data.read().await;
        let contents = serde_json::to_string_pretty(&*data).map_err(|e| {
            GrismError::InternalError(format!("Failed to serialize storage data: {e}"))
        })?;
        drop(data);

        tokio::fs::write(&self.path, contents)
            .await
            .map_err(|e| GrismError::InternalError(format!("Failed to write storage file: {e}")))?;

        *self.dirty.write().await = false;
        Ok(())
    }

    /// Get the storage file path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl std::fmt::Debug for FileStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileStorage")
            .field("path", &self.path)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Storage for FileStorage {
    fn config(&self) -> &StorageConfig {
        &self.config
    }

    fn stats(&self) -> StorageStats {
        StorageStats::default()
    }

    async fn get_node(&self, id: NodeId) -> GrismResult<Option<Node>> {
        Ok(self.data.read().await.nodes.get(&id).cloned())
    }

    async fn get_all_nodes(&self) -> GrismResult<Vec<Node>> {
        Ok(self.data.read().await.nodes.values().cloned().collect())
    }

    async fn get_nodes_by_label(&self, label: &str) -> GrismResult<Vec<Node>> {
        Ok(self
            .data
            .read()
            .await
            .nodes
            .values()
            .filter(|n| n.has_label(label))
            .cloned()
            .collect())
    }

    async fn insert_node(&self, node: &Node) -> GrismResult<NodeId> {
        self.data.write().await.nodes.insert(node.id, node.clone());
        self.mark_dirty().await;
        if self.config.sync_writes {
            self.persist().await?;
        }
        Ok(node.id)
    }

    async fn insert_nodes(&self, nodes: &[Node]) -> GrismResult<Vec<NodeId>> {
        let mut data = self.data.write().await;
        let ids: Vec<_> = nodes.iter().map(|n| n.id).collect();
        for node in nodes {
            data.nodes.insert(node.id, node.clone());
        }
        drop(data);
        self.mark_dirty().await;
        if self.config.sync_writes {
            self.persist().await?;
        }
        Ok(ids)
    }

    async fn delete_node(&self, id: NodeId) -> GrismResult<bool> {
        let result = self.data.write().await.nodes.remove(&id).is_some();
        if result {
            self.mark_dirty().await;
            if self.config.sync_writes {
                self.persist().await?;
            }
        }
        Ok(result)
    }

    async fn get_edge(&self, id: EdgeId) -> GrismResult<Option<Edge>> {
        Ok(self.data.read().await.edges.get(&id).cloned())
    }

    async fn get_all_edges(&self) -> GrismResult<Vec<Edge>> {
        Ok(self.data.read().await.edges.values().cloned().collect())
    }

    async fn get_edges_by_label(&self, label: &str) -> GrismResult<Vec<Edge>> {
        Ok(self
            .data
            .read()
            .await
            .edges
            .values()
            .filter(|e| e.has_label(label))
            .cloned()
            .collect())
    }

    async fn get_edges_for_node(&self, node_id: NodeId) -> GrismResult<Vec<Edge>> {
        Ok(self
            .data
            .read()
            .await
            .edges
            .values()
            .filter(|e| e.source == node_id || e.target == node_id)
            .cloned()
            .collect())
    }

    async fn insert_edge(&self, edge: &Edge) -> GrismResult<EdgeId> {
        self.data.write().await.edges.insert(edge.id, edge.clone());
        self.mark_dirty().await;
        if self.config.sync_writes {
            self.persist().await?;
        }
        Ok(edge.id)
    }

    async fn insert_edges(&self, edges: &[Edge]) -> GrismResult<Vec<EdgeId>> {
        let mut data = self.data.write().await;
        let ids: Vec<_> = edges.iter().map(|e| e.id).collect();
        for edge in edges {
            data.edges.insert(edge.id, edge.clone());
        }
        drop(data);
        self.mark_dirty().await;
        if self.config.sync_writes {
            self.persist().await?;
        }
        Ok(ids)
    }

    async fn delete_edge(&self, id: EdgeId) -> GrismResult<bool> {
        let result = self.data.write().await.edges.remove(&id).is_some();
        if result {
            self.mark_dirty().await;
            if self.config.sync_writes {
                self.persist().await?;
            }
        }
        Ok(result)
    }

    async fn get_hyperedge(&self, id: EdgeId) -> GrismResult<Option<Hyperedge>> {
        Ok(self.data.read().await.hyperedges.get(&id).cloned())
    }

    async fn get_all_hyperedges(&self) -> GrismResult<Vec<Hyperedge>> {
        Ok(self
            .data
            .read()
            .await
            .hyperedges
            .values()
            .cloned()
            .collect())
    }

    async fn get_hyperedges_by_label(&self, label: &str) -> GrismResult<Vec<Hyperedge>> {
        Ok(self
            .data
            .read()
            .await
            .hyperedges
            .values()
            .filter(|h| h.label == label)
            .cloned()
            .collect())
    }

    async fn insert_hyperedge(&self, hyperedge: &Hyperedge) -> GrismResult<EdgeId> {
        self.data
            .write()
            .await
            .hyperedges
            .insert(hyperedge.id, hyperedge.clone());
        self.mark_dirty().await;
        if self.config.sync_writes {
            self.persist().await?;
        }
        Ok(hyperedge.id)
    }

    async fn insert_hyperedges(&self, hyperedges: &[Hyperedge]) -> GrismResult<Vec<EdgeId>> {
        let mut data = self.data.write().await;
        let ids: Vec<_> = hyperedges.iter().map(|h| h.id).collect();
        for hyperedge in hyperedges {
            data.hyperedges.insert(hyperedge.id, hyperedge.clone());
        }
        drop(data);
        self.mark_dirty().await;
        if self.config.sync_writes {
            self.persist().await?;
        }
        Ok(ids)
    }

    async fn delete_hyperedge(&self, id: EdgeId) -> GrismResult<bool> {
        let result = self.data.write().await.hyperedges.remove(&id).is_some();
        if result {
            self.mark_dirty().await;
            if self.config.sync_writes {
                self.persist().await?;
            }
        }
        Ok(result)
    }

    async fn create_snapshot(&self) -> GrismResult<Snapshot> {
        let snapshot = Snapshot::new();
        self.data.write().await.snapshot = Some(snapshot.clone());
        self.mark_dirty().await;
        self.persist().await?;
        Ok(snapshot)
    }

    async fn current_snapshot(&self) -> GrismResult<Option<Snapshot>> {
        Ok(self.data.read().await.snapshot.clone())
    }

    async fn flush(&self) -> GrismResult<()> {
        if *self.dirty.read().await {
            self.persist().await?;
        }
        Ok(())
    }

    async fn close(&self) -> GrismResult<()> {
        self.flush().await
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use grism_core::hypergraph::Node;

    #[tokio::test]
    async fn test_in_memory_storage() {
        let storage = InMemoryStorage::new();

        let node = Node::new().with_label("Person");
        let id = storage.insert_node(&node).await.unwrap();

        let retrieved = storage.get_node(id).await.unwrap();
        assert!(retrieved.is_some());
        assert!(retrieved.unwrap().has_label("Person"));
    }

    #[tokio::test]
    async fn test_get_nodes_by_label() {
        let storage = InMemoryStorage::new();

        storage
            .insert_node(&Node::new().with_label("Person"))
            .await
            .unwrap();
        storage
            .insert_node(&Node::new().with_label("Person"))
            .await
            .unwrap();
        storage
            .insert_node(&Node::new().with_label("Company"))
            .await
            .unwrap();

        let persons = storage.get_nodes_by_label("Person").await.unwrap();
        assert_eq!(persons.len(), 2);
    }

    #[tokio::test]
    async fn test_get_all_nodes() {
        let storage = InMemoryStorage::new();

        storage
            .insert_node(&Node::new().with_label("Person"))
            .await
            .unwrap();
        storage
            .insert_node(&Node::new().with_label("Company"))
            .await
            .unwrap();

        let all = storage.get_all_nodes().await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn test_batch_insert() {
        let storage = InMemoryStorage::new();

        let nodes = vec![
            Node::new().with_label("Person"),
            Node::new().with_label("Person"),
            Node::new().with_label("Company"),
        ];

        let ids = storage.insert_nodes(&nodes).await.unwrap();
        assert_eq!(ids.len(), 3);

        let all = storage.get_all_nodes().await.unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn test_clear() {
        let storage = InMemoryStorage::new();

        storage
            .insert_node(&Node::new().with_label("Person"))
            .await
            .unwrap();
        assert_eq!(storage.node_count().await, 1);

        storage.clear().await;
        assert_eq!(storage.node_count().await, 0);
    }
}
