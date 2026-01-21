//! Storage trait and configuration.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use common_error::GrismResult;
use grism_core::hypergraph::{Edge, EdgeId, Hyperedge, Node, NodeId};

use crate::snapshot::Snapshot;

/// Storage configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Base path for data storage.
    pub base_path: String,
    /// Enable snapshot isolation.
    pub snapshot_isolation: bool,
    /// Maximum number of snapshots to retain.
    pub max_snapshots: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            base_path: "./grism_data".to_string(),
            snapshot_isolation: true,
            max_snapshots: 10,
        }
    }
}

/// Trait for storage backends.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Get the storage configuration.
    fn config(&self) -> &StorageConfig;

    // Node operations

    /// Get a node by ID.
    async fn get_node(&self, id: NodeId) -> GrismResult<Option<Node>>;

    /// Get nodes by label.
    async fn get_nodes_by_label(&self, label: &str) -> GrismResult<Vec<Node>>;

    /// Insert a node.
    async fn insert_node(&self, node: &Node) -> GrismResult<NodeId>;

    /// Delete a node.
    async fn delete_node(&self, id: NodeId) -> GrismResult<bool>;

    // Edge operations

    /// Get an edge by ID.
    async fn get_edge(&self, id: EdgeId) -> GrismResult<Option<Edge>>;

    /// Get edges by label.
    async fn get_edges_by_label(&self, label: &str) -> GrismResult<Vec<Edge>>;

    /// Get edges connected to a node.
    async fn get_edges_for_node(&self, node_id: NodeId) -> GrismResult<Vec<Edge>>;

    /// Insert an edge.
    async fn insert_edge(&self, edge: &Edge) -> GrismResult<EdgeId>;

    /// Delete an edge.
    async fn delete_edge(&self, id: EdgeId) -> GrismResult<bool>;

    // Hyperedge operations

    /// Get a hyperedge by ID.
    async fn get_hyperedge(&self, id: EdgeId) -> GrismResult<Option<Hyperedge>>;

    /// Get hyperedges by label.
    async fn get_hyperedges_by_label(&self, label: &str) -> GrismResult<Vec<Hyperedge>>;

    /// Insert a hyperedge.
    async fn insert_hyperedge(&self, hyperedge: &Hyperedge) -> GrismResult<EdgeId>;

    /// Delete a hyperedge.
    async fn delete_hyperedge(&self, id: EdgeId) -> GrismResult<bool>;

    // Snapshot operations

    /// Create a snapshot.
    async fn create_snapshot(&self) -> GrismResult<Snapshot>;

    /// Get the current snapshot.
    async fn current_snapshot(&self) -> GrismResult<Option<Snapshot>>;
}

/// In-memory storage implementation for testing.
pub struct InMemoryStorage {
    config: StorageConfig,
    nodes: tokio::sync::RwLock<std::collections::HashMap<NodeId, Node>>,
    edges: tokio::sync::RwLock<std::collections::HashMap<EdgeId, Edge>>,
    hyperedges: tokio::sync::RwLock<std::collections::HashMap<EdgeId, Hyperedge>>,
}

impl InMemoryStorage {
    /// Create a new in-memory storage.
    pub fn new() -> Self {
        Self::with_config(StorageConfig::default())
    }

    /// Create with configuration.
    pub fn with_config(config: StorageConfig) -> Self {
        Self {
            config,
            nodes: tokio::sync::RwLock::new(std::collections::HashMap::new()),
            edges: tokio::sync::RwLock::new(std::collections::HashMap::new()),
            hyperedges: tokio::sync::RwLock::new(std::collections::HashMap::new()),
        }
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    fn config(&self) -> &StorageConfig {
        &self.config
    }

    async fn get_node(&self, id: NodeId) -> GrismResult<Option<Node>> {
        Ok(self.nodes.read().await.get(&id).cloned())
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

    async fn delete_node(&self, id: NodeId) -> GrismResult<bool> {
        Ok(self.nodes.write().await.remove(&id).is_some())
    }

    async fn get_edge(&self, id: EdgeId) -> GrismResult<Option<Edge>> {
        Ok(self.edges.read().await.get(&id).cloned())
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

    async fn delete_edge(&self, id: EdgeId) -> GrismResult<bool> {
        Ok(self.edges.write().await.remove(&id).is_some())
    }

    async fn get_hyperedge(&self, id: EdgeId) -> GrismResult<Option<Hyperedge>> {
        Ok(self.hyperedges.read().await.get(&id).cloned())
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

    async fn delete_hyperedge(&self, id: EdgeId) -> GrismResult<bool> {
        Ok(self.hyperedges.write().await.remove(&id).is_some())
    }

    async fn create_snapshot(&self) -> GrismResult<Snapshot> {
        Ok(Snapshot::new())
    }

    async fn current_snapshot(&self) -> GrismResult<Option<Snapshot>> {
        Ok(Some(Snapshot::new()))
    }
}

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
}
