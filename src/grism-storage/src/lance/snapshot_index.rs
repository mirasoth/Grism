//! Snapshot index management for Lance storage.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use common_error::{GrismError, GrismResult};

use crate::snapshot::SnapshotId;

// ============================================================================
// Snapshot Index
// ============================================================================

/// Index tracking all snapshots and their metadata.
///
/// The snapshot index is persisted as JSON and loaded on startup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotIndex {
    /// Next snapshot ID to allocate.
    pub next_id: SnapshotId,
    /// Current/latest snapshot ID.
    pub current_id: SnapshotId,
    /// Snapshot metadata keyed by ID.
    pub snapshots: HashMap<SnapshotId, SnapshotMeta>,
    /// Storage version for compatibility.
    pub version: u32,
}

/// Metadata for a single snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    /// Snapshot ID.
    pub id: SnapshotId,
    /// Unix timestamp (millis) when created.
    pub created_at: i64,
    /// Parent snapshot ID (for branching).
    pub parent: Option<SnapshotId>,
    /// Optional name/tag.
    pub name: Option<String>,
    /// Node labels present in this snapshot.
    pub node_labels: Vec<String>,
    /// Hyperedge labels present in this snapshot.
    pub hyperedge_labels: Vec<String>,
    /// Whether the snapshot is finalized (immutable).
    pub finalized: bool,
}

impl SnapshotIndex {
    /// Current storage version.
    pub const VERSION: u32 = 1;

    /// Create a new empty index.
    pub fn new() -> Self {
        Self {
            next_id: 1,
            current_id: 0,
            snapshots: HashMap::new(),
            version: Self::VERSION,
        }
    }

    /// Load index from a file.
    pub async fn load(path: &Path) -> GrismResult<Self> {
        let content = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| GrismError::storage(format!("Failed to read snapshot index: {e}")))?;

        let index: Self = serde_json::from_str(&content)
            .map_err(|e| GrismError::storage(format!("Failed to parse snapshot index: {e}")))?;

        // Version check
        if index.version != Self::VERSION {
            return Err(GrismError::storage(format!(
                "Snapshot index version mismatch: expected {}, got {}",
                Self::VERSION,
                index.version
            )));
        }

        Ok(index)
    }

    /// Save index to a file.
    pub async fn save(&self, path: &Path) -> GrismResult<()> {
        let content = serde_json::to_string_pretty(self)
            .map_err(|e| GrismError::storage(format!("Failed to serialize snapshot index: {e}")))?;

        // Write to temp file first, then rename for atomicity
        let tmp_path = path.with_extension("tmp");
        tokio::fs::write(&tmp_path, &content)
            .await
            .map_err(|e| GrismError::storage(format!("Failed to write snapshot index: {e}")))?;

        tokio::fs::rename(&tmp_path, path)
            .await
            .map_err(|e| GrismError::storage(format!("Failed to rename snapshot index: {e}")))?;

        Ok(())
    }

    /// Allocate a new snapshot ID.
    pub fn allocate_id(&mut self) -> SnapshotId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Register a new snapshot.
    pub fn register(&mut self, meta: SnapshotMeta) {
        self.snapshots.insert(meta.id, meta);
    }

    /// Finalize a snapshot and set as current.
    pub fn finalize(&mut self, id: SnapshotId) -> GrismResult<()> {
        let meta = self
            .snapshots
            .get_mut(&id)
            .ok_or_else(|| GrismError::storage(format!("Snapshot {id} not found")))?;

        meta.finalized = true;
        self.current_id = id;
        Ok(())
    }

    /// Get snapshot metadata.
    pub fn get(&self, id: SnapshotId) -> Option<&SnapshotMeta> {
        self.snapshots.get(&id)
    }

    /// Get the current snapshot ID.
    pub fn current(&self) -> SnapshotId {
        self.current_id
    }

    /// Get the latest snapshot (reserved for future use).
    #[allow(dead_code)]
    pub fn latest(&self) -> Option<&SnapshotMeta> {
        if self.current_id == 0 {
            None
        } else {
            self.snapshots.get(&self.current_id)
        }
    }

    /// List all snapshot IDs in chronological order (reserved for future use).
    #[allow(dead_code)]
    pub fn list_ids(&self) -> Vec<SnapshotId> {
        let mut ids: Vec<_> = self.snapshots.keys().copied().collect();
        ids.sort();
        ids
    }
}

impl Default for SnapshotIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotMeta {
    /// Create metadata for a new snapshot.
    pub fn new(id: SnapshotId) -> Self {
        Self {
            id,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            parent: None,
            name: None,
            node_labels: Vec::new(),
            hyperedge_labels: Vec::new(),
            finalized: false,
        }
    }

    /// Set parent snapshot.
    pub fn with_parent(mut self, parent: SnapshotId) -> Self {
        self.parent = Some(parent);
        self
    }

    /// Set name (reserved for future use).
    #[allow(dead_code)]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Add node labels.
    pub fn with_node_labels(mut self, labels: impl IntoIterator<Item = String>) -> Self {
        self.node_labels = labels.into_iter().collect();
        self
    }

    /// Add hyperedge labels.
    pub fn with_hyperedge_labels(mut self, labels: impl IntoIterator<Item = String>) -> Self {
        self.hyperedge_labels = labels.into_iter().collect();
        self
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_index_new() {
        let index = SnapshotIndex::new();
        assert_eq!(index.next_id, 1);
        assert_eq!(index.current_id, 0);
        assert!(index.snapshots.is_empty());
    }

    #[test]
    fn test_allocate_id() {
        let mut index = SnapshotIndex::new();

        assert_eq!(index.allocate_id(), 1);
        assert_eq!(index.allocate_id(), 2);
        assert_eq!(index.allocate_id(), 3);
        assert_eq!(index.next_id, 4);
    }

    #[test]
    fn test_register_and_finalize() {
        let mut index = SnapshotIndex::new();

        let id = index.allocate_id();
        let meta = SnapshotMeta::new(id).with_node_labels(vec!["Person".to_string()]);

        index.register(meta);
        assert_eq!(index.snapshots.len(), 1);
        assert!(!index.get(id).unwrap().finalized);

        index.finalize(id).unwrap();
        assert!(index.get(id).unwrap().finalized);
        assert_eq!(index.current(), id);
    }

    #[test]
    fn test_snapshot_meta() {
        let meta = SnapshotMeta::new(1)
            .with_parent(0)
            .with_name("test")
            .with_node_labels(vec!["Person".to_string(), "Company".to_string()]);

        assert_eq!(meta.id, 1);
        assert_eq!(meta.parent, Some(0));
        assert_eq!(meta.name, Some("test".to_string()));
        assert_eq!(meta.node_labels.len(), 2);
    }

    #[tokio::test]
    async fn test_save_and_load() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = tmp_dir.path().join("snapshot_index.json");

        let mut index = SnapshotIndex::new();
        let id = index.allocate_id();
        index.register(SnapshotMeta::new(id));
        index.finalize(id).unwrap();

        // Save
        index.save(&path).await.unwrap();

        // Load
        let loaded = SnapshotIndex::load(&path).await.unwrap();
        assert_eq!(loaded.current_id, index.current_id);
        assert_eq!(loaded.next_id, index.next_id);
        assert_eq!(loaded.snapshots.len(), index.snapshots.len());
    }
}
