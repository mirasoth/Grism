//! Filesystem layout management for Lance storage.

use std::path::{Path, PathBuf};

use common_error::{GrismError, GrismResult};

use crate::snapshot::SnapshotId;
use crate::types::AdjacencySpec;

// ============================================================================
// Directory Layout
// ============================================================================

/// Manages the filesystem layout for Lance storage.
///
/// Layout per RFC-0019 §4:
/// ```text
/// <root>/
/// ├── snapshots/
/// │   └── <snapshot_id>/
/// │       ├── nodes/<label>.lance/
/// │       ├── hyperedges/<label>.lance/
/// │       └── adjacency/<spec>.lance/
/// └── metadata/
///     └── snapshot_index.json
/// ```
#[derive(Debug, Clone)]
pub struct StorageLayout {
    root: PathBuf,
}

impl StorageLayout {
    /// Create a new storage layout.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Get the root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Get the snapshots directory.
    pub fn snapshots_dir(&self) -> PathBuf {
        self.root.join("snapshots")
    }

    /// Get the metadata directory.
    pub fn metadata_dir(&self) -> PathBuf {
        self.root.join("metadata")
    }

    /// Get the snapshot index file path.
    pub fn snapshot_index_path(&self) -> PathBuf {
        self.metadata_dir().join("snapshot_index.json")
    }

    /// Get the directory for a specific snapshot.
    pub fn snapshot_dir(&self, snapshot: SnapshotId) -> PathBuf {
        self.snapshots_dir().join(snapshot.to_string())
    }

    /// Get the nodes directory for a snapshot.
    pub fn nodes_dir(&self, snapshot: SnapshotId) -> PathBuf {
        self.snapshot_dir(snapshot).join("nodes")
    }

    /// Get the hyperedges directory for a snapshot.
    pub fn hyperedges_dir(&self, snapshot: SnapshotId) -> PathBuf {
        self.snapshot_dir(snapshot).join("hyperedges")
    }

    /// Get the adjacency directory for a snapshot.
    pub fn adjacency_dir(&self, snapshot: SnapshotId) -> PathBuf {
        self.snapshot_dir(snapshot).join("adjacency")
    }

    /// Get the path for a node dataset.
    pub fn node_dataset_path(&self, snapshot: SnapshotId, label: &str) -> PathBuf {
        self.nodes_dir(snapshot)
            .join(format!("{}.lance", sanitize_label(label)))
    }

    /// Get the path for a hyperedge dataset.
    pub fn hyperedge_dataset_path(&self, snapshot: SnapshotId, label: &str) -> PathBuf {
        self.hyperedges_dir(snapshot)
            .join(format!("{}.lance", sanitize_label(label)))
    }

    /// Get the path for an adjacency dataset (reserved for future use).
    #[allow(dead_code)]
    pub fn adjacency_dataset_path(&self, snapshot: SnapshotId, spec: &AdjacencySpec) -> PathBuf {
        let name = format!("{}_{}", sanitize_label(&spec.edge_label), spec.direction);
        self.adjacency_dir(snapshot).join(format!("{name}.lance"))
    }

    /// Create all necessary directories for a snapshot.
    pub async fn create_snapshot_dirs(&self, snapshot: SnapshotId) -> GrismResult<()> {
        tokio::fs::create_dir_all(self.nodes_dir(snapshot))
            .await
            .map_err(|e| GrismError::storage(format!("Failed to create nodes dir: {e}")))?;
        tokio::fs::create_dir_all(self.hyperedges_dir(snapshot))
            .await
            .map_err(|e| GrismError::storage(format!("Failed to create hyperedges dir: {e}")))?;
        tokio::fs::create_dir_all(self.adjacency_dir(snapshot))
            .await
            .map_err(|e| GrismError::storage(format!("Failed to create adjacency dir: {e}")))?;
        Ok(())
    }

    /// Create metadata directory.
    pub async fn create_metadata_dir(&self) -> GrismResult<()> {
        tokio::fs::create_dir_all(self.metadata_dir())
            .await
            .map_err(|e| GrismError::storage(format!("Failed to create metadata dir: {e}")))?;
        Ok(())
    }

    /// Check if the storage root exists (reserved for future use).
    #[allow(dead_code)]
    pub fn exists(&self) -> bool {
        self.root.exists()
    }

    /// Check if a snapshot exists (reserved for future use).
    #[allow(dead_code)]
    pub fn snapshot_exists(&self, snapshot: SnapshotId) -> bool {
        self.snapshot_dir(snapshot).exists()
    }

    /// List all node dataset labels in a snapshot.
    pub async fn list_node_labels(&self, snapshot: SnapshotId) -> GrismResult<Vec<String>> {
        self.list_dataset_labels(self.nodes_dir(snapshot)).await
    }

    /// List all hyperedge dataset labels in a snapshot.
    pub async fn list_hyperedge_labels(&self, snapshot: SnapshotId) -> GrismResult<Vec<String>> {
        self.list_dataset_labels(self.hyperedges_dir(snapshot))
            .await
    }

    /// List dataset labels in a directory.
    async fn list_dataset_labels(&self, dir: PathBuf) -> GrismResult<Vec<String>> {
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut labels = Vec::new();
        let mut entries = tokio::fs::read_dir(&dir)
            .await
            .map_err(|e| GrismError::storage(format!("Failed to read dir: {e}")))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| GrismError::storage(format!("Failed to read entry: {e}")))?
        {
            let path = entry.path();
            if path.is_dir()
                && let Some(name) = path.file_stem()
                && let Some(name) = name.to_str()
            {
                // Remove .lance extension if present
                let label = name.strip_suffix(".lance").unwrap_or(name);
                labels.push(label.to_string());
            }
        }

        Ok(labels)
    }
}

/// Sanitize a label for use in filesystem paths.
fn sanitize_label(label: &str) -> String {
    label
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_layout_paths() {
        let layout = StorageLayout::new("/data/grism");

        assert_eq!(layout.root(), Path::new("/data/grism"));
        assert_eq!(
            layout.snapshots_dir(),
            PathBuf::from("/data/grism/snapshots")
        );
        assert_eq!(layout.metadata_dir(), PathBuf::from("/data/grism/metadata"));
        assert_eq!(
            layout.snapshot_index_path(),
            PathBuf::from("/data/grism/metadata/snapshot_index.json")
        );
    }

    #[test]
    fn test_snapshot_paths() {
        let layout = StorageLayout::new("/data/grism");
        let snapshot = 42;

        assert_eq!(
            layout.snapshot_dir(snapshot),
            PathBuf::from("/data/grism/snapshots/42")
        );
        assert_eq!(
            layout.nodes_dir(snapshot),
            PathBuf::from("/data/grism/snapshots/42/nodes")
        );
        assert_eq!(
            layout.hyperedges_dir(snapshot),
            PathBuf::from("/data/grism/snapshots/42/hyperedges")
        );
    }

    #[test]
    fn test_dataset_paths() {
        let layout = StorageLayout::new("/data/grism");
        let snapshot = 1;

        assert_eq!(
            layout.node_dataset_path(snapshot, "Person"),
            PathBuf::from("/data/grism/snapshots/1/nodes/Person.lance")
        );
        assert_eq!(
            layout.hyperedge_dataset_path(snapshot, "KNOWS"),
            PathBuf::from("/data/grism/snapshots/1/hyperedges/KNOWS.lance")
        );
    }

    #[test]
    fn test_sanitize_label() {
        assert_eq!(sanitize_label("Person"), "Person");
        assert_eq!(sanitize_label("my-label"), "my-label");
        assert_eq!(sanitize_label("my label"), "my_label");
        assert_eq!(sanitize_label("a/b\\c"), "a_b_c");
    }
}
