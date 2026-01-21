//! Snapshot management for MVCC.

use serde::{Deserialize, Serialize};

/// Snapshot identifier.
pub type SnapshotId = u64;

/// A snapshot represents a point-in-time view of the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Unique snapshot ID.
    pub id: SnapshotId,
    /// Timestamp when the snapshot was created (Unix epoch millis).
    pub timestamp: i64,
    /// Parent snapshot ID (for branching).
    pub parent: Option<SnapshotId>,
    /// Optional snapshot name/tag.
    pub name: Option<String>,
}

impl Snapshot {
    /// Create a new snapshot with auto-generated ID.
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(1);

        Self {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            parent: None,
            name: None,
        }
    }

    /// Create a child snapshot.
    pub fn child(&self) -> Self {
        let mut snapshot = Self::new();
        snapshot.parent = Some(self.id);
        snapshot
    }

    /// Set a name for this snapshot.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

impl Default for Snapshot {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_creation() {
        let snapshot = Snapshot::new();
        assert!(snapshot.id > 0);
        assert!(snapshot.timestamp > 0);
        assert!(snapshot.parent.is_none());
    }

    #[test]
    fn test_snapshot_child() {
        let parent = Snapshot::new();
        let child = parent.child();

        assert_eq!(child.parent, Some(parent.id));
        assert!(child.id > parent.id);
    }
}
