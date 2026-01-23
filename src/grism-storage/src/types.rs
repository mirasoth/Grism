//! Core storage types per RFC-0012.
//!
//! This module defines the fundamental types used by the storage layer:
//! - [`DatasetId`]: Identifies scannable datasets (nodes, hyperedges, adjacency)
//! - [`StorageCaps`]: Storage backend capabilities
//! - [`FragmentMeta`]: Fragment metadata for parallel scanning
//! - [`Projection`]: Column projection specification
//! - [`SnapshotSpec`]: Snapshot resolution specification

use std::fmt;

use serde::{Deserialize, Serialize};

// ============================================================================
// Dataset Identification
// ============================================================================

/// Identifies a scannable dataset in storage.
///
/// Per RFC-0012, storage organizes data into three types of datasets:
/// - Nodes: Entity datasets partitioned by label
/// - Hyperedges: Relation datasets partitioned by label
/// - Adjacency: Topology datasets for graph traversal
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DatasetId {
    /// Node dataset, optionally filtered by label.
    Nodes {
        /// Label filter. None means all nodes.
        label: Option<String>,
    },
    /// Hyperedge dataset, optionally filtered by label.
    Hyperedges {
        /// Label filter. None means all hyperedges.
        label: Option<String>,
    },
    /// Adjacency dataset for graph traversal.
    Adjacency {
        /// Adjacency specification defining the traversal pattern.
        spec: AdjacencySpec,
    },
}

impl DatasetId {
    /// Create a dataset ID for all nodes.
    pub fn all_nodes() -> Self {
        Self::Nodes { label: None }
    }

    /// Create a dataset ID for nodes with a specific label.
    pub fn nodes(label: impl Into<String>) -> Self {
        Self::Nodes {
            label: Some(label.into()),
        }
    }

    /// Create a dataset ID for all hyperedges.
    pub fn all_hyperedges() -> Self {
        Self::Hyperedges { label: None }
    }

    /// Create a dataset ID for hyperedges with a specific label.
    pub fn hyperedges(label: impl Into<String>) -> Self {
        Self::Hyperedges {
            label: Some(label.into()),
        }
    }

    /// Create a dataset ID for adjacency data.
    pub fn adjacency(spec: AdjacencySpec) -> Self {
        Self::Adjacency { spec }
    }
}

impl fmt::Display for DatasetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Nodes { label: Some(l) } => write!(f, "Nodes[{l}]"),
            Self::Nodes { label: None } => write!(f, "Nodes[*]"),
            Self::Hyperedges { label: Some(l) } => write!(f, "Hyperedges[{l}]"),
            Self::Hyperedges { label: None } => write!(f, "Hyperedges[*]"),
            Self::Adjacency { spec } => write!(f, "Adjacency[{spec}]"),
        }
    }
}

// ============================================================================
// Adjacency Specification
// ============================================================================

/// Specifies an adjacency dataset for graph traversal.
///
/// Adjacency datasets materialize topology for efficient traversal.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AdjacencySpec {
    /// Edge label for this adjacency.
    pub edge_label: String,
    /// Direction of traversal.
    pub direction: AdjacencyDirection,
    /// Source role (for n-ary hyperedges).
    pub from_role: Option<String>,
    /// Target role (for n-ary hyperedges).
    pub to_role: Option<String>,
}

impl AdjacencySpec {
    /// Create an outgoing adjacency spec for binary edges.
    pub fn outgoing(edge_label: impl Into<String>) -> Self {
        Self {
            edge_label: edge_label.into(),
            direction: AdjacencyDirection::Outgoing,
            from_role: None,
            to_role: None,
        }
    }

    /// Create an incoming adjacency spec for binary edges.
    pub fn incoming(edge_label: impl Into<String>) -> Self {
        Self {
            edge_label: edge_label.into(),
            direction: AdjacencyDirection::Incoming,
            from_role: None,
            to_role: None,
        }
    }

    /// Create a bidirectional adjacency spec.
    pub fn both(edge_label: impl Into<String>) -> Self {
        Self {
            edge_label: edge_label.into(),
            direction: AdjacencyDirection::Both,
            from_role: None,
            to_role: None,
        }
    }

    /// Set roles for n-ary hyperedge traversal.
    pub fn with_roles(mut self, from_role: impl Into<String>, to_role: impl Into<String>) -> Self {
        self.from_role = Some(from_role.into());
        self.to_role = Some(to_role.into());
        self
    }
}

impl fmt::Display for AdjacencySpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.edge_label, self.direction)?;
        if let (Some(from), Some(to)) = (&self.from_role, &self.to_role) {
            write!(f, "({from}->{to})")?;
        }
        Ok(())
    }
}

/// Direction for adjacency traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AdjacencyDirection {
    /// Follow outgoing edges (source -> target).
    Outgoing,
    /// Follow incoming edges (target -> source).
    Incoming,
    /// Follow edges in both directions.
    Both,
}

impl fmt::Display for AdjacencyDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Outgoing => write!(f, "OUT"),
            Self::Incoming => write!(f, "IN"),
            Self::Both => write!(f, "BOTH"),
        }
    }
}

// ============================================================================
// Storage Capabilities
// ============================================================================

/// Storage backend capabilities per RFC-0012 §5.3.
///
/// Advertises optional features that execution may leverage for optimization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StorageCaps {
    /// Backend supports predicate pushdown.
    pub predicate_pushdown: bool,
    /// Backend supports projection pushdown.
    pub projection_pushdown: bool,
    /// Backend supports fragment-level pruning.
    pub fragment_pruning: bool,
    /// Backend is compatible with object stores (S3, GCS, etc.).
    pub object_store: bool,
}

impl StorageCaps {
    /// Capabilities for in-memory storage (RFC-0020).
    pub fn memory() -> Self {
        Self {
            predicate_pushdown: false,
            projection_pushdown: true,
            fragment_pruning: true,
            object_store: false,
        }
    }

    /// Capabilities for Lance storage (RFC-0019).
    pub fn lance() -> Self {
        Self {
            predicate_pushdown: true,
            projection_pushdown: true,
            fragment_pruning: true,
            object_store: false,
        }
    }

    /// No capabilities (baseline).
    pub fn none() -> Self {
        Self::default()
    }
}

// ============================================================================
// Fragment Metadata
// ============================================================================

/// Fragment identifier.
pub type FragmentId = u64;

/// Fragment metadata per RFC-0012 §5.2.
///
/// A fragment represents a stable, addressable unit of persisted data
/// suitable for parallel scanning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentMeta {
    /// Unique fragment identifier within the dataset.
    pub id: FragmentId,
    /// Number of rows in this fragment.
    pub row_count: usize,
    /// Approximate size in bytes.
    pub byte_size: usize,
    /// Fragment location hint.
    pub location: FragmentLocation,
}

impl FragmentMeta {
    /// Create a new fragment metadata.
    pub fn new(id: FragmentId, row_count: usize, byte_size: usize) -> Self {
        Self {
            id,
            row_count,
            byte_size,
            location: FragmentLocation::Unknown,
        }
    }

    /// Set the fragment location.
    pub fn with_location(mut self, location: FragmentLocation) -> Self {
        self.location = location;
        self
    }
}

/// Fragment location hint for scheduling.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum FragmentLocation {
    /// Location unknown or not applicable.
    #[default]
    Unknown,
    /// Fragment is in memory.
    Memory,
    /// Fragment is on local disk.
    LocalDisk {
        /// Path to the fragment file.
        path: String,
    },
    /// Fragment is in object store.
    ObjectStore {
        /// Object store URI.
        uri: String,
    },
}

// ============================================================================
// Projection Specification
// ============================================================================

/// Column projection specification.
///
/// Specifies which columns to read from storage.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Projection {
    /// Columns to project. Empty means all columns.
    pub columns: Vec<String>,
}

impl Projection {
    /// Project all columns.
    pub fn all() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    /// Project specific columns.
    pub fn columns(cols: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            columns: cols.into_iter().map(Into::into).collect(),
        }
    }

    /// Check if this is a full projection (all columns).
    pub fn is_all(&self) -> bool {
        self.columns.is_empty()
    }

    /// Add a column to the projection.
    pub fn with_column(mut self, col: impl Into<String>) -> Self {
        self.columns.push(col.into());
        self
    }
}

// ============================================================================
// Snapshot Specification
// ============================================================================

/// Snapshot resolution specification.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SnapshotSpec {
    /// Use the latest snapshot.
    #[default]
    Latest,
    /// Use a specific snapshot by ID.
    Id(u64),
    /// Use a snapshot by name/tag.
    Named(String),
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dataset_id_display() {
        assert_eq!(DatasetId::all_nodes().to_string(), "Nodes[*]");
        assert_eq!(DatasetId::nodes("Person").to_string(), "Nodes[Person]");
        assert_eq!(DatasetId::all_hyperedges().to_string(), "Hyperedges[*]");
    }

    #[test]
    fn test_adjacency_spec() {
        let spec = AdjacencySpec::outgoing("KNOWS").with_roles("source", "target");
        assert_eq!(spec.to_string(), "KNOWS:OUT(source->target)");
    }

    #[test]
    fn test_storage_caps() {
        let memory_caps = StorageCaps::memory();
        assert!(!memory_caps.predicate_pushdown);
        assert!(memory_caps.projection_pushdown);

        let lance_caps = StorageCaps::lance();
        assert!(lance_caps.predicate_pushdown);
        assert!(lance_caps.projection_pushdown);
    }

    #[test]
    fn test_projection() {
        let all = Projection::all();
        assert!(all.is_all());

        let specific = Projection::columns(["name", "age"]);
        assert!(!specific.is_all());
        assert_eq!(specific.columns.len(), 2);
    }
}
