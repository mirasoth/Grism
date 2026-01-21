//! Scan operator for reading graph elements.

use serde::{Deserialize, Serialize};

/// Kind of scan operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScanKind {
    /// Scan nodes.
    Node,
    /// Scan binary edges.
    Edge,
    /// Scan hyperedges.
    HyperEdge,
}

/// Scan operator - entry point of all plans.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanOp {
    /// Kind of elements to scan.
    pub kind: ScanKind,
    /// Optional label filter.
    pub label: Option<String>,
    /// Optional namespace for multi-tenant scenarios.
    pub namespace: Option<String>,
}

impl ScanOp {
    /// Create a node scan.
    pub fn nodes(label: Option<impl Into<String>>) -> Self {
        Self {
            kind: ScanKind::Node,
            label: label.map(Into::into),
            namespace: None,
        }
    }

    /// Create an edge scan.
    pub fn edges(label: Option<impl Into<String>>) -> Self {
        Self {
            kind: ScanKind::Edge,
            label: label.map(Into::into),
            namespace: None,
        }
    }

    /// Create a hyperedge scan.
    pub fn hyperedges(label: Option<impl Into<String>>) -> Self {
        Self {
            kind: ScanKind::HyperEdge,
            label: label.map(Into::into),
            namespace: None,
        }
    }

    /// Set the namespace.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }
}
