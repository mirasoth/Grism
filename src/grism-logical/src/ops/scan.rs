//! Scan operator for logical planning (RFC-0002 compliant).
//!
//! Scan is the source operator that produces a Hypergraph from a data source.

use serde::{Deserialize, Serialize};

use crate::expr::LogicalExpr;

/// The kind of entity to scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ScanKind {
    /// Scan nodes.
    Node,
    /// Scan hyperedges.
    Hyperedge,
    /// Scan binary edge projections.
    Edge,
}

impl ScanKind {
    /// Get the name for display.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Node => "Node",
            Self::Hyperedge => "Hyperedge",
            Self::Edge => "Edge",
        }
    }
}

impl std::fmt::Display for ScanKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Scan operator - the source of data in a logical plan.
///
/// # Semantics (RFC-0002, Section 6.1)
///
/// - Produces a Hypergraph containing all hyperedges from the source
/// - Optional predicate restricts by type or metadata
/// - Order is undefined
///
/// # Invariants
///
/// - Deterministic
/// - Side-effect free
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScanOp {
    /// Kind of entity to scan (Node, Hyperedge, or Edge).
    pub kind: ScanKind,

    /// Optional label filter (e.g., "Person", "KNOWS").
    pub label: Option<String>,

    /// Optional namespace/graph name.
    pub namespace: Option<String>,

    /// Optional alias for the scanned entity.
    pub alias: Option<String>,

    /// Optional inline predicate for early filtering.
    pub predicate: Option<LogicalExpr>,

    /// Columns to project from the scan (empty means all).
    pub projection: Vec<String>,
}

impl ScanOp {
    /// Create a new node scan.
    pub const fn nodes() -> Self {
        Self {
            kind: ScanKind::Node,
            label: None,
            namespace: None,
            alias: None,
            predicate: None,
            projection: Vec::new(),
        }
    }

    /// Create a new node scan with a label.
    pub fn nodes_with_label(label: impl Into<String>) -> Self {
        Self {
            kind: ScanKind::Node,
            label: Some(label.into()),
            namespace: None,
            alias: None,
            predicate: None,
            projection: Vec::new(),
        }
    }

    /// Create a new hyperedge scan.
    pub const fn hyperedges() -> Self {
        Self {
            kind: ScanKind::Hyperedge,
            label: None,
            namespace: None,
            alias: None,
            predicate: None,
            projection: Vec::new(),
        }
    }

    /// Create a new hyperedge scan with a label.
    pub fn hyperedges_with_label(label: impl Into<String>) -> Self {
        Self {
            kind: ScanKind::Hyperedge,
            label: Some(label.into()),
            namespace: None,
            alias: None,
            predicate: None,
            projection: Vec::new(),
        }
    }

    /// Create a new edge scan (binary projection).
    pub const fn edges() -> Self {
        Self {
            kind: ScanKind::Edge,
            label: None,
            namespace: None,
            alias: None,
            predicate: None,
            projection: Vec::new(),
        }
    }

    /// Create a new edge scan with a label.
    pub fn edges_with_label(label: impl Into<String>) -> Self {
        Self {
            kind: ScanKind::Edge,
            label: Some(label.into()),
            namespace: None,
            alias: None,
            predicate: None,
            projection: Vec::new(),
        }
    }

    /// Set the label filter.
    #[must_use]
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Set the namespace.
    #[must_use]
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Set the alias.
    #[must_use]
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Set an inline predicate.
    #[must_use]
    pub fn with_predicate(mut self, predicate: LogicalExpr) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Set columns to project.
    #[must_use]
    pub fn with_projection(mut self, columns: Vec<String>) -> Self {
        self.projection = columns;
        self
    }

    /// Get the effective name for this scan (alias or label or kind).
    pub fn effective_name(&self) -> String {
        self.alias.as_ref().map_or_else(
            || {
                self.label
                    .as_ref()
                    .map_or_else(|| self.kind.name().to_string(), std::clone::Clone::clone)
            },
            std::clone::Clone::clone,
        )
    }
}

impl std::fmt::Display for ScanOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Scan({})", self.kind)?;

        if let Some(ref label) = self.label {
            write!(f, ", label={}", label)?;
        }

        if let Some(ref ns) = self.namespace {
            write!(f, ", namespace={}", ns)?;
        }

        if let Some(ref alias) = self.alias {
            write!(f, " AS {}", alias)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_creation() {
        let scan = ScanOp::nodes_with_label("Person")
            .with_alias("p")
            .with_namespace("social");

        assert_eq!(scan.kind, ScanKind::Node);
        assert_eq!(scan.label, Some("Person".to_string()));
        assert_eq!(scan.alias, Some("p".to_string()));
        assert_eq!(scan.effective_name(), "p");
    }

    #[test]
    fn test_scan_display() {
        let scan = ScanOp::nodes_with_label("Person");
        assert_eq!(scan.to_string(), "Scan(Node), label=Person");

        let scan = ScanOp::edges_with_label("KNOWS").with_alias("k");
        assert_eq!(scan.to_string(), "Scan(Edge), label=KNOWS AS k");
    }
}
