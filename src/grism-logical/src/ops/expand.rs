//! Expand operator for graph traversal.

use serde::{Deserialize, Serialize};

use super::LogicalOp;

/// Direction of edge traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    /// Follow outgoing edges (source -> target).
    Out,
    /// Follow incoming edges (target -> source).
    In,
    /// Follow edges in both directions.
    Both,
}

/// Expand operator - graph traversal (replaces joins).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpandOp {
    /// Input operator.
    pub input: Box<LogicalOp>,
    /// Edge label to traverse (None = any edge).
    pub edge_label: Option<String>,
    /// Target node label filter (None = any label).
    pub to_label: Option<String>,
    /// Traversal direction.
    pub direction: Direction,
    /// Number of hops.
    pub hops: u32,
    /// Alias for the expanded nodes.
    pub alias: Option<String>,
    /// Alias for the edges (to access edge properties).
    pub edge_alias: Option<String>,
}

impl ExpandOp {
    /// Create a new expand operation.
    pub fn new(input: LogicalOp) -> Self {
        Self {
            input: Box::new(input),
            edge_label: None,
            to_label: None,
            direction: Direction::Out,
            hops: 1,
            alias: None,
            edge_alias: None,
        }
    }

    /// Set the edge label filter.
    pub fn with_edge(mut self, label: impl Into<String>) -> Self {
        self.edge_label = Some(label.into());
        self
    }

    /// Set the target node label filter.
    pub fn with_to(mut self, label: impl Into<String>) -> Self {
        self.to_label = Some(label.into());
        self
    }

    /// Set the traversal direction.
    pub fn with_direction(mut self, direction: Direction) -> Self {
        self.direction = direction;
        self
    }

    /// Set the number of hops.
    pub fn with_hops(mut self, hops: u32) -> Self {
        self.hops = hops;
        self
    }

    /// Set the alias for expanded nodes.
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Set the alias for edges.
    pub fn with_edge_alias(mut self, alias: impl Into<String>) -> Self {
        self.edge_alias = Some(alias.into());
        self
    }
}
