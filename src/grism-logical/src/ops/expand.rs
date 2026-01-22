//! Expand operator for logical planning (RFC-0002 compliant).
//!
//! Expand is the sole traversal primitive in Hypergraph - there is no separate Join operator.
//! It follows role bindings in hyperedges and introduces new roles into scope.

use serde::{Deserialize, Serialize};

use crate::expr::LogicalExpr;

/// Direction of expansion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum Direction {
    /// Follow outgoing edges (source → target).
    #[default]
    Outgoing,
    /// Follow incoming edges (target → source).
    Incoming,
    /// Follow edges in both directions.
    Both,
}

impl Direction {
    /// Get the name for display.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Outgoing => "OUT",
            Self::Incoming => "IN",
            Self::Both => "BOTH",
        }
    }
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Mode of expansion (per RFC-0002, Section 6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum ExpandMode {
    /// Binary expansion for arity=2 hyperedges with {source, target} roles.
    #[default]
    Binary,
    /// Role-qualified traversal over n-ary hyperedges.
    Role,
    /// Materialize hyperedges as first-class outputs.
    MaterializeHyperedge,
}

impl ExpandMode {
    /// Get the name for display.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Binary => "BINARY",
            Self::Role => "ROLE",
            Self::MaterializeHyperedge => "MATERIALIZE",
        }
    }
}

impl std::fmt::Display for ExpandMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Expand operator - traversal over hyperedges.
///
/// # Semantics (RFC-0002, Section 6.2)
///
/// - Follows role bindings in hyperedges
/// - Introduces new roles into scope
/// - Does not alter existing bindings
///
/// # Modes
///
/// - **`BinaryExpand`**: For arity=2 hyperedges with {source, target} roles
/// - **`RoleExpand`**: For role-qualified traversal over n-ary hyperedges
/// - **`MaterializeHyperedge`**: When hyperedges should appear as first-class outputs
///
/// # Rules
///
/// - Expansion preserves hyperedge identity
/// - Role collisions MUST be resolved explicitly
/// - Direction is semantic, not physical
/// - Expand MUST NOT imply adjacency traversal; adjacency is an execution concern
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExpandOp {
    /// Expansion mode.
    pub mode: ExpandMode,

    /// Direction of traversal.
    pub direction: Direction,

    /// Edge label filter (optional).
    pub edge_label: Option<String>,

    /// Target node label filter (optional).
    pub to_label: Option<String>,

    /// Source role (for role-based expansion).
    pub from_role: Option<String>,

    /// Target role (for role-based expansion).
    pub to_role: Option<String>,

    /// Alias for the traversed edge.
    pub edge_alias: Option<String>,

    /// Alias for the target entity.
    pub target_alias: Option<String>,

    /// Number of hops (1 = single hop, 0 = variable length).
    pub hops: HopRange,

    /// Optional inline predicate for the edge.
    pub edge_predicate: Option<LogicalExpr>,

    /// Optional inline predicate for the target.
    pub target_predicate: Option<LogicalExpr>,

    /// Whether to include the path as output.
    pub include_path: bool,
}

/// Range of hops for variable-length expansion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HopRange {
    /// Minimum number of hops.
    pub min: u32,
    /// Maximum number of hops (0 = unlimited).
    pub max: u32,
}

impl Default for HopRange {
    fn default() -> Self {
        Self { min: 1, max: 1 }
    }
}

impl HopRange {
    /// Create a single hop range.
    pub const fn single() -> Self {
        Self { min: 1, max: 1 }
    }

    /// Create a variable length range.
    pub const fn range(min: u32, max: u32) -> Self {
        Self { min, max }
    }

    /// Create an unlimited range (min to infinity).
    pub const fn unbounded(min: u32) -> Self {
        Self { min, max: 0 }
    }

    /// Check if this is a single hop.
    pub const fn is_single(&self) -> bool {
        self.min == 1 && self.max == 1
    }

    /// Check if this is variable length.
    pub const fn is_variable(&self) -> bool {
        self.min != self.max || self.max == 0
    }
}

impl std::fmt::Display for HopRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_single() {
            write!(f, "1")
        } else if self.max == 0 {
            if self.min == 0 {
                write!(f, "*")
            } else {
                write!(f, "{}+", self.min)
            }
        } else if self.min == self.max {
            write!(f, "{}", self.min)
        } else {
            write!(f, "{}..{}", self.min, self.max)
        }
    }
}

impl ExpandOp {
    /// Create a new binary expand operation.
    pub const fn binary() -> Self {
        Self {
            mode: ExpandMode::Binary,
            direction: Direction::Outgoing,
            edge_label: None,
            to_label: None,
            from_role: None,
            to_role: None,
            edge_alias: None,
            target_alias: None,
            hops: HopRange::single(),
            edge_predicate: None,
            target_predicate: None,
            include_path: false,
        }
    }

    /// Create a role-based expand operation.
    pub fn role(from_role: impl Into<String>, to_role: impl Into<String>) -> Self {
        Self {
            mode: ExpandMode::Role,
            direction: Direction::Outgoing,
            edge_label: None,
            to_label: None,
            from_role: Some(from_role.into()),
            to_role: Some(to_role.into()),
            edge_alias: None,
            target_alias: None,
            hops: HopRange::single(),
            edge_predicate: None,
            target_predicate: None,
            include_path: false,
        }
    }

    /// Create an expand that materializes hyperedges.
    pub const fn materialize() -> Self {
        Self {
            mode: ExpandMode::MaterializeHyperedge,
            direction: Direction::Outgoing,
            edge_label: None,
            to_label: None,
            from_role: None,
            to_role: None,
            edge_alias: None,
            target_alias: None,
            hops: HopRange::single(),
            edge_predicate: None,
            target_predicate: None,
            include_path: false,
        }
    }

    /// Set direction.
    #[must_use]
    pub const fn with_direction(mut self, direction: Direction) -> Self {
        self.direction = direction;
        self
    }

    /// Set edge label filter.
    #[must_use]
    pub fn with_edge_label(mut self, label: impl Into<String>) -> Self {
        self.edge_label = Some(label.into());
        self
    }

    /// Set target node label filter.
    #[must_use]
    pub fn with_to_label(mut self, label: impl Into<String>) -> Self {
        self.to_label = Some(label.into());
        self
    }

    /// Set hop range.
    #[must_use]
    pub const fn with_hops(mut self, hops: HopRange) -> Self {
        self.hops = hops;
        self
    }

    /// Set edge alias.
    #[must_use]
    pub fn with_edge_alias(mut self, alias: impl Into<String>) -> Self {
        self.edge_alias = Some(alias.into());
        self
    }

    /// Set target alias.
    #[must_use]
    pub fn with_target_alias(mut self, alias: impl Into<String>) -> Self {
        self.target_alias = Some(alias.into());
        self
    }

    /// Set edge predicate.
    #[must_use]
    pub fn with_edge_predicate(mut self, predicate: LogicalExpr) -> Self {
        self.edge_predicate = Some(predicate);
        self
    }

    /// Set target predicate.
    #[must_use]
    pub fn with_target_predicate(mut self, predicate: LogicalExpr) -> Self {
        self.target_predicate = Some(predicate);
        self
    }

    /// Enable path output.
    #[must_use]
    pub const fn with_path(mut self) -> Self {
        self.include_path = true;
        self
    }
}

impl std::fmt::Display for ExpandOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Expand({}, {})", self.mode, self.direction)?;

        if let Some(ref label) = self.edge_label {
            write!(f, ", edge={label}")?;
        }

        if let Some(ref label) = self.to_label {
            write!(f, ", to={label}")?;
        }

        if !self.hops.is_single() {
            write!(f, ", hops={}", self.hops)?;
        }

        if let Some(ref alias) = self.target_alias {
            write!(f, " AS {alias}")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_creation() {
        let expand = ExpandOp::binary()
            .with_direction(Direction::Outgoing)
            .with_edge_label("KNOWS")
            .with_target_alias("friend");

        assert_eq!(expand.mode, ExpandMode::Binary);
        assert_eq!(expand.direction, Direction::Outgoing);
        assert_eq!(expand.edge_label, Some("KNOWS".to_string()));
        assert_eq!(expand.target_alias, Some("friend".to_string()));
    }

    #[test]
    fn test_role_expand() {
        let expand = ExpandOp::role("paper", "author").with_edge_label("AUTHORED_BY");

        assert_eq!(expand.mode, ExpandMode::Role);
        assert_eq!(expand.from_role, Some("paper".to_string()));
        assert_eq!(expand.to_role, Some("author".to_string()));
    }

    #[test]
    fn test_hop_range() {
        assert!(HopRange::single().is_single());
        assert!(!HopRange::single().is_variable());

        assert!(!HopRange::range(1, 3).is_single());
        assert!(HopRange::range(1, 3).is_variable());

        assert_eq!(HopRange::single().to_string(), "1");
        assert_eq!(HopRange::range(1, 3).to_string(), "1..3");
        assert_eq!(HopRange::unbounded(1).to_string(), "1+");
    }

    #[test]
    fn test_expand_display() {
        let expand = ExpandOp::binary()
            .with_edge_label("KNOWS")
            .with_target_alias("friend");

        assert_eq!(
            expand.to_string(),
            "Expand(BINARY, OUT), edge=KNOWS AS friend"
        );
    }
}
