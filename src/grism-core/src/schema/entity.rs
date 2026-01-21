//! Entity tracking for schema scoping.

use serde::{Deserialize, Serialize};

/// Kind of entity in the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntityKind {
    /// Node entity.
    Node,
    /// Binary edge entity.
    Edge,
    /// N-ary hyperedge entity.
    HyperEdge,
}

impl std::fmt::Display for EntityKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Node => write!(f, "Node"),
            Self::Edge => write!(f, "Edge"),
            Self::HyperEdge => write!(f, "HyperEdge"),
        }
    }
}

/// Information about an entity (label/alias) in scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityInfo {
    /// Name of the entity (label or alias).
    pub name: String,
    /// Kind of entity.
    pub kind: EntityKind,
    /// Available property names.
    pub columns: Vec<String>,
    /// Whether this is an alias (vs. a label).
    pub is_alias: bool,
}

impl EntityInfo {
    /// Create a new entity info for a node.
    pub fn node(name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            kind: EntityKind::Node,
            columns,
            is_alias: false,
        }
    }

    /// Create a new entity info for an edge.
    pub fn edge(name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            kind: EntityKind::Edge,
            columns,
            is_alias: false,
        }
    }

    /// Create a new entity info for a hyperedge.
    pub fn hyperedge(name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            kind: EntityKind::HyperEdge,
            columns,
            is_alias: false,
        }
    }

    /// Create an alias for this entity.
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.name = alias.into();
        self.is_alias = true;
        self
    }

    /// Check if this entity has a column.
    pub fn has_column(&self, name: &str) -> bool {
        self.columns.iter().any(|c| c == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_creation() {
        let entity = EntityInfo::node("Person", vec!["name".to_string(), "age".to_string()]);
        assert_eq!(entity.kind, EntityKind::Node);
        assert!(entity.has_column("name"));
        assert!(!entity.has_column("email"));
    }

    #[test]
    fn test_entity_alias() {
        let entity = EntityInfo::node("Person", vec!["name".to_string()]).with_alias("author");
        assert_eq!(entity.name, "author");
        assert!(entity.is_alias);
    }
}
