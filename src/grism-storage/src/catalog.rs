//! Catalog for managing graph schemas and metadata.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use grism_core::Schema;

/// Catalog entry for a graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphEntry {
    /// Graph name.
    pub name: String,
    /// Node schema.
    pub node_schema: Schema,
    /// Edge schema.
    pub edge_schema: Schema,
    /// Hyperedge schema.
    pub hyperedge_schema: Schema,
    /// Creation timestamp.
    pub created_at: i64,
}

/// Catalog for managing graph schemas.
#[derive(Debug, Default)]
pub struct Catalog {
    /// Graphs by name.
    graphs: HashMap<String, GraphEntry>,
    /// Default namespace.
    default_namespace: Option<String>,
}

impl Catalog {
    /// Create a new empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the default namespace.
    pub fn with_default_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.default_namespace = Some(namespace.into());
        self
    }

    /// Register a graph.
    pub fn register(&mut self, name: impl Into<String>, entry: GraphEntry) {
        self.graphs.insert(name.into(), entry);
    }

    /// Get a graph by name.
    pub fn get(&self, name: &str) -> Option<&GraphEntry> {
        self.graphs.get(name)
    }

    /// Remove a graph.
    pub fn remove(&mut self, name: &str) -> Option<GraphEntry> {
        self.graphs.remove(name)
    }

    /// List all graph names.
    pub fn list(&self) -> Vec<&str> {
        self.graphs.keys().map(String::as_str).collect()
    }

    /// Check if a graph exists.
    pub fn exists(&self, name: &str) -> bool {
        self.graphs.contains_key(name)
    }

    /// Get the default namespace.
    pub fn default_namespace(&self) -> Option<&str> {
        self.default_namespace.as_deref()
    }
}

impl GraphEntry {
    /// Create a new graph entry.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            node_schema: Schema::new(),
            edge_schema: Schema::new(),
            hyperedge_schema: Schema::new(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        }
    }

    /// Set the node schema.
    pub fn with_node_schema(mut self, schema: Schema) -> Self {
        self.node_schema = schema;
        self
    }

    /// Set the edge schema.
    pub fn with_edge_schema(mut self, schema: Schema) -> Self {
        self.edge_schema = schema;
        self
    }

    /// Set the hyperedge schema.
    pub fn with_hyperedge_schema(mut self, schema: Schema) -> Self {
        self.hyperedge_schema = schema;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_operations() {
        let mut catalog = Catalog::new();

        let entry = GraphEntry::new("my_graph");
        catalog.register("my_graph", entry);

        assert!(catalog.exists("my_graph"));
        assert!(!catalog.exists("other_graph"));
        assert_eq!(catalog.list(), vec!["my_graph"]);
    }
}
