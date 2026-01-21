//! Schema definition for Grism frames.

use serde::{Deserialize, Serialize};

use crate::types::DataType;

use super::EntityInfo;

/// Information about a column in the schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Entity qualifier (label or alias).
    pub qualifier: Option<String>,
    /// Data type.
    pub data_type: DataType,
    /// Whether this column can contain nulls.
    pub nullable: bool,
}

impl ColumnInfo {
    /// Create a new column info.
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            qualifier: None,
            data_type,
            nullable: true,
        }
    }

    /// Set the qualifier for this column.
    pub fn with_qualifier(mut self, qualifier: impl Into<String>) -> Self {
        self.qualifier = Some(qualifier.into());
        self
    }

    /// Set nullable for this column.
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Get the full qualified name.
    pub fn qualified_name(&self) -> String {
        if let Some(ref q) = self.qualifier {
            format!("{}.{}", q, self.name)
        } else {
            self.name.clone()
        }
    }
}

/// Schema for a frame, tracking columns and entities in scope.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    /// Columns in this schema.
    pub columns: Vec<ColumnInfo>,
    /// Entities (labels/aliases) in scope.
    pub entities: Vec<EntityInfo>,
}

impl Schema {
    /// Create a new empty schema.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a schema with the given columns.
    pub fn with_columns(columns: Vec<ColumnInfo>) -> Self {
        Self {
            columns,
            entities: Vec::new(),
        }
    }

    /// Add a column to the schema.
    pub fn add_column(&mut self, column: ColumnInfo) {
        self.columns.push(column);
    }

    /// Add an entity to the schema.
    pub fn add_entity(&mut self, entity: EntityInfo) {
        // Add entity's columns to schema with qualifier
        for col_name in &entity.columns {
            self.columns.push(ColumnInfo {
                name: col_name.clone(),
                qualifier: Some(entity.name.clone()),
                data_type: DataType::Null, // Type will be resolved later
                nullable: true,
            });
        }
        self.entities.push(entity);
    }

    /// Find an entity by name (label or alias).
    pub fn find_entity(&self, name: &str) -> Option<&EntityInfo> {
        self.entities.iter().find(|e| e.name == name)
    }

    /// Find a column index by name within an entity.
    pub fn find_column_in_entity(&self, entity_name: &str, column_name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.qualifier.as_deref() == Some(entity_name) && c.name == column_name)
    }

    /// Get all column names (unqualified).
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Get all qualified column names.
    pub fn qualified_column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.qualified_name()).collect()
    }

    /// Check if the schema is empty.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Get the number of columns.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Merge another schema into this one.
    pub fn merge(&mut self, other: &Schema) {
        self.columns.extend(other.columns.iter().cloned());
        self.entities.extend(other.entities.iter().cloned());
    }

    /// Create a projection of this schema with only selected columns.
    pub fn project(&self, indices: &[usize]) -> Self {
        let columns = indices
            .iter()
            .filter_map(|&i| self.columns.get(i).cloned())
            .collect();

        Self {
            columns,
            entities: Vec::new(), // Entities are not preserved after projection
        }
    }

    /// Create an empty schema.
    pub fn empty() -> Self {
        Self::new()
    }

    /// Register an entity with the schema.
    pub fn register_entity(&mut self, entity: EntityInfo) {
        self.add_entity(entity);
    }
}

impl std::fmt::Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Schema {{")?;
        for col in &self.columns {
            writeln!(
                f,
                "  {}: {} {}",
                col.qualified_name(),
                col.data_type,
                if col.nullable { "(nullable)" } else { "" }
            )?;
        }
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let mut schema = Schema::new();
        schema.add_column(ColumnInfo::new("id", DataType::Int64).with_nullable(false));
        schema.add_column(ColumnInfo::new("name", DataType::String));

        assert_eq!(schema.len(), 2);
        assert_eq!(schema.column_names(), vec!["id", "name"]);
    }

    #[test]
    fn test_schema_with_entity() {
        let mut schema = Schema::new();
        schema.add_entity(EntityInfo::node(
            "Person",
            vec!["name".to_string(), "age".to_string()],
        ));

        assert!(schema.find_entity("Person").is_some());
        assert!(schema.find_column_in_entity("Person", "name").is_some());
    }

    #[test]
    fn test_schema_projection() {
        let schema = Schema::with_columns(vec![
            ColumnInfo::new("a", DataType::Int64),
            ColumnInfo::new("b", DataType::String),
            ColumnInfo::new("c", DataType::Float64),
        ]);

        let projected = schema.project(&[0, 2]);
        assert_eq!(projected.len(), 2);
        assert_eq!(projected.column_names(), vec!["a", "c"]);
    }
}
