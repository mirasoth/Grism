//! Physical schema wrapping Arrow schema with graph metadata.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};

/// Physical schema for operator output.
///
/// Wraps Arrow schema with additional graph metadata such as entity qualifiers.
/// This allows tracking which entity (label or alias) each column belongs to.
#[derive(Debug, Clone)]
pub struct PhysicalSchema {
    /// Arrow schema.
    arrow_schema: SchemaRef,
    /// Entity qualifiers for columns (column_name -> qualifier).
    qualifiers: HashMap<String, String>,
}

impl PhysicalSchema {
    /// Create from Arrow schema.
    pub fn new(arrow_schema: SchemaRef) -> Self {
        Self {
            arrow_schema,
            qualifiers: HashMap::new(),
        }
    }

    /// Create with qualifiers.
    pub fn with_qualifiers(arrow_schema: SchemaRef, qualifiers: HashMap<String, String>) -> Self {
        Self {
            arrow_schema,
            qualifiers,
        }
    }

    /// Create an empty schema.
    pub fn empty() -> Self {
        Self::new(Arc::new(ArrowSchema::empty()))
    }

    /// Get the Arrow schema.
    pub fn arrow_schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }

    /// Get column qualifier.
    pub fn qualifier(&self, column: &str) -> Option<&str> {
        self.qualifiers.get(column).map(|s| s.as_str())
    }

    /// Set qualifier for a column.
    pub fn set_qualifier(&mut self, column: impl Into<String>, qualifier: impl Into<String>) {
        self.qualifiers.insert(column.into(), qualifier.into());
    }

    /// Get field by name.
    pub fn field(&self, name: &str) -> Option<&Field> {
        self.arrow_schema.field_with_name(name).ok()
    }

    /// Get field by index.
    pub fn field_by_index(&self, index: usize) -> Option<&Field> {
        self.arrow_schema.fields().get(index).map(|f| f.as_ref())
    }

    /// Number of columns.
    pub fn num_columns(&self) -> usize {
        self.arrow_schema.fields().len()
    }

    /// Check if schema is empty.
    pub fn is_empty(&self) -> bool {
        self.arrow_schema.fields().is_empty()
    }

    /// Get all field names.
    pub fn field_names(&self) -> Vec<&str> {
        self.arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect()
    }

    /// Get qualified field name.
    pub fn qualified_name(&self, field_name: &str) -> String {
        match self.qualifiers.get(field_name) {
            Some(qualifier) => format!("{}.{}", qualifier, field_name),
            None => field_name.to_string(),
        }
    }

    /// Create a projection of this schema with selected columns.
    pub fn project(&self, indices: &[usize]) -> Self {
        let fields: Vec<Arc<Field>> = indices
            .iter()
            .filter_map(|&i| self.arrow_schema.fields().get(i).cloned())
            .collect();

        let mut qualifiers = HashMap::new();
        for &i in indices {
            if let Some(field) = self.arrow_schema.fields().get(i) {
                if let Some(q) = self.qualifiers.get(field.name()) {
                    qualifiers.insert(field.name().clone(), q.clone());
                }
            }
        }

        Self {
            arrow_schema: Arc::new(ArrowSchema::new(fields)),
            qualifiers,
        }
    }

    /// Merge another schema into this one (for joins/expands).
    pub fn merge(&self, other: &Self) -> Self {
        let mut fields: Vec<Arc<Field>> = self.arrow_schema.fields().iter().cloned().collect();
        fields.extend(other.arrow_schema.fields().iter().cloned());

        let mut qualifiers = self.qualifiers.clone();
        qualifiers.extend(other.qualifiers.clone());

        Self {
            arrow_schema: Arc::new(ArrowSchema::new(fields)),
            qualifiers,
        }
    }
}

impl Default for PhysicalSchema {
    fn default() -> Self {
        Self::empty()
    }
}

impl fmt::Display for PhysicalSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for field in self.arrow_schema.fields() {
            let qualifier = self
                .qualifiers
                .get(field.name())
                .map(|q| format!("{}.", q))
                .unwrap_or_default();
            writeln!(
                f,
                "  {}{}: {} {}",
                qualifier,
                field.name(),
                field.data_type(),
                if field.is_nullable() {
                    "(nullable)"
                } else {
                    ""
                }
            )?;
        }
        Ok(())
    }
}

impl From<SchemaRef> for PhysicalSchema {
    fn from(schema: SchemaRef) -> Self {
        Self::new(schema)
    }
}

impl From<ArrowSchema> for PhysicalSchema {
    fn from(schema: ArrowSchema) -> Self {
        Self::new(Arc::new(schema))
    }
}

/// Builder for creating physical schemas.
#[derive(Debug, Default)]
pub struct PhysicalSchemaBuilder {
    fields: Vec<Field>,
    qualifiers: HashMap<String, String>,
}

impl PhysicalSchemaBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a field.
    pub fn field(mut self, name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        self.fields.push(Field::new(name, data_type, nullable));
        self
    }

    /// Add a field with qualifier.
    pub fn qualified_field(
        mut self,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
        qualifier: impl Into<String>,
    ) -> Self {
        let name = name.into();
        self.fields.push(Field::new(&name, data_type, nullable));
        self.qualifiers.insert(name, qualifier.into());
        self
    }

    /// Add a non-nullable Int64 field (common for IDs).
    pub fn id_field(self, name: impl Into<String>) -> Self {
        self.field(name, DataType::Int64, false)
    }

    /// Add a nullable String field.
    pub fn string_field(self, name: impl Into<String>) -> Self {
        self.field(name, DataType::Utf8, true)
    }

    /// Build the schema.
    pub fn build(self) -> PhysicalSchema {
        PhysicalSchema::with_qualifiers(Arc::new(ArrowSchema::new(self.fields)), self.qualifiers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let schema = PhysicalSchemaBuilder::new()
            .id_field("_id")
            .string_field("name")
            .build();

        assert_eq!(schema.num_columns(), 2);
        assert!(!schema.is_empty());
    }

    #[test]
    fn test_schema_with_qualifiers() {
        let schema = PhysicalSchemaBuilder::new()
            .qualified_field("name", DataType::Utf8, true, "Person")
            .build();

        assert_eq!(schema.qualifier("name"), Some("Person"));
        assert_eq!(schema.qualified_name("name"), "Person.name");
    }

    #[test]
    fn test_schema_projection() {
        let schema = PhysicalSchemaBuilder::new()
            .id_field("a")
            .id_field("b")
            .id_field("c")
            .build();

        let projected = schema.project(&[0, 2]);
        assert_eq!(projected.num_columns(), 2);
        assert_eq!(projected.field_names(), vec!["a", "c"]);
    }

    #[test]
    fn test_schema_merge() {
        let schema1 = PhysicalSchemaBuilder::new()
            .qualified_field("id", DataType::Int64, false, "Person")
            .build();

        let schema2 = PhysicalSchemaBuilder::new()
            .qualified_field("id", DataType::Int64, false, "Company")
            .build();

        let merged = schema1.merge(&schema2);
        assert_eq!(merged.num_columns(), 2);
    }
}
