#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::needless_collect)]
//! Schema definition for Grism frames.

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::types::{DataType, Value};

use super::EntityInfo;

/// A violation of schema constraints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaViolation {
    /// An entity declared in the schema is missing from the graph.
    MissingEntity {
        name: String,
        kind: super::EntityKind,
    },
    /// A property value has a different type than declared in the schema.
    TypeMismatch {
        entity: String,
        property: String,
        expected: DataType,
        actual: String,
    },
    /// A property exists on an entity but is not declared in the schema.
    UndeclaredProperty { entity: String, property: String },
    /// A required property is missing from an entity.
    MissingRequiredProperty { entity: String, property: String },
    /// A property is null but the schema declares it as non-nullable.
    NullNotAllowed { entity: String, property: String },
}

impl fmt::Display for SchemaViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEntity { name, kind } => {
                write!(f, "Missing {kind} '{name}' declared in schema")
            }
            Self::TypeMismatch {
                entity,
                property,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "Type mismatch for {entity}.{property}: expected {expected}, got {actual}"
                )
            }
            Self::UndeclaredProperty { entity, property } => {
                write!(f, "Undeclared property '{property}' on entity '{entity}'")
            }
            Self::MissingRequiredProperty { entity, property } => {
                write!(
                    f,
                    "Missing required property '{property}' on entity '{entity}'"
                )
            }
            Self::NullNotAllowed { entity, property } => {
                write!(
                    f,
                    "Property '{entity}.{property}' is null but schema declares it as non-nullable"
                )
            }
        }
    }
}

impl std::error::Error for SchemaViolation {}

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
    #[must_use]
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            qualifier: None,
            data_type,
            nullable: true,
        }
    }

    /// Set the qualifier for this column.
    #[must_use]
    pub fn with_qualifier(mut self, qualifier: impl Into<String>) -> Self {
        self.qualifier = Some(qualifier.into());
        self
    }

    /// Set nullable for this column.
    #[must_use]
    pub const fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Get the full qualified name.
    pub fn qualified_name(&self) -> String {
        self.qualifier
            .as_ref()
            .map_or_else(|| self.name.clone(), |q| format!("{}.{q}", self.name))
    }
}

/// Schema for a single property, defining its type and constraints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropertySchema {
    /// Property name.
    pub name: String,
    /// Expected data type.
    pub data_type: DataType,
    /// Whether this property can be null.
    pub nullable: bool,
    /// Whether this property is required on all entities of this label.
    pub required: bool,
}

impl PropertySchema {
    /// Create a new property schema.
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            required: false,
        }
    }

    /// Set whether this property can be null.
    #[must_use]
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Set whether this property is required.
    #[must_use]
    pub fn with_required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }
}

/// Schema for a frame, tracking columns and entities in scope.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    /// Columns in this schema.
    pub columns: Vec<ColumnInfo>,
    /// Entities (labels/aliases) in scope.
    pub entities: Vec<EntityInfo>,
    /// Property schemas per entity label.
    /// Maps label to `property_name` to `PropertySchema`
    #[serde(default)]
    pub property_schemas: HashMap<String, HashMap<String, PropertySchema>>,
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
            property_schemas: HashMap::new(),
        }
    }

    /// Register a property schema for a given entity label.
    ///
    /// This defines the expected type for a property on entities with the given label.
    ///
    /// # Example
    ///
    /// ```rust
    /// use grism_core::schema::Schema;
    /// use grism_core::types::DataType;
    ///
    /// let mut schema = Schema::new();
    /// schema.register_property("Person", "age", DataType::Int64);
    /// schema.register_property("Person", "name", DataType::String);
    /// ```
    pub fn register_property(
        &mut self,
        label: impl Into<String>,
        property_name: impl Into<String>,
        data_type: DataType,
    ) {
        let label = label.into();
        let property_name = property_name.into();

        let property_schema = PropertySchema::new(property_name.clone(), data_type);

        self.property_schemas
            .entry(label)
            .or_default()
            .insert(property_name, property_schema);
    }

    /// Register a property schema with full configuration.
    pub fn register_property_schema(
        &mut self,
        label: impl Into<String>,
        property_schema: PropertySchema,
    ) {
        let label = label.into();
        let name = property_schema.name.clone();

        self.property_schemas
            .entry(label)
            .or_default()
            .insert(name, property_schema);
    }

    /// Get the property schema for a given label and property name.
    pub fn get_property_schema(&self, label: &str, property_name: &str) -> Option<&PropertySchema> {
        self.property_schemas
            .get(label)
            .and_then(|props| props.get(property_name))
    }

    /// Get all property schemas for a given label.
    pub fn get_properties_for_label(
        &self,
        label: &str,
    ) -> Option<&HashMap<String, PropertySchema>> {
        self.property_schemas.get(label)
    }

    /// Check if a property is defined for a given label.
    pub fn has_property_schema(&self, label: &str, property_name: &str) -> bool {
        self.get_property_schema(label, property_name).is_some()
    }

    /// Validate a property value against the schema.
    ///
    /// Returns `Ok(())` if the property is valid, or a `SchemaViolation` if not.
    ///
    /// # Arguments
    /// * `label` - The entity label (e.g., "Person")
    /// * `property_name` - The property name (e.g., "age")
    /// * `value` - The value to validate
    /// * `strict` - If true, undeclared properties are violations
    ///
    /// # Example
    ///
    /// ```rust
    /// use grism_core::schema::Schema;
    /// use grism_core::types::{DataType, Value};
    ///
    /// let mut schema = Schema::new();
    /// schema.register_property("Person", "age", DataType::Int64);
    ///
    /// // Valid: Int64 value for Int64 property
    /// assert!(schema.validate_property("Person", "age", &Value::Int64(30), false).is_ok());
    ///
    /// // Invalid: String value for Int64 property
    /// assert!(schema.validate_property("Person", "age", &Value::String("thirty".into()), false).is_err());
    /// ```
    pub fn validate_property(
        &self,
        label: &str,
        property_name: &str,
        value: &Value,
        strict: bool,
    ) -> Result<(), SchemaViolation> {
        // Check if property is declared in schema
        let Some(property_schema) = self.get_property_schema(label, property_name) else {
            // In strict mode, undeclared properties are violations
            if strict {
                return Err(SchemaViolation::UndeclaredProperty {
                    entity: label.to_string(),
                    property: property_name.to_string(),
                });
            }
            // In non-strict mode, undeclared properties are allowed
            return Ok(());
        };

        // Check null values
        if value.is_null() {
            if !property_schema.nullable {
                return Err(SchemaViolation::NullNotAllowed {
                    entity: label.to_string(),
                    property: property_name.to_string(),
                });
            }
            return Ok(());
        }

        // Check type compatibility
        if !value_matches_type(value, &property_schema.data_type) {
            return Err(SchemaViolation::TypeMismatch {
                entity: label.to_string(),
                property: property_name.to_string(),
                expected: property_schema.data_type.clone(),
                actual: value.type_name().to_string(),
            });
        }

        Ok(())
    }

    /// Validate all properties of an entity.
    ///
    /// Returns a list of all violations found.
    pub fn validate_properties(
        &self,
        label: &str,
        properties: &crate::hypergraph::PropertyMap,
        strict: bool,
    ) -> Vec<SchemaViolation> {
        let mut violations = Vec::new();

        // Check each property
        for (name, value) in properties {
            if let Err(violation) = self.validate_property(label, name, value, strict) {
                violations.push(violation);
            }
        }

        // Check for missing required properties
        if let Some(property_schemas) = self.get_properties_for_label(label) {
            for (prop_name, prop_schema) in property_schemas {
                if prop_schema.required && !properties.contains_key(prop_name) {
                    violations.push(SchemaViolation::MissingRequiredProperty {
                        entity: label.to_string(),
                        property: prop_name.clone(),
                    });
                }
            }
        }

        violations
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
        self.columns
            .iter()
            .map(ColumnInfo::qualified_name)
            .collect()
    }

    /// Check if the schema is empty.
    pub const fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Get the number of columns.
    pub const fn len(&self) -> usize {
        self.columns.len()
    }

    /// Merge another schema into this one.
    pub fn merge(&mut self, other: &Self) {
        self.columns.extend(other.columns.iter().cloned());
        self.entities.extend(other.entities.iter().cloned());
    }

    /// Create a projection of this schema with only selected columns.
    #[must_use]
    pub fn project(&self, indices: &[usize]) -> Self {
        let columns = indices
            .iter()
            .filter_map(|&i| self.columns.get(i).cloned())
            .collect();

        Self {
            columns,
            entities: Vec::new(), // Entities are not preserved after projection
            property_schemas: HashMap::new(), // Property schemas not preserved after projection
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

/// Check if a Value is compatible with a `DataType`.
///
/// This function checks if a runtime value matches an expected schema type,
/// with support for type coercion (e.g., Int64 -> Float64).
fn value_matches_type(value: &Value, expected: &DataType) -> bool {
    match (value, expected) {
        // Null matches nullable types, exact type matches and type coercion
        (Value::Null, _)
        | (Value::Bool(_), DataType::Bool)
        | (Value::Int64(_), DataType::Int64 | DataType::Float64)
        | (Value::Float64(_), DataType::Float64)
        | (Value::String(_) | Value::Symbol(_), DataType::String)
        | (Value::Binary(_), DataType::Binary)
        | (Value::Symbol(_), DataType::Symbol)
        | (Value::Timestamp(_), DataType::Timestamp)
        | (Value::Date(_), DataType::Date) => true,

        // Vector type: check dimension if specified
        (Value::Vector(v), DataType::Vector(dim)) => *dim == 0 || v.len() == *dim,

        // Array type: elements must match inner type
        (Value::Array(arr), DataType::Array(inner_type)) => {
            arr.iter().all(|elem| value_matches_type(elem, inner_type))
        }

        // Map type: values must match inner type
        (Value::Map(map), DataType::Map(value_type)) => {
            map.values().all(|v| value_matches_type(v, value_type))
        }

        // No match
        _ => false,
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

    #[test]
    fn test_property_schema() {
        let prop = PropertySchema::new("age", DataType::Int64)
            .with_nullable(false)
            .with_required(true);

        assert_eq!(prop.name, "age");
        assert_eq!(prop.data_type, DataType::Int64);
        assert!(!prop.nullable);
        assert!(prop.required);
    }

    #[test]
    fn test_register_property() {
        let mut schema = Schema::new();
        schema.register_property("Person", "age", DataType::Int64);
        schema.register_property("Person", "name", DataType::String);
        schema.register_property("Company", "revenue", DataType::Float64);

        // Check Person properties
        assert!(schema.has_property_schema("Person", "age"));
        assert!(schema.has_property_schema("Person", "name"));
        assert!(!schema.has_property_schema("Person", "email"));

        let age_schema = schema.get_property_schema("Person", "age").unwrap();
        assert_eq!(age_schema.data_type, DataType::Int64);

        // Check Company properties
        assert!(schema.has_property_schema("Company", "revenue"));
        assert!(!schema.has_property_schema("Company", "name"));

        // Check non-existent label
        assert!(!schema.has_property_schema("Animal", "species"));
    }

    #[test]
    fn test_get_properties_for_label() {
        let mut schema = Schema::new();
        schema.register_property("Person", "age", DataType::Int64);
        schema.register_property("Person", "name", DataType::String);

        let person_props = schema.get_properties_for_label("Person").unwrap();
        assert_eq!(person_props.len(), 2);
        assert!(person_props.contains_key("age"));
        assert!(person_props.contains_key("name"));

        assert!(schema.get_properties_for_label("Unknown").is_none());
    }

    #[test]
    fn test_validate_property_type_match() {
        let mut schema = Schema::new();
        schema.register_property("Person", "age", DataType::Int64);
        schema.register_property("Person", "name", DataType::String);
        schema.register_property("Person", "score", DataType::Float64);

        // Valid type matches
        assert!(
            schema
                .validate_property("Person", "age", &Value::Int64(30), false)
                .is_ok()
        );
        assert!(
            schema
                .validate_property("Person", "name", &Value::String("Alice".into()), false)
                .is_ok()
        );
        assert!(
            schema
                .validate_property("Person", "score", &Value::Float64(95.5), false)
                .is_ok()
        );

        // Int64 can coerce to Float64
        assert!(
            schema
                .validate_property("Person", "score", &Value::Int64(95), false)
                .is_ok()
        );
    }

    #[test]
    fn test_validate_property_type_mismatch() {
        let mut schema = Schema::new();
        schema.register_property("Person", "age", DataType::Int64);

        let result =
            schema.validate_property("Person", "age", &Value::String("thirty".into()), false);
        assert!(result.is_err());

        match result.unwrap_err() {
            SchemaViolation::TypeMismatch {
                entity,
                property,
                expected,
                actual,
            } => {
                assert_eq!(entity, "Person");
                assert_eq!(property, "age");
                assert_eq!(expected, DataType::Int64);
                assert_eq!(actual, "String");
            }
            _ => panic!("Expected TypeMismatch violation"),
        }
    }

    #[test]
    fn test_validate_property_undeclared_strict() {
        let schema = Schema::new();

        // Non-strict mode: undeclared properties are allowed
        assert!(
            schema
                .validate_property("Person", "unknown", &Value::Int64(42), false)
                .is_ok()
        );

        // Strict mode: undeclared properties are violations
        let result = schema.validate_property("Person", "unknown", &Value::Int64(42), true);
        assert!(result.is_err());

        match result.unwrap_err() {
            SchemaViolation::UndeclaredProperty { entity, property } => {
                assert_eq!(entity, "Person");
                assert_eq!(property, "unknown");
            }
            _ => panic!("Expected UndeclaredProperty violation"),
        }
    }

    #[test]
    fn test_validate_property_null_handling() {
        let mut schema = Schema::new();

        // Nullable property (default)
        schema.register_property("Person", "nickname", DataType::String);

        // Non-nullable property
        schema.register_property_schema(
            "Person",
            PropertySchema::new("age", DataType::Int64).with_nullable(false),
        );

        // Null is allowed for nullable property
        assert!(
            schema
                .validate_property("Person", "nickname", &Value::Null, false)
                .is_ok()
        );

        // Null is not allowed for non-nullable property
        let result = schema.validate_property("Person", "age", &Value::Null, false);
        assert!(result.is_err());

        match result.unwrap_err() {
            SchemaViolation::NullNotAllowed { entity, property } => {
                assert_eq!(entity, "Person");
                assert_eq!(property, "age");
            }
            _ => panic!("Expected NullNotAllowed violation"),
        }
    }

    #[test]
    fn test_validate_properties_multiple() {
        let mut schema = Schema::new();
        schema.register_property("Person", "age", DataType::Int64);
        schema.register_property_schema(
            "Person",
            PropertySchema::new("name", DataType::String).with_required(true),
        );

        let mut properties = std::collections::HashMap::new();
        properties.insert("age".to_string(), Value::String("thirty".into())); // Type mismatch
        // "name" is missing but required

        let violations = schema.validate_properties("Person", &properties, false);
        assert_eq!(violations.len(), 2);

        // Check for type mismatch
        assert!(violations.iter().any(|v| matches!(
            v,
            SchemaViolation::TypeMismatch { property, .. } if property == "age"
        )));

        // Check for missing required property
        assert!(violations.iter().any(|v| matches!(
            v,
            SchemaViolation::MissingRequiredProperty { property, .. } if property == "name"
        )));
    }

    #[test]
    fn test_validate_vector_type() {
        let mut schema = Schema::new();
        schema.register_property("Entity", "embedding", DataType::Vector(3));

        // Correct dimension
        assert!(
            schema
                .validate_property(
                    "Entity",
                    "embedding",
                    &Value::Vector(vec![1.0, 2.0, 3.0]),
                    false
                )
                .is_ok()
        );

        // Wrong dimension
        let result =
            schema.validate_property("Entity", "embedding", &Value::Vector(vec![1.0, 2.0]), false);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_violation_display() {
        let violation = SchemaViolation::TypeMismatch {
            entity: "Person".into(),
            property: "age".into(),
            expected: DataType::Int64,
            actual: "String".into(),
        };

        let display = violation.to_string();
        assert!(display.contains("Person"));
        assert!(display.contains("age"));
        assert!(display.contains("Int64"));
        assert!(display.contains("String"));
    }
}
