//! Column reference resolution.

use serde::{Deserialize, Serialize};

use common_error::{GrismError, GrismResult};

use super::Schema;

/// Reference to a column, possibly qualified by entity name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
    /// Optional qualifier (label, alias, or edge label).
    pub qualifier: Option<String>,
    /// Column/property name.
    pub name: String,
}

impl ColumnRef {
    /// Create a new column reference.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            qualifier: None,
            name: name.into(),
        }
    }

    /// Create a new qualified column reference.
    pub fn qualified(qualifier: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            qualifier: Some(qualifier.into()),
            name: name.into(),
        }
    }

    /// Parse a column reference from a string.
    ///
    /// Supports formats:
    /// - `"column"` -> unqualified
    /// - `"Entity.column"` -> qualified
    pub fn parse(s: &str) -> Self {
        if let Some((qualifier, name)) = s.split_once('.') {
            Self::qualified(qualifier, name)
        } else {
            Self::new(s)
        }
    }

    /// Check if this reference is qualified.
    pub const fn is_qualified(&self) -> bool {
        self.qualifier.is_some()
    }

    /// Get the full display name.
    pub fn display_name(&self) -> String {
        self.qualifier
            .as_ref()
            .map_or_else(|| self.name.clone(), |q| format!("{}.{q}", self.name))
    }

    /// Resolve this column reference against a schema.
    ///
    /// Resolution algorithm:
    /// 1. If qualifier is present:
    ///    - Search for entity matching qualifier
    ///    - Resolve name within that entity's properties
    /// 2. If qualifier is absent:
    ///    - Search all entities in reverse order (most recent first)
    ///    - If exactly one match, return it
    ///    - If multiple matches, return ambiguous column error
    ///    - If no match, return column not found error
    pub fn resolve(&self, schema: &Schema) -> GrismResult<usize> {
        if let Some(ref qualifier) = self.qualifier {
            // Qualified lookup
            let entity = schema.find_entity(qualifier).ok_or_else(|| {
                GrismError::SchemaError(format!("Entity '{qualifier}' not found in schema"))
            })?;

            schema
                .find_column_in_entity(&entity.name, &self.name)
                .ok_or_else(|| {
                    GrismError::ColumnNotFound(format!(
                        "Column '{}' not found in entity '{}'",
                        self.name, qualifier
                    ))
                })
        } else {
            // Unqualified lookup - search all entities
            let matches: Vec<usize> = schema
                .columns
                .iter()
                .enumerate()
                .filter(|(_, col)| col.name == self.name)
                .map(|(i, _)| i)
                .collect();

            match matches.len() {
                0 => Err(GrismError::ColumnNotFound(format!(
                    "Column '{}' not found in schema. Available columns: {}",
                    self.name,
                    schema.column_names().join(", ")
                ))),
                1 => Ok(matches[0]),
                _ => {
                    let qualifiers: Vec<_> = matches
                        .iter()
                        .filter_map(|&i| schema.columns[i].qualifier.clone())
                        .collect();
                    Err(GrismError::AmbiguousColumn(format!(
                        "Column '{}' is ambiguous. Found in: {}. Use qualified name.",
                        self.name,
                        qualifiers.join(", ")
                    )))
                }
            }
        }
    }
}

impl std::fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        let unqualified = ColumnRef::parse("name");
        assert!(!unqualified.is_qualified());
        assert_eq!(unqualified.name, "name");

        let qualified = ColumnRef::parse("Author.name");
        assert!(qualified.is_qualified());
        assert_eq!(qualified.qualifier, Some("Author".to_string()));
        assert_eq!(qualified.name, "name");
    }

    #[test]
    fn test_display() {
        assert_eq!(ColumnRef::new("name").display_name(), "name");
        assert_eq!(
            ColumnRef::qualified("Author", "name").display_name(),
            "Author.name"
        );
    }
}
