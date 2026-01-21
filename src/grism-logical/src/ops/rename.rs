//! Rename operator for logical planning (RFC-0002 compliant).
//!
//! Rename changes column or role names.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Rename operator - column/role renaming.
///
/// # Semantics (RFC-0002, Section 6.5)
///
/// - Renames columns or roles
/// - Purely symbolic (no data transformation)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RenameOp {
    /// Mapping from old names to new names.
    pub mapping: HashMap<String, String>,
}

impl RenameOp {
    /// Create a new rename operation.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a rename with a single mapping.
    pub fn single(from: impl Into<String>, to: impl Into<String>) -> Self {
        let mut mapping = HashMap::new();
        mapping.insert(from.into(), to.into());
        Self { mapping }
    }

    /// Create a rename from a mapping.
    pub fn from_mapping(mapping: HashMap<String, String>) -> Self {
        Self { mapping }
    }

    /// Add a rename mapping.
    pub fn with_rename(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.mapping.insert(from.into(), to.into());
        self
    }

    /// Get the new name for a column (returns original if not renamed).
    pub fn get_new_name<'a>(&'a self, old_name: &'a str) -> &'a str {
        self.mapping.get(old_name).map_or(old_name, String::as_str)
    }

    /// Check if a column is renamed.
    pub fn is_renamed(&self, name: &str) -> bool {
        self.mapping.contains_key(name)
    }

    /// Get all columns that are renamed.
    pub fn renamed_columns(&self) -> impl Iterator<Item = &String> {
        self.mapping.keys()
    }
}

impl std::fmt::Display for RenameOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let renames = self
            .mapping
            .iter()
            .map(|(from, to)| format!("{} -> {}", from, to))
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "Rename({})", renames)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rename_creation() {
        let rename = RenameOp::new()
            .with_rename("old_name", "new_name")
            .with_rename("col_a", "column_a");

        assert_eq!(rename.get_new_name("old_name"), "new_name");
        assert_eq!(rename.get_new_name("col_a"), "column_a");
        assert_eq!(rename.get_new_name("unchanged"), "unchanged");
    }

    #[test]
    fn test_rename_single() {
        let rename = RenameOp::single("foo", "bar");
        assert!(rename.is_renamed("foo"));
        assert!(!rename.is_renamed("bar"));
    }

    #[test]
    fn test_rename_display() {
        let rename = RenameOp::single("a", "b");
        assert!(rename.to_string().contains("a -> b"));
    }
}
