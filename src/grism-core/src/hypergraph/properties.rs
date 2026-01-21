//! Property storage for graph elements.
//!
//! This module provides the `PropertyMap` type and `HasProperties` trait
//! for managing key-value properties on graph entities (nodes, hyperedges).

use std::collections::HashMap;

use crate::types::Value;

/// Map of property names to values.
pub type PropertyMap = HashMap<String, Value>;

/// Trait for elements that have properties.
pub trait HasProperties {
    /// Get the property map.
    fn properties(&self) -> &PropertyMap;

    /// Get a mutable reference to the property map.
    fn properties_mut(&mut self) -> &mut PropertyMap;

    /// Get a property value by name.
    fn get_property(&self, name: &str) -> Option<&Value> {
        self.properties().get(name)
    }

    /// Set a property value.
    fn set_property(&mut self, name: impl Into<String>, value: Value) {
        self.properties_mut().insert(name.into(), value);
    }

    /// Remove a property.
    fn remove_property(&mut self, name: &str) -> Option<Value> {
        self.properties_mut().remove(name)
    }

    /// Check if a property exists.
    fn has_property(&self, name: &str) -> bool {
        self.properties().contains_key(name)
    }
}
