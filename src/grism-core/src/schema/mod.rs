//! Schema system for Grism.
//!
//! This module provides:
//! - `Schema` for tracking available columns and their types
//! - `ColumnRef` for qualified and unqualified column references
//! - `EntityInfo` for tracking labels and aliases in scope
//! - `PropertySchema` for defining property type constraints

mod column_ref;
mod entity;
mod schema_impl;

pub use column_ref::ColumnRef;
pub use entity::{EntityInfo, EntityKind};
pub use schema_impl::{ColumnInfo, PropertySchema, Schema, SchemaViolation};
