//! Core data model for Grism hypergraph database.
//!
//! This crate provides the fundamental types for the hypergraph data model:
//! - `Value` and `DataType` for the type system
//! - `Schema` for column and entity information
//! - `HyperEdge`, `Node`, `Edge` for the graph model

pub mod hypergraph;
pub mod schema;
pub mod types;

// Re-export commonly used types
pub use hypergraph::{Edge, EdgeId, HyperEdge, Label, Node, NodeId, Role};
pub use schema::{ColumnInfo, ColumnRef, EntityInfo, EntityKind, Schema};
pub use types::{DataType, Value};
