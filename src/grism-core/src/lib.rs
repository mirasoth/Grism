//! Core data model for Grism hypergraph database.
//!
//! `grism-core` provides the fundamental types for Grism's AI-native hypergraph
//! data model. It implements the canonical data structures defined in the
//! Grism architecture design.
//!
//! # Key Concepts
//!
//! ## Hypergraph Model
//!
//! Grism uses a **hypergraph-first** data model where:
//!
//! - **[`Node`]** represents atomic entities with stable identity
//! - **[`Hyperedge`]** represents n-ary relations connecting entities via named roles
//! - **[`Edge`]** is a convenience view for binary hyperedges (arity = 2)
//!
//! All relations are represented as hyperedges. Binary edges are simply
//! hyperedges with arity = 2 and roles {source, target}.
//!
//! ## Role Bindings
//!
//! Hyperedges connect entities through **[`RoleBinding`]** structures, where
//! each binding associates a semantic role with a target entity:
//!
//! ```rust
//! use grism_core::{Hyperedge, EntityRef, RoleBinding};
//!
//! // A hyperedge can connect nodes...
//! let edge = Hyperedge::new("AUTHORED_BY")
//!     .with_node(1, "paper")
//!     .with_node(2, "author");
//!
//! // ...or even other hyperedges (meta-relations)
//! let meta = Hyperedge::new("INFERRED_BY")
//!     .with_hyperedge(42, "conclusion")
//!     .with_node(100, "rule");
//! ```
//!
//! ## Type System
//!
//! The type system includes:
//!
//! - **[`Value`]** - Runtime values (Int64, Float64, String, Vector, etc.)
//! - **[`DataType`]** - Type information for schema columns
//!
//! Arrow integration is provided for efficient columnar processing.
//!
//! ## Schema
//!
//! The schema system tracks:
//!
//! - **[`ColumnInfo`]** - Column metadata (name, type, nullability)
//! - **[`EntityInfo`]** - Entity information (labels, aliases)
//! - **[`ColumnRef`]** - Column reference resolution
//!
//! # Example
//!
//! ```rust
//! use grism_core::Hypergraph;
//!
//! let mut hg = Hypergraph::new();
//!
//! // Create nodes
//! let alice = hg.add_node("Person", [("name", "Alice")]);
//! let bob = hg.add_node("Person", [("name", "Bob")]);
//!
//! // Create a binary hyperedge
//! let knows = hg.add_hyperedge("KNOWS")
//!     .with_node(alice, "source")
//!     .with_node(bob, "target")
//!     .build();
//!
//! assert_eq!(hg.node_count(), 2);
//! assert_eq!(hg.hyperedge_count(), 1);
//! ```
//!
//! # Crate Features
//!
//! - `python` - Enable PyO3 bindings for Python integration

pub mod hypergraph;
pub mod schema;
pub mod testing;
pub mod types;

#[cfg(test)]
mod proptest_utils;

// Re-export commonly used types
pub use hypergraph::{
    Edge, EdgeId, EntityRef, Hyperedge, HyperedgeBuilder, Hypergraph, Label, Node, NodeId,
    PropertyKey, PropertyMap, ROLE_SOURCE, ROLE_TARGET, Role, RoleBinding, SubgraphView,
};
pub use schema::{
    ColumnInfo, ColumnRef, EntityInfo, EntityKind, PropertySchema, Schema, SchemaViolation,
};
pub use testing::{HypergraphAssertions, HypergraphFixture};
pub use types::{DataType, Value};
