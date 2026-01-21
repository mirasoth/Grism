//! Hypergraph data model.
//!
//! This module provides the core graph primitives:
//! - `Node` for graph nodes
//! - `Edge` for binary edges (property graph compatible)
//! - `Hyperedge` for n-ary hyperedges
//! - `Hypergraph` for the canonical user-facing container

mod container;
mod edge;
mod hyperedge;
mod identifiers;
mod node;
mod properties;

pub use container::{HyperedgeBuilder, Hypergraph, SubgraphView};
pub use edge::Edge;
pub use hyperedge::{Hyperedge, RoleBinding};
pub use identifiers::{
    EdgeId, EntityRef, Label, NodeId, PropertyKey, ROLE_SOURCE, ROLE_TARGET, Role,
};
pub use node::Node;
pub use properties::PropertyMap;
