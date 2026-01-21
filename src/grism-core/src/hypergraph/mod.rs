//! Hypergraph data model.
//!
//! This module provides the core graph primitives:
//! - `Node` for graph nodes
//! - `Edge` for binary edges (property graph compatible)
//! - `HyperEdge` for n-ary hyperedges

mod edge;
mod hyperedge;
mod identifiers;
mod node;
mod properties;

pub use edge::Edge;
pub use hyperedge::HyperEdge;
pub use identifiers::{EdgeId, Label, NodeId, Role};
pub use node::Node;
pub use properties::PropertyMap;
