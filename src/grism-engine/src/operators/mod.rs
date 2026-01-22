//! Physical execution operators.
//!
//! This module contains all physical operators for query execution.
//! Each operator implements the `PhysicalOperator` trait and follows
//! the pull-based execution model.
//!
//! # Operator Categories
//!
//! | Category | Blocking | Examples |
//! |----------|----------|----------|
//! | Source | No | `NodeScanExec`, `HyperedgeScanExec` |
//! | Unary | No | `FilterExec`, `ProjectExec`, `LimitExec` |
//! | Expand | No | `AdjacencyExpandExec`, `RoleExpandExec` |
//! | Blocking | Yes | `HashAggregateExec`, `SortExec` |
//! | Binary | No | `UnionExec` |
//! | Sink | Yes | `CollectExec` |

mod aggregate;
mod collect;
mod empty;
mod expand;
mod filter;
mod limit;
mod project;
mod rename;
mod scan;
mod sort;
mod traits;
mod union;

// Re-export operator trait and utilities
pub use traits::{BoxedPhysicalOperator, OperatorState, PhysicalOperator};

// Re-export operators
pub use aggregate::HashAggregateExec;
pub use collect::CollectExec;
pub use empty::EmptyExec;
pub use expand::{AdjacencyExpandExec, RoleExpandExec};
pub use filter::FilterExec;
pub use limit::LimitExec;
pub use project::ProjectExec;
pub use rename::RenameExec;
pub use scan::{HyperedgeScanExec, NodeScanExec};
pub use sort::SortExec;
pub use union::UnionExec;
