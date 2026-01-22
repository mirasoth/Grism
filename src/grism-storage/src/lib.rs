//! Storage layer for Grism.
//!
//! This crate provides storage backends for Grism hypergraph data:
//!
//! - [`InMemoryStorage`]: Hash-map based storage for testing and small datasets
//! - [`FileStorage`]: JSON file-based storage for production use
//!
//! # Architecture
//!
//! The storage layer follows RFC-0102's design principles:
//! - Thread-safe access via `RwLock`
//! - Async operations for non-blocking I/O
//! - Batch operations for better performance
//! - Snapshot support for MVCC
//!
//! # Example
//!
//! ```rust,ignore
//! use grism_storage::{InMemoryStorage, Storage};
//! use grism_core::hypergraph::Node;
//!
//! let storage = InMemoryStorage::new();
//!
//! // Insert a node
//! let node = Node::new().with_label("Person");
//! storage.insert_node(&node).await?;
//!
//! // Query nodes by label
//! let persons = storage.get_nodes_by_label("Person").await?;
//! ```

mod catalog;
mod snapshot;
mod storage;

pub use catalog::{Catalog, GraphEntry};
pub use snapshot::{Snapshot, SnapshotId};
pub use storage::{FileStorage, InMemoryStorage, Storage, StorageConfig, StorageStats};
