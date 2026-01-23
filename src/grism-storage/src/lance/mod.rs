//! Lance-based persistent storage backend (RFC-0019).
//!
//! This module provides a production-ready persistent storage implementation
//! using Lance datasets on the local filesystem.
//!
//! # Design
//!
//! Per RFC-0019, the Lance backend:
//! - Uses Lance as a columnar persistence format
//! - Provides snapshot-based isolation
//! - Supports predicate and projection pushdown
//! - Maps Grism fragments to Lance fragments
//!
//! # Filesystem Layout
//!
//! ```text
//! <grism_root>/
//! ├── snapshots/
//! │   └── <snapshot_id>/
//! │       ├── nodes/<label>.lance/
//! │       ├── hyperedges/<label>.lance/
//! │       └── adjacency/<spec>.lance/
//! └── metadata/
//!     └── snapshot_index.json
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use grism_storage::{LanceStorage, Storage, DatasetId, Projection, SnapshotSpec};
//!
//! // Open or create storage
//! let storage = LanceStorage::open("./data").await?;
//!
//! // Create a snapshot
//! let snapshot = storage.create_snapshot().await?;
//!
//! // Scan with pushdown
//! let stream = storage.scan(
//!     DatasetId::nodes("Person"),
//!     &Projection::columns(["name", "age"]),
//!     Some(&filter_expr),
//!     snapshot,
//! ).await?;
//! ```

mod layout;
mod snapshot_index;
mod storage;

pub use storage::LanceStorage;
