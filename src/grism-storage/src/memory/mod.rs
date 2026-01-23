//! In-memory storage backend (RFC-0020).
//!
//! This module provides a non-persistent, low-latency storage implementation
//! that stores data as Arrow `RecordBatches` in memory.
//!
//! # Design
//!
//! Per RFC-0020, the in-memory backend:
//! - Is semantically equivalent to persistent backends
//! - Uses Arrow-columnar storage (`Vec<RecordBatch>`)
//! - Supports snapshot isolation
//! - Provides fragment metadata for parallel scanning
//!
//! # Usage
//!
//! ```rust,ignore
//! use grism_storage::{MemoryStorage, Storage, DatasetId, Projection, SnapshotSpec};
//!
//! let storage = MemoryStorage::new();
//!
//! // Write some data
//! storage.write(DatasetId::nodes("Person"), batch).await?;
//!
//! // Create a snapshot
//! let snapshot_id = storage.create_snapshot().await?;
//!
//! // Scan the data
//! let stream = storage.scan(
//!     DatasetId::nodes("Person"),
//!     &Projection::all(),
//!     None,
//!     snapshot_id,
//! ).await?;
//! ```

mod storage;
mod stores;

pub use storage::MemoryStorage;
pub use stores::{HyperedgeBatchBuilder, HyperedgeStore, NodeBatchBuilder, NodeStore};
