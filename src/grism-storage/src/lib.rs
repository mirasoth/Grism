//! Storage layer for Grism.
//!
//! This crate provides storage backends for Grism hypergraph data:
//!
//! - [`MemoryStorage`]: Arrow-columnar in-memory storage (RFC-0020)
//! - [`LanceStorage`]: Lance-based persistent storage (RFC-0019)
//!
//! # Architecture
//!
//! The storage layer follows RFC-0012's design principles:
//! - Execution-agnostic: Storage does not know about operators or plans
//! - Pull-based: All scans return `RecordBatchStream` for lazy evaluation
//! - Snapshot-isolated: All reads occur against immutable snapshots
//! - Arrow-native: Data is exchanged as Arrow `RecordBatches`
//!
//! # Storage Trait (RFC-0012)
//!
//! All storage backends implement the [`Storage`] trait:
//!
//! ```rust,ignore
//! use grism_storage::{Storage, DatasetId, Projection, SnapshotSpec};
//!
//! // Resolve a snapshot
//! let snapshot = storage.resolve_snapshot(SnapshotSpec::Latest)?;
//!
//! // Scan nodes with a specific label
//! let stream = storage.scan(
//!     DatasetId::nodes("Person"),
//!     &Projection::all(),
//!     None, // no predicate pushdown
//!     snapshot,
//! ).await?;
//!
//! // Process batches
//! while let Some(batch) = stream.next().await {
//!     process(batch?);
//! }
//! ```
//!
//! # Storage Provider (RFC-0103)
//!
//! The [`StorageProvider`] provides a unified entry point for storage:
//!
//! ```rust,ignore
//! use grism_storage::{StorageProvider, StorageConfig, StorageMode};
//!
//! // Create in-memory storage
//! let provider = StorageProvider::new(StorageConfig {
//!     mode: StorageMode::Memory,
//!     ..Default::default()
//! })?;
//!
//! // Or Lance-based persistent storage
//! let provider = StorageProvider::new(StorageConfig {
//!     mode: StorageMode::Lance { path: "./data".into() },
//!     ..Default::default()
//! })?;
//!
//! // Get the storage trait object
//! let storage = provider.storage();
//! ```

// Core modules
mod catalog;
mod provider;
mod snapshot;
mod storage;
mod stream;
mod types;

// Storage implementations
pub mod lance;
pub mod memory;

// Re-exports
pub use catalog::{Catalog, GraphEntry};
pub use snapshot::{Snapshot, SnapshotId};
pub use storage::{Storage, StorageStats, StorageStatsExt, WritableStorage};
pub use stream::{
    MemoryBatchStream, ProjectedBatchStream, RecordBatchStream, RecordBatchStreamExt, empty_stream,
    iter_stream, once_stream, vec_stream,
};
pub use types::{
    AdjacencyDirection, AdjacencySpec, DatasetId, FragmentId, FragmentLocation, FragmentMeta,
    Projection, SnapshotSpec, StorageCaps,
};

// Memory storage
pub use memory::{HyperedgeStore, MemoryStorage, NodeStore};

#[cfg(feature = "test-utils")]
pub use memory::{HyperedgeBatchBuilder, NodeBatchBuilder, NodeBatchBuilderWithProps};

// Lance storage
pub use lance::LanceStorage;

// Provider
pub use provider::{MemoryConfig, StorageConfig, StorageMode, StorageProvider};
