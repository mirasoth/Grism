# Storage Engine Milestone

**Status**: Completed  
**Date**: 2026-01-23  
**RFCs**: RFC-0012, RFC-0019, RFC-0020, RFC-0103

## Overview

This milestone makes `grism-storage` production-ready with Memory and Lance storage backends, integrated with `grism-engine` via the RFC-0012 Storage trait.

## Completed Deliverables

### 1. RFC-0012 Storage Trait

Core abstractions for unified storage access:

```rust
pub trait Storage: Send + Sync {
    fn scan(&self, dataset: DatasetId, projection: &Projection, 
            predicate: Option<&LogicalExpr>, snapshot: SnapshotId) 
            -> impl Future<Output = GrismResult<RecordBatchStream>>;
    fn resolve_snapshot(&self, spec: SnapshotSpec) -> GrismResult<SnapshotId>;
    fn capabilities(&self) -> StorageCaps;
    fn fragments(&self, dataset: DatasetId, snapshot: SnapshotId) -> Vec<FragmentMeta>;
    fn current_snapshot(&self) -> GrismResult<SnapshotId>;
}

pub trait WritableStorage: Storage {
    fn write(&self, dataset: DatasetId, batch: RecordBatch) 
            -> impl Future<Output = GrismResult<usize>>;
    fn create_snapshot(&self) -> impl Future<Output = GrismResult<SnapshotId>>;
    fn close(&self) -> impl Future<Output = GrismResult<()>>;
}
```

Supporting types:
- `DatasetId` - identifies nodes, hyperedges, or adjacency datasets
- `Projection` - column selection for scans
- `SnapshotSpec` - snapshot resolution (Latest, Specific, At timestamp)
- `StorageCaps` - capability flags (streaming, predicate pushdown, etc.)
- `FragmentMeta` - fragment metadata for planning
- `RecordBatchStream` - pull-based Arrow batch stream

### 2. MemoryStorage (RFC-0020)

In-memory Arrow-columnar storage:

- **Location**: `src/grism-storage/src/memory/`
- **Features**:
  - Non-persistent, low-latency storage
  - Arrow `RecordBatch` native storage
  - Snapshot isolation via copy-on-write
  - Thread-safe with `tokio::sync::RwLock`
  - Label-partitioned node and hyperedge stores

### 3. LanceStorage (RFC-0019)

Lance-based persistent storage:

- **Location**: `src/grism-storage/src/lance/`
- **Features**:
  - Filesystem layout: `{root}/snapshots/{id}/{nodes,hyperedges,adjacency}/`
  - Lance dataset per label
  - Snapshot index with JSON persistence
  - Projection pushdown to Lance scanner
  - Arrow 56.0 / Lance 1.0.1 compatibility

### 4. StorageProvider (RFC-0103)

Unified entry point for storage:

- **Location**: `src/grism-storage/src/provider.rs`
- **Features**:
  - Single `Arc<dyn Storage>` interface regardless of backend
  - Memory and Lance mode configuration
  - Lifecycle management (open, close, ready states)
  - Memory usage tracking

```rust
// Memory mode
let provider = StorageProvider::new(StorageConfig::memory()).await?;

// Lance mode  
let provider = StorageProvider::new(StorageConfig::lance("/data/grism")).await?;

// Access storage
let storage: Arc<dyn Storage> = provider.storage();
```

### 5. grism-engine Integration

Updated scan operators to use RFC-0012 interface:

- **`NodeScanExec`**: Uses `Storage::scan(DatasetId::nodes(...))` 
- **`HyperedgeScanExec`**: Uses `Storage::scan(DatasetId::hyperedges(...))`
- **`ScanState`**: Changed from buffering entities to streaming `RecordBatchStream`

```rust
// Before (old interface)
let nodes = ctx.storage.get_nodes_by_label(label).await?;

// After (RFC-0012)
let stream = ctx.storage.scan(
    DatasetId::nodes(label),
    &Projection::all(),
    None,
    ctx.snapshot
).await?;
```

## Known Limitations

### Expand Operators (Stubbed)

`AdjacencyExpandExec` and `RoleExpandExec` return `not_implemented` error:

```rust
Err(GrismError::not_implemented(
    "AdjacencyExpandExec requires RFC-0012 adjacency dataset support"
))
```

**Reason**: These operators require adjacency dataset support (`DatasetId::Adjacency`) with efficient node-to-edge lookups. Current implementation only supports node and hyperedge scans.

**Future Work**: Implement adjacency index materialization and `Storage::scan()` for `DatasetId::Adjacency`.

### Predicate Pushdown

Lance scanner supports projection pushdown but predicate pushdown is not yet implemented:

```rust
// TODO: Convert LogicalExpr to Lance filter format
// For now, predicates are applied post-scan
```

## Test Coverage

| Crate | Tests | Status |
|-------|-------|--------|
| grism-storage | 44 | ✅ Pass |
| grism-engine (unit) | 99 | ✅ Pass |
| grism-engine (integration) | 33 | ✅ Pass |
| grism-engine (unit_tests) | 10 | ✅ Pass |

## Dependencies

- Arrow: 56.0
- Lance: 1.0.1
- Tokio: async runtime
- Futures: stream utilities

## File Structure

```
src/grism-storage/
├── lib.rs              # Public exports
├── storage.rs          # Storage, WritableStorage traits
├── types.rs            # DatasetId, Projection, StorageCaps, etc.
├── stream.rs           # RecordBatchStream utilities
├── snapshot.rs         # SnapshotId type
├── catalog.rs          # Dataset catalog
├── provider.rs         # StorageProvider (RFC-0103)
├── memory/
│   ├── mod.rs
│   ├── storage.rs      # MemoryStorage implementation
│   └── stores.rs       # NodeStore, HyperedgeStore
└── lance/
    ├── mod.rs
    ├── storage.rs      # LanceStorage implementation
    ├── layout.rs       # Filesystem layout
    └── snapshot_index.rs # Snapshot metadata
```

## Usage Examples

### Creating Storage

```rust
use grism_storage::{StorageProvider, StorageConfig, DatasetId, Projection};

// Memory mode
let provider = StorageProvider::new(StorageConfig::memory()).await?;

// Write data
let mut builder = NodeBatchBuilder::new();
builder.add(1, Some("Person"));
builder.add(2, Some("Person"));
provider.storage().write(DatasetId::nodes("Person"), builder.build()?).await?;

// Create snapshot
let snapshot = provider.storage().create_snapshot().await?;

// Scan data
let stream = provider.storage().scan(
    DatasetId::nodes("Person"),
    &Projection::all(),
    None,
    snapshot
).await?;
```

### Using with grism-engine

```rust
use grism_engine::{ExecutionContext, NodeScanExec, PhysicalOperator};
use grism_storage::{MemoryStorage, SnapshotId};

let storage = Arc::new(MemoryStorage::new());
// ... write data ...

let ctx = ExecutionContext::new(storage.clone(), SnapshotId::default());
let scan = NodeScanExec::with_label("Person");

scan.open(&ctx).await?;
while let Some(batch) = scan.next().await? {
    println!("Got {} rows", batch.num_rows());
}
scan.close().await?;
```

## Next Steps

1. **Adjacency Dataset Support**: Implement `DatasetId::Adjacency` scanning for expand operators
2. **Predicate Pushdown**: Convert `LogicalExpr` to Lance filter format
3. **Benchmarks**: Compare Memory vs Lance performance
4. **TieredStorage**: Implement memory + Lance tiered caching (future RFC)
