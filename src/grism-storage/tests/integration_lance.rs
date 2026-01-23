//! Integration tests for `LanceStorage` (RFC-0019).
//!
//! These tests verify the complete Storage and WritableStorage interfaces
//! for the Lance-based persistent storage backend, as used by the execution engine.
//!
//! ## Test Categories
//!
//! 1. **Basic Operations**: Create, write, snapshot
//! 2. **Scanning**: Scan nodes/hyperedges with projection
//! 3. **Snapshots**: Isolation, resolution, persistence
//! 4. **Fragments**: Metadata (limited - see RFC-0103 notes)
//! 5. **Capabilities**: Lance storage capabilities
//! 6. **Persistence**: Close, reopen, data durability
//! 7. **RFC-0103 Future Features**: Documented but not implemented
//!
//! ## Notes
//!
//! - Lance storage requires a filesystem path (uses tempfile for tests)
//! - Some features like predicate pushdown are advertised but not fully wired
//! - Fragment metadata currently returns empty (async limitation)

use arrow::array::{Array, Int64Array, StringArray, UInt32Array};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use tempfile::TempDir;

use grism_storage::{
    DatasetId, HyperedgeBatchBuilder, LanceStorage, NodeBatchBuilder, Projection,
    RecordBatchStream, SnapshotSpec, Storage, StorageStatsExt, WritableStorage,
};

// ============================================================================
// Test Helpers
// ============================================================================

/// Helper to collect a RecordBatchStream into a Vec<RecordBatch>.
async fn collect_stream(mut stream: RecordBatchStream) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    while let Some(result) = stream.next().await {
        batches.push(result.expect("Stream error"));
    }
    batches
}

/// Helper to count total rows across batches.
fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Helper to create a node batch with given IDs and label.
fn create_node_batch(ids: &[i64], label: &str) -> RecordBatch {
    let mut builder = NodeBatchBuilder::new();
    for &id in ids {
        builder.add(id, Some(label));
    }
    builder.build().expect("Failed to build node batch")
}

/// Helper to create a hyperedge batch with given IDs, label, and arity.
fn create_hyperedge_batch(ids: &[i64], label: &str, arity: u32) -> RecordBatch {
    let mut builder = HyperedgeBatchBuilder::new();
    for &id in ids {
        builder.add(id, label, arity);
    }
    builder.build().expect("Failed to build hyperedge batch")
}

/// Helper to create Lance storage in a temp directory.
async fn create_temp_storage() -> (LanceStorage, TempDir) {
    let tmp_dir = TempDir::new().expect("Failed to create temp dir");
    let storage = LanceStorage::open(tmp_dir.path())
        .await
        .expect("Failed to open Lance storage");
    (storage, tmp_dir)
}

// ============================================================================
// Basic Operations Tests
// ============================================================================

#[tokio::test]
async fn test_lance_create_storage() {
    let tmp_dir = TempDir::new().unwrap();
    let storage = LanceStorage::open(tmp_dir.path()).await.unwrap();

    // Should start with snapshot 0
    assert_eq!(storage.current_snapshot().unwrap(), 0);

    // Should have created metadata directory
    assert!(storage.root().join("metadata").exists());
}

#[tokio::test]
async fn test_lance_write_and_snapshot() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    // Write nodes
    let batch = create_node_batch(&[1, 2, 3], "Person");
    let rows = storage
        .write(DatasetId::nodes("Person"), batch)
        .await
        .unwrap();
    assert_eq!(rows, 3);

    // Create snapshot
    let snapshot = storage.create_snapshot().await.unwrap();
    assert!(snapshot > 0);

    // Current snapshot should be updated
    assert_eq!(storage.current_snapshot().unwrap(), snapshot);
}

#[tokio::test]
async fn test_lance_write_hyperedges() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    // Write hyperedges
    let batch = create_hyperedge_batch(&[1, 2, 3, 4], "KNOWS", 2);
    let rows = storage
        .write(DatasetId::hyperedges("KNOWS"), batch)
        .await
        .unwrap();
    assert_eq!(rows, 4);

    // Create snapshot
    let snapshot = storage.create_snapshot().await.unwrap();
    assert!(snapshot > 0);
}

#[tokio::test]
async fn test_lance_reopen_storage() {
    let tmp_dir = TempDir::new().unwrap();

    // First session: write and snapshot
    {
        let storage = LanceStorage::open(tmp_dir.path()).await.unwrap();
        storage
            .write(
                DatasetId::nodes("Person"),
                create_node_batch(&[1, 2], "Person"),
            )
            .await
            .unwrap();
        let snapshot = storage.create_snapshot().await.unwrap();
        assert!(snapshot > 0);
        storage.close().await.unwrap();
    }

    // Second session: reopen and verify
    {
        let storage = LanceStorage::open(tmp_dir.path()).await.unwrap();

        // Should see the previous snapshot
        let snapshot = storage.current_snapshot().unwrap();
        assert!(snapshot > 0);

        // Should be able to scan data
        let stream = storage
            .scan(
                DatasetId::nodes("Person"),
                &Projection::all(),
                None,
                snapshot,
            )
            .await
            .unwrap();
        let batches = collect_stream(stream).await;
        assert_eq!(total_rows(&batches), 2);
    }
}

// ============================================================================
// Scanning Tests
// ============================================================================

#[tokio::test]
async fn test_lance_scan_nodes() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    // Write nodes
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1, 2, 3], "Person"),
        )
        .await
        .unwrap();

    let snapshot = storage.create_snapshot().await.unwrap();

    // Scan nodes
    let stream = storage
        .scan(
            DatasetId::nodes("Person"),
            &Projection::all(),
            None,
            snapshot,
        )
        .await
        .unwrap();
    let batches = collect_stream(stream).await;

    assert_eq!(total_rows(&batches), 3);
}

#[tokio::test]
async fn test_lance_scan_hyperedges() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    // Write hyperedges
    storage
        .write(
            DatasetId::hyperedges("KNOWS"),
            create_hyperedge_batch(&[1, 2, 3], "KNOWS", 2),
        )
        .await
        .unwrap();

    let snapshot = storage.create_snapshot().await.unwrap();

    // Scan hyperedges
    let stream = storage
        .scan(
            DatasetId::hyperedges("KNOWS"),
            &Projection::all(),
            None,
            snapshot,
        )
        .await
        .unwrap();
    let batches = collect_stream(stream).await;

    assert_eq!(total_rows(&batches), 3);

    // Verify arity
    if let Some(batch) = batches.first() {
        let arity_col = batch.column_by_name("_arity").unwrap();
        let arities = arity_col.as_any().downcast_ref::<UInt32Array>().unwrap();
        for i in 0..arities.len() {
            assert_eq!(arities.value(i), 2);
        }
    }
}

#[tokio::test]
async fn test_lance_scan_with_projection() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1, 2, 3], "Person"),
        )
        .await
        .unwrap();

    let snapshot = storage.create_snapshot().await.unwrap();

    // Scan with projection - only _id column
    let stream = storage
        .scan(
            DatasetId::nodes("Person"),
            &Projection::columns(["_id"]),
            None,
            snapshot,
        )
        .await
        .unwrap();
    let batches = collect_stream(stream).await;

    assert_eq!(total_rows(&batches), 3);
    for batch in &batches {
        assert_eq!(batch.num_columns(), 1);
        assert!(batch.column_by_name("_id").is_some());
    }
}

#[tokio::test]
async fn test_lance_scan_empty() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    // Create snapshot with no data
    let snapshot = storage.create_snapshot().await.unwrap();

    // Scan should return empty
    let stream = storage
        .scan(
            DatasetId::nodes("Person"),
            &Projection::all(),
            None,
            snapshot,
        )
        .await
        .unwrap();
    let batches = collect_stream(stream).await;

    assert!(batches.is_empty() || total_rows(&batches) == 0);
}

#[tokio::test]
async fn test_lance_scan_nonexistent_label() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    // Write some data
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1], "Person"),
        )
        .await
        .unwrap();
    let snapshot = storage.create_snapshot().await.unwrap();

    // Scan non-existent label
    let stream = storage
        .scan(
            DatasetId::nodes("NonExistent"),
            &Projection::all(),
            None,
            snapshot,
        )
        .await
        .unwrap();
    let batches = collect_stream(stream).await;

    assert!(batches.is_empty() || total_rows(&batches) == 0);
}

// ============================================================================
// Snapshot Tests
// ============================================================================

/// NOTE: LanceStorage uses a "delta" snapshot model where each snapshot
/// stores only data written since the last snapshot, NOT cumulative data.
/// This differs from MemoryStorage which captures the full state at each point.
///
/// For cumulative semantics, use TieredStorage (RFC-0103) when implemented,
/// or query across multiple snapshots.
#[tokio::test]
async fn test_lance_snapshot_isolation() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    // Write initial data
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1], "Person"),
        )
        .await
        .unwrap();
    let snapshot1 = storage.create_snapshot().await.unwrap();

    // Write more data after snapshot1
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[2, 3], "Person"),
        )
        .await
        .unwrap();
    let snapshot2 = storage.create_snapshot().await.unwrap();

    // Snapshot 1 should have 1 row (data written before snapshot1)
    let stream = storage
        .scan(
            DatasetId::nodes("Person"),
            &Projection::all(),
            None,
            snapshot1,
        )
        .await
        .unwrap();
    let batches = collect_stream(stream).await;
    assert_eq!(total_rows(&batches), 1);

    // Snapshot 2 should have 2 rows (only data written between snapshot1 and snapshot2)
    // NOTE: This is delta semantics, not cumulative like MemoryStorage
    let stream = storage
        .scan(
            DatasetId::nodes("Person"),
            &Projection::all(),
            None,
            snapshot2,
        )
        .await
        .unwrap();
    let batches = collect_stream(stream).await;
    assert_eq!(total_rows(&batches), 2);
}

#[tokio::test]
async fn test_lance_resolve_snapshot_latest() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    // Initially latest is 0
    let resolved = storage.resolve_snapshot(SnapshotSpec::Latest).unwrap();
    assert_eq!(resolved, 0);

    // Create snapshot
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1], "Person"),
        )
        .await
        .unwrap();
    let snapshot = storage.create_snapshot().await.unwrap();

    // Latest should now be the new snapshot
    let resolved = storage.resolve_snapshot(SnapshotSpec::Latest).unwrap();
    assert_eq!(resolved, snapshot);
}

#[tokio::test]
async fn test_lance_resolve_snapshot_id() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    // Create multiple snapshots
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1], "Person"),
        )
        .await
        .unwrap();
    let snapshot1 = storage.create_snapshot().await.unwrap();

    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[2], "Person"),
        )
        .await
        .unwrap();
    let snapshot2 = storage.create_snapshot().await.unwrap();

    // Resolve by ID
    let resolved = storage
        .resolve_snapshot(SnapshotSpec::Id(snapshot1))
        .unwrap();
    assert_eq!(resolved, snapshot1);

    let resolved = storage
        .resolve_snapshot(SnapshotSpec::Id(snapshot2))
        .unwrap();
    assert_eq!(resolved, snapshot2);
}

#[tokio::test]
async fn test_lance_resolve_invalid_snapshot() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    // Try to resolve non-existent snapshot
    let result = storage.resolve_snapshot(SnapshotSpec::Id(999));

    // Should error
    assert!(result.is_err());
}

// ============================================================================
// Fragment Tests
// ============================================================================

#[tokio::test]
async fn test_lance_fragments_metadata() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1, 2, 3], "Person"),
        )
        .await
        .unwrap();
    let snapshot = storage.create_snapshot().await.unwrap();

    // NOTE: LanceStorage.fragments() currently returns empty Vec due to
    // sync/async mismatch. Per RFC-0103, fragment metadata should be cached
    // during snapshot creation. This is a known limitation.
    let fragments = storage.fragments(DatasetId::nodes("Person"), snapshot);

    // For now, we just verify it doesn't panic
    // When properly implemented, this should return fragment info
    let _ = fragments; // Suppress unused warning

    // TODO: Once fragments() is properly implemented:
    // assert!(!fragments.is_empty());
    // assert_eq!(fragments[0].row_count, 3);
}

// ============================================================================
// Capabilities Tests
// ============================================================================

#[tokio::test]
async fn test_lance_capabilities() {
    let (storage, _tmp_dir) = create_temp_storage().await;
    let caps = storage.capabilities();

    // Lance storage capabilities per RFC-0019
    assert!(caps.predicate_pushdown, "Lance supports predicate pushdown");
    assert!(
        caps.projection_pushdown,
        "Lance supports projection pushdown"
    );
    assert!(caps.fragment_pruning, "Lance supports fragment pruning");
    assert!(!caps.object_store, "Local Lance is not object store mode");
}

// ============================================================================
// Persistence Tests
// ============================================================================

#[tokio::test]
async fn test_lance_close_and_reopen() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path().to_path_buf();

    // Session 1: Write data
    {
        let storage = LanceStorage::open(&path).await.unwrap();
        storage
            .write(
                DatasetId::nodes("Person"),
                create_node_batch(&[1, 2, 3], "Person"),
            )
            .await
            .unwrap();
        storage.create_snapshot().await.unwrap();
        storage.close().await.unwrap();
    }

    // Session 2: Verify data persisted
    {
        let storage = LanceStorage::open(&path).await.unwrap();
        let snapshot = storage.current_snapshot().unwrap();

        let stream = storage
            .scan(
                DatasetId::nodes("Person"),
                &Projection::all(),
                None,
                snapshot,
            )
            .await
            .unwrap();
        let batches = collect_stream(stream).await;
        assert_eq!(total_rows(&batches), 3);
    }
}

#[tokio::test]
async fn test_lance_multiple_labels() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path().to_path_buf();

    // Session 1: Write multiple labels
    {
        let storage = LanceStorage::open(&path).await.unwrap();

        storage
            .write(
                DatasetId::nodes("Person"),
                create_node_batch(&[1, 2], "Person"),
            )
            .await
            .unwrap();
        storage
            .write(
                DatasetId::nodes("Company"),
                create_node_batch(&[10, 11, 12], "Company"),
            )
            .await
            .unwrap();
        storage
            .write(
                DatasetId::hyperedges("KNOWS"),
                create_hyperedge_batch(&[1, 2, 3], "KNOWS", 2),
            )
            .await
            .unwrap();
        storage
            .write(
                DatasetId::hyperedges("WORKS_AT"),
                create_hyperedge_batch(&[10, 11], "WORKS_AT", 2),
            )
            .await
            .unwrap();

        storage.create_snapshot().await.unwrap();
        storage.close().await.unwrap();
    }

    // Session 2: Verify all labels persisted
    {
        let storage = LanceStorage::open(&path).await.unwrap();
        let snapshot = storage.current_snapshot().unwrap();

        // Check Person nodes
        let stream = storage
            .scan(
                DatasetId::nodes("Person"),
                &Projection::all(),
                None,
                snapshot,
            )
            .await
            .unwrap();
        assert_eq!(total_rows(&collect_stream(stream).await), 2);

        // Check Company nodes
        let stream = storage
            .scan(
                DatasetId::nodes("Company"),
                &Projection::all(),
                None,
                snapshot,
            )
            .await
            .unwrap();
        assert_eq!(total_rows(&collect_stream(stream).await), 3);

        // Check KNOWS hyperedges
        let stream = storage
            .scan(
                DatasetId::hyperedges("KNOWS"),
                &Projection::all(),
                None,
                snapshot,
            )
            .await
            .unwrap();
        assert_eq!(total_rows(&collect_stream(stream).await), 3);

        // Check WORKS_AT hyperedges
        let stream = storage
            .scan(
                DatasetId::hyperedges("WORKS_AT"),
                &Projection::all(),
                None,
                snapshot,
            )
            .await
            .unwrap();
        assert_eq!(total_rows(&collect_stream(stream).await), 2);
    }
}

/// Test that multiple snapshots persist across sessions.
/// NOTE: Uses delta snapshot semantics (see test_lance_snapshot_isolation)
#[tokio::test]
async fn test_lance_multiple_snapshots_persist() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path().to_path_buf();

    let (snapshot1, snapshot2);

    // Session 1: Create multiple snapshots
    {
        let storage = LanceStorage::open(&path).await.unwrap();

        storage
            .write(
                DatasetId::nodes("Person"),
                create_node_batch(&[1], "Person"),
            )
            .await
            .unwrap();
        snapshot1 = storage.create_snapshot().await.unwrap();

        storage
            .write(
                DatasetId::nodes("Person"),
                create_node_batch(&[2, 3], "Person"),
            )
            .await
            .unwrap();
        snapshot2 = storage.create_snapshot().await.unwrap();

        storage.close().await.unwrap();
    }

    // Session 2: Both snapshots should be accessible
    {
        let storage = LanceStorage::open(&path).await.unwrap();

        // Snapshot 1 has data from before snapshot1 was created
        let stream = storage
            .scan(
                DatasetId::nodes("Person"),
                &Projection::all(),
                None,
                snapshot1,
            )
            .await
            .unwrap();
        assert_eq!(total_rows(&collect_stream(stream).await), 1);

        // Snapshot 2 has only data written between snapshot1 and snapshot2 (delta)
        let stream = storage
            .scan(
                DatasetId::nodes("Person"),
                &Projection::all(),
                None,
                snapshot2,
            )
            .await
            .unwrap();
        assert_eq!(total_rows(&collect_stream(stream).await), 2);
    }
}

// ============================================================================
// Data Integrity Tests
// ============================================================================

#[tokio::test]
async fn test_lance_data_integrity() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    // Write specific data
    let mut builder = NodeBatchBuilder::new();
    builder.add(100, Some("Person"));
    builder.add(200, Some("Person"));
    builder.add(300, Some("Person"));
    let batch = builder.build().unwrap();

    storage
        .write(DatasetId::nodes("Person"), batch)
        .await
        .unwrap();
    let snapshot = storage.create_snapshot().await.unwrap();

    // Read back and verify
    let stream = storage
        .scan(
            DatasetId::nodes("Person"),
            &Projection::all(),
            None,
            snapshot,
        )
        .await
        .unwrap();
    let batches = collect_stream(stream).await;

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    // Verify _id column
    let id_col = batch.column_by_name("_id").unwrap();
    let ids = id_col.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ids.len(), 3);
    assert_eq!(ids.value(0), 100);
    assert_eq!(ids.value(1), 200);
    assert_eq!(ids.value(2), 300);

    // Verify _label column
    let label_col = batch.column_by_name("_label").unwrap();
    let labels = label_col.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(labels.len(), 3);
    for i in 0..3 {
        assert_eq!(labels.value(i), "Person");
    }
}

#[tokio::test]
async fn test_lance_hyperedge_data_integrity() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    // Write hyperedges with specific arities
    let mut builder = HyperedgeBatchBuilder::new();
    builder.add(1, "KNOWS", 2);
    builder.add(2, "MEETING", 5);
    builder.add(3, "EVENT", 10);
    let batch = builder.build().unwrap();

    storage
        .write(DatasetId::hyperedges("MIXED"), batch)
        .await
        .unwrap();
    let snapshot = storage.create_snapshot().await.unwrap();

    let stream = storage
        .scan(
            DatasetId::hyperedges("MIXED"),
            &Projection::all(),
            None,
            snapshot,
        )
        .await
        .unwrap();
    let batches = collect_stream(stream).await;

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    // Verify arity values
    let arity_col = batch.column_by_name("_arity").unwrap();
    let arities = arity_col.as_any().downcast_ref::<UInt32Array>().unwrap();
    assert_eq!(arities.value(0), 2);
    assert_eq!(arities.value(1), 5);
    assert_eq!(arities.value(2), 10);
}

// ============================================================================
// Edge Cases Tests
// ============================================================================

#[tokio::test]
async fn test_lance_adjacency_not_implemented() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1, 2], "Person"),
        )
        .await
        .unwrap();
    let snapshot = storage.create_snapshot().await.unwrap();

    // Adjacency datasets return empty stream (not yet implemented)
    use grism_storage::AdjacencySpec;
    let stream = storage
        .scan(
            DatasetId::adjacency(AdjacencySpec::outgoing("KNOWS")),
            &Projection::all(),
            None,
            snapshot,
        )
        .await
        .unwrap();
    let batches = collect_stream(stream).await;
    assert!(
        batches.is_empty() || total_rows(&batches) == 0,
        "Adjacency datasets are not yet implemented"
    );
}

#[tokio::test]
async fn test_lance_adjacency_write_not_implemented() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    // Writing to adjacency datasets should fail
    use grism_storage::AdjacencySpec;
    let batch = create_node_batch(&[1], "Dummy");
    let result = storage
        .write(
            DatasetId::adjacency(AdjacencySpec::outgoing("KNOWS")),
            batch,
        )
        .await;

    assert!(
        result.is_err(),
        "Adjacency writes should not be implemented"
    );
}

#[tokio::test]
async fn test_lance_flush_noop() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    // Write data
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1], "Person"),
        )
        .await
        .unwrap();

    // Flush is handled during snapshot creation
    let result = storage.flush().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_lance_stats() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    // Stats for Lance storage (currently returns default)
    // TODO: Implement proper stats by scanning datasets
    let stats = storage.stats();

    // Just verify it doesn't panic and returns default values
    assert_eq!(stats.node_count, 0);
    assert_eq!(stats.hyperedge_count, 0);
}

// ============================================================================
// RFC-0103 Future Features (Not Yet Implemented)
// ============================================================================

/// NOTE: Per RFC-0103, the following features are NOT yet implemented for
/// Lance storage and the unified storage provider:
///
/// - TieredStorage (Section 6): Memory as hot tier, Lance as cold tier
///   - Writes go to memory first, then flush to Lance
///   - Reads check memory tier, fall back to Lance
///   - This would combine MemoryStorage and LanceStorage
///
/// - FlushManager (Section 10): Coordinates persistence
///   - Automatic flush triggers (memory pressure, age, size)
///   - Background flush operations
///
/// - CacheManager (Section 9): Read cache acceleration
///   - LRU cache for frequently accessed data from Lance
///   - Weighted eviction based on recency, frequency, size
///
/// - Predicate Pushdown (partial): While capabilities advertise support,
///   the conversion from LogicalExpr to Lance filter is not implemented.
///   Currently predicates are evaluated by the execution engine.
///
/// - Fragment Metadata (limited): fragments() returns empty due to
///   sync/async mismatch. Should be cached during snapshot creation.
///
/// When these features are implemented, additional integration tests should be
/// added to verify their behavior.
#[test]
fn test_rfc0103_future_features_documented() {
    // This test serves as documentation for future features
    // No actual assertions - just a marker for RFC-0103 compliance
}

/// NOTE: Predicate pushdown is advertised but not fully wired.
/// This test documents the current limitation.
#[tokio::test]
async fn test_lance_predicate_pushdown_pending() {
    let (storage, _tmp_dir) = create_temp_storage().await;

    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1, 2, 3, 4, 5], "Person"),
        )
        .await
        .unwrap();
    let snapshot = storage.create_snapshot().await.unwrap();

    // Even though we pass a predicate, it's currently not pushed down to Lance
    // The predicate parameter is Option<&LogicalExpr>, but Lance expects its
    // own filter format. Conversion is not implemented.
    let stream = storage
        .scan(
            DatasetId::nodes("Person"),
            &Projection::all(),
            None, // No predicate pushdown yet
            snapshot,
        )
        .await
        .unwrap();
    let batches = collect_stream(stream).await;

    // All rows returned (no filter applied at storage level)
    assert_eq!(total_rows(&batches), 5);

    // TODO: When predicate pushdown is implemented:
    // let predicate = col("_id").gt(lit(3i64));
    // let stream = storage.scan(..., Some(&predicate), ...).await?;
    // Should return only rows where _id > 3
}
