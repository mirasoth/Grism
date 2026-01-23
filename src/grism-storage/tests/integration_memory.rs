//! Integration tests for `MemoryStorage` (RFC-0020).
//!
//! These tests verify the complete Storage and WritableStorage interfaces
//! for the in-memory storage backend, as used by the execution engine.
//!
//! ## Test Categories
//!
//! 1. **Basic Operations**: Empty storage, write nodes/hyperedges
//! 2. **Scanning**: Scan by label, all, with projection
//! 3. **Snapshots**: Isolation, multiple snapshots, resolution
//! 4. **Fragments**: Metadata verification
//! 5. **Capabilities**: Memory storage capabilities
//! 6. **Stats**: Storage statistics
//! 7. **Edge Cases**: Unimplemented features, error handling

use arrow::array::{Array, Int64Array, StringArray, UInt32Array};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;

use grism_storage::{
    DatasetId, FragmentLocation, HyperedgeBatchBuilder, MemoryStorage, NodeBatchBuilder,
    Projection, RecordBatchStream, SnapshotSpec, Storage, StorageStatsExt, WritableStorage,
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

// ============================================================================
// Basic Operations Tests
// ============================================================================

#[tokio::test]
async fn test_memory_empty_storage() {
    let storage = MemoryStorage::new();

    // New storage should start with snapshot 0 (working state)
    let snapshot = storage.current_snapshot().unwrap();
    assert_eq!(snapshot, 0);

    // Resolve latest should return 0
    let resolved = storage.resolve_snapshot(SnapshotSpec::Latest).unwrap();
    assert_eq!(resolved, 0);

    // No labels yet
    assert_eq!(storage.node_label_count().await, 0);
    assert_eq!(storage.hyperedge_label_count().await, 0);
}

#[tokio::test]
async fn test_memory_write_nodes() {
    let storage = MemoryStorage::new();

    // Write Person nodes
    let batch = create_node_batch(&[1, 2, 3], "Person");
    let rows = storage
        .write(DatasetId::nodes("Person"), batch)
        .await
        .unwrap();
    assert_eq!(rows, 3);

    // Write Company nodes
    let batch = create_node_batch(&[10, 11], "Company");
    let rows = storage
        .write(DatasetId::nodes("Company"), batch)
        .await
        .unwrap();
    assert_eq!(rows, 2);

    // Verify label counts
    assert_eq!(storage.node_label_count().await, 2);
    assert!(storage.node_labels().await.contains(&"Person".to_string()));
    assert!(storage.node_labels().await.contains(&"Company".to_string()));
}

#[tokio::test]
async fn test_memory_write_hyperedges() {
    let storage = MemoryStorage::new();

    // Write KNOWS hyperedges (binary)
    let batch = create_hyperedge_batch(&[1, 2, 3, 4], "KNOWS", 2);
    let rows = storage
        .write(DatasetId::hyperedges("KNOWS"), batch)
        .await
        .unwrap();
    assert_eq!(rows, 4);

    // Write MEETING hyperedges (n-ary)
    let batch = create_hyperedge_batch(&[10], "MEETING", 5);
    let rows = storage
        .write(DatasetId::hyperedges("MEETING"), batch)
        .await
        .unwrap();
    assert_eq!(rows, 1);

    // Verify label counts
    assert_eq!(storage.hyperedge_label_count().await, 2);
    assert!(
        storage
            .hyperedge_labels()
            .await
            .contains(&"KNOWS".to_string())
    );
    assert!(
        storage
            .hyperedge_labels()
            .await
            .contains(&"MEETING".to_string())
    );
}

// ============================================================================
// Scanning Tests
// ============================================================================

#[tokio::test]
async fn test_memory_scan_nodes_by_label() {
    let storage = MemoryStorage::new();

    // Write nodes
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1, 2, 3], "Person"),
        )
        .await
        .unwrap();
    storage
        .write(
            DatasetId::nodes("Company"),
            create_node_batch(&[10, 11], "Company"),
        )
        .await
        .unwrap();

    // Create snapshot for reading
    let snapshot = storage.create_snapshot().await.unwrap();

    // Scan Person nodes only
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

    // Scan Company nodes only
    let stream = storage
        .scan(
            DatasetId::nodes("Company"),
            &Projection::all(),
            None,
            snapshot,
        )
        .await
        .unwrap();
    let batches = collect_stream(stream).await;
    assert_eq!(total_rows(&batches), 2);
}

#[tokio::test]
async fn test_memory_scan_all_nodes() {
    let storage = MemoryStorage::new();

    // Write nodes with different labels
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

    let snapshot = storage.create_snapshot().await.unwrap();

    // Scan all nodes (no label filter)
    let stream = storage
        .scan(DatasetId::all_nodes(), &Projection::all(), None, snapshot)
        .await
        .unwrap();
    let batches = collect_stream(stream).await;

    // Should have all 5 nodes
    assert_eq!(total_rows(&batches), 5);
}

#[tokio::test]
async fn test_memory_scan_hyperedges_by_label() {
    let storage = MemoryStorage::new();

    // Write hyperedges
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

    let snapshot = storage.create_snapshot().await.unwrap();

    // Scan KNOWS hyperedges
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

    // Verify arity column
    if let Some(batch) = batches.first() {
        let arity_col = batch.column_by_name("_arity").unwrap();
        let arities = arity_col.as_any().downcast_ref::<UInt32Array>().unwrap();
        for i in 0..arities.len() {
            assert_eq!(arities.value(i), 2);
        }
    }
}

#[tokio::test]
async fn test_memory_scan_empty_dataset() {
    let storage = MemoryStorage::new();

    // Create snapshot with no data
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

    // Should return empty
    assert!(batches.is_empty());
}

#[tokio::test]
async fn test_memory_scan_with_projection() {
    let storage = MemoryStorage::new();

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

    // Should have 3 rows but only 1 column
    assert_eq!(total_rows(&batches), 3);
    for batch in &batches {
        assert_eq!(batch.num_columns(), 1);
        assert!(batch.column_by_name("_id").is_some());
    }
}

#[tokio::test]
async fn test_memory_scan_working_state() {
    let storage = MemoryStorage::new();

    // Write data without creating snapshot
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1, 2], "Person"),
        )
        .await
        .unwrap();

    // Scan working state (snapshot 0)
    let stream = storage
        .scan(DatasetId::nodes("Person"), &Projection::all(), None, 0)
        .await
        .unwrap();
    let batches = collect_stream(stream).await;

    // Should see working state data
    assert_eq!(total_rows(&batches), 2);
}

// ============================================================================
// Snapshot Tests
// ============================================================================

#[tokio::test]
async fn test_memory_snapshot_isolation() {
    let storage = MemoryStorage::new();

    // Write initial data
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1], "Person"),
        )
        .await
        .unwrap();

    // Create first snapshot
    let snapshot1 = storage.create_snapshot().await.unwrap();
    assert!(snapshot1 > 0);

    // Write more data after snapshot
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[2, 3], "Person"),
        )
        .await
        .unwrap();

    // Create second snapshot
    let snapshot2 = storage.create_snapshot().await.unwrap();
    assert!(snapshot2 > snapshot1);

    // Snapshot 1 should only have 1 row
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

    // Snapshot 2 should have 3 rows
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
    assert_eq!(total_rows(&batches), 3);
}

#[tokio::test]
async fn test_memory_multiple_snapshots() {
    let storage = MemoryStorage::new();

    // Create 5 snapshots with incremental data
    let mut snapshots = Vec::new();
    for i in 1..=5 {
        storage
            .write(
                DatasetId::nodes("Person"),
                create_node_batch(&[i as i64], "Person"),
            )
            .await
            .unwrap();
        let snapshot = storage.create_snapshot().await.unwrap();
        snapshots.push((snapshot, i));
    }

    // Verify each snapshot has correct row count
    for (snapshot, expected_rows) in snapshots {
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
        assert_eq!(
            total_rows(&batches),
            expected_rows,
            "Snapshot {snapshot} should have {expected_rows} rows"
        );
    }
}

#[tokio::test]
async fn test_memory_resolve_snapshot_latest() {
    let storage = MemoryStorage::new();

    // Initially latest is 0
    let resolved = storage.resolve_snapshot(SnapshotSpec::Latest).unwrap();
    assert_eq!(resolved, 0);

    // Write and create snapshot
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1], "Person"),
        )
        .await
        .unwrap();
    let snapshot1 = storage.create_snapshot().await.unwrap();

    // Now latest should be snapshot1
    let resolved = storage.resolve_snapshot(SnapshotSpec::Latest).unwrap();
    assert_eq!(resolved, snapshot1);

    // Create another snapshot
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[2], "Person"),
        )
        .await
        .unwrap();
    let snapshot2 = storage.create_snapshot().await.unwrap();

    // Latest should update
    let resolved = storage.resolve_snapshot(SnapshotSpec::Latest).unwrap();
    assert_eq!(resolved, snapshot2);
}

#[tokio::test]
async fn test_memory_resolve_snapshot_id() {
    let storage = MemoryStorage::new();

    // Write and create snapshots
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

    // Resolve by specific ID
    let resolved = storage
        .resolve_snapshot(SnapshotSpec::Id(snapshot1))
        .unwrap();
    assert_eq!(resolved, snapshot1);

    let resolved = storage
        .resolve_snapshot(SnapshotSpec::Id(snapshot2))
        .unwrap();
    assert_eq!(resolved, snapshot2);

    // Snapshot 0 (working state) is always valid
    let resolved = storage.resolve_snapshot(SnapshotSpec::Id(0)).unwrap();
    assert_eq!(resolved, 0);
}

#[tokio::test]
async fn test_memory_current_snapshot() {
    let storage = MemoryStorage::new();

    // Initially 0
    assert_eq!(storage.current_snapshot().unwrap(), 0);

    // After creating snapshot, it updates
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1], "Person"),
        )
        .await
        .unwrap();
    let snapshot = storage.create_snapshot().await.unwrap();
    assert_eq!(storage.current_snapshot().unwrap(), snapshot);
}

// ============================================================================
// Fragment Tests
// ============================================================================

#[tokio::test]
async fn test_memory_fragments_metadata() {
    let storage = MemoryStorage::new();

    // Write a batch with known size
    let batch = create_node_batch(&[1, 2, 3, 4, 5], "Person");
    storage
        .write(DatasetId::nodes("Person"), batch)
        .await
        .unwrap();

    let snapshot = storage.create_snapshot().await.unwrap();

    // Get fragment metadata
    let fragments = storage.fragments(DatasetId::nodes("Person"), snapshot);

    assert_eq!(fragments.len(), 1);
    assert_eq!(fragments[0].row_count, 5);
    assert!(fragments[0].byte_size > 0);
    assert!(matches!(fragments[0].location, FragmentLocation::Memory));
}

#[tokio::test]
async fn test_memory_fragments_multiple_batches() {
    let storage = MemoryStorage::new();

    // Write multiple batches
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1, 2], "Person"),
        )
        .await
        .unwrap();
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[3, 4, 5], "Person"),
        )
        .await
        .unwrap();
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[6], "Person"),
        )
        .await
        .unwrap();

    let snapshot = storage.create_snapshot().await.unwrap();

    // Should have 3 fragments
    let fragments = storage.fragments(DatasetId::nodes("Person"), snapshot);
    assert_eq!(fragments.len(), 3);

    // Verify row counts match batches
    let row_counts: Vec<usize> = fragments.iter().map(|f| f.row_count).collect();
    assert_eq!(row_counts, vec![2, 3, 1]);

    // Verify unique fragment IDs
    let ids: Vec<u64> = fragments.iter().map(|f| f.id).collect();
    assert_eq!(
        ids.len(),
        ids.iter().collect::<std::collections::HashSet<_>>().len()
    );
}

#[tokio::test]
async fn test_memory_fragments_empty() {
    let storage = MemoryStorage::new();

    let snapshot = storage.create_snapshot().await.unwrap();

    // No fragments for non-existent dataset
    let fragments = storage.fragments(DatasetId::nodes("NonExistent"), snapshot);
    assert!(fragments.is_empty());
}

// ============================================================================
// Capabilities Tests
// ============================================================================

#[tokio::test]
async fn test_memory_capabilities() {
    let storage = MemoryStorage::new();
    let caps = storage.capabilities();

    // Memory storage capabilities per RFC-0020
    assert!(
        !caps.predicate_pushdown,
        "Memory storage does not support predicate pushdown"
    );
    assert!(
        caps.projection_pushdown,
        "Memory storage supports projection pushdown"
    );
    assert!(
        caps.fragment_pruning,
        "Memory storage supports fragment pruning"
    );
    assert!(
        !caps.object_store,
        "Memory storage is not object store compatible"
    );
}

// ============================================================================
// Stats Tests
// ============================================================================

#[tokio::test]
async fn test_memory_stats() {
    let storage = MemoryStorage::new();

    // Initially empty
    let stats = storage.stats();
    assert_eq!(stats.node_count, 0);
    assert_eq!(stats.hyperedge_count, 0);
    assert_eq!(stats.node_label_count, 0);
    assert_eq!(stats.hyperedge_label_count, 0);
    assert_eq!(stats.snapshot_count, 0);

    // Add data
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1, 2, 3], "Person"),
        )
        .await
        .unwrap();
    storage
        .write(
            DatasetId::hyperedges("KNOWS"),
            create_hyperedge_batch(&[1, 2], "KNOWS", 2),
        )
        .await
        .unwrap();

    // Create snapshot
    storage.create_snapshot().await.unwrap();

    // Check updated stats
    let stats = storage.stats();
    assert_eq!(stats.node_count, 3);
    assert_eq!(stats.hyperedge_count, 2);
    assert_eq!(stats.node_label_count, 1);
    assert_eq!(stats.hyperedge_label_count, 1);
    assert_eq!(stats.snapshot_count, 1);
}

#[tokio::test]
async fn test_memory_stats_multiple_labels() {
    let storage = MemoryStorage::new();

    // Add nodes with different labels
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
            DatasetId::nodes("Location"),
            create_node_batch(&[20], "Location"),
        )
        .await
        .unwrap();

    let stats = storage.stats();
    assert_eq!(stats.node_count, 6); // 2 + 3 + 1
    assert_eq!(stats.node_label_count, 3);
}

// ============================================================================
// Edge Cases Tests
// ============================================================================

#[tokio::test]
async fn test_memory_adjacency_not_implemented() {
    let storage = MemoryStorage::new();

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
        batches.is_empty(),
        "Adjacency datasets are not yet implemented"
    );
}

#[tokio::test]
async fn test_memory_named_snapshot_not_implemented() {
    let storage = MemoryStorage::new();

    // Named snapshots are not supported
    let result = storage.resolve_snapshot(SnapshotSpec::Named("my_snapshot".to_string()));
    assert!(result.is_err(), "Named snapshots are not yet implemented");
}

#[tokio::test]
async fn test_memory_invalid_snapshot() {
    let storage = MemoryStorage::new();

    // Create one snapshot
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1], "Person"),
        )
        .await
        .unwrap();
    storage.create_snapshot().await.unwrap();

    // Try to scan with non-existent snapshot ID
    let result = storage
        .scan(DatasetId::nodes("Person"), &Projection::all(), None, 999)
        .await;

    // Should error since snapshot 999 doesn't exist
    assert!(result.is_err());
}

#[tokio::test]
async fn test_memory_write_default_label() {
    let storage = MemoryStorage::new();

    // Write with no label (uses _default internally)
    let batch = create_node_batch(&[1, 2], "Person");
    storage
        .write(DatasetId::Nodes { label: None }, batch)
        .await
        .unwrap();

    let snapshot = storage.create_snapshot().await.unwrap();

    // Should be scannable via all_nodes
    let stream = storage
        .scan(DatasetId::all_nodes(), &Projection::all(), None, snapshot)
        .await
        .unwrap();
    let batches = collect_stream(stream).await;
    assert_eq!(total_rows(&batches), 2);
}

#[tokio::test]
async fn test_memory_flush_noop() {
    let storage = MemoryStorage::new();

    // Write data
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1], "Person"),
        )
        .await
        .unwrap();

    // Flush is a no-op for memory storage
    let result = storage.flush().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_memory_close() {
    let storage = MemoryStorage::new();

    // Write data
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1], "Person"),
        )
        .await
        .unwrap();

    // Close should succeed
    let result = storage.close().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_memory_clear() {
    let storage = MemoryStorage::new();

    // Write data
    storage
        .write(
            DatasetId::nodes("Person"),
            create_node_batch(&[1, 2, 3], "Person"),
        )
        .await
        .unwrap();

    // Verify data exists
    assert_eq!(storage.node_label_count().await, 1);

    // Clear
    storage.clear().await;

    // Data should be gone
    assert_eq!(storage.node_label_count().await, 0);
}

// ============================================================================
// Data Integrity Tests
// ============================================================================

#[tokio::test]
async fn test_memory_data_integrity() {
    let storage = MemoryStorage::new();

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
async fn test_memory_hyperedge_data_integrity() {
    let storage = MemoryStorage::new();

    // Write hyperedges with varying arities
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
// RFC-0103 Future Features (Not Yet Implemented)
// ============================================================================

/// NOTE: Per RFC-0103, the following features are NOT yet implemented:
///
/// - TieredStorage (Section 6): Memory as hot tier, Lance as cold tier
///   - Memory tier with write buffers
///   - Lance tier with persistent storage
///   - Automatic tiering based on access patterns
///
/// - FlushManager (Section 10): Coordinates persistence
///   - Automatic flush triggers based on memory pressure
///   - Buffer age thresholds
///   - Row count thresholds
///
/// - CacheManager (Section 9): Read cache acceleration
///   - LRU cache for read results
///   - Weighted eviction policy
///   - TTL-based cache expiry
///
/// - WriteBuffer (Section 6.4): Accumulates in-memory mutations
///   - Per-dataset/snapshot write buffers
///   - Memory accounting
///
/// When these features are implemented, additional integration tests should be
/// added to verify their behavior.
#[test]
fn test_rfc0103_future_features_documented() {
    // This test serves as documentation for future features
    // No actual assertions - just a marker for RFC-0103 compliance
}
