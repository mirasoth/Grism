//! Sample data generation for playground examples.
//!
//! This module provides functions to create sample hypergraph data
//! for testing and demonstrations using the RFC-0012 Storage interface.

use std::sync::Arc;

use common_error::GrismResult;
use grism_storage::{
    DatasetId, HyperedgeBatchBuilder, MemoryStorage, NodeBatchBuilder, WritableStorage,
};

/// Create a sample social network hypergraph.
///
/// Creates a simple social network with:
/// - Person nodes (Alice, Bob, Charlie, Diana, Eve)
/// - Company nodes (Acme Corp, Widgets Inc)
/// - KNOWS hyperedges between persons
/// - WORKS_AT hyperedges connecting persons to companies
///
/// # Example
///
/// ```rust,ignore
/// let storage = create_social_network().await?;
/// // Use RFC-0012 Storage::scan() to query data
/// ```
pub async fn create_social_network() -> GrismResult<Arc<MemoryStorage>> {
    let storage = Arc::new(MemoryStorage::new());

    // Create Person nodes
    let mut person_builder = NodeBatchBuilder::new();
    person_builder.add(1, Some("Person")); // Alice
    person_builder.add(2, Some("Person")); // Bob
    person_builder.add(3, Some("Person")); // Charlie
    person_builder.add(4, Some("Person")); // Diana
    person_builder.add(5, Some("Person")); // Eve
    storage
        .write(DatasetId::nodes("Person"), person_builder.build()?)
        .await?;

    // Create Company nodes
    let mut company_builder = NodeBatchBuilder::new();
    company_builder.add(10, Some("Company")); // Acme Corp
    company_builder.add(11, Some("Company")); // Widgets Inc
    storage
        .write(DatasetId::nodes("Company"), company_builder.build()?)
        .await?;

    // Create KNOWS hyperedges (binary relationships)
    let mut knows_builder = HyperedgeBatchBuilder::new();
    knows_builder.add(100, "KNOWS", 2); // Alice -> Bob
    knows_builder.add(101, "KNOWS", 2); // Alice -> Charlie
    knows_builder.add(102, "KNOWS", 2); // Bob -> Diana
    knows_builder.add(103, "KNOWS", 2); // Charlie -> Diana
    knows_builder.add(104, "KNOWS", 2); // Diana -> Eve
    knows_builder.add(105, "KNOWS", 2); // Eve -> Alice (cycle)
    storage
        .write(DatasetId::hyperedges("KNOWS"), knows_builder.build()?)
        .await?;

    // Create WORKS_AT hyperedges (n-ary relationships)
    let mut works_at_builder = HyperedgeBatchBuilder::new();
    works_at_builder.add(200, "WORKS_AT", 3); // Alice @ Acme
    works_at_builder.add(201, "WORKS_AT", 2); // Bob @ Widgets
    works_at_builder.add(202, "WORKS_AT", 2); // Charlie @ Acme
    works_at_builder.add(203, "WORKS_AT", 3); // Diana @ Acme
    storage
        .write(DatasetId::hyperedges("WORKS_AT"), works_at_builder.build()?)
        .await?;

    // Create MEETING hyperedge (multi-party relationship)
    let mut meeting_builder = HyperedgeBatchBuilder::new();
    meeting_builder.add(300, "MEETING", 4); // Weekly standup
    storage
        .write(DatasetId::hyperedges("MEETING"), meeting_builder.build()?)
        .await?;

    Ok(storage)
}

/// Create a minimal sample hypergraph for basic testing.
///
/// Creates a simple graph with:
/// - 3 nodes (A, B, C)
/// - 2 CONNECTS hyperedges (A→B, B→C)
/// - 1 TRIANGLE hyperedge connecting all three
pub async fn create_sample_hypergraph() -> GrismResult<Arc<MemoryStorage>> {
    let storage = Arc::new(MemoryStorage::new());

    // Create nodes
    let mut node_builder = NodeBatchBuilder::new();
    node_builder.add(1, Some("Node")); // A
    node_builder.add(2, Some("Node")); // B
    node_builder.add(3, Some("Node")); // C
    storage
        .write(DatasetId::nodes("Node"), node_builder.build()?)
        .await?;

    // Create CONNECTS hyperedges (edges)
    let mut connects_builder = HyperedgeBatchBuilder::new();
    connects_builder.add(10, "CONNECTS", 2); // A -> B
    connects_builder.add(11, "CONNECTS", 2); // B -> C
    storage
        .write(DatasetId::hyperedges("CONNECTS"), connects_builder.build()?)
        .await?;

    // Create TRIANGLE hyperedge
    let mut triangle_builder = HyperedgeBatchBuilder::new();
    triangle_builder.add(20, "TRIANGLE", 3); // All three vertices
    storage
        .write(DatasetId::hyperedges("TRIANGLE"), triangle_builder.build()?)
        .await?;

    Ok(storage)
}

/// Macro for creating property maps inline.
#[macro_export]
macro_rules! properties {
    ($($key:literal => $value:expr),* $(,)?) => {{
        let mut map = grism_core::hypergraph::PropertyMap::new();
        $(
            map.insert($key.to_string(), grism_core::types::Value::from($value));
        )*
        map
    }};
}

pub use properties;

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use grism_storage::{Projection, SnapshotSpec, Storage};

    #[tokio::test]
    async fn test_create_social_network() {
        let storage = create_social_network().await.unwrap();
        let snapshot = storage.resolve_snapshot(SnapshotSpec::Latest).unwrap();

        // Scan Person nodes
        let mut person_stream = storage
            .scan(
                DatasetId::nodes("Person"),
                &Projection::all(),
                None,
                snapshot,
            )
            .await
            .unwrap();
        let mut person_count = 0;
        while let Some(batch) = person_stream.next().await {
            person_count += batch.unwrap().num_rows();
        }
        assert_eq!(person_count, 5);

        // Scan Company nodes
        let mut company_stream = storage
            .scan(
                DatasetId::nodes("Company"),
                &Projection::all(),
                None,
                snapshot,
            )
            .await
            .unwrap();
        let mut company_count = 0;
        while let Some(batch) = company_stream.next().await {
            company_count += batch.unwrap().num_rows();
        }
        assert_eq!(company_count, 2);

        // Scan KNOWS hyperedges
        let mut knows_stream = storage
            .scan(
                DatasetId::hyperedges("KNOWS"),
                &Projection::all(),
                None,
                snapshot,
            )
            .await
            .unwrap();
        let mut knows_count = 0;
        while let Some(batch) = knows_stream.next().await {
            knows_count += batch.unwrap().num_rows();
        }
        assert_eq!(knows_count, 6);
    }

    #[tokio::test]
    async fn test_create_sample_hypergraph() {
        let storage = create_sample_hypergraph().await.unwrap();
        let snapshot = storage.resolve_snapshot(SnapshotSpec::Latest).unwrap();

        // Scan nodes
        let mut node_stream = storage
            .scan(DatasetId::nodes("Node"), &Projection::all(), None, snapshot)
            .await
            .unwrap();
        let mut node_count = 0;
        while let Some(batch) = node_stream.next().await {
            node_count += batch.unwrap().num_rows();
        }
        assert_eq!(node_count, 3);

        // Scan CONNECTS hyperedges
        let mut connects_stream = storage
            .scan(
                DatasetId::hyperedges("CONNECTS"),
                &Projection::all(),
                None,
                snapshot,
            )
            .await
            .unwrap();
        let mut connects_count = 0;
        while let Some(batch) = connects_stream.next().await {
            connects_count += batch.unwrap().num_rows();
        }
        assert_eq!(connects_count, 2);

        // Scan TRIANGLE hyperedges
        let mut triangle_stream = storage
            .scan(
                DatasetId::hyperedges("TRIANGLE"),
                &Projection::all(),
                None,
                snapshot,
            )
            .await
            .unwrap();
        let mut triangle_count = 0;
        while let Some(batch) = triangle_stream.next().await {
            triangle_count += batch.unwrap().num_rows();
        }
        assert_eq!(triangle_count, 1);
    }
}
