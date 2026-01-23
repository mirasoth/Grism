//! In-memory data stores for nodes and hyperedges.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use common_error::{GrismError, GrismResult};

use crate::types::{FragmentId, FragmentLocation, FragmentMeta};

// ============================================================================
// NodeStore
// ============================================================================

/// In-memory store for nodes of a specific label.
///
/// Stores node data as Arrow `RecordBatches` for efficient columnar access.
#[derive(Debug, Clone)]
pub struct NodeStore {
    /// Arrow schema for this node type.
    schema: Arc<Schema>,
    /// Data batches.
    batches: Vec<RecordBatch>,
    /// Fragment metadata (one per batch).
    fragments: Vec<FragmentMeta>,
    /// Total row count.
    row_count: usize,
    /// Next fragment ID.
    next_fragment_id: FragmentId,
}

impl NodeStore {
    /// Create a new empty node store.
    pub fn new() -> Self {
        Self {
            schema: Arc::new(Self::default_schema()),
            batches: Vec::new(),
            fragments: Vec::new(),
            row_count: 0,
            next_fragment_id: 0,
        }
    }

    /// Create a node store with a custom schema.
    pub fn with_schema(schema: Schema) -> Self {
        Self {
            schema: Arc::new(schema),
            batches: Vec::new(),
            fragments: Vec::new(),
            row_count: 0,
            next_fragment_id: 0,
        }
    }

    /// Default schema for nodes.
    pub fn default_schema() -> Schema {
        Schema::new(vec![
            Field::new("_id", DataType::Int64, false),
            Field::new("_label", DataType::Utf8, true),
        ])
    }

    /// Get the schema.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Get all batches.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Get fragment metadata.
    pub fn fragments(&self) -> &[FragmentMeta] {
        &self.fragments
    }

    /// Get total row count.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Add a batch to the store.
    pub fn add_batch(&mut self, batch: RecordBatch) -> GrismResult<()> {
        // Verify schema compatibility
        if batch.schema() != self.schema {
            // Try to reconcile schemas - allow adding columns
            if !self.is_schema_compatible(batch.schema().as_ref()) {
                return Err(GrismError::schema_error(format!(
                    "Schema mismatch: expected {:?}, got {:?}",
                    self.schema,
                    batch.schema()
                )));
            }
        }

        let num_rows = batch.num_rows();
        let byte_size = batch.get_array_memory_size();

        // Create fragment metadata
        let fragment = FragmentMeta {
            id: self.next_fragment_id,
            row_count: num_rows,
            byte_size,
            location: FragmentLocation::Memory,
        };

        self.batches.push(batch);
        self.fragments.push(fragment);
        self.row_count += num_rows;
        self.next_fragment_id += 1;

        Ok(())
    }

    /// Check if a schema is compatible with the store schema.
    fn is_schema_compatible(&self, other: &Schema) -> bool {
        // Other schema must have at least _id column
        other.field_with_name("_id").is_ok()
    }

    /// Clone the store for snapshot isolation.
    pub fn snapshot(&self) -> Self {
        Self {
            schema: Arc::clone(&self.schema),
            batches: self.batches.clone(),
            fragments: self.fragments.clone(),
            row_count: self.row_count,
            next_fragment_id: self.next_fragment_id,
        }
    }

    /// Clear all data.
    pub fn clear(&mut self) {
        self.batches.clear();
        self.fragments.clear();
        self.row_count = 0;
    }
}

impl Default for NodeStore {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// HyperedgeStore
// ============================================================================

/// In-memory store for hyperedges of a specific label.
///
/// Stores hyperedge data as Arrow `RecordBatches` for efficient columnar access.
#[derive(Debug, Clone)]
pub struct HyperedgeStore {
    /// Arrow schema for this hyperedge type.
    schema: Arc<Schema>,
    /// Data batches.
    batches: Vec<RecordBatch>,
    /// Fragment metadata (one per batch).
    fragments: Vec<FragmentMeta>,
    /// Total row count.
    row_count: usize,
    /// Next fragment ID.
    next_fragment_id: FragmentId,
}

impl HyperedgeStore {
    /// Create a new empty hyperedge store.
    pub fn new() -> Self {
        Self {
            schema: Arc::new(Self::default_schema()),
            batches: Vec::new(),
            fragments: Vec::new(),
            row_count: 0,
            next_fragment_id: 0,
        }
    }

    /// Create a hyperedge store with a custom schema.
    pub fn with_schema(schema: Schema) -> Self {
        Self {
            schema: Arc::new(schema),
            batches: Vec::new(),
            fragments: Vec::new(),
            row_count: 0,
            next_fragment_id: 0,
        }
    }

    /// Default schema for hyperedges.
    pub fn default_schema() -> Schema {
        Schema::new(vec![
            Field::new("_id", DataType::Int64, false),
            Field::new("_label", DataType::Utf8, false),
            Field::new("_arity", DataType::UInt32, false),
        ])
    }

    /// Get the schema.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Get all batches.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Get fragment metadata.
    pub fn fragments(&self) -> &[FragmentMeta] {
        &self.fragments
    }

    /// Get total row count.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Add a batch to the store.
    pub fn add_batch(&mut self, batch: RecordBatch) -> GrismResult<()> {
        let num_rows = batch.num_rows();
        let byte_size = batch.get_array_memory_size();

        // Create fragment metadata
        let fragment = FragmentMeta {
            id: self.next_fragment_id,
            row_count: num_rows,
            byte_size,
            location: FragmentLocation::Memory,
        };

        self.batches.push(batch);
        self.fragments.push(fragment);
        self.row_count += num_rows;
        self.next_fragment_id += 1;

        Ok(())
    }

    /// Clone the store for snapshot isolation.
    pub fn snapshot(&self) -> Self {
        Self {
            schema: Arc::clone(&self.schema),
            batches: self.batches.clone(),
            fragments: self.fragments.clone(),
            row_count: self.row_count,
            next_fragment_id: self.next_fragment_id,
        }
    }

    /// Clear all data.
    pub fn clear(&mut self) {
        self.batches.clear();
        self.fragments.clear();
        self.row_count = 0;
    }
}

impl Default for HyperedgeStore {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::test_utils::{HyperedgeBatchBuilder, NodeBatchBuilder};

    #[test]
    fn test_node_store_basic() {
        let mut store = NodeStore::new();
        assert!(store.is_empty());

        let mut builder = NodeBatchBuilder::new();
        builder.add(1, Some("Person"));
        builder.add(2, Some("Person"));
        let batch = builder.build().unwrap();

        store.add_batch(batch).unwrap();
        assert_eq!(store.row_count(), 2);
        assert_eq!(store.batches().len(), 1);
        assert_eq!(store.fragments().len(), 1);
    }

    #[test]
    fn test_node_store_snapshot() {
        let mut store = NodeStore::new();

        let mut builder = NodeBatchBuilder::new();
        builder.add(1, Some("Person"));
        store.add_batch(builder.build().unwrap()).unwrap();

        let snapshot = store.snapshot();
        assert_eq!(snapshot.row_count(), 1);

        // Modify original
        let mut builder2 = NodeBatchBuilder::new();
        builder2.add(2, Some("Person"));
        store.add_batch(builder2.build().unwrap()).unwrap();

        // Snapshot unchanged
        assert_eq!(snapshot.row_count(), 1);
        assert_eq!(store.row_count(), 2);
    }

    #[test]
    fn test_hyperedge_store_basic() {
        let mut store = HyperedgeStore::new();
        assert!(store.is_empty());

        let mut builder = HyperedgeBatchBuilder::new();
        builder.add(1, "KNOWS", 2);
        builder.add(2, "WORKS_AT", 2);
        let batch = builder.build().unwrap();

        store.add_batch(batch).unwrap();
        assert_eq!(store.row_count(), 2);
    }

    #[test]
    fn test_node_batch_builder() {
        let mut builder = NodeBatchBuilder::new();
        builder.add(1, Some("Person"));
        builder.add(2, None);
        builder.add(3, Some("Company"));

        assert_eq!(builder.len(), 3);
        let batch = builder.build().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_hyperedge_batch_builder() {
        let mut builder = HyperedgeBatchBuilder::new();
        builder.add(1, "KNOWS", 2);
        builder.add(2, "MEMBER_OF", 3);

        assert_eq!(builder.len(), 2);
        let batch = builder.build().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
    }
}
