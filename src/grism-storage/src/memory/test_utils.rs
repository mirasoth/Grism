//! Test utilities for creating sample data batches.
//!
//! This module provides builder utilities for creating Arrow RecordBatches
//! for testing and example purposes. These builders are only available when
//! the `test-utils` feature is enabled.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringBuilder, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use common_error::{GrismError, GrismResult};

use super::stores::{HyperedgeStore, NodeStore};

// ============================================================================
// Node Batch Builders
// ============================================================================

/// Builder for creating node batches.
pub struct NodeBatchBuilder {
    ids: Vec<i64>,
    labels: Vec<Option<String>>,
}

impl NodeBatchBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            ids: Vec::new(),
            labels: Vec::new(),
        }
    }

    /// Add a node.
    pub fn add(&mut self, id: i64, label: Option<&str>) {
        self.ids.push(id);
        self.labels.push(label.map(String::from));
    }

    /// Build the `RecordBatch`.
    pub fn build(self) -> GrismResult<RecordBatch> {
        let schema = Arc::new(NodeStore::default_schema());

        let id_array = Int64Array::from(self.ids);
        let mut label_builder = StringBuilder::new();
        for label in &self.labels {
            match label {
                Some(l) => label_builder.append_value(l),
                None => label_builder.append_null(),
            }
        }
        let label_array = label_builder.finish();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array) as ArrayRef,
                Arc::new(label_array) as ArrayRef,
            ],
        )
        .map_err(|e| GrismError::execution(format!("Failed to build node batch: {e}")))
    }

    /// Number of nodes added.
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
}

impl Default for NodeBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for creating node batches with properties.
///
/// This builder supports creating nodes with custom properties
/// in addition to the required `_id` and `_label` fields.
pub struct NodeBatchBuilderWithProps {
    ids: Vec<i64>,
    labels: Vec<Option<String>>,
    /// Property columns: (name, values)
    string_props: Vec<(String, Vec<Option<String>>)>,
    int_props: Vec<(String, Vec<Option<i64>>)>,
}

impl NodeBatchBuilderWithProps {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            ids: Vec::new(),
            labels: Vec::new(),
            string_props: Vec::new(),
            int_props: Vec::new(),
        }
    }

    /// Define a string property column.
    pub fn with_string_prop(mut self, name: &str) -> Self {
        self.string_props.push((name.to_string(), Vec::new()));
        self
    }

    /// Define an integer property column.
    pub fn with_int_prop(mut self, name: &str) -> Self {
        self.int_props.push((name.to_string(), Vec::new()));
        self
    }

    /// Add a node with properties.
    ///
    /// Properties are provided as parallel slices matching the order
    /// in which properties were defined.
    pub fn add(
        &mut self,
        id: i64,
        label: Option<&str>,
        string_values: &[Option<&str>],
        int_values: &[Option<i64>],
    ) {
        self.ids.push(id);
        self.labels.push(label.map(String::from));

        // Add string property values
        for (i, prop) in self.string_props.iter_mut().enumerate() {
            let value = string_values.get(i).copied().flatten().map(String::from);
            prop.1.push(value);
        }

        // Add integer property values
        for (i, prop) in self.int_props.iter_mut().enumerate() {
            let value = int_values.get(i).copied().flatten();
            prop.1.push(value);
        }
    }

    /// Build the `RecordBatch` with custom schema.
    pub fn build(self) -> GrismResult<RecordBatch> {
        // Build schema with properties
        let mut fields = vec![
            Field::new("_id", DataType::Int64, false),
            Field::new("_label", DataType::Utf8, true),
        ];

        // Add string property fields
        for (name, _) in &self.string_props {
            fields.push(Field::new(name, DataType::Utf8, true));
        }

        // Add integer property fields
        for (name, _) in &self.int_props {
            fields.push(Field::new(name, DataType::Int64, true));
        }

        let schema = Arc::new(Schema::new(fields));

        // Build arrays
        let mut columns: Vec<ArrayRef> = Vec::new();

        // ID column
        let id_array = Int64Array::from(self.ids);
        columns.push(Arc::new(id_array));

        // Label column
        let mut label_builder = StringBuilder::new();
        for label in &self.labels {
            match label {
                Some(l) => label_builder.append_value(l),
                None => label_builder.append_null(),
            }
        }
        columns.push(Arc::new(label_builder.finish()));

        // String property columns
        for (_, values) in self.string_props {
            let mut builder = StringBuilder::new();
            for value in values {
                match value {
                    Some(v) => builder.append_value(&v),
                    None => builder.append_null(),
                }
            }
            columns.push(Arc::new(builder.finish()));
        }

        // Integer property columns
        for (_, values) in self.int_props {
            let array: Int64Array = values.into_iter().collect();
            columns.push(Arc::new(array));
        }

        RecordBatch::try_new(schema, columns)
            .map_err(|e| GrismError::execution(format!("Failed to build node batch: {e}")))
    }

    /// Number of nodes added.
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
}

impl Default for NodeBatchBuilderWithProps {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Hyperedge Batch Builder
// ============================================================================

/// Builder for creating hyperedge batches.
pub struct HyperedgeBatchBuilder {
    ids: Vec<i64>,
    labels: Vec<String>,
    arities: Vec<u32>,
}

impl HyperedgeBatchBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            ids: Vec::new(),
            labels: Vec::new(),
            arities: Vec::new(),
        }
    }

    /// Add a hyperedge.
    pub fn add(&mut self, id: i64, label: &str, arity: u32) {
        self.ids.push(id);
        self.labels.push(label.to_string());
        self.arities.push(arity);
    }

    /// Build the `RecordBatch`.
    pub fn build(self) -> GrismResult<RecordBatch> {
        let schema = Arc::new(HyperedgeStore::default_schema());

        let id_array = Int64Array::from(self.ids);
        let mut label_builder = StringBuilder::new();
        for label in &self.labels {
            label_builder.append_value(label);
        }
        let label_array = label_builder.finish();
        let arity_array = UInt32Array::from(self.arities);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array) as ArrayRef,
                Arc::new(label_array) as ArrayRef,
                Arc::new(arity_array) as ArrayRef,
            ],
        )
        .map_err(|e| GrismError::execution(format!("Failed to build hyperedge batch: {e}")))
    }

    /// Number of hyperedges added.
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
}

impl Default for HyperedgeBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}
