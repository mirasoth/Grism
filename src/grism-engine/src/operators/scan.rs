//! Scan execution operators.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringBuilder};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use common_error::{GrismError, GrismResult};
use grism_core::hypergraph::{Hyperedge, Node};

use crate::executor::ExecutionContext;
use crate::metrics::ExecutionTimer;
use crate::operators::PhysicalOperator;
use crate::physical::{OperatorCaps, PhysicalSchema, PhysicalSchemaBuilder};

/// Internal state for scan operators.
#[derive(Debug)]
enum ScanState<T> {
    Uninitialized,
    Open {
        /// Buffered entities to return.
        buffer: Vec<T>,
        /// Current position in buffer.
        position: usize,
    },
    Exhausted,
    Closed,
}

impl<T> Default for ScanState<T> {
    fn default() -> Self {
        Self::Uninitialized
    }
}

/// Node scan execution operator.
///
/// Reads nodes from storage and produces Arrow `RecordBatch`.
#[derive(Debug)]
pub struct NodeScanExec {
    /// Label filter (None = all nodes).
    label: Option<String>,
    /// Alias for the scanned entity.
    alias: Option<String>,
    /// Output schema.
    schema: PhysicalSchema,
    /// Execution state.
    state: tokio::sync::Mutex<ScanState<Node>>,
    /// Operator ID for metrics.
    operator_id: String,
}

impl NodeScanExec {
    /// Create a node scan operator for all nodes.
    pub fn all() -> Self {
        Self::new(None, None)
    }

    /// Create a node scan operator with label filter.
    pub fn with_label(label: impl Into<String>) -> Self {
        Self::new(Some(label.into()), None)
    }

    /// Create a node scan operator with label and alias.
    pub fn new(label: Option<String>, alias: Option<String>) -> Self {
        let schema = Self::build_schema(&label, &alias);
        let operator_id = match (&label, &alias) {
            (Some(l), Some(a)) => format!("NodeScanExec[{}:{}]", l, a),
            (Some(l), None) => format!("NodeScanExec[{}]", l),
            (None, Some(a)) => format!("NodeScanExec[*:{}]", a),
            (None, None) => "NodeScanExec[*]".to_string(),
        };

        Self {
            label,
            alias,
            schema,
            state: tokio::sync::Mutex::new(ScanState::Uninitialized),
            operator_id,
        }
    }

    /// Set alias for the scanned entity.
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self.schema = Self::build_schema(&self.label, &self.alias);
        self
    }

    fn build_schema(label: &Option<String>, alias: &Option<String>) -> PhysicalSchema {
        let qualifier = alias.clone().or_else(|| label.clone());

        let mut builder = PhysicalSchemaBuilder::new();

        if let Some(q) = &qualifier {
            builder = builder
                .qualified_field("_id", DataType::Int64, false, q)
                .qualified_field("_label", DataType::Utf8, true, q);
        } else {
            builder = builder.id_field("_id").string_field("_label");
        }

        builder.build()
    }

    /// Convert nodes to RecordBatch.
    fn nodes_to_batch(&self, nodes: &[Node], batch_size: usize) -> GrismResult<RecordBatch> {
        let actual_size = nodes.len().min(batch_size);
        let mut id_builder = Int64Array::builder(actual_size);
        let mut label_builder = StringBuilder::new();

        for node in nodes.iter().take(actual_size) {
            id_builder.append_value(node.id as i64);
            let label_str = node.labels.first().map_or("", |l| l.as_str());
            label_builder.append_value(label_str);
        }

        let schema = self.schema.arrow_schema().clone();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_builder.finish()) as ArrayRef,
                Arc::new(label_builder.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| GrismError::execution(e.to_string()))
    }
}

#[async_trait]
impl PhysicalOperator for NodeScanExec {
    fn name(&self) -> &'static str {
        "NodeScanExec"
    }

    fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps::source()
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
        vec![]
    }

    async fn open(&self, ctx: &ExecutionContext) -> GrismResult<()> {
        let timer = ExecutionTimer::start();

        // Load nodes from storage
        let nodes = match &self.label {
            Some(label) => ctx.storage.get_nodes_by_label(label).await?,
            None => {
                // For now, return empty if no label specified
                // TODO: Implement get_all_nodes in storage
                vec![]
            }
        };

        let mut state = self.state.lock().await;
        *state = ScanState::Open {
            buffer: nodes,
            position: 0,
        };

        ctx.update_metrics(&self.operator_id, |m| {
            m.add_time(timer.stop());
        });

        Ok(())
    }

    async fn next(&self) -> GrismResult<Option<RecordBatch>> {
        let mut state = self.state.lock().await;

        match &mut *state {
            ScanState::Uninitialized => Err(GrismError::execution("Operator not opened")),
            ScanState::Open { buffer, position } => {
                if *position >= buffer.len() {
                    *state = ScanState::Exhausted;
                    return Ok(None);
                }

                // Get batch_size from somewhere - use default for now
                let batch_size = 8192;
                let end = (*position + batch_size).min(buffer.len());
                let batch_nodes = &buffer[*position..end];
                *position = end;

                let batch = self.nodes_to_batch(batch_nodes, batch_size)?;

                // Drop the lock before returning
                drop(state);

                Ok(Some(batch))
            }
            ScanState::Exhausted | ScanState::Closed => Ok(None),
        }
    }

    async fn close(&self) -> GrismResult<()> {
        let mut state = self.state.lock().await;
        *state = ScanState::Closed;
        Ok(())
    }

    fn display(&self) -> String {
        match (&self.label, &self.alias) {
            (Some(l), Some(a)) => format!("NodeScanExec(label={}, alias={})", l, a),
            (Some(l), None) => format!("NodeScanExec(label={})", l),
            (None, Some(a)) => format!("NodeScanExec(all, alias={})", a),
            (None, None) => "NodeScanExec(all)".to_string(),
        }
    }
}

/// Hyperedge scan execution operator.
///
/// Reads hyperedges from storage and produces Arrow `RecordBatch`.
#[derive(Debug)]
pub struct HyperedgeScanExec {
    /// Label filter (None = all hyperedges).
    label: Option<String>,
    /// Alias for the scanned entity.
    alias: Option<String>,
    /// Output schema.
    schema: PhysicalSchema,
    /// Execution state.
    state: tokio::sync::Mutex<ScanState<Hyperedge>>,
    /// Operator ID for metrics.
    operator_id: String,
}

impl HyperedgeScanExec {
    /// Create a hyperedge scan operator for all hyperedges.
    pub fn all() -> Self {
        Self::new(None, None)
    }

    /// Create a hyperedge scan operator with label filter.
    pub fn with_label(label: impl Into<String>) -> Self {
        Self::new(Some(label.into()), None)
    }

    /// Create a hyperedge scan operator with label and alias.
    pub fn new(label: Option<String>, alias: Option<String>) -> Self {
        let schema = Self::build_schema(&label, &alias);
        let operator_id = match (&label, &alias) {
            (Some(l), Some(a)) => format!("HyperedgeScanExec[{}:{}]", l, a),
            (Some(l), None) => format!("HyperedgeScanExec[{}]", l),
            (None, Some(a)) => format!("HyperedgeScanExec[*:{}]", a),
            (None, None) => "HyperedgeScanExec[*]".to_string(),
        };

        Self {
            label,
            alias,
            schema,
            state: tokio::sync::Mutex::new(ScanState::Uninitialized),
            operator_id,
        }
    }

    /// Set alias for the scanned entity.
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self.schema = Self::build_schema(&self.label, &self.alias);
        self
    }

    fn build_schema(label: &Option<String>, alias: &Option<String>) -> PhysicalSchema {
        let qualifier = alias.clone().or_else(|| label.clone());

        let mut builder = PhysicalSchemaBuilder::new();

        if let Some(q) = &qualifier {
            builder = builder
                .qualified_field("_id", DataType::Int64, false, q)
                .qualified_field("_label", DataType::Utf8, false, q)
                .qualified_field("_arity", DataType::Int64, false, q);
        } else {
            builder = builder
                .id_field("_id")
                .field("_label", DataType::Utf8, false)
                .field("_arity", DataType::Int64, false);
        }

        builder.build()
    }

    /// Convert hyperedges to RecordBatch.
    fn hyperedges_to_batch(
        &self,
        hyperedges: &[Hyperedge],
        batch_size: usize,
    ) -> GrismResult<RecordBatch> {
        let actual_size = hyperedges.len().min(batch_size);
        let mut id_builder = Int64Array::builder(actual_size);
        let mut label_builder = StringBuilder::new();
        let mut arity_builder = Int64Array::builder(actual_size);

        for he in hyperedges.iter().take(actual_size) {
            id_builder.append_value(he.id as i64);
            label_builder.append_value(&he.label);
            arity_builder.append_value(he.roles().len() as i64);
        }

        let schema = self.schema.arrow_schema().clone();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_builder.finish()) as ArrayRef,
                Arc::new(label_builder.finish()) as ArrayRef,
                Arc::new(arity_builder.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| GrismError::execution(e.to_string()))
    }
}

#[async_trait]
impl PhysicalOperator for HyperedgeScanExec {
    fn name(&self) -> &'static str {
        "HyperedgeScanExec"
    }

    fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps::source()
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
        vec![]
    }

    async fn open(&self, ctx: &ExecutionContext) -> GrismResult<()> {
        let timer = ExecutionTimer::start();

        // Load hyperedges from storage
        let hyperedges = match &self.label {
            Some(label) => ctx.storage.get_hyperedges_by_label(label).await?,
            None => {
                // For now, return empty if no label specified
                vec![]
            }
        };

        let mut state = self.state.lock().await;
        *state = ScanState::Open {
            buffer: hyperedges,
            position: 0,
        };

        ctx.update_metrics(&self.operator_id, |m| {
            m.add_time(timer.stop());
        });

        Ok(())
    }

    async fn next(&self) -> GrismResult<Option<RecordBatch>> {
        let mut state = self.state.lock().await;

        match &mut *state {
            ScanState::Uninitialized => Err(GrismError::execution("Operator not opened")),
            ScanState::Open { buffer, position } => {
                if *position >= buffer.len() {
                    *state = ScanState::Exhausted;
                    return Ok(None);
                }

                let batch_size = 8192;
                let end = (*position + batch_size).min(buffer.len());
                let batch_hyperedges = &buffer[*position..end];
                *position = end;

                let batch = self.hyperedges_to_batch(batch_hyperedges, batch_size)?;
                Ok(Some(batch))
            }
            ScanState::Exhausted | ScanState::Closed => Ok(None),
        }
    }

    async fn close(&self) -> GrismResult<()> {
        let mut state = self.state.lock().await;
        *state = ScanState::Closed;
        Ok(())
    }

    fn display(&self) -> String {
        match (&self.label, &self.alias) {
            (Some(l), Some(a)) => format!("HyperedgeScanExec(label={}, alias={})", l, a),
            (Some(l), None) => format!("HyperedgeScanExec(label={})", l),
            (None, Some(a)) => format!("HyperedgeScanExec(all, alias={})", a),
            (None, None) => "HyperedgeScanExec(all)".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_storage::{InMemoryStorage, SnapshotId, Storage};

    #[tokio::test]
    async fn test_node_scan_empty() {
        let op = NodeScanExec::with_label("Person");
        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        op.open(&ctx).await.unwrap();

        // No data in storage, should return None
        assert!(op.next().await.unwrap().is_none());

        op.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_node_scan_with_data() {
        let storage = Arc::new(InMemoryStorage::new());

        // Insert test nodes
        let node1 = Node::new().with_label("Person");
        let node2 = Node::new().with_label("Person");
        storage.insert_node(&node1).await.unwrap();
        storage.insert_node(&node2).await.unwrap();

        let op = NodeScanExec::with_label("Person");
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        op.open(&ctx).await.unwrap();

        let batch = op.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        // Should be exhausted
        assert!(op.next().await.unwrap().is_none());

        op.close().await.unwrap();
    }

    #[test]
    fn test_node_scan_schema() {
        let op = NodeScanExec::with_label("Person").with_alias("p");
        let schema = op.schema();

        assert_eq!(schema.num_columns(), 2);
        assert_eq!(schema.qualifier("_id"), Some("p"));
    }

    #[tokio::test]
    async fn test_hyperedge_scan_empty() {
        let op = HyperedgeScanExec::with_label("KNOWS");
        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        op.open(&ctx).await.unwrap();
        assert!(op.next().await.unwrap().is_none());
        op.close().await.unwrap();
    }
}
