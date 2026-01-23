//! Scan execution operators.
//!
//! These operators scan data from storage using the RFC-0012 Storage trait.
//! They return Arrow `RecordBatches` directly from the storage layer.

use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::StreamExt;

use common_error::{GrismError, GrismResult};
use grism_storage::{DatasetId, Projection, RecordBatchStream};

use crate::executor::ExecutionContext;
use crate::metrics::ExecutionTimer;
use crate::operators::PhysicalOperator;
use crate::physical::{OperatorCaps, PhysicalSchema, PhysicalSchemaBuilder};

/// Internal state for scan operators using `RecordBatchStream`.
#[derive(Default)]
enum ScanState {
    #[default]
    Uninitialized,
    Open {
        /// Stream of record batches from storage.
        stream: RecordBatchStream,
    },
    Exhausted,
    Closed,
}

impl std::fmt::Debug for ScanState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Uninitialized => write!(f, "Uninitialized"),
            Self::Open { .. } => write!(f, "Open"),
            Self::Exhausted => write!(f, "Exhausted"),
            Self::Closed => write!(f, "Closed"),
        }
    }
}

/// Node scan execution operator.
///
/// Reads nodes from storage using RFC-0012 `Storage::scan()` and produces Arrow `RecordBatch`.
#[derive(Debug)]
pub struct NodeScanExec {
    /// Label filter (None = all nodes).
    label: Option<String>,
    /// Alias for the scanned entity.
    alias: Option<String>,
    /// Output schema.
    schema: PhysicalSchema,
    /// Execution state.
    state: tokio::sync::Mutex<ScanState>,
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
        let schema = Self::build_schema(label.as_ref(), alias.as_ref());
        let operator_id = match (&label, &alias) {
            (Some(l), Some(a)) => format!("NodeScanExec[{l}:{a}]"),
            (Some(l), None) => format!("NodeScanExec[{l}]"),
            (None, Some(a)) => format!("NodeScanExec[*:{a}]"),
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
        self.schema = Self::build_schema(self.label.as_ref(), self.alias.as_ref());
        self
    }

    fn build_schema(label: Option<&String>, alias: Option<&String>) -> PhysicalSchema {
        let qualifier = alias.cloned().or_else(|| label.cloned());

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

        // Use RFC-0012 Storage::scan() to get a RecordBatchStream
        let dataset = match &self.label {
            Some(label) => DatasetId::nodes(label.clone()),
            None => DatasetId::all_nodes(),
        };
        let stream = ctx
            .storage
            .scan(dataset, &Projection::all(), None, ctx.snapshot)
            .await?;

        let mut state = self.state.lock().await;
        *state = ScanState::Open { stream };

        ctx.update_metrics(&self.operator_id, |m| {
            m.add_time(timer.stop());
        });

        Ok(())
    }

    async fn next(&self) -> GrismResult<Option<RecordBatch>> {
        let mut state = self.state.lock().await;

        match &mut *state {
            ScanState::Uninitialized => Err(GrismError::execution("Operator not opened")),
            ScanState::Open { stream } => match stream.next().await {
                Some(Ok(batch)) => Ok(Some(batch)),
                Some(Err(e)) => Err(e),
                None => {
                    *state = ScanState::Exhausted;
                    Ok(None)
                }
            },
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
            (Some(l), Some(a)) => format!("NodeScanExec(label={l}, alias={a})"),
            (Some(l), None) => format!("NodeScanExec(label={l})"),
            (None, Some(a)) => format!("NodeScanExec(all, alias={a})"),
            (None, None) => "NodeScanExec(all)".to_string(),
        }
    }
}

/// Hyperedge scan execution operator.
///
/// Reads hyperedges from storage using RFC-0012 `Storage::scan()` and produces Arrow `RecordBatch`.
#[derive(Debug)]
pub struct HyperedgeScanExec {
    /// Label filter (None = all hyperedges).
    label: Option<String>,
    /// Alias for the scanned entity.
    alias: Option<String>,
    /// Output schema.
    schema: PhysicalSchema,
    /// Execution state.
    state: tokio::sync::Mutex<ScanState>,
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
        let schema = Self::build_schema(label.as_ref(), alias.as_ref());
        let operator_id = match (&label, &alias) {
            (Some(l), Some(a)) => format!("HyperedgeScanExec[{l}:{a}]"),
            (Some(l), None) => format!("HyperedgeScanExec[{l}]"),
            (None, Some(a)) => format!("HyperedgeScanExec[*:{a}]"),
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
        self.schema = Self::build_schema(self.label.as_ref(), self.alias.as_ref());
        self
    }

    fn build_schema(label: Option<&String>, alias: Option<&String>) -> PhysicalSchema {
        let qualifier = alias.cloned().or_else(|| label.cloned());

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

        // Use RFC-0012 Storage::scan() to get a RecordBatchStream
        let dataset = match &self.label {
            Some(label) => DatasetId::hyperedges(label.clone()),
            None => DatasetId::all_hyperedges(),
        };
        let stream = ctx
            .storage
            .scan(dataset, &Projection::all(), None, ctx.snapshot)
            .await?;

        let mut state = self.state.lock().await;
        *state = ScanState::Open { stream };

        ctx.update_metrics(&self.operator_id, |m| {
            m.add_time(timer.stop());
        });

        Ok(())
    }

    async fn next(&self) -> GrismResult<Option<RecordBatch>> {
        let mut state = self.state.lock().await;

        match &mut *state {
            ScanState::Uninitialized => Err(GrismError::execution("Operator not opened")),
            ScanState::Open { stream } => match stream.next().await {
                Some(Ok(batch)) => Ok(Some(batch)),
                Some(Err(e)) => Err(e),
                None => {
                    *state = ScanState::Exhausted;
                    Ok(None)
                }
            },
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
            (Some(l), Some(a)) => format!("HyperedgeScanExec(label={l}, alias={a})"),
            (Some(l), None) => format!("HyperedgeScanExec(label={l})"),
            (None, Some(a)) => format!("HyperedgeScanExec(all, alias={a})"),
            (None, None) => "HyperedgeScanExec(all)".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_storage::{MemoryStorage, NodeBatchBuilder, SnapshotId, WritableStorage};

    #[tokio::test]
    async fn test_node_scan_empty() {
        let op = NodeScanExec::with_label("Person");
        let storage = Arc::new(MemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        op.open(&ctx).await.unwrap();

        // No data in storage, should return None
        assert!(op.next().await.unwrap().is_none());

        op.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_node_scan_with_data() {
        let storage = Arc::new(MemoryStorage::new());

        // Insert test nodes using new WritableStorage::write() API
        let mut builder = NodeBatchBuilder::new();
        builder.add(1, Some("Person"));
        builder.add(2, Some("Person"));
        let batch = builder.build().unwrap();

        storage
            .write(DatasetId::nodes("Person"), batch)
            .await
            .unwrap();

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
        let storage = Arc::new(MemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        op.open(&ctx).await.unwrap();
        assert!(op.next().await.unwrap().is_none());
        op.close().await.unwrap();
    }
}
