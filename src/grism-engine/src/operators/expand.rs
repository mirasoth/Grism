//! Expand execution operators for graph traversal.
//!
//! Note: These operators are currently stubs. Full implementation requires
//! adjacency dataset support in the RFC-0012 Storage trait.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringBuilder};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use common_error::{GrismError, GrismResult};
use grism_logical::Direction;

use crate::executor::ExecutionContext;
use crate::metrics::OperatorMetrics;
use crate::operators::PhysicalOperator;
use crate::physical::{OperatorCaps, PhysicalSchema, PhysicalSchemaBuilder};

/// Adjacency expand execution operator for binary hyperedges.
///
/// Uses adjacency traversal for efficient expansion when:
/// - Arity = 2
/// - Roles = {source, target}
#[derive(Debug)]
pub struct AdjacencyExpandExec {
    /// Input operator.
    input: Arc<dyn PhysicalOperator>,
    /// Direction of traversal.
    direction: Direction,
    /// Edge label filter.
    edge_label: Option<String>,
    /// Target node label filter.
    to_label: Option<String>,
    /// Alias for target entity.
    target_alias: Option<String>,
    /// Output schema.
    schema: PhysicalSchema,
    /// Execution state.
    state: tokio::sync::Mutex<ExpandState>,
    /// Metrics.
    #[allow(dead_code)]
    metrics: tokio::sync::Mutex<OperatorMetrics>,
}

/// State for expand operators.
#[derive(Debug, Default)]
struct ExpandState {
    /// Current input batch being expanded.
    current_batch: Option<RecordBatch>,
    /// Current row index in the batch.
    row_index: usize,
    /// Pending output batches.
    pending: Vec<RecordBatch>,
    /// Execution context (stored for async operations).
    ctx: Option<ExecutionContext>,
}

impl AdjacencyExpandExec {
    /// Create a new adjacency expand operator.
    pub fn new(input: Arc<dyn PhysicalOperator>, direction: Direction) -> Self {
        let input_schema = input.schema();
        let schema = Self::build_schema(input_schema, None, None);

        Self {
            input,
            direction,
            edge_label: None,
            to_label: None,
            target_alias: None,
            schema,
            state: tokio::sync::Mutex::new(ExpandState::default()),
            metrics: tokio::sync::Mutex::new(OperatorMetrics::default()),
        }
    }

    /// Set edge label filter.
    pub fn with_edge_label(mut self, label: impl Into<String>) -> Self {
        self.edge_label = Some(label.into());
        self
    }

    /// Set target node label filter.
    pub fn with_to_label(mut self, label: impl Into<String>) -> Self {
        self.to_label = Some(label.into());
        self
    }

    /// Set target alias.
    pub fn with_target_alias(mut self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        self.schema = Self::build_schema(
            self.input.schema(),
            Some(&alias),
            self.edge_label.as_deref(),
        );
        self.target_alias = Some(alias);
        self
    }

    fn build_schema(
        input_schema: &PhysicalSchema,
        target_alias: Option<&str>,
        _edge_label: Option<&str>,
    ) -> PhysicalSchema {
        // Start with input schema
        let mut builder = PhysicalSchemaBuilder::new();

        // Copy input fields
        for field in input_schema.arrow_schema().fields() {
            let qualifier = input_schema.qualifier(field.name());
            if let Some(q) = qualifier {
                builder = builder.qualified_field(
                    field.name(),
                    field.data_type().clone(),
                    field.is_nullable(),
                    q,
                );
            } else {
                builder =
                    builder.field(field.name(), field.data_type().clone(), field.is_nullable());
            }
        }

        // Add target columns
        let target_qual = target_alias.unwrap_or("_target");
        builder = builder
            .qualified_field("_id", arrow::datatypes::DataType::Int64, false, target_qual)
            .qualified_field(
                "_label",
                arrow::datatypes::DataType::Utf8,
                true,
                target_qual,
            );

        builder.build()
    }

    /// Expand a single row to produce output rows.
    async fn expand_row(
        &self,
        _ctx: &ExecutionContext,
        _input_batch: &RecordBatch,
        _row_idx: usize,
    ) -> GrismResult<Option<RecordBatch>> {
        // TODO: Implement using RFC-0012 Storage::scan() with DatasetId::Adjacency
        // This requires scanning adjacency datasets with predicate pushdown
        Err(GrismError::not_implemented(
            "AdjacencyExpandExec requires RFC-0012 adjacency dataset support",
        ))
    }

    #[allow(dead_code)]
    async fn build_expand_output(
        &self,
        input_batch: &RecordBatch,
        row_idx: usize,
        target_ids: &[u64],
        target_labels: &[String],
    ) -> GrismResult<Option<RecordBatch>> {
        if target_ids.is_empty() {
            return Ok(None);
        }

        // Build arrays for output
        let num_rows = target_ids.len();
        let mut columns: Vec<ArrayRef> = Vec::new();

        // Replicate input columns for each target
        for col_idx in 0..input_batch.num_columns() {
            let col = input_batch.column(col_idx);
            let sliced = col.slice(row_idx, 1);
            // Create array with single value repeated by concatenating slices
            let slice_refs: Vec<&dyn Array> = (0..num_rows).map(|_| sliced.as_ref()).collect();
            let repeated = arrow::compute::concat(&slice_refs)
                .map_err(|e| GrismError::execution(e.to_string()))?;
            columns.push(repeated);
        }

        // Add target node IDs and labels
        let mut target_id_builder = Int64Array::builder(num_rows);
        let mut target_label_builder = StringBuilder::new();

        for (id, label) in target_ids.iter().zip(target_labels.iter()) {
            target_id_builder.append_value(*id as i64);
            target_label_builder.append_value(label);
        }

        columns.push(Arc::new(target_id_builder.finish()) as ArrayRef);
        columns.push(Arc::new(target_label_builder.finish()) as ArrayRef);

        RecordBatch::try_new(self.schema.arrow_schema().clone(), columns)
            .map_err(|e| GrismError::execution(e.to_string()))
            .map(Some)
    }
}

#[async_trait]
impl PhysicalOperator for AdjacencyExpandExec {
    fn name(&self) -> &'static str {
        "AdjacencyExpandExec"
    }

    fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps::expand()
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
        vec![&self.input]
    }

    async fn open(&self, ctx: &ExecutionContext) -> GrismResult<()> {
        let mut state = self.state.lock().await;
        *state = ExpandState {
            ctx: Some(ctx.clone()),
            ..Default::default()
        };
        self.input.open(ctx).await
    }

    async fn next(&self) -> GrismResult<Option<RecordBatch>> {
        let mut state = self.state.lock().await;

        // Return pending output first
        if !state.pending.is_empty() {
            return Ok(Some(state.pending.remove(0)));
        }

        let ctx = state
            .ctx
            .clone()
            .ok_or_else(|| GrismError::execution("Operator not opened"))?;

        loop {
            // Need new batch?
            if state.current_batch.is_none() {
                match self.input.next().await? {
                    Some(batch) => {
                        state.current_batch = Some(batch);
                        state.row_index = 0;
                    }
                    None => return Ok(None),
                }
            }

            // Get batch info and clone what we need before modifying state
            let (batch_clone, row_idx, num_rows) = {
                let batch = state.current_batch.as_ref().unwrap();
                (batch.clone(), state.row_index, batch.num_rows())
            };

            // Process next row
            if row_idx < num_rows {
                state.row_index += 1;

                // Drop the lock during the async expand operation
                drop(state);

                if let Some(expanded) = self.expand_row(&ctx, &batch_clone, row_idx).await? {
                    return Ok(Some(expanded));
                }

                // Re-acquire lock and continue
                state = self.state.lock().await;
            } else {
                // Batch exhausted
                state.current_batch = None;
            }
        }
    }

    async fn close(&self) -> GrismResult<()> {
        let mut state = self.state.lock().await;
        state.current_batch = None;
        state.pending.clear();
        state.ctx = None;
        self.input.close().await
    }

    fn display(&self) -> String {
        let mut parts = vec![format!("dir={}", self.direction)];
        if let Some(ref label) = self.edge_label {
            parts.push(format!("edge={label}"));
        }
        if let Some(ref label) = self.to_label {
            parts.push(format!("to={label}"));
        }
        if let Some(ref alias) = self.target_alias {
            parts.push(format!("as={alias}"));
        }
        format!("AdjacencyExpandExec({})", parts.join(", "))
    }
}

/// Role-based expand execution operator for n-ary hyperedges.
///
/// Used when:
/// - Any n-ary hyperedge traversal
/// - Role-qualified traversal
#[derive(Debug)]
pub struct RoleExpandExec {
    /// Input operator.
    input: Arc<dyn PhysicalOperator>,
    /// Source role for traversal.
    from_role: String,
    /// Target role for traversal.
    to_role: String,
    /// Hyperedge label filter.
    edge_label: Option<String>,
    /// Whether to materialize the hyperedge.
    materialize_edge: bool,
    /// Target alias for output columns.
    target_alias: Option<String>,
    /// Output schema.
    schema: PhysicalSchema,
    /// Execution state.
    state: tokio::sync::Mutex<RoleExpandState>,
}

/// State for role-based expand.
#[derive(Debug, Default)]
struct RoleExpandState {
    /// Current input batch being expanded.
    current_batch: Option<RecordBatch>,
    /// Current row index in the batch.
    row_index: usize,
    /// Execution context.
    ctx: Option<ExecutionContext>,
}

impl RoleExpandExec {
    /// Create a new role-based expand operator.
    pub fn new(
        input: Arc<dyn PhysicalOperator>,
        from_role: impl Into<String>,
        to_role: impl Into<String>,
    ) -> Self {
        let from_role = from_role.into();
        let to_role = to_role.into();
        let schema = Self::build_schema(input.schema(), None, &to_role);

        Self {
            input,
            from_role,
            to_role,
            edge_label: None,
            materialize_edge: false,
            target_alias: None,
            schema,
            state: tokio::sync::Mutex::new(RoleExpandState::default()),
        }
    }

    /// Set edge label filter.
    pub fn with_edge_label(mut self, label: impl Into<String>) -> Self {
        self.edge_label = Some(label.into());
        self
    }

    /// Enable hyperedge materialization.
    pub fn with_materialize(mut self) -> Self {
        self.materialize_edge = true;
        // Rebuild schema to include edge columns
        self.schema = Self::build_schema(
            self.input.schema(),
            self.target_alias.as_deref(),
            &self.to_role,
        );
        self
    }

    /// Set target alias for output columns.
    pub fn with_target_alias(mut self, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        self.schema = Self::build_schema(self.input.schema(), Some(&alias), &self.to_role);
        self.target_alias = Some(alias);
        self
    }

    fn build_schema(
        input_schema: &PhysicalSchema,
        target_alias: Option<&str>,
        to_role: &str,
    ) -> PhysicalSchema {
        let mut builder = PhysicalSchemaBuilder::new();

        // Copy input fields
        for field in input_schema.arrow_schema().fields() {
            let qualifier = input_schema.qualifier(field.name());
            if let Some(q) = qualifier {
                builder = builder.qualified_field(
                    field.name(),
                    field.data_type().clone(),
                    field.is_nullable(),
                    q,
                );
            } else {
                builder =
                    builder.field(field.name(), field.data_type().clone(), field.is_nullable());
            }
        }

        // Add target columns with qualifier
        let target_qual = target_alias.unwrap_or(to_role);
        builder = builder
            .qualified_field("_id", arrow::datatypes::DataType::Int64, false, target_qual)
            .qualified_field(
                "_label",
                arrow::datatypes::DataType::Utf8,
                true,
                target_qual,
            );

        builder.build()
    }

    /// Expand a single row to produce output rows.
    async fn expand_row(
        &self,
        _ctx: &ExecutionContext,
        _input_batch: &RecordBatch,
        _row_idx: usize,
    ) -> GrismResult<Option<RecordBatch>> {
        // TODO: Implement using RFC-0012 Storage::scan() with DatasetId::Hyperedges
        // and DatasetId::Adjacency for role-based expansion
        Err(GrismError::not_implemented(
            "RoleExpandExec requires RFC-0012 adjacency dataset support",
        ))
    }

    #[allow(dead_code)]
    async fn build_role_expand_output(
        &self,
        input_batch: &RecordBatch,
        row_idx: usize,
        target_ids: &[u64],
        target_labels: &[String],
    ) -> GrismResult<Option<RecordBatch>> {
        if target_ids.is_empty() {
            return Ok(None);
        }

        let num_rows = target_ids.len();
        let mut columns: Vec<ArrayRef> = Vec::new();

        // Replicate input columns for each target
        for col_idx in 0..input_batch.num_columns() {
            let col = input_batch.column(col_idx);
            let sliced = col.slice(row_idx, 1);
            let slice_refs: Vec<&dyn Array> = (0..num_rows).map(|_| sliced.as_ref()).collect();
            let repeated = arrow::compute::concat(&slice_refs)
                .map_err(|e| GrismError::execution(e.to_string()))?;
            columns.push(repeated);
        }

        // Add target node IDs and labels
        let mut target_id_builder = Int64Array::builder(num_rows);
        let mut target_label_builder = StringBuilder::new();

        for (id, label) in target_ids.iter().zip(target_labels.iter()) {
            target_id_builder.append_value(*id as i64);
            target_label_builder.append_value(label);
        }

        columns.push(Arc::new(target_id_builder.finish()) as ArrayRef);
        columns.push(Arc::new(target_label_builder.finish()) as ArrayRef);

        RecordBatch::try_new(self.schema.arrow_schema().clone(), columns)
            .map_err(|e| GrismError::execution(e.to_string()))
            .map(Some)
    }
}

#[async_trait]
impl PhysicalOperator for RoleExpandExec {
    fn name(&self) -> &'static str {
        "RoleExpandExec"
    }

    fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps::expand()
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
        vec![&self.input]
    }

    async fn open(&self, ctx: &ExecutionContext) -> GrismResult<()> {
        let mut state = self.state.lock().await;
        *state = RoleExpandState {
            ctx: Some(ctx.clone()),
            ..Default::default()
        };
        self.input.open(ctx).await
    }

    async fn next(&self) -> GrismResult<Option<RecordBatch>> {
        let mut state = self.state.lock().await;

        let ctx = state
            .ctx
            .clone()
            .ok_or_else(|| GrismError::execution("Operator not opened"))?;

        loop {
            // Need new batch?
            if state.current_batch.is_none() {
                match self.input.next().await? {
                    Some(batch) => {
                        state.current_batch = Some(batch);
                        state.row_index = 0;
                    }
                    None => return Ok(None),
                }
            }

            // Get batch info and clone what we need before modifying state
            let (batch_clone, row_idx, num_rows) = {
                let batch = state.current_batch.as_ref().unwrap();
                (batch.clone(), state.row_index, batch.num_rows())
            };

            // Process next row
            if row_idx < num_rows {
                state.row_index += 1;

                // Drop the lock during the async expand operation
                drop(state);

                if let Some(expanded) = self.expand_row(&ctx, &batch_clone, row_idx).await? {
                    return Ok(Some(expanded));
                }

                // Re-acquire lock and continue
                state = self.state.lock().await;
            } else {
                // Batch exhausted
                state.current_batch = None;
            }
        }
    }

    async fn close(&self) -> GrismResult<()> {
        let mut state = self.state.lock().await;
        state.current_batch = None;
        state.ctx = None;
        self.input.close().await
    }

    fn display(&self) -> String {
        let mut parts = vec![format!("{} -> {}", self.from_role, self.to_role)];
        if let Some(ref label) = self.edge_label {
            parts.push(format!("edge={label}"));
        }
        if self.materialize_edge {
            parts.push("materialize".to_string());
        }
        if let Some(ref alias) = self.target_alias {
            parts.push(format!("as={alias}"));
        }
        format!("RoleExpandExec({})", parts.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::EmptyExec;
    use grism_storage::{MemoryStorage, SnapshotId};

    #[tokio::test]
    async fn test_adjacency_expand_empty() {
        let input = Arc::new(EmptyExec::new());
        let expand = AdjacencyExpandExec::new(input, Direction::Outgoing);

        let storage = Arc::new(MemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        expand.open(&ctx).await.unwrap();
        // With empty input, next returns None
        assert!(expand.next().await.unwrap().is_none());
        expand.close().await.unwrap();
    }

    #[test]
    fn test_adjacency_expand_display() {
        let input = Arc::new(EmptyExec::new());
        let expand = AdjacencyExpandExec::new(input, Direction::Outgoing)
            .with_edge_label("KNOWS")
            .with_target_alias("friend");

        let display = expand.display();
        assert!(display.contains("KNOWS"));
        assert!(display.contains("friend"));
    }

    #[test]
    fn test_role_expand_display() {
        let input = Arc::new(EmptyExec::new());
        let expand = RoleExpandExec::new(input, "author", "paper").with_edge_label("AUTHORED");

        let display = expand.display();
        assert!(display.contains("author"));
        assert!(display.contains("paper"));
    }

    #[tokio::test]
    async fn test_role_expand_empty() {
        let input = Arc::new(EmptyExec::new());
        let expand = RoleExpandExec::new(input, "author", "paper");

        let storage = Arc::new(MemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        expand.open(&ctx).await.unwrap();
        // With empty input, next returns None
        assert!(expand.next().await.unwrap().is_none());
        expand.close().await.unwrap();
    }

    #[test]
    fn test_role_expand_with_target_alias() {
        let input = Arc::new(EmptyExec::new());
        let expand = RoleExpandExec::new(input, "author", "paper")
            .with_edge_label("AUTHORED")
            .with_target_alias("p");

        let display = expand.display();
        assert!(display.contains("as=p"));
    }

    #[test]
    fn test_role_expand_capabilities() {
        let input = Arc::new(EmptyExec::new());
        let expand = RoleExpandExec::new(input, "a", "b");

        let caps = expand.capabilities();
        // Expand operators have state (tracking position)
        assert!(!caps.stateless);
        // But are not blocking
        assert!(!caps.blocking);
    }

    // NOTE: test_role_expand_with_data is removed because expand operators
    // are currently stubs pending RFC-0012 adjacency dataset support.
    // The test will be reinstated once expand operators are fully implemented.
}
