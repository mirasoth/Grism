//! Project execution operator.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use common_error::{GrismError, GrismResult};
use grism_logical::LogicalExpr;

use crate::executor::ExecutionContext;
use crate::expr::ExprEvaluator;
use crate::metrics::{ExecutionTimer, OperatorMetrics};
use crate::operators::PhysicalOperator;
use crate::physical::{OperatorCaps, PhysicalSchema};

/// Project execution operator.
///
/// Evaluates projection expressions and produces output batch
/// with selected/computed columns.
#[derive(Debug)]
pub struct ProjectExec {
    /// Input operator.
    input: Arc<dyn PhysicalOperator>,
    /// Projection expressions with output names.
    projections: Vec<(LogicalExpr, String)>,
    /// Output schema.
    schema: PhysicalSchema,
    /// Accumulated metrics.
    metrics: tokio::sync::Mutex<OperatorMetrics>,
    /// Operator ID for metrics.
    #[allow(dead_code)]
    operator_id: String,
}

impl ProjectExec {
    /// Create a new project operator.
    #[must_use]
    pub fn new(
        input: Arc<dyn PhysicalOperator>,
        projections: Vec<(LogicalExpr, String)>,
        schema: PhysicalSchema,
    ) -> Self {
        let col_names: Vec<_> = projections.iter().map(|(_, n)| n.as_str()).collect();
        let operator_id = format!("ProjectExec[{}]", col_names.join(", "));

        Self {
            input,
            projections,
            schema,
            metrics: tokio::sync::Mutex::new(OperatorMetrics::default()),
            operator_id,
        }
    }

    /// Create a simple column projection (selecting existing columns).
    pub fn columns(
        input: Arc<dyn PhysicalOperator>,
        column_names: Vec<String>,
    ) -> GrismResult<Self> {
        // Build projections from column references
        let projections: Vec<_> = column_names
            .iter()
            .map(|name| (grism_logical::expr::col(name), name.clone()))
            .collect();

        // Build schema from input schema
        let input_schema = input.schema();
        let indices: Vec<_> = column_names
            .iter()
            .filter_map(|name| {
                input_schema
                    .arrow_schema()
                    .fields()
                    .iter()
                    .position(|f| f.name() == name)
            })
            .collect();

        let schema = input_schema.project(&indices);

        Ok(Self::new(input, projections, schema))
    }

    /// Get the projection expressions.
    pub fn projections(&self) -> &[(LogicalExpr, String)] {
        &self.projections
    }

    /// Project a batch using the projection expressions.
    fn project_batch(&self, batch: &RecordBatch) -> GrismResult<RecordBatch> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.projections.len());

        for (expr, _name) in &self.projections {
            let array = self.evaluate_expr(expr, batch)?;
            columns.push(array);
        }

        RecordBatch::try_new(self.schema.arrow_schema().clone(), columns)
            .map_err(|e| GrismError::execution(e.to_string()))
    }

    /// Evaluate an expression against a batch.
    fn evaluate_expr(&self, expr: &LogicalExpr, batch: &RecordBatch) -> GrismResult<ArrayRef> {
        let evaluator = ExprEvaluator::new();
        evaluator.evaluate(expr, batch)
    }
}

#[async_trait]
impl PhysicalOperator for ProjectExec {
    fn name(&self) -> &'static str {
        "ProjectExec"
    }

    fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps::streaming()
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
        vec![&self.input]
    }

    async fn open(&self, ctx: &ExecutionContext) -> GrismResult<()> {
        self.input.open(ctx).await
    }

    async fn next(&self) -> GrismResult<Option<RecordBatch>> {
        let timer = ExecutionTimer::start();

        match self.input.next().await? {
            Some(batch) => {
                let input_rows = batch.num_rows();
                let projected = self.project_batch(&batch)?;

                // Update metrics
                {
                    let mut metrics = self.metrics.lock().await;
                    metrics.add_rows_in(input_rows);
                    metrics.add_rows_out(projected.num_rows());
                    metrics.add_batch();
                    metrics.add_time(timer.elapsed());
                }

                Ok(Some(projected))
            }
            None => Ok(None),
        }
    }

    async fn close(&self) -> GrismResult<()> {
        self.input.close().await
    }

    fn display(&self) -> String {
        let cols: Vec<_> = self
            .projections
            .iter()
            .map(|(_, name)| name.as_str())
            .collect();
        format!("ProjectExec({})", cols.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::EmptyExec;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use grism_logical::expr::{col, lit};
    use grism_storage::{InMemoryStorage, SnapshotId};

    /// Helper to create a mock input operator that returns a single batch.
    struct MockInputOp {
        batch: Option<RecordBatch>,
        returned: tokio::sync::Mutex<bool>,
        schema: PhysicalSchema,
    }

    impl MockInputOp {
        fn new(batch: RecordBatch) -> Self {
            let schema = PhysicalSchema::new(batch.schema());
            Self {
                batch: Some(batch),
                returned: tokio::sync::Mutex::new(false),
                schema,
            }
        }
    }

    impl std::fmt::Debug for MockInputOp {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockInputOp").finish()
        }
    }

    #[async_trait]
    impl PhysicalOperator for MockInputOp {
        fn name(&self) -> &'static str {
            "MockInputOp"
        }

        fn schema(&self) -> &PhysicalSchema {
            &self.schema
        }

        fn capabilities(&self) -> OperatorCaps {
            OperatorCaps::streaming()
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
            vec![]
        }

        async fn open(&self, _ctx: &ExecutionContext) -> GrismResult<()> {
            Ok(())
        }

        async fn next(&self) -> GrismResult<Option<RecordBatch>> {
            let mut returned = self.returned.lock().await;
            if *returned {
                return Ok(None);
            }
            *returned = true;
            Ok(self.batch.clone())
        }

        async fn close(&self) -> GrismResult<()> {
            Ok(())
        }

        fn display(&self) -> String {
            "MockInputOp".to_string()
        }
    }

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Int64, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_project_empty_input() {
        let input = Arc::new(EmptyExec::new());
        let schema = PhysicalSchema::empty();
        let project = ProjectExec::new(input, vec![], schema);

        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        project.open(&ctx).await.unwrap();
        assert!(project.next().await.unwrap().is_none());
        project.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_project_column_selection() {
        let batch = create_test_batch();
        let input: Arc<dyn PhysicalOperator> = Arc::new(MockInputOp::new(batch));

        let project = ProjectExec::columns(input, vec!["id".to_string(), "x".to_string()]).unwrap();

        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        project.open(&ctx).await.unwrap();
        let result = project.next().await.unwrap().unwrap();

        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.schema().field(0).name(), "id");
        assert_eq!(result.schema().field(1).name(), "x");

        project.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_project_computed_column() {
        let batch = create_test_batch();
        let input: Arc<dyn PhysicalOperator> = Arc::new(MockInputOp::new(batch));

        // Compute x + y
        let computed_expr = col("x").add_expr(col("y"));
        let projections = vec![
            (col("id").into(), "id".to_string()),
            (computed_expr.into(), "sum".to_string()),
        ];

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("sum", DataType::Int64, true),
        ]));
        let schema = PhysicalSchema::new(output_schema);

        let project = ProjectExec::new(input, projections, schema);

        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        project.open(&ctx).await.unwrap();
        let result = project.next().await.unwrap().unwrap();

        assert_eq!(result.num_columns(), 2);
        let sum_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(sum_col.value(0), 110); // 10 + 100
        assert_eq!(sum_col.value(1), 220); // 20 + 200
        assert_eq!(sum_col.value(2), 330); // 30 + 300

        project.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_project_with_literal() {
        let batch = create_test_batch();
        let input: Arc<dyn PhysicalOperator> = Arc::new(MockInputOp::new(batch));

        // Project id and a literal constant
        let projections = vec![
            (col("id").into(), "id".to_string()),
            (lit(42i64).into(), "constant".to_string()),
        ];

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("constant", DataType::Int64, false),
        ]));
        let schema = PhysicalSchema::new(output_schema);

        let project = ProjectExec::new(input, projections, schema);

        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        project.open(&ctx).await.unwrap();
        let result = project.next().await.unwrap().unwrap();

        let const_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(const_col.value(0), 42);
        assert_eq!(const_col.value(1), 42);
        assert_eq!(const_col.value(2), 42);

        project.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_project_arithmetic_expression() {
        let batch = create_test_batch();
        let input: Arc<dyn PhysicalOperator> = Arc::new(MockInputOp::new(batch));

        // Compute x * 2 + y
        let computed_expr = col("x").mul_expr(lit(2i64)).add_expr(col("y"));
        let projections = vec![(computed_expr.into(), "result".to_string())];

        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::Int64,
            true,
        )]));
        let schema = PhysicalSchema::new(output_schema);

        let project = ProjectExec::new(input, projections, schema);

        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        project.open(&ctx).await.unwrap();
        let result = project.next().await.unwrap().unwrap();

        let result_col = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(result_col.value(0), 120); // 10*2 + 100
        assert_eq!(result_col.value(1), 240); // 20*2 + 200
        assert_eq!(result_col.value(2), 360); // 30*2 + 300

        project.close().await.unwrap();
    }

    #[test]
    fn test_project_capabilities() {
        let input = Arc::new(EmptyExec::new());
        let schema = PhysicalSchema::empty();
        let project = ProjectExec::new(input, vec![], schema);

        let caps = project.capabilities();
        assert!(!caps.blocking);
    }
}
