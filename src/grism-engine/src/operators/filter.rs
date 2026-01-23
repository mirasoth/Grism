//! Filter execution operator.

use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use common_error::{GrismError, GrismResult};
use grism_logical::LogicalExpr;

use crate::executor::ExecutionContext;
use crate::expr::ExprEvaluator;
use crate::metrics::{ExecutionTimer, OperatorMetrics};
use crate::operators::PhysicalOperator;
use crate::physical::{OperatorCaps, PhysicalSchema};

/// Filter execution operator.
///
/// Applies a predicate expression to each batch, filtering out rows
/// that don't satisfy the predicate.
#[derive(Debug)]
pub struct FilterExec {
    /// Input operator.
    input: Arc<dyn PhysicalOperator>,
    /// Filter predicate.
    predicate: LogicalExpr,
    /// Output schema (same as input).
    schema: PhysicalSchema,
    /// Accumulated metrics.
    metrics: tokio::sync::Mutex<OperatorMetrics>,
    /// Operator ID for metrics.
    #[allow(dead_code)]
    operator_id: String,
}

impl FilterExec {
    /// Create a new filter operator.
    #[must_use]
    pub fn new(input: Arc<dyn PhysicalOperator>, predicate: LogicalExpr) -> Self {
        let schema = input.schema().clone();
        let operator_id = format!("FilterExec[{predicate}]");

        Self {
            input,
            predicate,
            schema,
            metrics: tokio::sync::Mutex::new(OperatorMetrics::default()),
            operator_id,
        }
    }

    /// Get the predicate expression.
    pub fn predicate(&self) -> &LogicalExpr {
        &self.predicate
    }

    /// Evaluate predicate and filter batch.
    fn filter_batch(&self, batch: &RecordBatch) -> GrismResult<RecordBatch> {
        // Evaluate predicate to boolean array
        let predicate_array = self.evaluate_predicate(batch)?;

        // Apply filter
        filter_record_batch(batch, &predicate_array)
            .map_err(|e| GrismError::execution(e.to_string()))
    }

    /// Evaluate the predicate expression against a batch.
    fn evaluate_predicate(&self, batch: &RecordBatch) -> GrismResult<BooleanArray> {
        let evaluator = ExprEvaluator::new();
        evaluator.evaluate_predicate(&self.predicate, batch)
    }
}

#[async_trait]
impl PhysicalOperator for FilterExec {
    fn name(&self) -> &'static str {
        "FilterExec"
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

        loop {
            match self.input.next().await? {
                Some(batch) => {
                    let input_rows = batch.num_rows();

                    let filtered = self.filter_batch(&batch)?;
                    let output_rows = filtered.num_rows();

                    // Update metrics
                    {
                        let mut metrics = self.metrics.lock().await;
                        metrics.add_rows_in(input_rows);
                        metrics.add_rows_out(output_rows);
                        metrics.add_batch();
                        metrics.add_time(timer.elapsed());
                    }

                    // Skip empty batches
                    if output_rows > 0 {
                        return Ok(Some(filtered));
                    }
                    // Continue to next input batch if all rows filtered
                }
                None => return Ok(None),
            }
        }
    }

    async fn close(&self) -> GrismResult<()> {
        self.input.close().await
    }

    fn display(&self) -> String {
        format!("FilterExec({})", self.predicate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::EmptyExec;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use grism_logical::expr::{col, lit};
    use grism_storage::{MemoryStorage, SnapshotId};

    /// Helper to create a mock input operator that returns a single batch.
    struct MockInputOp {
        batch: Option<RecordBatch>,
        returned: tokio::sync::Mutex<bool>,
    }

    impl MockInputOp {
        fn new(batch: RecordBatch) -> Self {
            Self {
                batch: Some(batch),
                returned: tokio::sync::Mutex::new(false),
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
            static SCHEMA: std::sync::OnceLock<PhysicalSchema> = std::sync::OnceLock::new();
            SCHEMA.get_or_init(|| {
                let arrow_schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("value", DataType::Int64, true),
                ]));
                PhysicalSchema::new(arrow_schema)
            })
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
            Field::new("value", DataType::Int64, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_filter_empty_input() {
        let input: Arc<dyn PhysicalOperator> = Arc::new(EmptyExec::new());
        let predicate = lit(true);
        let filter = FilterExec::new(input, predicate);

        let storage = Arc::new(MemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        filter.open(&ctx).await.unwrap();
        assert!(filter.next().await.unwrap().is_none());
        filter.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_filter_true_predicate() {
        let batch = create_test_batch();
        let input: Arc<dyn PhysicalOperator> = Arc::new(MockInputOp::new(batch.clone()));
        let predicate = lit(true);
        let filter = FilterExec::new(input, predicate);

        let storage = Arc::new(MemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        filter.open(&ctx).await.unwrap();
        let result = filter.next().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 5); // All rows pass
        assert!(filter.next().await.unwrap().is_none());
        filter.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_filter_false_predicate() {
        let batch = create_test_batch();
        let input: Arc<dyn PhysicalOperator> = Arc::new(MockInputOp::new(batch));
        let predicate = lit(false);
        let filter = FilterExec::new(input, predicate);

        let storage = Arc::new(MemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        filter.open(&ctx).await.unwrap();
        // All rows filtered out
        assert!(filter.next().await.unwrap().is_none());
        filter.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_filter_comparison_predicate() {
        let batch = create_test_batch();
        let input: Arc<dyn PhysicalOperator> = Arc::new(MockInputOp::new(batch));

        // value > 25 should keep rows where value is 30, 40, 50 (3 rows)
        let predicate = col("value").gt(lit(25i64));
        let filter = FilterExec::new(input, predicate);

        let storage = Arc::new(MemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        filter.open(&ctx).await.unwrap();
        let result = filter.next().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 3);

        // Verify filtered values
        let values = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 30);
        assert_eq!(values.value(1), 40);
        assert_eq!(values.value(2), 50);

        filter.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_filter_equality_predicate() {
        let batch = create_test_batch();
        let input: Arc<dyn PhysicalOperator> = Arc::new(MockInputOp::new(batch));

        // id = 3 should keep only 1 row
        let predicate = col("id").eq(lit(3i64));
        let filter = FilterExec::new(input, predicate);

        let storage = Arc::new(MemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        filter.open(&ctx).await.unwrap();
        let result = filter.next().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 1);

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 3);

        filter.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_filter_complex_predicate() {
        let batch = create_test_batch();
        let input: Arc<dyn PhysicalOperator> = Arc::new(MockInputOp::new(batch));

        // (id > 2) AND (value < 45)
        // Should keep: id=3 value=30, id=4 value=40 (2 rows)
        let predicate = col("id").gt(lit(2i64)).and(col("value").lt(lit(45i64)));
        let filter = FilterExec::new(input, predicate);

        let storage = Arc::new(MemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        filter.open(&ctx).await.unwrap();
        let result = filter.next().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 2);

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 3);
        assert_eq!(ids.value(1), 4);

        filter.close().await.unwrap();
    }

    #[test]
    fn test_filter_capabilities() {
        let input: Arc<dyn PhysicalOperator> = Arc::new(EmptyExec::new());
        let predicate = lit(true);
        let filter = FilterExec::new(input, predicate);

        let caps = filter.capabilities();
        assert!(!caps.blocking);
        assert!(caps.stateless);
    }

    #[test]
    fn test_filter_schema() {
        let input: Arc<dyn PhysicalOperator> = Arc::new(EmptyExec::new());
        let input_cols = input.schema().num_columns();
        let predicate = lit(true);
        let filter = FilterExec::new(input, predicate);

        // Schema should match input
        assert_eq!(filter.schema().num_columns(), input_cols);
    }
}
