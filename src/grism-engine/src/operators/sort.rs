//! Sort execution operator.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch as ArrowRecordBatch, UInt64Array};
use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use common_error::{GrismError, GrismResult};
use grism_logical::SortKey;

use crate::executor::ExecutionContext;
use crate::expr::ExprEvaluator;
use crate::operators::PhysicalOperator;
use crate::physical::{OperatorCaps, PhysicalSchema};

/// Sort execution operator.
///
/// This is a **blocking** operator that must consume all input
/// before producing sorted output.
#[derive(Debug)]
pub struct SortExec {
    /// Input operator.
    input: Arc<dyn PhysicalOperator>,
    /// Sort keys.
    keys: Vec<SortKey>,
    /// Output schema (same as input).
    schema: PhysicalSchema,
    /// Execution state.
    state: tokio::sync::Mutex<SortState>,
}

#[derive(Debug, Default)]
enum SortState {
    #[default]
    Uninitialized,
    Sorting,
    Ready(Vec<RecordBatch>),
    Exhausted,
}

impl SortExec {
    /// Create a new sort operator.
    pub fn new(input: Arc<dyn PhysicalOperator>, keys: Vec<SortKey>) -> Self {
        let schema = input.schema().clone();
        Self {
            input,
            keys,
            schema,
            state: tokio::sync::Mutex::new(SortState::Uninitialized),
        }
    }

    /// Get sort keys.
    pub fn keys(&self) -> &[SortKey] {
        &self.keys
    }

    /// Sort the collected batches.
    fn sort_batches(&self, batches: Vec<RecordBatch>) -> GrismResult<Vec<RecordBatch>> {
        if batches.is_empty() {
            return Ok(vec![]);
        }

        // No sort keys means no sorting needed
        if self.keys.is_empty() {
            return Ok(batches);
        }

        // Concatenate all batches into one
        let schema = batches[0].schema();
        let combined = concat_batches(&schema, &batches)
            .map_err(|e| GrismError::execution(format!("Failed to concatenate batches: {}", e)))?;

        if combined.num_rows() == 0 {
            return Ok(vec![combined]);
        }

        // Build sort columns
        let evaluator = ExprEvaluator::new();
        let mut sort_columns: Vec<SortColumn> = Vec::with_capacity(self.keys.len());

        for key in &self.keys {
            // Evaluate the sort expression
            let values = evaluator.evaluate(&key.expr, &combined)?;

            let options = SortOptions {
                descending: !key.ascending, // ascending=true means descending=false
                nulls_first: key.nulls_first,
            };

            sort_columns.push(SortColumn {
                values,
                options: Some(options),
            });
        }

        // Get sort indices
        let indices = lexsort_to_indices(&sort_columns, None)
            .map_err(|e| GrismError::execution(format!("Failed to sort: {}", e)))?;

        // Reorder all columns using the indices
        let sorted_columns: Vec<_> = combined
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| GrismError::execution(format!("Failed to reorder columns: {}", e)))?;

        let sorted_batch = RecordBatch::try_new(schema, sorted_columns)
            .map_err(|e| GrismError::execution(format!("Failed to create sorted batch: {}", e)))?;

        Ok(vec![sorted_batch])
    }
}

#[async_trait]
impl PhysicalOperator for SortExec {
    fn name(&self) -> &'static str {
        "SortExec"
    }

    fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps::blocking()
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
        vec![&self.input]
    }

    async fn open(&self, ctx: &ExecutionContext) -> GrismResult<()> {
        let mut state = self.state.lock().await;
        *state = SortState::Sorting;
        self.input.open(ctx).await
    }

    async fn next(&self) -> GrismResult<Option<RecordBatch>> {
        let mut state = self.state.lock().await;

        match &mut *state {
            SortState::Uninitialized => Err(GrismError::execution("Operator not opened")),
            SortState::Sorting => {
                // Consume all input
                let mut batches = Vec::new();
                while let Some(batch) = self.input.next().await? {
                    batches.push(batch);
                }

                // Perform sorting
                let sorted = self.sort_batches(batches)?;

                *state = SortState::Ready(sorted);

                // Recurse to emit
                drop(state);
                self.next().await
            }
            SortState::Ready(batches) => {
                if batches.is_empty() {
                    *state = SortState::Exhausted;
                    Ok(None)
                } else {
                    Ok(Some(batches.remove(0)))
                }
            }
            SortState::Exhausted => Ok(None),
        }
    }

    async fn close(&self) -> GrismResult<()> {
        let mut state = self.state.lock().await;
        *state = SortState::Exhausted;
        self.input.close().await
    }

    fn display(&self) -> String {
        let keys: Vec<_> = self.keys.iter().map(|k| format!("{}", k)).collect();
        format!("SortExec({})", keys.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::EmptyExec;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use grism_logical::expr::col;
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
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![3, 1, 4, 1, 5])),
                Arc::new(StringArray::from(vec![
                    Some("charlie"),
                    Some("alice"),
                    Some("dave"),
                    Some("bob"),
                    Some("eve"),
                ])),
                Arc::new(Int64Array::from(vec![300, 100, 400, 150, 500])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_sort_empty() {
        let input = Arc::new(EmptyExec::new());
        let sort = SortExec::new(input, vec![]);

        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        sort.open(&ctx).await.unwrap();
        assert!(sort.next().await.unwrap().is_none());
        sort.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_sort_ascending() {
        let batch = create_test_batch();
        let input: Arc<dyn PhysicalOperator> = Arc::new(MockInputOp::new(batch));

        // Sort by id ascending
        let keys = vec![SortKey {
            expr: col("id").into(),
            ascending: true,
            nulls_first: false,
        }];

        let sort = SortExec::new(input, keys);

        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        sort.open(&ctx).await.unwrap();
        let result = sort.next().await.unwrap().unwrap();

        assert_eq!(result.num_rows(), 5);

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Should be: 1, 1, 3, 4, 5
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 1);
        assert_eq!(ids.value(2), 3);
        assert_eq!(ids.value(3), 4);
        assert_eq!(ids.value(4), 5);

        sort.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_sort_descending() {
        let batch = create_test_batch();
        let input: Arc<dyn PhysicalOperator> = Arc::new(MockInputOp::new(batch));

        // Sort by value descending (ascending = false)
        let keys = vec![SortKey {
            expr: col("value").into(),
            ascending: false,
            nulls_first: false,
        }];

        let sort = SortExec::new(input, keys);

        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        sort.open(&ctx).await.unwrap();
        let result = sort.next().await.unwrap().unwrap();

        let values = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Should be: 500, 400, 300, 150, 100 (descending)
        assert_eq!(values.value(0), 500);
        assert_eq!(values.value(1), 400);
        assert_eq!(values.value(2), 300);
        assert_eq!(values.value(3), 150);
        assert_eq!(values.value(4), 100);

        sort.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_sort_multi_key() {
        let batch = create_test_batch();
        let input: Arc<dyn PhysicalOperator> = Arc::new(MockInputOp::new(batch));

        // Sort by id ascending, then value descending
        let keys = vec![
            SortKey {
                expr: col("id").into(),
                ascending: true,
                nulls_first: false,
            },
            SortKey {
                expr: col("value").into(),
                ascending: false, // descending
                nulls_first: false,
            },
        ];

        let sort = SortExec::new(input, keys);

        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        sort.open(&ctx).await.unwrap();
        let result = sort.next().await.unwrap().unwrap();

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // For id=1: values should be 150, 100 (descending)
        assert_eq!(ids.value(0), 1);
        assert_eq!(values.value(0), 150);
        assert_eq!(ids.value(1), 1);
        assert_eq!(values.value(1), 100);

        // Rest in order
        assert_eq!(ids.value(2), 3);
        assert_eq!(ids.value(3), 4);
        assert_eq!(ids.value(4), 5);

        sort.close().await.unwrap();
    }

    #[test]
    fn test_sort_capabilities() {
        let input = Arc::new(EmptyExec::new());
        let sort = SortExec::new(input, vec![]);

        let caps = sort.capabilities();
        assert!(caps.blocking);
    }
}
