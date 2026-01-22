//! Limit execution operator.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use common_error::GrismResult;

use crate::executor::ExecutionContext;
use crate::operators::PhysicalOperator;
use crate::physical::{OperatorCaps, PhysicalSchema};

/// Limit execution operator.
///
/// Limits output to a maximum number of rows, with optional offset.
#[derive(Debug)]
pub struct LimitExec {
    /// Input operator.
    input: Arc<dyn PhysicalOperator>,
    /// Maximum number of rows to return.
    limit: usize,
    /// Number of rows to skip.
    offset: usize,
    /// Output schema (same as input).
    schema: PhysicalSchema,
    /// Rows returned so far.
    rows_returned: AtomicUsize,
    /// Rows skipped so far.
    rows_skipped: AtomicUsize,
}

impl LimitExec {
    /// Create a new limit operator.
    #[must_use]
    pub fn new(input: Arc<dyn PhysicalOperator>, limit: usize) -> Self {
        let schema = input.schema().clone();
        Self {
            input,
            limit,
            offset: 0,
            schema,
            rows_returned: AtomicUsize::new(0),
            rows_skipped: AtomicUsize::new(0),
        }
    }

    /// Create with offset.
    #[must_use]
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    /// Get the limit.
    pub const fn limit(&self) -> usize {
        self.limit
    }

    /// Get the offset.
    pub const fn offset(&self) -> usize {
        self.offset
    }
}

#[async_trait]
impl PhysicalOperator for LimitExec {
    fn name(&self) -> &'static str {
        "LimitExec"
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
        self.rows_returned.store(0, Ordering::SeqCst);
        self.rows_skipped.store(0, Ordering::SeqCst);
        self.input.open(ctx).await
    }

    async fn next(&self) -> GrismResult<Option<RecordBatch>> {
        // Check if we've returned enough rows
        if self.rows_returned.load(Ordering::SeqCst) >= self.limit {
            return Ok(None);
        }

        while let Some(batch) = self.input.next().await? {
            let batch_rows = batch.num_rows();

            // Handle offset
            let rows_skipped = self.rows_skipped.load(Ordering::SeqCst);
            if rows_skipped < self.offset {
                let to_skip = (self.offset - rows_skipped).min(batch_rows);
                self.rows_skipped.fetch_add(to_skip, Ordering::SeqCst);

                if to_skip == batch_rows {
                    continue; // Skip entire batch
                }

                // Slice batch to skip offset rows
                let batch = batch.slice(to_skip, batch_rows - to_skip);
                return self.process_batch(batch);
            }

            return self.process_batch(batch);
        }

        Ok(None)
    }

    async fn close(&self) -> GrismResult<()> {
        self.input.close().await
    }

    fn display(&self) -> String {
        if self.offset > 0 {
            format!("LimitExec(limit={}, offset={})", self.limit, self.offset)
        } else {
            format!("LimitExec({})", self.limit)
        }
    }
}

impl LimitExec {
    /// Process a batch, applying the limit.
    fn process_batch(&self, batch: RecordBatch) -> GrismResult<Option<RecordBatch>> {
        let rows_returned = self.rows_returned.load(Ordering::SeqCst);
        let remaining = self.limit - rows_returned;

        if remaining == 0 {
            return Ok(None);
        }

        let to_return = batch.num_rows().min(remaining);
        self.rows_returned.fetch_add(to_return, Ordering::SeqCst);

        if to_return < batch.num_rows() {
            Ok(Some(batch.slice(0, to_return)))
        } else {
            Ok(Some(batch))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::EmptyExec;
    use grism_storage::{InMemoryStorage, SnapshotId};

    #[tokio::test]
    async fn test_limit_empty_input() {
        let input = Arc::new(EmptyExec::new());
        let limit = LimitExec::new(input, 10);

        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        limit.open(&ctx).await.unwrap();
        assert!(limit.next().await.unwrap().is_none());
        limit.close().await.unwrap();
    }

    #[test]
    fn test_limit_display() {
        let input = Arc::new(EmptyExec::new());
        let limit = LimitExec::new(input.clone(), 10);
        assert_eq!(limit.display(), "LimitExec(10)");

        let limit_offset = LimitExec::new(input, 10).with_offset(5);
        assert_eq!(limit_offset.display(), "LimitExec(limit=10, offset=5)");
    }

    #[test]
    fn test_limit_capabilities() {
        let input = Arc::new(EmptyExec::new());
        let limit = LimitExec::new(input, 10);

        let caps = limit.capabilities();
        assert!(!caps.blocking);
    }
}
