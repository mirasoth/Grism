//! Union execution operator.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use common_error::GrismResult;

use crate::executor::ExecutionContext;
use crate::operators::PhysicalOperator;
use crate::physical::{OperatorCaps, PhysicalSchema};

/// Union execution operator.
///
/// Combines output from two input operators.
#[derive(Debug)]
pub struct UnionExec {
    /// Left input operator.
    left: Arc<dyn PhysicalOperator>,
    /// Right input operator.
    right: Arc<dyn PhysicalOperator>,
    /// Whether to keep all rows (true) or deduplicate (false).
    all: bool,
    /// Output schema.
    schema: PhysicalSchema,
    /// Whether left input is exhausted.
    left_exhausted: AtomicBool,
}

impl UnionExec {
    /// Create a new union operator.
    pub fn new(
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
        all: bool,
    ) -> Self {
        // Use left schema as output schema
        let schema = left.schema().clone();
        Self {
            left,
            right,
            all,
            schema,
            left_exhausted: AtomicBool::new(false),
        }
    }

    /// Create a UNION ALL operator.
    pub fn all(left: Arc<dyn PhysicalOperator>, right: Arc<dyn PhysicalOperator>) -> Self {
        Self::new(left, right, true)
    }

    /// Create a UNION DISTINCT operator.
    pub fn distinct(left: Arc<dyn PhysicalOperator>, right: Arc<dyn PhysicalOperator>) -> Self {
        Self::new(left, right, false)
    }
}

#[async_trait]
impl PhysicalOperator for UnionExec {
    fn name(&self) -> &'static str {
        "UnionExec"
    }

    fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps::streaming()
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
        vec![&self.left, &self.right]
    }

    async fn open(&self, ctx: &ExecutionContext) -> GrismResult<()> {
        self.left_exhausted.store(false, Ordering::SeqCst);
        self.left.open(ctx).await?;
        self.right.open(ctx).await
    }

    async fn next(&self) -> GrismResult<Option<RecordBatch>> {
        // First exhaust left, then right
        if !self.left_exhausted.load(Ordering::SeqCst) {
            match self.left.next().await? {
                Some(batch) => return Ok(Some(batch)),
                None => {
                    self.left_exhausted.store(true, Ordering::SeqCst);
                }
            }
        }

        // Left exhausted, read from right
        self.right.next().await
    }

    async fn close(&self) -> GrismResult<()> {
        self.left.close().await?;
        self.right.close().await
    }

    fn display(&self) -> String {
        if self.all {
            "UnionExec(all)".to_string()
        } else {
            "UnionExec(distinct)".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::EmptyExec;
    use grism_storage::{MemoryStorage, SnapshotId};

    #[tokio::test]
    async fn test_union_empty() {
        let left = Arc::new(EmptyExec::new());
        let right = Arc::new(EmptyExec::new());
        let union = UnionExec::all(left, right);

        let storage = Arc::new(MemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        union.open(&ctx).await.unwrap();
        assert!(union.next().await.unwrap().is_none());
        union.close().await.unwrap();
    }

    #[test]
    fn test_union_children() {
        let left = Arc::new(EmptyExec::new());
        let right = Arc::new(EmptyExec::new());
        let union = UnionExec::all(left, right);

        assert_eq!(union.children().len(), 2);
    }
}
