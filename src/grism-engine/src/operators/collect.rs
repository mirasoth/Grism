//! Collect execution operator.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use common_error::{GrismError, GrismResult};

use crate::executor::ExecutionContext;
use crate::operators::PhysicalOperator;
use crate::physical::{OperatorCaps, PhysicalSchema};

/// Collect execution operator.
///
/// A **terminal/sink** operator that collects all input into memory.
/// This is typically the root of execution plans that need to
/// materialize results.
#[derive(Debug)]
pub struct CollectExec {
    /// Input operator.
    input: Arc<dyn PhysicalOperator>,
    /// Output schema.
    schema: PhysicalSchema,
    /// Collected batches.
    state: tokio::sync::Mutex<CollectState>,
}

#[derive(Debug, Default)]
enum CollectState {
    #[default]
    Uninitialized,
    Collecting,
    Ready(Vec<RecordBatch>),
    Exhausted,
}

impl CollectExec {
    /// Create a new collect operator.
    pub fn new(input: Arc<dyn PhysicalOperator>) -> Self {
        let schema = input.schema().clone();
        Self {
            input,
            schema,
            state: tokio::sync::Mutex::new(CollectState::Uninitialized),
        }
    }
}

#[async_trait]
impl PhysicalOperator for CollectExec {
    fn name(&self) -> &'static str {
        "CollectExec"
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
        *state = CollectState::Collecting;
        self.input.open(ctx).await
    }

    async fn next(&self) -> GrismResult<Option<RecordBatch>> {
        let mut state = self.state.lock().await;

        match &mut *state {
            CollectState::Uninitialized => Err(GrismError::execution("Operator not opened")),
            CollectState::Collecting => {
                // Collect all input batches
                let mut batches = Vec::new();
                while let Some(batch) = self.input.next().await? {
                    batches.push(batch);
                }

                *state = CollectState::Ready(batches);

                // Recurse to emit
                drop(state);
                self.next().await
            }
            CollectState::Ready(batches) => {
                if batches.is_empty() {
                    *state = CollectState::Exhausted;
                    Ok(None)
                } else {
                    Ok(Some(batches.remove(0)))
                }
            }
            CollectState::Exhausted => Ok(None),
        }
    }

    async fn close(&self) -> GrismResult<()> {
        let mut state = self.state.lock().await;
        *state = CollectState::Exhausted;
        self.input.close().await
    }

    fn display(&self) -> String {
        "CollectExec".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::EmptyExec;
    use grism_storage::{MemoryStorage, SnapshotId};

    #[tokio::test]
    async fn test_collect_empty() {
        let input = Arc::new(EmptyExec::new());
        let collect = CollectExec::new(input);

        let storage = Arc::new(MemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        collect.open(&ctx).await.unwrap();
        assert!(collect.next().await.unwrap().is_none());
        collect.close().await.unwrap();
    }

    #[test]
    fn test_collect_capabilities() {
        let input = Arc::new(EmptyExec::new());
        let collect = CollectExec::new(input);

        let caps = collect.capabilities();
        assert!(caps.blocking);
    }
}
