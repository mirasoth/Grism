//! Empty execution operator.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use common_error::GrismResult;

use crate::executor::ExecutionContext;
use crate::operators::{OperatorState, PhysicalOperator};
use crate::physical::{OperatorCaps, PhysicalSchema};

/// Empty execution operator that produces no rows.
///
/// Used for:
/// - Queries that evaluate to empty results at planning time
/// - Filter predicates that are always false
/// - Test purposes
#[derive(Debug)]
pub struct EmptyExec {
    schema: PhysicalSchema,
    state: std::sync::Mutex<OperatorState>,
}

impl EmptyExec {
    /// Create a new empty operator with empty schema.
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema: PhysicalSchema::empty(),
            state: std::sync::Mutex::new(OperatorState::Uninitialized),
        }
    }

    /// Create with a specific schema.
    #[must_use]
    pub fn with_schema(schema: PhysicalSchema) -> Self {
        Self {
            schema,
            state: std::sync::Mutex::new(OperatorState::Uninitialized),
        }
    }
}

impl Default for EmptyExec {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PhysicalOperator for EmptyExec {
    fn name(&self) -> &'static str {
        "EmptyExec"
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

    async fn open(&self, _ctx: &ExecutionContext) -> GrismResult<()> {
        let mut state = self.state.lock().unwrap();
        *state = OperatorState::Exhausted; // Immediately exhausted
        Ok(())
    }

    async fn next(&self) -> GrismResult<Option<RecordBatch>> {
        Ok(None) // Always returns None
    }

    async fn close(&self) -> GrismResult<()> {
        let mut state = self.state.lock().unwrap();
        *state = OperatorState::Closed;
        Ok(())
    }

    fn display(&self) -> String {
        "EmptyExec".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_storage::{MemoryStorage, SnapshotId};

    #[tokio::test]
    async fn test_empty_exec() {
        let op = EmptyExec::new();
        let storage = Arc::new(MemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        op.open(&ctx).await.unwrap();

        // Should return None immediately
        assert!(op.next().await.unwrap().is_none());
        assert!(op.next().await.unwrap().is_none());

        op.close().await.unwrap();
    }

    #[test]
    fn test_empty_exec_capabilities() {
        let op = EmptyExec::new();
        let caps = op.capabilities();

        assert!(!caps.blocking);
        assert!(caps.stateless);
    }
}
