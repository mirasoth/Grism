//! Rename execution operator.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use common_error::{GrismError, GrismResult};

use crate::executor::ExecutionContext;
use crate::operators::PhysicalOperator;
use crate::physical::{OperatorCaps, PhysicalSchema};

/// Rename execution operator.
///
/// Renames columns in the output schema.
#[derive(Debug)]
pub struct RenameExec {
    /// Input operator.
    input: Arc<dyn PhysicalOperator>,
    /// Column rename mappings (`old_name` -> `new_name`).
    mappings: HashMap<String, String>,
    /// Output schema with renamed columns.
    schema: PhysicalSchema,
}

impl RenameExec {
    /// Create a new rename operator.
    pub fn new(input: Arc<dyn PhysicalOperator>, mappings: HashMap<String, String>) -> Self {
        let schema = Self::build_schema(input.schema(), &mappings);
        Self {
            input,
            mappings,
            schema,
        }
    }

    /// Get rename mappings.
    pub fn mappings(&self) -> &HashMap<String, String> {
        &self.mappings
    }

    fn build_schema(
        input_schema: &PhysicalSchema,
        mappings: &HashMap<String, String>,
    ) -> PhysicalSchema {
        let fields: Vec<Arc<Field>> = input_schema
            .arrow_schema()
            .fields()
            .iter()
            .map(|f| {
                let new_name = mappings.get(f.name()).unwrap_or(f.name());
                Arc::new(Field::new(new_name, f.data_type().clone(), f.is_nullable()))
            })
            .collect();

        let mut qualifiers = HashMap::new();
        for (old_name, new_name) in mappings {
            if let Some(q) = input_schema.qualifier(old_name) {
                qualifiers.insert(new_name.clone(), q.to_string());
            }
        }

        PhysicalSchema::with_qualifiers(Arc::new(ArrowSchema::new(fields)), qualifiers)
    }

    /// Rename columns in a batch.
    fn rename_batch(&self, batch: &RecordBatch) -> GrismResult<RecordBatch> {
        RecordBatch::try_new(self.schema.arrow_schema().clone(), batch.columns().to_vec())
            .map_err(|e| GrismError::execution(e.to_string()))
    }
}

#[async_trait]
impl PhysicalOperator for RenameExec {
    fn name(&self) -> &'static str {
        "RenameExec"
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
        match self.input.next().await? {
            Some(batch) => Ok(Some(self.rename_batch(&batch)?)),
            None => Ok(None),
        }
    }

    async fn close(&self) -> GrismResult<()> {
        self.input.close().await
    }

    fn display(&self) -> String {
        let renames: Vec<_> = self
            .mappings
            .iter()
            .map(|(old, new)| format!("{old}->{new}"))
            .collect();
        format!("RenameExec({})", renames.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::EmptyExec;
    use grism_storage::{InMemoryStorage, SnapshotId};

    #[tokio::test]
    async fn test_rename_empty() {
        let input = Arc::new(EmptyExec::new());
        let rename = RenameExec::new(input, HashMap::new());

        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        rename.open(&ctx).await.unwrap();
        assert!(rename.next().await.unwrap().is_none());
        rename.close().await.unwrap();
    }

    #[test]
    fn test_rename_display() {
        let input = Arc::new(EmptyExec::new());
        let mut mappings = HashMap::new();
        mappings.insert("old".to_string(), "new".to_string());
        let rename = RenameExec::new(input, mappings);

        let display = rename.display();
        assert!(display.contains("old->new"));
    }
}
