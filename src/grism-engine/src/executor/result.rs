//! Query execution result types.

use std::fmt::Write;
use std::time::Duration;

use arrow::record_batch::RecordBatch;

use common_error::{GrismError, GrismResult};

use crate::metrics::MetricsSink;
use crate::physical::PhysicalSchema;

/// Result of query execution.
#[derive(Debug)]
pub struct ExecutionResult {
    /// Output batches.
    pub batches: Vec<RecordBatch>,
    /// Output schema.
    pub schema: PhysicalSchema,
    /// Execution metrics.
    pub metrics: MetricsSink,
    /// Total execution time.
    pub elapsed: Duration,
}

impl ExecutionResult {
    /// Create a new execution result.
    pub const fn new(
        batches: Vec<RecordBatch>,
        schema: PhysicalSchema,
        metrics: MetricsSink,
        elapsed: Duration,
    ) -> Self {
        Self {
            batches,
            schema,
            metrics,
            elapsed,
        }
    }

    /// Create an empty result.
    pub fn empty(schema: PhysicalSchema) -> Self {
        Self {
            batches: Vec::new(),
            schema,
            metrics: MetricsSink::new(),
            elapsed: Duration::ZERO,
        }
    }

    /// Get total row count.
    pub fn total_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// Get number of batches.
    pub const fn num_batches(&self) -> usize {
        self.batches.len()
    }

    /// Check if result is empty.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty() || self.total_rows() == 0
    }

    /// Get the output schema.
    pub const fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    /// Get the Arrow schema.
    pub fn arrow_schema(&self) -> &arrow::datatypes::SchemaRef {
        self.schema.arrow_schema()
    }

    /// Iterate over batches.
    pub fn iter(&self) -> impl Iterator<Item = &RecordBatch> {
        self.batches.iter()
    }

    /// Consume and iterate over batches.
    pub fn into_iter(self) -> impl Iterator<Item = RecordBatch> {
        self.batches.into_iter()
    }

    /// Combine all batches into one.
    pub fn concat(&self) -> GrismResult<RecordBatch> {
        if self.batches.is_empty() {
            // Return empty batch with schema
            return Ok(RecordBatch::new_empty(self.schema.arrow_schema().clone()));
        }

        arrow::compute::concat_batches(self.schema.arrow_schema(), &self.batches)
            .map_err(|e| GrismError::execution(e.to_string()))
    }

    /// Take all batches, leaving an empty result.
    pub fn take_batches(&mut self) -> Vec<RecordBatch> {
        std::mem::take(&mut self.batches)
    }

    /// Format as EXPLAIN ANALYZE output.
    pub fn explain_analyze(&self) -> String {
        let mut output = String::new();
        let _ = writeln!(output, "Execution Time: {:?}", self.elapsed);
        let _ = writeln!(output, "Total Rows: {}", self.total_rows());
        let _ = writeln!(output, "Batches: {}", self.num_batches());
        output.push_str("\nOperator Metrics:\n");
        output.push_str(&self.metrics.format_analyze());
        output
    }

    /// Get throughput (rows per second).
    pub fn throughput(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs == 0.0 {
            0.0
        } else {
            self.total_rows() as f64 / secs
        }
    }
}

impl IntoIterator for ExecutionResult {
    type Item = RecordBatch;
    type IntoIter = std::vec::IntoIter<RecordBatch>;

    fn into_iter(self) -> Self::IntoIter {
        self.batches.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let array = Int64Array::from_iter_values(0..num_rows as i64);
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_execution_result() {
        let batch1 = create_test_batch(100);
        let batch2 = create_test_batch(50);

        let schema = PhysicalSchema::new(batch1.schema());
        let result = ExecutionResult::new(
            vec![batch1, batch2],
            schema,
            MetricsSink::new(),
            Duration::from_millis(100),
        );

        assert_eq!(result.total_rows(), 150);
        assert_eq!(result.num_batches(), 2);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_execution_result_concat() {
        let batch1 = create_test_batch(100);
        let batch2 = create_test_batch(50);

        let schema = PhysicalSchema::new(batch1.schema());
        let result = ExecutionResult::new(
            vec![batch1, batch2],
            schema,
            MetricsSink::new(),
            Duration::ZERO,
        );

        let combined = result.concat().unwrap();
        assert_eq!(combined.num_rows(), 150);
    }

    #[test]
    fn test_empty_result() {
        let schema = PhysicalSchema::empty();
        let result = ExecutionResult::empty(schema);

        assert!(result.is_empty());
        assert_eq!(result.total_rows(), 0);
    }
}
