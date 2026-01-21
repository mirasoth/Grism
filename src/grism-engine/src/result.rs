//! Query result types.

use std::collections::HashMap;

use grism_core::types::Value;

/// A batch of query results.
#[derive(Debug, Clone, Default)]
pub struct ResultBatch {
    /// Column names.
    columns: Vec<String>,
    /// Rows as maps of column name to value.
    rows: Vec<HashMap<String, Value>>,
}

impl ResultBatch {
    /// Create an empty batch.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Create a batch with columns.
    pub fn with_columns(columns: Vec<String>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
        }
    }

    /// Add a row to the batch.
    pub fn add_row(&mut self, row: HashMap<String, Value>) {
        self.rows.push(row);
    }

    /// Get the number of rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get the column names.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Get the rows.
    pub fn rows(&self) -> &[HashMap<String, Value>] {
        &self.rows
    }

    /// Iterate over rows.
    pub fn iter(&self) -> impl Iterator<Item = &HashMap<String, Value>> {
        self.rows.iter()
    }
}

impl IntoIterator for ResultBatch {
    type Item = HashMap<String, Value>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

/// Complete query result.
#[derive(Debug, Default)]
pub struct QueryResult {
    /// All batches in the result.
    batches: Vec<ResultBatch>,
}

impl QueryResult {
    /// Create an empty result.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a batch to the result.
    pub fn add_batch(&mut self, batch: ResultBatch) {
        self.batches.push(batch);
    }

    /// Get the total number of rows.
    pub fn total_rows(&self) -> usize {
        self.batches.iter().map(|b| b.len()).sum()
    }

    /// Get all batches.
    pub fn batches(&self) -> &[ResultBatch] {
        &self.batches
    }

    /// Convert to a single list of rows.
    pub fn to_rows(self) -> Vec<HashMap<String, Value>> {
        self.batches.into_iter().flat_map(|b| b.rows).collect()
    }

    /// Collect all rows into a single batch.
    pub fn collect(self) -> ResultBatch {
        let columns = self
            .batches
            .first()
            .map(|b| b.columns.clone())
            .unwrap_or_default();

        let rows = self.batches.into_iter().flat_map(|b| b.rows).collect();

        ResultBatch { columns, rows }
    }
}
