//! `RecordBatch` stream utilities for storage.
//!
//! This module provides the [`RecordBatchStream`] abstraction used by
//! the storage layer to return Arrow data.

use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;
use common_error::GrismResult;
use futures::stream::Stream;

// ============================================================================
// RecordBatchStream Type
// ============================================================================

/// A stream of Arrow `RecordBatches`.
///
/// This is the primary data exchange type between storage and execution.
/// Per RFC-0012, all scans return pull-based `RecordBatch` streams.
pub type RecordBatchStream = Pin<Box<dyn Stream<Item = GrismResult<RecordBatch>> + Send>>;

// ============================================================================
// Stream Utilities
// ============================================================================

/// Create an empty `RecordBatchStream`.
pub fn empty_stream() -> RecordBatchStream {
    Box::pin(futures::stream::empty())
}

/// Create a `RecordBatchStream` from a single batch.
pub fn once_stream(batch: RecordBatch) -> RecordBatchStream {
    Box::pin(futures::stream::once(async move { Ok(batch) }))
}

/// Create a `RecordBatchStream` from a vector of batches.
pub fn vec_stream(batches: Vec<RecordBatch>) -> RecordBatchStream {
    Box::pin(futures::stream::iter(batches.into_iter().map(Ok)))
}

/// Create a `RecordBatchStream` from a fallible iterator.
pub fn iter_stream<I>(iter: I) -> RecordBatchStream
where
    I: IntoIterator<Item = GrismResult<RecordBatch>> + Send + 'static,
    I::IntoIter: Send,
{
    Box::pin(futures::stream::iter(iter))
}

// ============================================================================
// MemoryBatchStream
// ============================================================================

/// A stream over in-memory `RecordBatches`.
///
/// This stream efficiently iterates over a vector of batches
/// without additional allocation.
pub struct MemoryBatchStream {
    batches: Vec<RecordBatch>,
    index: usize,
}

impl MemoryBatchStream {
    /// Create a new memory batch stream.
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches, index: 0 }
    }

    /// Create a boxed stream.
    pub fn boxed(batches: Vec<RecordBatch>) -> RecordBatchStream {
        Box::pin(Self::new(batches))
    }
}

impl Stream for MemoryBatchStream {
    type Item = GrismResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.index < self.batches.len() {
            let batch = self.batches[self.index].clone();
            self.index += 1;
            Poll::Ready(Some(Ok(batch)))
        } else {
            Poll::Ready(None)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.batches.len() - self.index;
        (remaining, Some(remaining))
    }
}

// ============================================================================
// ProjectedBatchStream
// ============================================================================

/// A stream that applies projection to batches.
pub struct ProjectedBatchStream {
    inner: RecordBatchStream,
    columns: Vec<String>,
}

impl ProjectedBatchStream {
    /// Create a new projected stream.
    pub fn new(inner: RecordBatchStream, columns: Vec<String>) -> Self {
        Self { inner, columns }
    }

    /// Create a boxed projected stream.
    pub fn boxed(inner: RecordBatchStream, columns: Vec<String>) -> RecordBatchStream {
        if columns.is_empty() {
            // No projection needed
            inner
        } else {
            Box::pin(Self::new(inner, columns))
        }
    }

    /// Project a batch to the specified columns.
    fn project_batch(&self, batch: &RecordBatch) -> GrismResult<RecordBatch> {
        let schema = batch.schema();
        let mut indices = Vec::with_capacity(self.columns.len());

        for col_name in &self.columns {
            match schema.index_of(col_name) {
                Ok(idx) => indices.push(idx),
                Err(_) => {
                    // Column not found - skip it (might be aliased differently)
                    continue;
                }
            }
        }

        if indices.is_empty() {
            // Return original if no columns matched
            return Ok(batch.clone());
        }

        batch
            .project(&indices)
            .map_err(|e| common_error::GrismError::execution(format!("Projection failed: {e}")))
    }
}

impl Stream for ProjectedBatchStream {
    type Item = GrismResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(self.project_batch(&batch))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// ============================================================================
// Stream Extensions
// ============================================================================

/// Extension trait for `RecordBatchStream`.
pub trait RecordBatchStreamExt {
    /// Collect all batches into a vector.
    fn collect_vec(
        self,
    ) -> Pin<Box<dyn std::future::Future<Output = GrismResult<Vec<RecordBatch>>> + Send>>;
}

impl RecordBatchStreamExt for RecordBatchStream {
    fn collect_vec(
        self,
    ) -> Pin<Box<dyn std::future::Future<Output = GrismResult<Vec<RecordBatch>>> + Send>> {
        use futures::StreamExt;

        Box::pin(async move {
            let mut batches = Vec::new();
            let mut stream = self;

            while let Some(result) = stream.next().await {
                batches.push(result?);
            }

            Ok(batches)
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;
    use std::sync::Arc;

    fn make_test_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let ids: Vec<i64> = (0..num_rows as i64).collect();
        let values: Vec<i64> = (0..num_rows as i64).map(|x| x * 10).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_empty_stream() {
        let mut stream = empty_stream();
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_once_stream() {
        let batch = make_test_batch(5);
        let mut stream = once_stream(batch.clone());

        let result = stream.next().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 5);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_vec_stream() {
        let batches = vec![make_test_batch(3), make_test_batch(5)];
        let mut stream = vec_stream(batches);

        let b1 = stream.next().await.unwrap().unwrap();
        assert_eq!(b1.num_rows(), 3);

        let b2 = stream.next().await.unwrap().unwrap();
        assert_eq!(b2.num_rows(), 5);

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_memory_batch_stream() {
        let batches = vec![make_test_batch(2), make_test_batch(4)];
        let mut stream = MemoryBatchStream::new(batches);

        assert_eq!(stream.size_hint(), (2, Some(2)));

        let b1 = stream.next().await.unwrap().unwrap();
        assert_eq!(b1.num_rows(), 2);
        assert_eq!(stream.size_hint(), (1, Some(1)));

        let b2 = stream.next().await.unwrap().unwrap();
        assert_eq!(b2.num_rows(), 4);
        assert_eq!(stream.size_hint(), (0, Some(0)));

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_projected_stream() {
        let batch = make_test_batch(3);
        let inner = once_stream(batch);
        let mut stream = ProjectedBatchStream::new(inner, vec!["id".to_string()]);

        let result = stream.next().await.unwrap().unwrap();
        assert_eq!(result.num_columns(), 1);
        assert_eq!(result.num_rows(), 3);
    }

    #[tokio::test]
    async fn test_collect_vec() {
        let batches = vec![make_test_batch(2), make_test_batch(3)];
        let stream = vec_stream(batches);

        let collected = stream.collect_vec().await.unwrap();
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0].num_rows(), 2);
        assert_eq!(collected[1].num_rows(), 3);
    }
}
