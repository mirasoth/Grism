//! Execution node trait for the query engine.

use async_trait::async_trait;

use common_error::GrismResult;

use crate::result::ResultBatch;

/// Trait for execution nodes in the query plan.
///
/// Execution nodes form a tree that processes data in a pull-based fashion.
/// Each call to `next()` returns the next batch of results.
#[async_trait]
pub trait ExecNode: Send + Sync {
    /// Get the name of this execution node.
    fn name(&self) -> &'static str;

    /// Get the next batch of results.
    ///
    /// Returns `None` when there are no more results.
    async fn next(&mut self) -> GrismResult<Option<ResultBatch>>;

    /// Reset the node to its initial state.
    async fn reset(&mut self) -> GrismResult<()>;
}

/// A boxed execution node for dynamic dispatch.
pub type BoxedExecNode = Box<dyn ExecNode>;

/// Placeholder scan node implementation.
pub struct ScanNode {
    label: Option<String>,
    exhausted: bool,
}

impl ScanNode {
    /// Create a new scan node.
    pub fn new(label: Option<String>) -> Self {
        Self {
            label,
            exhausted: false,
        }
    }
}

#[async_trait]
impl ExecNode for ScanNode {
    fn name(&self) -> &'static str {
        "Scan"
    }

    async fn next(&mut self) -> GrismResult<Option<ResultBatch>> {
        if self.exhausted {
            return Ok(None);
        }
        self.exhausted = true;

        // Return empty batch for now
        Ok(Some(ResultBatch::empty()))
    }

    async fn reset(&mut self) -> GrismResult<()> {
        self.exhausted = false;
        Ok(())
    }
}

/// Placeholder filter node implementation.
pub struct FilterNode {
    input: BoxedExecNode,
}

impl FilterNode {
    /// Create a new filter node.
    pub fn new(input: BoxedExecNode) -> Self {
        Self { input }
    }
}

#[async_trait]
impl ExecNode for FilterNode {
    fn name(&self) -> &'static str {
        "Filter"
    }

    async fn next(&mut self) -> GrismResult<Option<ResultBatch>> {
        // Pass through to input for now
        self.input.next().await
    }

    async fn reset(&mut self) -> GrismResult<()> {
        self.input.reset().await
    }
}

/// Placeholder limit node implementation.
pub struct LimitNode {
    input: BoxedExecNode,
    limit: usize,
    count: usize,
}

impl LimitNode {
    /// Create a new limit node.
    pub fn new(input: BoxedExecNode, limit: usize) -> Self {
        Self {
            input,
            limit,
            count: 0,
        }
    }
}

#[async_trait]
impl ExecNode for LimitNode {
    fn name(&self) -> &'static str {
        "Limit"
    }

    async fn next(&mut self) -> GrismResult<Option<ResultBatch>> {
        if self.count >= self.limit {
            return Ok(None);
        }

        match self.input.next().await? {
            Some(batch) => {
                self.count += batch.len();
                Ok(Some(batch))
            }
            None => Ok(None),
        }
    }

    async fn reset(&mut self) -> GrismResult<()> {
        self.count = 0;
        self.input.reset().await
    }
}
