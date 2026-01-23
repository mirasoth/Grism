//! Physical operator trait and common utilities.

use std::fmt::Debug;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use common_error::GrismResult;

use crate::executor::ExecutionContext;
use crate::physical::{OperatorCaps, PhysicalSchema};

/// Trait for physical operators in the execution plan.
///
/// Physical operators form a tree that processes data in a **pull-based** fashion.
/// Each call to `next()` returns the next batch of results.
///
/// # Lifecycle (RFC-0008, Section 5.2)
///
/// ```text
/// create → open → next* → close
/// ```
///
/// - `open()` initializes resources and child operators
/// - `next()` returns batches until exhausted (returns None)
/// - `close()` releases resources (MUST be idempotent)
///
/// # Contract
///
/// Operators MUST NOT:
/// - Mutate upstream data
/// - Perform side effects outside `ExecutionContext`
/// - Block indefinitely without yielding
#[async_trait]
pub trait PhysicalOperator: Send + Sync + Debug {
    /// Get the operator name for display.
    fn name(&self) -> &'static str;

    /// Get the output schema.
    fn schema(&self) -> &PhysicalSchema;

    /// Get operator capabilities.
    fn capabilities(&self) -> OperatorCaps;

    /// Get child operators.
    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>>;

    /// Initialize the operator and its children.
    ///
    /// Called once before the first `next()`. Opens resources,
    /// initializes state, and recursively opens children.
    async fn open(&self, ctx: &ExecutionContext) -> GrismResult<()>;

    /// Get the next batch of results.
    ///
    /// Returns `Ok(Some(batch))` while data is available.
    /// Returns `Ok(None)` when exhausted.
    /// Returns `Err(_)` on failure (aborts pipeline).
    async fn next(&self) -> GrismResult<Option<RecordBatch>>;

    /// Close the operator and release resources.
    ///
    /// MUST be called after execution completes or on error.
    /// MUST be idempotent (safe to call multiple times).
    async fn close(&self) -> GrismResult<()>;

    /// Generate EXPLAIN output at given indentation level.
    fn explain(&self, indent: usize) -> String {
        let prefix = "  ".repeat(indent);
        let mut output = format!("{}{}\n", prefix, self.display());
        for child in self.children() {
            output.push_str(&child.explain(indent + 1));
        }
        output
    }

    /// Display string for EXPLAIN.
    fn display(&self) -> String {
        self.name().to_string()
    }
}

/// Boxed physical operator for dynamic dispatch.
pub type BoxedPhysicalOperator = Arc<dyn PhysicalOperator>;

/// State for operators that need to track execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OperatorState {
    /// Operator has not been opened yet.
    #[default]
    Uninitialized,
    /// Operator is open and ready to produce output.
    Open,
    /// Operator has finished producing output.
    Exhausted,
    /// Operator has been closed.
    Closed,
}

impl OperatorState {
    /// Check if the operator is in a valid state for `next()`.
    pub fn can_produce(&self) -> bool {
        *self == Self::Open
    }

    /// Check if the operator needs to be opened.
    pub fn needs_open(&self) -> bool {
        *self == Self::Uninitialized
    }

    /// Check if the operator is exhausted or closed.
    pub fn is_finished(&self) -> bool {
        matches!(self, Self::Exhausted | Self::Closed)
    }
}
