//! Local executor implementation.

use common_error::{GrismError, GrismResult};
use grism_logical::{LogicalOp, LogicalPlan};

use crate::node::{BoxedExecNode, FilterNode, LimitNode, ScanNode};
use crate::result::QueryResult;

/// Configuration for the local executor.
#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    /// Number of parallel threads.
    pub parallelism: Option<usize>,
    /// Memory limit in bytes.
    pub memory_limit: Option<usize>,
    /// Batch size for processing.
    pub batch_size: usize,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            parallelism: None,
            memory_limit: None,
            batch_size: 1024,
        }
    }
}

/// Local single-machine executor.
pub struct LocalExecutor {
    config: ExecutionConfig,
}

impl LocalExecutor {
    /// Create a new local executor with default configuration.
    pub fn new() -> Self {
        Self {
            config: ExecutionConfig::default(),
        }
    }

    /// Create a local executor with custom configuration.
    pub fn with_config(config: ExecutionConfig) -> Self {
        Self { config }
    }

    /// Execute a logical plan.
    pub async fn execute(&self, plan: LogicalPlan) -> GrismResult<QueryResult> {
        let mut root = self.build_exec_tree(plan.root())?;

        let mut result = QueryResult::new();
        while let Some(batch) = root.next().await? {
            result.add_batch(batch);
        }

        Ok(result)
    }

    /// Execute a logical plan synchronously.
    pub fn execute_sync(&self, plan: LogicalPlan) -> GrismResult<QueryResult> {
        common_runtime::block_on(self.execute(plan))?
    }

    /// Build an execution tree from a logical plan.
    fn build_exec_tree(&self, op: &LogicalOp) -> GrismResult<BoxedExecNode> {
        match op {
            LogicalOp::Scan(scan) => Ok(Box::new(ScanNode::new(scan.label.clone()))),
            LogicalOp::Filter { input, filter: _ } => {
                let input_node = self.build_exec_tree(input)?;
                Ok(Box::new(FilterNode::new(input_node)))
            }
            LogicalOp::Limit { input, limit } => {
                let input_node = self.build_exec_tree(input)?;
                Ok(Box::new(LimitNode::new(input_node, limit.limit)))
            }
            LogicalOp::Project { .. } => {
                // Placeholder: would need ProjectNode
                Err(GrismError::not_implemented("Project execution"))
            }
            LogicalOp::Expand { .. } => Err(GrismError::not_implemented("Expand execution")),
            LogicalOp::Aggregate { .. } => Err(GrismError::not_implemented("Aggregate execution")),
            LogicalOp::Infer { .. } => Err(GrismError::not_implemented("Infer execution")),
            LogicalOp::Sort { .. } => Err(GrismError::not_implemented("Sort execution")),
            LogicalOp::Union { .. } => Err(GrismError::not_implemented("Union execution")),
            LogicalOp::Rename { .. } => Err(GrismError::not_implemented("Rename execution")),
            LogicalOp::Empty => Err(GrismError::not_implemented("Empty execution")),
        }
    }

    /// Get the executor configuration.
    pub fn config(&self) -> &ExecutionConfig {
        &self.config
    }
}

impl Default for LocalExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_logical::ScanOp;

    #[tokio::test]
    async fn test_execute_scan() {
        let executor = LocalExecutor::new();
        let scan = LogicalOp::Scan(ScanOp::nodes_with_label("Person"));
        let plan = LogicalPlan::new(scan);

        let result = executor.execute(plan).await.unwrap();
        assert_eq!(result.total_rows(), 0); // Empty result for now
    }

    #[test]
    fn test_execute_sync() {
        let executor = LocalExecutor::new();
        let scan = LogicalOp::Scan(ScanOp::nodes_with_label("Person"));
        let plan = LogicalPlan::new(scan);

        let result = executor.execute_sync(plan).unwrap();
        assert_eq!(result.total_rows(), 0);
    }
}
