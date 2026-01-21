//! Worker task execution.

use common_error::GrismResult;

use crate::planner::Stage;
use crate::transport::ArrowTransport;

/// A task executed by a worker.
pub struct WorkerTask {
    /// Stage to execute.
    stage: Stage,
    /// Partition ID.
    partition_id: usize,
    /// Input data (Arrow IPC format).
    input_data: Vec<u8>,
}

impl WorkerTask {
    /// Create a new worker task.
    pub fn new(stage: Stage, partition_id: usize, input_data: Vec<u8>) -> Self {
        Self {
            stage,
            partition_id,
            input_data,
        }
    }

    /// Execute the task.
    pub async fn execute(self) -> GrismResult<Vec<u8>> {
        // Deserialize input data
        let _input = if self.input_data.is_empty() {
            None
        } else {
            Some(ArrowTransport::deserialize(&self.input_data)?)
        };

        // Execute operators in sequence
        for op in &self.stage.operators {
            // TODO: Actually execute the operator
            let _ = op; // Placeholder
        }

        // Serialize output
        // For now, return empty result
        Ok(Vec::new())
    }

    /// Get the stage.
    pub fn stage(&self) -> &Stage {
        &self.stage
    }

    /// Get the partition ID.
    pub fn partition_id(&self) -> usize {
        self.partition_id
    }
}

/// Task result from a worker.
#[derive(Debug)]
#[allow(dead_code)]
pub struct TaskResult {
    /// Stage ID.
    pub stage_id: u64,
    /// Partition ID.
    pub partition_id: usize,
    /// Output data (Arrow IPC format).
    pub data: Vec<u8>,
    /// Execution time in milliseconds.
    pub execution_time_ms: u64,
}

#[allow(dead_code)]
impl TaskResult {
    /// Create a new task result.
    pub fn new(stage_id: u64, partition_id: usize, data: Vec<u8>, execution_time_ms: u64) -> Self {
        Self {
            stage_id,
            partition_id,
            data,
            execution_time_ms,
        }
    }

    /// Check if the result is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
