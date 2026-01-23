//! Rust worker for distributed execution.

mod task;

pub use task::WorkerTask;

use serde::{Deserialize, Serialize};

use common_error::GrismResult;

use crate::planner::ExecutionStage;

/// Worker configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Worker ID.
    pub worker_id: u64,
    /// Memory limit in bytes.
    pub memory_limit: Option<usize>,
    /// Number of threads.
    pub num_threads: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: 0,
            memory_limit: None,
            num_threads: num_cpus(),
        }
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1)
}

/// Rust worker that executes stages.
pub struct Worker {
    config: WorkerConfig,
}

impl Worker {
    /// Create a new worker.
    pub fn new(config: WorkerConfig) -> Self {
        Self { config }
    }

    /// Execute a stage partition.
    pub async fn execute_partition(
        &self,
        stage: &ExecutionStage,
        partition_id: usize,
        input_data: Vec<u8>,
    ) -> GrismResult<Vec<u8>> {
        let task = WorkerTask::new(stage.clone(), partition_id, input_data);
        task.execute().await
    }

    /// Get worker configuration.
    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }

    /// Get worker ID.
    pub fn worker_id(&self) -> u64 {
        self.config.worker_id
    }
}

impl Default for Worker {
    fn default() -> Self {
        Self::new(WorkerConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_creation() {
        let worker = Worker::default();
        assert_eq!(worker.worker_id(), 0);
    }
}
