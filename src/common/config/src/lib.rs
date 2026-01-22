//! Configuration management for Grism.
//!
//! Provides runtime configuration for executors, storage, and query processing.

use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub mod python;

/// Global Grism configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass)]
#[derive(Default)]
pub struct GrismConfig {
    /// Execution configuration.
    pub execution: ExecutionConfig,
    /// Storage configuration.
    pub storage: StorageConfig,
}

/// Execution backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    /// Default executor type.
    pub default_executor: ExecutorType,
    /// Number of parallel threads for local execution.
    pub parallelism: Option<usize>,
    /// Memory limit in bytes.
    pub memory_limit: Option<usize>,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            default_executor: ExecutorType::Local,
            parallelism: None,
            memory_limit: None,
        }
    }
}

/// Executor type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ExecutorType {
    /// Local single-machine executor.
    #[default]
    Local,
    /// Ray distributed executor.
    Ray,
}

/// Storage layer configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Base path for data storage.
    pub base_path: Option<String>,
    /// Enable snapshot isolation.
    pub snapshot_isolation: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            base_path: None,
            snapshot_isolation: true,
        }
    }
}
