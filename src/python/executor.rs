//! Python bindings for executors.

use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Python wrapper for executors.
#[pyclass(name = "Executor")]
#[derive(Clone)]
pub struct PyExecutor {
    /// Executor type.
    executor_type: String,
}

#[pymethods]
impl PyExecutor {
    /// Create a local executor.
    #[staticmethod]
    #[pyo3(signature = (parallelism=None, memory_limit=None))]
    fn local(parallelism: Option<usize>, memory_limit: Option<usize>) -> Self {
        let _ = (parallelism, memory_limit);
        Self {
            executor_type: "local".to_string(),
        }
    }

    /// Create a Ray executor.
    #[staticmethod]
    #[pyo3(signature = (num_workers=None, resources=None))]
    fn ray(num_workers: Option<usize>, resources: Option<Bound<'_, PyDict>>) -> Self {
        let _ = (num_workers, resources);
        Self {
            executor_type: "ray".to_string(),
        }
    }

    fn __repr__(&self) -> String {
        format!("Executor(type='{}')", self.executor_type)
    }
}

/// Local executor convenience class.
#[pyclass(name = "LocalExecutor")]
pub struct PyLocalExecutor {
    parallelism: Option<usize>,
    memory_limit: Option<usize>,
}

#[pymethods]
impl PyLocalExecutor {
    #[new]
    #[pyo3(signature = (parallelism=None, memory_limit=None))]
    fn new(parallelism: Option<usize>, memory_limit: Option<usize>) -> Self {
        Self {
            parallelism,
            memory_limit,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "LocalExecutor(parallelism={:?}, memory_limit={:?})",
            self.parallelism, self.memory_limit
        )
    }
}

/// Ray executor convenience class.
#[pyclass(name = "RayExecutor")]
pub struct PyRayExecutor {
    num_workers: Option<usize>,
}

#[pymethods]
impl PyRayExecutor {
    #[new]
    #[pyo3(signature = (num_workers=None))]
    fn new(num_workers: Option<usize>) -> Self {
        Self { num_workers }
    }

    fn __repr__(&self) -> String {
        format!("RayExecutor(num_workers={:?})", self.num_workers)
    }
}
