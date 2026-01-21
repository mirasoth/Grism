//! Python bindings for Grism configuration.

use pyo3::prelude::*;

use crate::{ExecutionConfig, ExecutorType, GrismConfig, StorageConfig};

#[pymethods]
impl GrismConfig {
    #[new]
    fn py_new() -> Self {
        Self::default()
    }
}

// Note: Full Python bindings would be implemented here
// This is a placeholder for the structure.
