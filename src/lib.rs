//! Grism - AI-native neurosymbolic hypergraph database system
//!
//! Grism is a hypergraph-backed, relationally-executable, AI-native graph container
//! designed for modern agentic and LLM-driven workflows.

#![forbid(unsafe_code)]
#![allow(clippy::module_name_repetitions)]

// Re-export core crates
pub use common_error as error;
pub use grism_core as core;
pub use grism_distributed as distributed;
pub use grism_engine as engine;
pub use grism_logical as logical;
pub use grism_optimizer as optimizer;
pub use grism_storage as storage;

/// Grism version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(feature = "python")]
mod python;

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// Python module for Grism
///
/// The module is named `_grism` to allow the Python wrapper package
/// to re-export with additional convenience functions.
#[cfg(feature = "python")]
#[pymodule]
fn _grism(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", VERSION)?;

    // Register Python classes and functions
    python::register_module(m)?;

    Ok(())
}
