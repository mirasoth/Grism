//! Python bindings for Grism.
//!
//! This module provides PyO3 bindings for the Grism Python API,
//! following the Daft pattern of individual python modules per crate.
//!
//! The Python API implements the specification in `specs/2_python_api_v0.1.md`,
//! with expression lowering to Rust LogicalPlan per RFC-0002, RFC-0003, and RFC-0006.

#![allow(dead_code)] // Public API items may appear unused to Rust but are used by Python
#![allow(deprecated)] // Some PyO3 generated code uses deprecated constants
#![allow(unsafe_op_in_unsafe_fn)] // PyO3 macros generate unsafe code that needs this allow

mod hypergraph;

use pyo3::prelude::*;

// Re-export types from this crate's modules
pub use hypergraph::{
    PyEdgeFrame, PyFrameSchema, PyGroupedFrame, PyHyperedgeFrame, PyHypergraph, PyNodeFrame,
    PyTransaction,
};

/// Register all Python bindings with the module.
///
/// Following the Daft pattern, this function orchestrates registration
/// by calling each sub-crate's `register_modules` function and then
/// registering types specific to the main crate.
pub fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // ========== Register Sub-crate Modules (Daft Pattern) ==========
    // Each crate manages its own Python bindings
    grism_logical::python::register_modules(m)?;
    grism_engine::python::register_modules(m)?;

    // ========== Core Classes (Main Crate) ==========
    // These types span multiple crates and are kept in the main crate
    m.add_class::<PyHypergraph>()?;
    m.add_class::<PyNodeFrame>()?;
    m.add_class::<PyEdgeFrame>()?;
    m.add_class::<PyHyperedgeFrame>()?;
    m.add_class::<PyGroupedFrame>()?;
    m.add_class::<PyFrameSchema>()?;
    m.add_class::<PyTransaction>()?;

    // ========== Path Functions ==========
    m.add_function(wrap_pyfunction!(hypergraph::shortest_path, m)?)?;
    m.add_function(wrap_pyfunction!(hypergraph::all_paths, m)?)?;

    Ok(())
}
