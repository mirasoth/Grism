//! Python bindings for Grism.
//!
//! This module provides PyO3 bindings for the Grism Python API,
//! following the Daft pattern of individual python modules per crate.

mod executor;
mod expressions;
mod hypergraph;

use pyo3::prelude::*;

pub use executor::{PyExecutor, PyLocalExecutor, PyRayExecutor};
pub use expressions::{PyAggExpr, PyExpr};
pub use hypergraph::{PyEdgeFrame, PyHyperEdgeFrame, PyHyperGraph, PyNodeFrame};

/// Register all Python bindings with the module.
pub fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Classes
    m.add_class::<PyHyperGraph>()?;
    m.add_class::<PyNodeFrame>()?;
    m.add_class::<PyEdgeFrame>()?;
    m.add_class::<PyHyperEdgeFrame>()?;
    m.add_class::<PyExpr>()?;
    m.add_class::<PyAggExpr>()?;
    m.add_class::<PyExecutor>()?;
    m.add_class::<PyLocalExecutor>()?;
    m.add_class::<PyRayExecutor>()?;

    // Expression functions
    m.add_function(wrap_pyfunction!(expressions::col, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::lit, m)?)?;

    // Aggregation functions
    m.add_function(wrap_pyfunction!(expressions::count, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::sum, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::avg, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::min, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::max, m)?)?;

    // Utility functions
    m.add_function(wrap_pyfunction!(expressions::sim, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::contains, m)?)?;

    Ok(())
}
