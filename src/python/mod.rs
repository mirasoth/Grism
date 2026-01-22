//! Python bindings for Grism.
//!
//! This module provides PyO3 bindings for the Grism Python API,
//! following the Daft pattern of individual python modules per crate.
//!
//! The Python API implements the specification in `specs/2_python_api_v0.1.md`,
//! with expression lowering to Rust LogicalPlan per RFC-0002, RFC-0003, and RFC-0006.

mod executor;
mod expressions;
mod hypergraph;

use pyo3::prelude::*;

// Re-export types for use by other Rust code
pub use executor::{PyExecutor, PyLocalExecutor, PyRayExecutor};
pub use expressions::{PyAggExpr, PyExpr, PyPattern};
pub use hypergraph::{
    PyEdgeFrame, PyFrameSchema, PyGroupedFrame, PyHyperedgeFrame, PyHypergraph, PyNodeFrame,
    PyTransaction,
};

/// Register all Python bindings with the module.
pub fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // ========== Core Classes ==========
    m.add_class::<PyHypergraph>()?;
    m.add_class::<PyNodeFrame>()?;
    m.add_class::<PyEdgeFrame>()?;
    m.add_class::<PyHyperedgeFrame>()?;
    m.add_class::<PyGroupedFrame>()?;
    m.add_class::<PyFrameSchema>()?;
    m.add_class::<PyTransaction>()?;

    // ========== Expression Classes ==========
    m.add_class::<PyExpr>()?;
    m.add_class::<PyAggExpr>()?;
    m.add_class::<PyPattern>()?;

    // ========== Executor Classes ==========
    m.add_class::<PyExecutor>()?;
    m.add_class::<PyLocalExecutor>()?;
    m.add_class::<PyRayExecutor>()?;

    // ========== Expression Functions ==========
    // Column references
    m.add_function(wrap_pyfunction!(expressions::col, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::lit, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::prop, m)?)?;

    // ========== Aggregation Functions ==========
    m.add_function(wrap_pyfunction!(expressions::count, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::count_distinct, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::sum_agg, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::avg, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::min_agg, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::max_agg, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::collect, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::collect_distinct, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::first, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::last, m)?)?;

    // ========== String Functions ==========
    m.add_function(wrap_pyfunction!(expressions::concat, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::length, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::lower, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::upper, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::trim, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::contains_fn, m)?)?;

    // ========== Math Functions ==========
    m.add_function(wrap_pyfunction!(expressions::abs_fn, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::ceil, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::floor, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::round_fn, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::sqrt, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::power, m)?)?;

    // ========== Date/Time Functions ==========
    m.add_function(wrap_pyfunction!(expressions::date, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::year, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::month, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::day, m)?)?;

    // ========== Conditional Functions ==========
    m.add_function(wrap_pyfunction!(expressions::coalesce, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::if_expr, m)?)?;

    // ========== Graph Functions ==========
    m.add_function(wrap_pyfunction!(expressions::labels, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::type_fn, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::id_fn, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::properties, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::nodes, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::relationships, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::path_length, m)?)?;

    // ========== Vector/AI Functions ==========
    m.add_function(wrap_pyfunction!(expressions::sim, m)?)?;

    // ========== String Functions (standalone) ==========
    m.add_function(wrap_pyfunction!(expressions::substring, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::replace, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::split, m)?)?;

    // ========== Conditional Functions (additional) ==========
    m.add_function(wrap_pyfunction!(expressions::when, m)?)?;

    // ========== Predicate Functions ==========
    m.add_function(wrap_pyfunction!(expressions::exists_fn, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::any_fn, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::all_fn, m)?)?;

    // ========== Path Functions ==========
    m.add_function(wrap_pyfunction!(expressions::shortest_path, m)?)?;
    m.add_function(wrap_pyfunction!(expressions::all_paths, m)?)?;

    Ok(())
}

// Keep backwards-compatible alias
pub use PyHypergraph as PyHyperGraph;
