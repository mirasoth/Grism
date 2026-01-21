//! Python bindings for Hypergraph and Frame types.
//!
//! This module provides PyO3 bindings for the Grism Python API,
//! implementing the Frame types (NodeFrame, EdgeFrame, HyperedgeFrame)
//! with proper lowering to Rust logical plans.

use std::collections::HashMap;

use grism_logical::{
    ops::{Direction, ExpandMode, HopRange},
    AggregateOp, ExpandOp, FilterOp, LimitOp, LogicalOp, LogicalPlan, ProjectOp, ScanKind, ScanOp,
    SortKey, SortOp,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::expressions::{ExprKind, PyAggExpr, PyExpr};

/// Python wrapper for Hypergraph - the canonical user-facing container.
///
/// Hypergraph is a logical handle to a versioned hypergraph state.
/// It is immutable, snapshot-bound, and storage-agnostic.
#[pyclass(name = "Hypergraph")]
#[derive(Clone)]
pub struct PyHypergraph {
    /// Connection URI.
    uri: String,
    /// Namespace.
    namespace: Option<String>,
    /// Executor type ("local" or "ray").
    executor: String,
}

#[pymethods]
impl PyHypergraph {
    /// Connect to a Grism hypergraph.
    ///
    /// Args:
    ///     uri: Connection URI (e.g., "grism://local", "grism:///path/to/data")
    ///     executor: Execution backend ("local" or "ray")
    ///     namespace: Optional namespace for logical graph isolation
    ///
    /// Returns:
    ///     Hypergraph instance (immutable, lazy, snapshot-bound)
    #[staticmethod]
    #[pyo3(signature = (uri, executor="local", namespace=None))]
    fn connect(uri: &str, executor: &str, namespace: Option<&str>) -> PyResult<Self> {
        Ok(Self {
            uri: uri.to_string(),
            namespace: namespace.map(String::from),
            executor: executor.to_string(),
        })
    }

    /// Create a new Grism hypergraph.
    #[staticmethod]
    #[pyo3(signature = (uri, executor="local"))]
    fn create(uri: &str, executor: &str) -> PyResult<Self> {
        Ok(Self {
            uri: uri.to_string(),
            namespace: None,
            executor: executor.to_string(),
        })
    }

    /// Create a new Hypergraph scoped to a namespace.
    fn with_namespace(&self, name: &str) -> Self {
        Self {
            uri: self.uri.clone(),
            namespace: Some(name.to_string()),
            executor: self.executor.clone(),
        }
    }

    /// Get nodes, optionally filtered by label.
    ///
    /// Args:
    ///     label: Node label to filter by (None = all nodes)
    ///
    /// Returns:
    ///     NodeFrame (lazy, immutable)
    #[pyo3(signature = (label=None))]
    fn nodes(&self, label: Option<&str>) -> PyResult<PyNodeFrame> {
        let scan = if let Some(l) = label {
            ScanOp::nodes_with_label(l)
        } else {
            ScanOp::nodes()
        };
        
        // Apply namespace if set
        let scan = if let Some(ref ns) = self.namespace {
            scan.with_namespace(ns)
        } else {
            scan
        };
        
        Ok(PyNodeFrame {
            plan: LogicalOp::Scan(scan),
            label: label.map(String::from),
        })
    }

    /// Get binary edges (arity=2 hyperedges), optionally filtered by label.
    ///
    /// Args:
    ///     label: Edge label to filter by (None = all edges)
    ///
    /// Returns:
    ///     EdgeFrame (lazy, immutable)
    #[pyo3(signature = (label=None))]
    fn edges(&self, label: Option<&str>) -> PyResult<PyEdgeFrame> {
        let scan = if let Some(l) = label {
            ScanOp::edges_with_label(l)
        } else {
            ScanOp::edges()
        };
        
        let scan = if let Some(ref ns) = self.namespace {
            scan.with_namespace(ns)
        } else {
            scan
        };
        
        Ok(PyEdgeFrame {
            plan: LogicalOp::Scan(scan),
            label: label.map(String::from),
        })
    }

    /// Get hyperedges, optionally filtered by label.
    ///
    /// Args:
    ///     label: Hyperedge label to filter by (None = all hyperedges)
    ///
    /// Returns:
    ///     HyperedgeFrame (lazy, immutable)
    #[pyo3(signature = (label=None))]
    fn hyperedges(&self, label: Option<&str>) -> PyResult<PyHyperedgeFrame> {
        let scan = if let Some(l) = label {
            ScanOp::hyperedges_with_label(l)
        } else {
            ScanOp::hyperedges()
        };
        
        let scan = if let Some(ref ns) = self.namespace {
            scan.with_namespace(ns)
        } else {
            scan
        };
        
        Ok(PyHyperedgeFrame {
            plan: LogicalOp::Scan(scan),
            label: label.map(String::from),
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Hypergraph(uri='{}', namespace={:?}, executor='{}')",
            self.uri, self.namespace, self.executor
        )
    }
}

/// Python wrapper for NodeFrame - a frame representing nodes.
///
/// NodeFrame is immutable, lazy, and composable. All operations return new frames.
#[pyclass(name = "NodeFrame")]
#[derive(Clone)]
pub struct PyNodeFrame {
    /// The logical plan for this frame.
    pub(crate) plan: LogicalOp,
    /// Node label filter (if any).
    label: Option<String>,
}

#[pymethods]
impl PyNodeFrame {
    /// Filter nodes based on a predicate expression.
    ///
    /// Args:
    ///     predicate: Boolean expression (Expr that evaluates to bool)
    ///
    /// Returns:
    ///     New NodeFrame with filtered rows
    fn filter(&self, predicate: &PyExpr) -> PyResult<Self> {
        let filter_op = FilterOp::new(predicate.to_logical_expr());
        Ok(Self {
            plan: LogicalOp::Filter {
                input: Box::new(self.plan.clone()),
                filter: filter_op,
            },
            label: self.label.clone(),
        })
    }

    /// Alias for filter().
    fn r#where(&self, predicate: &PyExpr) -> PyResult<Self> {
        self.filter(predicate)
    }

    /// Project columns (select, rename, compute expressions).
    ///
    /// Args:
    ///     *columns: Column names or expressions to select
    ///     **aliases: Keyword arguments for aliased columns
    #[pyo3(signature = (*columns, **aliases))]
    fn select(
        &self,
        columns: Vec<Bound<'_, PyAny>>,
        aliases: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let mut projections = Vec::new();
        
        // Process positional columns
        for col in columns {
            if let Ok(expr) = col.extract::<PyExpr>() {
                projections.push(expr.to_logical_expr());
            } else if let Ok(name) = col.extract::<String>() {
                if name == "*" {
                    projections.push(grism_logical::LogicalExpr::Wildcard);
                } else {
                    projections.push(grism_logical::expr::col(&name));
                }
            }
        }
        
        // Process keyword aliases
        if let Some(kw) = aliases {
            for (key, value) in kw.iter() {
                let alias: String = key.extract()?;
                if let Ok(expr) = value.extract::<PyExpr>() {
                    projections.push(expr.to_logical_expr().alias(&alias));
                }
            }
        }
        
        let project_op = ProjectOp::new(projections);
        
        Ok(Self {
            plan: LogicalOp::Project {
                input: Box::new(self.plan.clone()),
                project: project_op,
            },
            label: self.label.clone(),
        })
    }

    /// Add a computed column without removing existing columns.
    fn with_column(&self, name: &str, expr: &PyExpr) -> PyResult<Self> {
        let projection = vec![
            grism_logical::LogicalExpr::Wildcard,
            expr.to_logical_expr().alias(name),
        ];
        let project_op = ProjectOp::new(projection);
        
        Ok(Self {
            plan: LogicalOp::Project {
                input: Box::new(self.plan.clone()),
                project: project_op,
            },
            label: self.label.clone(),
        })
    }

    /// Expand to adjacent nodes via hyperedges (graph traversal).
    ///
    /// Args:
    ///     edge: Edge/hyperedge label to traverse (None = any edge)
    ///     to: Target node label filter (None = any label)
    ///     direction: Traversal direction ("out", "in", "both")
    ///     hops: Number of hops (int or tuple for range, default 1)
    ///     as_: Alias for the expanded node frame
    ///     edge_as: Alias for accessing edge/hyperedge properties
    #[pyo3(signature = (edge=None, to=None, direction="out", hops=None, as_=None, edge_as=None))]
    fn expand(
        &self,
        edge: Option<&str>,
        to: Option<&str>,
        direction: &str,
        hops: Option<Bound<'_, PyAny>>,
        as_: Option<&str>,
        edge_as: Option<&str>,
    ) -> PyResult<Self> {
        let dir = match direction {
            "in" => Direction::Incoming,
            "out" => Direction::Outgoing,
            "both" => Direction::Both,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    format!("Invalid direction: {}. Must be 'in', 'out', or 'both'", direction)
                ))
            }
        };
        
        // Parse hops - can be int or tuple, default to 1
        let hop_range = if let Some(hops) = hops {
            if let Ok(h) = hops.extract::<u32>() {
                HopRange { min: h, max: h }
            } else if let Ok((min, max)) = hops.extract::<(u32, u32)>() {
                HopRange { min, max }
            } else {
                HopRange::single()
            }
        } else {
            HopRange::single()
        };
        
        let mut expand_op = ExpandOp::binary()
            .with_direction(dir)
            .with_hops(hop_range);
        
        if let Some(e) = edge {
            expand_op = expand_op.with_edge_label(e);
        }
        if let Some(t) = to {
            expand_op = expand_op.with_to_label(t);
        }
        if let Some(alias) = as_ {
            expand_op = expand_op.with_target_alias(alias);
        }
        if let Some(e_alias) = edge_as {
            expand_op = expand_op.with_edge_alias(e_alias);
        }
        
        Ok(Self {
            plan: LogicalOp::Expand {
                input: Box::new(self.plan.clone()),
                expand: expand_op,
            },
            label: to.map(String::from).or_else(|| self.label.clone()),
        })
    }

    /// Optional expansion - keeps source nodes even if no match found.
    #[pyo3(signature = (edge=None, to=None, direction="out", hops=None, as_=None, edge_as=None))]
    fn optional_expand(
        &self,
        edge: Option<&str>,
        to: Option<&str>,
        direction: &str,
        hops: Option<Bound<'_, PyAny>>,
        as_: Option<&str>,
        edge_as: Option<&str>,
    ) -> PyResult<Self> {
        // For now, implement as regular expand
        // TODO: Add LEFT OUTER JOIN semantics
        self.expand(edge, to, direction, hops, as_, edge_as)
    }

    /// Sort rows by one or more expressions.
    #[pyo3(signature = (*exprs, ascending=None))]
    fn order_by(&self, exprs: Vec<Bound<'_, PyAny>>, ascending: Option<Bound<'_, PyAny>>) -> PyResult<Self> {
        let mut sort_keys = Vec::new();
        
        // Handle ascending - can be bool or list of bools
        let ascending_list: Vec<bool> = if let Some(asc_val) = ascending {
            if let Ok(asc) = asc_val.extract::<bool>() {
                vec![asc; exprs.len()]
            } else if let Ok(list) = asc_val.extract::<Vec<bool>>() {
                list
            } else {
                vec![true; exprs.len()]
            }
        } else {
            vec![true; exprs.len()]
        };
        
        for (i, expr) in exprs.iter().enumerate() {
            let asc = ascending_list.get(i).copied().unwrap_or(true);
            
            let logical_expr = if let Ok(e) = expr.extract::<PyExpr>() {
                e.to_logical_expr()
            } else if let Ok(name) = expr.extract::<String>() {
                grism_logical::expr::col(&name)
            } else {
                continue;
            };
            
            sort_keys.push(SortKey {
                expr: logical_expr,
                ascending: asc,
                nulls_first: false,
            });
        }
        
        let sort_op = SortOp::new(sort_keys);
        
        Ok(Self {
            plan: LogicalOp::Sort {
                input: Box::new(self.plan.clone()),
                sort: sort_op,
            },
            label: self.label.clone(),
        })
    }

    /// Alias for order_by().
    #[pyo3(signature = (*exprs, ascending=None))]
    fn sort(&self, exprs: Vec<Bound<'_, PyAny>>, ascending: Option<Bound<'_, PyAny>>) -> PyResult<Self> {
        self.order_by(exprs, ascending)
    }

    /// Limit the number of rows returned.
    fn limit(&self, n: usize) -> PyResult<Self> {
        let limit_op = LimitOp::new(n);
        Ok(Self {
            plan: LogicalOp::Limit {
                input: Box::new(self.plan.clone()),
                limit: limit_op,
            },
            label: self.label.clone(),
        })
    }

    /// Skip the first n rows.
    fn offset(&self, n: usize) -> PyResult<Self> {
        let limit_op = LimitOp::with_offset(usize::MAX, n);
        Ok(Self {
            plan: LogicalOp::Limit {
                input: Box::new(self.plan.clone()),
                limit: limit_op,
            },
            label: self.label.clone(),
        })
    }

    /// Alias for offset().
    fn skip(&self, n: usize) -> PyResult<Self> {
        self.offset(n)
    }

    /// Return first n rows.
    #[pyo3(signature = (n=5))]
    fn head(&self, n: usize) -> PyResult<Self> {
        self.limit(n)
    }

    /// Remove duplicate rows.
    fn distinct(&self) -> PyResult<Self> {
        // Implement as Project with DISTINCT flag
        // For now, use a placeholder
        Ok(self.clone())
    }

    /// Group rows by key expressions.
    #[pyo3(signature = (*keys))]
    fn group_by(&self, keys: Vec<Bound<'_, PyAny>>) -> PyResult<PyGroupedFrame> {
        let mut group_keys = Vec::new();
        
        for key in keys {
            if let Ok(expr) = key.extract::<PyExpr>() {
                group_keys.push(expr.inner.clone());
            } else if let Ok(name) = key.extract::<String>() {
                group_keys.push(ExprKind::Column(name));
            }
        }
        
        Ok(PyGroupedFrame {
            input_plan: self.plan.clone(),
            group_keys,
        })
    }

    /// Alias for group_by().
    #[pyo3(signature = (*keys))]
    fn groupby(&self, keys: Vec<Bound<'_, PyAny>>) -> PyResult<PyGroupedFrame> {
        self.group_by(keys)
    }

    /// Execute the query and return results.
    #[pyo3(signature = (executor=None, as_pandas=false, as_arrow=false, as_polars=false))]
    fn collect(
        &self,
        executor: Option<&str>,
        as_pandas: bool,
        as_arrow: bool,
        as_polars: bool,
    ) -> PyResult<Vec<pyo3::PyObject>> {
        let _ = (executor, as_pandas, as_arrow, as_polars);
        // TODO: Implement actual execution
        Python::with_gil(|_py| Ok(Vec::new()))
    }

    /// Execute and return the row count.
    fn count(&self) -> PyResult<usize> {
        // TODO: Implement actual execution
        Ok(0)
    }

    /// Execute and return the first row.
    fn first(&self) -> PyResult<Option<pyo3::PyObject>> {
        // TODO: Implement actual execution
        Ok(None)
    }

    /// Explain the query plan.
    #[pyo3(signature = (mode="logical"))]
    fn explain(&self, mode: &str) -> PyResult<String> {
        let plan = LogicalPlan::new(self.plan.clone());
        match mode {
            "logical" | "optimized" | "physical" | "cost" => Ok(plan.explain()),
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Invalid mode: {}. Must be 'logical', 'optimized', 'physical', or 'cost'", mode)
            )),
        }
    }

    fn __repr__(&self) -> String {
        format!("NodeFrame(label={:?})", self.label)
    }

    fn __str__(&self) -> String {
        let plan = LogicalPlan::new(self.plan.clone());
        plan.explain()
    }

    fn __iter__(&self) -> PyResult<pyo3::PyObject> {
        // Return an iterator over collected results
        self.collect(None, false, false, false).map(|v| {
            Python::with_gil(|py| v.into_py(py))
        })
    }
}

/// Python wrapper for EdgeFrame - a frame representing binary edges.
#[pyclass(name = "EdgeFrame")]
#[derive(Clone)]
pub struct PyEdgeFrame {
    /// The logical plan for this frame.
    pub(crate) plan: LogicalOp,
    /// Edge label filter (if any).
    label: Option<String>,
}

#[pymethods]
impl PyEdgeFrame {
    /// Filter edges based on a predicate expression.
    fn filter(&self, predicate: &PyExpr) -> PyResult<Self> {
        let filter_op = FilterOp::new(predicate.to_logical_expr());
        Ok(Self {
            plan: LogicalOp::Filter {
                input: Box::new(self.plan.clone()),
                filter: filter_op,
            },
            label: self.label.clone(),
        })
    }

    /// Get source nodes of these edges.
    fn source(&self) -> PyResult<PyNodeFrame> {
        let expand = ExpandOp::binary()
            .with_direction(Direction::Incoming)
            .with_target_alias("source");
        
        Ok(PyNodeFrame {
            plan: LogicalOp::Expand {
                input: Box::new(self.plan.clone()),
                expand,
            },
            label: None,
        })
    }

    /// Get target nodes of these edges.
    fn target(&self) -> PyResult<PyNodeFrame> {
        let expand = ExpandOp::binary()
            .with_direction(Direction::Outgoing)
            .with_target_alias("target");
        
        Ok(PyNodeFrame {
            plan: LogicalOp::Expand {
                input: Box::new(self.plan.clone()),
                expand,
            },
            label: None,
        })
    }

    /// Get nodes connected by these edges.
    #[pyo3(signature = (which="both"))]
    fn endpoints(&self, which: &str) -> PyResult<PyNodeFrame> {
        match which {
            "source" => self.source(),
            "target" => self.target(),
            "both" => {
                // Return union of source and target
                self.source() // Simplified for now
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Invalid which: {}. Must be 'source', 'target', or 'both'", which)
            )),
        }
    }

    /// Limit the number of rows.
    fn limit(&self, n: usize) -> PyResult<Self> {
        let limit_op = LimitOp::new(n);
        Ok(Self {
            plan: LogicalOp::Limit {
                input: Box::new(self.plan.clone()),
                limit: limit_op,
            },
            label: self.label.clone(),
        })
    }

    /// Execute and collect results.
    fn collect(&self) -> PyResult<Vec<pyo3::PyObject>> {
        Python::with_gil(|_py| Ok(Vec::new()))
    }

    /// Explain the query plan.
    #[pyo3(signature = (mode="logical"))]
    fn explain(&self, mode: &str) -> PyResult<String> {
        let _ = mode;
        let plan = LogicalPlan::new(self.plan.clone());
        Ok(plan.explain())
    }

    fn __repr__(&self) -> String {
        format!("EdgeFrame(label={:?})", self.label)
    }
}

/// Python wrapper for HyperedgeFrame - a frame representing hyperedges.
#[pyclass(name = "HyperedgeFrame")]
#[derive(Clone)]
pub struct PyHyperedgeFrame {
    /// The logical plan for this frame.
    pub(crate) plan: LogicalOp,
    /// Hyperedge label filter (if any).
    label: Option<String>,
}

#[pymethods]
impl PyHyperedgeFrame {
    /// Filter hyperedges based on a predicate expression.
    fn filter(&self, predicate: &PyExpr) -> PyResult<Self> {
        let filter_op = FilterOp::new(predicate.to_logical_expr());
        Ok(Self {
            plan: LogicalOp::Filter {
                input: Box::new(self.plan.clone()),
                filter: filter_op,
            },
            label: self.label.clone(),
        })
    }

    /// Filter hyperedges where a role matches a value.
    fn where_role(&self, role: &str, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        // Create a filter on the role binding
        let value_expr = if let Ok(s) = value.extract::<String>() {
            grism_logical::LogicalExpr::literal(s)
        } else if let Ok(i) = value.extract::<i64>() {
            grism_logical::LogicalExpr::literal(i)
        } else {
            grism_logical::LogicalExpr::literal(value.str()?.to_string())
        };
        
        let role_ref = grism_logical::expr::col(role);
        let predicate = role_ref.eq(value_expr);
        let filter_op = FilterOp::new(predicate);
        
        Ok(Self {
            plan: LogicalOp::Filter {
                input: Box::new(self.plan.clone()),
                filter: filter_op,
            },
            label: self.label.clone(),
        })
    }

    /// Expand to nodes connected via a specific role.
    #[pyo3(signature = (role, as_=None))]
    fn expand(&self, role: &str, as_: Option<&str>) -> PyResult<PyNodeFrame> {
        let mut expand_op = ExpandOp::role("_", role);
        
        if let Some(alias) = as_ {
            expand_op = expand_op.with_target_alias(alias);
        }
        
        Ok(PyNodeFrame {
            plan: LogicalOp::Expand {
                input: Box::new(self.plan.clone()),
                expand: expand_op,
            },
            label: None,
        })
    }

    /// Get all role names present in this hyperedge frame.
    fn roles(&self) -> PyResult<Vec<String>> {
        // Would need schema information to return actual roles
        Ok(Vec::new())
    }

    /// Limit the number of rows.
    fn limit(&self, n: usize) -> PyResult<Self> {
        let limit_op = LimitOp::new(n);
        Ok(Self {
            plan: LogicalOp::Limit {
                input: Box::new(self.plan.clone()),
                limit: limit_op,
            },
            label: self.label.clone(),
        })
    }

    /// Execute and collect results.
    fn collect(&self) -> PyResult<Vec<pyo3::PyObject>> {
        Python::with_gil(|_py| Ok(Vec::new()))
    }

    /// Explain the query plan.
    #[pyo3(signature = (mode="logical"))]
    fn explain(&self, mode: &str) -> PyResult<String> {
        let _ = mode;
        let plan = LogicalPlan::new(self.plan.clone());
        Ok(plan.explain())
    }

    fn __repr__(&self) -> String {
        format!("HyperedgeFrame(label={:?})", self.label)
    }
}

/// Python wrapper for GroupedFrame - result of group_by().
#[pyclass(name = "GroupedFrame")]
#[derive(Clone)]
pub struct PyGroupedFrame {
    /// The input plan.
    input_plan: LogicalOp,
    /// Grouping keys.
    group_keys: Vec<ExprKind>,
}

#[pymethods]
impl PyGroupedFrame {
    /// Apply aggregations to grouped rows.
    #[pyo3(signature = (*aggs, **named_aggs))]
    fn agg(
        &self,
        aggs: Vec<PyAggExpr>,
        named_aggs: Option<Bound<'_, PyDict>>,
    ) -> PyResult<PyNodeFrame> {
        let keys: Vec<_> = self.group_keys.iter()
            .map(|k| k.to_logical_expr())
            .collect();
        
        let mut aggregations = Vec::new();
        
        // Process positional aggregations
        for agg in aggs {
            aggregations.push(agg.to_rust_agg_expr());
        }
        
        // Process keyword aggregations
        if let Some(kw) = named_aggs {
            for (key, value) in kw.iter() {
                let alias: String = key.extract()?;
                if let Ok(agg) = value.extract::<PyAggExpr>() {
                    let mut rust_agg = agg.to_rust_agg_expr();
                    rust_agg = rust_agg.with_alias(&alias);
                    aggregations.push(rust_agg);
                }
            }
        }
        
        let agg_op = AggregateOp::new(keys, aggregations);
        
        Ok(PyNodeFrame {
            plan: LogicalOp::Aggregate {
                input: Box::new(self.input_plan.clone()),
                aggregate: agg_op,
            },
            label: None,
        })
    }

    /// Count rows per group.
    fn count(&self) -> PyResult<PyNodeFrame> {
        let keys: Vec<_> = self.group_keys.iter()
            .map(|k| k.to_logical_expr())
            .collect();
        
        let count_agg = grism_logical::AggExpr::count_star().with_alias("count");
        let agg_op = AggregateOp::new(keys, vec![count_agg]);
        
        Ok(PyNodeFrame {
            plan: LogicalOp::Aggregate {
                input: Box::new(self.input_plan.clone()),
                aggregate: agg_op,
            },
            label: None,
        })
    }

    fn __repr__(&self) -> String {
        format!("GroupedFrame(keys={:?})", self.group_keys.len())
    }
}

// Re-export with proper names for the module
pub use PyHypergraph as PyHyperGraph;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hypergraph_creation() {
        Python::with_gil(|_py| {
            let hg = PyHypergraph::connect("grism://local", "local", None).unwrap();
            assert_eq!(hg.uri, "grism://local");
            assert!(hg.namespace.is_none());
        });
    }

    #[test]
    fn test_node_frame_creation() {
        Python::with_gil(|_py| {
            let hg = PyHypergraph::connect("grism://local", "local", None).unwrap();
            let nf = hg.nodes(Some("Person")).unwrap();
            assert_eq!(nf.label, Some("Person".to_string()));
        });
    }
}
