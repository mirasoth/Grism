//! Python bindings for Hypergraph and Frame types.
//!
//! This module provides PyO3 bindings for the Grism Python API,
//! implementing the Frame types (NodeFrame, EdgeFrame, HyperedgeFrame)
//! with proper lowering to Rust logical plans.

#![allow(dead_code, unused_imports, unused_variables)] // Python bindings may have unused items
#![allow(clippy::useless_conversion)] // Some conversions are for type clarity

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::StringArray;
use common_config::ExecutionConfig;
use grism_core::types::Value;
use grism_engine::planner::{LocalPhysicalPlanner, PhysicalPlanner};
use grism_engine::{ExecutionContext, ExecutionResult, LocalExecutor};
use grism_logical::{
    AggregateOp, ExpandOp, FilterOp, LimitOp, LogicalOp, LogicalPlan, ProjectOp, ScanOp, SortKey,
    SortOp, UnionOp,
    ops::{Direction, HopRange},
    python::{ExprKind, PyAggExpr, PyExpr},
};
use grism_storage::{InMemoryStorage, SnapshotId, Storage};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

/// Convert a Grism Value to a Python object.
fn value_to_py(py: Python<'_>, value: &Value) -> PyObject {
    match value {
        Value::Null => py.None(),
        Value::Bool(b) => b.into_py(py),
        Value::Int64(i) => i.into_py(py),
        Value::Float64(f) => f.into_py(py),
        Value::String(s) => s.into_py(py),
        Value::Symbol(s) => s.into_py(py),
        Value::Binary(b) => b.clone().into_py(py),
        Value::Array(arr) => {
            let list = PyList::empty_bound(py);
            for v in arr {
                list.append(value_to_py(py, v)).ok();
            }
            list.into_py(py)
        }
        Value::Map(map) => {
            let dict = PyDict::new_bound(py);
            for (k, v) in map {
                dict.set_item(k, value_to_py(py, v)).ok();
            }
            dict.into_py(py)
        }
        Value::Vector(v) => v.clone().into_py(py),
        Value::Timestamp(t) => t.into_py(py),
        Value::Date(d) => d.into_py(py),
    }
}

/// Convert a row (HashMap<String, Value>) to a Python dict.
fn row_to_py(py: Python<'_>, row: &HashMap<String, Value>) -> PyObject {
    let dict = PyDict::new_bound(py);
    for (k, v) in row {
        dict.set_item(k, value_to_py(py, v)).ok();
    }
    dict.into_py(py)
}

/// Convert ExecutionResult to a list of Python dicts.
fn result_to_py(py: Python<'_>, result: ExecutionResult) -> Vec<PyObject> {
    result
        .batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row_idx| {
                let mut dict: HashMap<String, PyObject> = HashMap::new();
                for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                    if let Some(array) = batch
                        .column(col_idx)
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                    {
                        dict.insert(field.name().to_string(), array.value(row_idx).into_py(py));
                    }
                }
                dict.into_py(py)
            })
        })
        .collect()
}

/// Execute a logical plan and return Python results.
fn execute_plan(plan: &LogicalOp, _config: Option<&ExecutionConfig>) -> PyResult<ExecutionResult> {
    let logical_plan = LogicalPlan::new(plan.clone());

    // Create a simple in-memory storage for execution
    let storage = Arc::new(crate::storage::InMemoryStorage::new());

    // Create physical plan
    let planner = LocalPhysicalPlanner::new();
    let physical_plan = planner.plan(&logical_plan).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Planning error: {}", e))
    })?;

    // Execute the plan
    let executor = LocalExecutor::new();
    executor
        .execute_sync(
            physical_plan,
            storage as Arc<dyn Storage>,
            SnapshotId::default(),
        )
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Execution error: {}", e))
        })
}

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
    /// Execution configuration.
    exec_config: Option<ExecutionConfig>,
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
            exec_config: None,
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
            exec_config: None,
        })
    }

    /// Create a new Hypergraph scoped to a namespace.
    fn with_namespace(&self, name: &str) -> Self {
        Self {
            uri: self.uri.clone(),
            namespace: Some(name.to_string()),
            executor: self.executor.clone(),
            exec_config: self.exec_config.clone(),
        }
    }

    /// Configure execution settings.
    ///
    /// Args:
    ///     parallelism: Number of parallel threads (None = auto)
    ///     memory_limit: Memory limit in bytes (None = unlimited)
    ///     batch_size: Batch size for processing (default 1024)
    #[pyo3(signature = (parallelism=None, memory_limit=None))]
    fn with_config(&self, parallelism: Option<usize>, memory_limit: Option<usize>) -> Self {
        Self {
            uri: self.uri.clone(),
            namespace: self.namespace.clone(),
            executor: self.executor.clone(),
            exec_config: Some(ExecutionConfig {
                default_executor: common_config::ExecutorType::Local,
                parallelism,
                memory_limit,
            }),
        }
    }

    // ========== Schema Introspection Methods ==========

    /// Get all node labels in the graph.
    ///
    /// Returns:
    ///     List of node label names
    fn node_labels(&self) -> PyResult<Vec<String>> {
        // TODO: Query catalog for actual labels
        // For now, return empty list (will be populated when storage is connected)
        Ok(Vec::new())
    }

    /// Get all edge/relationship types in the graph.
    ///
    /// Returns:
    ///     List of edge type names
    fn edge_types(&self) -> PyResult<Vec<String>> {
        // TODO: Query catalog for actual edge types
        Ok(Vec::new())
    }

    /// Get all hyperedge labels in the graph.
    ///
    /// Returns:
    ///     List of hyperedge label names
    fn hyperedge_labels(&self) -> PyResult<Vec<String>> {
        // TODO: Query catalog for actual hyperedge labels
        Ok(Vec::new())
    }

    /// Get property keys for a given node label.
    ///
    /// Args:
    ///     label: Node label to get properties for
    ///
    /// Returns:
    ///     List of property names
    fn node_properties(&self, label: &str) -> PyResult<Vec<String>> {
        let _ = label;
        // TODO: Query catalog for actual properties
        Ok(Vec::new())
    }

    /// Get property keys for a given edge type.
    ///
    /// Args:
    ///     edge_type: Edge type to get properties for
    ///
    /// Returns:
    ///     List of property names
    fn edge_properties(&self, edge_type: &str) -> PyResult<Vec<String>> {
        let _ = edge_type;
        // TODO: Query catalog for actual properties
        Ok(Vec::new())
    }

    /// Get the number of nodes in the graph.
    ///
    /// Args:
    ///     label: Optional node label to count (None = all nodes)
    ///
    /// Returns:
    ///     Number of nodes
    #[pyo3(signature = (label=None))]
    fn node_count(&self, label: Option<&str>) -> PyResult<usize> {
        // Create count query
        let scan = if let Some(l) = label {
            ScanOp::nodes_with_label(l)
        } else {
            ScanOp::nodes()
        };

        let plan = LogicalOp::Scan(scan);
        let result = execute_plan(&plan, self.exec_config.as_ref())?;
        Ok(result.total_rows())
    }

    /// Get the number of edges in the graph.
    ///
    /// Args:
    ///     edge_type: Optional edge type to count (None = all edges)
    ///
    /// Returns:
    ///     Number of edges
    #[pyo3(signature = (edge_type=None))]
    fn edge_count(&self, edge_type: Option<&str>) -> PyResult<usize> {
        let scan = if let Some(t) = edge_type {
            ScanOp::edges_with_label(t)
        } else {
            ScanOp::edges()
        };

        let plan = LogicalOp::Scan(scan);
        let result = execute_plan(&plan, self.exec_config.as_ref())?;
        Ok(result.total_rows())
    }

    /// Get the number of hyperedges in the graph.
    ///
    /// Args:
    ///     label: Optional hyperedge label to count (None = all hyperedges)
    ///
    /// Returns:
    ///     Number of hyperedges
    #[pyo3(signature = (label=None))]
    fn hyperedge_count(&self, label: Option<&str>) -> PyResult<usize> {
        let scan = if let Some(l) = label {
            ScanOp::hyperedges_with_label(l)
        } else {
            ScanOp::hyperedges()
        };

        let plan = LogicalOp::Scan(scan);
        let result = execute_plan(&plan, self.exec_config.as_ref())?;
        Ok(result.total_rows())
    }

    /// Begin a transaction for write operations.
    ///
    /// Returns:
    ///     Transaction context manager
    fn transaction(&self) -> PyResult<PyTransaction> {
        Ok(PyTransaction::new(self.clone()))
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
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Invalid direction: {}. Must be 'in', 'out', or 'both'",
                    direction
                )));
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

        let mut expand_op = ExpandOp::binary().with_direction(dir).with_hops(hop_range);

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
    fn order_by(
        &self,
        exprs: Vec<Bound<'_, PyAny>>,
        ascending: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
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
    fn sort(
        &self,
        exprs: Vec<Bound<'_, PyAny>>,
        ascending: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
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

    /// Explode an array column into separate rows.
    ///
    /// This is similar to SQL's UNNEST or Spark's explode function.
    /// Each array element becomes a separate row.
    ///
    /// Args:
    ///     column: The array column to explode
    ///     as_: Alias for the exploded values (optional)
    #[pyo3(signature = (column, as_=None))]
    fn explode(&self, column: &str, as_: Option<&str>) -> PyResult<Self> {
        use grism_logical::expr::{FuncExpr, LogicalExpr};

        // Create an explode expression: explode(col(column))
        let explode_func = FuncExpr::udf("explode", vec![grism_logical::expr::col(column)]);
        let explode_expr = LogicalExpr::Function(explode_func);

        // If alias is provided, wrap in Alias expression
        let final_expr = if let Some(alias) = as_ {
            LogicalExpr::Alias {
                expr: Box::new(explode_expr),
                alias: alias.to_string(),
            }
        } else {
            explode_expr
        };

        let project_op = ProjectOp::new(vec![final_expr]);

        Ok(Self {
            plan: LogicalOp::Project {
                input: Box::new(self.plan.clone()),
                project: project_op,
            },
            label: self.label.clone(),
        })
    }

    /// Group rows by key expressions.
    #[pyo3(signature = (*keys, **kwargs))]
    fn group_by(
        &self,
        keys: Vec<Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyGroupedFrame> {
        let mut group_keys = Vec::new();

        // Process positional keys
        for key in keys {
            if let Ok(expr) = key.extract::<PyExpr>() {
                group_keys.push(expr.inner.clone());
            } else if let Ok(name) = key.extract::<String>() {
                group_keys.push(ExprKind::Column(name));
            }
        }

        // Process keyword arguments for aliasing
        if let Some(kwargs) = kwargs {
            for (key, value) in kwargs {
                if let Ok(expr) = value.extract::<PyExpr>() {
                    // Create an aliased expression
                    group_keys.push(ExprKind::Alias {
                        expr: Box::new(expr.inner.clone()),
                        alias: key.to_string(),
                    });
                }
            }
        }

        Ok(PyGroupedFrame {
            input_plan: self.plan.clone(),
            group_keys,
        })
    }

    /// Alias for group_by().
    #[pyo3(signature = (*keys, **kwargs))]
    fn groupby(
        &self,
        keys: Vec<Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyGroupedFrame> {
        self.group_by(keys, kwargs)
    }

    /// Execute the query and return results.
    ///
    /// Args:
    ///     executor: Executor type ("local" or "ray"), default "local"
    ///     as_pandas: Return as pandas DataFrame (requires pandas)
    ///     as_arrow: Return as Arrow RecordBatch (requires pyarrow)
    ///     as_polars: Return as polars DataFrame (requires polars)
    ///
    /// Returns:
    ///     List of dicts (default), or DataFrame if as_pandas/as_polars is True
    #[pyo3(signature = (executor=None, as_pandas=false, as_arrow=false, as_polars=false))]
    fn collect(
        &self,
        executor: Option<&str>,
        as_pandas: bool,
        as_arrow: bool,
        as_polars: bool,
    ) -> PyResult<PyObject> {
        let _ = (executor, as_arrow);

        // Execute the plan
        let result = execute_plan(&self.plan, None)?;

        Python::with_gil(|py| {
            let rows = result_to_py(py, result);

            // Convert to pandas DataFrame if requested
            if as_pandas {
                let pandas = py.import_bound("pandas")?;
                let df = pandas.call_method1("DataFrame", (rows,))?;
                return Ok(df.into_py(py));
            }

            // Convert to polars DataFrame if requested
            if as_polars {
                let polars = py.import_bound("polars")?;
                let df = polars.call_method1("from_dicts", (rows,))?;
                return Ok(df.into_py(py));
            }

            // Return as list of dicts
            Ok(rows.into_py(py))
        })
    }

    /// Execute and return the row count.
    fn count(&self) -> PyResult<usize> {
        let result = execute_plan(&self.plan, None)?;
        Ok(result.total_rows())
    }

    /// Execute and return the first row.
    fn first(&self) -> PyResult<Option<PyObject>> {
        // Add limit 1 to the plan
        let limited_plan = LogicalOp::Limit {
            input: Box::new(self.plan.clone()),
            limit: LimitOp::new(1),
        };

        let result = execute_plan(&limited_plan, None)?;

        Python::with_gil(|py| {
            let rows = result_to_py(py, result);
            Ok(rows.into_iter().next())
        })
    }

    /// Check if the frame has any results.
    fn exists(&self) -> PyResult<bool> {
        Ok(self.count()? > 0)
    }

    /// Get the schema of this frame.
    fn schema(&self) -> PyResult<PyFrameSchema> {
        // Extract schema from the plan
        // For now, return a placeholder schema
        Ok(PyFrameSchema {
            columns: Vec::new(),
            entity_type: "node".to_string(),
            label: self.label.clone(),
        })
    }

    /// Explain the query plan.
    #[pyo3(signature = (mode="logical"))]
    fn explain(&self, mode: &str) -> PyResult<String> {
        let plan = LogicalPlan::new(self.plan.clone());
        match mode {
            "logical" | "optimized" | "physical" | "cost" => Ok(plan.explain()),
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Invalid mode: {}. Must be 'logical', 'optimized', 'physical', or 'cost'",
                mode
            ))),
        }
    }

    fn __repr__(&self) -> String {
        format!("NodeFrame(label={:?})", self.label)
    }

    fn __str__(&self) -> String {
        let plan = LogicalPlan::new(self.plan.clone());
        plan.explain()
    }

    fn __iter__(&self) -> PyResult<PyObject> {
        // Return an iterator over collected results
        let result = execute_plan(&self.plan, None)?;

        Python::with_gil(|py| {
            let rows = result_to_py(py, result);
            let list = PyList::new_bound(py, rows);
            Ok(list.call_method0("__iter__")?.into_py(py))
        })
    }

    fn __len__(&self) -> PyResult<usize> {
        self.count()
    }

    /// Union with another frame.
    fn union(&self, other: &Self) -> PyResult<Self> {
        Ok(Self {
            plan: LogicalOp::Union {
                left: Box::new(self.plan.clone()),
                right: Box::new(other.plan.clone()),
                union: UnionOp::all(),
            },
            label: self.label.clone(),
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
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Invalid which: {}. Must be 'source', 'target', or 'both'",
                which
            ))),
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
    ///
    /// Args:
    ///     as_pandas: Return as pandas DataFrame
    ///     as_polars: Return as polars DataFrame
    ///
    /// Returns:
    ///     List of dicts or DataFrame
    #[pyo3(signature = (as_pandas=false, as_polars=false))]
    fn collect(&self, as_pandas: bool, as_polars: bool) -> PyResult<PyObject> {
        let result = execute_plan(&self.plan, None)?;

        Python::with_gil(|py| {
            let rows = result_to_py(py, result);

            if as_pandas {
                let pandas = py.import_bound("pandas")?;
                let df = pandas.call_method1("DataFrame", (rows,))?;
                return Ok(df.into_py(py));
            }

            if as_polars {
                let polars = py.import_bound("polars")?;
                let df = polars.call_method1("from_dicts", (rows,))?;
                return Ok(df.into_py(py));
            }

            Ok(rows.into_py(py))
        })
    }

    /// Execute and return the row count.
    fn count(&self) -> PyResult<usize> {
        let result = execute_plan(&self.plan, None)?;
        Ok(result.total_rows())
    }

    /// Execute and return the first row.
    fn first(&self) -> PyResult<Option<PyObject>> {
        let limited_plan = LogicalOp::Limit {
            input: Box::new(self.plan.clone()),
            limit: LimitOp::new(1),
        };

        let result = execute_plan(&limited_plan, None)?;

        Python::with_gil(|py| {
            let rows = result_to_py(py, result);
            Ok(rows.into_iter().next())
        })
    }

    /// Check if the frame has any results.
    fn exists(&self) -> PyResult<bool> {
        Ok(self.count()? > 0)
    }

    /// Get the schema of this frame.
    fn schema(&self) -> PyResult<PyFrameSchema> {
        Ok(PyFrameSchema {
            columns: Vec::new(),
            entity_type: "edge".to_string(),
            label: self.label.clone(),
        })
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

    fn __len__(&self) -> PyResult<usize> {
        self.count()
    }

    /// Union with another frame.
    fn union(&self, other: &Self) -> PyResult<Self> {
        Ok(Self {
            plan: LogicalOp::Union {
                left: Box::new(self.plan.clone()),
                right: Box::new(other.plan.clone()),
                union: UnionOp::all(),
            },
            label: self.label.clone(),
        })
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
    ///
    /// Args:
    ///     as_pandas: Return as pandas DataFrame
    ///     as_polars: Return as polars DataFrame
    ///
    /// Returns:
    ///     List of dicts or DataFrame
    #[pyo3(signature = (as_pandas=false, as_polars=false))]
    fn collect(&self, as_pandas: bool, as_polars: bool) -> PyResult<PyObject> {
        let result = execute_plan(&self.plan, None)?;

        Python::with_gil(|py| {
            let rows = result_to_py(py, result);

            if as_pandas {
                let pandas = py.import_bound("pandas")?;
                let df = pandas.call_method1("DataFrame", (rows,))?;
                return Ok(df.into_py(py));
            }

            if as_polars {
                let polars = py.import_bound("polars")?;
                let df = polars.call_method1("from_dicts", (rows,))?;
                return Ok(df.into_py(py));
            }

            Ok(rows.into_py(py))
        })
    }

    /// Execute and return the row count.
    fn count(&self) -> PyResult<usize> {
        let result = execute_plan(&self.plan, None)?;
        Ok(result.total_rows())
    }

    /// Execute and return the first row.
    fn first(&self) -> PyResult<Option<PyObject>> {
        let limited_plan = LogicalOp::Limit {
            input: Box::new(self.plan.clone()),
            limit: LimitOp::new(1),
        };

        let result = execute_plan(&limited_plan, None)?;

        Python::with_gil(|py| {
            let rows = result_to_py(py, result);
            Ok(rows.into_iter().next())
        })
    }

    /// Check if the frame has any results.
    fn exists(&self) -> PyResult<bool> {
        Ok(self.count()? > 0)
    }

    /// Get the schema of this frame.
    fn schema(&self) -> PyResult<PyFrameSchema> {
        Ok(PyFrameSchema {
            columns: Vec::new(),
            entity_type: "hyperedge".to_string(),
            label: self.label.clone(),
        })
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

    fn __len__(&self) -> PyResult<usize> {
        self.count()
    }

    /// Union with another frame.
    fn union(&self, other: &Self) -> PyResult<Self> {
        Ok(Self {
            plan: LogicalOp::Union {
                left: Box::new(self.plan.clone()),
                right: Box::new(other.plan.clone()),
                union: UnionOp::all(),
            },
            label: self.label.clone(),
        })
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
        let keys: Vec<_> = self
            .group_keys
            .iter()
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
        let keys: Vec<_> = self
            .group_keys
            .iter()
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

/// Python wrapper for frame schema information.
#[pyclass(name = "FrameSchema")]
#[derive(Clone)]
pub struct PyFrameSchema {
    /// Column information.
    columns: Vec<(String, String, bool)>, // (name, type, nullable)
    /// Entity type ("node", "edge", "hyperedge").
    entity_type: String,
    /// Entity label (if any).
    label: Option<String>,
}

#[pymethods]
impl PyFrameSchema {
    /// Get column names.
    fn column_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|(name, _, _)| name.clone())
            .collect()
    }

    /// Get column count.
    fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get entity type.
    fn entity_type(&self) -> String {
        self.entity_type.clone()
    }

    /// Get entity label.
    fn label(&self) -> Option<String> {
        self.label.clone()
    }

    /// Check if a column exists.
    fn has_column(&self, name: &str) -> bool {
        self.columns.iter().any(|(n, _, _)| n == name)
    }

    /// Get column type by name.
    fn column_type(&self, name: &str) -> Option<String> {
        self.columns
            .iter()
            .find(|(n, _, _)| n == name)
            .map(|(_, t, _)| t.clone())
    }

    fn __repr__(&self) -> String {
        format!(
            "FrameSchema(entity_type='{}', label={:?}, columns={})",
            self.entity_type,
            self.label,
            self.columns.len()
        )
    }

    fn __str__(&self) -> String {
        let mut s = format!("FrameSchema ({}):\n", self.entity_type);
        if let Some(ref label) = self.label {
            s.push_str(&format!("  label: {}\n", label));
        }
        s.push_str("  columns:\n");
        for (name, dtype, nullable) in &self.columns {
            let null_str = if *nullable { " (nullable)" } else { "" };
            s.push_str(&format!("    {}: {}{}\n", name, dtype, null_str));
        }
        s
    }
}

/// Python wrapper for transactions.
///
/// Transaction provides atomic write operations on the hypergraph.
/// Supports context manager protocol for automatic commit/rollback.
#[pyclass(name = "Transaction")]
#[derive(Clone)]
pub struct PyTransaction {
    /// Parent hypergraph.
    hg: PyHypergraph,
    /// Pending write operations.
    pending_ops: Vec<WriteOp>,
    /// Whether the transaction is active.
    active: bool,
}

/// Internal enum for pending write operations.
#[derive(Clone, Debug)]
enum WriteOp {
    CreateNode {
        label: String,
        properties: HashMap<String, Value>,
    },
    CreateEdge {
        edge_type: String,
        source_id: String,
        target_id: String,
        properties: HashMap<String, Value>,
    },
    CreateHyperedge {
        label: String,
        bindings: Vec<(String, String)>, // (role, entity_id)
        properties: HashMap<String, Value>,
    },
    UpdateProperties {
        entity_id: String,
        properties: HashMap<String, Value>,
    },
    DeleteNode {
        node_id: String,
    },
    DeleteEdge {
        edge_id: String,
    },
    DeleteHyperedge {
        hyperedge_id: String,
    },
}

impl PyTransaction {
    fn new(hg: PyHypergraph) -> Self {
        Self {
            hg,
            pending_ops: Vec::new(),
            active: true,
        }
    }
}

#[pymethods]
impl PyTransaction {
    /// Create a new node.
    ///
    /// Args:
    ///     label: Node label
    ///     **properties: Node properties
    ///
    /// Returns:
    ///     Node ID (placeholder until commit)
    #[pyo3(signature = (label, **properties))]
    fn create_node(
        &mut self,
        label: &str,
        properties: Option<Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        if !self.active {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction is not active",
            ));
        }

        let props = if let Some(dict) = properties {
            dict_to_value_map(&dict)?
        } else {
            HashMap::new()
        };

        let node_id = format!("node_{}", self.pending_ops.len());

        self.pending_ops.push(WriteOp::CreateNode {
            label: label.to_string(),
            properties: props,
        });

        Ok(node_id)
    }

    /// Create a new edge (binary relationship).
    ///
    /// Args:
    ///     edge_type: Edge type/label
    ///     source: Source node ID
    ///     target: Target node ID
    ///     **properties: Edge properties
    ///
    /// Returns:
    ///     Edge ID (placeholder until commit)
    #[pyo3(signature = (edge_type, source, target, **properties))]
    fn create_edge(
        &mut self,
        edge_type: &str,
        source: &str,
        target: &str,
        properties: Option<Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        if !self.active {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction is not active",
            ));
        }

        let props = if let Some(dict) = properties {
            dict_to_value_map(&dict)?
        } else {
            HashMap::new()
        };

        let edge_id = format!("edge_{}", self.pending_ops.len());

        self.pending_ops.push(WriteOp::CreateEdge {
            edge_type: edge_type.to_string(),
            source_id: source.to_string(),
            target_id: target.to_string(),
            properties: props,
        });

        Ok(edge_id)
    }

    /// Create a new hyperedge (n-ary relationship).
    ///
    /// Args:
    ///     label: Hyperedge label
    ///     bindings: Dict mapping role names to entity IDs
    ///     **properties: Hyperedge properties
    ///
    /// Returns:
    ///     Hyperedge ID (placeholder until commit)
    #[pyo3(signature = (label, bindings, **properties))]
    fn create_hyperedge(
        &mut self,
        label: &str,
        bindings: Bound<'_, PyDict>,
        properties: Option<Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        if !self.active {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction is not active",
            ));
        }

        let binding_list: Vec<(String, String)> = bindings
            .iter()
            .filter_map(|(k, v)| {
                let role: String = k.extract().ok()?;
                let entity_id: String = v.extract().ok()?;
                Some((role, entity_id))
            })
            .collect();

        let props = if let Some(dict) = properties {
            dict_to_value_map(&dict)?
        } else {
            HashMap::new()
        };

        let he_id = format!("hyperedge_{}", self.pending_ops.len());

        self.pending_ops.push(WriteOp::CreateHyperedge {
            label: label.to_string(),
            bindings: binding_list,
            properties: props,
        });

        Ok(he_id)
    }

    /// Update properties on an entity.
    ///
    /// Args:
    ///     entity_id: ID of node, edge, or hyperedge
    ///     **properties: Properties to set or update
    #[pyo3(signature = (entity_id, **properties))]
    fn update(&mut self, entity_id: &str, properties: Option<Bound<'_, PyDict>>) -> PyResult<()> {
        if !self.active {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction is not active",
            ));
        }

        let props = if let Some(dict) = properties {
            dict_to_value_map(&dict)?
        } else {
            HashMap::new()
        };

        self.pending_ops.push(WriteOp::UpdateProperties {
            entity_id: entity_id.to_string(),
            properties: props,
        });

        Ok(())
    }

    /// Delete a node.
    ///
    /// Args:
    ///     node_id: ID of node to delete
    fn delete_node(&mut self, node_id: &str) -> PyResult<()> {
        if !self.active {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction is not active",
            ));
        }

        self.pending_ops.push(WriteOp::DeleteNode {
            node_id: node_id.to_string(),
        });

        Ok(())
    }

    /// Delete an edge.
    ///
    /// Args:
    ///     edge_id: ID of edge to delete
    fn delete_edge(&mut self, edge_id: &str) -> PyResult<()> {
        if !self.active {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction is not active",
            ));
        }

        self.pending_ops.push(WriteOp::DeleteEdge {
            edge_id: edge_id.to_string(),
        });

        Ok(())
    }

    /// Delete a hyperedge.
    ///
    /// Args:
    ///     hyperedge_id: ID of hyperedge to delete
    fn delete_hyperedge(&mut self, hyperedge_id: &str) -> PyResult<()> {
        if !self.active {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction is not active",
            ));
        }

        self.pending_ops.push(WriteOp::DeleteHyperedge {
            hyperedge_id: hyperedge_id.to_string(),
        });

        Ok(())
    }

    /// Commit the transaction.
    ///
    /// All pending operations are applied atomically.
    fn commit(&mut self) -> PyResult<()> {
        if !self.active {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction is not active",
            ));
        }

        // TODO: Implement actual commit to storage
        // For now, just mark as committed
        self.active = false;
        self.pending_ops.clear();

        Ok(())
    }

    /// Rollback the transaction.
    ///
    /// All pending operations are discarded.
    fn rollback(&mut self) -> PyResult<()> {
        if !self.active {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction is not active",
            ));
        }

        self.active = false;
        self.pending_ops.clear();

        Ok(())
    }

    /// Get the number of pending operations.
    fn pending_count(&self) -> usize {
        self.pending_ops.len()
    }

    /// Check if the transaction is active.
    fn is_active(&self) -> bool {
        self.active
    }

    // Context manager protocol
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        exc_type: Option<PyObject>,
        exc_val: Option<PyObject>,
        exc_tb: Option<PyObject>,
    ) -> PyResult<bool> {
        // Check if an exception occurred
        let has_exception = exc_type.is_some();

        // Consume the unused values
        let _ = (exc_type, exc_val, exc_tb);

        if self.active {
            // Auto-rollback on exception (if exc_type is Some)
            // Auto-commit otherwise
            if has_exception {
                self.rollback()?;
            } else {
                self.commit()?;
            }
        }
        Ok(false) // Don't suppress exceptions
    }

    fn __repr__(&self) -> String {
        format!(
            "Transaction(active={}, pending_ops={})",
            self.active,
            self.pending_ops.len()
        )
    }
}

/// Convert Python dict to Value map.
fn dict_to_value_map(dict: &Bound<'_, PyDict>) -> PyResult<HashMap<String, Value>> {
    let mut map = HashMap::new();
    for (key, value) in dict.iter() {
        let k: String = key.extract()?;
        let v = py_to_value(&value)?;
        map.insert(k, v);
    }
    Ok(map)
}

/// Convert Python value to Grism Value.
fn py_to_value(obj: &Bound<'_, pyo3::PyAny>) -> PyResult<Value> {
    use pyo3::types::{PyBool, PyFloat, PyInt, PyList, PyString};

    if obj.is_none() {
        return Ok(Value::Null);
    }

    if let Ok(b) = obj.downcast::<PyBool>() {
        return Ok(Value::Bool(b.is_true()));
    }

    if let Ok(i) = obj.downcast::<PyInt>() {
        return Ok(Value::Int64(i.extract()?));
    }

    if let Ok(f) = obj.downcast::<PyFloat>() {
        return Ok(Value::Float64(f.extract()?));
    }

    if let Ok(s) = obj.downcast::<PyString>() {
        return Ok(Value::String(s.to_string()));
    }

    if let Ok(list) = obj.downcast::<PyList>() {
        let values: PyResult<Vec<Value>> = list.iter().map(|item| py_to_value(&item)).collect();
        return Ok(Value::Array(values?));
    }

    if let Ok(dict) = obj.downcast::<PyDict>() {
        let map = dict_to_value_map(dict)?;
        return Ok(Value::Map(map));
    }

    // Fallback: convert to string
    Ok(Value::String(obj.str()?.to_string()))
}

// ========== Path Functions ==========

/// Find the shortest path between two node frames.
#[pyfunction]
pub fn shortest_path(
    start: &Bound<'_, PyAny>,
    end: &Bound<'_, PyAny>,
    edge_type: Option<&str>,
    max_hops: Option<i64>,
    direction: Option<&str>,
) -> PyResult<PyNodeFrame> {
    use pyo3::types::PyAnyMethods;

    // Extract PyNodeFrame from either direct PyNodeFrame or FrameCapture wrapper
    let start_frame = if let Ok(frame) = start.extract::<PyNodeFrame>() {
        frame
    } else if let Ok(frame_attr) = start.getattr("_frame") {
        frame_attr.extract::<PyNodeFrame>()?
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "start argument must be a NodeFrame or FrameCapture",
        ));
    };

    let end_frame = if let Ok(frame) = end.extract::<PyNodeFrame>() {
        frame
    } else if let Ok(frame_attr) = end.getattr("_frame") {
        frame_attr.extract::<PyNodeFrame>()?
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "end argument must be a NodeFrame or FrameCapture",
        ));
    };

    // Create a union of the two frames as a placeholder for the path operation
    // In a real implementation, this would be a specialized Path operator
    Ok(PyNodeFrame {
        plan: LogicalOp::Union {
            left: Box::new(start_frame.plan.clone()),
            right: Box::new(end_frame.plan.clone()),
            union: UnionOp::all(),
        },
        label: None,
    })
}

/// Find all paths between two node frames.
#[pyfunction]
pub fn all_paths(
    start: &Bound<'_, PyAny>,
    end: &Bound<'_, PyAny>,
    edge_type: Option<&str>,
    min_hops: Option<i64>,
    max_hops: Option<i64>,
    direction: Option<&str>,
) -> PyResult<PyNodeFrame> {
    use pyo3::types::PyAnyMethods;

    // Extract PyNodeFrame from either direct PyNodeFrame or FrameCapture wrapper
    let start_frame = if let Ok(frame) = start.extract::<PyNodeFrame>() {
        frame
    } else if let Ok(frame_attr) = start.getattr("_frame") {
        frame_attr.extract::<PyNodeFrame>()?
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "start argument must be a NodeFrame or FrameCapture",
        ));
    };

    let end_frame = if let Ok(frame) = end.extract::<PyNodeFrame>() {
        frame
    } else if let Ok(frame_attr) = end.getattr("_frame") {
        frame_attr.extract::<PyNodeFrame>()?
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "end argument must be a NodeFrame or FrameCapture",
        ));
    };

    // Create a union of the two frames as a placeholder for the path operation
    Ok(PyNodeFrame {
        plan: LogicalOp::Union {
            left: Box::new(start_frame.plan.clone()),
            right: Box::new(end_frame.plan.clone()),
            union: UnionOp::all(),
        },
        label: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hypergraph_creation() {
        Python::with_gil(|_py| {
            let hg = PyHypergraph::connect("grism://local", "local", None).unwrap();
            assert_eq!(hg.uri, "grism://local");
            assert!(hg.namespace.is_none());
            assert!(hg.exec_config.is_none());
        });
    }

    #[test]
    fn test_hypergraph_with_namespace() {
        Python::with_gil(|_py| {
            let hg = PyHypergraph::connect("grism://local", "local", None).unwrap();
            let hg_ns = hg.with_namespace("test_ns");
            assert_eq!(hg_ns.namespace, Some("test_ns".to_string()));
        });
    }

    #[test]
    fn test_hypergraph_with_config() {
        Python::with_gil(|_py| {
            let hg = PyHypergraph::connect("grism://local", "local", None).unwrap();
            let hg_cfg = hg.with_config(Some(4), Some(1024 * 1024));
            assert!(hg_cfg.exec_config.is_some());
            let config = hg_cfg.exec_config.unwrap();
            assert_eq!(config.parallelism, Some(4));
            assert_eq!(config.memory_limit, Some(1024 * 1024));
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

    #[test]
    fn test_edge_frame_creation() {
        Python::with_gil(|_py| {
            let hg = PyHypergraph::connect("grism://local", "local", None).unwrap();
            let ef = hg.edges(Some("KNOWS")).unwrap();
            assert_eq!(ef.label, Some("KNOWS".to_string()));
        });
    }

    #[test]
    fn test_hyperedge_frame_creation() {
        Python::with_gil(|_py| {
            let hg = PyHypergraph::connect("grism://local", "local", None).unwrap();
            let hef = hg.hyperedges(Some("Collaboration")).unwrap();
            assert_eq!(hef.label, Some("Collaboration".to_string()));
        });
    }

    #[test]
    fn test_frame_schema() {
        Python::with_gil(|_py| {
            let hg = PyHypergraph::connect("grism://local", "local", None).unwrap();
            let nf = hg.nodes(Some("Person")).unwrap();
            let schema = nf.schema().unwrap();
            assert_eq!(schema.entity_type(), "node");
            assert_eq!(schema.label(), Some("Person".to_string()));
        });
    }

    #[test]
    fn test_transaction_creation() {
        Python::with_gil(|_py| {
            let hg = PyHypergraph::connect("grism://local", "local", None).unwrap();
            let tx = hg.transaction().unwrap();
            assert!(tx.is_active());
            assert_eq!(tx.pending_count(), 0);
        });
    }

    #[test]
    fn test_transaction_operations() {
        Python::with_gil(|py| {
            let hg = PyHypergraph::connect("grism://local", "local", None).unwrap();
            let mut tx = hg.transaction().unwrap();

            // Create a node
            let node_id = tx.create_node("Person", None).unwrap();
            assert!(node_id.starts_with("node_"));
            assert_eq!(tx.pending_count(), 1);

            // Create an edge
            let edge_id = tx.create_edge("KNOWS", "node1", "node2", None).unwrap();
            assert!(edge_id.starts_with("edge_"));
            assert_eq!(tx.pending_count(), 2);

            // Create a hyperedge
            let bindings = PyDict::new_bound(py);
            bindings.set_item("author", "node1").unwrap();
            bindings.set_item("paper", "node2").unwrap();
            let he_id = tx
                .create_hyperedge("Authored", bindings.clone(), None)
                .unwrap();
            assert!(he_id.starts_with("hyperedge_"));
            assert_eq!(tx.pending_count(), 3);

            // Rollback
            tx.rollback().unwrap();
            assert!(!tx.is_active());
            assert_eq!(tx.pending_count(), 0);
        });
    }

    #[test]
    fn test_transaction_commit() {
        Python::with_gil(|_py| {
            let hg = PyHypergraph::connect("grism://local", "local", None).unwrap();
            let mut tx = hg.transaction().unwrap();

            tx.create_node("Person", None).unwrap();
            tx.commit().unwrap();
            assert!(!tx.is_active());
        });
    }

    #[test]
    fn test_value_conversion() {
        // Test value_to_py conversion
        Python::with_gil(|py| {
            // Test null
            let null_obj = value_to_py(py, &Value::Null);
            assert!(null_obj.is_none(py));

            // Test bool
            let bool_obj = value_to_py(py, &Value::Bool(true));
            assert!(bool_obj.extract::<bool>(py).unwrap());

            // Test int
            let int_obj = value_to_py(py, &Value::Int64(42));
            assert_eq!(int_obj.extract::<i64>(py).unwrap(), 42);

            // Test float
            let test_value = 2.234213;
            let float_obj = value_to_py(py, &Value::Float64(test_value));
            assert!((float_obj.extract::<f64>(py).unwrap() - test_value).abs() < 0.001);

            // Test string
            let str_obj = value_to_py(py, &Value::String("hello".to_string()));
            assert_eq!(str_obj.extract::<String>(py).unwrap(), "hello");
        });
    }

    #[test]
    fn test_node_frame_explain() {
        Python::with_gil(|_py| {
            let hg = PyHypergraph::connect("grism://local", "local", None).unwrap();
            let nf = hg.nodes(Some("Person")).unwrap();
            let explain = nf.explain("logical").unwrap();
            assert!(explain.contains("Scan"));
        });
    }

    #[test]
    fn test_node_frame_filter() {
        Python::with_gil(|_py| {
            let hg = PyHypergraph::connect("grism://local", "local", None).unwrap();
            let nf = hg.nodes(Some("Person")).unwrap();

            // Create a filter expression
            let expr = PyExpr::column("age");
            let filtered = nf.filter(&expr).unwrap();

            let explain = filtered.explain("logical").unwrap();
            assert!(explain.contains("Filter"));
        });
    }

    #[test]
    fn test_node_frame_limit() {
        Python::with_gil(|_py| {
            let hg = PyHypergraph::connect("grism://local", "local", None).unwrap();
            let nf = hg.nodes(Some("Person")).unwrap();
            let limited = nf.limit(10).unwrap();

            let explain = limited.explain("logical").unwrap();
            assert!(explain.contains("Limit"));
        });
    }
}
