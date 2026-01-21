//! Python bindings for the Grism expression system.
//!
//! This module provides PyO3 bindings for expressions, following the Python API spec
//! (specs/2_python_api_v0.1.md) and implementing expression lowering to Rust LogicalExpr
//! per RFC-0003.

use grism_core::Value;
use grism_logical::{
    expr::{AggFunc, BinaryOp, FuncExpr, FuncKind, UnaryOp},
    AggExpr as RustAggExpr, LogicalExpr,
};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};

/// Internal representation of a Python expression that can be lowered to LogicalExpr.
#[derive(Debug, Clone)]
pub enum ExprKind {
    /// Column reference.
    Column(String),
    /// Qualified column reference.
    QualifiedColumn { qualifier: String, name: String },
    /// Literal value.
    Literal(Value),
    /// Binary operation.
    Binary {
        left: Box<ExprKind>,
        op: BinaryOp,
        right: Box<ExprKind>,
    },
    /// Unary operation.
    Unary { op: UnaryOp, expr: Box<ExprKind> },
    /// Function call.
    Function { name: String, args: Vec<ExprKind> },
    /// Aggregate expression.
    Aggregate { func: AggFunc, expr: Box<ExprKind> },
    /// Alias.
    Alias { expr: Box<ExprKind>, alias: String },
    /// Wildcard (*).
    Wildcard,
    /// Ascending sort marker.
    SortAsc(Box<ExprKind>),
    /// Descending sort marker.
    SortDesc(Box<ExprKind>),
}

impl ExprKind {
    /// Lower this expression to a Rust LogicalExpr.
    pub fn to_logical_expr(&self) -> LogicalExpr {
        match self {
            ExprKind::Column(name) => {
                // Check if it's a qualified reference (contains '.')
                if let Some((qualifier, col_name)) = name.split_once('.') {
                    LogicalExpr::qualified_column(qualifier, col_name)
                } else {
                    LogicalExpr::column(name)
                }
            }
            ExprKind::QualifiedColumn { qualifier, name } => {
                LogicalExpr::qualified_column(qualifier, name)
            }
            ExprKind::Literal(value) => LogicalExpr::Literal(value.clone()),
            ExprKind::Binary { left, op, right } => LogicalExpr::Binary {
                left: Box::new(left.to_logical_expr()),
                op: *op,
                right: Box::new(right.to_logical_expr()),
            },
            ExprKind::Unary { op, expr } => LogicalExpr::Unary {
                op: *op,
                expr: Box::new(expr.to_logical_expr()),
            },
            ExprKind::Function { name, args } => {
                let func_kind = match name.as_str() {
                    "length" | "len" => FuncKind::Builtin(grism_logical::BuiltinFunc::Length),
                    "upper" => FuncKind::Builtin(grism_logical::BuiltinFunc::Upper),
                    "lower" => FuncKind::Builtin(grism_logical::BuiltinFunc::Lower),
                    "trim" => FuncKind::Builtin(grism_logical::BuiltinFunc::Trim),
                    "abs" => FuncKind::Builtin(grism_logical::BuiltinFunc::Abs),
                    "ceil" => FuncKind::Builtin(grism_logical::BuiltinFunc::Ceil),
                    "floor" => FuncKind::Builtin(grism_logical::BuiltinFunc::Floor),
                    "round" => FuncKind::Builtin(grism_logical::BuiltinFunc::Round),
                    "sqrt" => FuncKind::Builtin(grism_logical::BuiltinFunc::Sqrt),
                    "coalesce" => FuncKind::Builtin(grism_logical::BuiltinFunc::Coalesce),
                    "concat" => FuncKind::Builtin(grism_logical::BuiltinFunc::Concat),
                    "substring" => FuncKind::Builtin(grism_logical::BuiltinFunc::Substring),
                    "replace" => FuncKind::Builtin(grism_logical::BuiltinFunc::Replace),
                    "similarity" | "sim" => FuncKind::Builtin(grism_logical::BuiltinFunc::CosineSimilarity),
                    "contains" => FuncKind::Builtin(grism_logical::BuiltinFunc::Contains),
                    "starts_with" => FuncKind::Builtin(grism_logical::BuiltinFunc::StartsWith),
                    "ends_with" => FuncKind::Builtin(grism_logical::BuiltinFunc::EndsWith),
                    "regex_match" | "matches" => FuncKind::Builtin(grism_logical::BuiltinFunc::RegexMatch),
                    "like" => FuncKind::Builtin(grism_logical::BuiltinFunc::Like),
                    "cast" => FuncKind::Builtin(grism_logical::BuiltinFunc::Cast),
                    "id" => FuncKind::Builtin(grism_logical::BuiltinFunc::Id),
                    "labels" => FuncKind::Builtin(grism_logical::BuiltinFunc::Labels),
                    "type" => FuncKind::Builtin(grism_logical::BuiltinFunc::Type),
                    "properties" => FuncKind::Builtin(grism_logical::BuiltinFunc::Properties),
                    "size" => FuncKind::Builtin(grism_logical::BuiltinFunc::Size),
                    _ => FuncKind::UserDefined(name.clone()),
                };
                let func_args: Vec<_> = args.iter().map(|a| a.to_logical_expr()).collect();
                LogicalExpr::Function(FuncExpr {
                    func: func_kind,
                    args: func_args,
                })
            }
            ExprKind::Aggregate { func, expr } => {
                LogicalExpr::Aggregate(RustAggExpr::new(*func, expr.to_logical_expr()))
            }
            ExprKind::Alias { expr, alias } => expr.to_logical_expr().alias(alias),
            ExprKind::Wildcard => LogicalExpr::Wildcard,
            ExprKind::SortAsc(expr) => LogicalExpr::SortKey {
                expr: Box::new(expr.to_logical_expr()),
                ascending: true,
                nulls_first: false,
            },
            ExprKind::SortDesc(expr) => LogicalExpr::SortKey {
                expr: Box::new(expr.to_logical_expr()),
                ascending: false,
                nulls_first: false,
            },
        }
    }
}

/// Python wrapper for expressions.
///
/// Expr represents a computation over columns in a frame.
/// Expressions are immutable, composable, and lazily evaluated.
#[pyclass(name = "Expr")]
#[derive(Clone)]
pub struct PyExpr {
    /// Internal expression representation.
    pub(crate) inner: ExprKind,
}

impl PyExpr {
    /// Create a new PyExpr from an ExprKind.
    pub fn new(inner: ExprKind) -> Self {
        Self { inner }
    }

    /// Create a column reference expression.
    pub fn column(name: impl Into<String>) -> Self {
        Self::new(ExprKind::Column(name.into()))
    }

    /// Create a literal expression.
    pub fn literal(value: Value) -> Self {
        Self::new(ExprKind::Literal(value))
    }

    /// Lower to a Rust LogicalExpr.
    pub fn to_logical_expr(&self) -> LogicalExpr {
        self.inner.to_logical_expr()
    }
}

#[pymethods]
impl PyExpr {
    // ========== Comparison Operators ==========

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let right = py_any_to_expr(other)?;
        Ok(Self::new(ExprKind::Binary {
            left: Box::new(self.inner.clone()),
            op: BinaryOp::Eq,
            right: Box::new(right),
        }))
    }

    fn __ne__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let right = py_any_to_expr(other)?;
        Ok(Self::new(ExprKind::Binary {
            left: Box::new(self.inner.clone()),
            op: BinaryOp::NotEq,
            right: Box::new(right),
        }))
    }

    fn __gt__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let right = py_any_to_expr(other)?;
        Ok(Self::new(ExprKind::Binary {
            left: Box::new(self.inner.clone()),
            op: BinaryOp::Gt,
            right: Box::new(right),
        }))
    }

    fn __ge__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let right = py_any_to_expr(other)?;
        Ok(Self::new(ExprKind::Binary {
            left: Box::new(self.inner.clone()),
            op: BinaryOp::GtEq,
            right: Box::new(right),
        }))
    }

    fn __lt__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let right = py_any_to_expr(other)?;
        Ok(Self::new(ExprKind::Binary {
            left: Box::new(self.inner.clone()),
            op: BinaryOp::Lt,
            right: Box::new(right),
        }))
    }

    fn __le__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let right = py_any_to_expr(other)?;
        Ok(Self::new(ExprKind::Binary {
            left: Box::new(self.inner.clone()),
            op: BinaryOp::LtEq,
            right: Box::new(right),
        }))
    }

    // ========== Logical Operators ==========

    fn __and__(&self, other: &Self) -> Self {
        Self::new(ExprKind::Binary {
            left: Box::new(self.inner.clone()),
            op: BinaryOp::And,
            right: Box::new(other.inner.clone()),
        })
    }

    fn __or__(&self, other: &Self) -> Self {
        Self::new(ExprKind::Binary {
            left: Box::new(self.inner.clone()),
            op: BinaryOp::Or,
            right: Box::new(other.inner.clone()),
        })
    }

    fn __invert__(&self) -> Self {
        Self::new(ExprKind::Unary {
            op: UnaryOp::Not,
            expr: Box::new(self.inner.clone()),
        })
    }

    // ========== Arithmetic Operators ==========

    fn __add__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let right = py_any_to_expr(other)?;
        Ok(Self::new(ExprKind::Binary {
            left: Box::new(self.inner.clone()),
            op: BinaryOp::Add,
            right: Box::new(right),
        }))
    }

    fn __radd__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let left = py_any_to_expr(other)?;
        Ok(Self::new(ExprKind::Binary {
            left: Box::new(left),
            op: BinaryOp::Add,
            right: Box::new(self.inner.clone()),
        }))
    }

    fn __sub__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let right = py_any_to_expr(other)?;
        Ok(Self::new(ExprKind::Binary {
            left: Box::new(self.inner.clone()),
            op: BinaryOp::Subtract,
            right: Box::new(right),
        }))
    }

    fn __rsub__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let left = py_any_to_expr(other)?;
        Ok(Self::new(ExprKind::Binary {
            left: Box::new(left),
            op: BinaryOp::Subtract,
            right: Box::new(self.inner.clone()),
        }))
    }

    fn __mul__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let right = py_any_to_expr(other)?;
        Ok(Self::new(ExprKind::Binary {
            left: Box::new(self.inner.clone()),
            op: BinaryOp::Multiply,
            right: Box::new(right),
        }))
    }

    fn __rmul__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let left = py_any_to_expr(other)?;
        Ok(Self::new(ExprKind::Binary {
            left: Box::new(left),
            op: BinaryOp::Multiply,
            right: Box::new(self.inner.clone()),
        }))
    }

    fn __truediv__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let right = py_any_to_expr(other)?;
        Ok(Self::new(ExprKind::Binary {
            left: Box::new(self.inner.clone()),
            op: BinaryOp::Divide,
            right: Box::new(right),
        }))
    }

    fn __mod__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let right = py_any_to_expr(other)?;
        Ok(Self::new(ExprKind::Binary {
            left: Box::new(self.inner.clone()),
            op: BinaryOp::Modulo,
            right: Box::new(right),
        }))
    }

    fn __neg__(&self) -> Self {
        Self::new(ExprKind::Unary {
            op: UnaryOp::Neg,
            expr: Box::new(self.inner.clone()),
        })
    }

    // ========== Null Handling ==========

    /// Check if expression is NULL.
    fn is_null(&self) -> Self {
        Self::new(ExprKind::Unary {
            op: UnaryOp::IsNull,
            expr: Box::new(self.inner.clone()),
        })
    }

    /// Check if expression is not NULL.
    fn is_not_null(&self) -> Self {
        Self::new(ExprKind::Unary {
            op: UnaryOp::IsNotNull,
            expr: Box::new(self.inner.clone()),
        })
    }

    /// Return first non-NULL value.
    #[pyo3(signature = (*values))]
    fn coalesce(&self, values: Vec<Bound<'_, PyAny>>) -> PyResult<Self> {
        let mut args = vec![self.inner.clone()];
        for v in values {
            args.push(py_any_to_expr(&v)?);
        }
        Ok(Self::new(ExprKind::Function {
            name: "coalesce".to_string(),
            args,
        }))
    }

    /// Replace NULL with a default value.
    fn fill_null(&self, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let default = py_any_to_expr(value)?;
        Ok(Self::new(ExprKind::Function {
            name: "coalesce".to_string(),
            args: vec![self.inner.clone(), default],
        }))
    }

    // ========== String Operations ==========

    /// Check if string contains substring.
    #[pyo3(signature = (substring, case_sensitive=true))]
    fn contains(&self, substring: &str, case_sensitive: bool) -> Self {
        let _ = case_sensitive; // TODO: Implement case sensitivity
        Self::new(ExprKind::Function {
            name: "contains".to_string(),
            args: vec![
                self.inner.clone(),
                ExprKind::Literal(Value::String(substring.to_string())),
            ],
        })
    }

    /// Check if string starts with prefix.
    fn starts_with(&self, prefix: &str) -> Self {
        Self::new(ExprKind::Function {
            name: "starts_with".to_string(),
            args: vec![
                self.inner.clone(),
                ExprKind::Literal(Value::String(prefix.to_string())),
            ],
        })
    }

    /// Check if string ends with suffix.
    fn ends_with(&self, suffix: &str) -> Self {
        Self::new(ExprKind::Function {
            name: "ends_with".to_string(),
            args: vec![
                self.inner.clone(),
                ExprKind::Literal(Value::String(suffix.to_string())),
            ],
        })
    }

    /// Match against regex pattern.
    fn matches(&self, pattern: &str) -> Self {
        Self::new(ExprKind::Function {
            name: "matches".to_string(),
            args: vec![
                self.inner.clone(),
                ExprKind::Literal(Value::String(pattern.to_string())),
            ],
        })
    }

    /// SQL-style LIKE pattern matching.
    fn like(&self, pattern: &str) -> Self {
        Self::new(ExprKind::Function {
            name: "like".to_string(),
            args: vec![
                self.inner.clone(),
                ExprKind::Literal(Value::String(pattern.to_string())),
            ],
        })
    }

    /// Convert to lowercase.
    fn lower(&self) -> Self {
        Self::new(ExprKind::Function {
            name: "lower".to_string(),
            args: vec![self.inner.clone()],
        })
    }

    /// Convert to uppercase.
    fn upper(&self) -> Self {
        Self::new(ExprKind::Function {
            name: "upper".to_string(),
            args: vec![self.inner.clone()],
        })
    }

    /// Remove leading/trailing whitespace.
    fn trim(&self) -> Self {
        Self::new(ExprKind::Function {
            name: "trim".to_string(),
            args: vec![self.inner.clone()],
        })
    }

    /// Replace substring occurrences.
    fn replace(&self, old: &str, new: &str) -> Self {
        Self::new(ExprKind::Function {
            name: "replace".to_string(),
            args: vec![
                self.inner.clone(),
                ExprKind::Literal(Value::String(old.to_string())),
                ExprKind::Literal(Value::String(new.to_string())),
            ],
        })
    }

    /// Extract substring.
    #[pyo3(signature = (start, length=None))]
    fn substring(&self, start: i64, length: Option<i64>) -> Self {
        let mut args = vec![
            self.inner.clone(),
            ExprKind::Literal(Value::Int64(start)),
        ];
        if let Some(len) = length {
            args.push(ExprKind::Literal(Value::Int64(len)));
        }
        Self::new(ExprKind::Function {
            name: "substring".to_string(),
            args,
        })
    }

    /// Split string into array.
    fn split(&self, delimiter: &str) -> Self {
        Self::new(ExprKind::Function {
            name: "split".to_string(),
            args: vec![
                self.inner.clone(),
                ExprKind::Literal(Value::String(delimiter.to_string())),
            ],
        })
    }

    /// Concatenate strings.
    #[pyo3(signature = (*others))]
    fn concat(&self, others: Vec<Bound<'_, PyAny>>) -> PyResult<Self> {
        let mut args = vec![self.inner.clone()];
        for o in others {
            args.push(py_any_to_expr(&o)?);
        }
        Ok(Self::new(ExprKind::Function {
            name: "concat".to_string(),
            args,
        }))
    }

    /// Get string or array length.
    fn length(&self) -> Self {
        Self::new(ExprKind::Function {
            name: "length".to_string(),
            args: vec![self.inner.clone()],
        })
    }

    // ========== Collection Operations ==========

    /// Get size of array or map.
    fn size(&self) -> Self {
        Self::new(ExprKind::Function {
            name: "length".to_string(),
            args: vec![self.inner.clone()],
        })
    }

    /// Get element by index or key.
    fn get(&self, index: &Bound<'_, PyAny>) -> PyResult<Self> {
        let idx = py_any_to_expr(index)?;
        Ok(Self::new(ExprKind::Function {
            name: "get".to_string(),
            args: vec![self.inner.clone(), idx],
        }))
    }

    /// Check if array contains element.
    fn contains_element(&self, element: &Bound<'_, PyAny>) -> PyResult<Self> {
        let elem = py_any_to_expr(element)?;
        Ok(Self::new(ExprKind::Function {
            name: "array_contains".to_string(),
            args: vec![self.inner.clone(), elem],
        }))
    }

    /// Get keys of a map.
    fn keys(&self) -> Self {
        Self::new(ExprKind::Function {
            name: "map_keys".to_string(),
            args: vec![self.inner.clone()],
        })
    }

    /// Get values of a map.
    fn values(&self) -> Self {
        Self::new(ExprKind::Function {
            name: "map_values".to_string(),
            args: vec![self.inner.clone()],
        })
    }

    // ========== Type Operations ==========

    /// Cast expression to target type.
    fn cast(&self, target_type: &str) -> Self {
        Self::new(ExprKind::Function {
            name: "cast".to_string(),
            args: vec![
                self.inner.clone(),
                ExprKind::Literal(Value::String(target_type.to_string())),
            ],
        })
    }

    // ========== Ordering ==========

    /// Mark for ascending sort.
    fn asc(&self) -> Self {
        Self::new(ExprKind::SortAsc(Box::new(self.inner.clone())))
    }

    /// Mark for descending sort.
    fn desc(&self) -> Self {
        Self::new(ExprKind::SortDesc(Box::new(self.inner.clone())))
    }

    /// Sort NULLs before other values.
    fn nulls_first(&self) -> Self {
        // For now, just return self - would need to modify the sort key
        self.clone()
    }

    /// Sort NULLs after other values.
    fn nulls_last(&self) -> Self {
        // For now, just return self - would need to modify the sort key
        self.clone()
    }

    // ========== Aliasing ==========

    /// Give the expression an alias.
    fn alias(&self, name: &str) -> Self {
        Self::new(ExprKind::Alias {
            expr: Box::new(self.inner.clone()),
            alias: name.to_string(),
        })
    }

    /// Alias for alias().
    fn as_(&self, name: &str) -> Self {
        self.alias(name)
    }

    // ========== Display ==========

    fn __repr__(&self) -> String {
        format!("Expr({})", self.to_logical_expr())
    }

    fn __str__(&self) -> String {
        format!("{}", self.to_logical_expr())
    }
}

/// Convert a Python value to an ExprKind.
fn py_any_to_expr(value: &Bound<'_, PyAny>) -> PyResult<ExprKind> {
    // Check if it's already a PyExpr
    if let Ok(expr) = value.extract::<PyExpr>() {
        return Ok(expr.inner);
    }

    // Convert Python types to literals
    if value.is_none() {
        return Ok(ExprKind::Literal(Value::Null));
    }

    if let Ok(b) = value.downcast::<PyBool>() {
        return Ok(ExprKind::Literal(Value::Bool(b.is_true())));
    }

    if let Ok(i) = value.downcast::<PyInt>() {
        return Ok(ExprKind::Literal(Value::Int64(i.extract()?)));
    }

    if let Ok(f) = value.downcast::<PyFloat>() {
        return Ok(ExprKind::Literal(Value::Float64(f.extract()?)));
    }

    if let Ok(s) = value.downcast::<PyString>() {
        return Ok(ExprKind::Literal(Value::String(s.to_string())));
    }

    if let Ok(list) = value.downcast::<PyList>() {
        let values: PyResult<Vec<Value>> = list
            .iter()
            .map(|item| py_any_to_value(&item))
            .collect();
        return Ok(ExprKind::Literal(Value::Array(values?)));
    }

    // Fallback: try to convert to string
    Ok(ExprKind::Literal(Value::String(value.str()?.to_string())))
}

/// Convert a Python value to a Grism Value.
fn py_any_to_value(value: &Bound<'_, PyAny>) -> PyResult<Value> {
    if value.is_none() {
        return Ok(Value::Null);
    }

    if let Ok(b) = value.downcast::<PyBool>() {
        return Ok(Value::Bool(b.is_true()));
    }

    if let Ok(i) = value.downcast::<PyInt>() {
        return Ok(Value::Int64(i.extract()?));
    }

    if let Ok(f) = value.downcast::<PyFloat>() {
        return Ok(Value::Float64(f.extract()?));
    }

    if let Ok(s) = value.downcast::<PyString>() {
        return Ok(Value::String(s.to_string()));
    }

    if let Ok(list) = value.downcast::<PyList>() {
        let values: PyResult<Vec<Value>> = list
            .iter()
            .map(|item| py_any_to_value(&item))
            .collect();
        return Ok(Value::Array(values?));
    }

    // Fallback
    Ok(Value::String(value.str()?.to_string()))
}

// ========== Expression Construction Functions ==========

/// Create a column reference expression.
#[pyfunction]
pub fn col(name: &str) -> PyExpr {
    PyExpr::column(name)
}

/// Create a literal value expression.
#[pyfunction]
pub fn lit(value: &Bound<'_, PyAny>) -> PyResult<PyExpr> {
    let v = py_any_to_value(value)?;
    Ok(PyExpr::literal(v))
}

/// Alias for col() - provided for semantic clarity.
#[pyfunction]
pub fn prop(name: &str) -> PyExpr {
    col(name)
}

// ========== Aggregation Functions ==========

/// Aggregation expression wrapper.
#[pyclass(name = "AggExpr")]
#[derive(Clone)]
pub struct PyAggExpr {
    pub(crate) func: AggFunc,
    pub(crate) expr: ExprKind,
    pub(crate) alias: Option<String>,
}

impl PyAggExpr {
    fn new(func: AggFunc, expr: ExprKind) -> Self {
        Self {
            func,
            expr,
            alias: None,
        }
    }

    /// Convert to a Rust AggExpr.
    pub fn to_rust_agg_expr(&self) -> RustAggExpr {
        let mut agg = RustAggExpr::new(self.func, self.expr.to_logical_expr());
        if let Some(ref alias) = self.alias {
            agg = agg.with_alias(alias);
        }
        agg
    }
}

#[pymethods]
impl PyAggExpr {
    /// Give the aggregation an alias.
    fn alias(&self, name: &str) -> Self {
        let mut new = self.clone();
        new.alias = Some(name.to_string());
        new
    }

    /// Alias for alias().
    fn as_(&self, name: &str) -> Self {
        self.alias(name)
    }

    fn __repr__(&self) -> String {
        let alias_str = self.alias.as_deref().map_or(String::new(), |a| format!(" AS {}", a));
        format!("AggExpr({:?}({:?}){}", self.func, self.expr, alias_str)
    }
}

/// Count aggregation.
#[pyfunction]
#[pyo3(signature = (expr=None))]
pub fn count(expr: Option<&PyExpr>) -> PyAggExpr {
    match expr {
        Some(e) => PyAggExpr::new(AggFunc::Count, e.inner.clone()),
        None => PyAggExpr::new(AggFunc::Count, ExprKind::Wildcard),
    }
}

/// Count distinct aggregation.
#[pyfunction]
pub fn count_distinct(expr: &PyExpr) -> PyAggExpr {
    PyAggExpr::new(AggFunc::CountDistinct, expr.inner.clone())
}

/// Sum aggregation.
#[pyfunction]
#[pyo3(name = "sum_")]
pub fn sum_agg(expr: &PyExpr) -> PyAggExpr {
    PyAggExpr::new(AggFunc::Sum, expr.inner.clone())
}

/// Average aggregation.
#[pyfunction]
pub fn avg(expr: &PyExpr) -> PyAggExpr {
    PyAggExpr::new(AggFunc::Avg, expr.inner.clone())
}

/// Minimum aggregation.
#[pyfunction]
#[pyo3(name = "min_")]
pub fn min_agg(expr: &PyExpr) -> PyAggExpr {
    PyAggExpr::new(AggFunc::Min, expr.inner.clone())
}

/// Maximum aggregation.
#[pyfunction]
#[pyo3(name = "max_")]
pub fn max_agg(expr: &PyExpr) -> PyAggExpr {
    PyAggExpr::new(AggFunc::Max, expr.inner.clone())
}

/// Collect values into an array.
#[pyfunction]
pub fn collect(expr: &PyExpr) -> PyAggExpr {
    PyAggExpr::new(AggFunc::Collect, expr.inner.clone())
}

/// Collect distinct values into an array.
#[pyfunction]
pub fn collect_distinct(expr: &PyExpr) -> PyAggExpr {
    PyAggExpr::new(AggFunc::CollectDistinct, expr.inner.clone())
}

/// First value in group.
#[pyfunction]
pub fn first(expr: &PyExpr) -> PyAggExpr {
    PyAggExpr::new(AggFunc::First, expr.inner.clone())
}

/// Last value in group.
#[pyfunction]
pub fn last(expr: &PyExpr) -> PyAggExpr {
    PyAggExpr::new(AggFunc::Last, expr.inner.clone())
}

// ========== Utility Functions ==========

/// Similarity function for vectors.
#[pyfunction]
pub fn sim(left: &PyExpr, right: &Bound<'_, PyAny>) -> PyResult<PyExpr> {
    let right_expr = py_any_to_expr(right)?;
    Ok(PyExpr::new(ExprKind::Function {
        name: "similarity".to_string(),
        args: vec![left.inner.clone(), right_expr],
    }))
}

/// Contains function for strings.
#[pyfunction]
#[pyo3(name = "contains")]
pub fn contains_fn(expr: &PyExpr, substring: &str) -> PyExpr {
    expr.contains(substring, true)
}

/// Concatenate strings.
#[pyfunction]
#[pyo3(signature = (*exprs))]
pub fn concat(exprs: Vec<Bound<'_, PyAny>>) -> PyResult<PyExpr> {
    let args: PyResult<Vec<ExprKind>> = exprs.iter().map(|e| py_any_to_expr(e)).collect();
    Ok(PyExpr::new(ExprKind::Function {
        name: "concat".to_string(),
        args: args?,
    }))
}

/// Get string/array length.
#[pyfunction]
pub fn length(expr: &PyExpr) -> PyExpr {
    expr.length()
}

/// Convert to lowercase.
#[pyfunction]
pub fn lower(expr: &PyExpr) -> PyExpr {
    expr.lower()
}

/// Convert to uppercase.
#[pyfunction]
pub fn upper(expr: &PyExpr) -> PyExpr {
    expr.upper()
}

/// Trim whitespace.
#[pyfunction]
pub fn trim(expr: &PyExpr) -> PyExpr {
    expr.trim()
}

// ========== Math Functions ==========

/// Absolute value.
#[pyfunction]
#[pyo3(name = "abs_")]
pub fn abs_fn(expr: &PyExpr) -> PyExpr {
    PyExpr::new(ExprKind::Function {
        name: "abs".to_string(),
        args: vec![expr.inner.clone()],
    })
}

/// Ceiling function.
#[pyfunction]
pub fn ceil(expr: &PyExpr) -> PyExpr {
    PyExpr::new(ExprKind::Function {
        name: "ceil".to_string(),
        args: vec![expr.inner.clone()],
    })
}

/// Floor function.
#[pyfunction]
pub fn floor(expr: &PyExpr) -> PyExpr {
    PyExpr::new(ExprKind::Function {
        name: "floor".to_string(),
        args: vec![expr.inner.clone()],
    })
}

/// Round function.
#[pyfunction]
#[pyo3(signature = (expr, decimals=0))]
#[pyo3(name = "round_")]
pub fn round_fn(expr: &PyExpr, decimals: i64) -> PyExpr {
    PyExpr::new(ExprKind::Function {
        name: "round".to_string(),
        args: vec![
            expr.inner.clone(),
            ExprKind::Literal(Value::Int64(decimals)),
        ],
    })
}

/// Square root.
#[pyfunction]
pub fn sqrt(expr: &PyExpr) -> PyExpr {
    PyExpr::new(ExprKind::Function {
        name: "sqrt".to_string(),
        args: vec![expr.inner.clone()],
    })
}

/// Power function.
#[pyfunction]
pub fn power(base: &PyExpr, exponent: &Bound<'_, PyAny>) -> PyResult<PyExpr> {
    let exp = py_any_to_expr(exponent)?;
    Ok(PyExpr::new(ExprKind::Function {
        name: "power".to_string(),
        args: vec![base.inner.clone(), exp],
    }))
}

// ========== Date/Time Functions ==========

/// Parse or convert to date.
#[pyfunction]
pub fn date(expr: &Bound<'_, PyAny>) -> PyResult<PyExpr> {
    let e = py_any_to_expr(expr)?;
    Ok(PyExpr::new(ExprKind::Function {
        name: "date".to_string(),
        args: vec![e],
    }))
}

/// Extract year from date.
#[pyfunction]
pub fn year(expr: &PyExpr) -> PyExpr {
    PyExpr::new(ExprKind::Function {
        name: "year".to_string(),
        args: vec![expr.inner.clone()],
    })
}

/// Extract month from date.
#[pyfunction]
pub fn month(expr: &PyExpr) -> PyExpr {
    PyExpr::new(ExprKind::Function {
        name: "month".to_string(),
        args: vec![expr.inner.clone()],
    })
}

/// Extract day from date.
#[pyfunction]
pub fn day(expr: &PyExpr) -> PyExpr {
    PyExpr::new(ExprKind::Function {
        name: "day".to_string(),
        args: vec![expr.inner.clone()],
    })
}

// ========== Conditional Functions ==========

/// Return first non-NULL value.
#[pyfunction]
#[pyo3(signature = (*exprs))]
pub fn coalesce(exprs: Vec<Bound<'_, PyAny>>) -> PyResult<PyExpr> {
    let args: PyResult<Vec<ExprKind>> = exprs.iter().map(|e| py_any_to_expr(e)).collect();
    Ok(PyExpr::new(ExprKind::Function {
        name: "coalesce".to_string(),
        args: args?,
    }))
}

/// Simple if-then-else expression.
#[pyfunction]
#[pyo3(name = "if_")]
pub fn if_expr(condition: &PyExpr, then: &Bound<'_, PyAny>, else_: &Bound<'_, PyAny>) -> PyResult<PyExpr> {
    let then_expr = py_any_to_expr(then)?;
    let else_expr = py_any_to_expr(else_)?;
    Ok(PyExpr::new(ExprKind::Function {
        name: "if".to_string(),
        args: vec![condition.inner.clone(), then_expr, else_expr],
    }))
}

// ========== Graph-Specific Functions ==========

/// Get labels of a node.
#[pyfunction]
pub fn labels(expr: &PyExpr) -> PyExpr {
    PyExpr::new(ExprKind::Function {
        name: "labels".to_string(),
        args: vec![expr.inner.clone()],
    })
}

/// Get type of an edge/hyperedge.
#[pyfunction]
#[pyo3(name = "type_")]
pub fn type_fn(expr: &PyExpr) -> PyExpr {
    PyExpr::new(ExprKind::Function {
        name: "type".to_string(),
        args: vec![expr.inner.clone()],
    })
}

/// Get ID of a node or edge.
#[pyfunction]
#[pyo3(name = "id_")]
pub fn id_fn(expr: &PyExpr) -> PyExpr {
    PyExpr::new(ExprKind::Function {
        name: "id".to_string(),
        args: vec![expr.inner.clone()],
    })
}

/// Get all properties as a map.
#[pyfunction]
pub fn properties(expr: &PyExpr) -> PyExpr {
    PyExpr::new(ExprKind::Function {
        name: "properties".to_string(),
        args: vec![expr.inner.clone()],
    })
}

/// Get nodes from a path.
#[pyfunction]
pub fn nodes(expr: &PyExpr) -> PyExpr {
    PyExpr::new(ExprKind::Function {
        name: "nodes".to_string(),
        args: vec![expr.inner.clone()],
    })
}

/// Get relationships from a path.
#[pyfunction]
pub fn relationships(expr: &PyExpr) -> PyExpr {
    PyExpr::new(ExprKind::Function {
        name: "relationships".to_string(),
        args: vec![expr.inner.clone()],
    })
}

/// Get length of a path.
#[pyfunction]
pub fn path_length(expr: &PyExpr) -> PyExpr {
    PyExpr::new(ExprKind::Function {
        name: "path_length".to_string(),
        args: vec![expr.inner.clone()],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_expr() {
        let expr = PyExpr::column("name");
        let logical = expr.to_logical_expr();
        assert!(matches!(logical, LogicalExpr::Column(_)));
    }

    #[test]
    fn test_qualified_column() {
        let expr = PyExpr::column("Person.name");
        let logical = expr.to_logical_expr();
        assert!(matches!(logical, LogicalExpr::QualifiedColumn { .. }));
    }

    #[test]
    fn test_literal_expr() {
        let expr = PyExpr::literal(Value::Int64(42));
        let logical = expr.to_logical_expr();
        assert!(matches!(logical, LogicalExpr::Literal(_)));
    }

    #[test]
    fn test_binary_expr() {
        let left = PyExpr::column("a");
        let right = PyExpr::literal(Value::Int64(10));
        let expr = PyExpr::new(ExprKind::Binary {
            left: Box::new(left.inner),
            op: BinaryOp::Gt,
            right: Box::new(right.inner),
        });
        let logical = expr.to_logical_expr();
        assert!(matches!(logical, LogicalExpr::Binary { .. }));
    }

    #[test]
    fn test_aggregate_expr() {
        let col_expr = PyExpr::column("amount");
        let agg = PyAggExpr::new(AggFunc::Sum, col_expr.inner);
        let rust_agg = agg.to_rust_agg_expr();
        assert_eq!(rust_agg.func, AggFunc::Sum);
    }
}
