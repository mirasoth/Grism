//! Python bindings for the Grism expression system.
//!
//! This module provides `PyO3` bindings for expressions, following the Daft pattern
//! of individual python modules per crate. Implements expression lowering to Rust
//! `LogicalExpr` per RFC-0003.

#![allow(unused_unsafe)]
#![allow(unsafe_op_in_unsafe_fn)]

use grism_core::Value;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBool, PyFloat, PyInt, PyList, PyString};

use crate::{
    AggExpr as RustAggExpr, LogicalExpr,
    expr::{AggFunc, BinaryOp, FuncExpr, FuncKind, UnaryOp},
};

/// Internal representation of a Python expression that can be lowered to `LogicalExpr`.
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
        left: Box<Self>,
        op: BinaryOp,
        right: Box<Self>,
    },
    /// Unary operation.
    Unary { op: UnaryOp, expr: Box<Self> },
    /// Function call.
    Function { name: String, args: Vec<Self> },
    /// Aggregate expression.
    Aggregate { func: AggFunc, expr: Box<Self> },
    /// Alias.
    Alias { expr: Box<Self>, alias: String },
    /// Wildcard (*).
    Wildcard,
    /// Ascending sort marker.
    SortAsc(Box<Self>),
    /// Descending sort marker.
    SortDesc(Box<Self>),
}

impl ExprKind {
    /// Lower this expression to a Rust `LogicalExpr`.
    pub fn to_logical_expr(&self) -> LogicalExpr {
        match self {
            Self::Column(name) => {
                // Check if it's a qualified reference (contains '.')
                if let Some((qualifier, col_name)) = name.split_once('.') {
                    LogicalExpr::qualified_column(qualifier, col_name)
                } else {
                    LogicalExpr::column(name)
                }
            }
            Self::QualifiedColumn { qualifier, name } => {
                LogicalExpr::qualified_column(qualifier, name)
            }
            Self::Literal(value) => LogicalExpr::Literal(value.clone()),
            Self::Binary { left, op, right } => LogicalExpr::Binary {
                left: Box::new(left.to_logical_expr()),
                op: *op,
                right: Box::new(right.to_logical_expr()),
            },
            Self::Unary { op, expr } => LogicalExpr::Unary {
                op: *op,
                expr: Box::new(expr.to_logical_expr()),
            },
            Self::Function { name, args } => {
                let func_kind = match name.as_str() {
                    "length" | "len" => FuncKind::Builtin(crate::BuiltinFunc::Length),
                    "upper" => FuncKind::Builtin(crate::BuiltinFunc::Upper),
                    "lower" => FuncKind::Builtin(crate::BuiltinFunc::Lower),
                    "trim" => FuncKind::Builtin(crate::BuiltinFunc::Trim),
                    "abs" => FuncKind::Builtin(crate::BuiltinFunc::Abs),
                    "ceil" => FuncKind::Builtin(crate::BuiltinFunc::Ceil),
                    "floor" => FuncKind::Builtin(crate::BuiltinFunc::Floor),
                    "round" => FuncKind::Builtin(crate::BuiltinFunc::Round),
                    "sqrt" => FuncKind::Builtin(crate::BuiltinFunc::Sqrt),
                    "coalesce" => FuncKind::Builtin(crate::BuiltinFunc::Coalesce),
                    "concat" => FuncKind::Builtin(crate::BuiltinFunc::Concat),
                    "substring" => FuncKind::Builtin(crate::BuiltinFunc::Substring),
                    "replace" => FuncKind::Builtin(crate::BuiltinFunc::Replace),
                    "similarity" | "sim" => FuncKind::Builtin(crate::BuiltinFunc::CosineSimilarity),
                    "contains" => FuncKind::Builtin(crate::BuiltinFunc::Contains),
                    "starts_with" => FuncKind::Builtin(crate::BuiltinFunc::StartsWith),
                    "ends_with" => FuncKind::Builtin(crate::BuiltinFunc::EndsWith),
                    "regex_match" | "matches" => FuncKind::Builtin(crate::BuiltinFunc::RegexMatch),
                    "like" => FuncKind::Builtin(crate::BuiltinFunc::Like),
                    "cast" => FuncKind::Builtin(crate::BuiltinFunc::Cast),
                    "id" => FuncKind::Builtin(crate::BuiltinFunc::Id),
                    "labels" => FuncKind::Builtin(crate::BuiltinFunc::Labels),
                    "type" => FuncKind::Builtin(crate::BuiltinFunc::Type),
                    "properties" => FuncKind::Builtin(crate::BuiltinFunc::Properties),
                    "size" => FuncKind::Builtin(crate::BuiltinFunc::Size),
                    _ => FuncKind::UserDefined(name.clone()),
                };
                let func_args: Vec<_> = args.iter().map(Self::to_logical_expr).collect();
                LogicalExpr::Function(FuncExpr {
                    func: func_kind,
                    args: func_args,
                })
            }
            Self::Aggregate { func, expr } => {
                LogicalExpr::Aggregate(RustAggExpr::new(*func, expr.to_logical_expr()))
            }
            Self::Alias { expr, alias } => expr.to_logical_expr().alias(alias),
            Self::Wildcard => LogicalExpr::Wildcard,
            Self::SortAsc(expr) => LogicalExpr::SortKey {
                expr: Box::new(expr.to_logical_expr()),
                ascending: true,
                nulls_first: false,
            },
            Self::SortDesc(expr) => LogicalExpr::SortKey {
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
    pub inner: ExprKind,
}

impl PyExpr {
    /// Create a new `PyExpr` from an `ExprKind`.
    pub const fn new(inner: ExprKind) -> Self {
        Self { inner }
    }

    /// Create a column reference expression.
    pub fn column(name: impl Into<String>) -> Self {
        Self::new(ExprKind::Column(name.into()))
    }

    /// Create a literal expression.
    pub const fn literal(value: Value) -> Self {
        Self::new(ExprKind::Literal(value))
    }

    /// Lower to a Rust `LogicalExpr`.
    pub fn to_logical_expr(&self) -> LogicalExpr {
        self.inner.to_logical_expr()
    }
}

#[pymethods]
#[allow(unsafe_op_in_unsafe_fn)]
impl PyExpr {
    // ========== Comparison Operators ==========

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        unsafe {
            let right = py_any_to_expr(other)?;
            Ok(Self::new(ExprKind::Binary {
                left: Box::new(self.inner.clone()),
                op: BinaryOp::Eq,
                right: Box::new(right),
            }))
        }
    }

    fn __ne__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        unsafe {
            let right = py_any_to_expr(other)?;
            Ok(Self::new(ExprKind::Binary {
                left: Box::new(self.inner.clone()),
                op: BinaryOp::NotEq,
                right: Box::new(right),
            }))
        }
    }

    fn __gt__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        unsafe {
            let right = py_any_to_expr(other)?;
            Ok(Self::new(ExprKind::Binary {
                left: Box::new(self.inner.clone()),
                op: BinaryOp::Gt,
                right: Box::new(right),
            }))
        }
    }

    fn __ge__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        unsafe {
            let right = py_any_to_expr(other)?;
            Ok(Self::new(ExprKind::Binary {
                left: Box::new(self.inner.clone()),
                op: BinaryOp::GtEq,
                right: Box::new(right),
            }))
        }
    }

    fn __lt__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        unsafe {
            let right = py_any_to_expr(other)?;
            Ok(Self::new(ExprKind::Binary {
                left: Box::new(self.inner.clone()),
                op: BinaryOp::Lt,
                right: Box::new(right),
            }))
        }
    }

    fn __le__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        unsafe {
            let right = py_any_to_expr(other)?;
            Ok(Self::new(ExprKind::Binary {
                left: Box::new(self.inner.clone()),
                op: BinaryOp::LtEq,
                right: Box::new(right),
            }))
        }
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
        let mut args = vec![self.inner.clone(), ExprKind::Literal(Value::Int64(start))];
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

    /// Alias for `alias()`.
    fn as_(&self, name: &str) -> Self {
        self.alias(name)
    }

    // ========== Utility Methods ==========

    /// Get all properties as a map.
    fn properties(&self) -> Self {
        Self::new(ExprKind::Function {
            name: "properties".to_string(),
            args: vec![self.inner.clone()],
        })
    }

    /// Explode an array or list into multiple rows.
    fn explode(&self) -> Self {
        Self::new(ExprKind::Function {
            name: "explode".to_string(),
            args: vec![self.inner.clone()],
        })
    }

    // ========== Display ==========

    fn __repr__(&self) -> String {
        format!("Expr({})", self.to_logical_expr())
    }

    fn __str__(&self) -> String {
        format!("{}", self.to_logical_expr())
    }
}

/// Convert a Python value to an `ExprKind`.
pub fn py_any_to_expr(value: &Bound<'_, PyAny>) -> PyResult<ExprKind> {
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
        let values: PyResult<Vec<Value>> = list.iter().map(|item| py_any_to_value(&item)).collect();
        return Ok(ExprKind::Literal(Value::Array(values?)));
    }

    // Fallback: try to convert to string
    Ok(ExprKind::Literal(Value::String(value.str()?.to_string())))
}

/// Convert a Python value to a Grism Value.
pub fn py_any_to_value(value: &Bound<'_, PyAny>) -> PyResult<Value> {
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
        let values: PyResult<Vec<Value>> = list.iter().map(|item| py_any_to_value(&item)).collect();
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

/// Alias for `col()` - provided for semantic clarity.
#[pyfunction]
pub fn prop(name: &str) -> PyExpr {
    col(name)
}

// ========== Aggregation Functions ==========

/// Aggregation expression wrapper.
#[pyclass(name = "AggExpr")]
#[derive(Clone)]
pub struct PyAggExpr {
    pub func: AggFunc,
    pub expr: ExprKind,
    pub alias: Option<String>,
}

impl PyAggExpr {
    pub const fn new(func: AggFunc, expr: ExprKind) -> Self {
        Self {
            func,
            expr,
            alias: None,
        }
    }

    /// Convert to a Rust `AggExpr`.
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

    /// Alias for `alias()`.
    fn as_(&self, name: &str) -> Self {
        self.alias(name)
    }

    fn __repr__(&self) -> String {
        let alias_str = self
            .alias
            .as_deref()
            .map_or(String::new(), |a| format!(" AS {a}"));
        format!("AggExpr({:?}({:?}){}", self.func, self.expr, alias_str)
    }
}

/// Count aggregation.
#[pyfunction]
#[pyo3(signature = (expr=None))]
pub fn count(expr: Option<&PyExpr>) -> PyAggExpr {
    expr.map_or_else(
        || PyAggExpr::new(AggFunc::Count, ExprKind::Wildcard),
        |e| PyAggExpr::new(AggFunc::Count, e.inner.clone()),
    )
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
#[allow(clippy::needless_pass_by_value)]
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
#[allow(clippy::needless_pass_by_value)]
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
pub fn if_expr(
    condition: &PyExpr,
    then: &Bound<'_, PyAny>,
    else_: &Bound<'_, PyAny>,
) -> PyResult<PyExpr> {
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

// ========== String Functions (standalone) ==========

/// Extract substring (standalone function).
#[pyfunction]
#[pyo3(signature = (expr, start, length=None))]
pub fn substring(expr: &PyExpr, start: i64, length: Option<i64>) -> PyExpr {
    expr.substring(start, length)
}

/// Replace substring occurrences (standalone function).
#[pyfunction]
pub fn replace(expr: &PyExpr, old: &str, new: &str) -> PyExpr {
    expr.replace(old, new)
}

/// Split string into array (standalone function).
#[pyfunction]
pub fn split(expr: &PyExpr, delimiter: &str) -> PyExpr {
    expr.split(delimiter)
}

// ========== Conditional Functions (additional) ==========

/// When expression (alias for if_).
/// Used for conditional expressions: when(condition, `then_value`, `else_value`)
#[pyfunction]
pub fn when(
    condition: &PyExpr,
    then: &Bound<'_, PyAny>,
    else_: &Bound<'_, PyAny>,
) -> PyResult<PyExpr> {
    if_expr(condition, then, else_)
}

// ========== Predicate Functions ==========

/// Check if a subquery/pattern exists.
/// Returns an expression that evaluates to true if the pattern exists.
#[pyfunction]
#[pyo3(name = "exists")]
pub fn exists_fn(expr: &Bound<'_, PyAny>) -> PyResult<PyExpr> {
    let e = py_any_to_expr(expr)?;
    Ok(PyExpr::new(ExprKind::Function {
        name: "exists".to_string(),
        args: vec![e],
    }))
}

/// Check if any element in a collection satisfies a predicate.
#[pyfunction]
#[pyo3(name = "any_")]
#[pyo3(signature = (expr, predicate=None))]
pub fn any_fn(expr: &PyExpr, predicate: Option<&PyExpr>) -> PyExpr {
    let args = predicate.map_or_else(
        || vec![expr.inner.clone()],
        |p| vec![expr.inner.clone(), p.inner.clone()],
    );
    PyExpr::new(ExprKind::Function {
        name: "any".to_string(),
        args,
    })
}

/// Check if all elements in a collection satisfy a predicate.
#[pyfunction]
#[pyo3(name = "all_")]
#[pyo3(signature = (expr, predicate=None))]
pub fn all_fn(expr: &PyExpr, predicate: Option<&PyExpr>) -> PyExpr {
    let args = predicate.map_or_else(
        || vec![expr.inner.clone()],
        |p| vec![expr.inner.clone(), p.inner.clone()],
    );
    PyExpr::new(ExprKind::Function {
        name: "all".to_string(),
        args,
    })
}

// ========== Pattern Class ==========

/// Pattern for graph matching.
///
/// Pattern represents a graph pattern that can be used in queries
/// for complex graph matching operations.
#[pyclass(name = "Pattern")]
#[derive(Clone)]
pub struct PyPattern {
    /// Pattern specification.
    pub spec: String,
    /// Optional start node.
    pub start: Option<String>,
    /// Optional end node.
    pub end: Option<String>,
    /// Edge type filter.
    pub edge_type: Option<String>,
    /// Minimum hops.
    pub min_hops: Option<u32>,
    /// Maximum hops.
    pub max_hops: Option<u32>,
}

#[pymethods]
impl PyPattern {
    /// Create a new pattern.
    #[new]
    #[pyo3(signature = (spec=None))]
    fn new(spec: Option<&str>) -> Self {
        Self {
            spec: spec.unwrap_or("").to_string(),
            start: None,
            end: None,
            edge_type: None,
            min_hops: None,
            max_hops: None,
        }
    }

    /// Set the start node label.
    fn start(&self, label: &str) -> Self {
        Self {
            spec: self.spec.clone(),
            start: Some(label.to_string()),
            end: self.end.clone(),
            edge_type: self.edge_type.clone(),
            min_hops: self.min_hops,
            max_hops: self.max_hops,
        }
    }

    /// Set the end node label.
    fn end(&self, label: &str) -> Self {
        Self {
            spec: self.spec.clone(),
            start: self.start.clone(),
            end: Some(label.to_string()),
            edge_type: self.edge_type.clone(),
            min_hops: self.min_hops,
            max_hops: self.max_hops,
        }
    }

    /// Set the edge type filter.
    fn via(&self, edge_type: &str) -> Self {
        Self {
            spec: self.spec.clone(),
            start: self.start.clone(),
            end: self.end.clone(),
            edge_type: Some(edge_type.to_string()),
            min_hops: self.min_hops,
            max_hops: self.max_hops,
        }
    }

    /// Set the hop range.
    #[pyo3(signature = (min_hops=1, max_hops=None))]
    fn hops(&self, min_hops: u32, max_hops: Option<u32>) -> Self {
        Self {
            spec: self.spec.clone(),
            start: self.start.clone(),
            end: self.end.clone(),
            edge_type: self.edge_type.clone(),
            min_hops: Some(min_hops),
            max_hops: max_hops.or(Some(min_hops)),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Pattern(spec='{}', start={:?}, end={:?}, edge_type={:?}, hops={:?}..{:?})",
            self.spec, self.start, self.end, self.edge_type, self.min_hops, self.max_hops
        )
    }
}

// ========== Module Registration (Daft Pattern) ==========

/// Register all Python bindings from this crate with the parent module.
///
/// Following the Daft pattern, each crate exports a `register_modules` function
/// that registers its Python classes and functions with the parent module.
pub fn register_modules(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    // ========== Expression Classes ==========
    parent.add_class::<PyExpr>()?;
    parent.add_class::<PyAggExpr>()?;
    parent.add_class::<PyPattern>()?;

    // ========== Expression Functions ==========
    // Column references
    parent.add_function(wrap_pyfunction!(col, parent)?)?;
    parent.add_function(wrap_pyfunction!(lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(prop, parent)?)?;

    // ========== Aggregation Functions ==========
    parent.add_function(wrap_pyfunction!(count, parent)?)?;
    parent.add_function(wrap_pyfunction!(count_distinct, parent)?)?;
    parent.add_function(wrap_pyfunction!(sum_agg, parent)?)?;
    parent.add_function(wrap_pyfunction!(avg, parent)?)?;
    parent.add_function(wrap_pyfunction!(min_agg, parent)?)?;
    parent.add_function(wrap_pyfunction!(max_agg, parent)?)?;
    parent.add_function(wrap_pyfunction!(collect, parent)?)?;
    parent.add_function(wrap_pyfunction!(collect_distinct, parent)?)?;
    parent.add_function(wrap_pyfunction!(first, parent)?)?;
    parent.add_function(wrap_pyfunction!(last, parent)?)?;

    // ========== String Functions ==========
    parent.add_function(wrap_pyfunction!(concat, parent)?)?;
    parent.add_function(wrap_pyfunction!(length, parent)?)?;
    parent.add_function(wrap_pyfunction!(lower, parent)?)?;
    parent.add_function(wrap_pyfunction!(upper, parent)?)?;
    parent.add_function(wrap_pyfunction!(trim, parent)?)?;
    parent.add_function(wrap_pyfunction!(contains_fn, parent)?)?;

    // ========== Math Functions ==========
    parent.add_function(wrap_pyfunction!(abs_fn, parent)?)?;
    parent.add_function(wrap_pyfunction!(ceil, parent)?)?;
    parent.add_function(wrap_pyfunction!(floor, parent)?)?;
    parent.add_function(wrap_pyfunction!(round_fn, parent)?)?;
    parent.add_function(wrap_pyfunction!(sqrt, parent)?)?;
    parent.add_function(wrap_pyfunction!(power, parent)?)?;

    // ========== Date/Time Functions ==========
    parent.add_function(wrap_pyfunction!(date, parent)?)?;
    parent.add_function(wrap_pyfunction!(year, parent)?)?;
    parent.add_function(wrap_pyfunction!(month, parent)?)?;
    parent.add_function(wrap_pyfunction!(day, parent)?)?;

    // ========== Conditional Functions ==========
    parent.add_function(wrap_pyfunction!(coalesce, parent)?)?;
    parent.add_function(wrap_pyfunction!(if_expr, parent)?)?;

    // ========== Graph Functions ==========
    parent.add_function(wrap_pyfunction!(labels, parent)?)?;
    parent.add_function(wrap_pyfunction!(type_fn, parent)?)?;
    parent.add_function(wrap_pyfunction!(id_fn, parent)?)?;
    parent.add_function(wrap_pyfunction!(properties, parent)?)?;
    parent.add_function(wrap_pyfunction!(nodes, parent)?)?;
    parent.add_function(wrap_pyfunction!(relationships, parent)?)?;
    parent.add_function(wrap_pyfunction!(path_length, parent)?)?;

    // ========== Vector/AI Functions ==========
    parent.add_function(wrap_pyfunction!(sim, parent)?)?;

    // ========== String Functions (standalone) ==========
    parent.add_function(wrap_pyfunction!(substring, parent)?)?;
    parent.add_function(wrap_pyfunction!(replace, parent)?)?;
    parent.add_function(wrap_pyfunction!(split, parent)?)?;

    // ========== Conditional Functions (additional) ==========
    parent.add_function(wrap_pyfunction!(when, parent)?)?;

    // ========== Predicate Functions ==========
    parent.add_function(wrap_pyfunction!(exists_fn, parent)?)?;
    parent.add_function(wrap_pyfunction!(any_fn, parent)?)?;
    parent.add_function(wrap_pyfunction!(all_fn, parent)?)?;

    Ok(())
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
