//! Python bindings for expressions.

use pyo3::prelude::*;

/// Python wrapper for expressions.
#[pyclass(name = "Expr")]
#[derive(Clone)]
pub struct PyExpr {
    /// Expression representation (placeholder).
    repr: String,
}

impl PyExpr {
    pub(crate) fn new(repr: impl Into<String>) -> Self {
        Self { repr: repr.into() }
    }
}

#[pymethods]
impl PyExpr {
    // Comparison operators

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(format!("({} == {:?})", self.repr, other)))
    }

    fn __ne__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(format!("({} != {:?})", self.repr, other)))
    }

    fn __gt__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(format!("({} > {:?})", self.repr, other)))
    }

    fn __ge__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(format!("({} >= {:?})", self.repr, other)))
    }

    fn __lt__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(format!("({} < {:?})", self.repr, other)))
    }

    fn __le__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(format!("({} <= {:?})", self.repr, other)))
    }

    // Logical operators

    fn __and__(&self, other: &Self) -> Self {
        Self::new(format!("({} & {})", self.repr, other.repr))
    }

    fn __or__(&self, other: &Self) -> Self {
        Self::new(format!("({} | {})", self.repr, other.repr))
    }

    fn __invert__(&self) -> Self {
        Self::new(format!("~{}", self.repr))
    }

    // Arithmetic operators

    fn __add__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(format!("({} + {:?})", self.repr, other)))
    }

    fn __sub__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(format!("({} - {:?})", self.repr, other)))
    }

    fn __mul__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(format!("({} * {:?})", self.repr, other)))
    }

    fn __truediv__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(format!("({} / {:?})", self.repr, other)))
    }

    // Null checks

    fn is_null(&self) -> Self {
        Self::new(format!("{}.is_null()", self.repr))
    }

    fn is_not_null(&self) -> Self {
        Self::new(format!("{}.is_not_null()", self.repr))
    }

    // Coalesce

    #[pyo3(signature = (*values))]
    fn coalesce(&self, values: Vec<&Bound<'_, PyAny>>) -> PyResult<Self> {
        Ok(Self::new(format!(
            "{}.coalesce({:?})",
            self.repr,
            values.len()
        )))
    }

    fn __repr__(&self) -> String {
        format!("Expr({})", self.repr)
    }
}

/// Create a column reference expression.
#[pyfunction]
pub fn col(name: &str) -> PyExpr {
    PyExpr::new(format!("col(\"{}\")", name))
}

/// Create a literal value expression.
#[pyfunction]
pub fn lit(value: &Bound<'_, PyAny>) -> PyResult<PyExpr> {
    Ok(PyExpr::new(format!("lit({:?})", value)))
}

/// Aggregation expression wrapper.
#[pyclass(name = "AggExpr")]
#[derive(Clone)]
pub struct PyAggExpr {
    repr: String,
}

impl PyAggExpr {
    fn new(repr: impl Into<String>) -> Self {
        Self { repr: repr.into() }
    }
}

#[pymethods]
impl PyAggExpr {
    fn __repr__(&self) -> String {
        format!("AggExpr({})", self.repr)
    }
}

/// Count aggregation.
#[pyfunction]
#[pyo3(signature = (expr=None))]
pub fn count(expr: Option<&PyExpr>) -> PyAggExpr {
    match expr {
        Some(e) => PyAggExpr::new(format!("count({})", e.repr)),
        None => PyAggExpr::new("count(*)"),
    }
}

/// Sum aggregation.
#[pyfunction]
pub fn sum(expr: &PyExpr) -> PyAggExpr {
    PyAggExpr::new(format!("sum({})", expr.repr))
}

/// Average aggregation.
#[pyfunction]
pub fn avg(expr: &PyExpr) -> PyAggExpr {
    PyAggExpr::new(format!("avg({})", expr.repr))
}

/// Minimum aggregation.
#[pyfunction]
pub fn min(expr: &PyExpr) -> PyAggExpr {
    PyAggExpr::new(format!("min({})", expr.repr))
}

/// Maximum aggregation.
#[pyfunction]
pub fn max(expr: &PyExpr) -> PyAggExpr {
    PyAggExpr::new(format!("max({})", expr.repr))
}

/// Similarity function for vectors.
#[pyfunction]
pub fn sim(left: &PyExpr, right: &Bound<'_, PyAny>) -> PyResult<PyExpr> {
    Ok(PyExpr::new(format!("sim({}, {:?})", left.repr, right)))
}

/// Contains function for strings.
#[pyfunction]
pub fn contains(expr: &PyExpr, substring: &str) -> PyExpr {
    PyExpr::new(format!("contains({}, \"{}\")", expr.repr, substring))
}
