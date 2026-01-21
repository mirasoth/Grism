//! Python bindings for Grism errors.

use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;

use crate::GrismError;

impl From<GrismError> for PyErr {
    fn from(err: GrismError) -> Self {
        match err {
            GrismError::TypeError(msg) => PyTypeError::new_err(msg),
            GrismError::ValueError(msg) => PyValueError::new_err(msg),
            GrismError::InvalidParameter(msg) => PyValueError::new_err(msg),
            _ => PyRuntimeError::new_err(err.to_string()),
        }
    }
}
