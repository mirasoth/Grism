//! Core error types for Grism.

use thiserror::Error;

/// Result type alias using `GrismError`.
pub type GrismResult<T> = std::result::Result<T, GrismError>;

/// Generic boxed error for external error sources.
pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

/// Core error type for Grism operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum GrismError {
    /// Type mismatch or invalid type operation.
    #[error("TypeError: {0}")]
    TypeError(String),

    /// Invalid value provided.
    #[error("ValueError: {0}")]
    ValueError(String),

    /// Schema-related error (column not found, ambiguous reference, etc.).
    #[error("SchemaError: {0}")]
    SchemaError(String),

    /// Column not found in schema.
    #[error("ColumnNotFound: {0}")]
    ColumnNotFound(String),

    /// Ambiguous column reference.
    #[error("AmbiguousColumn: {0}")]
    AmbiguousColumn(String),

    /// Graph structure error.
    #[error("GraphError: {0}")]
    GraphError(String),

    /// Query execution error.
    #[error("ExecutionError: {0}")]
    ExecutionError(String),

    /// Storage layer error.
    #[error("StorageError: {0}")]
    StorageError(String),

    /// Distributed execution error.
    #[error("DistributedError: {0}")]
    DistributedError(String),

    /// Feature not yet implemented.
    #[error("NotImplemented: {0}")]
    NotImplemented(String),

    /// Internal error (bug in Grism).
    #[error("InternalError: {0}")]
    InternalError(String),

    /// IO error.
    #[error("IoError: {0}")]
    IoError(#[from] std::io::Error),

    /// Arrow error.
    #[error("ArrowError: {0}")]
    ArrowError(#[from] arrow_schema::ArrowError),

    /// JSON serialization error.
    #[error("SerdeJsonError: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    /// External error from third-party libraries.
    #[error("ExternalError: {0}")]
    ExternalError(GenericError),

    /// Invalid parameter provided.
    #[error("InvalidParameter: {0}")]
    InvalidParameter(String),

    /// Out of specification error (data doesn't conform to expected format).
    #[error("OutOfSpec: {0}")]
    OutOfSpec(String),

    #[cfg(feature = "python")]
    /// Python interop error.
    #[error("PyO3Error: {0}")]
    PyO3Error(#[from] pyo3::PyErr),
}

impl GrismError {
    /// Create a new `TypeError`.
    pub fn type_error<S: Into<String>>(msg: S) -> Self {
        Self::TypeError(msg.into())
    }

    /// Create a new `ValueError`.
    pub fn value_error<S: Into<String>>(msg: S) -> Self {
        Self::ValueError(msg.into())
    }

    /// Create a new `SchemaError`.
    pub fn schema_error<S: Into<String>>(msg: S) -> Self {
        Self::SchemaError(msg.into())
    }

    /// Create a new `NotImplemented` error.
    pub fn not_implemented<S: Into<String>>(msg: S) -> Self {
        Self::NotImplemented(msg.into())
    }

    /// Create a new `InternalError`.
    pub fn internal<S: Into<String>>(msg: S) -> Self {
        Self::InternalError(msg.into())
    }

    /// Create an out-of-spec error (following Daft's parquet2 pattern).
    pub fn oos<S: Into<String>>(msg: S) -> Self {
        Self::OutOfSpec(msg.into())
    }

    /// Create a new `ExecutionError`.
    pub fn execution<S: Into<String>>(msg: S) -> Self {
        Self::ExecutionError(msg.into())
    }

    /// Create a new `StorageError`.
    pub fn storage<S: Into<String>>(msg: S) -> Self {
        Self::StorageError(msg.into())
    }

    /// Create a new `GraphError`.
    pub fn graph<S: Into<String>>(msg: S) -> Self {
        Self::GraphError(msg.into())
    }

    /// Create a new `InvalidParameter` error.
    pub fn invalid_parameter<S: Into<String>>(msg: S) -> Self {
        Self::InvalidParameter(msg.into())
    }

    /// Create a cancellation error (using `ExecutionError`).
    pub fn cancelled<S: Into<String>>(msg: S) -> Self {
        Self::ExecutionError(format!("Cancelled: {}", msg.into()))
    }

    /// Create a resource exhausted error (using `ExecutionError`).
    pub fn resource_exhausted<S: Into<String>>(msg: S) -> Self {
        Self::ExecutionError(format!("ResourceExhausted: {}", msg.into()))
    }

    /// Create a planning error (using `ExecutionError`).
    pub fn planning<S: Into<String>>(msg: S) -> Self {
        Self::ExecutionError(format!("PlanningError: {}", msg.into()))
    }
}

/// Ensure a condition holds, returning a `ComputeError` if not.
#[macro_export]
macro_rules! ensure {
    ($cond:expr, $msg:expr) => {
        if !$cond {
            return Err($crate::GrismError::ExecutionError($msg.to_string()));
        }
    };
    ($cond:expr, $variant:ident: $($msg:tt)*) => {
        if !$cond {
            return Err($crate::GrismError::$variant(format!($($msg)*)));
        }
    };
}

/// Return early with a `ValueError`.
#[macro_export]
macro_rules! value_err {
    ($($arg:tt)*) => {
        return Err($crate::GrismError::ValueError(format!($($arg)*)))
    };
}

/// Return early with a `TypeError`.
#[macro_export]
macro_rules! type_err {
    ($($arg:tt)*) => {
        return Err($crate::GrismError::TypeError(format!($($arg)*)))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = GrismError::type_error("expected Int64, got String");
        assert_eq!(err.to_string(), "TypeError: expected Int64, got String");
    }

    #[test]
    fn test_error_constructors() {
        let _ = GrismError::value_error("invalid value");
        let _ = GrismError::schema_error("column not found");
        let _ = GrismError::not_implemented("feature X");
        let _ = GrismError::internal("unexpected state");
        let _ = GrismError::oos("invalid parquet format");
    }
}
