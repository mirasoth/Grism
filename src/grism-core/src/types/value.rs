//! Runtime value representation.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Symbol identifier for interned strings.
pub type SymbolId = u64;

/// Runtime value in Grism.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// Null value.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int64(i64),
    /// 64-bit floating point.
    Float64(f64),
    /// UTF-8 string.
    String(String),
    /// Binary data.
    Binary(Vec<u8>),
    /// Vector embedding with fixed dimension.
    Vector(Vec<f32>),
    /// Interned symbol.
    Symbol(SymbolId),
    /// Timestamp (nanoseconds since Unix epoch).
    Timestamp(i64),
    /// Date (days since Unix epoch).
    Date(i32),
    /// Array of values.
    Array(Vec<Value>),
    /// Map of string keys to values.
    Map(HashMap<String, Value>),
}

impl Value {
    /// Check if this value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Try to get as boolean.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to get as i64.
    pub fn as_int64(&self) -> Option<i64> {
        match self {
            Self::Int64(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to get as f64.
    pub fn as_float64(&self) -> Option<f64> {
        match self {
            Self::Float64(f) => Some(*f),
            Self::Int64(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to get as string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as vector reference.
    pub fn as_vector(&self) -> Option<&[f32]> {
        match self {
            Self::Vector(v) => Some(v),
            _ => None,
        }
    }

    /// Get the type name for error messages.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Null => "Null",
            Self::Bool(_) => "Bool",
            Self::Int64(_) => "Int64",
            Self::Float64(_) => "Float64",
            Self::String(_) => "String",
            Self::Binary(_) => "Binary",
            Self::Vector(_) => "Vector",
            Self::Symbol(_) => "Symbol",
            Self::Timestamp(_) => "Timestamp",
            Self::Date(_) => "Date",
            Self::Array(_) => "Array",
            Self::Map(_) => "Map",
        }
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Self::Int64(i)
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Self::Int64(i64::from(i))
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Self::Float64(f)
    }
}

impl From<f32> for Value {
    fn from(f: f32) -> Self {
        Self::Float64(f64::from(f))
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self::String(s.to_string())
    }
}

impl From<Vec<f32>> for Value {
    fn from(v: Vec<f32>) -> Self {
        Self::Vector(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_conversions() {
        assert_eq!(Value::from(42i64).as_int64(), Some(42));
        assert_eq!(Value::from(3.14f64).as_float64(), Some(3.14));
        assert_eq!(Value::from("hello").as_str(), Some("hello"));
        assert!(Value::Null.is_null());
    }

    #[test]
    fn test_value_type_names() {
        assert_eq!(Value::Null.type_name(), "Null");
        assert_eq!(Value::Bool(true).type_name(), "Bool");
        assert_eq!(Value::Int64(42).type_name(), "Int64");
    }
}
