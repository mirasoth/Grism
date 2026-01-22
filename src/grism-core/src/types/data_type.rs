//! Data type definitions for Grism schema.

use serde::{Deserialize, Serialize};

/// Data type for schema columns.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    /// Null type (unknown or absent).
    Null,
    /// Boolean type.
    Bool,
    /// 64-bit signed integer.
    Int64,
    /// 64-bit floating point.
    Float64,
    /// UTF-8 string.
    String,
    /// Binary data.
    Binary,
    /// Vector embedding with specified dimension.
    Vector(usize),
    /// Timestamp with nanosecond precision.
    Timestamp,
    /// Date (days since epoch).
    Date,
    /// Array of elements with specified type.
    Array(Box<Self>),
    /// Map from string keys to values.
    Map(Box<Self>),
    /// Symbol (interned string reference).
    Symbol,
}

impl DataType {
    /// Check if this type is numeric.
    pub const fn is_numeric(&self) -> bool {
        matches!(self, Self::Int64 | Self::Float64)
    }

    /// Check if this type is a string type.
    pub const fn is_string(&self) -> bool {
        matches!(self, Self::String | Self::Symbol)
    }

    /// Check if this type is a temporal type.
    pub const fn is_temporal(&self) -> bool {
        matches!(self, Self::Timestamp | Self::Date)
    }

    /// Check if this type is nullable.
    pub const fn is_nullable(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Get the display name for this type.
    pub fn display_name(&self) -> String {
        match self {
            Self::Null => "Null".to_string(),
            Self::Bool => "Bool".to_string(),
            Self::Int64 => "Int64".to_string(),
            Self::Float64 => "Float64".to_string(),
            Self::String => "String".to_string(),
            Self::Binary => "Binary".to_string(),
            Self::Vector(dim) => format!("Vector({dim})"),
            Self::Timestamp => "Timestamp".to_string(),
            Self::Date => "Date".to_string(),
            Self::Array(inner) => format!("Array<{}>", inner.display_name()),
            Self::Map(value) => format!("Map<String, {}>", value.display_name()),
            Self::Symbol => "Symbol".to_string(),
        }
    }

    /// Check if this type can be coerced to another type.
    pub fn can_coerce_to(&self, target: &Self) -> bool {
        if self == target {
            return true;
        }

        match (self, target) {
            // Null can coerce to anything, Int64 can coerce to Float64, and Symbol can coerce to String
            (Self::Null, _) | (Self::Int64, Self::Float64) | (Self::Symbol, Self::String) => true,
            // Arrays can coerce if inner types can coerce
            (Self::Array(a), Self::Array(b)) => a.can_coerce_to(b),
            _ => false,
        }
    }

    /// Get the common supertype of two types (for type inference).
    pub fn common_supertype(&self, other: &Self) -> Option<Self> {
        if self == other {
            return Some(self.clone());
        }

        match (self, other) {
            (Self::Null, t) | (t, Self::Null) => Some(t.clone()),
            (Self::Int64, Self::Float64) | (Self::Float64, Self::Int64) => Some(Self::Float64),
            (Self::Symbol, Self::String) | (Self::String, Self::Symbol) => Some(Self::String),
            _ => None,
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_coercion() {
        assert!(DataType::Int64.can_coerce_to(&DataType::Float64));
        assert!(DataType::Null.can_coerce_to(&DataType::String));
        assert!(!DataType::String.can_coerce_to(&DataType::Int64));
    }

    #[test]
    fn test_common_supertype() {
        assert_eq!(
            DataType::Int64.common_supertype(&DataType::Float64),
            Some(DataType::Float64)
        );
        assert_eq!(
            DataType::Null.common_supertype(&DataType::String),
            Some(DataType::String)
        );
    }

    #[test]
    fn test_display_name() {
        assert_eq!(DataType::Vector(128).display_name(), "Vector(128)");
        assert_eq!(
            DataType::Array(Box::new(DataType::Int64)).display_name(),
            "Array<Int64>"
        );
    }
}
