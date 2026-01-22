//! Runtime value representation.

#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::needless_collect)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, ListArray, StringArray,
    StructArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType as ArrowDataType, Field, Fields};
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
    Array(Vec<Self>),
    /// Map of string keys to values.
    Map(HashMap<String, Self>),
}

impl Value {
    /// Check if this value is null.
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Try to get as boolean.
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to get as i64.
    pub const fn as_int64(&self) -> Option<i64> {
        match self {
            Self::Int64(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to get as f64.
    pub const fn as_float64(&self) -> Option<f64> {
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
    pub const fn type_name(&self) -> &'static str {
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

    /// Convert this value to an Arrow scalar value.
    #[must_use]
    pub fn to_arrow_scalar(&self) -> Option<ArrayRef> {
        match self {
            Self::Null => None,
            Self::Bool(b) => Some(Arc::new(BooleanArray::from(vec![*b]))),
            Self::Int64(i) => Some(Arc::new(Int64Array::from(vec![*i]))),
            Self::Float64(f) => Some(Arc::new(Float64Array::from(vec![*f]))),
            Self::String(s) => Some(Arc::new(StringArray::from(vec![s.as_str()]))),
            Self::Binary(data) => Some(Arc::new(BinaryArray::from(vec![data.as_slice()]))),
            Self::Vector(v) => {
                // Convert vector to a list of floats
                let float_array =
                    Float64Array::from(v.iter().map(|&x| f64::from(x)).collect::<Vec<_>>());
                let field = Arc::new(Field::new("item", ArrowDataType::Float64, false));
                let offsets = vec![0, v.len() as i32];
                Some(Arc::new(ListArray::new(
                    field,
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    Arc::new(float_array),
                    None,
                )))
            }
            Self::Timestamp(ts) => {
                // Timestamp as int64 nanoseconds since epoch
                let field = Field::new(
                    "timestamp",
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                    false,
                );
                Some(Arc::new(
                    arrow_array::TimestampNanosecondArray::from(vec![*ts])
                        .with_data_type(field.data_type().clone()),
                ))
            }
            Self::Date(days) => {
                // Date as int32 days since epoch
                let field = Field::new("date", ArrowDataType::Date32, false);
                Some(Arc::new(
                    arrow_array::Date32Array::from(vec![*days])
                        .with_data_type(field.data_type().clone()),
                ))
            }
            Self::Array(values) => {
                // Convert array to Arrow list
                if values.is_empty() {
                    let field = Arc::new(Field::new("item", ArrowDataType::Null, true));
                    let offsets = vec![0, 0];
                    return Some(Arc::new(ListArray::new(
                        field,
                        OffsetBuffer::new(ScalarBuffer::from(offsets)),
                        Arc::new(arrow_array::NullArray::new(0)),
                        None,
                    )));
                }

                // For simplicity, convert all values to their string representation
                let string_values: Vec<String> = values.iter().map(|v| format!("{v:?}")).collect();
                let string_array = StringArray::from(string_values);
                let field = Arc::new(Field::new("item", ArrowDataType::Utf8, false));
                let offsets: Vec<i32> = std::iter::once(0)
                    .chain(values.iter().scan(0, |acc, _| {
                        *acc += 1;
                        Some(*acc)
                    }))
                    .collect();
                Some(Arc::new(ListArray::new(
                    field,
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    Arc::new(string_array),
                    None,
                )))
            }
            Self::Map(map) => {
                // Convert map to Arrow struct - simplified implementation
                if map.is_empty() {
                    return Some(Arc::new(StructArray::from(vec![])));
                }

                let mut struct_data: Vec<(Arc<Field>, ArrayRef)> = Vec::new();
                for (key, value) in map {
                    let field =
                        Arc::new(Field::new(key, value.to_arrow_data_type(), value.is_null()));
                    if let Some(array) = value.to_arrow_scalar() {
                        struct_data.push((field, array));
                    }
                }

                if struct_data.is_empty() {
                    return Some(Arc::new(StructArray::from(vec![])));
                }

                let fields: Vec<Arc<Field>> =
                    struct_data.iter().map(|(field, _)| field.clone()).collect();
                let arrays: Vec<ArrayRef> =
                    struct_data.iter().map(|(_, array)| array.clone()).collect();

                // Create struct array from fields and arrays
                let struct_fields: Fields = fields.into_iter().collect();
                Some(Arc::new(
                    StructArray::try_new(struct_fields, arrays, None).unwrap(),
                ))
            }
            Self::Symbol(_) => {
                // Symbols as strings
                Some(Arc::new(StringArray::from(vec![format!(
                    "symbol:{:?}",
                    self
                )])))
            }
        }
    }

    /// Get the corresponding Arrow data type for this value.
    pub fn to_arrow_data_type(&self) -> ArrowDataType {
        match self {
            Self::Null => ArrowDataType::Null,
            Self::Bool(_) => ArrowDataType::Boolean,
            Self::Int64(_) => ArrowDataType::Int64,
            Self::Float64(_) => ArrowDataType::Float64,
            Self::String(_) | Self::Symbol(_) => ArrowDataType::Utf8,
            Self::Binary(_) => ArrowDataType::Binary,
            Self::Vector(_) => {
                ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Float64, false)))
            }
            Self::Timestamp(_) => {
                ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
            }
            Self::Date(_) => ArrowDataType::Date32,
            Self::Array(_) => {
                ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8, true)))
            }
            Self::Map(_) => {
                // For maps, we use a struct type with dynamic fields
                // This is a simplified implementation
                ArrowDataType::Struct(Fields::empty())
            }
        }
    }

    /// Create a Value from an Arrow array element.
    ///
    /// # Arguments
    /// * `array` - The Arrow array
    /// * `index` - The index of the element to extract
    ///
    /// # Returns
    /// * `Some(Value)` if the element could be extracted
    /// * `None` if the index is out of bounds or the value is null
    pub fn from_arrow_array(array: &ArrayRef, index: usize) -> Option<Self> {
        if index >= array.len() {
            return None;
        }

        if array.is_null(index) {
            return Some(Self::Null);
        }

        match array.data_type() {
            ArrowDataType::Boolean => {
                let bool_array = array.as_any().downcast_ref::<BooleanArray>()?;
                Some(Self::Bool(bool_array.value(index)))
            }
            ArrowDataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>()?;
                Some(Self::Int64(int_array.value(index)))
            }
            ArrowDataType::Float64 => {
                let float_array = array.as_any().downcast_ref::<Float64Array>()?;
                Some(Self::Float64(float_array.value(index)))
            }
            ArrowDataType::Utf8 => {
                let string_array = array.as_any().downcast_ref::<StringArray>()?;
                Some(Self::String(string_array.value(index).to_string()))
            }
            ArrowDataType::Binary => {
                let binary_array = array.as_any().downcast_ref::<BinaryArray>()?;
                Some(Self::Binary(binary_array.value(index).to_vec()))
            }
            ArrowDataType::List(field_ref) => {
                let list_array = array.as_any().downcast_ref::<ListArray>()?;
                let start = list_array.value_offsets()[index] as usize;
                let end = list_array.value_offsets()[index + 1] as usize;
                let values = list_array.values();

                if field_ref.data_type() == &ArrowDataType::Float64 {
                    let float_array = values.as_any().downcast_ref::<Float64Array>()?;
                    let vector: Vec<f32> =
                        (start..end).map(|i| float_array.value(i) as f32).collect();
                    Some(Self::Vector(vector))
                } else {
                    // For other list types, convert to array of values
                    let mut result = Vec::new();
                    for i in start..end {
                        if let Some(value) = Self::from_arrow_array(values, i) {
                            result.push(value);
                        }
                    }
                    Some(Self::Array(result))
                }
            }
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
                let timestamp_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::TimestampNanosecondArray>()?;
                Some(Self::Timestamp(timestamp_array.value(index)))
            }
            ArrowDataType::Date32 => {
                let date_array = array.as_any().downcast_ref::<arrow_array::Date32Array>()?;
                Some(Self::Date(date_array.value(index)))
            }
            ArrowDataType::Struct(_) => {
                let struct_array = array.as_any().downcast_ref::<StructArray>()?;
                let mut map = HashMap::new();

                // Simplified struct handling - access columns by index
                let columns = struct_array.columns();
                for (i, column) in columns.iter().enumerate() {
                    // Use a generic field name based on index for now
                    let field_name = format!("field_{i}");
                    if let Some(value) = Self::from_arrow_array(column, index) {
                        map.insert(field_name, value);
                    }
                }
                Some(Self::Map(map))
            }
            _ => None,
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
    use arrow_array::Float64Array;
    use std::sync::Arc;

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

    #[test]
    fn test_arrow_scalar_conversion() {
        // Test basic types
        let bool_val = Value::Bool(true);
        let bool_array = bool_val.to_arrow_scalar().unwrap();
        assert_eq!(bool_array.len(), 1);
        assert_eq!(bool_array.data_type(), &ArrowDataType::Boolean);

        let int_val = Value::Int64(42);
        let int_array = int_val.to_arrow_scalar().unwrap();
        assert_eq!(int_array.len(), 1);
        assert_eq!(int_array.data_type(), &ArrowDataType::Int64);

        let float_val = Value::Float64(3.14);
        let float_array = float_val.to_arrow_scalar().unwrap();
        assert_eq!(float_array.len(), 1);
        assert_eq!(float_array.data_type(), &ArrowDataType::Float64);

        let string_val = Value::String("hello".to_string());
        let string_array = string_val.to_arrow_scalar().unwrap();
        assert_eq!(string_array.len(), 1);
        assert_eq!(string_array.data_type(), &ArrowDataType::Utf8);

        // Test vector type
        let vector_val = Value::Vector(vec![1.0, 2.0, 3.0]);
        let vector_array = vector_val.to_arrow_scalar().unwrap();
        assert_eq!(vector_array.len(), 1);
        matches!(vector_array.data_type(), ArrowDataType::List(_));
    }

    #[test]
    fn test_arrow_from_array() {
        // Test boolean array
        let bool_array: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));
        let value = Value::from_arrow_array(&bool_array, 0).unwrap();
        assert_eq!(value, Value::Bool(true));

        // Test int64 array
        let int_array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let value = Value::from_arrow_array(&int_array, 1).unwrap();
        assert_eq!(value, Value::Int64(2));

        // Test float64 array
        let float_array: ArrayRef = Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3]));
        let value = Value::from_arrow_array(&float_array, 2).unwrap();
        assert_eq!(value, Value::Float64(3.3));

        // Test string array
        let string_array: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let value = Value::from_arrow_array(&string_array, 1).unwrap();
        assert_eq!(value, Value::String("b".to_string()));

        // Test null handling
        let null_array: ArrayRef =
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)]));
        let value = Value::from_arrow_array(&null_array, 1).unwrap();
        assert_eq!(value, Value::Null);
    }

    #[test]
    fn test_vector_arrow_roundtrip() {
        let original = Value::Vector(vec![1.0, 2.0, 3.0, 4.0]);

        // Convert to Arrow
        let arrow_array = original.to_arrow_scalar().unwrap();

        // Convert back
        let recovered = Value::from_arrow_array(&arrow_array, 0).unwrap();

        match (original, recovered) {
            (Value::Vector(orig_vec), Value::Vector(rec_vec)) => {
                assert_eq!(orig_vec.len(), rec_vec.len());
                for (i, &orig_val) in orig_vec.iter().enumerate() {
                    assert!((orig_val - rec_vec[i]).abs() < f32::EPSILON);
                }
            }
            _ => panic!("Both values should be vectors"),
        }
    }

    #[test]
    fn test_timestamp_arrow_conversion() {
        let timestamp_ns = 1640995200000000000i64; // 2022-01-01 00:00:00 UTC
        let original = Value::Timestamp(timestamp_ns);

        // Convert to Arrow
        let arrow_array = original.to_arrow_scalar().unwrap();
        assert!(matches!(
            arrow_array.data_type(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
        ));

        // Convert back
        let recovered = Value::from_arrow_array(&arrow_array, 0).unwrap();
        assert_eq!(original, recovered);
    }

    #[test]
    fn test_date_arrow_conversion() {
        let days_since_epoch = 18993; // 2022-01-01
        let original = Value::Date(days_since_epoch);

        // Convert to Arrow
        let arrow_array = original.to_arrow_scalar().unwrap();
        assert_eq!(arrow_array.data_type(), &ArrowDataType::Date32);

        // Convert back
        let recovered = Value::from_arrow_array(&arrow_array, 0).unwrap();
        assert_eq!(original, recovered);
    }

    #[test]
    fn test_map_arrow_conversion() {
        let mut map = HashMap::new();
        map.insert("name".to_string(), Value::String("Alice".to_string()));
        map.insert("age".to_string(), Value::Int64(30));
        map.insert("active".to_string(), Value::Bool(true));

        let original = Value::Map(map);

        // Convert to Arrow
        let arrow_array = original.to_arrow_scalar().unwrap();
        assert!(matches!(arrow_array.data_type(), ArrowDataType::Struct(_)));

        // Convert back (note: struct conversion is simplified in current implementation)
        let recovered = Value::from_arrow_array(&arrow_array, 0).unwrap();

        match recovered {
            Value::Map(recovered_map) => {
                // With simplified struct handling, we get generic field names
                assert!(!recovered_map.is_empty());
                // Should have 3 fields with generic names
                assert_eq!(recovered_map.len(), 3);
                // Check that we have the expected values (order may vary)
                let mut found_values = std::collections::HashSet::new();
                for value in recovered_map.values() {
                    match value {
                        Value::String(s) if s == "Alice" => {
                            found_values.insert("name");
                        }
                        Value::Int64(i) if *i == 30 => {
                            found_values.insert("age");
                        }
                        Value::Bool(b) if *b == true => {
                            found_values.insert("active");
                        }
                        _ => {}
                    };
                }
                assert_eq!(found_values.len(), 3); // All expected values found
            }
            _ => panic!("Recovered value should be a map"),
        }
    }

    #[test]
    fn test_arrow_data_type_mapping() {
        assert_eq!(Value::Null.to_arrow_data_type(), ArrowDataType::Null);
        assert_eq!(
            Value::Bool(true).to_arrow_data_type(),
            ArrowDataType::Boolean
        );
        assert_eq!(Value::Int64(42).to_arrow_data_type(), ArrowDataType::Int64);
        assert_eq!(
            Value::Float64(3.14).to_arrow_data_type(),
            ArrowDataType::Float64
        );
        assert_eq!(
            Value::String("test".to_string()).to_arrow_data_type(),
            ArrowDataType::Utf8
        );

        let vector_type = Value::Vector(vec![1.0, 2.0]).to_arrow_data_type();
        matches!(vector_type, ArrowDataType::List(_));

        let timestamp_type = Value::Timestamp(0).to_arrow_data_type();
        matches!(
            timestamp_type,
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
        );

        assert_eq!(Value::Date(0).to_arrow_data_type(), ArrowDataType::Date32);
    }
}
