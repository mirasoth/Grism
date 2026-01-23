//! Type invariant checking.

use common_error::{GrismError, GrismResult};

use super::{DataType, Value};

/// Check that a value conforms to the expected data type.
#[allow(clippy::match_same_arms)]
#[allow(clippy::unnested_or_patterns)]
pub fn check_type_invariants(value: &Value, expected: &DataType) -> GrismResult<()> {
    match (value, expected) {
        // Null matches, direct type matches, and coercible types
        (Value::Null, _) | // Null is valid for any type in nullable context
        (Value::Bool(_), DataType::Bool) |
        (Value::Int64(_), DataType::Int64) |
        (Value::Float64(_), DataType::Float64) |
        (Value::String(_), DataType::String) |
        (Value::Binary(_), DataType::Binary) |
        (Value::Symbol(_), DataType::Symbol) |
        (Value::Timestamp(_), DataType::Timestamp) |
        (Value::Date(_), DataType::Date) |
        // Coercible types
        (Value::Int64(_), DataType::Float64) | // Int can coerce to Float
        (Value::Symbol(_), DataType::String) => Ok(()), // Symbol can coerce to String

        // Vector dimension check
        (Value::Vector(v), DataType::Vector(dim)) => check_vector_invariants(v.len(), *dim),

        // Array element type check
        (Value::Array(arr), DataType::Array(inner_type)) => {
            for (i, elem) in arr.iter().enumerate() {
                check_type_invariants(elem, inner_type)
                    .map_err(|e| GrismError::type_error(format!("Array element {i}: {e}")))?;
            }
            Ok(())
        }

        // Map value type check
        (Value::Map(map), DataType::Map(value_type)) => {
            for (key, val) in map {
                check_type_invariants(val, value_type)
                    .map_err(|e| GrismError::type_error(format!("Map key '{key}': {e}")))?;
            }
            Ok(())
        }

        // Type mismatch
        (val, ty) => Err(GrismError::type_error(format!(
            "Expected {}, got {}",
            ty.display_name(),
            val.type_name()
        ))),
    }
}

/// Check vector dimension invariants.
pub fn check_vector_invariants(actual_dim: usize, expected_dim: usize) -> GrismResult<()> {
    if actual_dim != expected_dim {
        return Err(GrismError::oos(format!(
            "Vector dimension mismatch: expected {expected_dim}, got {actual_dim}"
        )));
    }
    Ok(())
}

/// Check that a data type is valid for aggregation.
#[allow(dead_code)]
pub fn check_aggregation_type(data_type: &DataType, agg_name: &str) -> GrismResult<()> {
    match agg_name {
        "sum" | "avg" => {
            if !data_type.is_numeric() {
                return Err(GrismError::type_error(format!(
                    "{} requires numeric type, got {}",
                    agg_name,
                    data_type.display_name()
                )));
            }
        }
        "min" | "max" => {
            // min/max work on comparable types
            match data_type {
                DataType::Int64
                | DataType::Float64
                | DataType::String
                | DataType::Timestamp
                | DataType::Date => {}
                _ => {
                    return Err(GrismError::type_error(format!(
                        "{} requires comparable type, got {}",
                        agg_name,
                        data_type.display_name()
                    )));
                }
            }
        }
        "count" | "collect" => {
            // count and collect work on any type
        }
        _ => {
            return Err(GrismError::not_implemented(format!(
                "Unknown aggregation: {agg_name}"
            )));
        }
    }
    Ok(())
}

/// Check binary operation type compatibility.
#[allow(dead_code)]
pub fn check_binary_op_types(left: &DataType, right: &DataType, op: &str) -> GrismResult<DataType> {
    match op {
        // Comparison operators return Bool
        "==" | "!=" | ">" | ">=" | "<" | "<=" => {
            if left.common_supertype(right).is_some() {
                Ok(DataType::Bool)
            } else {
                Err(GrismError::type_error(format!(
                    "Cannot compare {} and {}",
                    left.display_name(),
                    right.display_name()
                )))
            }
        }

        // Logical operators require Bool
        "&&" | "||" => {
            if left == &DataType::Bool && right == &DataType::Bool {
                Ok(DataType::Bool)
            } else {
                Err(GrismError::type_error(format!(
                    "Logical {} requires Bool operands, got {} and {}",
                    op,
                    left.display_name(),
                    right.display_name()
                )))
            }
        }

        // Arithmetic operators
        "+" | "-" | "*" | "/" | "%" => left.common_supertype(right).map_or_else(
            || {
                Err(GrismError::type_error(format!(
                    "Cannot perform {op} on {} and {}",
                    left.display_name(),
                    right.display_name()
                )))
            },
            |result| {
                if result.is_numeric() {
                    Ok(result)
                } else {
                    Err(GrismError::type_error(format!(
                        "Arithmetic {op} requires numeric operands, got {} and {}",
                        left.display_name(),
                        right.display_name()
                    )))
                }
            },
        ),

        _ => Err(GrismError::not_implemented(format!(
            "Unknown binary operator: {op}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_invariants() {
        assert!(check_type_invariants(&Value::Int64(42), &DataType::Int64).is_ok());
        assert!(check_type_invariants(&Value::Int64(42), &DataType::Float64).is_ok());
        assert!(check_type_invariants(&Value::Int64(42), &DataType::String).is_err());
    }

    #[test]
    fn test_vector_invariants() {
        assert!(check_vector_invariants(128, 128).is_ok());
        assert!(check_vector_invariants(64, 128).is_err());
    }

    #[test]
    fn test_binary_op_types() {
        assert_eq!(
            check_binary_op_types(&DataType::Int64, &DataType::Float64, "+").unwrap(),
            DataType::Float64
        );
        assert!(check_binary_op_types(&DataType::String, &DataType::Int64, "+").is_err());
    }
}
