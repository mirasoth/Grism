//! Schema inference utilities for physical planning.
//!
//! Provides type inference for LogicalExpr using PhysicalSchema (Arrow schema).

use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use grism_core::Value;
use grism_logical::LogicalExpr;
use grism_logical::expr::{AggExpr, AggFunc, BinaryOp, UnaryOp};
use grism_logical::ops::AggregateOp;

use crate::physical::PhysicalSchema;

/// Infer the Arrow DataType of a LogicalExpr given an input schema.
///
/// Returns `None` if the type cannot be inferred (e.g., unknown column).
pub fn infer_expr_type(expr: &LogicalExpr, schema: &PhysicalSchema) -> Option<ArrowDataType> {
    match expr {
        LogicalExpr::Literal(value) => Some(value_to_arrow_type(value)),

        LogicalExpr::Column(name) => schema.field(name).map(|f| f.data_type().clone()),

        LogicalExpr::QualifiedColumn { qualifier, name } => {
            // Try qualified name first, then unqualified
            schema
                .field(&format!("{qualifier}.{name}"))
                .or_else(|| schema.field(name))
                .map(|f| f.data_type().clone())
        }

        LogicalExpr::Binary { left, op, right } => {
            let left_type = infer_expr_type(left, schema)?;
            let right_type = infer_expr_type(right, schema)?;
            infer_binary_result_type(op, &left_type, &right_type)
        }

        LogicalExpr::Unary { op, expr } => {
            let input_type = infer_expr_type(expr, schema)?;
            infer_unary_result_type(op, &input_type)
        }

        LogicalExpr::Aggregate(agg) => infer_aggregate_type(agg, schema),

        LogicalExpr::Alias { expr, .. } => infer_expr_type(expr, schema),

        LogicalExpr::Case {
            when_clauses,
            else_result,
            ..
        } => {
            // Result type is the type of the first THEN clause
            for (_, result) in when_clauses {
                if let Some(t) = infer_expr_type(result, schema) {
                    return Some(t);
                }
            }
            if let Some(else_expr) = else_result {
                return infer_expr_type(else_expr, schema);
            }
            None
        }

        LogicalExpr::InList { .. } | LogicalExpr::Between { .. } => Some(ArrowDataType::Boolean),

        LogicalExpr::Wildcard => None, // Wildcard doesn't have a single type

        LogicalExpr::Function(_) => {
            // Function type inference would need a function registry
            // For now, return None (caller should handle)
            None
        }

        _ => None,
    }
}

/// Infer the result type of a binary operation.
fn infer_binary_result_type(
    op: &BinaryOp,
    left: &ArrowDataType,
    right: &ArrowDataType,
) -> Option<ArrowDataType> {
    match op {
        // Arithmetic operators
        BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide => {
            match (left, right) {
                (ArrowDataType::Int64, ArrowDataType::Int64) => Some(ArrowDataType::Int64),
                (ArrowDataType::Float64, ArrowDataType::Float64) => Some(ArrowDataType::Float64),
                (ArrowDataType::Int64, ArrowDataType::Float64)
                | (ArrowDataType::Float64, ArrowDataType::Int64) => Some(ArrowDataType::Float64),
                _ => None,
            }
        }

        BinaryOp::Modulo => Some(ArrowDataType::Int64),

        // Comparison operators always return Bool
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq
        | BinaryOp::IsDistinctFrom
        | BinaryOp::IsNotDistinctFrom => Some(ArrowDataType::Boolean),

        // Logical operators return Bool
        BinaryOp::And | BinaryOp::Or => Some(ArrowDataType::Boolean),

        // String concatenation
        BinaryOp::Concat => Some(ArrowDataType::Utf8),
    }
}

/// Infer the result type of a unary operation.
fn infer_unary_result_type(op: &UnaryOp, input: &ArrowDataType) -> Option<ArrowDataType> {
    match op {
        UnaryOp::Not => Some(ArrowDataType::Boolean),
        UnaryOp::IsNull | UnaryOp::IsNotNull => Some(ArrowDataType::Boolean),
        UnaryOp::IsTrue | UnaryOp::IsFalse | UnaryOp::IsUnknown => Some(ArrowDataType::Boolean),
        UnaryOp::Neg => Some(input.clone()),
    }
}

/// Infer the result type of an aggregate function.
fn infer_aggregate_type(agg: &AggExpr, schema: &PhysicalSchema) -> Option<ArrowDataType> {
    // Special case for COUNT(*) - input is Wildcard
    if matches!(agg.func, AggFunc::Count | AggFunc::CountDistinct) {
        return Some(ArrowDataType::Int64);
    }

    let input_type = infer_expr_type(&agg.expr, schema)?;

    match agg.func {
        AggFunc::Count | AggFunc::CountDistinct => Some(ArrowDataType::Int64),

        AggFunc::Sum => match input_type {
            ArrowDataType::Int64 => Some(ArrowDataType::Int64),
            ArrowDataType::Float64 => Some(ArrowDataType::Float64),
            _ => None,
        },

        AggFunc::Min | AggFunc::Max => Some(input_type),

        AggFunc::Avg => Some(ArrowDataType::Float64),

        AggFunc::First | AggFunc::Last => Some(input_type),

        AggFunc::Collect | AggFunc::CollectDistinct => Some(ArrowDataType::List(Arc::new(
            Field::new("item", input_type, true),
        ))),
    }
}

/// Convert a Grism Value to Arrow DataType.
fn value_to_arrow_type(value: &Value) -> ArrowDataType {
    match value {
        Value::Null => ArrowDataType::Null,
        Value::Bool(_) => ArrowDataType::Boolean,
        Value::Int64(_) => ArrowDataType::Int64,
        Value::Float64(_) => ArrowDataType::Float64,
        Value::String(_) => ArrowDataType::Utf8,
        Value::Binary(_) => ArrowDataType::Binary,
        Value::Timestamp(_) => {
            ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        Value::Date(_) => ArrowDataType::Date32,
        Value::Vector(v) => ArrowDataType::FixedSizeList(
            Arc::new(Field::new("item", ArrowDataType::Float64, false)),
            v.len() as i32,
        ),
        Value::Symbol(_) => ArrowDataType::Utf8,
        Value::Array(arr) => {
            let elem_type = arr
                .first()
                .map(value_to_arrow_type)
                .unwrap_or(ArrowDataType::Null);
            ArrowDataType::List(Arc::new(Field::new("item", elem_type, true)))
        }
        Value::Map(_) => ArrowDataType::Struct(Vec::<Field>::new().into()), // Simplified
    }
}

/// Build a PhysicalSchema for a projection operation.
///
/// Handles both simple column references and computed expressions.
pub fn build_project_schema(
    input_schema: &PhysicalSchema,
    projections: &[(LogicalExpr, String)],
) -> PhysicalSchema {
    let fields: Vec<Field> = projections
        .iter()
        .map(|(expr, name)| {
            let data_type = infer_expr_type(expr, input_schema).unwrap_or({
                // Default to Utf8 for unknown types
                ArrowDataType::Utf8
            });
            // Computed expressions are typically nullable
            let nullable = !matches!(expr, LogicalExpr::Column(_));
            Field::new(name.clone(), data_type, nullable)
        })
        .collect();

    PhysicalSchema::new(Arc::new(ArrowSchema::new(fields)))
}

/// Build a PhysicalSchema for an aggregate operation.
pub fn build_aggregate_schema(
    input_schema: &PhysicalSchema,
    aggregate: &AggregateOp,
) -> PhysicalSchema {
    let mut fields: Vec<Field> = Vec::new();

    // Add group key columns first
    for key_expr in &aggregate.group_keys {
        let name = key_expr.output_name();
        let data_type = infer_expr_type(key_expr, input_schema).unwrap_or(ArrowDataType::Utf8);
        fields.push(Field::new(name, data_type, true));
    }

    // Add aggregate result columns
    for agg_expr in &aggregate.aggregates {
        let name = agg_expr.output_name();
        let data_type =
            infer_aggregate_type(agg_expr, input_schema).unwrap_or(ArrowDataType::Float64);
        fields.push(Field::new(name, data_type, true));
    }

    PhysicalSchema::new(Arc::new(ArrowSchema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::PhysicalSchemaBuilder;
    use grism_logical::expr::{col, lit};

    fn test_schema() -> PhysicalSchema {
        PhysicalSchemaBuilder::new()
            .field("_id", ArrowDataType::Int64, false)
            .field("name", ArrowDataType::Utf8, true)
            .field("age", ArrowDataType::Int64, true)
            .field("salary", ArrowDataType::Float64, true)
            .build()
    }

    #[test]
    fn test_infer_column_type() {
        let schema = test_schema();
        assert_eq!(
            infer_expr_type(&col("_id"), &schema),
            Some(ArrowDataType::Int64)
        );
        assert_eq!(
            infer_expr_type(&col("name"), &schema),
            Some(ArrowDataType::Utf8)
        );
        assert_eq!(infer_expr_type(&col("unknown"), &schema), None);
    }

    #[test]
    fn test_infer_literal_type() {
        let schema = test_schema();
        assert_eq!(
            infer_expr_type(&lit(42i64), &schema),
            Some(ArrowDataType::Int64)
        );
        assert_eq!(
            infer_expr_type(&lit(3.14f64), &schema),
            Some(ArrowDataType::Float64)
        );
        assert_eq!(
            infer_expr_type(&lit("hello"), &schema),
            Some(ArrowDataType::Utf8)
        );
    }

    #[test]
    fn test_infer_arithmetic_type() {
        let schema = test_schema();

        // Int + Int = Int
        let expr = col("_id").add_expr(lit(1i64));
        assert_eq!(infer_expr_type(&expr, &schema), Some(ArrowDataType::Int64));

        // Int + Float = Float
        let expr = col("age").add_expr(col("salary"));
        assert_eq!(
            infer_expr_type(&expr, &schema),
            Some(ArrowDataType::Float64)
        );

        // Float * Float = Float
        let expr = col("salary").mul_expr(lit(1.1f64));
        assert_eq!(
            infer_expr_type(&expr, &schema),
            Some(ArrowDataType::Float64)
        );
    }

    #[test]
    fn test_infer_comparison_type() {
        let schema = test_schema();

        let expr = col("age").gt(lit(18i64));
        assert_eq!(
            infer_expr_type(&expr, &schema),
            Some(ArrowDataType::Boolean)
        );
    }

    #[test]
    fn test_infer_aggregate_type() {
        let schema = test_schema();

        // COUNT(*) returns Int64
        let count_star = AggExpr::count_star();
        assert_eq!(
            infer_aggregate_type(&count_star, &schema),
            Some(ArrowDataType::Int64)
        );

        // SUM(age) where age is Int64 returns Int64
        let sum_age = AggExpr::sum(col("age"));
        assert_eq!(
            infer_aggregate_type(&sum_age, &schema),
            Some(ArrowDataType::Int64)
        );

        // AVG(salary) returns Float64
        let avg_salary = AggExpr::avg(col("salary"));
        assert_eq!(
            infer_aggregate_type(&avg_salary, &schema),
            Some(ArrowDataType::Float64)
        );

        // MIN(age) preserves type
        let min_age = AggExpr::min(col("age"));
        assert_eq!(
            infer_aggregate_type(&min_age, &schema),
            Some(ArrowDataType::Int64)
        );
    }

    #[test]
    fn test_build_project_schema() {
        let schema = test_schema();

        // Simple column projection
        let projections = vec![
            (col("_id"), "_id".to_string()),
            (col("name"), "name".to_string()),
        ];
        let result = build_project_schema(&schema, &projections);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(
            result.field("_id").unwrap().data_type(),
            &ArrowDataType::Int64
        );
        assert_eq!(
            result.field("name").unwrap().data_type(),
            &ArrowDataType::Utf8
        );

        // Computed expression projection
        let projections = vec![
            (col("_id").add_expr(lit(1i64)), "id_plus_one".to_string()),
            (
                col("salary").mul_expr(lit(12f64)),
                "annual_salary".to_string(),
            ),
        ];
        let result = build_project_schema(&schema, &projections);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(
            result.field("id_plus_one").unwrap().data_type(),
            &ArrowDataType::Int64
        );
        assert_eq!(
            result.field("annual_salary").unwrap().data_type(),
            &ArrowDataType::Float64
        );
    }

    #[test]
    fn test_build_aggregate_schema() {
        let schema = test_schema();

        // Global aggregation (no group by)
        let aggregate = AggregateOp::new(vec![], vec![AggExpr::count_star()]);
        let result = build_aggregate_schema(&schema, &aggregate);
        assert_eq!(result.num_columns(), 1);
        assert_eq!(
            result.field("COUNT(*)").unwrap().data_type(),
            &ArrowDataType::Int64
        );

        // Group by with aggregate
        let aggregate = AggregateOp::new(vec![col("name")], vec![AggExpr::avg(col("salary"))]);
        let result = build_aggregate_schema(&schema, &aggregate);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(
            result.field("name").unwrap().data_type(),
            &ArrowDataType::Utf8
        );
        assert_eq!(
            result.field("AVG(salary)").unwrap().data_type(),
            &ArrowDataType::Float64
        );
    }
}
