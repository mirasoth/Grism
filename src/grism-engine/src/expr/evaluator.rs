//! Expression evaluator implementation.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, new_null_array,
};
use arrow::compute::{self, kernels::boolean, kernels::cmp};
use arrow::datatypes::DataType as ArrowDataType;
use arrow::record_batch::RecordBatch;

use common_error::{GrismError, GrismResult};
use grism_core::Value;
use grism_logical::LogicalExpr;
use grism_logical::expr::{BinaryOp, UnaryOp};

/// Expression evaluator for physical execution.
///
/// Converts `LogicalExpr` to Arrow arrays by evaluating against a `RecordBatch`.
#[derive(Debug, Default)]
pub struct ExprEvaluator;

impl ExprEvaluator {
    /// Create a new expression evaluator.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Evaluate an expression against a record batch.
    ///
    /// Returns an Arrow array with the evaluation result.
    pub fn evaluate(&self, expr: &LogicalExpr, batch: &RecordBatch) -> GrismResult<ArrayRef> {
        match expr {
            LogicalExpr::Literal(value) => self.eval_literal(value, batch.num_rows()),

            LogicalExpr::Column(name) => self.eval_column(name, None, batch),

            LogicalExpr::QualifiedColumn { qualifier, name } => {
                self.eval_column(name, Some(qualifier), batch)
            }

            LogicalExpr::Binary { left, op, right } => self.eval_binary(left, *op, right, batch),

            LogicalExpr::Unary { op, expr } => self.eval_unary(*op, expr, batch),

            LogicalExpr::Case {
                operand,
                when_clauses,
                else_result,
            } => self.eval_case(
                operand.as_deref(),
                when_clauses,
                else_result.as_deref(),
                batch,
            ),

            LogicalExpr::InList {
                expr,
                list,
                negated,
            } => self.eval_in_list(expr, list, *negated, batch),

            LogicalExpr::Between {
                expr,
                low,
                high,
                negated,
            } => self.eval_between(expr, low, high, *negated, batch),

            LogicalExpr::Alias { expr, .. } => {
                // Alias doesn't change the value
                self.evaluate(expr, batch)
            }

            LogicalExpr::TypeLiteral(_) => Err(GrismError::execution(
                "TypeLiteral cannot be evaluated directly",
            )),

            LogicalExpr::Function(_) => Err(GrismError::not_implemented(
                "Function expression evaluation",
            )),

            LogicalExpr::Aggregate(_) => Err(GrismError::execution(
                "Aggregate expressions must be evaluated by aggregate operators",
            )),

            LogicalExpr::Wildcard | LogicalExpr::QualifiedWildcard(_) => Err(
                GrismError::execution("Wildcard cannot be evaluated directly"),
            ),

            // Subqueries, exists, placeholders, and sort keys require special handling
            LogicalExpr::Subquery(_)
            | LogicalExpr::Exists { .. }
            | LogicalExpr::Placeholder { .. }
            | LogicalExpr::SortKey { .. } => Err(GrismError::not_implemented(format!(
                "Expression type {:?} not supported in physical evaluation",
                expr
            ))),
        }
    }

    /// Evaluate a predicate expression, returning a BooleanArray.
    pub fn evaluate_predicate(
        &self,
        expr: &LogicalExpr,
        batch: &RecordBatch,
    ) -> GrismResult<BooleanArray> {
        let result = self.evaluate(expr, batch)?;

        // Cast to BooleanArray
        result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .cloned()
            .ok_or_else(|| {
                GrismError::type_error(format!(
                    "Predicate must evaluate to boolean, got {:?}",
                    result.data_type()
                ))
            })
    }

    /// Evaluate a literal value.
    fn eval_literal(&self, value: &Value, num_rows: usize) -> GrismResult<ArrayRef> {
        match value {
            Value::Null => Ok(new_null_array(&ArrowDataType::Null, num_rows)),

            Value::Bool(b) => Ok(Arc::new(BooleanArray::from(vec![*b; num_rows]))),

            Value::Int64(i) => Ok(Arc::new(Int64Array::from(vec![*i; num_rows]))),

            Value::Float64(f) => Ok(Arc::new(Float64Array::from(vec![*f; num_rows]))),

            Value::String(s) => Ok(Arc::new(StringArray::from(vec![s.as_str(); num_rows]))),

            _ => Err(GrismError::not_implemented(format!(
                "Literal evaluation for {:?}",
                value
            ))),
        }
    }

    /// Evaluate a column reference.
    fn eval_column(
        &self,
        name: &str,
        qualifier: Option<&String>,
        batch: &RecordBatch,
    ) -> GrismResult<ArrayRef> {
        // Try qualified name first if qualifier is provided
        if let Some(q) = qualifier {
            if let Some(col) = batch.column_by_name(&format!("{q}.{name}")) {
                return Ok(col.clone());
            }
        }

        // Try unqualified name
        batch.column_by_name(name).cloned().ok_or_else(|| {
            GrismError::execution(format!(
                "Column '{}' not found in batch with columns: {:?}",
                name,
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            ))
        })
    }

    /// Evaluate a binary expression.
    fn eval_binary(
        &self,
        left: &LogicalExpr,
        op: BinaryOp,
        right: &LogicalExpr,
        batch: &RecordBatch,
    ) -> GrismResult<ArrayRef> {
        let left_arr = self.evaluate(left, batch)?;
        let right_arr = self.evaluate(right, batch)?;

        match op {
            // Arithmetic operators
            BinaryOp::Add => self.eval_arithmetic_add(&left_arr, &right_arr),
            BinaryOp::Subtract => self.eval_arithmetic_sub(&left_arr, &right_arr),
            BinaryOp::Multiply => self.eval_arithmetic_mul(&left_arr, &right_arr),
            BinaryOp::Divide => self.eval_arithmetic_div(&left_arr, &right_arr),
            BinaryOp::Modulo => self.eval_arithmetic_rem(&left_arr, &right_arr),

            // Comparison operators
            BinaryOp::Eq => self.eval_cmp_eq(&left_arr, &right_arr),
            BinaryOp::NotEq => self.eval_cmp_neq(&left_arr, &right_arr),
            BinaryOp::Lt => self.eval_cmp_lt(&left_arr, &right_arr),
            BinaryOp::LtEq => self.eval_cmp_lt_eq(&left_arr, &right_arr),
            BinaryOp::Gt => self.eval_cmp_gt(&left_arr, &right_arr),
            BinaryOp::GtEq => self.eval_cmp_gt_eq(&left_arr, &right_arr),

            // Logical operators
            BinaryOp::And => self.eval_logical_and(&left_arr, &right_arr),
            BinaryOp::Or => self.eval_logical_or(&left_arr, &right_arr),

            // String operators
            BinaryOp::Concat => self.eval_concat(&left_arr, &right_arr),

            // Null-safe comparison
            BinaryOp::IsDistinctFrom | BinaryOp::IsNotDistinctFrom => {
                Err(GrismError::not_implemented("IS DISTINCT FROM evaluation"))
            }
        }
    }

    /// Evaluate arithmetic addition.
    fn eval_arithmetic_add(&self, left: &ArrayRef, right: &ArrayRef) -> GrismResult<ArrayRef> {
        use arrow::compute::kernels::numeric::add;
        add(left, right).map_err(|e| GrismError::execution(e.to_string()))
    }

    /// Evaluate arithmetic subtraction.
    fn eval_arithmetic_sub(&self, left: &ArrayRef, right: &ArrayRef) -> GrismResult<ArrayRef> {
        use arrow::compute::kernels::numeric::sub;
        sub(left, right).map_err(|e| GrismError::execution(e.to_string()))
    }

    /// Evaluate arithmetic multiplication.
    fn eval_arithmetic_mul(&self, left: &ArrayRef, right: &ArrayRef) -> GrismResult<ArrayRef> {
        use arrow::compute::kernels::numeric::mul;
        mul(left, right).map_err(|e| GrismError::execution(e.to_string()))
    }

    /// Evaluate arithmetic division.
    fn eval_arithmetic_div(&self, left: &ArrayRef, right: &ArrayRef) -> GrismResult<ArrayRef> {
        use arrow::compute::kernels::numeric::div;
        div(left, right).map_err(|e| GrismError::execution(e.to_string()))
    }

    /// Evaluate arithmetic modulo.
    fn eval_arithmetic_rem(&self, left: &ArrayRef, right: &ArrayRef) -> GrismResult<ArrayRef> {
        use arrow::compute::kernels::numeric::rem;
        rem(left, right).map_err(|e| GrismError::execution(e.to_string()))
    }

    /// Evaluate equality comparison.
    fn eval_cmp_eq(&self, left: &ArrayRef, right: &ArrayRef) -> GrismResult<ArrayRef> {
        let result = cmp::eq(left, right).map_err(|e| GrismError::execution(e.to_string()))?;
        Ok(Arc::new(result))
    }

    /// Evaluate not-equal comparison.
    fn eval_cmp_neq(&self, left: &ArrayRef, right: &ArrayRef) -> GrismResult<ArrayRef> {
        let result = cmp::neq(left, right).map_err(|e| GrismError::execution(e.to_string()))?;
        Ok(Arc::new(result))
    }

    /// Evaluate less-than comparison.
    fn eval_cmp_lt(&self, left: &ArrayRef, right: &ArrayRef) -> GrismResult<ArrayRef> {
        let result = cmp::lt(left, right).map_err(|e| GrismError::execution(e.to_string()))?;
        Ok(Arc::new(result))
    }

    /// Evaluate less-than-or-equal comparison.
    fn eval_cmp_lt_eq(&self, left: &ArrayRef, right: &ArrayRef) -> GrismResult<ArrayRef> {
        let result = cmp::lt_eq(left, right).map_err(|e| GrismError::execution(e.to_string()))?;
        Ok(Arc::new(result))
    }

    /// Evaluate greater-than comparison.
    fn eval_cmp_gt(&self, left: &ArrayRef, right: &ArrayRef) -> GrismResult<ArrayRef> {
        let result = cmp::gt(left, right).map_err(|e| GrismError::execution(e.to_string()))?;
        Ok(Arc::new(result))
    }

    /// Evaluate greater-than-or-equal comparison.
    fn eval_cmp_gt_eq(&self, left: &ArrayRef, right: &ArrayRef) -> GrismResult<ArrayRef> {
        let result = cmp::gt_eq(left, right).map_err(|e| GrismError::execution(e.to_string()))?;
        Ok(Arc::new(result))
    }

    /// Evaluate logical AND.
    fn eval_logical_and(&self, left: &ArrayRef, right: &ArrayRef) -> GrismResult<ArrayRef> {
        let left_bool = left
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| GrismError::type_error("AND requires boolean operands"))?;
        let right_bool = right
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| GrismError::type_error("AND requires boolean operands"))?;

        let result = boolean::and(left_bool, right_bool)
            .map_err(|e| GrismError::execution(e.to_string()))?;
        Ok(Arc::new(result))
    }

    /// Evaluate logical OR.
    fn eval_logical_or(&self, left: &ArrayRef, right: &ArrayRef) -> GrismResult<ArrayRef> {
        let left_bool = left
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| GrismError::type_error("OR requires boolean operands"))?;
        let right_bool = right
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| GrismError::type_error("OR requires boolean operands"))?;

        let result =
            boolean::or(left_bool, right_bool).map_err(|e| GrismError::execution(e.to_string()))?;
        Ok(Arc::new(result))
    }

    /// Evaluate string concatenation.
    fn eval_concat(&self, left: &ArrayRef, right: &ArrayRef) -> GrismResult<ArrayRef> {
        let left_str = left
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| GrismError::type_error("CONCAT requires string operands"))?;
        let right_str = right
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| GrismError::type_error("CONCAT requires string operands"))?;

        let result: StringArray = left_str
            .iter()
            .zip(right_str.iter())
            .map(|(l, r)| match (l, r) {
                (Some(l), Some(r)) => Some(format!("{l}{r}")),
                _ => None,
            })
            .collect();

        Ok(Arc::new(result))
    }

    /// Evaluate a unary expression.
    fn eval_unary(
        &self,
        op: UnaryOp,
        expr: &LogicalExpr,
        batch: &RecordBatch,
    ) -> GrismResult<ArrayRef> {
        let arr = self.evaluate(expr, batch)?;

        match op {
            UnaryOp::Not => {
                let bool_arr = arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| GrismError::type_error("NOT requires boolean operand"))?;
                let result =
                    boolean::not(bool_arr).map_err(|e| GrismError::execution(e.to_string()))?;
                Ok(Arc::new(result))
            }

            UnaryOp::IsNull => {
                let result =
                    compute::is_null(&arr).map_err(|e| GrismError::execution(e.to_string()))?;
                Ok(Arc::new(result))
            }

            UnaryOp::IsNotNull => {
                let result =
                    compute::is_not_null(&arr).map_err(|e| GrismError::execution(e.to_string()))?;
                Ok(Arc::new(result))
            }

            UnaryOp::Neg => {
                use arrow::compute::kernels::numeric::neg;
                let result = neg(&arr).map_err(|e| GrismError::execution(e.to_string()))?;
                Ok(result)
            }

            _ => Err(GrismError::not_implemented(format!(
                "Unary operator {:?}",
                op
            ))),
        }
    }

    /// Evaluate a CASE expression.
    fn eval_case(
        &self,
        operand: Option<&LogicalExpr>,
        when_clauses: &[(LogicalExpr, LogicalExpr)],
        else_result: Option<&LogicalExpr>,
        batch: &RecordBatch,
    ) -> GrismResult<ArrayRef> {
        let num_rows = batch.num_rows();

        // Evaluate operand if present (simple CASE)
        let operand_arr = operand.map(|o| self.evaluate(o, batch)).transpose()?;

        // Track which rows have been assigned a result
        let mut result_assigned = vec![false; num_rows];
        let mut result_values: Vec<Option<ArrayRef>> = vec![None; num_rows];

        for (when_expr, then_expr) in when_clauses {
            // Evaluate condition
            let condition = if let Some(ref op_arr) = operand_arr {
                // Simple CASE: operand = when_expr
                let when_arr = self.evaluate(when_expr, batch)?;
                cmp::eq(op_arr, &when_arr).map_err(|e| GrismError::execution(e.to_string()))?
            } else {
                // Searched CASE: when_expr is a boolean
                self.evaluate_predicate(when_expr, batch)?
            };

            // Evaluate then expression
            let then_arr = self.evaluate(then_expr, batch)?;

            // Assign results for matching rows
            for (i, (cond, assigned)) in
                condition.iter().zip(result_assigned.iter_mut()).enumerate()
            {
                if !*assigned && cond == Some(true) {
                    *assigned = true;
                    result_values[i] = Some(then_arr.clone());
                }
            }
        }

        // Handle ELSE clause
        let else_arr = if let Some(else_expr) = else_result {
            Some(self.evaluate(else_expr, batch)?)
        } else {
            None
        };

        // Build final result
        // For simplicity, we'll return the first when result type or else type
        // This is a simplified implementation
        if when_clauses.is_empty() && else_result.is_none() {
            return Ok(new_null_array(&ArrowDataType::Null, num_rows));
        }

        // Get the result data type
        let result_type = if !when_clauses.is_empty() {
            let first_then = self.evaluate(&when_clauses[0].1, batch)?;
            first_then.data_type().clone()
        } else if let Some(ref ea) = else_arr {
            ea.data_type().clone()
        } else {
            ArrowDataType::Null
        };

        // For simplicity, build result by selecting from each when clause
        // This is a basic implementation - production would use arrow::compute::case
        let default = else_arr.unwrap_or_else(|| new_null_array(&result_type, num_rows));

        // Re-evaluate all when clauses and build result using compute::if_then_else
        let mut result = default;
        for (when_expr, then_expr) in when_clauses.iter().rev() {
            let condition = if let Some(ref op_arr) = operand_arr {
                let when_arr = self.evaluate(when_expr, batch)?;
                cmp::eq(op_arr, &when_arr).map_err(|e| GrismError::execution(e.to_string()))?
            } else {
                self.evaluate_predicate(when_expr, batch)?
            };

            let then_arr = self.evaluate(then_expr, batch)?;

            // Use zip to build result
            result = self.if_then_else(&condition, &then_arr, &result)?;
        }

        Ok(result)
    }

    /// Simple if-then-else implementation.
    fn if_then_else(
        &self,
        condition: &BooleanArray,
        then_arr: &ArrayRef,
        else_arr: &ArrayRef,
    ) -> GrismResult<ArrayRef> {
        // Use arrow's zip/if_then_else when available
        // For now, implement manually based on data type
        match then_arr.data_type() {
            ArrowDataType::Int64 => {
                let then_i64 = then_arr.as_any().downcast_ref::<Int64Array>().unwrap();
                let else_i64 = else_arr.as_any().downcast_ref::<Int64Array>().unwrap();
                let result: Int64Array = condition
                    .iter()
                    .zip(then_i64.iter())
                    .zip(else_i64.iter())
                    .map(|((c, t), e)| if c == Some(true) { t } else { e })
                    .collect();
                Ok(Arc::new(result))
            }
            ArrowDataType::Float64 => {
                let then_f64 = then_arr.as_any().downcast_ref::<Float64Array>().unwrap();
                let else_f64 = else_arr.as_any().downcast_ref::<Float64Array>().unwrap();
                let result: Float64Array = condition
                    .iter()
                    .zip(then_f64.iter())
                    .zip(else_f64.iter())
                    .map(|((c, t), e)| if c == Some(true) { t } else { e })
                    .collect();
                Ok(Arc::new(result))
            }
            ArrowDataType::Utf8 => {
                let then_str = then_arr.as_any().downcast_ref::<StringArray>().unwrap();
                let else_str = else_arr.as_any().downcast_ref::<StringArray>().unwrap();
                let result: StringArray = condition
                    .iter()
                    .zip(then_str.iter())
                    .zip(else_str.iter())
                    .map(|((c, t), e)| {
                        if c == Some(true) {
                            t.map(|s| s.to_string())
                        } else {
                            e.map(|s| s.to_string())
                        }
                    })
                    .collect();
                Ok(Arc::new(result))
            }
            ArrowDataType::Boolean => {
                let then_bool = then_arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                let else_bool = else_arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                let result: BooleanArray = condition
                    .iter()
                    .zip(then_bool.iter())
                    .zip(else_bool.iter())
                    .map(|((c, t), e)| if c == Some(true) { t } else { e })
                    .collect();
                Ok(Arc::new(result))
            }
            _ => Err(GrismError::not_implemented(format!(
                "CASE for data type {:?}",
                then_arr.data_type()
            ))),
        }
    }

    /// Evaluate an IN list expression.
    fn eval_in_list(
        &self,
        expr: &LogicalExpr,
        list: &[LogicalExpr],
        negated: bool,
        batch: &RecordBatch,
    ) -> GrismResult<ArrayRef> {
        let value_arr = self.evaluate(expr, batch)?;

        // Evaluate each list item and check equality
        let mut result = BooleanArray::from(vec![false; batch.num_rows()]);

        for list_item in list {
            let item_arr = self.evaluate(list_item, batch)?;
            let eq_result =
                cmp::eq(&value_arr, &item_arr).map_err(|e| GrismError::execution(e.to_string()))?;
            result = boolean::or(&result, &eq_result)
                .map_err(|e| GrismError::execution(e.to_string()))?;
        }

        if negated {
            result = boolean::not(&result).map_err(|e| GrismError::execution(e.to_string()))?;
        }

        Ok(Arc::new(result))
    }

    /// Evaluate a BETWEEN expression.
    fn eval_between(
        &self,
        expr: &LogicalExpr,
        low: &LogicalExpr,
        high: &LogicalExpr,
        negated: bool,
        batch: &RecordBatch,
    ) -> GrismResult<ArrayRef> {
        let value_arr = self.evaluate(expr, batch)?;
        let low_arr = self.evaluate(low, batch)?;
        let high_arr = self.evaluate(high, batch)?;

        // value >= low AND value <= high
        let ge_low =
            cmp::gt_eq(&value_arr, &low_arr).map_err(|e| GrismError::execution(e.to_string()))?;
        let le_high =
            cmp::lt_eq(&value_arr, &high_arr).map_err(|e| GrismError::execution(e.to_string()))?;

        let mut result =
            boolean::and(&ge_low, &le_high).map_err(|e| GrismError::execution(e.to_string()))?;

        if negated {
            result = boolean::not(&result).map_err(|e| GrismError::execution(e.to_string()))?;
        }

        Ok(Arc::new(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use grism_logical::expr::{col, lit};

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, true),
            Field::new("age", ArrowDataType::Int64, true),
            Field::new("active", ArrowDataType::Boolean, true),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec![
            Some("Alice"),
            Some("Bob"),
            Some("Charlie"),
            None,
            Some("Eve"),
        ]);
        let age_array = Int64Array::from(vec![Some(25), Some(30), Some(35), Some(40), None]);
        let active_array = BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(true),
        ]);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(age_array),
                Arc::new(active_array),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_eval_literal() {
        let evaluator = ExprEvaluator::new();
        let batch = create_test_batch();

        let expr = lit(42i64);
        let result = evaluator.evaluate(&expr, &batch).unwrap();
        let int_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(int_arr.len(), 5);
        assert_eq!(int_arr.value(0), 42);
    }

    #[test]
    fn test_eval_column() {
        let evaluator = ExprEvaluator::new();
        let batch = create_test_batch();

        let expr = col("age");
        let result = evaluator.evaluate(&expr, &batch).unwrap();
        let int_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(int_arr.len(), 5);
        assert_eq!(int_arr.value(0), 25);
        assert_eq!(int_arr.value(1), 30);
    }

    #[test]
    fn test_eval_comparison() {
        let evaluator = ExprEvaluator::new();
        let batch = create_test_batch();

        // age > 30
        let expr = col("age").gt(lit(30i64));
        let result = evaluator.evaluate_predicate(&expr, &batch).unwrap();

        assert_eq!(result.len(), 5);
        assert!(!result.value(0)); // 25 > 30 = false
        assert!(!result.value(1)); // 30 > 30 = false
        assert!(result.value(2)); // 35 > 30 = true
        assert!(result.value(3)); // 40 > 30 = true
    }

    #[test]
    fn test_eval_logical_and() {
        let evaluator = ExprEvaluator::new();
        let batch = create_test_batch();

        // age > 25 AND active = true
        let expr = col("age").gt(lit(25i64)).and(col("active").eq(lit(true)));
        let result = evaluator.evaluate_predicate(&expr, &batch).unwrap();

        assert!(!result.value(0)); // 25 > 25 = false
        assert!(!result.value(1)); // 30 > 25 AND false = false
        assert!(result.value(2)); // 35 > 25 AND true = true
    }

    #[test]
    fn test_eval_arithmetic() {
        let evaluator = ExprEvaluator::new();
        let batch = create_test_batch();

        // age + 10
        let expr = col("age").add_expr(lit(10i64));
        let result = evaluator.evaluate(&expr, &batch).unwrap();
        let int_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(int_arr.value(0), 35); // 25 + 10
        assert_eq!(int_arr.value(1), 40); // 30 + 10
    }

    #[test]
    fn test_eval_unary_not() {
        let evaluator = ExprEvaluator::new();
        let batch = create_test_batch();

        // NOT active
        let expr = col("active").logical_not();
        let result = evaluator.evaluate_predicate(&expr, &batch).unwrap();

        assert!(!result.value(0)); // NOT true = false
        assert!(result.value(1)); // NOT false = true
    }

    #[test]
    fn test_eval_is_null() {
        let evaluator = ExprEvaluator::new();
        let batch = create_test_batch();

        // name IS NULL
        let expr = col("name").is_null();
        let result = evaluator.evaluate_predicate(&expr, &batch).unwrap();

        assert!(!result.value(0)); // Alice is not null
        assert!(!result.value(1)); // Bob is not null
        assert!(result.value(3)); // NULL is null
    }

    #[test]
    fn test_eval_between() {
        let evaluator = ExprEvaluator::new();
        let batch = create_test_batch();

        // age BETWEEN 25 AND 35
        let expr = LogicalExpr::Between {
            expr: Box::new(col("age")),
            low: Box::new(lit(25i64)),
            high: Box::new(lit(35i64)),
            negated: false,
        };
        let result = evaluator.evaluate_predicate(&expr, &batch).unwrap();

        assert!(result.value(0)); // 25 in [25, 35]
        assert!(result.value(1)); // 30 in [25, 35]
        assert!(result.value(2)); // 35 in [25, 35]
        assert!(!result.value(3)); // 40 not in [25, 35]
    }

    #[test]
    fn test_eval_in_list() {
        let evaluator = ExprEvaluator::new();
        let batch = create_test_batch();

        // age IN (25, 35)
        let expr = LogicalExpr::InList {
            expr: Box::new(col("age")),
            list: vec![lit(25i64), lit(35i64)],
            negated: false,
        };
        let result = evaluator.evaluate_predicate(&expr, &batch).unwrap();

        assert!(result.value(0)); // 25 in list
        assert!(!result.value(1)); // 30 not in list
        assert!(result.value(2)); // 35 in list
    }

    #[test]
    fn test_column_not_found_error() {
        let evaluator = ExprEvaluator::new();
        let batch = create_test_batch();

        let expr = col("nonexistent");
        let result = evaluator.evaluate(&expr, &batch);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }
}
