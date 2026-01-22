//! Logical expression system (RFC-0003 compliant).
//!
//! Expressions are the smallest executable semantic units used in predicates,
//! projections, relational composition, aggregations, and inference rules.

use std::collections::HashSet;

use grism_core::{DataType, Schema, Value};
use serde::{Deserialize, Serialize};

use super::{AggExpr, BinaryOp, FuncExpr, UnaryOp};

/// A logical expression in the Grism query system.
///
/// Expressions form a DAG and are pure computations over columns.
/// Per RFC-0003, expressions must be deterministic unless explicitly marked otherwise.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LogicalExpr {
    /// A literal constant value.
    Literal(Value),

    /// A type literal (used in CAST expressions).
    TypeLiteral(DataType),

    /// A column reference (unqualified or qualified).
    Column(String),

    /// A qualified column reference (entity.column).
    QualifiedColumn {
        /// Entity qualifier (label or alias).
        qualifier: String,
        /// Column name.
        name: String,
    },

    /// A binary operation.
    Binary {
        /// Left operand.
        left: Box<Self>,
        /// Binary operator.
        op: BinaryOp,
        /// Right operand.
        right: Box<Self>,
    },

    /// A unary operation.
    Unary {
        /// Unary operator.
        op: UnaryOp,
        /// Operand.
        expr: Box<Self>,
    },

    /// A function call.
    Function(FuncExpr),

    /// An aggregate function (only valid in Aggregate context).
    Aggregate(AggExpr),

    /// A CASE WHEN expression.
    Case {
        /// Optional operand for simple CASE.
        operand: Option<Box<Self>>,
        /// WHEN clauses: (condition, result).
        when_clauses: Vec<(Self, Self)>,
        /// ELSE clause.
        else_result: Option<Box<Self>>,
    },

    /// An IN expression.
    InList {
        /// Expression to check.
        expr: Box<Self>,
        /// List of values.
        list: Vec<Self>,
        /// Whether this is NOT IN.
        negated: bool,
    },

    /// A BETWEEN expression.
    Between {
        /// Expression to check.
        expr: Box<Self>,
        /// Lower bound.
        low: Box<Self>,
        /// Upper bound.
        high: Box<Self>,
        /// Whether this is NOT BETWEEN.
        negated: bool,
    },

    /// An aliased expression.
    Alias {
        /// Original expression.
        expr: Box<Self>,
        /// Alias name.
        alias: String,
    },

    /// A wildcard (*) for all columns or COUNT(*).
    Wildcard,

    /// A qualified wildcard (entity.*) for all columns of an entity.
    QualifiedWildcard(String),

    /// A subquery expression (for scalar subqueries).
    Subquery(Box<crate::LogicalPlan>),

    /// An EXISTS subquery.
    Exists {
        /// The subquery.
        subquery: Box<crate::LogicalPlan>,
        /// Whether this is NOT EXISTS.
        negated: bool,
    },

    /// A placeholder for parameter binding.
    Placeholder {
        /// Parameter index.
        index: usize,
        /// Expected type.
        data_type: Option<DataType>,
    },

    /// A sort key expression.
    SortKey {
        /// Expression to sort by.
        expr: Box<Self>,
        /// Ascending or descending.
        ascending: bool,
        /// Nulls first or last.
        nulls_first: bool,
    },
}

impl LogicalExpr {
    // ========== Constructors ==========

    /// Create a literal expression.
    pub fn literal(value: impl Into<Value>) -> Self {
        Self::Literal(value.into())
    }

    /// Create a column reference.
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column(name.into())
    }

    /// Create a qualified column reference.
    pub fn qualified_column(qualifier: impl Into<String>, name: impl Into<String>) -> Self {
        Self::QualifiedColumn {
            qualifier: qualifier.into(),
            name: name.into(),
        }
    }

    /// Create a binary expression.
    pub fn binary(left: Self, op: BinaryOp, right: Self) -> Self {
        Self::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    /// Create a unary expression.
    pub fn unary(op: UnaryOp, expr: Self) -> Self {
        Self::Unary {
            op,
            expr: Box::new(expr),
        }
    }

    /// Create a function call expression.
    pub const fn function(func: FuncExpr) -> Self {
        Self::Function(func)
    }

    /// Create an aggregate expression.
    pub const fn aggregate(agg: AggExpr) -> Self {
        Self::Aggregate(agg)
    }

    /// Create an aliased expression.
    #[must_use]
    pub fn alias(self, alias: impl Into<String>) -> Self {
        Self::Alias {
            expr: Box::new(self),
            alias: alias.into(),
        }
    }

    // ========== Convenience builders ==========

    /// Create an AND expression.
    #[must_use]
    pub fn and(self, other: Self) -> Self {
        Self::binary(self, BinaryOp::And, other)
    }

    /// Create an OR expression.
    #[must_use]
    pub fn or(self, other: Self) -> Self {
        Self::binary(self, BinaryOp::Or, other)
    }

    /// Create a NOT expression.
    #[must_use]
    pub fn logical_not(self) -> Self {
        Self::unary(UnaryOp::Not, self)
    }

    /// Create an equality expression.
    #[must_use]
    pub fn eq(self, other: Self) -> Self {
        Self::binary(self, BinaryOp::Eq, other)
    }

    /// Create an inequality expression.
    #[must_use]
    pub fn not_eq(self, other: Self) -> Self {
        Self::binary(self, BinaryOp::NotEq, other)
    }

    /// Create a less than expression.
    #[must_use]
    pub fn lt(self, other: Self) -> Self {
        Self::binary(self, BinaryOp::Lt, other)
    }

    /// Create a less than or equal expression.
    #[must_use]
    pub fn lt_eq(self, other: Self) -> Self {
        Self::binary(self, BinaryOp::LtEq, other)
    }

    /// Create a greater than expression.
    #[must_use]
    pub fn gt(self, other: Self) -> Self {
        Self::binary(self, BinaryOp::Gt, other)
    }

    /// Create a greater than or equal expression.
    #[must_use]
    pub fn gt_eq(self, other: Self) -> Self {
        Self::binary(self, BinaryOp::GtEq, other)
    }

    /// Create an IS NULL expression.
    #[must_use]
    pub fn is_null(self) -> Self {
        Self::unary(UnaryOp::IsNull, self)
    }

    /// Create an IS NOT NULL expression.
    #[must_use]
    pub fn is_not_null(self) -> Self {
        Self::unary(UnaryOp::IsNotNull, self)
    }

    /// Create an addition expression.
    #[must_use]
    pub fn add_expr(self, other: Self) -> Self {
        Self::binary(self, BinaryOp::Add, other)
    }

    /// Create a subtraction expression.
    #[must_use]
    pub fn sub_expr(self, other: Self) -> Self {
        Self::binary(self, BinaryOp::Subtract, other)
    }

    /// Create a multiplication expression.
    #[must_use]
    pub fn mul_expr(self, other: Self) -> Self {
        Self::binary(self, BinaryOp::Multiply, other)
    }

    /// Create a division expression.
    #[must_use]
    pub fn div_expr(self, other: Self) -> Self {
        Self::binary(self, BinaryOp::Divide, other)
    }

    // ========== Analysis methods ==========

    /// Get all column references in this expression.
    pub fn column_refs(&self) -> HashSet<String> {
        let mut refs = HashSet::new();
        self.collect_column_refs(&mut refs);
        refs
    }

    fn collect_column_refs(&self, refs: &mut HashSet<String>) {
        match self {
            Self::Column(name) => {
                refs.insert(name.clone());
            }
            Self::QualifiedColumn { qualifier, name } => {
                refs.insert(format!("{qualifier}.{name}"));
            }
            Self::Binary { left, right, .. } => {
                left.collect_column_refs(refs);
                right.collect_column_refs(refs);
            }
            Self::Unary { expr, .. }
            | Self::Aggregate(AggExpr { expr, .. })
            | Self::Alias { expr, .. }
            | Self::SortKey { expr, .. } => {
                expr.collect_column_refs(refs);
            }
            Self::Function(func) => {
                for arg in &func.args {
                    arg.collect_column_refs(refs);
                }
            }
            Self::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                if let Some(op) = operand {
                    op.collect_column_refs(refs);
                }
                for (cond, result) in when_clauses {
                    cond.collect_column_refs(refs);
                    result.collect_column_refs(refs);
                }
                if let Some(else_expr) = else_result {
                    else_expr.collect_column_refs(refs);
                }
            }
            Self::InList { expr, list, .. } => {
                expr.collect_column_refs(refs);
                for item in list {
                    item.collect_column_refs(refs);
                }
            }
            Self::Between {
                expr, low, high, ..
            } => {
                expr.collect_column_refs(refs);
                low.collect_column_refs(refs);
                high.collect_column_refs(refs);
            }
            Self::QualifiedWildcard(qualifier) => {
                refs.insert(format!("{qualifier}.*"));
            }
            Self::Subquery(_)
            | Self::Exists { .. }
            | Self::Literal(_)
            | Self::TypeLiteral(_)
            | Self::Wildcard
            | Self::Placeholder { .. } => {}
        }
    }

    /// Check if this expression contains any aggregate functions.
    pub fn contains_aggregate(&self) -> bool {
        match self {
            Self::Aggregate(_) => true,
            Self::Binary { left, right, .. } => {
                left.contains_aggregate() || right.contains_aggregate()
            }
            Self::Unary { expr, .. } | Self::Alias { expr, .. } => expr.contains_aggregate(),
            Self::Function(func) => func.args.iter().any(Self::contains_aggregate),
            Self::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                operand.as_ref().is_some_and(|o| o.contains_aggregate())
                    || when_clauses
                        .iter()
                        .any(|(c, r)| c.contains_aggregate() || r.contains_aggregate())
                    || else_result.as_ref().is_some_and(|e| e.contains_aggregate())
            }
            _ => false,
        }
    }

    /// Check if this expression is deterministic.
    pub fn is_deterministic(&self) -> bool {
        match self {
            Self::Literal(_)
            | Self::TypeLiteral(_)
            | Self::Column(_)
            | Self::QualifiedColumn { .. }
            | Self::Wildcard
            | Self::QualifiedWildcard(_)
            | Self::Placeholder { .. } => true,

            Self::Binary { left, right, .. } => left.is_deterministic() && right.is_deterministic(),

            Self::Unary { expr, .. }
            | Self::Alias { expr, .. }
            | Self::SortKey { expr, .. }
            | Self::Aggregate(AggExpr { expr, .. }) => expr.is_deterministic(),

            Self::Function(func) => {
                use super::func::Determinism;
                func.determinism() == Determinism::Deterministic
                    && func.args.iter().all(Self::is_deterministic)
            }

            Self::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                operand.as_ref().is_some_and(|o| o.is_deterministic())
                    && when_clauses
                        .iter()
                        .all(|(c, r)| c.is_deterministic() && r.is_deterministic())
                    && else_result.as_ref().is_some_and(|e| e.is_deterministic())
            }

            Self::InList { expr, list, .. } => {
                expr.is_deterministic() && list.iter().all(Self::is_deterministic)
            }

            Self::Between {
                expr, low, high, ..
            } => expr.is_deterministic() && low.is_deterministic() && high.is_deterministic(),

            Self::Subquery(_) | Self::Exists { .. } => false, // Conservative
        }
    }

    /// Check if this expression is a simple column reference.
    pub const fn is_column(&self) -> bool {
        matches!(self, Self::Column(_) | Self::QualifiedColumn { .. })
    }

    /// Check if this expression is a literal.
    pub const fn is_literal(&self) -> bool {
        matches!(self, Self::Literal(_))
    }

    /// Try to evaluate as a constant if possible.
    pub const fn try_as_literal(&self) -> Option<&Value> {
        match self {
            Self::Literal(v) => Some(v),
            _ => None,
        }
    }

    /// Get the output name for this expression.
    pub fn output_name(&self) -> String {
        match self {
            Self::Column(name) => name.clone(),
            Self::QualifiedColumn { qualifier, name } => format!("{qualifier}.{name}"),
            Self::Alias { alias, .. } => alias.clone(),
            Self::Aggregate(agg) => agg.output_name(),
            Self::Function(func) => func.name(),
            Self::Literal(v) => format!("{v:?}"),
            Self::Binary { op, .. } => op.symbol().to_string(),
            Self::Unary { op, .. } => op.name().to_string(),
            Self::Wildcard => "*".to_string(),
            Self::QualifiedWildcard(q) => format!("{q}.*"),
            _ => "expr".to_string(),
        }
    }

    /// Resolve column references using a schema and return the type.
    pub fn resolve_type(&self, schema: &Schema) -> Option<DataType> {
        match self {
            Self::Literal(v) => Some(value_to_data_type(v)),
            Self::TypeLiteral(dt) => Some(dt.clone()),
            Self::Column(name) => schema
                .columns
                .iter()
                .find(|c| c.name == *name)
                .map(|c| c.data_type.clone()),
            Self::QualifiedColumn { qualifier, name } => schema
                .columns
                .iter()
                .find(|c| c.qualifier.as_deref() == Some(qualifier.as_str()) && c.name == *name)
                .map(|c| c.data_type.clone()),
            Self::Binary { left, op, right } => {
                let left_type = left.resolve_type(schema)?;
                let right_type = right.resolve_type(schema)?;
                op.result_type(&left_type, &right_type)
            }
            Self::Unary { op, expr } => {
                let input_type = expr.resolve_type(schema)?;
                op.result_type(&input_type)
            }
            Self::Aggregate(agg) => {
                let input_type = agg.expr.resolve_type(schema)?;
                agg.result_type(&input_type)
            }
            Self::Alias { expr, .. } | Self::SortKey { expr, .. } => expr.resolve_type(schema),
            Self::Case {
                when_clauses,
                else_result,
                ..
            } => {
                // Result type is the common type of all branches
                let types: Vec<_> = when_clauses
                    .iter()
                    .filter_map(|(_, result)| result.resolve_type(schema))
                    .chain(else_result.as_ref().and_then(|e| e.resolve_type(schema)))
                    .collect();

                if types.is_empty() {
                    return None;
                }

                types
                    .into_iter()
                    .reduce(|a, b| a.common_supertype(&b).unwrap_or(a))
            }
            Self::InList { .. }
            | Self::Between { .. }
            | Self::Subquery(_)
            | Self::Exists { .. } => Some(DataType::Bool),
            Self::Function(_) | Self::Wildcard | Self::QualifiedWildcard(_) => None, // Would need function registry
            Self::Placeholder { data_type, .. } => data_type.clone(),
        }
    }
}

/// Convert a Value to its corresponding `DataType`.
fn value_to_data_type(value: &Value) -> DataType {
    match value {
        Value::Null => DataType::Null,
        Value::Bool(_) => DataType::Bool,
        Value::Int64(_) => DataType::Int64,
        Value::Float64(_) => DataType::Float64,
        Value::String(_) => DataType::String,
        Value::Binary(_) => DataType::Binary,
        Value::Vector(v) => DataType::Vector(v.len()),
        Value::Symbol(_) => DataType::Symbol,
        Value::Timestamp(_) => DataType::Timestamp,
        Value::Date(_) => DataType::Date,
        Value::Array(arr) => arr.first().map_or_else(
            || DataType::Array(Box::new(DataType::Null)),
            |first| DataType::Array(Box::new(value_to_data_type(first))),
        ),
        Value::Map(_) => DataType::Map(Box::new(DataType::Null)), // Simplified
    }
}

impl std::fmt::Display for LogicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Literal(v) => write!(f, "{v:?}"),
            Self::TypeLiteral(dt) => write!(f, "{dt}"),
            Self::Column(name) => write!(f, "{name}"),
            Self::QualifiedColumn { qualifier, name } => write!(f, "{qualifier}.{name}"),
            Self::Binary { left, op, right } => write!(f, "({left} {op} {right})"),
            Self::Unary { op, expr } => {
                if matches!(op, UnaryOp::Not | UnaryOp::Neg) {
                    write!(f, "{op} {expr}")
                } else {
                    write!(f, "{expr} {op}")
                }
            }
            Self::Function(func) => write!(f, "{func}"),
            Self::Aggregate(agg) => write!(f, "{agg}"),
            Self::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                write!(f, "CASE")?;
                if let Some(op) = operand {
                    write!(f, " {op}")?;
                }
                for (cond, result) in when_clauses {
                    write!(f, " WHEN {cond} THEN {result}")?;
                }
                if let Some(else_expr) = else_result {
                    write!(f, " ELSE {else_expr}")?;
                }
                write!(f, " END")
            }
            Self::InList {
                expr,
                list,
                negated,
            } => {
                let not = if *negated { " NOT" } else { "" };
                let items = list
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{expr}{not} IN ({items})")
            }
            Self::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let not = if *negated { " NOT" } else { "" };
                write!(f, "{expr}{not} BETWEEN {low} AND {high}")
            }
            Self::Alias { expr, alias } => write!(f, "{expr} AS {alias}"),
            Self::Wildcard => write!(f, "*"),
            Self::QualifiedWildcard(q) => write!(f, "{q}.*"),
            Self::Subquery(_) => write!(f, "(subquery)"),
            Self::Exists { negated, .. } => {
                let not = if *negated { "NOT " } else { "" };
                write!(f, "{not}EXISTS (subquery)")
            }
            Self::Placeholder { index, .. } => write!(f, "${index}"),
            Self::SortKey {
                expr,
                ascending,
                nulls_first,
            } => {
                let dir = if *ascending { "ASC" } else { "DESC" };
                let nulls = if *nulls_first {
                    "NULLS FIRST"
                } else {
                    "NULLS LAST"
                };
                write!(f, "{expr} {dir} {nulls}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_creation() {
        let expr = LogicalExpr::literal(42i64);
        assert!(expr.is_literal());
        assert_eq!(expr.try_as_literal(), Some(&Value::Int64(42)));
    }

    #[test]
    fn test_column_creation() {
        let expr = LogicalExpr::column("name");
        assert!(expr.is_column());
        assert_eq!(expr.output_name(), "name");
    }

    #[test]
    fn test_binary_operations() {
        let left = LogicalExpr::column("a");
        let right = LogicalExpr::literal(5i64);
        let expr = left.gt(right);

        assert!(expr.is_deterministic());
        let refs = expr.column_refs();
        assert!(refs.contains("a"));
    }

    #[test]
    fn test_logical_operations() {
        let a = LogicalExpr::column("x").gt(LogicalExpr::literal(10i64));
        let b = LogicalExpr::column("y").lt(LogicalExpr::literal(20i64));
        let combined = a.and(b);

        assert_eq!(
            combined.to_string(),
            "((x > Int64(10)) AND (y < Int64(20)))"
        );
    }

    #[test]
    fn test_column_refs_collection() {
        let expr = LogicalExpr::column("a")
            .add_expr(LogicalExpr::column("b"))
            .mul_expr(LogicalExpr::column("c"));

        let refs = expr.column_refs();
        assert_eq!(refs.len(), 3);
        assert!(refs.contains("a"));
        assert!(refs.contains("b"));
        assert!(refs.contains("c"));
    }

    #[test]
    fn test_contains_aggregate() {
        let simple = LogicalExpr::column("x").add_expr(LogicalExpr::literal(1i64));
        assert!(!simple.contains_aggregate());

        let agg = LogicalExpr::aggregate(AggExpr::sum(LogicalExpr::column("amount")));
        assert!(agg.contains_aggregate());
    }

    #[test]
    fn test_alias() {
        let expr = LogicalExpr::column("price")
            .mul_expr(LogicalExpr::column("quantity"))
            .alias("total");

        assert_eq!(expr.output_name(), "total");
    }

    #[test]
    fn test_determinism() {
        let deterministic = LogicalExpr::column("x").add_expr(LogicalExpr::literal(1i64));
        assert!(deterministic.is_deterministic());
    }

    #[test]
    fn test_display() {
        let expr = LogicalExpr::column("age").gt_eq(LogicalExpr::literal(18i64));
        assert_eq!(expr.to_string(), "(age >= Int64(18))");

        let expr = LogicalExpr::column("name").is_null();
        assert_eq!(expr.to_string(), "name IS NULL");
    }
}
