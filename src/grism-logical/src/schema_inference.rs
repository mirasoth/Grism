//! Schema inference for logical operators.
//!
//! This module provides schema inference capabilities for logical plans,
//! determining the output schema based on input schemas and operator semantics.

use grism_core::{ColumnInfo, DataType, Schema};

use crate::{LogicalExpr, LogicalOp, LogicalPlan};

/// Error during schema inference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaInferenceError {
    /// Column not found in input schema.
    ColumnNotFound {
        column: String,
        available: Vec<String>,
    },
    /// Type mismatch.
    TypeMismatch { message: String },
    /// Cannot infer schema (missing information).
    CannotInfer { reason: String },
    /// Incompatible schemas in union.
    IncompatibleSchemas { message: String },
}

impl std::fmt::Display for SchemaInferenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ColumnNotFound { column, available } => {
                write!(f, "Column '{column}' not found. Available: {available:?}")
            }
            Self::TypeMismatch { message } => write!(f, "Type mismatch: {message}"),
            Self::CannotInfer { reason } => write!(f, "Cannot infer schema: {reason}"),
            Self::IncompatibleSchemas { message } => write!(f, "Incompatible schemas: {message}"),
        }
    }
}

impl std::error::Error for SchemaInferenceError {}

/// Schema inference for logical operators.
pub struct SchemaInference;

impl SchemaInference {
    /// Infer the output schema of a logical plan.
    pub fn infer_plan(plan: &LogicalPlan) -> Result<Schema, SchemaInferenceError> {
        Self::infer_op(plan.root())
    }

    /// Infer the output schema of a logical operator.
    pub fn infer_op(op: &LogicalOp) -> Result<Schema, SchemaInferenceError> {
        match op {
            LogicalOp::Scan(scan) => Ok(Self::infer_scan(scan)),
            LogicalOp::Empty => Ok(Schema::empty()),
            LogicalOp::Filter { input, .. } => {
                // Filter doesn't change schema
                Self::infer_op(input)
            }
            LogicalOp::Project { input, project } => {
                let input_schema = Self::infer_op(input)?;
                Self::infer_project(&input_schema, &project.expressions)
            }
            LogicalOp::Expand { input, expand } => {
                let input_schema = Self::infer_op(input)?;
                Ok(Self::infer_expand(&input_schema, expand))
            }
            LogicalOp::Aggregate { input, aggregate } => {
                let input_schema = Self::infer_op(input)?;
                Self::infer_aggregate(&input_schema, aggregate)
            }
            LogicalOp::Sort { input, .. } => {
                // Sort doesn't change schema
                Self::infer_op(input)
            }
            LogicalOp::Limit { input, .. } => {
                // Limit doesn't change schema
                Self::infer_op(input)
            }
            LogicalOp::Union { left, right, .. } => {
                let left_schema = Self::infer_op(left)?;
                let right_schema = Self::infer_op(right)?;
                Ok(Self::infer_union(&left_schema, &right_schema))
            }
            LogicalOp::Rename { input, rename } => {
                let input_schema = Self::infer_op(input)?;
                Ok(Self::infer_rename(&input_schema, &rename.mapping))
            }
            LogicalOp::Infer { input, .. } => {
                // For now, pass through - infer rules could add columns
                Self::infer_op(input)
            }
        }
    }

    /// Infer schema for a scan operation.
    fn infer_scan(scan: &crate::ScanOp) -> Schema {
        let mut schema = Schema::empty();

        // Determine qualifier (alias or label)
        let qualifier = scan.alias.as_ref().or(scan.label.as_ref());

        // Add standard entity columns
        schema.add_column(ColumnInfo::new("_id", DataType::String).with_nullable(false));

        // Add label-specific standard columns based on scan kind
        match scan.kind {
            crate::ScanKind::Node => {
                schema.add_column(ColumnInfo::new(
                    "_labels",
                    DataType::Array(Box::new(DataType::String)),
                ));
            }
            crate::ScanKind::Edge | crate::ScanKind::Hyperedge => {
                schema.add_column(ColumnInfo::new("_type", DataType::String));
            }
        }

        // Apply qualifier to all columns
        if let Some(q) = qualifier {
            for col in &mut schema.columns {
                col.qualifier = Some(q.clone());
            }
        }

        // If specific columns are projected, add those with String type as default
        for col_name in &scan.projection {
            schema.add_column(
                ColumnInfo::new(col_name.clone(), DataType::String)
                    .with_qualifier(qualifier.cloned().unwrap_or_default()),
            );
        }

        schema
    }

    /// Infer schema for a project operation.
    fn infer_project(
        input_schema: &Schema,
        expressions: &[LogicalExpr],
    ) -> Result<Schema, SchemaInferenceError> {
        let mut schema = Schema::empty();

        for expr in expressions {
            match expr {
                LogicalExpr::Wildcard => {
                    // Include all columns from input
                    for col in &input_schema.columns {
                        schema.add_column(col.clone());
                    }
                }
                LogicalExpr::QualifiedWildcard(qualifier) => {
                    // Include all columns matching the qualifier
                    for col in &input_schema.columns {
                        if col.qualifier.as_ref() == Some(qualifier) {
                            schema.add_column(col.clone());
                        }
                    }
                }
                _ => {
                    // Infer column info from expression
                    let col_info = Self::infer_expression_type(expr, input_schema)?;
                    schema.add_column(col_info);
                }
            }
        }

        Ok(schema)
    }

    /// Infer schema for an expand operation.
    fn infer_expand(input_schema: &Schema, expand: &crate::ExpandOp) -> Schema {
        let mut schema = input_schema.clone();

        // Add target entity columns
        if let Some(ref alias) = expand.target_alias {
            schema.add_column(
                ColumnInfo::new("_id", DataType::String)
                    .with_qualifier(alias.clone())
                    .with_nullable(false),
            );
            schema.add_column(
                ColumnInfo::new("_labels", DataType::Array(Box::new(DataType::String)))
                    .with_qualifier(alias.clone()),
            );
        }

        // Add edge columns
        if let Some(ref alias) = expand.edge_alias {
            schema.add_column(
                ColumnInfo::new("_id", DataType::String)
                    .with_qualifier(alias.clone())
                    .with_nullable(false),
            );
            schema.add_column(
                ColumnInfo::new("_type", DataType::String).with_qualifier(alias.clone()),
            );
        }

        // If path is included, add path column
        if expand.include_path {
            schema.add_column(ColumnInfo::new(
                "_path",
                DataType::Array(Box::new(DataType::String)),
            ));
        }

        schema
    }

    /// Infer schema for an aggregate operation.
    fn infer_aggregate(
        input_schema: &Schema,
        aggregate: &crate::AggregateOp,
    ) -> Result<Schema, SchemaInferenceError> {
        let mut schema = Schema::empty();

        // Add group key columns
        for key in &aggregate.group_keys {
            let col_info = Self::infer_expression_type(key, input_schema)?;
            schema.add_column(col_info);
        }

        // Add aggregation result columns
        for agg in &aggregate.aggregates {
            let output_type = Self::infer_agg_type(agg.func.clone());
            let name = agg.output_name();
            schema.add_column(ColumnInfo::new(name, output_type));
        }

        Ok(schema)
    }

    /// Infer schema for a union operation.
    fn infer_union(left_schema: &Schema, _right_schema: &Schema) -> Schema {
        // Union takes schema from left branch
        // In a real implementation, we'd verify compatibility
        left_schema.clone()
    }

    /// Infer schema for a rename operation.
    fn infer_rename(
        input_schema: &Schema,
        mapping: &std::collections::HashMap<String, String>,
    ) -> Schema {
        let mut schema = Schema::empty();

        for col in &input_schema.columns {
            let mut new_col = col.clone();
            if let Some(new_name) = mapping.get(&col.name) {
                new_col.name.clone_from(new_name);
            }
            schema.add_column(new_col);
        }

        schema
    }

    /// Infer the type of an expression.
    fn infer_expression_type(
        expr: &LogicalExpr,
        input_schema: &Schema,
    ) -> Result<ColumnInfo, SchemaInferenceError> {
        match expr {
            LogicalExpr::Column(name) => {
                // Find column in input schema
                for col in &input_schema.columns {
                    if col.name == *name || col.qualified_name() == *name {
                        return Ok(col.clone());
                    }
                }
                // Column not found in schema - return with String type as fallback
                Ok(ColumnInfo::new(name.clone(), DataType::String))
            }
            LogicalExpr::QualifiedColumn { qualifier, name } => {
                // Find qualified column
                for col in &input_schema.columns {
                    if col.qualifier.as_ref() == Some(qualifier) && col.name == *name {
                        return Ok(col.clone());
                    }
                }
                Ok(ColumnInfo::new(name.clone(), DataType::String)
                    .with_qualifier(qualifier.clone()))
            }
            LogicalExpr::Literal(value) => {
                let dtype = match value {
                    grism_core::Value::Null => DataType::Null,
                    grism_core::Value::Bool(_) => DataType::Bool,
                    grism_core::Value::Int64(_) => DataType::Int64,
                    grism_core::Value::Float64(_) => DataType::Float64,
                    grism_core::Value::String(_) => DataType::String,
                    grism_core::Value::Binary(_) => DataType::Binary,
                    grism_core::Value::Array(_) => DataType::Array(Box::new(DataType::String)),
                    grism_core::Value::Map(_) => DataType::Map(Box::new(DataType::String)),
                    grism_core::Value::Vector(v) => DataType::Vector(v.len()),
                    grism_core::Value::Timestamp(_) => DataType::Timestamp,
                    grism_core::Value::Date(_) => DataType::Date,
                    grism_core::Value::Symbol(_) => DataType::Symbol,
                };
                Ok(ColumnInfo::new("_literal", dtype))
            }
            LogicalExpr::Alias { expr, alias } => {
                let mut col_info = Self::infer_expression_type(expr, input_schema)?;
                col_info.name.clone_from(alias);
                col_info.qualifier = None;
                Ok(col_info)
            }
            LogicalExpr::Binary { left, op, right } => {
                let left_type = Self::infer_expression_type(left, input_schema)?;
                let right_type = Self::infer_expression_type(right, input_schema)?;
                let result_type =
                    Self::infer_binary_op_type(*op, &left_type.data_type, &right_type.data_type);
                Ok(ColumnInfo::new(expr.output_name(), result_type))
            }
            LogicalExpr::Unary { op, expr: inner } => {
                let inner_type = Self::infer_expression_type(inner, input_schema)?;
                let result_type = Self::infer_unary_op_type(*op, &inner_type.data_type);
                Ok(ColumnInfo::new(expr.output_name(), result_type))
            }
            LogicalExpr::Function(func) => {
                let result_type = Self::infer_function_type(&func.func);
                Ok(ColumnInfo::new(expr.output_name(), result_type))
            }
            LogicalExpr::Aggregate(agg) => {
                let result_type = Self::infer_agg_type(agg.func.clone());
                Ok(ColumnInfo::new(agg.output_name(), result_type))
            }
            LogicalExpr::Case { else_result, .. } => {
                // Type is the type of the result expressions
                else_result.as_ref().map_or_else(
                    || Ok(ColumnInfo::new(expr.output_name(), DataType::String)),
                    |else_expr| Self::infer_expression_type(else_expr, input_schema),
                )
            }
            _ => {
                // Default to String for other expressions
                Ok(ColumnInfo::new(expr.output_name(), DataType::String))
            }
        }
    }

    /// Infer the result type of a binary operation.
    const fn infer_binary_op_type(
        op: crate::BinaryOp,
        _left: &DataType,
        _right: &DataType,
    ) -> DataType {
        use crate::BinaryOp;
        match op {
            // Comparison and logical operators return bool
            BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
            | BinaryOp::And
            | BinaryOp::Or
            | BinaryOp::IsDistinctFrom
            | BinaryOp::IsNotDistinctFrom => DataType::Bool,

            // Arithmetic operators - could be more precise based on input types
            BinaryOp::Add
            | BinaryOp::Subtract
            | BinaryOp::Multiply
            | BinaryOp::Divide
            | BinaryOp::Modulo => DataType::Float64,

            // String concat returns string
            BinaryOp::Concat => DataType::String,
        }
    }

    /// Infer the result type of a unary operation.
    fn infer_unary_op_type(op: crate::UnaryOp, inner: &DataType) -> DataType {
        use crate::UnaryOp;
        match op {
            UnaryOp::Not
            | UnaryOp::IsNull
            | UnaryOp::IsNotNull
            | UnaryOp::IsTrue
            | UnaryOp::IsFalse
            | UnaryOp::IsUnknown => DataType::Bool,
            UnaryOp::Neg => inner.clone(),
        }
    }

    /// Infer the result type of a function.
    fn infer_function_type(func: &crate::FuncKind) -> DataType {
        use crate::{BuiltinFunc, FuncKind};
        match func {
            FuncKind::Builtin(builtin) => match builtin {
                // String functions
                BuiltinFunc::Length | BuiltinFunc::Size => DataType::Int64,

                // Math and vector functions
                BuiltinFunc::Abs
                | BuiltinFunc::Ceil
                | BuiltinFunc::Floor
                | BuiltinFunc::Round
                | BuiltinFunc::Sqrt
                | BuiltinFunc::CosineSimilarity
                | BuiltinFunc::EuclideanDistance => DataType::Float64,

                // Boolean functions
                BuiltinFunc::Contains
                | BuiltinFunc::StartsWith
                | BuiltinFunc::EndsWith
                | BuiltinFunc::RegexMatch
                | BuiltinFunc::Like => DataType::Bool,

                // Graph functions
                BuiltinFunc::Labels => DataType::Array(Box::new(DataType::String)),
                BuiltinFunc::Properties => DataType::Map(Box::new(DataType::String)),

                // Default
                _ => DataType::String,
            },
            FuncKind::UserDefined(_) => DataType::String,
        }
    }

    /// Infer the result type of an aggregate function.
    fn infer_agg_type(func: crate::AggFunc) -> DataType {
        use crate::AggFunc;
        match func {
            AggFunc::Count | AggFunc::CountDistinct => DataType::Int64,
            AggFunc::Sum | AggFunc::Avg => DataType::Float64,
            AggFunc::Min | AggFunc::Max | AggFunc::First | AggFunc::Last => DataType::String, // Simplified
            AggFunc::Collect | AggFunc::CollectDistinct => {
                DataType::Array(Box::new(DataType::String))
            }
        }
    }
}

/// Extension trait for `LogicalOp` to add schema inference.
pub trait SchemaInfer {
    /// Infer the output schema of this operator.
    fn infer_schema(&self) -> Result<Schema, SchemaInferenceError>;
}

impl SchemaInfer for LogicalOp {
    fn infer_schema(&self) -> Result<Schema, SchemaInferenceError> {
        SchemaInference::infer_op(self)
    }
}

impl SchemaInfer for LogicalPlan {
    fn infer_schema(&self) -> Result<Schema, SchemaInferenceError> {
        SchemaInference::infer_plan(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AggExpr, AggregateOp, ExpandOp, FilterOp, PlanBuilder, ProjectOp, ScanOp, col, lit,
    };

    #[test]
    fn test_infer_scan_schema() {
        let scan = ScanOp::nodes_with_label("Person").with_alias("p");
        let plan = LogicalPlan::new(LogicalOp::scan(scan));

        let schema = plan.infer_schema().unwrap();
        assert!(schema.columns.iter().any(|c| c.name == "_id"));
        assert!(schema.columns.iter().any(|c| c.name == "_labels"));
    }

    #[test]
    fn test_infer_filter_preserves_schema() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("age").gt(lit(18i64))))
            .build();

        let schema = plan.infer_schema().unwrap();
        // Filter preserves input schema
        assert!(schema.columns.iter().any(|c| c.name == "_id"));
    }

    #[test]
    fn test_infer_project_schema() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .project(ProjectOp::new(vec![col("name"), col("age").alias("years")]))
            .build();

        let schema = plan.infer_schema().unwrap();
        assert!(schema.columns.iter().any(|c| c.name == "name"));
        assert!(schema.columns.iter().any(|c| c.name == "years"));
    }

    #[test]
    fn test_infer_expand_schema() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .expand(
                ExpandOp::binary()
                    .with_edge_label("KNOWS")
                    .with_target_alias("friend")
                    .with_edge_alias("knows"),
            )
            .build();

        let schema = plan.infer_schema().unwrap();

        // Should have columns from both source and target
        assert!(
            schema.columns.iter().any(|c| {
                c.qualifier.as_ref() == Some(&"friend".to_string()) && c.name == "_id"
            })
        );
        assert!(
            schema.columns.iter().any(|c| {
                c.qualifier.as_ref() == Some(&"knows".to_string()) && c.name == "_type"
            })
        );
    }

    #[test]
    fn test_infer_aggregate_schema() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .aggregate(
                AggregateOp::group_by(["city"])
                    .with_agg(AggExpr::count_star().with_alias("count"))
                    .with_agg(AggExpr::avg(col("age")).with_alias("avg_age")),
            )
            .build();

        let schema = plan.infer_schema().unwrap();

        // Should have group key + aggregations
        assert!(schema.columns.iter().any(|c| c.name == "city"));
        assert!(schema.columns.iter().any(|c| c.name == "count"));
        assert!(schema.columns.iter().any(|c| c.name == "avg_age"));
    }
}
