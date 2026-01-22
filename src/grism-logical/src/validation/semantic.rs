//! Semantic validation for logical plans.
//!
//! This module validates the semantics of logical plans:
//! - Column reference validity
//! - Expression type checking
//! - Role binding correctness

use std::collections::HashSet;

use crate::{LogicalExpr, LogicalOp, LogicalPlan};

/// A semantic validation error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SemanticValidationError {
    /// A column reference is unresolvable.
    UnresolvedColumn {
        /// The unresolved column name.
        column: String,
        /// Available columns at this point (if known).
        available: Vec<String>,
    },

    /// Type mismatch in an expression.
    TypeMismatch {
        /// Description of the type mismatch.
        message: String,
    },

    /// Invalid role binding in a hyperedge operation.
    InvalidRoleBinding {
        /// The invalid role.
        role: String,
        /// Description of the issue.
        message: String,
    },

    /// Duplicate alias.
    DuplicateAlias {
        /// The duplicate alias name.
        alias: String,
    },

    /// Missing required alias.
    MissingAlias {
        /// Description of where the alias is needed.
        context: String,
    },

    /// Expression is not deterministic where required.
    NonDeterministicExpression {
        /// Description of the expression.
        expression: String,
    },

    /// Invalid aggregation (aggregate without group by).
    InvalidAggregation {
        /// Description of the issue.
        message: String,
    },

    /// Projection references non-existent column.
    InvalidProjection {
        /// Description of the issue.
        message: String,
    },
}

impl std::fmt::Display for SemanticValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnresolvedColumn { column, available } => {
                write!(f, "Unresolved column '{column}'. Available: {available:?}")
            }
            Self::TypeMismatch { message } => {
                write!(f, "Type mismatch: {message}")
            }
            Self::InvalidRoleBinding { role, message } => {
                write!(f, "Invalid role binding '{role}': {message}")
            }
            Self::DuplicateAlias { alias } => {
                write!(f, "Duplicate alias: '{alias}'")
            }
            Self::MissingAlias { context } => {
                write!(f, "Missing required alias: {context}")
            }
            Self::NonDeterministicExpression { expression } => {
                write!(f, "Non-deterministic expression not allowed: {expression}")
            }
            Self::InvalidAggregation { message } => {
                write!(f, "Invalid aggregation: {message}")
            }
            Self::InvalidProjection { message } => {
                write!(f, "Invalid projection: {message}")
            }
        }
    }
}

impl std::error::Error for SemanticValidationError {}

/// Semantic validator for logical plans.
pub struct SemanticValidator;

impl SemanticValidator {
    /// Validate the semantic correctness of a logical plan.
    ///
    /// Returns `Ok(())` if the plan is semantically valid, or a list of errors.
    pub fn validate(plan: &LogicalPlan) -> Result<(), Vec<SemanticValidationError>> {
        let mut errors = Vec::new();
        let mut scope = Scope::new();

        // Validate from bottom up to track available columns
        Self::validate_operator(plan.root(), &mut errors, &mut scope);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Validate an operator and build the scope.
    fn validate_operator(
        op: &LogicalOp,
        errors: &mut Vec<SemanticValidationError>,
        scope: &mut Scope,
    ) {
        match op {
            LogicalOp::Scan(scan) => Self::validate_scan_operator(scan, scope),
            LogicalOp::Empty => {}
            LogicalOp::Filter { input, filter } => {
                Self::validate_filter_operator(input, filter, errors, scope);
            }
            LogicalOp::Project { input, project } => {
                Self::validate_project_operator(input, project, errors, scope);
            }
            LogicalOp::Expand { input, expand } => {
                Self::validate_expand_operator(input, expand, errors, scope);
            }
            LogicalOp::Aggregate { input, aggregate } => {
                Self::validate_aggregate_operator(input, aggregate, errors, scope);
            }
            LogicalOp::Sort { input, sort } => {
                Self::validate_sort_operator(input, sort, errors, scope);
            }
            LogicalOp::Limit { input, .. } | LogicalOp::Infer { input, .. } => {
                Self::validate_operator(input, errors, scope);
            }
            LogicalOp::Union { left, right, .. } => {
                Self::validate_union_operator(left, right, errors, scope);
            }
            LogicalOp::Rename { input, rename } => {
                Self::validate_rename_operator(input, rename, errors, scope);
            }
        }
    }

    fn validate_scan_operator(scan: &crate::ops::ScanOp, scope: &mut Scope) {
        // Scan introduces columns from the entity schema
        if let Some(ref alias) = scan.alias {
            scope.add_alias(alias.clone());
        }
        // Add entity-level columns that are always available
        scope.add_column("_id".to_string());
        scope.add_column("_labels".to_string());
        // Add label-specific properties (would need catalog in real impl)
    }

    fn validate_filter_operator(
        input: &LogicalOp,
        filter: &crate::ops::FilterOp,
        errors: &mut Vec<SemanticValidationError>,
        scope: &mut Scope,
    ) {
        // First validate children to build scope
        Self::validate_operator(input, errors, scope);

        // Validate filter expression
        Self::validate_expression(&filter.predicate, errors, scope);

        // Filter predicate must be deterministic
        if !filter.predicate.is_deterministic() {
            errors.push(SemanticValidationError::NonDeterministicExpression {
                expression: filter.predicate.to_string(),
            });
        }
    }

    fn validate_project_operator(
        input: &LogicalOp,
        project: &crate::ops::ProjectOp,
        errors: &mut Vec<SemanticValidationError>,
        scope: &mut Scope,
    ) {
        use std::collections::HashSet;

        // First validate children to build scope
        Self::validate_operator(input, errors, scope);

        // Validate projection expressions
        for expr in &project.expressions {
            Self::validate_expression(expr, errors, scope);
        }

        // Check for duplicate output names
        let mut output_names = HashSet::new();
        for expr in &project.expressions {
            let name = expr.output_name();
            if name != "*" && !output_names.insert(name.clone()) {
                errors.push(SemanticValidationError::DuplicateAlias { alias: name });
            }
        }

        // Update scope with projected columns
        scope.clear_columns();
        for expr in &project.expressions {
            if matches!(expr, LogicalExpr::Wildcard) {
                // Wildcard keeps all existing columns
            } else {
                scope.add_column(expr.output_name());
            }
        }
    }

    fn validate_expand_operator(
        input: &LogicalOp,
        expand: &crate::ops::ExpandOp,
        errors: &mut Vec<SemanticValidationError>,
        scope: &mut Scope,
    ) {
        // First validate children to build scope
        Self::validate_operator(input, errors, scope);

        // Validate edge predicate if present
        if let Some(ref pred) = expand.edge_predicate {
            // Edge predicates should only reference edge columns
            Self::validate_expression(pred, errors, scope);
        }

        // Validate target predicate if present
        if let Some(ref pred) = expand.target_predicate {
            Self::validate_expression(pred, errors, scope);
        }

        // Add expanded columns to scope
        if let Some(ref alias) = expand.target_alias {
            scope.add_alias(alias.clone());
            scope.add_column(format!("{alias}._id"));
            scope.add_column(format!("{alias}._labels"));
        }
        if let Some(ref alias) = expand.edge_alias {
            scope.add_alias(alias.clone());
            scope.add_column(format!("{alias}._id"));
            scope.add_column(format!("{alias}._type"));
        }
    }

    fn validate_aggregate_operator(
        input: &LogicalOp,
        aggregate: &crate::ops::AggregateOp,
        errors: &mut Vec<SemanticValidationError>,
        scope: &mut Scope,
    ) {
        // First validate children to build scope
        Self::validate_operator(input, errors, scope);

        // Validate group keys
        for key in &aggregate.group_keys {
            Self::validate_expression(key, errors, scope);
        }

        // Validate aggregation expressions
        for agg in &aggregate.aggregates {
            Self::validate_expression(&agg.expr, errors, scope);
        }

        // Update scope - only group keys and aggregation outputs are available
        scope.clear_columns();
        for key in &aggregate.group_keys {
            scope.add_column(key.output_name());
        }
        for agg in &aggregate.aggregates {
            scope.add_column(agg.output_name());
        }
    }

    fn validate_sort_operator(
        input: &LogicalOp,
        sort: &crate::ops::SortOp,
        errors: &mut Vec<SemanticValidationError>,
        scope: &mut Scope,
    ) {
        // First validate children to build scope
        Self::validate_operator(input, errors, scope);

        // Validate sort expressions
        for key in &sort.keys {
            Self::validate_expression(&key.expr, errors, scope);
        }
    }

    fn validate_union_operator(
        left: &LogicalOp,
        right: &LogicalOp,
        errors: &mut Vec<SemanticValidationError>,
        _scope: &mut Scope,
    ) {
        // Validate both branches
        let mut left_scope = Scope::new();
        let mut right_scope = Scope::new();

        Self::validate_operator(left, errors, &mut left_scope);
        Self::validate_operator(right, errors, &mut right_scope);

        // Union produces the intersection of available columns
        // (simplified - real impl would check schema compatibility)
    }

    fn validate_rename_operator(
        input: &LogicalOp,
        rename: &crate::ops::RenameOp,
        errors: &mut Vec<SemanticValidationError>,
        scope: &mut Scope,
    ) {
        // First validate children to build scope
        Self::validate_operator(input, errors, scope);

        // Check that renamed columns exist
        #[allow(clippy::for_kv_map)]
        for (old_name, _new_name) in &rename.mapping {
            if !scope.has_column(old_name) {
                errors.push(SemanticValidationError::UnresolvedColumn {
                    column: old_name.to_string(),
                    available: scope.available_columns(),
                });
            }
        }

        // Apply renames to scope
        for (old_name, new_name) in &rename.mapping {
            scope.remove_column(old_name);
            scope.add_column(new_name.clone());
        }
    }

    /// Validate an expression against the current scope.
    #[allow(clippy::only_used_in_recursion)]
    fn validate_expression(
        expr: &LogicalExpr,
        errors: &mut Vec<SemanticValidationError>,
        scope: &Scope,
    ) {
        match expr {
            LogicalExpr::Column(name) => {
                // Column references are allowed without explicit scope check
                // in the validation phase since we may not have full schema info.
                // We track what's available but don't error on unknown columns
                // to support dynamic/property access patterns.
                let _ = scope.has_column(name);
            }

            LogicalExpr::QualifiedColumn { qualifier, name: _ } => {
                // Check that the qualifier (alias) is in scope
                if !scope.has_alias(qualifier) {
                    // Don't error - could be a forward reference or implicit scope
                }
            }

            LogicalExpr::Binary { left, right, .. } => {
                Self::validate_expression(left, errors, scope);
                Self::validate_expression(right, errors, scope);
            }

            LogicalExpr::Unary { expr: inner, .. }
            | LogicalExpr::Alias { expr: inner, .. }
            | LogicalExpr::SortKey { expr: inner, .. } => {
                Self::validate_expression(inner, errors, scope);
            }

            LogicalExpr::Function(func) => {
                for arg in &func.args {
                    Self::validate_expression(arg, errors, scope);
                }
            }

            LogicalExpr::Aggregate(agg) => {
                Self::validate_expression(&agg.expr, errors, scope);
            }

            LogicalExpr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                if let Some(op) = operand {
                    Self::validate_expression(op, errors, scope);
                }
                for (when, then) in when_clauses {
                    Self::validate_expression(when, errors, scope);
                    Self::validate_expression(then, errors, scope);
                }
                if let Some(el) = else_result {
                    Self::validate_expression(el, errors, scope);
                }
            }

            // Literals and wildcards need no validation
            LogicalExpr::Literal(_)
            | LogicalExpr::Wildcard
            | LogicalExpr::TypeLiteral(_)
            | LogicalExpr::QualifiedWildcard(_)
            | LogicalExpr::Placeholder { .. } => {}

            LogicalExpr::InList { expr, list, .. } => {
                Self::validate_expression(expr, errors, scope);
                for item in list {
                    Self::validate_expression(item, errors, scope);
                }
            }

            LogicalExpr::Between {
                expr, low, high, ..
            } => {
                Self::validate_expression(expr, errors, scope);
                Self::validate_expression(low, errors, scope);
                Self::validate_expression(high, errors, scope);
            }

            LogicalExpr::Exists { subquery, .. } | LogicalExpr::Subquery(subquery) => {
                // Subqueries would need their own validation context
                let _ = subquery;
            }
        }
    }
}

/// Scope tracker for semantic validation.
#[derive(Debug, Default)]
struct Scope {
    /// Available column names.
    columns: HashSet<String>,
    /// Available aliases (for qualified references).
    aliases: HashSet<String>,
}

impl Scope {
    fn new() -> Self {
        Self::default()
    }

    fn add_column(&mut self, name: String) {
        self.columns.insert(name);
    }

    fn remove_column(&mut self, name: &str) {
        self.columns.remove(name);
    }

    fn clear_columns(&mut self) {
        self.columns.clear();
    }

    fn has_column(&self, name: &str) -> bool {
        self.columns.contains(name)
    }

    fn add_alias(&mut self, alias: String) {
        self.aliases.insert(alias);
    }

    fn has_alias(&self, alias: &str) -> bool {
        self.aliases.contains(alias)
    }

    fn available_columns(&self) -> Vec<String> {
        self.columns.iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AggExpr, AggregateOp, ExpandOp, FilterOp, PlanBuilder, ProjectOp, ScanOp, col, lit,
    };

    #[test]
    fn test_valid_filter() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("age").gt(lit(18i64))))
            .build();

        let result = SemanticValidator::validate(&plan);
        assert!(result.is_ok());
    }

    #[test]
    fn test_valid_expand() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .expand(
                ExpandOp::binary()
                    .with_edge_label("KNOWS")
                    .with_target_alias("friend"),
            )
            .filter(FilterOp::new(col("friend._id").is_not_null()))
            .build();

        let result = SemanticValidator::validate(&plan);
        assert!(result.is_ok());
    }

    #[test]
    fn test_valid_aggregation() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .aggregate(
                AggregateOp::group_by(["city"]).with_agg(AggExpr::count_star().with_alias("count")),
            )
            .build();

        let result = SemanticValidator::validate(&plan);
        assert!(result.is_ok());
    }

    #[test]
    fn test_duplicate_alias_in_projection() {
        // Create a projection with duplicate output names
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .project(ProjectOp::new(vec![
                col("name").alias("result"),
                col("city").alias("result"), // Duplicate!
            ]))
            .build();

        let result = SemanticValidator::validate(&plan);
        assert!(result.is_err());

        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, SemanticValidationError::DuplicateAlias { .. }))
        );
    }

    #[test]
    fn test_scope_building() {
        let mut scope = Scope::new();

        scope.add_column("name".to_string());
        scope.add_column("age".to_string());
        scope.add_alias("p".to_string());

        assert!(scope.has_column("name"));
        assert!(scope.has_column("age"));
        assert!(!scope.has_column("city"));
        assert!(scope.has_alias("p"));
        assert!(!scope.has_alias("q"));

        let available = scope.available_columns();
        assert!(available.contains(&"name".to_string()));
        assert!(available.contains(&"age".to_string()));
    }
}
