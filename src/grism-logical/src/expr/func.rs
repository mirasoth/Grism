//! Function expressions for logical planning (RFC-0003 compliant).

use grism_core::DataType;
use serde::{Deserialize, Serialize};

use super::LogicalExpr;

/// Function category for classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FuncCategory {
    /// Row-wise scalar function.
    Scalar,
    /// Boolean-valued predicate.
    Predicate,
    /// Vector similarity and distance functions.
    Vector,
    /// Type casting and conversion.
    TypeCast,
    /// String manipulation.
    String,
    /// Math functions.
    Math,
    /// Date/time functions.
    DateTime,
    /// Utility functions.
    Utility,
}

/// Determinism classification for functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Determinism {
    /// Always returns same result for same inputs.
    Deterministic,
    /// Result depends on snapshot (e.g., CURRENT_TIMESTAMP).
    Stable,
    /// Non-deterministic (e.g., RANDOM()).
    Volatile,
}

impl Default for Determinism {
    fn default() -> Self {
        Self::Deterministic
    }
}

/// Null behavior classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NullBehavior {
    /// Returns NULL if any argument is NULL.
    NullIfNull,
    /// Function handles NULLs specially (e.g., COALESCE).
    NullAware,
    /// Ignores NULL values.
    IgnoreNull,
}

impl Default for NullBehavior {
    fn default() -> Self {
        Self::NullIfNull
    }
}

/// Built-in scalar function types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BuiltinFunc {
    // String functions
    /// Get string length
    Length,
    /// Convert to uppercase
    Upper,
    /// Convert to lowercase
    Lower,
    /// Trim whitespace
    Trim,
    /// Left trim
    LTrim,
    /// Right trim
    RTrim,
    /// Substring extraction
    Substring,
    /// String replacement
    Replace,
    /// Pattern matching (LIKE)
    Like,
    /// Pattern matching (regex)
    RegexMatch,
    /// String concatenation (variadic)
    Concat,
    /// String starts with
    StartsWith,
    /// String ends with
    EndsWith,
    /// String contains
    Contains,
    /// String split
    Split,

    // Math functions
    /// Absolute value
    Abs,
    /// Ceiling
    Ceil,
    /// Floor
    Floor,
    /// Round to n decimal places
    Round,
    /// Square root
    Sqrt,
    /// Natural logarithm
    Ln,
    /// Logarithm base 10
    Log10,
    /// Power function
    Pow,
    /// Exponential function
    Exp,
    /// Sign function
    Sign,

    // Type casting
    /// Cast to type
    Cast,
    /// Try cast (returns NULL on failure)
    TryCast,
    /// Type of value
    TypeOf,

    // Null handling
    /// Return first non-null value
    Coalesce,
    /// Return NULL if values are equal
    NullIf,
    /// Return default if NULL
    IfNull,

    // Date/time functions
    /// Get current date
    CurrentDate,
    /// Get current timestamp
    CurrentTimestamp,
    /// Extract date part
    DatePart,
    /// Date difference
    DateDiff,
    /// Add interval to date
    DateAdd,
    /// Truncate date
    DateTrunc,

    // Vector functions (AI-native)
    /// Cosine similarity
    CosineSimilarity,
    /// Euclidean distance
    EuclideanDistance,
    /// Inner product
    InnerProduct,
    /// L2 norm
    L2Norm,
    /// Vector dimension
    VectorDim,

    // Graph functions
    /// Get node ID
    Id,
    /// Get node/edge labels
    Labels,
    /// Get hyperedge type
    Type,
    /// Get properties as map
    Properties,
    /// Check if entity exists
    Exists,
    /// Count of elements
    Size,
}

impl BuiltinFunc {
    /// Get function name.
    pub fn name(&self) -> &'static str {
        match self {
            // String functions
            Self::Length => "LENGTH",
            Self::Upper => "UPPER",
            Self::Lower => "LOWER",
            Self::Trim => "TRIM",
            Self::LTrim => "LTRIM",
            Self::RTrim => "RTRIM",
            Self::Substring => "SUBSTRING",
            Self::Replace => "REPLACE",
            Self::Like => "LIKE",
            Self::RegexMatch => "REGEX_MATCH",
            Self::Concat => "CONCAT",
            Self::StartsWith => "STARTS_WITH",
            Self::EndsWith => "ENDS_WITH",
            Self::Contains => "CONTAINS",
            Self::Split => "SPLIT",

            // Math functions
            Self::Abs => "ABS",
            Self::Ceil => "CEIL",
            Self::Floor => "FLOOR",
            Self::Round => "ROUND",
            Self::Sqrt => "SQRT",
            Self::Ln => "LN",
            Self::Log10 => "LOG10",
            Self::Pow => "POW",
            Self::Exp => "EXP",
            Self::Sign => "SIGN",

            // Type casting
            Self::Cast => "CAST",
            Self::TryCast => "TRY_CAST",
            Self::TypeOf => "TYPEOF",

            // Null handling
            Self::Coalesce => "COALESCE",
            Self::NullIf => "NULLIF",
            Self::IfNull => "IFNULL",

            // Date/time
            Self::CurrentDate => "CURRENT_DATE",
            Self::CurrentTimestamp => "CURRENT_TIMESTAMP",
            Self::DatePart => "DATE_PART",
            Self::DateDiff => "DATE_DIFF",
            Self::DateAdd => "DATE_ADD",
            Self::DateTrunc => "DATE_TRUNC",

            // Vector functions
            Self::CosineSimilarity => "COSINE_SIMILARITY",
            Self::EuclideanDistance => "EUCLIDEAN_DISTANCE",
            Self::InnerProduct => "INNER_PRODUCT",
            Self::L2Norm => "L2_NORM",
            Self::VectorDim => "VECTOR_DIM",

            // Graph functions
            Self::Id => "ID",
            Self::Labels => "LABELS",
            Self::Type => "TYPE",
            Self::Properties => "PROPERTIES",
            Self::Exists => "EXISTS",
            Self::Size => "SIZE",
        }
    }

    /// Get function category.
    pub fn category(&self) -> FuncCategory {
        match self {
            Self::Length
            | Self::Upper
            | Self::Lower
            | Self::Trim
            | Self::LTrim
            | Self::RTrim
            | Self::Substring
            | Self::Replace
            | Self::Concat
            | Self::StartsWith
            | Self::EndsWith
            | Self::Contains
            | Self::Split => FuncCategory::String,

            Self::Like | Self::RegexMatch | Self::Exists => FuncCategory::Predicate,

            Self::Abs
            | Self::Ceil
            | Self::Floor
            | Self::Round
            | Self::Sqrt
            | Self::Ln
            | Self::Log10
            | Self::Pow
            | Self::Exp
            | Self::Sign => FuncCategory::Math,

            Self::Cast | Self::TryCast | Self::TypeOf => FuncCategory::TypeCast,

            Self::Coalesce | Self::NullIf | Self::IfNull => FuncCategory::Scalar,

            Self::CurrentDate
            | Self::CurrentTimestamp
            | Self::DatePart
            | Self::DateDiff
            | Self::DateAdd
            | Self::DateTrunc => FuncCategory::DateTime,

            Self::CosineSimilarity
            | Self::EuclideanDistance
            | Self::InnerProduct
            | Self::L2Norm
            | Self::VectorDim => FuncCategory::Vector,

            Self::Id | Self::Labels | Self::Type | Self::Properties | Self::Size => {
                FuncCategory::Utility
            }
        }
    }

    /// Get function determinism.
    pub fn determinism(&self) -> Determinism {
        match self {
            Self::CurrentDate | Self::CurrentTimestamp => Determinism::Stable,
            _ => Determinism::Deterministic,
        }
    }

    /// Get null behavior.
    pub fn null_behavior(&self) -> NullBehavior {
        match self {
            Self::Coalesce | Self::NullIf | Self::IfNull => NullBehavior::NullAware,
            _ => NullBehavior::NullIfNull,
        }
    }
}

impl std::fmt::Display for BuiltinFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// A function call expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FuncExpr {
    /// Function identifier (builtin or user-defined name).
    pub func: FuncKind,
    /// Function arguments.
    pub args: Vec<LogicalExpr>,
}

/// Kind of function (builtin or user-defined).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FuncKind {
    /// A built-in function.
    Builtin(BuiltinFunc),
    /// A user-defined function by name.
    UserDefined(String),
}

impl FuncExpr {
    /// Create a new builtin function call.
    pub fn builtin(func: BuiltinFunc, args: Vec<LogicalExpr>) -> Self {
        Self {
            func: FuncKind::Builtin(func),
            args,
        }
    }

    /// Create a new user-defined function call.
    pub fn udf(name: impl Into<String>, args: Vec<LogicalExpr>) -> Self {
        Self {
            func: FuncKind::UserDefined(name.into()),
            args,
        }
    }

    /// Get the function name.
    pub fn name(&self) -> String {
        match &self.func {
            FuncKind::Builtin(f) => f.name().to_string(),
            FuncKind::UserDefined(name) => name.clone(),
        }
    }

    /// Get determinism (assumes UDFs are deterministic unless registered otherwise).
    pub fn determinism(&self) -> Determinism {
        match &self.func {
            FuncKind::Builtin(f) => f.determinism(),
            FuncKind::UserDefined(_) => Determinism::Deterministic, // Default assumption
        }
    }
}

impl std::fmt::Display for FuncExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let args = self
            .args
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{}({})", self.name(), args)
    }
}

// Convenient constructors for common functions
impl FuncExpr {
    /// Create CAST(expr AS type).
    pub fn cast(expr: LogicalExpr, target_type: DataType) -> Self {
        Self::builtin(
            BuiltinFunc::Cast,
            vec![expr, LogicalExpr::TypeLiteral(target_type)],
        )
    }

    /// Create COALESCE(exprs...).
    pub fn coalesce(exprs: Vec<LogicalExpr>) -> Self {
        Self::builtin(BuiltinFunc::Coalesce, exprs)
    }

    /// Create COSINE_SIMILARITY(vec1, vec2).
    pub fn cosine_similarity(vec1: LogicalExpr, vec2: LogicalExpr) -> Self {
        Self::builtin(BuiltinFunc::CosineSimilarity, vec![vec1, vec2])
    }

    /// Create LENGTH(str).
    pub fn length(expr: LogicalExpr) -> Self {
        Self::builtin(BuiltinFunc::Length, vec![expr])
    }

    /// Create UPPER(str).
    pub fn upper(expr: LogicalExpr) -> Self {
        Self::builtin(BuiltinFunc::Upper, vec![expr])
    }

    /// Create LOWER(str).
    pub fn lower(expr: LogicalExpr) -> Self {
        Self::builtin(BuiltinFunc::Lower, vec![expr])
    }

    /// Create ID(entity).
    pub fn id(expr: LogicalExpr) -> Self {
        Self::builtin(BuiltinFunc::Id, vec![expr])
    }

    /// Create LABELS(node).
    pub fn labels(expr: LogicalExpr) -> Self {
        Self::builtin(BuiltinFunc::Labels, vec![expr])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_core::Value;

    #[test]
    fn test_builtin_func_names() {
        assert_eq!(BuiltinFunc::Length.name(), "LENGTH");
        assert_eq!(BuiltinFunc::CosineSimilarity.name(), "COSINE_SIMILARITY");
    }

    #[test]
    fn test_builtin_func_categories() {
        assert_eq!(BuiltinFunc::Length.category(), FuncCategory::String);
        assert_eq!(BuiltinFunc::Abs.category(), FuncCategory::Math);
        assert_eq!(
            BuiltinFunc::CosineSimilarity.category(),
            FuncCategory::Vector
        );
    }

    #[test]
    fn test_builtin_func_determinism() {
        assert_eq!(
            BuiltinFunc::Length.determinism(),
            Determinism::Deterministic
        );
        assert_eq!(
            BuiltinFunc::CurrentTimestamp.determinism(),
            Determinism::Stable
        );
    }

    #[test]
    fn test_func_expr_display() {
        let func = FuncExpr::length(LogicalExpr::Column("name".to_string()));
        assert_eq!(func.to_string(), "LENGTH(name)");

        let func = FuncExpr::coalesce(vec![
            LogicalExpr::Column("a".to_string()),
            LogicalExpr::Literal(Value::Int64(0)),
        ]);
        assert_eq!(func.to_string(), "COALESCE(a, Int64(0))");
    }
}
