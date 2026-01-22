//! Infer operator for logical planning (RFC-0002 compliant).
//!
//! Infer applies declarative rules to derive new hyperedges.

use serde::{Deserialize, Serialize};

/// Inference mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum InferMode {
    /// Standard inference.
    #[default]
    Standard,
    /// Fixpoint iteration until no new facts.
    Fixpoint,
    /// Bounded iterations.
    Bounded(u32),
}

impl std::fmt::Display for InferMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Standard => write!(f, "STANDARD"),
            Self::Fixpoint => write!(f, "FIXPOINT"),
            Self::Bounded(n) => write!(f, "BOUNDED({})", n),
        }
    }
}

/// Infer operator - declarative rule application.
///
/// # Semantics (RFC-0002, Section 6.8)
///
/// - Applies declarative rules
/// - May introduce new hyperedges
///
/// # Rules
///
/// - Infer MUST be monotonic unless explicitly stated
/// - Fixpoint semantics MAY apply (defined in RFC-0013)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferOp {
    /// Name of the ruleset to apply.
    pub ruleset: String,

    /// Inference mode.
    pub mode: InferMode,

    /// Optional parameters for the ruleset.
    pub parameters: std::collections::HashMap<String, String>,
}

impl InferOp {
    /// Create a new inference operation.
    pub fn new(ruleset: impl Into<String>) -> Self {
        Self {
            ruleset: ruleset.into(),
            mode: InferMode::Standard,
            parameters: std::collections::HashMap::new(),
        }
    }

    /// Set inference mode.
    #[must_use]
    pub const fn with_mode(mut self, mode: InferMode) -> Self {
        self.mode = mode;
        self
    }

    /// Use fixpoint iteration.
    #[must_use]
    pub const fn fixpoint(mut self) -> Self {
        self.mode = InferMode::Fixpoint;
        self
    }

    /// Use bounded iterations.
    #[must_use]
    pub const fn bounded(mut self, iterations: u32) -> Self {
        self.mode = InferMode::Bounded(iterations);
        self
    }

    /// Add a parameter.
    #[must_use]
    pub fn with_param(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.parameters.insert(key.into(), value.into());
        self
    }
}

impl std::fmt::Display for InferOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Infer(ruleset={}, mode={})", self.ruleset, self.mode)?;

        if !self.parameters.is_empty() {
            let params = self
                .parameters
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(", ");
            write!(f, ", params=[{}]", params)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_creation() {
        let infer = InferOp::new("transitivity")
            .fixpoint()
            .with_param("max_depth", "3");

        assert_eq!(infer.ruleset, "transitivity");
        assert_eq!(infer.mode, InferMode::Fixpoint);
        assert_eq!(infer.parameters.get("max_depth"), Some(&"3".to_string()));
    }

    #[test]
    fn test_infer_bounded() {
        let infer = InferOp::new("rules").bounded(10);
        assert_eq!(infer.mode, InferMode::Bounded(10));
    }

    #[test]
    fn test_infer_display() {
        let infer = InferOp::new("ontology").fixpoint();
        assert!(infer.to_string().contains("ontology"));
        assert!(infer.to_string().contains("FIXPOINT"));
    }
}
