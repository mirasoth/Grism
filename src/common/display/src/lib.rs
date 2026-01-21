//! Display and visualization utilities for Grism.
//!
//! Provides formatting for query plans, schemas, and results.

mod tree;

pub use tree::{DisplayTree, TreeNode};

/// Format a value for display with optional truncation.
pub fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

/// Indent a multi-line string.
pub fn indent(s: &str, prefix: &str) -> String {
    s.lines()
        .map(|line| format!("{prefix}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}
