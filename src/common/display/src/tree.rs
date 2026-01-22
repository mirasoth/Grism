//! Tree display utilities for query plans.

use std::fmt;

/// A node in a display tree.
pub trait TreeNode {
    /// Get the display name of this node.
    fn name(&self) -> &str;

    /// Get child nodes.
    fn children(&self) -> Vec<&dyn TreeNode>;

    /// Get additional details to display.
    fn details(&self) -> Option<String> {
        None
    }
}

/// Helper for displaying tree structures.
pub struct DisplayTree<'a> {
    root: &'a dyn TreeNode,
    #[allow(dead_code)]
    indent: usize,
}

impl<'a> DisplayTree<'a> {
    /// Create a new display tree.
    pub fn new(root: &'a dyn TreeNode) -> Self {
        Self { root, indent: 0 }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn fmt_node(
        &self,
        f: &mut fmt::Formatter<'_>,
        node: &dyn TreeNode,
        prefix: &str,
        is_last: bool,
    ) -> fmt::Result {
        let connector = if is_last { "└─ " } else { "├─ " };

        write!(f, "{prefix}{connector}{}", node.name())?;

        if let Some(details) = node.details() {
            write!(f, " ({details})")?;
        }
        writeln!(f)?;

        let children = node.children();
        let child_prefix = format!("{prefix}{}", if is_last { "   " } else { "│  " });

        for (i, child) in children.iter().enumerate() {
            let is_last_child = i == children.len() - 1;
            self.fmt_node(f, *child, &child_prefix, is_last_child)?;
        }

        Ok(())
    }
}

impl fmt::Display for DisplayTree<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", self.root.name())?;

        let children = self.root.children();
        for (i, child) in children.iter().enumerate() {
            let is_last = i == children.len() - 1;
            self.fmt_node(f, *child, "", is_last)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestNode {
        name: String,
        children: Vec<TestNode>,
    }

    impl TreeNode for TestNode {
        fn name(&self) -> &str {
            &self.name
        }

        fn children(&self) -> Vec<&dyn TreeNode> {
            self.children.iter().map(|c| c as &dyn TreeNode).collect()
        }
    }

    #[test]
    fn test_display_tree() {
        let tree = TestNode {
            name: "Root".to_string(),
            children: vec![
                TestNode {
                    name: "Child1".to_string(),
                    children: vec![],
                },
                TestNode {
                    name: "Child2".to_string(),
                    children: vec![],
                },
            ],
        };

        let display = DisplayTree::new(&tree);
        let output = display.to_string();
        assert!(output.contains("Root"));
        assert!(output.contains("Child1"));
        assert!(output.contains("Child2"));
    }
}
