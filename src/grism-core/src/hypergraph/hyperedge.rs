//! N-ary hyperedge representation.

use serde::{Deserialize, Serialize};

use super::identifiers::{EntityRef, ROLE_SOURCE, ROLE_TARGET, new_edge_id};
use super::properties::HasProperties;
use super::{EdgeId, Label, NodeId, PropertyMap, Role};

/// A binding between a role and an entity (node or hyperedge).
///
/// Role bindings are the fundamental building blocks of hyperedges, allowing
/// hyperedges to connect entities through explicitly named roles.
///
/// # Example
///
/// ```rust
/// use grism_core::{EntityRef, hypergraph::RoleBinding};
///
/// let binding = RoleBinding::new("author", EntityRef::Node(42));
/// assert_eq!(binding.role(), "author");
/// assert!(binding.target().is_node());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RoleBinding {
    /// The semantic role name.
    role: Role,
    /// The target entity (node or hyperedge).
    target: EntityRef,
}

impl RoleBinding {
    /// Create a new role binding.
    pub fn new(role: impl Into<Role>, target: EntityRef) -> Self {
        Self {
            role: role.into(),
            target,
        }
    }

    /// Create a role binding to a node.
    pub fn to_node(role: impl Into<Role>, node_id: NodeId) -> Self {
        Self::new(role, EntityRef::Node(node_id))
    }

    /// Create a role binding to a hyperedge.
    pub fn to_hyperedge(role: impl Into<Role>, edge_id: EdgeId) -> Self {
        Self::new(role, EntityRef::Hyperedge(edge_id))
    }

    /// Get the role name.
    pub fn role(&self) -> &str {
        &self.role
    }

    /// Get the target entity reference.
    pub const fn target(&self) -> &EntityRef {
        &self.target
    }

    /// Get the target node ID if this binding points to a node.
    pub const fn target_node(&self) -> Option<NodeId> {
        self.target.as_node()
    }

    /// Get the target edge ID if this binding points to a hyperedge.
    pub const fn target_hyperedge(&self) -> Option<EdgeId> {
        self.target.as_hyperedge()
    }
}

/// An n-ary hyperedge connecting multiple entities with named roles.
///
/// A hyperedge is the fundamental relational primitive in Grism. It represents
/// an n-ary relation whose participants are bound through named roles. Binary
/// edges (traditional graph edges) are a special case with arity = 2.
///
/// # Architecture
///
/// Per the Grism architecture design:
/// - Hyperedges are the sole relational primitive
/// - Binary edges are hyperedges with arity = 2 and roles {source, target}
/// - Hyperedges can reference other hyperedges (enabling meta-relations)
/// - Roles are explicitly named, unordered, and semantically meaningful
///
/// # Invariants
///
/// - Arity must be >= 2 (at least two role bindings)
/// - Roles are explicitly named and semantically meaningful
///
/// # Example
///
/// ```rust
/// use grism_core::Hyperedge;
///
/// let edge = Hyperedge::new("AUTHORED_BY")
///     .with_node(1, "paper")
///     .with_node(2, "author");
///
/// assert_eq!(edge.arity(), 2);
/// assert!(edge.is_binary());
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hyperedge {
    /// Unique hyperedge identifier.
    pub id: EdgeId,
    /// Hyperedge label/type.
    pub label: Label,
    /// Role bindings connecting entities.
    pub bindings: Vec<RoleBinding>,
    /// Hyperedge properties.
    pub properties: PropertyMap,
}

impl Hyperedge {
    // =========================================================================
    // Construction
    // =========================================================================

    /// Create a new hyperedge with generated ID.
    pub fn new(label: impl Into<Label>) -> Self {
        Self {
            id: new_edge_id(),
            label: label.into(),
            bindings: Vec::new(),
            properties: PropertyMap::new(),
        }
    }

    /// Create a new hyperedge with a specific ID.
    pub fn with_id(id: EdgeId, label: impl Into<Label>) -> Self {
        Self {
            id,
            label: label.into(),
            bindings: Vec::new(),
            properties: PropertyMap::new(),
        }
    }

    // =========================================================================
    // Building: Adding Bindings
    // =========================================================================

    /// Add a binding to any entity (node or hyperedge) with a role.
    ///
    /// This is the generic method that works with any `EntityRef`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use grism_core::{Hyperedge, EntityRef};
    ///
    /// let edge = Hyperedge::new("RELATION")
    ///     .with_binding(EntityRef::Node(1), "source")
    ///     .with_binding(EntityRef::Hyperedge(42), "evidence");
    /// ```
    #[must_use]
    pub fn with_binding(mut self, entity: EntityRef, role: impl Into<Role>) -> Self {
        self.bindings.push(RoleBinding::new(role, entity));
        self
    }

    /// Add a node binding with a role.
    ///
    /// This is the most common way to build hyperedges, connecting nodes.
    ///
    /// # Example
    ///
    /// ```rust
    /// use grism_core::Hyperedge;
    ///
    /// let edge = Hyperedge::new("KNOWS")
    ///     .with_node(1, "source")
    ///     .with_node(2, "target");
    /// ```
    #[must_use]
    pub fn with_node(mut self, node_id: NodeId, role: impl Into<Role>) -> Self {
        self.bindings.push(RoleBinding::to_node(role, node_id));
        self
    }

    /// Add a hyperedge binding with a role.
    ///
    /// This enables hyperedge-to-hyperedge relations (meta-relations) for:
    /// - Provenance and justification
    /// - Causal and temporal dependencies
    /// - Rule application and inference confidence
    ///
    /// # Example
    ///
    /// ```rust
    /// use grism_core::Hyperedge;
    ///
    /// let meta = Hyperedge::new("INFERRED_BY")
    ///     .with_hyperedge(42, "conclusion")
    ///     .with_node(100, "rule");
    /// ```
    #[must_use]
    pub fn with_hyperedge(mut self, edge_id: EdgeId, role: impl Into<Role>) -> Self {
        self.bindings.push(RoleBinding::to_hyperedge(role, edge_id));
        self
    }

    /// Add multiple node bindings.
    ///
    /// # Example
    ///
    /// ```rust
    /// use grism_core::Hyperedge;
    ///
    /// let edge = Hyperedge::new("MEETING")
    ///     .with_nodes([(1, "host"), (2, "attendee"), (3, "attendee")]);
    /// ```
    #[must_use]
    pub fn with_nodes(
        mut self,
        nodes: impl IntoIterator<Item = (NodeId, impl Into<Role>)>,
    ) -> Self {
        for (node_id, role) in nodes {
            self.bindings.push(RoleBinding::to_node(role, node_id));
        }
        self
    }

    /// Add properties to this hyperedge.
    #[must_use]
    pub fn with_properties(mut self, properties: PropertyMap) -> Self {
        self.properties = properties;
        self
    }

    // =========================================================================
    // Arity and Structure
    // =========================================================================

    /// Get the arity (number of role bindings).
    pub const fn arity(&self) -> usize {
        self.bindings.len()
    }

    /// Check if this is a binary edge (arity == 2).
    pub const fn is_binary(&self) -> bool {
        self.arity() == 2
    }

    /// Check if this hyperedge has standard binary edge roles (source and target).
    pub fn has_binary_roles(&self) -> bool {
        if self.arity() != 2 {
            return false;
        }
        let roles: Vec<&str> = self.bindings.iter().map(RoleBinding::role).collect();
        roles.contains(&ROLE_SOURCE) && roles.contains(&ROLE_TARGET)
    }

    /// Get all role bindings.
    pub fn bindings(&self) -> &[RoleBinding] {
        &self.bindings
    }

    /// Get all unique roles in this hyperedge.
    pub fn roles(&self) -> Vec<&str> {
        let mut roles: Vec<&str> = self.bindings.iter().map(RoleBinding::role).collect();
        roles.sort_unstable();
        roles.dedup();
        roles
    }

    // =========================================================================
    // Checking Involvement
    // =========================================================================

    /// Check if this hyperedge involves a specific entity (node or hyperedge).
    ///
    /// This is the generic method that works with any `EntityRef`.
    pub fn involves_entity(&self, entity: &EntityRef) -> bool {
        self.bindings.iter().any(|b| b.target() == entity)
    }

    /// Check if this hyperedge involves a specific node.
    pub fn involves_node(&self, node_id: NodeId) -> bool {
        self.bindings
            .iter()
            .any(|b| b.target_node() == Some(node_id))
    }

    /// Check if this hyperedge involves a specific hyperedge (meta-relation).
    pub fn involves_hyperedge(&self, edge_id: EdgeId) -> bool {
        self.bindings
            .iter()
            .any(|b| b.target_hyperedge() == Some(edge_id))
    }

    // =========================================================================
    // Getting Involved Entities
    // =========================================================================

    /// Get all entity references involved in this hyperedge.
    pub fn involved_entities(&self) -> Vec<&EntityRef> {
        self.bindings.iter().map(RoleBinding::target).collect()
    }

    /// Get all node IDs involved in this hyperedge.
    pub fn involved_nodes(&self) -> Vec<NodeId> {
        self.bindings
            .iter()
            .filter_map(RoleBinding::target_node)
            .collect()
    }

    /// Get all hyperedge IDs involved in this hyperedge (for meta-relations).
    pub fn involved_hyperedges(&self) -> Vec<EdgeId> {
        self.bindings
            .iter()
            .filter_map(RoleBinding::target_hyperedge)
            .collect()
    }

    // =========================================================================
    // Getting Entities by Role
    // =========================================================================

    /// Get all entity references with a specific role.
    pub fn entities_with_role(&self, role: &str) -> Vec<&EntityRef> {
        self.bindings
            .iter()
            .filter(|b| b.role() == role)
            .map(RoleBinding::target)
            .collect()
    }

    /// Get all node IDs with a specific role.
    pub fn nodes_with_role(&self, role: &str) -> Vec<NodeId> {
        self.bindings
            .iter()
            .filter(|b| b.role() == role)
            .filter_map(RoleBinding::target_node)
            .collect()
    }

    /// Get all hyperedge IDs with a specific role.
    pub fn hyperedges_with_role(&self, role: &str) -> Vec<EdgeId> {
        self.bindings
            .iter()
            .filter(|b| b.role() == role)
            .filter_map(RoleBinding::target_hyperedge)
            .collect()
    }

    /// Get the role of a node in this hyperedge.
    ///
    /// Returns the first role found for the given node ID.
    pub fn role_of_node(&self, node_id: NodeId) -> Option<&Role> {
        self.bindings.iter().find_map(|b| {
            if b.target_node() == Some(node_id) {
                Some(&b.role)
            } else {
                None
            }
        })
    }

    /// Get the role of an entity in this hyperedge.
    ///
    /// Returns the first role found for the given entity.
    pub fn role_of(&self, entity: &EntityRef) -> Option<&Role> {
        self.bindings.iter().find_map(|b| {
            if b.target() == entity {
                Some(&b.role)
            } else {
                None
            }
        })
    }

    // =========================================================================
    // Conversion
    // =========================================================================

    /// Convert to a binary edge if arity is 2 and has source/target roles.
    ///
    /// Returns None if:
    /// - Arity is not 2
    /// - Roles are not {source, target}
    /// - Any binding points to a hyperedge instead of a node
    pub fn to_binary_edge(&self) -> Option<super::Edge> {
        if !self.has_binary_roles() {
            return None;
        }

        let source = self
            .bindings
            .iter()
            .find(|b| b.role() == ROLE_SOURCE)?
            .target_node()?;
        let target = self
            .bindings
            .iter()
            .find(|b| b.role() == ROLE_TARGET)?
            .target_node()?;

        Some(super::Edge {
            id: self.id,
            label: self.label.clone(),
            source,
            target,
            properties: self.properties.clone(),
        })
    }
}

impl HasProperties for Hyperedge {
    fn properties(&self) -> &PropertyMap {
        &self.properties
    }

    fn properties_mut(&mut self) -> &mut PropertyMap {
        &mut self.properties
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_binding_creation() {
        let binding = RoleBinding::to_node("author", 42);
        assert_eq!(binding.role(), "author");
        assert_eq!(binding.target_node(), Some(42));
        assert_eq!(binding.target_hyperedge(), None);
    }

    #[test]
    fn test_role_binding_to_hyperedge() {
        let binding = RoleBinding::to_hyperedge("conclusion", 99);
        assert_eq!(binding.role(), "conclusion");
        assert_eq!(binding.target_node(), None);
        assert_eq!(binding.target_hyperedge(), Some(99));
    }

    #[test]
    fn test_hyperedge_creation() {
        let edge = Hyperedge::new("Event")
            .with_node(1, "participant")
            .with_node(2, "participant")
            .with_node(3, "location");

        assert_eq!(edge.arity(), 3);
        assert!(!edge.is_binary());
        assert!(edge.involves_node(1));
        assert!(!edge.involves_node(99));
    }

    #[test]
    fn test_hyperedge_with_nodes() {
        let edge =
            Hyperedge::new("Meeting").with_nodes([(1, "host"), (2, "attendee"), (3, "attendee")]);

        assert_eq!(edge.arity(), 3);
        assert!(edge.involves_node(1));
        assert!(edge.involves_node(2));
        assert!(edge.involves_node(3));
    }

    #[test]
    fn test_hyperedge_roles() {
        let edge = Hyperedge::new("Meeting")
            .with_node(1, "host")
            .with_node(2, "attendee")
            .with_node(3, "attendee");

        assert_eq!(edge.role_of_node(1), Some(&"host".to_string()));
        let mut attendees = edge.nodes_with_role("attendee");
        attendees.sort();
        assert_eq!(attendees, vec![2, 3]);
    }

    #[test]
    fn test_binary_conversion() {
        let binary = Hyperedge::new("KNOWS")
            .with_node(1, "source")
            .with_node(2, "target");

        assert!(binary.is_binary());
        assert!(binary.has_binary_roles());

        let edge = binary.to_binary_edge().unwrap();
        assert_eq!(edge.source, 1);
        assert_eq!(edge.target, 2);
    }

    #[test]
    fn test_binary_conversion_fails_without_standard_roles() {
        let edge = Hyperedge::new("AUTHORED_BY")
            .with_node(1, "paper")
            .with_node(2, "author");

        assert!(edge.is_binary());
        assert!(!edge.has_binary_roles());
        assert!(edge.to_binary_edge().is_none());
    }

    #[test]
    fn test_hyperedge_to_hyperedge_relation() {
        // Create a meta-relation: INFERRED_BY connecting a conclusion edge to a rule
        let conclusion_edge_id = 42;
        let rule_node_id = 100;

        let meta_edge = Hyperedge::new("INFERRED_BY")
            .with_hyperedge(conclusion_edge_id, "conclusion")
            .with_node(rule_node_id, "rule");

        assert_eq!(meta_edge.arity(), 2);
        assert!(meta_edge.involves_hyperedge(conclusion_edge_id));
        assert!(meta_edge.involves_node(rule_node_id));
        assert!(!meta_edge.involves_node(conclusion_edge_id as NodeId)); // Different type

        let involved_edges = meta_edge.involved_hyperedges();
        assert_eq!(involved_edges, vec![conclusion_edge_id]);

        let involved_nodes = meta_edge.involved_nodes();
        assert_eq!(involved_nodes, vec![rule_node_id]);
    }

    #[test]
    fn test_unique_roles() {
        let edge = Hyperedge::new("Event")
            .with_node(1, "participant")
            .with_node(2, "participant")
            .with_node(3, "location");

        let roles = edge.roles();
        assert_eq!(roles.len(), 2); // "location" and "participant" (deduplicated)
        assert!(roles.contains(&"participant"));
        assert!(roles.contains(&"location"));
    }

    #[test]
    fn test_entities_with_role() {
        let edge = Hyperedge::new("CAUSES")
            .with_hyperedge(10, "cause")
            .with_hyperedge(20, "effect");

        let causes = edge.entities_with_role("cause");
        assert_eq!(causes.len(), 1);
        assert!(causes[0].is_hyperedge());

        let effects = edge.entities_with_role("effect");
        assert_eq!(effects.len(), 1);
        assert!(effects[0].is_hyperedge());
    }

    #[test]
    fn test_hyperedges_with_role() {
        let edge = Hyperedge::new("CAUSES")
            .with_hyperedge(10, "cause")
            .with_hyperedge(20, "effect")
            .with_node(1, "witness");

        let causes = edge.hyperedges_with_role("cause");
        assert_eq!(causes, vec![10]);

        let effects = edge.hyperedges_with_role("effect");
        assert_eq!(effects, vec![20]);

        // Node should not appear in hyperedges_with_role
        let witnesses = edge.hyperedges_with_role("witness");
        assert!(witnesses.is_empty());
    }

    #[test]
    fn test_involves_entity() {
        let edge = Hyperedge::new("RELATION")
            .with_node(1, "source")
            .with_hyperedge(42, "evidence");

        assert!(edge.involves_entity(&EntityRef::Node(1)));
        assert!(edge.involves_entity(&EntityRef::Hyperedge(42)));
        assert!(!edge.involves_entity(&EntityRef::Node(99)));
    }

    #[test]
    fn test_involved_entities() {
        let edge = Hyperedge::new("RELATION")
            .with_node(1, "source")
            .with_hyperedge(42, "evidence");

        let entities = edge.involved_entities();
        assert_eq!(entities.len(), 2);
        assert!(entities.contains(&&EntityRef::Node(1)));
        assert!(entities.contains(&&EntityRef::Hyperedge(42)));
    }

    #[test]
    fn test_with_binding_generic() {
        let edge = Hyperedge::new("MIXED")
            .with_binding(EntityRef::Node(1), "node_role")
            .with_binding(EntityRef::Hyperedge(42), "edge_role");

        assert_eq!(edge.arity(), 2);
        assert!(edge.involves_node(1));
        assert!(edge.involves_hyperedge(42));
    }

    #[test]
    fn test_role_of_entity() {
        let edge = Hyperedge::new("RELATION")
            .with_node(1, "source")
            .with_hyperedge(42, "evidence");

        assert_eq!(
            edge.role_of(&EntityRef::Node(1)),
            Some(&"source".to_string())
        );
        assert_eq!(
            edge.role_of(&EntityRef::Hyperedge(42)),
            Some(&"evidence".to_string())
        );
        assert_eq!(edge.role_of(&EntityRef::Node(99)), None);
    }
}
