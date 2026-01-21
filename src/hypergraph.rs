use std::fmt;

/// Stable identifier for nodes in the HyperGraph.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct NodeId(u32);

impl NodeId {
    /// Returns the zero-based index of the node.
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// Stable identifier for edges in the HyperGraph.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct EdgeId(u32);

impl EdgeId {
    /// Returns the zero-based index of the edge.
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphError {
    InvalidNodeId { id: NodeId },
    InvalidEdgeId { id: EdgeId },
    EmptyEdge,
    InconsistentIncidence { node: NodeId, edge: EdgeId },
}

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GraphError::InvalidNodeId { id } => write!(f, "invalid node id: {}", id.index()),
            GraphError::InvalidEdgeId { id } => write!(f, "invalid edge id: {}", id.index()),
            GraphError::EmptyEdge => write!(f, "edge must contain at least one node"),
            GraphError::InconsistentIncidence { node, edge } => write!(
                f,
                "inconsistent incidence between node {} and edge {}",
                node.index(),
                edge.index()
            ),
        }
    }
}

impl std::error::Error for GraphError {}

#[derive(Debug, Clone)]
struct NodeEntry<N> {
    data: N,
    edges: Vec<EdgeId>,
}

#[derive(Debug, Clone)]
struct EdgeEntry<E> {
    data: E,
    nodes: Vec<NodeId>,
}

/// HyperGraph stores nodes, hyperedges, and their incidence lists.
#[derive(Debug, Clone)]
pub struct HyperGraph<N, E> {
    nodes: Vec<NodeEntry<N>>,
    edges: Vec<EdgeEntry<E>>,
}

impl<N, E> Default for HyperGraph<N, E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<N, E> HyperGraph<N, E> {
    /// Creates an empty HyperGraph.
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// Creates an empty HyperGraph with reserved capacity.
    pub fn with_capacity(node_capacity: usize, edge_capacity: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(node_capacity),
            edges: Vec::with_capacity(edge_capacity),
        }
    }

    /// Returns the number of nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Returns the number of edges.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Adds a node and returns its identifier.
    pub fn add_node(&mut self, data: N) -> NodeId {
        let id = NodeId(self.nodes.len() as u32);
        self.nodes.push(NodeEntry {
            data,
            edges: Vec::new(),
        });
        id
    }

    /// Adds a hyperedge connecting the provided nodes.
    pub fn add_edge<I>(&mut self, nodes: I, data: E) -> Result<EdgeId, GraphError>
    where
        I: IntoIterator<Item = NodeId>,
    {
        let mut unique_nodes = Vec::new();
        let mut seen = vec![false; self.nodes.len()];
        for node_id in nodes {
            let index = node_id.index();
            if index >= self.nodes.len() {
                return Err(GraphError::InvalidNodeId { id: node_id });
            }
            if !seen[index] {
                seen[index] = true;
                unique_nodes.push(node_id);
            }
        }

        if unique_nodes.is_empty() {
            return Err(GraphError::EmptyEdge);
        }

        let edge_id = EdgeId(self.edges.len() as u32);
        for node_id in &unique_nodes {
            self.nodes[node_id.index()].edges.push(edge_id);
        }

        self.edges.push(EdgeEntry {
            data,
            nodes: unique_nodes,
        });

        Ok(edge_id)
    }

    /// Returns a reference to a node's data.
    pub fn node(&self, id: NodeId) -> Option<&N> {
        self.nodes.get(id.index()).map(|entry| &entry.data)
    }

    /// Returns a mutable reference to a node's data.
    pub fn node_mut(&mut self, id: NodeId) -> Option<&mut N> {
        self.nodes.get_mut(id.index()).map(|entry| &mut entry.data)
    }

    /// Returns a reference to an edge's data.
    pub fn edge(&self, id: EdgeId) -> Option<&E> {
        self.edges.get(id.index()).map(|entry| &entry.data)
    }

    /// Returns a mutable reference to an edge's data.
    pub fn edge_mut(&mut self, id: EdgeId) -> Option<&mut E> {
        self.edges.get_mut(id.index()).map(|entry| &mut entry.data)
    }

    /// Returns the nodes attached to an edge.
    pub fn nodes_for_edge(&self, id: EdgeId) -> Option<&[NodeId]> {
        self.edges.get(id.index()).map(|entry| entry.nodes.as_slice())
    }

    /// Returns the edges incident to a node.
    pub fn edges_for_node(&self, id: NodeId) -> Option<&[EdgeId]> {
        self.nodes.get(id.index()).map(|entry| entry.edges.as_slice())
    }

    /// Returns all neighbor nodes that share at least one edge.
    pub fn neighbors(&self, id: NodeId) -> Option<Vec<NodeId>> {
        let node_index = id.index();
        let node = self.nodes.get(node_index)?;
        let mut seen = vec![false; self.nodes.len()];
        seen[node_index] = true;

        let mut neighbors = Vec::new();
        for edge_id in &node.edges {
            if let Some(edge) = self.edges.get(edge_id.index()) {
                for &other in &edge.nodes {
                    let other_index = other.index();
                    if !seen[other_index] {
                        seen[other_index] = true;
                        neighbors.push(other);
                    }
                }
            }
        }

        neighbors.sort_unstable();
        Some(neighbors)
    }

    /// Optimizes adjacency lists for deterministic traversal and compactness.
    pub fn optimize(&mut self) {
        for edge in &mut self.edges {
            edge.nodes.sort_unstable();
            edge.nodes.dedup();
        }

        for node in &mut self.nodes {
            node.edges.sort_unstable();
            node.edges.dedup();
        }
    }

    /// Validates incidence symmetry and edge constraints.
    pub fn validate(&self) -> Result<(), GraphError> {
        for (edge_index, edge) in self.edges.iter().enumerate() {
            if edge.nodes.is_empty() {
                return Err(GraphError::EmptyEdge);
            }
            let edge_id = EdgeId(edge_index as u32);
            for &node_id in &edge.nodes {
                if node_id.index() >= self.nodes.len() {
                    return Err(GraphError::InvalidNodeId { id: node_id });
                }
                let node = &self.nodes[node_id.index()];
                if !node.edges.contains(&edge_id) {
                    return Err(GraphError::InconsistentIncidence {
                        node: node_id,
                        edge: edge_id,
                    });
                }
            }
        }

        for (node_index, node) in self.nodes.iter().enumerate() {
            let node_id = NodeId(node_index as u32);
            for &edge_id in &node.edges {
                if edge_id.index() >= self.edges.len() {
                    return Err(GraphError::InvalidEdgeId { id: edge_id });
                }
                let edge = &self.edges[edge_id.index()];
                if !edge.nodes.contains(&node_id) {
                    return Err(GraphError::InconsistentIncidence {
                        node: node_id,
                        edge: edge_id,
                    });
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_nodes_edges_and_neighbors() {
        let mut graph = HyperGraph::new();
        let a = graph.add_node("alpha");
        let b = graph.add_node("beta");
        let c = graph.add_node("gamma");

        let edge = graph.add_edge([a, b, c], "link").expect("edge");

        assert_eq!(graph.nodes_for_edge(edge).unwrap(), &[a, b, c]);
        assert_eq!(graph.edges_for_node(a).unwrap(), &[edge]);

        let mut neighbors = graph.neighbors(a).unwrap();
        neighbors.sort_unstable();
        assert_eq!(neighbors, vec![b, c]);
    }

    #[test]
    fn optimize_sorts_edge_nodes() {
        let mut graph = HyperGraph::new();
        let n0 = graph.add_node(0);
        let n1 = graph.add_node(1);
        let n2 = graph.add_node(2);

        let edge = graph.add_edge([n2, n0, n1], ()).expect("edge");
        assert_eq!(graph.nodes_for_edge(edge).unwrap(), &[n2, n0, n1]);

        graph.optimize();
        assert_eq!(graph.nodes_for_edge(edge).unwrap(), &[n0, n1, n2]);
    }
}
