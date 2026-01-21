# Grism Logical Data Structure Specification

## Preference: HyperGraph over HyperFrame

Grism uses **HyperGraph** as the canonical logical data structure. HyperFrame is
not used in this spec and should be considered deprecated for Grism modeling.

## Core Model

### Entities

- **Node**: A logical entity. Nodes are identified by stable `NodeId` values.
- **HyperEdge**: A relation that connects one or more nodes.
- **Incidence**: The membership relation between a node and a hyperedge.

### Identifiers

- **NodeId** and **EdgeId** are stable, dense identifiers (0..N-1).
- Identifiers are opaque to the user but can be converted to indexes.

## Storage Layout

The HyperGraph maintains two parallel index tables:

1. **Node Table**: `NodeEntry { data, edges: Vec<EdgeId> }`
2. **Edge Table**: `EdgeEntry { data, nodes: Vec<NodeId> }`

This makes incidence symmetric and allows O(1) access to adjacency lists.

## Invariants

1. Every hyperedge references at least one node.
2. For every `node -> edge` incidence, the inverse `edge -> node` incidence
   must exist.
3. Adjacency lists are free of duplicates after optimization.
4. Identifiers are stable for the lifetime of the graph.

## Operations

Required operations for Grism:

- `add_node(data) -> NodeId`
- `add_edge(nodes, data) -> EdgeId`
- `nodes_for_edge(edge_id) -> [NodeId]`
- `edges_for_node(node_id) -> [EdgeId]`
- `neighbors(node_id) -> [NodeId]`
- `optimize()` (see below)

## Optimization Rules

The optimized form is deterministic and compact:

- Sort and de-duplicate `EdgeEntry.nodes`.
- Sort and de-duplicate `NodeEntry.edges`.
- Elide redundant references produced by repeated inserts.

These rules reduce memory overhead and allow reproducible traversal order.

## Extension Points

The spec supports optional data fields:

- Node metadata (labels, attributes, payloads).
- Edge metadata (weights, constraints, annotations).

## Serialization

To ensure stable serialization across runs:

- Emit nodes and edges in ID order.
- Emit adjacency lists in sorted order after `optimize()`.
