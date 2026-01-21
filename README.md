# Grism HyperGraph

Grism prefers **HyperGraph** as its logical data structure. The goal is a compact,
deterministic representation of multi-node relations with fast incidence lookup.
HyperFrame is not used in this design.

This repository provides a small Rust implementation plus the specification used
by Grism. See `SPEC.md` for the formal model and invariants.

## Quick overview

- **Node**: a logical entity in Grism.
- **HyperEdge**: a relation that can connect multiple nodes.
- **Incidence**: the membership relation between nodes and hyperedges.

The implementation exposes a simple API to add nodes/edges, query incidence, and
optimize adjacency lists for deterministic, efficient traversal.
