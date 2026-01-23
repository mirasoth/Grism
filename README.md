# Grism

[![CI](https://github.com/mirasoth/Grism/actions/workflows/ci.yml/badge.svg)](https://github.com/mirasoth/Grism/actions/workflows/ci.yml)

**AI-native, neurosymbolic hypergraph database**

Grism is a hypergraph-first database designed for knowledge representation, semantic reasoning, and multi-modal data processing. It combines the expressiveness of n-ary relations with low-latency graph traversal and scalable computation.

## Key Features

- **Hypergraph-first** - N-ary relations (hyperedges) as the sole relational primitive; binary edges are projections
- **Neurosymbolic** - Integrates symbolic reasoning with neural signals (embeddings, LLM outputs)
- **Multi-modal** - Images, video, audio, and text as queryable, indexable data

## Quick Start

```bash
# Build
cargo build

# Run tests
cargo test
```

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Python API                         │
├─────────────────────────────────────────────────────┤
│              Logical Plan (grism-logical)            │
├─────────────────────────────────────────────────────┤
│              Optimizer (grism-optimizer)             │
├─────────────────────────────────────────────────────┤
│   Local Engine          │    Distributed Engine      │
│   (grism-engine)        │    (grism-distributed)     │
├─────────────────────────────────────────────────────┤
│              Storage (grism-storage)                 │
└─────────────────────────────────────────────────────┘
```

## Data Model

| Concept | Description |
|---------|-------------|
| **Node** | Atomic entity with stable identity, labels, and properties |
| **Hyperedge** | N-ary relation connecting entities via named roles (arity ≥ 2) |
| **Edge** | Binary projection of a hyperedge (source/target roles) |
| **Hypergraph** | Container for nodes and hyperedges |

## Documentation

- [Architecture Design](specs/rfc-0100.md) - Full system design
- [RFC Index](specs/rfc-index.md) - Design decisions and proposals
- [Development Guide](AGENTS.md) - Contribution guidelines and specs

## License

See [LICENSE](LICENSE) for details.