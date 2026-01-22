"""Logical plan pattern tests for Grism.

This package contains tests that validate the logical plan generation
for various query patterns. Each test module covers a specific category
of operators and patterns.

Modules:
    test_scan_patterns: Scan operator variations (Node, Edge, Hyperedge)
    test_filter_patterns: Filter predicates (comparison, string, null)
    test_expand_patterns: Traversal patterns (single-hop, multi-hop, directions)
    test_aggregation_patterns: Group-by and aggregation functions
    test_projection_patterns: Select, distinct, sort, limit operations
    test_complex_patterns: Multi-stage query chains
"""
