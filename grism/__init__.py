"""
Grism - AI-native neurosymbolic hypergraph database system.

Grism is a hypergraph-backed, relationally-executable, AI-native graph container
designed for modern agentic and LLM-driven workflows.

Example usage:

    >>> import grism as gr
    >>> 
    >>> # Connect to a hypergraph
    >>> hg = gr.Hypergraph.connect("grism://local")
    >>> 
    >>> # Query nodes
    >>> persons = hg.nodes("Person").filter(gr.col("age") > 18).collect()
    >>> 
    >>> # Graph traversal
    >>> friends = (
    ...     hg.nodes("Person")
    ...     .filter(gr.col("name") == "Alice")
    ...     .expand("KNOWS", direction="out")
    ...     .collect()
    ... )
    >>> 
    >>> # Aggregations
    >>> stats = (
    ...     hg.nodes("Person")
    ...     .group_by("city")
    ...     .agg(gr.count().alias("count"), gr.avg(gr.col("age")).alias("avg_age"))
    ...     .collect()
    ... )

For more information, visit: https://github.com/mirasoth/Grism
"""

from __future__ import annotations

# Import the native Rust module
from grism._grism import (
    # Core classes
    Hypergraph,
    NodeFrame,
    EdgeFrame,
    HyperedgeFrame,
    GroupedFrame,
    FrameSchema,
    Transaction,
    # Expression classes
    Expr,
    AggExpr,
    # Executor classes
    Executor,
    LocalExecutor,
    RayExecutor,
    # Expression functions
    col,
    lit,
    prop,
    # Aggregation functions
    count,
    count_distinct,
    sum_,
    avg,
    min_,
    max_,
    collect,
    collect_distinct,
    first,
    last,
    # String functions
    concat,
    length,
    lower,
    upper,
    trim,
    contains,
    # Math functions
    abs_,
    ceil,
    floor,
    round_,
    sqrt,
    power,
    # Date/time functions
    date,
    year,
    month,
    day,
    # Conditional functions
    coalesce,
    if_,
    # Graph functions
    labels,
    type_,
    id_,
    properties,
    nodes,
    relationships,
    path_length,
    # Vector/AI functions
    sim,
    # Version
    __version__,
)

# Re-export with standard Python names
sum = sum_  # noqa: A001
min = min_  # noqa: A001
max = max_  # noqa: A001
abs = abs_  # noqa: A001
round = round_  # noqa: A001
type = type_  # noqa: A001
id = id_  # noqa: A001

# Convenience functions
def connect(
    uri: str = "grism://local",
    executor: str = "local",
    namespace: str | None = None,
) -> Hypergraph:
    """
    Connect to a Grism hypergraph.
    
    This is a convenience function equivalent to Hypergraph.connect().
    
    Args:
        uri: Connection URI (e.g., "grism://local", "grism:///path/to/data")
        executor: Execution backend ("local" or "ray")
        namespace: Optional namespace for logical graph isolation
    
    Returns:
        Hypergraph instance (immutable, lazy, snapshot-bound)
    
    Example:
        >>> import grism as gr
        >>> hg = gr.connect("grism://local")
        >>> hg = gr.connect("grism:///data/my_graph", namespace="production")
    """
    return Hypergraph.connect(uri, executor, namespace)


def create(uri: str, executor: str = "local") -> Hypergraph:
    """
    Create a new Grism hypergraph.
    
    This is a convenience function equivalent to Hypergraph.create().
    
    Args:
        uri: Storage URI for the new hypergraph
        executor: Execution backend ("local" or "ray")
    
    Returns:
        Hypergraph instance
    
    Example:
        >>> import grism as gr
        >>> hg = gr.create("grism:///data/new_graph")
    """
    return Hypergraph.create(uri, executor)


# Public API
__all__ = [
    # Version
    "__version__",
    # Core classes
    "Hypergraph",
    "NodeFrame",
    "EdgeFrame",
    "HyperedgeFrame",
    "GroupedFrame",
    "FrameSchema",
    "Transaction",
    # Expression classes
    "Expr",
    "AggExpr",
    # Executor classes
    "Executor",
    "LocalExecutor",
    "RayExecutor",
    # Convenience functions
    "connect",
    "create",
    # Expression functions
    "col",
    "lit",
    "prop",
    # Aggregation functions
    "count",
    "count_distinct",
    "sum",
    "sum_",
    "avg",
    "min",
    "min_",
    "max",
    "max_",
    "collect",
    "collect_distinct",
    "first",
    "last",
    # String functions
    "concat",
    "length",
    "lower",
    "upper",
    "trim",
    "contains",
    # Math functions
    "abs",
    "abs_",
    "ceil",
    "floor",
    "round",
    "round_",
    "sqrt",
    "power",
    # Date/time functions
    "date",
    "year",
    "month",
    "day",
    # Conditional functions
    "coalesce",
    "if_",
    # Graph functions
    "labels",
    "type",
    "type_",
    "id",
    "id_",
    "properties",
    "nodes",
    "relationships",
    "path_length",
    # Vector/AI functions
    "sim",
]
