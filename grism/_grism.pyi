"""Type stubs for the Grism native module."""

from __future__ import annotations

from typing import Any, Iterator, Literal, overload

# Version
__version__: str

# ============================================================================
# Core Classes
# ============================================================================

class Hypergraph:
    """
    Python wrapper for Hypergraph - the canonical user-facing container.
    
    Hypergraph is a logical handle to a versioned hypergraph state.
    It is immutable, snapshot-bound, and storage-agnostic.
    """
    
    @staticmethod
    def connect(
        uri: str,
        executor: str = "local",
        namespace: str | None = None,
    ) -> Hypergraph:
        """
        Connect to a Grism hypergraph.
        
        Args:
            uri: Connection URI (e.g., "grism://local", "grism:///path/to/data")
            executor: Execution backend ("local" or "ray")
            namespace: Optional namespace for logical graph isolation
        
        Returns:
            Hypergraph instance (immutable, lazy, snapshot-bound)
        """
        ...
    
    @staticmethod
    def create(uri: str, executor: str = "local") -> Hypergraph:
        """Create a new Grism hypergraph."""
        ...
    
    def with_namespace(self, name: str) -> Hypergraph:
        """Create a new Hypergraph scoped to a namespace."""
        ...
    
    def with_config(
        self,
        parallelism: int | None = None,
        memory_limit: int | None = None,
        batch_size: int = 1024,
    ) -> Hypergraph:
        """Configure execution settings."""
        ...
    
    def nodes(self, label: str | None = None) -> NodeFrame:
        """
        Get nodes, optionally filtered by label.
        
        Args:
            label: Node label to filter by (None = all nodes)
        
        Returns:
            NodeFrame (lazy, immutable)
        """
        ...
    
    def edges(self, label: str | None = None) -> EdgeFrame:
        """
        Get binary edges (arity=2 hyperedges), optionally filtered by label.
        
        Args:
            label: Edge label to filter by (None = all edges)
        
        Returns:
            EdgeFrame (lazy, immutable)
        """
        ...
    
    def hyperedges(self, label: str | None = None) -> HyperedgeFrame:
        """
        Get hyperedges, optionally filtered by label.
        
        Args:
            label: Hyperedge label to filter by (None = all hyperedges)
        
        Returns:
            HyperedgeFrame (lazy, immutable)
        """
        ...
    
    # Schema introspection
    def node_labels(self) -> list[str]:
        """Get all node labels in the graph."""
        ...
    
    def edge_types(self) -> list[str]:
        """Get all edge/relationship types in the graph."""
        ...
    
    def hyperedge_labels(self) -> list[str]:
        """Get all hyperedge labels in the graph."""
        ...
    
    def node_properties(self, label: str) -> list[str]:
        """Get property keys for a given node label."""
        ...
    
    def edge_properties(self, edge_type: str) -> list[str]:
        """Get property keys for a given edge type."""
        ...
    
    def node_count(self, label: str | None = None) -> int:
        """Get the number of nodes in the graph."""
        ...
    
    def edge_count(self, edge_type: str | None = None) -> int:
        """Get the number of edges in the graph."""
        ...
    
    def hyperedge_count(self, label: str | None = None) -> int:
        """Get the number of hyperedges in the graph."""
        ...
    
    def transaction(self) -> Transaction:
        """Begin a transaction for write operations."""
        ...


class NodeFrame:
    """
    Python wrapper for NodeFrame - a frame representing nodes.
    
    NodeFrame is immutable, lazy, and composable. All operations return new frames.
    """
    
    def filter(self, predicate: Expr) -> NodeFrame:
        """Filter nodes based on a predicate expression."""
        ...
    
    def where(self, predicate: Expr) -> NodeFrame:
        """Alias for filter()."""
        ...
    
    def select(self, *columns: str | Expr, **aliases: Expr) -> NodeFrame:
        """Project columns (select, rename, compute expressions)."""
        ...
    
    def with_column(self, name: str, expr: Expr) -> NodeFrame:
        """Add a computed column without removing existing columns."""
        ...
    
    def expand(
        self,
        edge: str | None = None,
        to: str | None = None,
        direction: Literal["out", "in", "both"] = "out",
        hops: int | tuple[int, int] | None = None,
        as_: str | None = None,
        edge_as: str | None = None,
    ) -> NodeFrame:
        """
        Expand to adjacent nodes via hyperedges (graph traversal).
        
        Args:
            edge: Edge/hyperedge label to traverse (None = any edge)
            to: Target node label filter (None = any label)
            direction: Traversal direction ("out", "in", "both")
            hops: Number of hops (int or tuple for range, default 1)
            as_: Alias for the expanded node frame
            edge_as: Alias for accessing edge/hyperedge properties
        """
        ...
    
    def optional_expand(
        self,
        edge: str | None = None,
        to: str | None = None,
        direction: Literal["out", "in", "both"] = "out",
        hops: int | tuple[int, int] | None = None,
        as_: str | None = None,
        edge_as: str | None = None,
    ) -> NodeFrame:
        """Optional expansion - keeps source nodes even if no match found."""
        ...
    
    def order_by(self, *exprs: str | Expr, ascending: bool | list[bool] | None = None) -> NodeFrame:
        """Sort rows by one or more expressions."""
        ...
    
    def sort(self, *exprs: str | Expr, ascending: bool | list[bool] | None = None) -> NodeFrame:
        """Alias for order_by()."""
        ...
    
    def limit(self, n: int) -> NodeFrame:
        """Limit the number of rows returned."""
        ...
    
    def offset(self, n: int) -> NodeFrame:
        """Skip the first n rows."""
        ...
    
    def skip(self, n: int) -> NodeFrame:
        """Alias for offset()."""
        ...
    
    def head(self, n: int = 5) -> NodeFrame:
        """Return first n rows."""
        ...
    
    def distinct(self) -> NodeFrame:
        """Remove duplicate rows."""
        ...
    
    def group_by(self, *keys: str | Expr) -> GroupedFrame:
        """Group rows by key expressions."""
        ...
    
    def groupby(self, *keys: str | Expr) -> GroupedFrame:
        """Alias for group_by()."""
        ...
    
    @overload
    def collect(
        self,
        executor: str | None = None,
        as_pandas: Literal[False] = False,
        as_arrow: bool = False,
        as_polars: Literal[False] = False,
    ) -> list[dict[str, Any]]:
        ...
    
    @overload
    def collect(
        self,
        executor: str | None = None,
        as_pandas: Literal[True] = True,
        as_arrow: bool = False,
        as_polars: bool = False,
    ) -> Any:  # pandas.DataFrame
        ...
    
    @overload
    def collect(
        self,
        executor: str | None = None,
        as_pandas: bool = False,
        as_arrow: bool = False,
        as_polars: Literal[True] = True,
    ) -> Any:  # polars.DataFrame
        ...
    
    def collect(
        self,
        executor: str | None = None,
        as_pandas: bool = False,
        as_arrow: bool = False,
        as_polars: bool = False,
    ) -> list[dict[str, Any]] | Any:
        """Execute the query and return results."""
        ...
    
    def count(self) -> int:
        """Execute and return the row count."""
        ...
    
    def first(self) -> dict[str, Any] | None:
        """Execute and return the first row."""
        ...
    
    def exists(self) -> bool:
        """Check if the frame has any results."""
        ...
    
    def schema(self) -> FrameSchema:
        """Get the schema of this frame."""
        ...
    
    def explain(self, mode: Literal["logical", "optimized", "physical", "cost"] = "logical") -> str:
        """Explain the query plan."""
        ...
    
    def __iter__(self) -> Iterator[dict[str, Any]]:
        ...
    
    def __len__(self) -> int:
        ...


class EdgeFrame:
    """Python wrapper for EdgeFrame - a frame representing binary edges."""
    
    def filter(self, predicate: Expr) -> EdgeFrame:
        """Filter edges based on a predicate expression."""
        ...
    
    def source(self) -> NodeFrame:
        """Get source nodes of these edges."""
        ...
    
    def target(self) -> NodeFrame:
        """Get target nodes of these edges."""
        ...
    
    def endpoints(self, which: Literal["source", "target", "both"] = "both") -> NodeFrame:
        """Get nodes connected by these edges."""
        ...
    
    def limit(self, n: int) -> EdgeFrame:
        """Limit the number of rows."""
        ...
    
    def collect(self, as_pandas: bool = False, as_polars: bool = False) -> list[dict[str, Any]] | Any:
        """Execute and collect results."""
        ...
    
    def count(self) -> int:
        """Execute and return the row count."""
        ...
    
    def first(self) -> dict[str, Any] | None:
        """Execute and return the first row."""
        ...
    
    def exists(self) -> bool:
        """Check if the frame has any results."""
        ...
    
    def schema(self) -> FrameSchema:
        """Get the schema of this frame."""
        ...
    
    def explain(self, mode: str = "logical") -> str:
        """Explain the query plan."""
        ...
    
    def __len__(self) -> int:
        ...


class HyperedgeFrame:
    """Python wrapper for HyperedgeFrame - a frame representing hyperedges."""
    
    def filter(self, predicate: Expr) -> HyperedgeFrame:
        """Filter hyperedges based on a predicate expression."""
        ...
    
    def where_role(self, role: str, value: Any) -> HyperedgeFrame:
        """Filter hyperedges where a role matches a value."""
        ...
    
    def expand(self, role: str, as_: str | None = None) -> NodeFrame:
        """Expand to nodes connected via a specific role."""
        ...
    
    def roles(self) -> list[str]:
        """Get all role names present in this hyperedge frame."""
        ...
    
    def limit(self, n: int) -> HyperedgeFrame:
        """Limit the number of rows."""
        ...
    
    def collect(self, as_pandas: bool = False, as_polars: bool = False) -> list[dict[str, Any]] | Any:
        """Execute and collect results."""
        ...
    
    def count(self) -> int:
        """Execute and return the row count."""
        ...
    
    def first(self) -> dict[str, Any] | None:
        """Execute and return the first row."""
        ...
    
    def exists(self) -> bool:
        """Check if the frame has any results."""
        ...
    
    def schema(self) -> FrameSchema:
        """Get the schema of this frame."""
        ...
    
    def explain(self, mode: str = "logical") -> str:
        """Explain the query plan."""
        ...
    
    def __len__(self) -> int:
        ...


class GroupedFrame:
    """Python wrapper for GroupedFrame - result of group_by()."""
    
    def agg(self, *aggs: AggExpr, **named_aggs: AggExpr) -> NodeFrame:
        """Apply aggregations to grouped rows."""
        ...
    
    def count(self) -> NodeFrame:
        """Count rows per group."""
        ...


class FrameSchema:
    """Python wrapper for frame schema information."""
    
    def column_names(self) -> list[str]:
        """Get column names."""
        ...
    
    def column_count(self) -> int:
        """Get column count."""
        ...
    
    def entity_type(self) -> str:
        """Get entity type."""
        ...
    
    def label(self) -> str | None:
        """Get entity label."""
        ...
    
    def has_column(self, name: str) -> bool:
        """Check if a column exists."""
        ...
    
    def column_type(self, name: str) -> str | None:
        """Get column type by name."""
        ...


class Transaction:
    """
    Python wrapper for transactions.
    
    Transaction provides atomic write operations on the hypergraph.
    Supports context manager protocol for automatic commit/rollback.
    """
    
    def create_node(self, label: str, **properties: Any) -> str:
        """Create a new node."""
        ...
    
    def create_edge(
        self,
        edge_type: str,
        source: str,
        target: str,
        **properties: Any,
    ) -> str:
        """Create a new edge (binary relationship)."""
        ...
    
    def create_hyperedge(
        self,
        label: str,
        bindings: dict[str, str],
        **properties: Any,
    ) -> str:
        """Create a new hyperedge (n-ary relationship)."""
        ...
    
    def update(self, entity_id: str, **properties: Any) -> None:
        """Update properties on an entity."""
        ...
    
    def delete_node(self, node_id: str) -> None:
        """Delete a node."""
        ...
    
    def delete_edge(self, edge_id: str) -> None:
        """Delete an edge."""
        ...
    
    def delete_hyperedge(self, hyperedge_id: str) -> None:
        """Delete a hyperedge."""
        ...
    
    def commit(self) -> None:
        """Commit the transaction."""
        ...
    
    def rollback(self) -> None:
        """Rollback the transaction."""
        ...
    
    def pending_count(self) -> int:
        """Get the number of pending operations."""
        ...
    
    def is_active(self) -> bool:
        """Check if the transaction is active."""
        ...
    
    def __enter__(self) -> Transaction:
        ...
    
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        ...


# ============================================================================
# Expression Classes
# ============================================================================

class Expr:
    """
    Python wrapper for expressions.
    
    Expr represents a computation over columns in a frame.
    Expressions are immutable, composable, and lazily evaluated.
    """
    
    # Comparison operators
    def __eq__(self, other: Any) -> Expr: ...  # type: ignore[override]
    def __ne__(self, other: Any) -> Expr: ...  # type: ignore[override]
    def __gt__(self, other: Any) -> Expr: ...
    def __ge__(self, other: Any) -> Expr: ...
    def __lt__(self, other: Any) -> Expr: ...
    def __le__(self, other: Any) -> Expr: ...
    
    # Logical operators
    def __and__(self, other: Expr) -> Expr: ...
    def __or__(self, other: Expr) -> Expr: ...
    def __invert__(self) -> Expr: ...
    
    # Arithmetic operators
    def __add__(self, other: Any) -> Expr: ...
    def __radd__(self, other: Any) -> Expr: ...
    def __sub__(self, other: Any) -> Expr: ...
    def __rsub__(self, other: Any) -> Expr: ...
    def __mul__(self, other: Any) -> Expr: ...
    def __rmul__(self, other: Any) -> Expr: ...
    def __truediv__(self, other: Any) -> Expr: ...
    def __mod__(self, other: Any) -> Expr: ...
    def __neg__(self) -> Expr: ...
    
    # Null handling
    def is_null(self) -> Expr:
        """Check if expression is NULL."""
        ...
    
    def is_not_null(self) -> Expr:
        """Check if expression is not NULL."""
        ...
    
    def coalesce(self, *values: Any) -> Expr:
        """Return first non-NULL value."""
        ...
    
    def fill_null(self, value: Any) -> Expr:
        """Replace NULL with a default value."""
        ...
    
    # String operations
    def contains(self, substring: str, case_sensitive: bool = True) -> Expr:
        """Check if string contains substring."""
        ...
    
    def starts_with(self, prefix: str) -> Expr:
        """Check if string starts with prefix."""
        ...
    
    def ends_with(self, suffix: str) -> Expr:
        """Check if string ends with suffix."""
        ...
    
    def matches(self, pattern: str) -> Expr:
        """Match against regex pattern."""
        ...
    
    def like(self, pattern: str) -> Expr:
        """SQL-style LIKE pattern matching."""
        ...
    
    def lower(self) -> Expr:
        """Convert to lowercase."""
        ...
    
    def upper(self) -> Expr:
        """Convert to uppercase."""
        ...
    
    def trim(self) -> Expr:
        """Remove leading/trailing whitespace."""
        ...
    
    def replace(self, old: str, new: str) -> Expr:
        """Replace substring occurrences."""
        ...
    
    def substring(self, start: int, length: int | None = None) -> Expr:
        """Extract substring."""
        ...
    
    def split(self, delimiter: str) -> Expr:
        """Split string into array."""
        ...
    
    def concat(self, *others: Any) -> Expr:
        """Concatenate strings."""
        ...
    
    def length(self) -> Expr:
        """Get string or array length."""
        ...
    
    # Collection operations
    def size(self) -> Expr:
        """Get size of array or map."""
        ...
    
    def get(self, index: Any) -> Expr:
        """Get element by index or key."""
        ...
    
    def contains_element(self, element: Any) -> Expr:
        """Check if array contains element."""
        ...
    
    def keys(self) -> Expr:
        """Get keys of a map."""
        ...
    
    def values(self) -> Expr:
        """Get values of a map."""
        ...
    
    # Type operations
    def cast(self, target_type: str) -> Expr:
        """Cast expression to target type."""
        ...
    
    # Ordering
    def asc(self) -> Expr:
        """Mark for ascending sort."""
        ...
    
    def desc(self) -> Expr:
        """Mark for descending sort."""
        ...
    
    def nulls_first(self) -> Expr:
        """Sort NULLs before other values."""
        ...
    
    def nulls_last(self) -> Expr:
        """Sort NULLs after other values."""
        ...
    
    # Aliasing
    def alias(self, name: str) -> Expr:
        """Give the expression an alias."""
        ...
    
    def as_(self, name: str) -> Expr:
        """Alias for alias()."""
        ...


class AggExpr:
    """Aggregation expression wrapper."""
    
    def alias(self, name: str) -> AggExpr:
        """Give the aggregation an alias."""
        ...
    
    def as_(self, name: str) -> AggExpr:
        """Alias for alias()."""
        ...


# ============================================================================
# Executor Classes
# ============================================================================

class Executor:
    """Python wrapper for executors."""
    
    @staticmethod
    def local(parallelism: int | None = None, memory_limit: int | None = None) -> Executor:
        """Create a local executor."""
        ...
    
    @staticmethod
    def ray(num_workers: int | None = None, resources: dict[str, Any] | None = None) -> Executor:
        """Create a Ray executor."""
        ...


class LocalExecutor:
    """Local executor convenience class."""
    
    def __init__(self, parallelism: int | None = None, memory_limit: int | None = None) -> None:
        ...


class RayExecutor:
    """Ray executor convenience class."""
    
    def __init__(self, num_workers: int | None = None) -> None:
        ...


# ============================================================================
# Expression Functions
# ============================================================================

def col(name: str) -> Expr:
    """Create a column reference expression."""
    ...


def lit(value: Any) -> Expr:
    """Create a literal value expression."""
    ...


def prop(name: str) -> Expr:
    """Alias for col() - provided for semantic clarity."""
    ...


# ============================================================================
# Aggregation Functions
# ============================================================================

def count(expr: Expr | None = None) -> AggExpr:
    """Count aggregation."""
    ...


def count_distinct(expr: Expr) -> AggExpr:
    """Count distinct aggregation."""
    ...


def sum_(expr: Expr) -> AggExpr:
    """Sum aggregation."""
    ...


def avg(expr: Expr) -> AggExpr:
    """Average aggregation."""
    ...


def min_(expr: Expr) -> AggExpr:
    """Minimum aggregation."""
    ...


def max_(expr: Expr) -> AggExpr:
    """Maximum aggregation."""
    ...


def collect(expr: Expr) -> AggExpr:
    """Collect values into an array."""
    ...


def collect_distinct(expr: Expr) -> AggExpr:
    """Collect distinct values into an array."""
    ...


def first(expr: Expr) -> AggExpr:
    """First value in group."""
    ...


def last(expr: Expr) -> AggExpr:
    """Last value in group."""
    ...


# ============================================================================
# String Functions
# ============================================================================

def concat(*exprs: Any) -> Expr:
    """Concatenate strings."""
    ...


def length(expr: Expr) -> Expr:
    """Get string/array length."""
    ...


def lower(expr: Expr) -> Expr:
    """Convert to lowercase."""
    ...


def upper(expr: Expr) -> Expr:
    """Convert to uppercase."""
    ...


def trim(expr: Expr) -> Expr:
    """Trim whitespace."""
    ...


def contains(expr: Expr, substring: str) -> Expr:
    """Contains function for strings."""
    ...


# ============================================================================
# Math Functions
# ============================================================================

def abs_(expr: Expr) -> Expr:
    """Absolute value."""
    ...


def ceil(expr: Expr) -> Expr:
    """Ceiling function."""
    ...


def floor(expr: Expr) -> Expr:
    """Floor function."""
    ...


def round_(expr: Expr, decimals: int = 0) -> Expr:
    """Round function."""
    ...


def sqrt(expr: Expr) -> Expr:
    """Square root."""
    ...


def power(base: Expr, exponent: Any) -> Expr:
    """Power function."""
    ...


# ============================================================================
# Date/Time Functions
# ============================================================================

def date(expr: Any) -> Expr:
    """Parse or convert to date."""
    ...


def year(expr: Expr) -> Expr:
    """Extract year from date."""
    ...


def month(expr: Expr) -> Expr:
    """Extract month from date."""
    ...


def day(expr: Expr) -> Expr:
    """Extract day from date."""
    ...


# ============================================================================
# Conditional Functions
# ============================================================================

def coalesce(*exprs: Any) -> Expr:
    """Return first non-NULL value."""
    ...


def if_(condition: Expr, then: Any, else_: Any) -> Expr:
    """Simple if-then-else expression."""
    ...


# ============================================================================
# Graph Functions
# ============================================================================

def labels(expr: Expr) -> Expr:
    """Get labels of a node."""
    ...


def type_(expr: Expr) -> Expr:
    """Get type of an edge/hyperedge."""
    ...


def id_(expr: Expr) -> Expr:
    """Get ID of a node or edge."""
    ...


def properties(expr: Expr) -> Expr:
    """Get all properties as a map."""
    ...


def nodes(expr: Expr) -> Expr:
    """Get nodes from a path."""
    ...


def relationships(expr: Expr) -> Expr:
    """Get relationships from a path."""
    ...


def path_length(expr: Expr) -> Expr:
    """Get length of a path."""
    ...


# ============================================================================
# Vector/AI Functions
# ============================================================================

def sim(left: Expr, right: Any) -> Expr:
    """Similarity function for vectors."""
    ...


# ============================================================================
# String Functions (standalone)
# ============================================================================

def substring(expr: Expr, start: int, length: int | None = None) -> Expr:
    """Extract substring."""
    ...


def replace(expr: Expr, old: str, new: str) -> Expr:
    """Replace substring occurrences."""
    ...


def split(expr: Expr, delimiter: str) -> Expr:
    """Split string into array."""
    ...


# ============================================================================
# Conditional Functions (additional)
# ============================================================================

def when(condition: Expr, then: Any, else_: Any) -> Expr:
    """When expression (alias for if_)."""
    ...


# ============================================================================
# Predicate Functions
# ============================================================================

def exists(expr: Any) -> Expr:
    """Check if a subquery/pattern exists."""
    ...


def any_(expr: Expr, predicate: Expr | None = None) -> Expr:
    """Check if any element in a collection satisfies a predicate."""
    ...


def all_(expr: Expr, predicate: Expr | None = None) -> Expr:
    """Check if all elements in a collection satisfy a predicate."""
    ...


# ============================================================================
# Path Functions
# ============================================================================

def shortest_path(
    start: Expr,
    end: Expr,
    edge_type: str | None = None,
    max_hops: int | None = None,
) -> Expr:
    """Find the shortest path between two nodes."""
    ...


def all_paths(
    start: Expr,
    end: Expr,
    edge_type: str | None = None,
    min_hops: int | None = None,
    max_hops: int | None = None,
) -> Expr:
    """Find all paths between two nodes."""
    ...


# ============================================================================
# Pattern Class
# ============================================================================

class Pattern:
    """Pattern for graph matching."""
    
    def __init__(self, spec: str | None = None) -> None:
        """Create a new pattern."""
        ...
    
    def start(self, label: str) -> Pattern:
        """Set the start node label."""
        ...
    
    def end(self, label: str) -> Pattern:
        """Set the end node label."""
        ...
    
    def via(self, edge_type: str) -> Pattern:
        """Set the edge type filter."""
        ...
    
    def hops(self, min_hops: int = 1, max_hops: int | None = None) -> Pattern:
        """Set the hop range."""
        ...
