# Grism Python API v0.1 Specification

> **Status**: Frozen  
> **Version**: 0.1  
> **Last Updated**: 2026-01-21  
> **Depends on**: RFC-0001, RFC-0002, RFC-0003, RFC-0017  

This document defines the **canonical Python API** for Grism. This API is the **authoritative user-facing interface** and should remain backward-compatible within v0.x.

---

## 1. Design Principles

### 1.1 Core Principles

1. **Hypergraph-first semantics**: All relations are hyperedges; binary edges are projections only
2. **Python-first UX**: Python is the primary interface; no mandatory query language
3. **Lazy evaluation**: No execution until `.collect()` or iteration
4. **Immutability**: All operations return new objects; originals unchanged
5. **Composability**: Frames can be freely chained and nested
6. **Explainability**: Every API call maps to a deterministic logical operator

### 1.2 Naming Conventions

Per RFC-namings.md:

| Concept | Python Name | Notes |
|---------|-------------|-------|
| Graph container | `Hypergraph` | Canonical user-facing container |
| Atomic entity | `Node` | Stable identity |
| N-ary relation | `Hyperedge` | Sole relational primitive |
| Binary relation | `Edge` (view) | Arity=2 hyperedge projection |
| Node view | `NodeFrame` | Immutable, lazy view |
| Hyperedge view | `HyperedgeFrame` | Primary relational view |
| Binary edge view | `EdgeFrame` | Projection of HyperedgeFrame |

---

## 2. Core Entry Point

```python
class Hypergraph:
    """
    The canonical user-facing container for a Grism hypergraph.
    
    Hypergraph is a logical handle to a versioned hypergraph state.
    It is immutable, snapshot-bound, and storage-agnostic.
    """
    
    @staticmethod
    def connect(
        uri: str,
        *,
        executor: "Executor | str" = "local",
        namespace: str | None = None,
    ) -> "Hypergraph":
        """
        Connect to a Grism hypergraph.

        Args:
            uri: Connection URI
                - "grism://local" or "grism:///path/to/data" for local storage
                - "grism://host:port/db" for remote connection
            executor: Execution backend
                - "local": LocalExecutor (default)
                - "ray": RayExecutor for distributed execution
                - Executor instance for custom configuration
            namespace: Optional namespace for logical graph isolation

        Returns:
            Hypergraph instance (immutable, lazy, snapshot-bound)
            
        Examples:
            hg = Hypergraph.connect("grism://local")
            hg = Hypergraph.connect("grism:///data/mydb", executor="ray")
        """
        ...

    @staticmethod
    def create(
        uri: str,
        *,
        executor: "Executor | str" = "local",
    ) -> "Hypergraph":
        """
        Create a new Grism hypergraph at the specified URI.
        
        Returns:
            New empty Hypergraph instance
        """
        ...

    # Namespace / logical graph isolation
    def with_namespace(self, name: str) -> "Hypergraph":
        """
        Create a new Hypergraph scoped to a namespace.
        Returns a new instance; original unchanged.
        """
        ...

    # Graph views - primary entry points
    def nodes(self, label: str | None = None) -> "NodeFrame":
        """
        Get nodes, optionally filtered by label.

        Args:
            label: Node label to filter by (None = all nodes)

        Returns:
            NodeFrame (lazy, immutable)
            
        Examples:
            hg.nodes()  # All nodes
            hg.nodes("Person")  # Only Person nodes
        """
        ...

    def edges(self, label: str | None = None) -> "EdgeFrame":
        """
        Get binary edges (arity=2 hyperedges), optionally filtered by label.

        Args:
            label: Edge label to filter by (None = all edges)

        Returns:
            EdgeFrame (lazy, immutable)
            
        Note:
            EdgeFrame is a projection of HyperedgeFrame with arity=2.
            For n-ary hyperedges, use hyperedges() instead.
        """
        ...

    def hyperedges(self, label: str | None = None) -> "HyperedgeFrame":
        """
        Get hyperedges, optionally filtered by label.

        Args:
            label: Hyperedge label to filter by (None = all hyperedges)

        Returns:
            HyperedgeFrame (lazy, immutable)
        """
        ...

    # Pattern matching
    def match(self, pattern: "Pattern") -> "PathFrame":
        """
        Match a graph pattern and return matching paths.
        
        Args:
            pattern: Pattern specification (see Pattern class)
            
        Returns:
            PathFrame containing matched patterns
            
        Examples:
            # Find paths from Person to Company via WORKS_AT
            hg.match(
                Pattern()
                .node("p", "Person")
                .edge("WORKS_AT")
                .node("c", "Company")
            )
        """
        ...

    # Snapshot management
    def snapshot(self) -> "Snapshot":
        """Get the current snapshot identifier."""
        ...

    def at_snapshot(self, snapshot: "Snapshot | str") -> "Hypergraph":
        """Return a Hypergraph view at a specific snapshot (time travel)."""
        ...

    # Schema introspection
    def labels(self) -> list[str]:
        """Get all node labels in the graph."""
        ...

    def edge_types(self) -> list[str]:
        """Get all edge/hyperedge types in the graph."""
        ...

    def schema(self) -> "GraphSchema":
        """Get the full graph schema."""
        ...
```

---

## 3. Frame Base Class

```python
from typing import Self, Iterator, Any
import pyarrow

class Frame:
    """
    Base class for all frames (NodeFrame, EdgeFrame, HyperedgeFrame, PathFrame).

    Properties:
        - Immutable: All operations return new frames
        - Lazy: No execution until .collect() or iteration
        - Typed: Schema information available via .schema property
        - Composable: Frames can be chained and nested
    """

    @property
    def schema(self) -> "Schema":
        """
        Get the schema of this frame (available columns and types).
        May be partial if schema cannot be inferred statically.
        """
        ...

    @property
    def columns(self) -> list[str]:
        """Get list of column names in the current frame."""
        ...

    # ========== Filtering Operations ==========
    
    def filter(self, predicate: "Expr") -> Self:
        """
        Filter rows based on a predicate expression.

        Args:
            predicate: Boolean expression (Expr that evaluates to bool)

        Returns:
            New Frame with filtered rows

        Raises:
            TypeError: If predicate is not a boolean expression
            ColumnNotFoundError: If referenced columns don't exist
            
        Examples:
            .filter(col("age") >= 18)
            .filter((col("status") == "active") & col("verified").is_not_null())
        """
        ...

    def where(self, predicate: "Expr") -> Self:
        """Alias for filter()."""
        return self.filter(predicate)

    # ========== Projection Operations ==========
    
    def select(self, *columns: str | "Expr", **aliases: "Expr") -> Self:
        """
        Project columns (select, rename, compute expressions).

        Args:
            *columns: Column names or expressions to select
            **aliases: Keyword arguments for aliased columns

        Examples:
            .select("title", "year")
            .select(col("title"), col("year") * 2)
            .select(title=col("Paper.title"), author=col("Author.name"))
            .select("*")  # Select all columns
            .select("*", full_name=col("first_name") + " " + col("last_name"))

        Returns:
            New Frame with selected columns only

        Note:
            After select(), only selected columns are available in subsequent operations.
        """
        ...

    def with_column(self, name: str, expr: "Expr") -> Self:
        """
        Add a computed column without removing existing columns.
        
        Args:
            name: Name for the new column
            expr: Expression to compute
            
        Returns:
            New Frame with the additional column
        """
        ...

    def with_columns(self, **columns: "Expr") -> Self:
        """
        Add multiple computed columns.
        
        Args:
            **columns: Column name to expression mapping
            
        Returns:
            New Frame with additional columns
        """
        ...

    def drop(self, *columns: str) -> Self:
        """
        Remove columns from the frame.
        
        Args:
            *columns: Column names to remove
            
        Returns:
            New Frame without the specified columns
        """
        ...

    def rename(self, mapping: dict[str, str]) -> Self:
        """
        Rename columns.
        
        Args:
            mapping: Dict mapping old names to new names
            
        Returns:
            New Frame with renamed columns
        """
        ...

    # ========== Ordering & Limiting ==========
    
    def order_by(self, *exprs: "Expr | str", ascending: bool | list[bool] = True) -> Self:
        """
        Sort rows by one or more expressions.

        Args:
            *exprs: Column names or expressions to sort by
            ascending: Sort direction (True=ASC, False=DESC)
                      Can be a list to specify per-column

        Returns:
            New Frame with ordered rows
            
        Examples:
            .order_by("name")
            .order_by("year", ascending=False)
            .order_by("year", "name", ascending=[False, True])
            .order_by(col("score").desc())
        """
        ...

    def sort(self, *exprs: "Expr | str", ascending: bool | list[bool] = True) -> Self:
        """Alias for order_by()."""
        return self.order_by(*exprs, ascending=ascending)

    def limit(self, n: int) -> Self:
        """
        Limit the number of rows returned.

        Args:
            n: Maximum number of rows (must be positive)

        Returns:
            New Frame with limit applied
        """
        ...

    def offset(self, n: int) -> Self:
        """
        Skip the first n rows.
        
        Args:
            n: Number of rows to skip
            
        Returns:
            New Frame with offset applied
        """
        ...

    def skip(self, n: int) -> Self:
        """Alias for offset()."""
        return self.offset(n)

    def head(self, n: int = 5) -> Self:
        """Return first n rows. Alias for limit(n)."""
        return self.limit(n)

    def tail(self, n: int = 5) -> Self:
        """Return last n rows."""
        ...

    # ========== Deduplication ==========
    
    def distinct(self) -> Self:
        """
        Remove duplicate rows.
        
        Returns:
            New Frame with unique rows only
        """
        ...

    def unique(self, *columns: str) -> Self:
        """
        Remove duplicates based on specific columns.
        
        Args:
            *columns: Columns to use for uniqueness check
            
        Returns:
            New Frame with unique rows
        """
        ...

    # ========== Grouping & Aggregation ==========
    
    def group_by(self, *keys: str | "Expr") -> "GroupedFrame":
        """
        Group rows by key expressions.

        Args:
            *keys: Column names or expressions to group by

        Returns:
            GroupedFrame for aggregation

        Examples:
            .group_by("author")
            .group_by(col("Author.name"), col("Author.affiliation"))
        """
        ...

    def groupby(self, *keys: str | "Expr") -> "GroupedFrame":
        """Alias for group_by()."""
        return self.group_by(*keys)

    # ========== Set Operations ==========
    
    def union(self, other: "Frame") -> Self:
        """
        Union with another frame (multiset union, keeps duplicates).
        
        Args:
            other: Another frame with compatible schema
            
        Returns:
            New Frame containing rows from both frames
        """
        ...

    def union_all(self, other: "Frame") -> Self:
        """Alias for union()."""
        return self.union(other)

    def intersect(self, other: "Frame") -> Self:
        """
        Intersection with another frame.
        
        Args:
            other: Another frame with compatible schema
            
        Returns:
            New Frame with rows present in both frames
        """
        ...

    def except_(self, other: "Frame") -> Self:
        """
        Difference with another frame.
        
        Args:
            other: Another frame with compatible schema
            
        Returns:
            New Frame with rows in self but not in other
        """
        ...

    # ========== Array Operations ==========
    
    def explode(self, column: str, *, as_: str | None = None) -> Self:
        """
        Expand an array column into multiple rows.
        
        Args:
            column: Array column to expand
            as_: Optional alias for the expanded values
            
        Returns:
            New Frame with array elements as separate rows
            
        Example:
            # If "tags" is ["a", "b", "c"], this creates 3 rows
            .explode("tags")
        """
        ...

    def unwind(self, column: str, *, as_: str | None = None) -> Self:
        """Alias for explode() (Cypher terminology)."""
        return self.explode(column, as_=as_)

    # ========== Subquery Operations ==========
    
    def exists(self, subquery: "Frame") -> "Expr":
        """
        Create an EXISTS subquery expression.
        
        Args:
            subquery: A frame representing the subquery
            
        Returns:
            Boolean expression that is true if subquery returns any rows
        """
        ...

    # ========== Execution ==========
    
    def collect(
        self,
        *,
        executor: "Executor | str | None" = None,
        as_pandas: bool = False,
        as_arrow: bool = False,
        as_polars: bool = False,
    ) -> "list[dict] | pd.DataFrame | pa.Table | pl.DataFrame":
        """
        Execute the query and return results.

        Args:
            executor: Override executor for this query (None = use default)
            as_pandas: Return pandas DataFrame (requires pandas)
            as_arrow: Return PyArrow Table
            as_polars: Return Polars DataFrame (requires polars)
            Default: Return list of dicts

        Returns:
            Query results in requested format

        Raises:
            ExecutionError: If query execution fails
        """
        ...

    def to_list(self) -> list[dict]:
        """Execute and return as list of dicts."""
        return self.collect()

    def to_pandas(self) -> "pd.DataFrame":
        """Execute and return as pandas DataFrame."""
        return self.collect(as_pandas=True)

    def to_arrow(self) -> "pa.Table":
        """Execute and return as PyArrow Table."""
        return self.collect(as_arrow=True)

    def to_polars(self) -> "pl.DataFrame":
        """Execute and return as Polars DataFrame."""
        return self.collect(as_polars=True)

    def count(self) -> int:
        """Execute and return the row count."""
        ...

    def first(self) -> dict | None:
        """Execute and return the first row, or None if empty."""
        ...

    def explain(self, mode: str = "logical") -> str:
        """
        Explain the query plan.

        Args:
            mode: Explanation format
                - "logical": Logical plan tree (default)
                - "physical": Physical execution plan
                - "optimized": Optimized logical plan
                - "cost": Plan with cost estimates

        Returns:
            String representation of the plan
        """
        ...

    def __iter__(self) -> Iterator[dict]:
        """
        Iterate over results (triggers execution).
        Equivalent to iter(collect()).
        """
        ...

    def __len__(self) -> int:
        """Execute and return row count."""
        return self.count()
```

---

## 4. NodeFrame

```python
class NodeFrame(Frame):
    """
    Frame representing nodes in the graph.

    Attributes:
        label: Node label filter (None = all labels)
    """

    label: str | None

    def expand(
        self,
        edge: str | None = None,
        *,
        to: str | None = None,
        direction: str = "out",
        hops: int | tuple[int, int] = 1,
        as_: str | None = None,
        edge_as: str | None = None,
    ) -> "NodeFrame":
        """
        Expand to adjacent nodes via hyperedges (graph traversal).

        Args:
            edge: Edge/hyperedge label to traverse (None = any edge)
            to: Target node label filter (None = any label)
            direction: Traversal direction
                - "out": Follow outgoing edges (source → target)
                - "in": Follow incoming edges (target → source)
                - "both": Follow edges in both directions
            hops: Number of hops to traverse
                - int: Exactly N hops
                - tuple[int, int]: Range [min, max] hops (variable length path)
            as_: Alias for the expanded node frame (for column references)
            edge_as: Alias for accessing edge/hyperedge properties

        Returns:
            New NodeFrame representing the expanded nodes

        Semantics:
            - Expansion follows the Expand logical operator (RFC-0002)
            - Binary edges (arity=2) use adjacency traversal for performance
            - After expansion, both original and expanded node columns are accessible
            - Hyperedge properties are accessible via edge_as or edge label

        Examples:
            # Single hop, outgoing
            hg.nodes("Paper").expand("AUTHORED_BY", to="Author")

            # Multi-hop (exactly 2 hops)
            hg.nodes("Person").expand("KNOWS", hops=2)
            
            # Variable length path (1 to 3 hops)
            hg.nodes("Person").expand("KNOWS", hops=(1, 3))

            # With alias for column disambiguation
            hg.nodes("Paper").expand("AUTHORED_BY", to="Author", as_="author")
                .filter(col("author.name") == "Alice")

            # Access hyperedge properties
            hg.nodes("Paper").expand("CITES", edge_as="cite_edge")
                .filter(col("cite_edge.year") >= 2020)
                
            # Bidirectional traversal
            hg.nodes("Person").expand("KNOWS", direction="both")
        """
        ...

    def optional_expand(
        self,
        edge: str | None = None,
        *,
        to: str | None = None,
        direction: str = "out",
        hops: int | tuple[int, int] = 1,
        as_: str | None = None,
        edge_as: str | None = None,
    ) -> "NodeFrame":
        """
        Optional expansion - keeps source nodes even if no match found.
        
        Similar to OPTIONAL MATCH in Cypher. Unmatched expansions produce
        NULL values for expanded columns.
        
        Args:
            Same as expand()
            
        Returns:
            NodeFrame with optional expansion (left outer join semantics)
        """
        ...

    def neighbors(
        self,
        edge: str | None = None,
        *,
        direction: str = "both",
    ) -> "NodeFrame":
        """
        Get immediate neighbors. Convenience method for expand with hops=1.
        
        Args:
            edge: Edge label filter
            direction: Traversal direction
            
        Returns:
            NodeFrame containing neighbor nodes
        """
        ...

    def in_edges(self, label: str | None = None) -> "EdgeFrame":
        """Get incoming edges to these nodes."""
        ...

    def out_edges(self, label: str | None = None) -> "EdgeFrame":
        """Get outgoing edges from these nodes."""
        ...

    def degree(self, *, direction: str = "both") -> "NodeFrame":
        """
        Compute node degree.
        
        Args:
            direction: "in", "out", or "both"
            
        Returns:
            NodeFrame with added "degree" column
        """
        ...

    def labels(self) -> "Expr":
        """Get labels of nodes as an expression (for select/filter)."""
        ...

    def properties(self) -> "Expr":
        """Get all properties as a map expression."""
        ...

    def id(self) -> "Expr":
        """Get node ID as an expression."""
        ...
```

---

## 5. EdgeFrame

```python
class EdgeFrame(Frame):
    """
    Frame representing binary edges (arity=2 hyperedges) in the graph.

    Note:
        EdgeFrame is a projection of HyperedgeFrame with arity=2.
        For n-ary hyperedges, use HyperedgeFrame.
    """

    label: str | None

    def endpoints(self, which: str = "both") -> "NodeFrame":
        """
        Get nodes connected by these edges.

        Args:
            which: Which endpoint to return
                - "source": Source nodes
                - "target": Target nodes
                - "both": All endpoints (union)

        Returns:
            NodeFrame containing the endpoint nodes
        """
        ...

    def source(self) -> "NodeFrame":
        """Get source nodes. Equivalent to endpoints("source")."""
        ...

    def target(self) -> "NodeFrame":
        """Get target nodes. Equivalent to endpoints("target")."""
        ...

    def type(self) -> "Expr":
        """Get edge type/label as an expression."""
        ...

    def id(self) -> "Expr":
        """Get edge ID as an expression."""
        ...

    def properties(self) -> "Expr":
        """Get all properties as a map expression."""
        ...
```

---

## 6. HyperedgeFrame

```python
class HyperedgeFrame(Frame):
    """
    Frame representing hyperedges (N-ary relationships).

    HyperedgeFrame is the primary relational view in Grism.
    All hyperedges, including binary edges, are accessible through this frame.
    """

    label: str | None

    def where_role(
        self,
        role: str,
        value: str | int | "NodeFrame" | "Expr",
    ) -> "HyperedgeFrame":
        """
        Filter hyperedges where a role matches a value.

        Args:
            role: Role name to filter by
            value: Value to match
                - str/int: Literal value or node ID
                - NodeFrame: Match against nodes in frame
                - Expr: Expression to evaluate

        Returns:
            New HyperedgeFrame with filtered hyperedges

        Examples:
            hg.hyperedges("Event").where_role("participant", "Alice")
            hg.hyperedges("Event").where_role("participant", hg.nodes("Person"))
            hg.hyperedges("Event").where_role("participant", col("Person.id"))
        """
        ...

    def roles(self) -> list[str]:
        """
        Get all role names present in this hyperedge frame.
        Returns empty list if schema is unknown.
        """
        ...

    def expand(self, role: str, *, as_: str | None = None) -> "NodeFrame":
        """
        Expand to nodes connected via a specific role.

        Args:
            role: Role name to follow
            as_: Alias for the expanded node frame

        Returns:
            NodeFrame containing nodes connected via the specified role

        Examples:
            hg.hyperedges("Event").expand("participant")
            hg.hyperedges("AUTHORED_BY").expand("author", as_="writer")
        """
        ...

    def arity(self) -> "Expr":
        """Get hyperedge arity as an expression."""
        ...

    def type(self) -> "Expr":
        """Get hyperedge type/label as an expression."""
        ...

    def id(self) -> "Expr":
        """Get hyperedge ID as an expression."""
        ...

    def properties(self) -> "Expr":
        """Get all properties as a map expression."""
        ...
```

---

## 7. PathFrame

```python
class PathFrame(Frame):
    """
    Frame representing matched paths from pattern matching.
    
    PathFrame contains sequences of alternating nodes and edges.
    """

    def nodes(self) -> "NodeFrame":
        """Get all nodes in matched paths."""
        ...

    def edges(self) -> "EdgeFrame":
        """Get all edges in matched paths."""
        ...

    def length(self) -> "Expr":
        """Get path length as an expression."""
        ...

    def start_node(self) -> "NodeFrame":
        """Get start nodes of paths."""
        ...

    def end_node(self) -> "NodeFrame":
        """Get end nodes of paths."""
        ...
```

---

## 8. GroupedFrame

```python
class GroupedFrame:
    """
    Frame representing grouped rows (result of group_by()).

    Cannot be used directly; must call agg() to produce a Frame.
    """

    def agg(self, *aggs: "AggExpr", **named_aggs: "AggExpr") -> "Frame":
        """
        Apply aggregations to grouped rows.

        Args:
            *aggs: Positional aggregation expressions
            **named_aggs: Named aggregation expressions

        Returns:
            Frame with one row per group

        Examples:
            .group_by("author").agg(count(), total=sum_(col("citations")))
            .group_by("year").agg(
                papers=count(),
                avg_citations=avg(col("citations")),
                top_paper=max_(col("title"))
            )
        """
        ...

    def count(self) -> "Frame":
        """
        Count rows per group.
        Convenience method equivalent to .agg(count=count()).
        """
        ...

    def first(self) -> "Frame":
        """Get first row per group."""
        ...

    def last(self) -> "Frame":
        """Get last row per group."""
        ...
```

---

## 9. Pattern Matching

```python
class Pattern:
    """
    Builder for graph patterns used in match() operations.
    """

    def node(
        self,
        binding: str | None = None,
        label: str | None = None,
        **props: Any,
    ) -> "Pattern":
        """
        Add a node to the pattern.
        
        Args:
            binding: Variable name to bind matched nodes
            label: Node label filter
            **props: Property filters
            
        Returns:
            Updated Pattern
            
        Examples:
            Pattern().node("p", "Person")
            Pattern().node("p", "Person", name="Alice")
        """
        ...

    def edge(
        self,
        label: str | None = None,
        binding: str | None = None,
        *,
        direction: str = "out",
        hops: int | tuple[int, int] = 1,
        **props: Any,
    ) -> "Pattern":
        """
        Add an edge to the pattern.
        
        Args:
            label: Edge label filter
            binding: Variable name to bind matched edges
            direction: Edge direction ("out", "in", "both")
            hops: Number of hops (int or tuple for range)
            **props: Property filters
            
        Returns:
            Updated Pattern
        """
        ...

    def to(
        self,
        binding: str | None = None,
        label: str | None = None,
        **props: Any,
    ) -> "Pattern":
        """Alias for node() - adds destination node after edge."""
        ...


def shortest_path(
    start: "NodeFrame",
    end: "NodeFrame",
    *,
    edge: str | None = None,
    direction: str = "both",
    max_hops: int | None = None,
) -> "PathFrame":
    """
    Find shortest paths between nodes.
    
    Args:
        start: Starting nodes
        end: Ending nodes
        edge: Edge label filter
        direction: Edge direction
        max_hops: Maximum path length (None = no limit)
        
    Returns:
        PathFrame containing shortest paths
        
    Examples:
        shortest_path(
            hg.nodes("Person").filter(col("name") == "Alice"),
            hg.nodes("Person").filter(col("name") == "Bob"),
            edge="KNOWS"
        )
    """
    ...


def all_paths(
    start: "NodeFrame",
    end: "NodeFrame",
    *,
    edge: str | None = None,
    direction: str = "both",
    min_hops: int = 1,
    max_hops: int = 10,
) -> "PathFrame":
    """
    Find all paths between nodes within hop constraints.
    
    Args:
        start: Starting nodes
        end: Ending nodes
        edge: Edge label filter
        direction: Edge direction
        min_hops: Minimum path length
        max_hops: Maximum path length
        
    Returns:
        PathFrame containing all matching paths
    """
    ...
```

---

## 10. Expression System

```python
class Expr:
    """
    Base class for all expressions.
    
    Expressions are immutable, composable, and lazily evaluated.
    All expressions follow RFC-0003 semantics.
    """
    
    # ========== Comparison Operators ==========
    
    def __eq__(self, other: Any) -> "Expr":
        """Equality: self == other"""
        ...
    
    def __ne__(self, other: Any) -> "Expr":
        """Inequality: self != other"""
        ...
    
    def __gt__(self, other: Any) -> "Expr":
        """Greater than: self > other"""
        ...
    
    def __ge__(self, other: Any) -> "Expr":
        """Greater than or equal: self >= other"""
        ...
    
    def __lt__(self, other: Any) -> "Expr":
        """Less than: self < other"""
        ...
    
    def __le__(self, other: Any) -> "Expr":
        """Less than or equal: self <= other"""
        ...

    # ========== Logical Operators ==========
    
    def __and__(self, other: "Expr") -> "Expr":
        """Logical AND: self & other"""
        ...
    
    def __or__(self, other: "Expr") -> "Expr":
        """Logical OR: self | other"""
        ...
    
    def __invert__(self) -> "Expr":
        """Logical NOT: ~self"""
        ...

    # ========== Arithmetic Operators ==========
    
    def __add__(self, other: Any) -> "Expr":
        """Addition: self + other"""
        ...
    
    def __radd__(self, other: Any) -> "Expr":
        """Reverse addition: other + self"""
        ...
    
    def __sub__(self, other: Any) -> "Expr":
        """Subtraction: self - other"""
        ...
    
    def __rsub__(self, other: Any) -> "Expr":
        """Reverse subtraction: other - self"""
        ...
    
    def __mul__(self, other: Any) -> "Expr":
        """Multiplication: self * other"""
        ...
    
    def __rmul__(self, other: Any) -> "Expr":
        """Reverse multiplication: other * self"""
        ...
    
    def __truediv__(self, other: Any) -> "Expr":
        """Division: self / other"""
        ...
    
    def __rtruediv__(self, other: Any) -> "Expr":
        """Reverse division: other / self"""
        ...
    
    def __mod__(self, other: Any) -> "Expr":
        """Modulo: self % other"""
        ...
    
    def __neg__(self) -> "Expr":
        """Negation: -self"""
        ...

    # ========== Null Handling ==========
    
    def is_null(self) -> "Expr":
        """Check if expression is NULL."""
        ...
    
    def is_not_null(self) -> "Expr":
        """Check if expression is not NULL."""
        ...
    
    def coalesce(self, *values: Any) -> "Expr":
        """Return first non-NULL value."""
        ...

    def fill_null(self, value: Any) -> "Expr":
        """Replace NULL with a default value."""
        ...

    # ========== String Operations ==========
    
    def contains(self, substring: str, *, case_sensitive: bool = True) -> "Expr":
        """Check if string contains substring."""
        ...
    
    def starts_with(self, prefix: str) -> "Expr":
        """Check if string starts with prefix."""
        ...
    
    def ends_with(self, suffix: str) -> "Expr":
        """Check if string ends with suffix."""
        ...
    
    def matches(self, pattern: str) -> "Expr":
        """Match against regex pattern."""
        ...
    
    def like(self, pattern: str) -> "Expr":
        """SQL-style LIKE pattern matching (% and _ wildcards)."""
        ...

    def lower(self) -> "Expr":
        """Convert to lowercase."""
        ...

    def upper(self) -> "Expr":
        """Convert to uppercase."""
        ...

    def trim(self) -> "Expr":
        """Remove leading/trailing whitespace."""
        ...

    def ltrim(self) -> "Expr":
        """Remove leading whitespace."""
        ...

    def rtrim(self) -> "Expr":
        """Remove trailing whitespace."""
        ...

    def replace(self, old: str, new: str) -> "Expr":
        """Replace substring occurrences."""
        ...

    def substring(self, start: int, length: int | None = None) -> "Expr":
        """Extract substring."""
        ...

    def split(self, delimiter: str) -> "Expr":
        """Split string into array."""
        ...

    def concat(self, *others: "Expr | str") -> "Expr":
        """Concatenate strings."""
        ...

    def length(self) -> "Expr":
        """Get string length (or array length)."""
        ...

    # ========== Collection Operations ==========
    
    def size(self) -> "Expr":
        """Get size of array or map."""
        ...

    def get(self, index: int | str) -> "Expr":
        """Get element by index (array) or key (map)."""
        ...

    def contains_element(self, element: Any) -> "Expr":
        """Check if array contains element."""
        ...

    def keys(self) -> "Expr":
        """Get keys of a map."""
        ...

    def values(self) -> "Expr":
        """Get values of a map."""
        ...

    # ========== Type Operations ==========
    
    def cast(self, target_type: str) -> "Expr":
        """
        Cast expression to target type.
        
        Args:
            target_type: Target type name
                - "int", "float", "string", "bool"
                - "date", "timestamp"
                - "array", "map"
        """
        ...

    def type_of(self) -> "Expr":
        """Get the type name of the value."""
        ...

    # ========== Ordering ==========
    
    def asc(self) -> "Expr":
        """Mark for ascending sort."""
        ...

    def desc(self) -> "Expr":
        """Mark for descending sort."""
        ...

    def nulls_first(self) -> "Expr":
        """Sort NULLs before other values."""
        ...

    def nulls_last(self) -> "Expr":
        """Sort NULLs after other values."""
        ...

    # ========== Aliasing ==========
    
    def alias(self, name: str) -> "Expr":
        """Give the expression an alias."""
        ...

    def as_(self, name: str) -> "Expr":
        """Alias for alias()."""
        return self.alias(name)


# ========== Expression Construction Functions ==========

def col(name: str) -> Expr:
    """
    Reference a column by name.
    
    Args:
        name: Column name
            - Qualified: "Label.column" or "alias.column"
            - Unqualified: "column"
        
    Returns:
        Column reference expression
        
    Examples:
        col("name")
        col("Author.name")
        col("Paper.title")
    """
    ...


def lit(value: Any) -> Expr:
    """
    Create a literal value expression.
    
    Args:
        value: Python value (int, float, str, bool, None, list, dict, etc.)
        
    Returns:
        Literal expression
        
    Examples:
        lit(42)
        lit("Alice")
        lit([1, 2, 3])
        lit(None)
    """
    ...


def prop(name: str) -> Expr:
    """
    Reference a property by name.
    Alias for col() - provided for semantic clarity when accessing properties.
    """
    return col(name)


# ========== Vector/AI Functions ==========

def sim(left: Expr, right: Expr | "np.ndarray") -> Expr:
    """
    Compute cosine similarity between two vectors.
    
    Args:
        left: Expression evaluating to a vector
        right: Expression or numpy array
        
    Returns:
        Expression evaluating to similarity score (0.0 to 1.0)
    """
    ...


def distance(left: Expr, right: Expr | "np.ndarray", metric: str = "l2") -> Expr:
    """
    Compute distance between two vectors.
    
    Args:
        left: Expression evaluating to a vector
        right: Expression or numpy array
        metric: Distance metric ("l2", "cosine", "dot")
        
    Returns:
        Expression evaluating to distance
    """
    ...


# ========== Math Functions ==========

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
    """Round to specified decimal places."""
    ...

def sqrt(expr: Expr) -> Expr:
    """Square root."""
    ...

def power(base: Expr, exponent: Expr | int | float) -> Expr:
    """Raise to power."""
    ...

def log(expr: Expr, base: float = math.e) -> Expr:
    """Logarithm."""
    ...

def exp(expr: Expr) -> Expr:
    """Exponential function."""
    ...

def sign(expr: Expr) -> Expr:
    """Sign of a number (-1, 0, or 1)."""
    ...


# ========== String Functions ==========

def concat(*exprs: Expr | str) -> Expr:
    """Concatenate strings."""
    ...

def substring(expr: Expr, start: int, length: int | None = None) -> Expr:
    """Extract substring."""
    ...

def replace(expr: Expr, old: str, new: str) -> Expr:
    """Replace substring."""
    ...

def split(expr: Expr, delimiter: str) -> Expr:
    """Split string into array."""
    ...

def trim(expr: Expr) -> Expr:
    """Trim whitespace."""
    ...

def lower(expr: Expr) -> Expr:
    """Convert to lowercase."""
    ...

def upper(expr: Expr) -> Expr:
    """Convert to uppercase."""
    ...

def length(expr: Expr) -> Expr:
    """Get string or array length."""
    ...

def left(expr: Expr, n: int) -> Expr:
    """Get leftmost n characters."""
    ...

def right(expr: Expr, n: int) -> Expr:
    """Get rightmost n characters."""
    ...

def reverse(expr: Expr) -> Expr:
    """Reverse string or array."""
    ...

def to_string(expr: Expr) -> Expr:
    """Convert to string."""
    ...


# ========== Date/Time Functions ==========

def date(expr: Expr | str) -> Expr:
    """Parse or convert to date."""
    ...

def timestamp(expr: Expr | str) -> Expr:
    """Parse or convert to timestamp."""
    ...

def year(expr: Expr) -> Expr:
    """Extract year from date/timestamp."""
    ...

def month(expr: Expr) -> Expr:
    """Extract month from date/timestamp."""
    ...

def day(expr: Expr) -> Expr:
    """Extract day from date/timestamp."""
    ...

def hour(expr: Expr) -> Expr:
    """Extract hour from timestamp."""
    ...

def minute(expr: Expr) -> Expr:
    """Extract minute from timestamp."""
    ...

def second(expr: Expr) -> Expr:
    """Extract second from timestamp."""
    ...

def now() -> Expr:
    """Current timestamp."""
    ...

def today() -> Expr:
    """Current date."""
    ...

def date_diff(start: Expr, end: Expr, unit: str = "day") -> Expr:
    """
    Compute difference between dates.
    
    Args:
        start: Start date/timestamp
        end: End date/timestamp
        unit: Unit ("day", "month", "year", "hour", "minute", "second")
    """
    ...

def date_add(expr: Expr, amount: int, unit: str = "day") -> Expr:
    """Add to a date."""
    ...


# ========== Conditional Functions ==========

def when(condition: Expr) -> "WhenBuilder":
    """
    Start a conditional expression (CASE WHEN).
    
    Examples:
        when(col("age") < 18).then("minor").otherwise("adult")
        when(col("score") > 90).then("A")
            .when(col("score") > 80).then("B")
            .otherwise("C")
    """
    ...


class WhenBuilder:
    def then(self, value: Any) -> "WhenBuilder":
        """Value when condition is true."""
        ...
    
    def when(self, condition: Expr) -> "WhenBuilder":
        """Add another condition."""
        ...
    
    def otherwise(self, value: Any) -> Expr:
        """Default value (ELSE clause)."""
        ...


def if_(condition: Expr, then: Any, else_: Any) -> Expr:
    """
    Simple if-then-else expression.
    
    Args:
        condition: Boolean expression
        then: Value if true
        else_: Value if false
    """
    ...


def coalesce(*exprs: Expr | Any) -> Expr:
    """Return first non-NULL value."""
    ...


def nullif(expr1: Expr, expr2: Any) -> Expr:
    """Return NULL if expr1 equals expr2, else expr1."""
    ...


# ========== Collection Functions ==========

def array(*elements: Any) -> Expr:
    """Create an array from elements."""
    ...

def array_length(expr: Expr) -> Expr:
    """Get array length."""
    ...

def array_contains(arr: Expr, element: Any) -> Expr:
    """Check if array contains element."""
    ...

def array_agg(expr: Expr) -> "AggExpr":
    """Aggregate values into an array."""
    ...

def map_keys(expr: Expr) -> Expr:
    """Get keys from a map."""
    ...

def map_values(expr: Expr) -> Expr:
    """Get values from a map."""
    ...


# ========== Graph-Specific Functions ==========

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

def start_node(expr: Expr) -> Expr:
    """Get start node of an edge."""
    ...

def end_node(expr: Expr) -> Expr:
    """Get end node of an edge."""
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


# ========== Existence Checks ==========

def exists(subquery: "Frame") -> Expr:
    """
    Check if subquery returns any rows.
    
    Args:
        subquery: Frame representing the subquery
        
    Returns:
        Boolean expression
        
    Example:
        hg.nodes("Person").filter(
            exists(hg.edges("KNOWS").filter(col("source") == col("Person.id")))
        )
    """
    ...


def any_(expr: Expr, predicate: Expr) -> Expr:
    """
    Check if any element in collection satisfies predicate.
    
    Args:
        expr: Collection expression
        predicate: Predicate to check
    """
    ...


def all_(expr: Expr, predicate: Expr) -> Expr:
    """
    Check if all elements in collection satisfy predicate.
    """
    ...


def none_(expr: Expr, predicate: Expr) -> Expr:
    """
    Check if no elements in collection satisfy predicate.
    """
    ...
```

---

## 11. Aggregation Functions

```python
class AggExpr:
    """
    Aggregation expression (used in group_by().agg()).
    
    Aggregation functions follow RFC-0003 semantics.
    """
    
    def alias(self, name: str) -> "AggExpr":
        """Give the aggregation an alias."""
        ...

    def as_(self, name: str) -> "AggExpr":
        """Alias for alias()."""
        return self.alias(name)


def count(expr: Expr | None = None) -> AggExpr:
    """
    Count rows (or non-NULL values if expr provided).
    
    Args:
        expr: Optional expression to count non-NULL values of
            None = count all rows (equivalent to COUNT(*))
        
    Returns:
        Aggregation expression
        
    Examples:
        count()  # Count all rows
        count(col("author"))  # Count non-NULL authors
    """
    ...


def count_distinct(expr: Expr) -> AggExpr:
    """
    Count distinct non-NULL values.
    
    Args:
        expr: Expression to count distinct values of
    """
    ...


def sum_(expr: Expr) -> AggExpr:
    """
    Sum values.
    
    Args:
        expr: Numeric expression
    """
    ...


def avg(expr: Expr) -> AggExpr:
    """
    Average values.
    
    Args:
        expr: Numeric expression
    """
    ...


def min_(expr: Expr) -> AggExpr:
    """
    Minimum value.
    
    Args:
        expr: Comparable expression
    """
    ...


def max_(expr: Expr) -> AggExpr:
    """
    Maximum value.
    
    Args:
        expr: Comparable expression
    """
    ...


def collect(expr: Expr) -> AggExpr:
    """
    Collect values into an array.
    
    Args:
        expr: Expression to collect
        
    Returns:
        Aggregation expression (returns array of values)
    """
    ...


def collect_distinct(expr: Expr) -> AggExpr:
    """
    Collect distinct values into an array.
    """
    ...


def first(expr: Expr) -> AggExpr:
    """First value in group."""
    ...


def last(expr: Expr) -> AggExpr:
    """Last value in group."""
    ...


def stddev(expr: Expr) -> AggExpr:
    """Standard deviation."""
    ...


def stddev_pop(expr: Expr) -> AggExpr:
    """Population standard deviation."""
    ...


def variance(expr: Expr) -> AggExpr:
    """Sample variance."""
    ...


def variance_pop(expr: Expr) -> AggExpr:
    """Population variance."""
    ...


def percentile(expr: Expr, percentile: float) -> AggExpr:
    """
    Compute percentile.
    
    Args:
        expr: Numeric expression
        percentile: Percentile value (0.0 to 1.0)
    """
    ...


def median(expr: Expr) -> AggExpr:
    """Median value (50th percentile)."""
    ...


def mode(expr: Expr) -> AggExpr:
    """Most frequent value."""
    ...


def string_agg(expr: Expr, separator: str = ",") -> AggExpr:
    """
    Concatenate strings with separator.
    
    Args:
        expr: String expression
        separator: Separator between values
    """
    ...
```

---

## 12. Mutation API

```python
class Hypergraph:
    """
    Mutation operations are explicit and return new graph states.
    Follows RFC-0017 transaction semantics.
    """

    def insert_node(
        self,
        label: str,
        properties: dict[str, Any] | None = None,
        *,
        id: int | None = None,
    ) -> "Hypergraph":
        """
        Insert a node.

        Args:
            label: Node label
            properties: Node properties (dict)
            id: Optional node ID (auto-generated if None)

        Returns:
            New Hypergraph instance with node inserted
        """
        ...

    def insert_nodes(
        self,
        label: str,
        data: "list[dict] | pd.DataFrame | pa.Table",
    ) -> "Hypergraph":
        """
        Bulk insert nodes.
        
        Args:
            label: Node label for all nodes
            data: Node data (list of dicts, pandas DataFrame, or Arrow Table)
            
        Returns:
            New Hypergraph instance
        """
        ...

    def insert_edge(
        self,
        label: str,
        source: int | "NodeFrame",
        target: int | "NodeFrame",
        properties: dict[str, Any] | None = None,
    ) -> "Hypergraph":
        """
        Insert a binary edge (arity=2 hyperedge).

        Args:
            label: Edge label
            source: Source node ID or NodeFrame (must yield single node)
            target: Target node ID or NodeFrame (must yield single node)
            properties: Edge properties

        Returns:
            New Hypergraph instance with edge inserted
        """
        ...

    def insert_edges(
        self,
        label: str,
        data: "list[dict] | pd.DataFrame | pa.Table",
        *,
        source_col: str = "source",
        target_col: str = "target",
    ) -> "Hypergraph":
        """
        Bulk insert edges.
        
        Args:
            label: Edge label for all edges
            data: Edge data with source/target columns
            source_col: Column name for source node IDs
            target_col: Column name for target node IDs
            
        Returns:
            New Hypergraph instance
        """
        ...

    def insert_hyperedge(
        self,
        label: str,
        roles: dict[str, int | "NodeFrame"],
        properties: dict[str, Any] | None = None,
    ) -> "Hypergraph":
        """
        Insert a hyperedge.

        Args:
            label: Hyperedge label
            roles: Role-to-node mapping
            properties: Hyperedge properties

        Returns:
            New Hypergraph instance with hyperedge inserted

        Examples:
            hg.insert_hyperedge(
                "Event",
                roles={"participant": alice_id, "location": venue_id, "organizer": bob_id},
                properties={"date": "2024-01-01", "name": "Conference"}
            )
        """
        ...

    def update_node(
        self,
        node_id: int,
        properties: dict[str, Any],
        *,
        merge: bool = True,
    ) -> "Hypergraph":
        """
        Update node properties.
        
        Args:
            node_id: Node to update
            properties: New property values
            merge: If True, merge with existing properties; if False, replace
            
        Returns:
            New Hypergraph instance
        """
        ...

    def update_edge(
        self,
        edge_id: int,
        properties: dict[str, Any],
        *,
        merge: bool = True,
    ) -> "Hypergraph":
        """Update edge properties."""
        ...

    def delete_node(self, node_id: int) -> "Hypergraph":
        """
        Delete a node (and all connected hyperedges).
        
        Note: This is a logical deletion per RFC-0017.
        """
        ...

    def delete_edge(self, edge_id: int) -> "Hypergraph":
        """Delete a binary edge."""
        ...

    def delete_hyperedge(self, hyperedge_id: int) -> "Hypergraph":
        """Delete a hyperedge."""
        ...

    # Transaction support
    def begin_transaction(self) -> "Transaction":
        """Begin a new transaction."""
        ...

    def commit(self) -> "Hypergraph":
        """
        Commit pending mutations.
        Returns new Hypergraph instance with mutations applied.
        """
        ...


class Transaction:
    """
    Transaction context for batched mutations.
    Follows RFC-0017 semantics.
    """
    
    def insert_node(self, label: str, properties: dict[str, Any] | None = None) -> int:
        """Insert node and return assigned ID."""
        ...
    
    def insert_edge(self, label: str, source: int, target: int, properties: dict[str, Any] | None = None) -> int:
        """Insert edge and return assigned ID."""
        ...
    
    def insert_hyperedge(self, label: str, roles: dict[str, int], properties: dict[str, Any] | None = None) -> int:
        """Insert hyperedge and return assigned ID."""
        ...
    
    def commit(self) -> "Hypergraph":
        """Commit the transaction."""
        ...
    
    def rollback(self) -> None:
        """Abort the transaction."""
        ...
    
    def __enter__(self) -> "Transaction":
        ...
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        ...
```

---

## 13. Execution Backends

```python
class Executor:
    """
    Base class for execution backends.
    """
    name: str
    
    def execute(self, plan: "LogicalPlan") -> "Result":
        """Execute a logical plan."""
        ...


class LocalExecutor(Executor):
    """
    Local single-machine executor.
    Reference implementation for Grism execution.
    """
    def __init__(
        self,
        *,
        parallelism: int | None = None,
        memory_limit: int | None = None,
        temp_dir: str | None = None,
    ):
        """
        Args:
            parallelism: Number of parallel threads (None = CPU count)
            memory_limit: Memory limit in bytes (None = no limit)
            temp_dir: Directory for temporary files
        """
        ...


class RayExecutor(Executor):
    """
    Ray-distributed executor.
    Enables scale-out execution for large graphs.
    """
    def __init__(
        self,
        *,
        num_workers: int | None = None,
        resources: dict[str, float] | None = None,
        ray_address: str | None = None,
        **ray_config,
    ):
        """
        Args:
            num_workers: Number of Ray workers (None = auto-scale)
            resources: Resource requirements per task
            ray_address: Ray cluster address (None = local)
            **ray_config: Additional Ray configuration
        """
        ...
```

---

## 14. Schema and Type Information

```python
class Schema:
    """
    Schema information for a frame.
    """
    
    @property
    def columns(self) -> list["ColumnInfo"]:
        """List of columns in the schema."""
        ...
    
    @property
    def column_names(self) -> list[str]:
        """List of column names."""
        ...
    
    def __getitem__(self, name: str) -> "ColumnInfo":
        """Get column info by name."""
        ...
    
    def __contains__(self, name: str) -> bool:
        """Check if column exists."""
        ...


class ColumnInfo:
    """
    Information about a single column.
    """
    name: str
    data_type: "DataType"
    nullable: bool
    qualifier: str | None  # Entity qualifier (label or alias)


class DataType:
    """
    Data type enumeration aligned with RFC-0003.
    """
    INT64: "DataType"
    FLOAT64: "DataType"
    BOOL: "DataType"
    STRING: "DataType"
    BINARY: "DataType"
    TIMESTAMP: "DataType"
    DATE: "DataType"
    VECTOR: "DataType"
    ARRAY: "DataType"
    MAP: "DataType"
    NULL: "DataType"
    
    @property
    def name(self) -> str:
        """Type name."""
        ...
    
    def is_numeric(self) -> bool:
        """Check if numeric type."""
        ...
    
    def is_temporal(self) -> bool:
        """Check if date/time type."""
        ...


class GraphSchema:
    """
    Full schema information for a hypergraph.
    """
    
    def node_labels(self) -> list[str]:
        """Get all node labels."""
        ...
    
    def edge_types(self) -> list[str]:
        """Get all edge/hyperedge types."""
        ...
    
    def node_properties(self, label: str) -> Schema:
        """Get property schema for a node label."""
        ...
    
    def edge_properties(self, type_: str) -> Schema:
        """Get property schema for an edge type."""
        ...
```

---

## 15. Error Types

```python
class GrismError(Exception):
    """Base exception for all Grism errors."""
    pass


class ConnectionError(GrismError):
    """Error connecting to a hypergraph."""
    pass


class ColumnNotFoundError(GrismError):
    """Referenced column does not exist."""
    column: str
    available_columns: list[str]


class AmbiguousColumnError(GrismError):
    """Column reference is ambiguous."""
    column: str
    matching_sources: list[str]


class TypeError(GrismError):
    """Type mismatch in expression."""
    expected: str
    actual: str
    expression: str


class ValidationError(GrismError):
    """Plan validation failed."""
    errors: list[str]


class ExecutionError(GrismError):
    """Query execution failed."""
    pass


class TransactionError(GrismError):
    """Transaction operation failed."""
    pass


class ConstraintViolationError(GrismError):
    """Constraint violated during mutation."""
    constraint: str
    details: str
```

---

## 16. Canonical Usage Examples

### Example 1: Basic Query

```python
from grism import Hypergraph, col

hg = Hypergraph.connect("grism://local")

result = (
    hg.nodes("Paper")
      .filter(col("year") >= 2022)
      .expand("CITES")
      .filter(col("citations") > 100)
      .select("title", "year", "citations")
      .order_by("citations", ascending=False)
      .limit(10)
      .collect()
)
```

### Example 2: Multi-Hop Expansion with Aliases

```python
result = (
    hg.nodes("Person")
      .expand("KNOWS", hops=2, as_="friend")
      .filter(col("friend.age") > 25)
      .select("name", friend_name=col("friend.name"))
      .distinct()
      .collect()
)
```

### Example 3: Aggregation

```python
from grism import count, avg, max_

result = (
    hg.nodes("Paper")
      .expand("AUTHORED_BY", to="Author")
      .group_by("Author.name")
      .agg(
          paper_count=count(),
          avg_citations=avg(col("Paper.citations")),
          top_paper=max_(col("Paper.title"))
      )
      .order_by("paper_count", ascending=False)
      .limit(20)
      .collect()
)
```

### Example 4: Hyperedge Query with Roles

```python
result = (
    hg.hyperedges("Event")
      .where_role("participant", hg.nodes("Person").filter(col("name") == "Alice"))
      .where_role("location", hg.nodes("Venue").filter(col("city") == "Boston"))
      .select("date", "description")
      .order_by("date")
      .collect()
)
```

### Example 5: Complex Filtering with String Operations

```python
result = (
    hg.nodes("Article")
      .filter(
          (col("year") >= 2020) &
          (col("year") <= 2023) &
          (col("citations") > 10) &
          col("title").is_not_null() &
          col("abstract").contains("machine learning")
      )
      .expand("AUTHORED_BY", to="Author", as_="author")
      .filter(col("author.affiliation").starts_with("MIT"))
      .select(
          title=col("Article.title"),
          author=col("author.name"),
          year=col("Article.year")
      )
      .limit(100)
      .collect()
)
```

### Example 6: Variable Length Path

```python
result = (
    hg.nodes("Person").filter(col("name") == "Alice")
      .expand("KNOWS", hops=(1, 3), as_="connection")
      .filter(col("connection.city") == "Boston")
      .select(
          person=col("connection.name"),
          distance=path_length(col("_path"))
      )
      .distinct()
      .collect()
)
```

### Example 7: Shortest Path

```python
from grism import shortest_path

paths = shortest_path(
    hg.nodes("Person").filter(col("name") == "Alice"),
    hg.nodes("Person").filter(col("name") == "Bob"),
    edge="KNOWS",
    max_hops=6
)

result = (
    paths
      .select(
          length=path_length(col("path")),
          nodes=nodes(col("path"))
      )
      .collect()
)
```

### Example 8: Pattern Matching

```python
from grism import Pattern

result = (
    hg.match(
        Pattern()
        .node("p", "Person")
        .edge("WORKS_AT")
        .node("c", "Company")
        .edge("LOCATED_IN")
        .node("city", "City", name="San Francisco")
    )
    .select(
        person=col("p.name"),
        company=col("c.name")
    )
    .collect()
)
```

### Example 9: Optional Match (Left Outer Join)

```python
result = (
    hg.nodes("Person")
      .optional_expand("HAS_SPOUSE", as_="spouse")
      .select(
          name=col("Person.name"),
          spouse_name=col("spouse.name").coalesce("Single")
      )
      .collect()
)
```

### Example 10: Array Expansion (UNWIND)

```python
result = (
    hg.nodes("Movie")
      .filter(col("languages").is_not_null())
      .explode("languages", as_="language")
      .group_by("language")
      .agg(movie_count=count())
      .order_by("movie_count", ascending=False)
      .limit(5)
      .collect()
)
```

### Example 11: Conditional Expression

```python
from grism import when

result = (
    hg.nodes("Person")
      .with_column(
          "age_group",
          when(col("age") < 18).then("minor")
              .when(col("age") < 65).then("adult")
              .otherwise("senior")
      )
      .group_by("age_group")
      .agg(count=count())
      .collect()
)
```

### Example 12: Bulk Data Import

```python
import pandas as pd

# Create nodes from DataFrame
people_df = pd.DataFrame({
    "name": ["Alice", "Bob", "Charlie"],
    "age": [30, 25, 35],
    "city": ["Boston", "NYC", "LA"]
})

hg = (
    Hypergraph.create("grism:///data/social")
      .insert_nodes("Person", people_df)
      .commit()
)

# Create edges
edges_df = pd.DataFrame({
    "source": [0, 1],  # Alice -> Bob, Bob -> Charlie
    "target": [1, 2],
    "since": [2020, 2021]
})

hg = hg.insert_edges("KNOWS", edges_df).commit()
```

---

## 17. DSL Grammar (Informal)

```
Frame := NodeFrame | EdgeFrame | HyperedgeFrame | PathFrame

NodeFrame := hg.nodes([label]) [.operation]*
EdgeFrame := hg.edges([label]) [.operation]*
HyperedgeFrame := hg.hyperedges([label]) [.operation]*
PathFrame := hg.match(pattern) [.operation]*

operation := filter(expr)
           | select(*columns, **aliases)
           | expand(edge, to=label, direction=direction, hops=hops, as_=alias)
           | optional_expand(...)
           | group_by(*keys)
           | order_by(*exprs, ascending=bool)
           | limit(n)
           | offset(n)
           | distinct()
           | union(frame)
           | intersect(frame)
           | except_(frame)
           | explode(column)
           | with_column(name, expr)
           | drop(*columns)
           | rename(mapping)
           | collect([executor])
           | explain([mode])

expr := col(name)
      | lit(value)
      | expr op expr
      | func(expr, ...)
      | expr.method(...)
      | when(condition).then(value)...

op := == | != | > | >= | < | <= | & | | | + | - | * | / | %

func := sim | distance | abs_ | ceil | floor | round_ | sqrt | power | log | exp
      | concat | substring | replace | split | trim | lower | upper | length
      | date | timestamp | year | month | day | hour | minute | second
      | coalesce | nullif | if_ | when
      | labels | type_ | id_ | properties | nodes | relationships | path_length
      | exists | any_ | all_ | none_

agg := count | count_distinct | sum_ | avg | min_ | max_ | collect | collect_distinct
     | first | last | stddev | variance | percentile | median | mode | string_agg
```

---

## 18. Column Resolution Examples

```python
# Example 1: Unqualified name (unique)
hg.nodes("Paper").filter(col("year") >= 2022)
# Resolves: Paper.year ✓

# Example 2: Unqualified name (ambiguous)
hg.nodes("Paper").expand("AUTHORED_BY", to="Author")
  .filter(col("name") == "Alice")  # Error: ambiguous (Paper.name? Author.name?)
# Must use: col("Author.name")

# Example 3: Qualified name (via label)
hg.nodes("Paper").expand("AUTHORED_BY", to="Author")
  .filter(col("Author.name") == "Alice")  # Resolves: Author.name ✓

# Example 4: Qualified name (via alias)
hg.nodes("Paper").expand("AUTHORED_BY", to="Author", as_="author")
  .filter(col("author.name") == "Alice")  # Resolves: author.name ✓

# Example 5: Edge properties
hg.nodes("Paper").expand("AUTHORED_BY", to="Author", edge_as="authored")
  .filter(col("authored.contribution") == "primary")  # ✓

# Example 6: After select (only selected columns available)
hg.nodes("Paper").expand("AUTHORED_BY", to="Author")
  .select("Paper.title", author_name=col("Author.name"))
  .filter(col("title") == "AI Paper")  # Resolves: Paper.title ✓
  .filter(col("author_name") == "Alice")  # Resolves: alias ✓
  .filter(col("Author.name") == "Alice")  # Error: not in schema after select
```

---

## 19. Type Coercion Rules

| Left Type | Right Type | Coercion | Result |
|-----------|------------|----------|--------|
| Int64 | Float64 | Int → Float | Float64 |
| Float64 | Int64 | Int → Float | Float64 |
| String | Int64 | None | Error |
| Int64 | String | None | Error |
| Any | Null | Keep left | Left type |
| Null | Any | Keep right | Right type |

Explicit coercion:

```python
col("age").cast("float")  # Int64 → Float64
col("score").cast("string")  # Float64 → String
```

---

## 20. Completion Criteria

- [x] Python API frozen with complete operator coverage
- [x] All frame types defined (NodeFrame, EdgeFrame, HyperedgeFrame, PathFrame)
- [x] Expression system complete with all operator categories
- [x] Aggregation functions defined
- [x] Pattern matching and path queries specified
- [x] Mutation API with transaction support
- [x] Execution backends documented
- [x] Column scoping rules documented
- [x] Type system semantics specified
- [x] Hypergraph-first model consistently applied
- [x] 12+ canonical examples provided
- [x] Aligned with RFC-0001, RFC-0002, RFC-0003, RFC-0017
- [x] Naming follows rfc-namings.md conventions
