
## Phase 0 – Python API Contract (Frozen v0.1)

> This section defines the **exact Python class and method surface** for Grism. This API is considered the **user contract** and should remain backward-compatible within v0.x.

### 0.1 Core Entry Point

```python
class HyperGraph:
    @staticmethod
    def connect(
        uri: str,
        *,
        executor: "Executor | str" = "local",
        namespace: str | None = None,
    ) -> "HyperGraph":
        """
        Connect to a Grism hypergraph.
        
        Args:
            uri: Connection URI (e.g., "grism://local", "grism://path/to/data")
            executor: Execution backend ("local" | "ray" | Executor instance)
            namespace: Optional namespace for logical graph isolation
            
        Returns:
            HyperGraph instance (immutable, lazy)
        """
        ...

    # namespace / logical graph
    def with_namespace(self, name: str) -> "HyperGraph":
        """
        Create a new HyperGraph scoped to a namespace.
        Returns a new instance; original unchanged.
        """
        ...

    # graph views
    def nodes(self, label: str | None = None) -> "NodeFrame":
        """
        Get nodes, optionally filtered by label.
        
        Args:
            label: Node label to filter by (None = all nodes)
            
        Returns:
            NodeFrame (lazy, immutable)
        """
        ...

    def edges(self, label: str | None = None) -> "EdgeFrame":
        """
        Get edges, optionally filtered by label.
        
        Args:
            label: Edge label to filter by (None = all edges)
            
        Returns:
            EdgeFrame (lazy, immutable)
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

    # execution
    def collect(self, *, executor: "Executor | str | None" = None):
        """
        Execute the query and return results.
        Not applicable on HyperGraph directly; use on frames.
        """
        raise TypeError("collect() must be called on a Frame, not HyperGraph")

    def explain(self, mode: str = "logical") -> str:
        """
        Explain the query plan.
        Not applicable on HyperGraph directly; use on frames.
        """
        raise TypeError("explain() must be called on a Frame, not HyperGraph")
```

---

### 0.2 Frame Base Class

```python
class GraphFrame:
    """
    Base class for all graph frames (NodeFrame, EdgeFrame, HyperedgeFrame).
    
    Properties:
        - Immutable: All operations return new frames
        - Lazy: No execution until .collect() or iteration
        - Typed: Schema information available via .schema property
    """
    
    @property
    def schema(self) -> "Schema":
        """
        Get the schema of this frame (available columns and types).
        May be partial if schema cannot be inferred statically.
        """
        ...
    
    # structural ops
    def filter(self, predicate: "Expr") -> "Self":
        """
        Filter rows based on a predicate expression.
        
        Args:
            predicate: Boolean expression (Expr that evaluates to bool)
            
        Returns:
            New GraphFrame with filtered rows
            
        Raises:
            TypeError: If predicate is not a boolean expression
            ColumnNotFoundError: If referenced columns don't exist
        """
        ...

    def select(self, *columns: str | "Expr", **aliases: "Expr") -> "Self":
        """
        Project columns (rename, compute expressions).
        
        Args:
            *columns: Column names or expressions to select
            **aliases: Keyword arguments for aliased columns
                      (e.g., name=col("Author.name"))
        
        Examples:
            .select("title", "year")
            .select(col("title"), col("year") * 2)
            .select(title=col("Paper.title"), author=col("Author.name"))
        
        Returns:
            New GraphFrame with selected columns only
            
        Note:
            After select(), only selected columns are available in subsequent operations.
        """
        ...

    def limit(self, n: int) -> "Self":
        """
        Limit the number of rows returned.
        
        Args:
            n: Maximum number of rows (must be positive)
            
        Returns:
            New GraphFrame with limit applied
            
        Note:
            Limit is applied after all filtering and expansion.
            Ordering is not guaranteed unless explicitly sorted (future feature).
        """
        ...

    # grouping
    def groupby(self, *keys: str | "Expr") -> "GroupedFrame":
        """
        Group rows by key expressions.
        
        Args:
            *keys: Column names or expressions to group by
        
        Returns:
            GroupedFrame for aggregation
            
        Examples:
            .groupby("author")
            .groupby(col("Author.name"), col("Author.affiliation"))
        """
        ...

    # execution
    def collect(
        self,
        *,
        executor: "Executor | str | None" = None,
        as_pandas: bool = False,
        as_arrow: bool = False,
    ) -> "DataFrame | pyarrow.Table | list[dict]":
        """
        Execute the query and return results.
        
        Args:
            executor: Override executor for this query (None = use default)
            as_pandas: Return pandas DataFrame (requires pandas)
            as_arrow: Return PyArrow Table
            Default: Return list of dicts
        
        Returns:
            Query results in requested format
            
        Raises:
            ExecutionError: If query execution fails
        """
        ...

    def explain(self, mode: str = "logical") -> str:
        """
        Explain the query plan.
        
        Args:
            mode: Explanation format
                - "logical": Logical plan tree
                - "physical": Physical execution plan
                - "gql": GraphQL-like representation
                - "cypher": Cypher query representation
                
        Returns:
            String representation of the plan
        """
        ...
    
    def __iter__(self):
        """
        Iterate over results (triggers execution).
        Equivalent to iter(collect()).
        """
        ...
```

---

### 0.3 NodeFrame

```python
class NodeFrame(GraphFrame):
    """
    Frame representing nodes in the graph.
    
    Properties:
        label: str | None - Node label filter (None = all labels)
    """
    
    label: str | None

    def expand(
        self,
        edge: str | None = None,
        *,
        to: str | None = None,
        direction: str = "out",  # "in" | "out" | "both"
        hops: int = 1,
        as_: str | None = None,
    ) -> "NodeFrame":
        """
        Expand to adjacent nodes via edges (graph traversal).
        
        Args:
            edge: Edge label to traverse (None = any edge)
            to: Target node label filter (None = any label)
            direction: Traversal direction
                - "out": Follow outgoing edges (default)
                - "in": Follow incoming edges
                - "both": Follow edges in both directions
            hops: Number of hops to traverse (default: 1)
            as_: Alias for the expanded node frame (for column references)
        
        Returns:
            New NodeFrame representing the expanded nodes
            
        Semantics:
            - Expansion replaces SQL joins; no explicit join() method
            - Multi-hop expansion (hops > 1) traverses paths of length N
            - After expansion, both original and expanded node columns are accessible
            - Edge properties are accessible via edge label (e.g., col("AUTHORED_BY.year"))
            - If multiple edges match, all are traversed (union semantics)
            
        Examples:
            # Single hop, outgoing
            hg.nodes("Paper").expand("AUTHORED_BY", to="Author")
            
            # Multi-hop
            hg.nodes("Person").expand("KNOWS", hops=2)
            
            # With alias
            hg.nodes("Paper").expand("AUTHORED_BY", to="Author", as_="author")
                .filter(col("author.name") == "Alice")
            
            # Access edge properties
            hg.nodes("Paper").expand("CITES")
                .filter(col("CITES.year") >= 2020)
        """
        ...
```

**Expansion Semantics Details**:

1. **Single-hop expansion** (`hops=1`):
   - Traverses edges directly connected to source nodes
   - Returns target nodes (or source nodes for `direction="in"`)

2. **Multi-hop expansion** (`hops=N`):
   - Traverses paths of exactly N edges
   - Intermediate nodes are not included in the result
   - Path properties are accessible via path expressions (future feature)

3. **Column scoping after expansion**:
   - Original frame columns remain accessible (e.g., `col("Paper.title")`)
   - Expanded node columns accessible via:
     - Alias: `col("author.name")` if `as_="author"`
     - Label: `col("Author.name")` if label is unique
     - Edge label: `col("AUTHORED_BY.year")` for edge properties
   - If label collision occurs, alias must be used

4. **Direction semantics**:
   - `"out"`: Source → Target (follow edges from source to target)
   - `"in"`: Target → Source (reverse traversal)
   - `"both"`: Union of both directions

---

### 0.4 EdgeFrame

```python
class EdgeFrame(GraphFrame):
    """
    Frame representing edges in the graph.
    
    Properties:
        label: str | None - Edge label filter (None = all labels)
    """
    
    label: str | None

    def endpoints(self, which: str = "target") -> "NodeFrame":
        """
        Get nodes connected by these edges.
        
        Args:
            which: Which endpoint to return
                - "source": Source nodes (for directed edges)
                - "target": Target nodes (for directed edges)
                - "both": All endpoints (union)
                
        Returns:
            NodeFrame containing the endpoint nodes
            
        Note:
            For hyperedges, use HyperedgeFrame.where_role() instead.
        """
        ...
    
    def source(self) -> "NodeFrame":
        """Convenience method for endpoints("source")."""
        ...
    
    def target(self) -> "NodeFrame":
        """Convenience method for endpoints("target")."""
        ...
```

---

### 0.5 HyperedgeFrame

```python
class HyperedgeFrame(GraphFrame):
    """
    Frame representing hyperedges (N-ary relationships).
    
    Properties:
        label: str | None - Hyperedge label filter (None = all labels)
    """
    
    label: str | None

    def where_role(
        self,
        role: str,
        value: str | "NodeFrame" | "Expr",
    ) -> "HyperedgeFrame":
        """
        Filter hyperedges where a role matches a value.
        
        Args:
            role: Role name to filter by
            value: Value to match (string, NodeFrame, or expression)
            
        Returns:
            New HyperedgeFrame with filtered hyperedges
            
        Examples:
            # Filter by role value (string)
            hg.hyperedges("Event").where_role("participant", "Alice")
            
            # Filter by role value (node frame)
            hg.hyperedges("Event").where_role("participant", hg.nodes("Person"))
            
            # Filter by role value (expression)
            hg.hyperedges("Event").where_role("participant", col("Person.name"))
        """
        ...
    
    def roles(self) -> "list[str]":
        """
        Get all role names present in this hyperedge frame.
        Returns empty list if schema is unknown.
        """
        ...
```

---

### 0.6 GroupedFrame

```python
class GroupedFrame:
    """
    Frame representing grouped rows (result of groupby()).
    
    Cannot be used directly; must call agg() to produce a GraphFrame.
    """
    
    def agg(self, **aggregations: "AggExpr") -> "GraphFrame":
        """
        Apply aggregations to grouped rows.
        
        Args:
            **aggregations: Aggregation expressions keyed by output column names
            
        Returns:
            GraphFrame with one row per group
            
        Examples:
            .groupby("author").agg(count=count(), total=sum(col("citations")))
            .groupby("year").agg(
                papers=count(),
                avg_citations=avg(col("citations")),
                top_paper=max(col("title"))
            )
        """
        ...
    
    def count(self) -> "GraphFrame":
        """
        Convenience method: count rows per group.
        Equivalent to .agg(count=count()).
        """
        ...
```

**Aggregation Semantics**:

1. **Grouping keys**: All grouping expressions become columns in the output
2. **Aggregation functions**: Applied per group, produce scalar values
3. **Null handling**: `NULL` values are ignored in aggregations (except `count(*)`)
4. **Empty groups**: Groups with no rows produce `NULL` for all aggregations

---

### 0.7 Expression System (Public API)

```python
class Expr:
    """
    Base class for all expressions.
    
    Expressions are immutable and composable.
    Evaluation happens at execution time.
    """
    
    def __and__(self, other: "Expr") -> "Expr":
        """Logical AND: self & other"""
        ...
    
    def __or__(self, other: "Expr") -> "Expr":
        """Logical OR: self | other"""
        ...
    
    def __invert__(self) -> "Expr":
        """Logical NOT: ~self"""
        ...
    
    def __eq__(self, other) -> "Expr":
        """Equality: self == other"""
        ...
    
    def __ne__(self, other) -> "Expr":
        """Inequality: self != other"""
        ...
    
    def __gt__(self, other) -> "Expr":
        """Greater than: self > other"""
        ...
    
    def __ge__(self, other) -> "Expr":
        """Greater than or equal: self >= other"""
        ...
    
    def __lt__(self, other) -> "Expr":
        """Less than: self < other"""
        ...
    
    def __le__(self, other) -> "Expr":
        """Less than or equal: self <= other"""
        ...
    
    def __add__(self, other) -> "Expr":
        """Addition: self + other"""
        ...
    
    def __sub__(self, other) -> "Expr":
        """Subtraction: self - other"""
        ...
    
    def __mul__(self, other) -> "Expr":
        """Multiplication: self * other"""
        ...
    
    def __truediv__(self, other) -> "Expr":
        """Division: self / other"""
        ...
    
    def __mod__(self, other) -> "Expr":
        """Modulo: self % other"""
        ...
    
    def is_null(self) -> "Expr":
        """Check if expression is NULL"""
        ...
    
    def is_not_null(self) -> "Expr":
        """Check if expression is not NULL"""
        ...
    
    def coalesce(self, *values) -> "Expr":
        """Return first non-NULL value"""
        ...


# Expression construction helpers

def col(name: str) -> Expr:
    """
    Reference a column by name.
    
    Args:
        name: Column name (qualified: "Label.column" or unqualified: "column")
        
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
        value: Python value (int, float, str, bool, None, list, etc.)
        
    Returns:
        Literal expression
        
    Examples:
        lit(42)
        lit("Alice")
        lit([1, 2, 3])
    """
    ...


def sim(left: Expr, right: Expr | "np.ndarray") -> Expr:
    """
    Compute similarity between two vectors (cosine similarity).
    
    Args:
        left: Expression evaluating to a vector
        right: Expression or numpy array
        
    Returns:
        Expression evaluating to similarity score (0.0 to 1.0)
        
    Examples:
        sim(col("embedding"), query_vector)
        sim(col("Paper.embedding"), col("Query.embedding"))
    """
    ...


def contains(expr: Expr, substring: str) -> Expr:
    """
    Check if string contains substring.
    
    Args:
        expr: Expression evaluating to a string
        substring: Substring to search for
        
    Returns:
        Boolean expression
    """
    ...


def cast(expr: Expr, target_type: str) -> Expr:
    """
    Cast expression to target type.
    
    Args:
        expr: Expression to cast
        target_type: Target type ("int", "float", "string", "bool", etc.)
        
    Returns:
        Cast expression
    """
    ...


def len_(expr: Expr) -> Expr:
    """
    Get length of array or string.
    
    Args:
        expr: Expression evaluating to array or string
        
    Returns:
        Integer expression
    """
    ...
```

---

### 0.8 Aggregations

```python
class AggExpr:
    """
    Aggregation expression (used in groupby().agg()).
    """
    ...


def count(expr: Expr | None = None) -> AggExpr:
    """
    Count rows (or non-NULL values if expr provided).
    
    Args:
        expr: Optional expression to count non-NULL values of
        
    Returns:
        Aggregation expression
        
    Examples:
        count()  # Count all rows
        count(col("author"))  # Count non-NULL authors
    """
    ...


def sum(expr: Expr) -> AggExpr:
    """
    Sum values.
    
    Args:
        expr: Numeric expression
        
    Returns:
        Aggregation expression
    """
    ...


def avg(expr: Expr) -> AggExpr:
    """
    Average values.
    
    Args:
        expr: Numeric expression
        
    Returns:
        Aggregation expression
    """
    ...


def min(expr: Expr) -> AggExpr:
    """
    Minimum value.
    
    Args:
        expr: Comparable expression
        
    Returns:
        Aggregation expression
    """
    ...


def max(expr: Expr) -> AggExpr:
    """
    Maximum value.
    
    Args:
        expr: Comparable expression
        
    Returns:
        Aggregation expression
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
```

---

### 0.9 Mutation API (Explicit)

```python
class HyperGraph:
    """
    Mutation operations are explicit and return new graph states.
    """
    
    def insert_node(
        self,
        label: str,
        properties: dict[str, Any] | None = None,
        *,
        id: int | None = None,
    ) -> "HyperGraph":
        """
        Insert a node.
        
        Args:
            label: Node label
            properties: Node properties (dict)
            id: Optional node ID (auto-generated if None)
            
        Returns:
            New HyperGraph instance with node inserted
            
        Note:
            Mutations are not immediately visible in the same transaction
            until committed (future feature).
        """
        ...
    
    def insert_edge(
        self,
        label: str,
        src: int | "NodeFrame",
        dst: int | "NodeFrame",
        properties: dict[str, Any] | None = None,
    ) -> "HyperGraph":
        """
        Insert an edge.
        
        Args:
            label: Edge label
            src: Source node ID or NodeFrame (must be single node)
            dst: Target node ID or NodeFrame (must be single node)
            properties: Edge properties (dict)
            
        Returns:
            New HyperGraph instance with edge inserted
        """
        ...
    
    def insert_hyperedge(
        self,
        label: str,
        roles: dict[str, int | "NodeFrame"],
        properties: dict[str, Any] | None = None,
    ) -> "HyperGraph":
        """
        Insert a hyperedge.
        
        Args:
            label: Hyperedge label
            roles: Role-to-node mapping (dict[str, node_id | NodeFrame])
            properties: Hyperedge properties (dict)
            
        Returns:
            New HyperGraph instance with hyperedge inserted
            
        Examples:
            hg.insert_hyperedge(
                "Event",
                roles={"participant": alice_id, "location": mit_id},
                properties={"date": "2024-01-01"}
            )
        """
        ...
    
    def delete_node(self, node_id: int) -> "HyperGraph":
        """Delete a node (and all connected edges)."""
        ...
    
    def delete_edge(self, edge_id: int) -> "HyperGraph":
        """Delete an edge."""
        ...
    
    def commit(self) -> "HyperGraph":
        """
        Commit pending mutations.
        Returns new HyperGraph instance with mutations applied.
        """
        ...
```

**Mutation Semantics**:

1. **Immutability**: All mutations return new `HyperGraph` instances
2. **Batching**: Multiple mutations can be chained before `commit()`
3. **Validation**: Node/edge existence checked at commit time
4. **Cascading**: Deleting a node deletes all connected edges

---

### 0.10 Execution Backends

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
    """
    def __init__(
        self,
        *,
        parallelism: int | None = None,  # None = auto-detect
        memory_limit: int | None = None,  # bytes
    ):
        """
        Args:
            parallelism: Number of parallel threads (None = CPU count)
            memory_limit: Memory limit in bytes (None = no limit)
        """
        ...


class RayExecutor(Executor):
    """
    Ray-distributed executor.
    """
    def __init__(
        self,
        *,
        num_workers: int | None = None,
        resources: dict[str, float] | None = None,
        **ray_config,
    ):
        """
        Args:
            num_workers: Number of Ray workers (None = auto-scale)
            resources: Resource requirements per task
            **ray_config: Additional Ray configuration
        """
        ...
```

---

### 0.11 Canonical Usage Examples

```python
# Example 1: Basic query
hg = HyperGraph.connect("grism://local")

result = (
    hg.nodes("Paper")
      .filter(col("year") >= 2022)
      .expand("CITES")
      .filter(sim(col("embedding"), query_emb) > 0.8)
      .select("title")
      .limit(10)
      .collect()
)

# Example 2: Multi-hop expansion with aliases
result = (
    hg.nodes("Person")
      .expand("KNOWS", hops=2, as_="friend")
      .filter(col("friend.age") > 25)
      .select("name", friend_name=col("friend.name"))
      .collect()
)

# Example 3: Aggregation
result = (
    hg.nodes("Paper")
      .expand("AUTHORED_BY", to="Author")
      .groupby("Author.name")
      .agg(
          paper_count=count(),
          avg_citations=avg(col("Paper.citations")),
          top_paper=max(col("Paper.title"))
      )
      .collect()
)

# Example 4: Hyperedge query
result = (
    hg.hyperedges("Event")
      .where_role("participant", hg.nodes("Person").filter(col("name") == "Alice"))
      .where_role("location", "MIT")
      .select("date", "description")
      .collect()
)

# Example 5: Complex filtering
result = (
    hg.nodes("Paper")
      .filter(
          (col("year") >= 2020) &
          (col("year") <= 2023) &
          (col("citations") > 10) &
          col("title").is_not_null()
      )
      .expand("AUTHORED_BY", to="Author", as_="author")
      .filter(col("author.affiliation") == "MIT")
      .select(
          title=col("Paper.title"),
          author=col("author.name"),
          year=col("Paper.year")
      )
      .limit(100)
      .collect()
)
```

---

### Phase 0 Completion Criteria

* Python API frozen
* 10 reference examples
* LogicalPlan lowering validated
* No execution logic in Python layer
* Column scoping rules documented and tested
* Type system semantics specified

---

## Phase 1.1 – Canonical Rust LogicalPlan (Frozen v0.1)

> This section defines the **canonical Rust logical representation** for Grism. All execution backends (local, Ray, future engines) must consume this plan **without semantic loss**.

---

### 1.1.1 Design Principles

* Execution-agnostic
* Deterministic & replayable
* Serializable (Serde)
* Graph-native (no SQL bias)
* Expression trees are immutable
* Type information preserved through plan

---

### 1.1.2 Core Identifiers

```rust
pub type NodeId = u64;
pub type EdgeId = u64;
pub type Label = String;
pub type Role = String;
pub type Column = String;
pub type Alias = String;
```

---

### 1.1.3 LogicalPlan Root

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogicalPlan {
    pub root: LogicalOp,
    pub schema: Option<Schema>, // Output schema (if known)
}
```

---

### 1.1.4 Logical Operators

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogicalOp {
    Scan(ScanOp),
    Expand(ExpandOp),
    Filter(FilterOp),
    Project(ProjectOp),
    Aggregate(AggregateOp),
    Limit(LimitOp),
    Infer(InferOp),
}
```

Each operator:

* Has exactly **one input** (except Scan)
* Forms a strict DAG (tree in v0.1)
* Carries schema information (input and output)

---

### 1.1.5 ScanOp

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScanOp {
    pub kind: ScanKind,
    pub label: Option<Label>,
    pub namespace: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ScanKind {
    Node,
    Edge,
    Hyperedge,
}
```

Semantics:

* Entry point of all plans
* No filtering here (pushdown happens later)
* Namespace scoping for multi-tenant scenarios

---

### 1.1.6 ExpandOp (Graph Primitive)

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExpandOp {
    pub input: Box<LogicalOp>,
    pub edge_label: Option<Label>,
    pub to_label: Option<Label>,
    pub direction: Direction,
    pub hops: u32,
    pub alias: Option<Alias>, // Binding name for expanded nodes
    pub edge_alias: Option<Alias>, // Binding name for edges (for edge properties)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Direction {
    In,
    Out,
    Both,
}
```

Invariants:

* `Expand` replaces joins
* Multi-hop is explicit (`hops` parameter)
* Alias introduces a new binding scope
* Edge alias allows accessing edge properties
* Schema after expand includes both input and expanded columns

**Column Binding After Expand**:

After an `ExpandOp`, the output schema contains:
1. All columns from `input` (with original qualifiers)
2. Columns from expanded nodes (qualified by `alias` or `to_label`)
3. Edge properties (qualified by `edge_alias` or `edge_label`)

---

### 1.1.7 FilterOp

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilterOp {
    pub input: Box<LogicalOp>,
    pub predicate: LogicalExpr, // Must evaluate to bool
}
```

Semantics:

* Predicate must be a boolean expression
* Three-valued logic: `true`, `false`, `NULL`
* Rows where predicate is `NULL` are filtered out (SQL-style)

---

### 1.1.8 ProjectOp

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectOp {
    pub input: Box<LogicalOp>,
    pub columns: Vec<Projection>, // Named expressions
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Projection {
    pub expr: LogicalExpr,
    pub alias: Option<String>, // Output column name
}
```

Semantics:

* Only projected columns are available after Project
* Column names can be aliased
* Expressions can reference any column from input schema

---

### 1.1.9 AggregateOp

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregateOp {
    pub input: Box<LogicalOp>,
    pub keys: Vec<LogicalExpr>, // Grouping keys
    pub aggs: Vec<AggExpr>, // Aggregations
}
```

Semantics:

* Grouping keys become columns in output
* One row per unique combination of grouping keys
* Aggregations applied per group

---

### 1.1.10 LimitOp

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LimitOp {
    pub input: Box<LogicalOp>,
    pub limit: usize,
    pub offset: Option<usize>, // For pagination (future)
}
```

Semantics:

* Limits number of rows returned
* Ordering not guaranteed (unless explicitly sorted - future feature)
* Applied after all filtering and expansion

---

### 1.1.11 InferOp (Reasoning Placeholder)

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InferOp {
    pub input: Box<LogicalOp>,
    pub rule_set: String, // Identifier for rule set
    pub materialize: bool, // Whether to materialize inferred edges
}
```

Semantics:

* Applies rule-based inference to input
* Can materialize inferred edges back into graph
* Rule sets defined separately (Datalog-style)

---

### 1.1.12 Expression System

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogicalExpr {
    Column(ColumnRef),
    Literal(Value),
    Binary {
        left: Box<LogicalExpr>,
        op: BinaryOp,
        right: Box<LogicalExpr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<LogicalExpr>,
    },
    Func(FuncExpr),
    Cast {
        expr: Box<LogicalExpr>,
        target_type: DataType,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum UnaryOp {
    Not, // Logical NOT
    Neg, // Numeric negation
    IsNull,
    IsNotNull,
}
```

---

### 1.1.13 ColumnRef

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnRef {
    pub qualifier: Option<String>, // Label, alias, or edge label
    pub name: String, // Column/property name
}

impl ColumnRef {
    pub fn resolve(&self, schema: &Schema) -> Result<ColumnId, ResolutionError> {
        // Resolution logic: check qualifier, then unqualified search
    }
}
```

**Resolution Algorithm**:

1. If `qualifier` is present:
   - Search for entity (label/alias) matching qualifier in schema
   - If found, resolve `name` within that entity's properties
   - If not found, return `ResolutionError::QualifierNotFound`

2. If `qualifier` is absent:
   - Search all entities in schema (reverse order of addition)
   - If exactly one match, return it
   - If multiple matches, return `ResolutionError::Ambiguous`
   - If no match, return `ResolutionError::NotFound`

---

### 1.1.14 BinaryOp

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BinaryOp {
    // Comparison
    Eq, Neq, Gt, Gte, Lt, Lte,
    // Logical
    And, Or,
    // Arithmetic
    Add, Sub, Mul, Div, Mod,
    // String
    Like, // Pattern matching (future)
    // Vector
    Similarity, // Special handling for vector similarity
}
```

---

### 1.1.15 Function Expressions

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FuncExpr {
    pub name: String,
    pub args: Vec<LogicalExpr>,
    pub return_type: Option<DataType>, // Inferred or explicit
}

// Built-in functions
pub enum BuiltinFunc {
    Sim, // Similarity
    Contains,
    Len,
    Coalesce,
    // ... more
}
```

Examples:

* `sim(a, b)` → `FuncExpr { name: "sim", args: [a, b] }`
* `contains(text, "LLM")` → `FuncExpr { name: "contains", args: [text, lit("LLM")] }`

---

### 1.1.16 Aggregation Expressions

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggExpr {
    pub func: AggFunc,
    pub expr: Option<LogicalExpr>, // None for count(*)
    pub alias: Option<String>, // Output column name
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect, // Array aggregation
    // ... more
}
```

---

### 1.1.17 Schema System

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<ColumnInfo>,
    pub entities: Vec<EntityInfo>, // Labels/aliases in scope
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub qualifier: Option<String>, // Entity qualifier
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntityInfo {
    pub name: String, // Label or alias
    pub kind: EntityKind, // Node, Edge, Hyperedge
    pub columns: Vec<String>, // Available properties
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EntityKind {
    Node,
    Edge,
    Hyperedge,
}
```

---

### 1.1.18 Type System

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataType {
    Int64,
    Float64,
    Bool,
    String,
    Binary,
    Vector(usize), // Dimension
    Timestamp,
    Date,
    Array(Box<DataType>), // Nested arrays
    Null, // Unknown type
}
```

**Type Inference Rules**:

1. **Literals**: Type inferred from Python value
2. **Columns**: Type from schema (if available)
3. **Binary ops**: Type promotion rules (e.g., `Int + Float → Float`)
4. **Functions**: Return type from function signature
5. **Aggregations**: Return type from aggregation function

---

### 1.1.19 Serialization Guarantees

* `serde_json` for debugging and human-readable formats
* `bincode` / Arrow IPC for execution (binary, efficient)
* Hash-stable for caching (deterministic serialization)
* Versioned schema for forward/backward compatibility

---

### 1.1.20 Example Lowering (Python → Rust)

```python
hg.nodes("Paper") \
  .filter(col("year") >= 2022) \
  .expand("CITES", to="Paper", as_="cited") \
  .filter(col("cited.year") >= 2020) \
  .select("Paper.title", cited_title=col("cited.title")) \
  .limit(10)
```

Lowered to:

```text
Limit(limit=10)
 └─ Project(columns=[
      Projection(expr=ColumnRef(qualifier="Paper", name="title"), alias=None),
      Projection(expr=ColumnRef(qualifier="cited", name="title"), alias="cited_title")
    ])
    └─ Filter(predicate=Binary(
         left=ColumnRef(qualifier="cited", name="year"),
         op=Gte,
         right=Literal(Int64(2020))
       ))
       └─ Expand(
            input=...,
            edge_label="CITES",
            to_label="Paper",
            alias="cited",
            direction=Out,
            hops=1
          )
          └─ Filter(predicate=Binary(
               left=ColumnRef(qualifier=None, name="year"),
               op=Gte,
               right=Literal(Int64(2022))
             ))
             └─ Scan(kind=Node, label="Paper")
```

---

### Phase 1.1 Completion Criteria

* LogicalPlan fully specified
* Python lowering produces this plan
* Optimizer consumes this plan
* No backend-specific logic present
* Column resolution algorithm specified
* Type system and inference rules documented
* Schema propagation through operators defined

---

## Appendix A: Python DSL Grammar (Informal)

```
Frame := NodeFrame | EdgeFrame | HyperedgeFrame
NodeFrame := hg.nodes([label]) [.operation]*
EdgeFrame := hg.edges([label]) [.operation]*
HyperedgeFrame := hg.hyperedges([label]) [.operation]*

operation := filter(expr)
           | select(*columns, **aliases)
           | expand(edge, to=label, direction=direction, hops=n, as_=alias)
           | groupby(*keys)
           | limit(n)
           | collect([executor])
           | explain([mode])

expr := col(name)
      | lit(value)
      | expr op expr
      | func(expr, ...)
      | expr.is_null()
      | expr.is_not_null()

op := == | != | > | >= | < | <= | & | | | + | - | * | /

func := sim | contains | cast | len | coalesce | ...

agg := count([expr]) | sum(expr) | avg(expr) | min(expr) | max(expr) | ...
```

---

## Appendix B: Column Resolution Examples

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
hg.nodes("Paper").expand("AUTHORED_BY", to="Author")
  .filter(col("AUTHORED_BY.year") >= 2020)  # Resolves: AUTHORED_BY.year ✓

# Example 6: After select (only selected columns available)
hg.nodes("Paper").expand("AUTHORED_BY", to="Author")
  .select("Paper.title", author_name=col("Author.name"))
  .filter(col("title") == "AI Paper")  # Resolves: Paper.title ✓
  .filter(col("author_name") == "Alice")  # Resolves: alias ✓
  .filter(col("Author.name") == "Alice")  # Error: not in schema after select
```

---

## Appendix C: Type Coercion Rules

| Left Type | Right Type | Coercion | Result |
|-----------|------------|----------|--------|
| Int64 | Float64 | Int → Float | Float64 |
| Float64 | Int64 | Int → Float | Float64 |
| String | Int64 | None | Error |
| Int64 | String | None | Error |
| Any | Null | Keep left | Left type |
| Null | Any | Keep right | Right type |

**Explicit Coercion**:
```python
cast(col("age"), "float")  # Int64 → Float64
cast(col("score"), "string")  # Float64 → String
```

---

## Appendix D: Future Extensions (Not in v0.1)

* **Ordering**: `.orderby()` for deterministic sorting
* **Deduplication**: `.distinct()` for removing duplicates
* **Union/Intersection**: Set operations on frames
* **Subqueries**: Nested queries in expressions
* **Window Functions**: `.over()` for analytical functions
* **Recursive Expansion**: `.expand_recursive()` for graph algorithms
* **Path Queries**: Path expressions and pattern matching
* **Temporal Queries**: Time-travel and version queries
* **Full-text Search**: `.search()` for text indexing
* **Graph Algorithms**: Built-in algorithms (PageRank, etc.)
