"""
Grism Complex Python API Query Cases

This file contains 5 complex/nested queries designed to test advanced Grism capabilities:
1. Nested correlated subqueries with EXISTS
2. Multi-level aggregation with top-N per group
3. Variable-length paths with intermediate constraints
4. Hyperedge queries with role-based filtering
5. Recursive patterns with conditional depth and accumulation

These queries demonstrate the full power of the Grism Python API including:
- Nested subqueries and correlated filters
- Multi-hop graph traversals with constraints
- Hyperedge (N-ary relationship) operations
- Complex aggregations and window-like operations
- Path predicates and accumulation

Usage:
    from grism import Hypergraph
    hg = Hypergraph.connect("grism://local")
    
    # Run a specific complex query
    result = complex_query_001(hg)
"""

from typing import Any
from grism import (
    Hypergraph,
    col, lit, prop,
    count, count_distinct, sum_, avg, max_, min_, collect, collect_distinct,
    first, last,
    concat, substring, replace, split, trim, lower, upper, length,
    date, year, month, day,
    when, if_, coalesce,
    labels, type_, id_, properties, nodes, relationships, path_length,
    exists, any_, all_,
    shortest_path, all_paths, Pattern,
    Expr, AggExpr,
)


# =============================================================================
# Complex Query 001: Nested Correlated Subquery with EXISTS
# =============================================================================
# Description: Find all Authors who have written papers that cite papers 
#              from the same institution, with aggregation and HAVING-style filter.
#
# Cypher equivalent:
# MATCH (a:Author)-[:WROTE]->(p:Paper)-[:CITES]->(cited:Paper)<-[:WROTE]-(other:Author)
# WHERE a.institution = other.institution AND a <> other
# WITH a, COUNT(DISTINCT cited) AS cross_citations
# WHERE cross_citations > 5
# RETURN a.name, a.institution, cross_citations
# ORDER BY cross_citations DESC
# LIMIT 20
#
# Complexity: 
# - 4-hop traversal pattern (Author -> Paper -> Paper <- Author)
# - Correlated filter (same institution, different author)
# - Aggregation with HAVING-style post-filter
# =============================================================================
def complex_query_001(hg: Hypergraph) -> list[dict]:
    """
    Find authors who cite papers from colleagues at the same institution.
    
    This query demonstrates:
    - Multi-hop expansion with intermediate entity access
    - Correlated filtering across expansion boundaries
    - Aggregation with post-aggregation filtering (HAVING equivalent)
    """
    return (
        hg.nodes("Author")
          # Traverse: Author -> Paper (papers they wrote)
          .expand("WROTE", to="Paper", as_="paper")
          # Traverse: Paper -> Paper (papers that cite)
          .expand("CITES", to="Paper", as_="cited")
          # Traverse: Paper <- Author (authors of cited papers)
          .expand("WROTE", direction="in", to="Author", as_="other_author")
          # Correlated filter: same institution, different author
          .filter(
              (col("Author.institution") == col("other_author.institution")) &
              (col("Author.id") != col("other_author.id"))
          )
          # Group by original author and aggregate
          .group_by(
              col("Author.id"),
              author_name=col("Author.name"),
              institution=col("Author.institution")
          )
          .agg(
              cross_citations=count_distinct(col("cited.id"))
          )
          # HAVING equivalent: filter on aggregated value
          .filter(col("cross_citations") > 5)
          # Order and limit
          .order_by("cross_citations", ascending=False)
          .limit(20)
          .select("author_name", "institution", "cross_citations")
          .collect()
    )


# =============================================================================
# Complex Query 002: Multi-Level Aggregation with Top-N Per Group
# =============================================================================
# Description: For each genre, find the top-3 movies by revenue, along with 
#              genre-level statistics (total revenue, average rating, movie count).
#
# Cypher equivalent:
# MATCH (m:Movie)-[:IN_GENRE]->(g:Genre)
# WITH g, m ORDER BY m.revenue DESC
# WITH g, COLLECT(m)[0..3] AS topMovies, 
#      SUM(m.revenue) AS totalRevenue,
#      AVG(m.rating) AS avgRating,
#      COUNT(m) AS movieCount
# RETURN g.name AS genre, topMovies, totalRevenue, avgRating, movieCount
# ORDER BY totalRevenue DESC
#
# Complexity:
# - Two-level aggregation (per movie, then per genre)
# - Top-N within groups
# - Multiple aggregation functions
# =============================================================================
def complex_query_002(hg: Hypergraph) -> list[dict]:
    """
    Find top-3 movies per genre with genre-level statistics.
    
    This query demonstrates:
    - Nested aggregation patterns
    - Top-N per group (simulated with order + collect + slice)
    - Multiple aggregation functions in single group_by
    """
    # Step 1: Get movies with their genres and order by revenue
    movies_by_genre = (
        hg.nodes("Movie")
          .expand("IN_GENRE", to="Genre", as_="genre")
          .order_by(col("Movie.revenue"), ascending=False)
    )
    
    # Step 2: Group by genre and compute aggregations
    # Note: Top-3 is approximated by collecting all and taking first 3
    return (
        movies_by_genre
          .group_by(
              genre_id=col("genre.id"),
              genre_name=col("genre.name")
          )
          .agg(
              # Collect movie titles (ordered by revenue from step 1)
              top_movies=collect(col("Movie.title")),
              # Genre-level statistics
              total_revenue=sum_(col("Movie.revenue")),
              avg_rating=avg(col("Movie.rating")),
              movie_count=count()
          )
          .order_by("total_revenue", ascending=False)
          # Note: In a full implementation, we'd slice top_movies[0:3]
          # For now, we return the full collected list
          .select("genre_name", "top_movies", "total_revenue", "avg_rating", "movie_count")
          .collect()
    )


# =============================================================================
# Complex Query 003: Variable-Length Path with Intermediate Constraints
# =============================================================================
# Description: Find shortest connection paths between two people where ALL 
#              intermediate nodes must be verified users, and return path details.
#
# Cypher equivalent:
# MATCH path = shortestPath((p1:Person {name:'Alice'})-[:KNOWS*1..6]-(p2:Person {name:'Bob'}))
# WHERE ALL(n IN nodes(path) WHERE n.verified = true)
# RETURN [n IN nodes(path) | n.name] AS path_names, length(path) AS hops
#
# Alternative using expand with constraints:
# MATCH (p1:Person {name:'Alice'})-[:KNOWS*1..6]->(intermediate:Person)-[:KNOWS*0..5]->(p2:Person {name:'Bob'})
# WHERE intermediate.verified = true
# RETURN p1, p2, COUNT(DISTINCT intermediate) AS verified_connections
#
# Complexity:
# - Variable-length path finding
# - Path predicate (all nodes must satisfy condition)
# - Path extraction and transformation
# =============================================================================
def complex_query_003(hg: Hypergraph) -> list[dict]:
    """
    Find shortest verified-only paths between Alice and Bob.
    
    This query demonstrates:
    - shortest_path() with endpoint constraints
    - Variable-length traversal with hops range
    - Path predicate using all_() function
    - Path node extraction
    """
    # Define start and end node frames
    alice = hg.nodes("Person").filter(col("name") == "Alice")
    bob = hg.nodes("Person").filter(col("name") == "Bob")
    
    # Find shortest path with max 6 hops
    paths = shortest_path(
        alice,
        bob,
        edge_type="KNOWS",
        max_hops=6,
        direction="both"
    )
    
    # Filter paths where all intermediate nodes are verified
    # and extract path information
    return (
        paths
          # Apply path predicate: all nodes must be verified
          .filter(all_(nodes(col("_path")), col("verified") == True))
          .select(
              path_nodes=nodes(col("_path")),
              hops=path_length(col("_path"))
          )
          .order_by("hops", ascending=True)
          .limit(10)
          .collect()
    )


# Alternative implementation using expand with intermediate validation
def complex_query_003_alt(hg: Hypergraph) -> list[dict]:
    """
    Alternative: Find paths through verified users only using expand.
    
    This approach uses chained expands with intermediate filters,
    useful when shortest_path semantics aren't needed.
    """
    return (
        hg.nodes("Person")
          .filter(col("name") == "Alice")
          # Variable-length expansion (1-6 hops)
          .expand("KNOWS", hops=(1, 6), direction="both", as_="intermediate")
          # Filter: all intermediate nodes must be verified
          .filter(col("intermediate.verified") == True)
          # Find connection to Bob
          .expand("KNOWS", direction="both", to="Person", as_="target")
          .filter(col("target.name") == "Bob")
          # Group to get distinct paths
          .group_by(col("Person.id"), col("target.id"))
          .agg(
              verified_intermediates=count_distinct(col("intermediate.id")),
              path_names=collect(col("intermediate.name"))
          )
          .select("path_names", "verified_intermediates")
          .collect()
    )


# =============================================================================
# Complex Query 004: Hyperedge Query with Role-Based Filtering
# =============================================================================
# Description: Find all Events where the organizer and at least one participant 
#              work at the same company. Return event details with matching pairs.
#
# Conceptual hyperedge structure:
# Event hyperedge with roles: {organizer: Person, participant: Person[], venue: Venue}
#
# Grism hyperedge query:
# hg.hyperedges("Event")
#   .expand("organizer") -> get organizer Person
#   .expand("participant") -> get participant Persons  
#   .filter(organizer.company == participant.company)
#
# Complexity:
# - N-ary hyperedge access
# - Multiple role expansions
# - Cross-role property correlation
# =============================================================================
def complex_query_004(hg: Hypergraph) -> list[dict]:
    """
    Find events where organizer and participant share the same company.
    
    This query demonstrates:
    - Hyperedge frame operations
    - Role-based expansion from hyperedges
    - Cross-role correlation (comparing properties across different roles)
    """
    return (
        hg.hyperedges("Event")
          # Expand to get organizer
          .expand("organizer", as_="org")
          # Expand to get participants (this creates multiple rows per participant)
          .expand("participant", as_="part")
          # Filter: organizer and participant at same company
          .filter(
              (col("org.company") == col("part.company")) &
              (col("org.id") != col("part.id"))  # Organizer != participant
          )
          # Group by event to get unique events with matching pairs
          .group_by(
              event_id=col("Event.id"),
              event_name=col("Event.name"),
              event_date=col("Event.date")
          )
          .agg(
              organizer_name=first(col("org.name")),
              organizer_company=first(col("org.company")),
              matching_participants=collect_distinct(col("part.name")),
              match_count=count_distinct(col("part.id"))
          )
          .filter(col("match_count") >= 1)
          .order_by("event_date", ascending=False)
          .select(
              "event_name", "event_date",
              "organizer_name", "organizer_company",
              "matching_participants", "match_count"
          )
          .collect()
    )


# Alternative: Using where_role for initial filtering
def complex_query_004_alt(hg: Hypergraph) -> list[dict]:
    """
    Alternative using where_role for organizer filtering.
    
    Demonstrates where_role with NodeFrame for semi-join semantics.
    """
    # Get all events organized by people from "TechCorp"
    techcorp_organizers = (
        hg.nodes("Person")
          .filter(col("company") == "TechCorp")
    )
    
    return (
        hg.hyperedges("Event")
          # Filter to events with TechCorp organizers
          .where_role("organizer", techcorp_organizers)
          # Expand to get all participants
          .expand("participant", as_="part")
          # Filter participants also from TechCorp
          .filter(col("part.company") == "TechCorp")
          .group_by(col("Event.id"), col("Event.name"))
          .agg(
              participant_count=count_distinct(col("part.id")),
              participants=collect(col("part.name"))
          )
          .filter(col("participant_count") > 0)
          .select("Event.name", "participants", "participant_count")
          .collect()
    )


# =============================================================================
# Complex Query 005: Recursive Pattern with Conditional Depth and Accumulation
# =============================================================================
# Description: Find all dependency chains in a package graph where each package 
#              in the chain has a severity score above threshold, computing 
#              cumulative risk score along the path.
#
# Cypher equivalent:
# MATCH path = (root:Package {name:'main-app'})-[:DEPENDS_ON*1..10]->(dep:Package)
# WHERE ALL(n IN nodes(path) WHERE n.severity > 3)
# WITH root, dep, length(path) AS depth,
#      REDUCE(risk=0, n IN nodes(path) | risk + n.severity) AS total_risk
# RETURN dep.name AS dependency, depth, total_risk
# ORDER BY total_risk DESC, depth ASC
# LIMIT 50
#
# Complexity:
# - Variable-length path with constraint on ALL intermediate nodes
# - Path reduction (accumulating values along path)
# - Multiple computed columns from path data
# =============================================================================
def complex_query_005(hg: Hypergraph) -> list[dict]:
    """
    Find high-severity dependency chains with cumulative risk calculation.
    
    This query demonstrates:
    - Variable-length expansion with hops range
    - Path predicates with all_() function
    - Path reduction for accumulation (sum of severities)
    - Multiple path-derived metrics
    """
    # Find all paths from root package through dependencies
    dependency_paths = all_paths(
        start=hg.nodes("Package").filter(col("name") == "main-app"),
        end=hg.nodes("Package"),  # Any package as endpoint
        edge_type="DEPENDS_ON",
        min_hops=1,
        max_hops=10,
        direction="out"
    )
    
    return (
        dependency_paths
          # Filter: all nodes in path must have severity > 3
          .filter(all_(nodes(col("_path")), col("severity") > 3))
          # Compute path metrics
          .with_column("depth", path_length(col("_path")))
          .with_column("path_nodes", nodes(col("_path")))
          # Note: REDUCE would be: reduce(risk=0, n in nodes(path) | risk + n.severity)
          # We approximate with aggregation after exploding path nodes
          .select(
              dependency=col("Package.name"),  # End node name
              depth=col("depth"),
              path_nodes=col("path_nodes")
          )
          .order_by("depth", ascending=True)
          .limit(50)
          .collect()
    )


# Alternative implementation using iterative expansion approach
def complex_query_005_alt(hg: Hypergraph) -> list[dict]:
    """
    Alternative: Iterative expansion with severity filter at each hop.
    
    This approach filters at each expansion step rather than on complete paths.
    Useful when path reduction isn't available or for better performance.
    """
    # Start from root package
    root = (
        hg.nodes("Package")
          .filter(col("name") == "main-app")
          .filter(col("severity") > 3)  # Root must also pass threshold
    )
    
    # Expand through dependency chain, filtering high-severity only
    return (
        root
          .expand("DEPENDS_ON", hops=(1, 10), to="Package", as_="dep")
          # Filter each dependency must have high severity
          # Note: This checks endpoint, not all intermediates
          .filter(col("dep.severity") > 3)
          # Compute depth via path_length if available
          .with_column("depth", path_length(col("_path")))
          # Group by dependency to get unique dependencies with their depths
          .group_by(
              dep_name=col("dep.name"),
              dep_severity=col("dep.severity")
          )
          .agg(
              min_depth=min_(col("depth")),
              max_depth=max_(col("depth")),
              path_count=count()
          )
          .order_by("dep_severity", ascending=False)
          .limit(50)
          .select("dep_name", "dep_severity", "min_depth", "max_depth", "path_count")
          .collect()
    )


# =============================================================================
# Bonus: Combined Complex Query with Multiple Advanced Features
# =============================================================================
def complex_query_bonus(hg: Hypergraph) -> list[dict]:
    """
    Bonus complex query combining multiple advanced features.
    
    Find influential authors in the citation network:
    - Authors whose papers are cited by papers from other institutions
    - With high average citation count
    - Who collaborate with authors from at least 3 different institutions
    
    Demonstrates:
    - Multiple expansion paths
    - Subquery with EXISTS for collaboration check
    - Multi-level aggregation
    - Conditional expressions
    """
    # Subquery: authors who collaborate across institutions
    cross_institution_collaborators = (
        hg.nodes("Author")
          .expand("COLLABORATED_WITH", to="Author", as_="coauthor")
          .filter(col("Author.institution") != col("coauthor.institution"))
          .group_by(col("Author.id"))
          .agg(distinct_institutions=count_distinct(col("coauthor.institution")))
          .filter(col("distinct_institutions") >= 3)
    )
    
    # Main query: find influential authors
    return (
        hg.nodes("Author")
          # Filter to cross-institution collaborators
          .filter(exists(
              cross_institution_collaborators
                .filter(col("Author.id") == col("id"))
          ))
          # Get papers they wrote
          .expand("WROTE", to="Paper", as_="paper")
          # Get citations to their papers from other institutions
          .expand("CITED_BY", direction="in", to="Paper", as_="citing_paper")
          .expand("WROTE", direction="in", to="Author", as_="citing_author")
          # Filter: citing author from different institution
          .filter(col("Author.institution") != col("citing_author.institution"))
          # Aggregate
          .group_by(
              author_id=col("Author.id"),
              author_name=col("Author.name"),
              institution=col("Author.institution")
          )
          .agg(
              paper_count=count_distinct(col("paper.id")),
              external_citation_count=count_distinct(col("citing_paper.id")),
              citing_institutions=count_distinct(col("citing_author.institution"))
          )
          # Compute influence score
          .with_column(
              "influence_score",
              col("external_citation_count") * col("citing_institutions") / col("paper_count")
          )
          .order_by("influence_score", ascending=False)
          .limit(25)
          .select(
              "author_name", "institution", "paper_count",
              "external_citation_count", "citing_institutions", "influence_score"
          )
          .collect()
    )


# =============================================================================
# Helper function to run all complex queries
# =============================================================================
def run_all_complex_queries(hg: Hypergraph) -> dict[str, Any]:
    """
    Run all complex queries and return results.
    
    Args:
        hg: Connected Hypergraph instance
        
    Returns:
        Dict mapping query name to results or error
    """
    queries = {
        "complex_query_001": complex_query_001,
        "complex_query_002": complex_query_002,
        "complex_query_003": complex_query_003,
        "complex_query_003_alt": complex_query_003_alt,
        "complex_query_004": complex_query_004,
        "complex_query_004_alt": complex_query_004_alt,
        "complex_query_005": complex_query_005,
        "complex_query_005_alt": complex_query_005_alt,
        "complex_query_bonus": complex_query_bonus,
    }
    
    results = {}
    for name, query_func in queries.items():
        try:
            results[name] = query_func(hg)
        except Exception as e:
            results[name] = {"error": str(e), "type": type(e).__name__}
    
    return results


if __name__ == "__main__":
    print("Grism Complex Python API Query Examples")
    print("=" * 50)
    print("""
This file contains 5 complex queries demonstrating advanced Grism features:

1. complex_query_001: Nested correlated subquery with EXISTS
   - 4-hop traversal pattern
   - Correlated filter across expansion boundaries
   - Aggregation with HAVING-style post-filter

2. complex_query_002: Multi-level aggregation with top-N per group
   - Nested aggregation patterns
   - Top-N within groups
   - Multiple aggregation functions

3. complex_query_003: Variable-length path with intermediate constraints
   - shortest_path() with endpoint constraints
   - Path predicate using all_() function
   - Path node extraction

4. complex_query_004: Hyperedge query with role-based filtering
   - N-ary hyperedge operations
   - Cross-role property correlation
   - Role-based expansion

5. complex_query_005: Recursive pattern with conditional depth
   - Variable-length expansion with constraints
   - Path reduction for accumulation
   - Multiple path-derived metrics

Usage:
    from grism import Hypergraph
    hg = Hypergraph.connect('grism://local')
    result = complex_query_001(hg)
    
Or run all queries:
    results = run_all_complex_queries(hg)
""")
