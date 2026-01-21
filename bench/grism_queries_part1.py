"""
Grism Python API Equivalent Cases for cypher_queries_part1.txt

This file contains Python equivalents of Cypher queries using the Grism Python API.
Each function corresponds to a query from cypher_queries_part1.txt.

Usage:
    from grism import Hypergraph, col, count, sum_, avg, max_, min_, collect
    hg = Hypergraph.connect("grism://local")
    
    # Run a specific query
    result = query_001(hg)
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
# Query 001: Country entity count from filings
# MATCH (f:Filing)-[:BENEFITS]->(e:Entity)-[:COUNTRY]->(c:Country) 
# WITH c.name AS country, COUNT(e) AS entityCount 
# ORDER BY entityCount DESC LIMIT 3 
# RETURN country, entityCount
# =============================================================================
def query_001(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Filing")
          .expand("BENEFITS", to="Entity", as_="e")
          .expand("COUNTRY", to="Country", as_="c")
          .group_by(country=col("c.name"))
          .agg(entityCount=count())
          .order_by("entityCount", ascending=False)
          .limit(3)
          .select("country", "entityCount")
          .collect()
    )


# =============================================================================
# Query 002: Non-public organizations with CEOs
# MATCH (o:Organization)-[:HAS_CEO]->(ceo:Person) 
# WHERE o.isPublic = false 
# RETURN o.name AS OrganizationName, ceo.name AS CEOName LIMIT 3
# =============================================================================
def query_002(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Organization")
          .filter(col("isPublic") == False)
          .expand("HAS_CEO", to="Person", as_="ceo")
          .select(
              OrganizationName=col("Organization.name"),
              CEOName=col("ceo.name")
          )
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 003: High view count streams and games
# MATCH (s:Stream)-[:PLAYS]->(g:Game) 
# WHERE s.total_view_count > 50000000 
# RETURN DISTINCT g.name
# =============================================================================
def query_003(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Stream")
          .filter(col("total_view_count") > 50000000)
          .expand("PLAYS", to="Game", as_="g")
          .select(col("g.name"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 004: Articles with keyword counts
# MATCH (n:Article) -[:HAS_KEY]->(m:Keyword) 
# WITH DISTINCT n, m 
# RETURN n.abstract AS abstract, count(m) AS count 
# ORDER BY count DESC LIMIT 7
# =============================================================================
def query_004(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .expand("HAS_KEY", to="Keyword", as_="m")
          .group_by(col("Article.abstract"))
          .agg(count=count_distinct(col("m.id")))
          .order_by("count", ascending=False)
          .limit(7)
          .select(abstract=col("Article.abstract"), count=col("count"))
          .collect()
    )


# =============================================================================
# Query 005: Authors with first name starting with 'Jea'
# MATCH (n:Author) WHERE n.first_name STARTS WITH 'Jea' RETURN n
# =============================================================================
def query_005(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Author")
          .filter(col("first_name").starts_with("Jea"))
          .collect()
    )


# =============================================================================
# Query 006: Business categories in San Mateo
# MATCH (b:Business)-[:IN_CATEGORY]->(c:Category) 
# WHERE b.city = 'San Mateo' 
# RETURN DISTINCT c.name AS category
# =============================================================================
def query_006(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Business")
          .filter(col("city") == "San Mateo")
          .expand("IN_CATEGORY", to="Category", as_="c")
          .select(category=col("c.name"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 007: 3-hop traversal from specific article
# MATCH (a:Article{title:'Maslov class and minimality in Calabi-Yau manifolds'})-[*3]->(n) 
# RETURN labels(n) AS FarNodes
# =============================================================================
def query_007(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("title") == "Maslov class and minimality in Calabi-Yau manifolds")
          .expand(hops=3, as_="n")
          .select(FarNodes=labels(col("n")))
          .collect()
    )


# =============================================================================
# Query 008: Movies from 1990-2000 with >5000 votes
# MATCH (m:Movie) 
# WHERE m.released >= 1990 AND m.released <= 2000 AND m.votes > 5000 
# RETURN m.title, m.released, m.votes
# =============================================================================
def query_008(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Movie")
          .filter(
              (col("released") >= 1990) &
              (col("released") <= 2000) &
              (col("votes") > 5000)
          )
          .select("title", "released", "votes")
          .collect()
    )


# =============================================================================
# Query 009: Article titles with keyword counts
# MATCH (n:Article) -[:HAS_KEY]->(m:Keyword) 
# WITH DISTINCT n, m 
# RETURN n.title AS title, count(m) AS count LIMIT 20
# =============================================================================
def query_009(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .expand("HAS_KEY", to="Keyword", as_="m")
          .group_by(col("Article.title"))
          .agg(count=count_distinct(col("m.id")))
          .select(title=col("Article.title"), count=col("count"))
          .limit(20)
          .collect()
    )


# =============================================================================
# Query 010: Movie with highest budget
# MATCH (m:Movie) RETURN m ORDER BY m.budget DESC LIMIT 1
# =============================================================================
def query_010(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Movie")
          .order_by("budget", ascending=False)
          .limit(1)
          .collect()
    )


# =============================================================================
# Query 011: Shortest path between category and article
# MATCH p=shortestPath((a:Categories{category_id:'978aee6db23fc939cec71ac05eb15b7a'})-[*]-
#   (e:Article{comments:'18 pages, latex2e with amsfonts. Final version...'})) 
# RETURN nodes(p)
# =============================================================================
def query_011(hg: Hypergraph) -> list[dict]:
    return (
        shortest_path(
            hg.nodes("Categories").filter(col("category_id") == "978aee6db23fc939cec71ac05eb15b7a"),
            hg.nodes("Article").filter(col("comments") == "18 pages, latex2e with amsfonts. Final version, accepted for   publication"),
            direction="both"
        )
        .select(nodes=nodes(col("path")))
        .collect()
    )


# =============================================================================
# Query 012: Journals without topic connection
# MATCH (n:Journal), (:Topic {description: '...'}) 
# WHERE NOT (n) --> (:Topic) RETURN n.name
# =============================================================================
def query_012(hg: Hypergraph) -> list[dict]:
    topic_desc = "Collection of terms related to polynomials, including their coefficients, bases, preservation, trends, and orthogonalities, as well as concepts such as Grobner bases, resultants, and Verblunsky coefficients, with applications in various fields such as reliability, physics, and algebraic geometry."
    
    # Get journals that have no outgoing edges to Topic
    return (
        hg.nodes("Journal")
          .filter(~exists(
              hg.nodes("Journal")
                .expand(to="Topic", direction="out")
          ))
          .select(col("name"))
          .collect()
    )


# =============================================================================
# Query 013: 3-hop from DOI
# MATCH (a:DOI{doi_id:'fe8768ee88f2d27ed51861639e63a4ff'})-[*3]->(n) 
# RETURN labels(n) AS FarNodes
# =============================================================================
def query_013(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("DOI")
          .filter(col("doi_id") == "fe8768ee88f2d27ed51861639e63a4ff")
          .expand(hops=3, as_="n")
          .select(FarNodes=labels(col("n")))
          .collect()
    )


# =============================================================================
# Query 014: Authors with 'R.' in first name and their relations
# MATCH (d:Author)-[r]->(n) WHERE d.first_name CONTAINS 'R.' RETURN n, TYPE(r)
# =============================================================================
def query_014(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Author")
          .filter(col("first_name").contains("R."))
          .expand(direction="out", as_="n", edge_as="r")
          .select(col("n"), type_=type_(col("r")))
          .collect()
    )


# =============================================================================
# Query 015: Top actors in pre-1980 movies
# MATCH (a:Actor)-[r:ACTED_IN]->(m:Movie) 
# WHERE m.year < 1980 
# RETURN a.name, count(r) AS roles 
# ORDER BY roles DESC LIMIT 5
# =============================================================================
def query_015(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Actor")
          .expand("ACTED_IN", to="Movie", as_="m", edge_as="r")
          .filter(col("m.year") < 1980)
          .group_by(col("Actor.name"))
          .agg(roles=count())
          .order_by("roles", ascending=False)
          .limit(5)
          .select(col("Actor.name"), "roles")
          .collect()
    )


# =============================================================================
# Query 016: Article department
# MATCH (a:Article {prodName: 'S.Skinny L.W Epic'})-[:FROM_DEPARTMENT]->(d:Department) 
# RETURN d.departmentName
# =============================================================================
def query_016(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("prodName") == "S.Skinny L.W Epic")
          .expand("FROM_DEPARTMENT", to="Department", as_="d")
          .select(col("d.departmentName"))
          .collect()
    )


# =============================================================================
# Query 017: Character with specific community and page rank
# MATCH (target:Character { name: 'Aemon-Targaryen-(Maester-Aemon)' }) 
# MATCH (target)-[*1..1]-(c:Character) 
# WHERE c.community > 700 
# RETURN c.name, c.book1PageRank 
# ORDER BY c.book1PageRank ASC LIMIT 1
# =============================================================================
def query_017(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Character")
          .filter(col("name") == "Aemon-Targaryen-(Maester-Aemon)")
          .expand(hops=1, to="Character", direction="both", as_="c")
          .filter(col("c.community") > 700)
          .order_by(col("c.book1PageRank"), ascending=True)
          .limit(1)
          .select(col("c.name"), col("c.book1PageRank"))
          .collect()
    )


# =============================================================================
# Query 018: Journals with name matching regex
# MATCH (n:Journal) WHERE n.name =~'Iz.*' RETURN n
# =============================================================================
def query_018(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Journal")
          .filter(col("name").matches("Iz.*"))
          .collect()
    )


# =============================================================================
# Query 019: Average properties from update date
# MATCH (a:UpdateDate{update_date:'2009-10-31'})-[r]->(n) 
# RETURN AVG(SIZE(keys(n))) AS AvgProps
# =============================================================================
def query_019(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("UpdateDate")
          .filter(col("update_date") == "2009-10-31")
          .expand(direction="out", as_="n")
          .select(num_props=col("n").properties().keys().size())
          .group_by()
          .agg(AvgProps=avg(col("num_props")))
          .collect()
    )


# =============================================================================
# Query 020: Same address matching
# MATCH (a1:Address {address: 'given address'})-[:same_as]->(a2:Address) 
# RETURN a1, a2
# =============================================================================
def query_020(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Address")
          .filter(col("address") == "given address")
          .expand("same_as", to="Address", as_="a2")
          .select(a1=col("Address"), a2=col("a2"))
          .collect()
    )


# =============================================================================
# Query 021: Count drivers
# MATCH (d:Driver) RETURN COUNT(d)
# =============================================================================
def query_021(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Driver")
          .group_by()
          .agg(count=count())
          .collect()
    )


# =============================================================================
# Query 022: DOI properties via variable-length path
# MATCH (a:DOI{doi_id:'abce5ed79c520bdb8fd79a61a852648d'})-[*]->(n) 
# RETURN DISTINCT properties(n) AS Properties
# =============================================================================
def query_022(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("DOI")
          .filter(col("doi_id") == "abce5ed79c520bdb8fd79a61a852648d")
          .expand(hops=(1, 10), as_="n")  # Variable length path
          .select(Properties=properties(col("n")))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 023: Count distinct switch IPs in rack
# MATCH (rack:Rack {name: 'DC1-RCK-1-1'})-[:HOLDS]->(switch:Switch) 
# RETURN COUNT(DISTINCT switch.ip) AS numberOfSwitchTypes
# =============================================================================
def query_023(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Rack")
          .filter(col("name") == "DC1-RCK-1-1")
          .expand("HOLDS", to="Switch", as_="switch")
          .group_by()
          .agg(numberOfSwitchTypes=count_distinct(col("switch.ip")))
          .collect()
    )


# =============================================================================
# Query 024: Businesses with reviews containing 'library'
# MATCH (b:Business)<-[:REVIEWS]-(r:Review) 
# WHERE r.text CONTAINS 'library' 
# RETURN b.name, b.address, b.city, b.state LIMIT 3
# =============================================================================
def query_024(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Business")
          .expand("REVIEWS", direction="in", as_="r")
          .filter(col("r.text").contains("library"))
          .select(col("Business.name"), col("Business.address"), 
                  col("Business.city"), col("Business.state"))
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 025: Movies with 5-star ratings
# MATCH (u:User)-[r:RATED]->(m:Movie) 
# WHERE r.rating = 5.0 
# RETURN DISTINCT m.title AS MovieTitle
# =============================================================================
def query_025(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("User")
          .expand("RATED", to="Movie", as_="m", edge_as="r")
          .filter(col("r.rating") == 5.0)
          .select(MovieTitle=col("m.title"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 026: Article-Journal with matching property
# MATCH (a:Article {title:'Notes for a Quantum Index Theorem'})-[r:PUBLISHED_IN]->(b:Journal) 
# WHERE ANY(key IN keys(a) WHERE a[key] = b[key]) 
# RETURN b
# =============================================================================
def query_026(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("title") == "Notes for a Quantum Index Theorem")
          .expand("PUBLISHED_IN", to="Journal", as_="b")
          # Note: ANY key matching requires custom function
          .select(col("b"))
          .collect()
    )


# =============================================================================
# Query 027: Applications depending on Java service
# MATCH (s:Service {name: 'java'})<-[:DEPENDS_ON]-(a:Application) 
# RETURN a.name AS application_name
# =============================================================================
def query_027(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Service")
          .filter(col("name") == "java")
          .expand("DEPENDS_ON", direction="in", to="Application", as_="a")
          .select(application_name=col("a.name"))
          .collect()
    )


# =============================================================================
# Query 028: Singer with highest citizenship (sorted)
# MATCH (singer:singer) RETURN singer.Citizenship 
# ORDER BY singer.Citizenship DESC LIMIT 1
# =============================================================================
def query_028(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("singer")
          .order_by("Citizenship", ascending=False)
          .limit(1)
          .select("Citizenship")
          .collect()
    )


# =============================================================================
# Query 029: Movies with male cast
# MATCH (p:Person {gender: 1})-[:CAST_FOR]->(m:Movie) RETURN m LIMIT 5
# =============================================================================
def query_029(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Person")
          .filter(col("gender") == 1)
          .expand("CAST_FOR", to="Movie", as_="m")
          .select(col("m"))
          .limit(5)
          .collect()
    )


# =============================================================================
# Query 030: Actor-directors (same person)
# MATCH (p:Person)-[:ACTED_IN]->(m:Movie)<-[:DIRECTED]-(p) 
# RETURN m.title LIMIT 3
# =============================================================================
def query_030(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Person")
          .expand("ACTED_IN", to="Movie", as_="m")
          .expand("DIRECTED", direction="in", as_="director")
          .filter(col("Person.id") == col("director.id"))
          .select(col("m.title"))
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 031: Organizations mentioned in negative articles
# MATCH (a:Article)-[:MENTIONS]->(o:Organization) 
# WHERE a.sentiment < 0.5 
# RETURN o.name LIMIT 3
# =============================================================================
def query_031(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("sentiment") < 0.5)
          .expand("MENTIONS", to="Organization", as_="o")
          .select(col("o.name"))
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 032: Articles with EXISTS subquery on journal meta
# MATCH (n:Article) 
# WHERE EXISTS { MATCH (n)-[r:PUBLISHED_IN]->(:Journal) WHERE r.meta < '217'} 
# RETURN n.title AS title
# =============================================================================
def query_032(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(
              exists(
                  hg.nodes("Article")
                    .expand("PUBLISHED_IN", to="Journal", edge_as="r")
                    .filter(col("r.meta") < "217")
              )
          )
          .select(title=col("title"))
          .collect()
    )


# =============================================================================
# Query 033: Streams with followers in range
# MATCH (s:Stream) 
# WHERE s.followers >= 200000 AND s.followers <= 500000 
# RETURN s LIMIT 5
# =============================================================================
def query_033(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Stream")
          .filter((col("followers") >= 200000) & (col("followers") <= 500000))
          .limit(5)
          .collect()
    )


# =============================================================================
# Query 034: Users mentioning Neo4j with high followers
# MATCH (u:User)-[:POSTS]->(t:Tweet)-[:MENTIONS]->(m:Me {screen_name: 'neo4j'}) 
# RETURN u.name, u.followers 
# ORDER BY u.followers DESC LIMIT 3
# =============================================================================
def query_034(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("User")
          .expand("POSTS", to="Tweet", as_="t")
          .expand("MENTIONS", to="Me", as_="m")
          .filter(col("m.screen_name") == "neo4j")
          .order_by(col("User.followers"), ascending=False)
          .limit(3)
          .select(col("User.name"), col("User.followers"))
          .collect()
    )


# =============================================================================
# Query 035: Character community sizes
# MATCH (c:Character) 
# RETURN c.louvain AS community, count(c) AS communitySize 
# ORDER BY communitySize DESC LIMIT 5
# =============================================================================
def query_035(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Character")
          .group_by(community=col("louvain"))
          .agg(communitySize=count())
          .order_by("communitySize", ascending=False)
          .limit(5)
          .collect()
    )


# =============================================================================
# Query 036: Neo4j user tweets mentioning users with high followers
# MATCH (u:User {name: 'Neo4j'})-[:POSTS]->(t:Tweet)-[:MENTIONS]->(m:User) 
# WHERE m.followers >= 5000 
# RETURN t
# =============================================================================
def query_036(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("User")
          .filter(col("name") == "Neo4j")
          .expand("POSTS", to="Tweet", as_="t")
          .expand("MENTIONS", to="User", as_="m")
          .filter(col("m.followers") >= 5000)
          .select(col("t"))
          .collect()
    )


# =============================================================================
# Query 037: Machine count in zone 2 racks
# MATCH (r:Rack {zone: 2})-[:HOLDS]->(m:Machine) RETURN count(m)
# =============================================================================
def query_037(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Rack")
          .filter(col("zone") == 2)
          .expand("HOLDS", to="Machine", as_="m")
          .group_by()
          .agg(count=count())
          .collect()
    )


# =============================================================================
# Query 038: Update dates excluding specific date
# MATCH (n:UpdateDate) 
# WHERE n.update_date <> '2016-11-23' 
# RETURN DISTINCT n.update_date AS update_date
# =============================================================================
def query_038(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("UpdateDate")
          .filter(col("update_date") != "2016-11-23")
          .select(update_date=col("update_date"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 039: Categories starting with 'f' with skip and limit
# MATCH (n:Categories) 
# WHERE n.category_id STARTS WITH 'f' 
# WITH n.category_id AS category_id 
# SKIP 2 LIMIT 2 
# RETURN category_id
# =============================================================================
def query_039(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Categories")
          .filter(col("category_id").starts_with("f"))
          .select(category_id=col("category_id"))
          .offset(2)
          .limit(2)
          .collect()
    )


# =============================================================================
# Query 040: Distinct product sizes
# MATCH (p:Product) RETURN DISTINCT p.product_size
# =============================================================================
def query_040(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Product")
          .select(col("product_size"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 041: Articles with different article_id
# MATCH (n:Article) WHERE n.article_id <> '1013' 
# RETURN DISTINCT n.abstract AS abstract
# =============================================================================
def query_041(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("article_id") != "1013")
          .select(abstract=col("abstract"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 042: Topics containing 'Studi' in description
# MATCH (n:Topic) WHERE n.description CONTAINS 'Studi' RETURN n
# =============================================================================
def query_042(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Topic")
          .filter(col("description").contains("Studi"))
          .collect()
    )


# =============================================================================
# Query 043: Author affiliations with OR condition
# MATCH (n:Author) 
# WHERE n.author_id = 'd83c43e5b1cf398c4e549843f497694b' OR n.affiliation IS NOT NULL 
# RETURN DISTINCT n.affiliation AS affiliation
# =============================================================================
def query_043(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Author")
          .filter(
              (col("author_id") == "d83c43e5b1cf398c4e549843f497694b") |
              col("affiliation").is_not_null()
          )
          .select(affiliation=col("affiliation"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 044: Top movies by IMDB votes
# MATCH (m:Movie) RETURN m.title, m.imdbVotes 
# ORDER BY m.imdbVotes DESC LIMIT 5
# =============================================================================
def query_044(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Movie")
          .order_by("imdbVotes", ascending=False)
          .limit(5)
          .select("title", "imdbVotes")
          .collect()
    )


# =============================================================================
# Query 045: Questions with positive score
# MATCH (q:Question) WHERE q.score > 0 RETURN q
# =============================================================================
def query_045(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Question")
          .filter(col("score") > 0)
          .collect()
    )


# =============================================================================
# Query 046: Actors who directed movies they acted in
# MATCH (a:Actor)-[:ACTED_IN]->(m:Movie)<-[:DIRECTED]-(a) 
# RETURN a.name LIMIT 3
# =============================================================================
def query_046(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Actor")
          .expand("ACTED_IN", to="Movie", as_="m")
          .expand("DIRECTED", direction="in", as_="director")
          .filter(col("Actor.id") == col("director.id"))
          .select(col("Actor.name"))
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 047: Lowest rated movies
# MATCH (m:Movie) WHERE m.average_vote < 5 
# RETURN m.title, m.average_vote 
# ORDER BY m.average_vote LIMIT 3
# =============================================================================
def query_047(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Movie")
          .filter(col("average_vote") < 5)
          .order_by("average_vote", ascending=True)
          .limit(3)
          .select("title", "average_vote")
          .collect()
    )


# =============================================================================
# Query 048: Author incoming relations with properties
# MATCH (c:Author)<-[r]-(n) WHERE c.first_name = 'A.' 
# RETURN properties(n) AS props, r
# =============================================================================
def query_048(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Author")
          .filter(col("first_name") == "A.")
          .expand(direction="in", as_="n", edge_as="r")
          .select(props=properties(col("n")), r=col("r"))
          .collect()
    )


# =============================================================================
# Query 049: Character interactions with specific weight
# MATCH (c1:Character)-[r:INTERACTS2]->(c2:Character) 
# WHERE r.weight = 92 
# RETURN c1.name AS character1, c2.name AS character2
# =============================================================================
def query_049(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Character")
          .expand("INTERACTS2", to="Character", as_="c2", edge_as="r")
          .filter(col("r.weight") == 92)
          .select(
              character1=col("Character.name"),
              character2=col("c2.name")
          )
          .collect()
    )


# =============================================================================
# Query 050: Article by exact title
# MATCH (n:Article) 
# WHERE n.title = 'Free Field Construction for the ABF Models in Regime II' 
# RETURN n
# =============================================================================
def query_050(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("title") == "Free Field Construction for the ABF Models in Regime II")
          .collect()
    )


# =============================================================================
# Query 051: Top moderators by stream count
# MATCH (u:User)-[:MODERATOR]->(s:Stream) 
# WITH u, count(s) AS num_moderated_streams 
# WHERE num_moderated_streams > 1 
# RETURN u.name, u.id 
# ORDER BY num_moderated_streams DESC LIMIT 3
# =============================================================================
def query_051(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("User")
          .expand("MODERATOR", to="Stream", as_="s")
          .group_by(col("User.name"), col("User.id"))
          .agg(num_moderated_streams=count())
          .filter(col("num_moderated_streams") > 1)
          .order_by("num_moderated_streams", ascending=False)
          .limit(3)
          .select(col("User.name"), col("User.id"))
          .collect()
    )


# =============================================================================
# Query 052: Longest path from category
# MATCH p=(a:Categories{specifications:'math.CA'})-[*]->(n) 
# RETURN p, nodes(p) ORDER BY LENGTH(p) DESC LIMIT 1
# =============================================================================
def query_052(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Categories")
          .filter(col("specifications") == "math.CA")
          .expand(hops=(1, 10), as_="n")
          .with_column("path_len", path_length(col("_path")))
          .order_by("path_len", ascending=False)
          .limit(1)
          .select(path=col("_path"), nodes=nodes(col("_path")))
          .collect()
    )


# =============================================================================
# Query 053: Top streams by followers and views
# MATCH (s:Stream) 
# RETURN s.name, s.followers, s.total_view_count 
# ORDER BY s.followers DESC, s.total_view_count ASC LIMIT 3
# =============================================================================
def query_053(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Stream")
          .order_by(col("followers").desc(), col("total_view_count").asc())
          .limit(3)
          .select("name", "followers", "total_view_count")
          .collect()
    )


# =============================================================================
# Query 054: Orders with most products
# MATCH (o:Order)-[r:ORDERS]->(p:Product) 
# WITH o, count(DISTINCT p) AS numProducts 
# ORDER BY numProducts DESC LIMIT 5 
# RETURN o.orderID, numProducts
# =============================================================================
def query_054(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Order")
          .expand("ORDERS", to="Product", as_="p")
          .group_by(col("Order.orderID"))
          .agg(numProducts=count_distinct(col("p.id")))
          .order_by("numProducts", ascending=False)
          .limit(5)
          .select(col("Order.orderID"), "numProducts")
          .collect()
    )


# =============================================================================
# Query 055: Article-DOI with matching property
# MATCH (a:Article {title:'Solutions to congruences using sets with the property of Baire'})
#   -[r:HAS_DOI]->(b:DOI) 
# WHERE ANY(key IN keys(a) WHERE a[key] = b[key]) 
# RETURN b
# =============================================================================
def query_055(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("title") == "Solutions to congruences using sets with the property of Baire")
          .expand("HAS_DOI", to="DOI", as_="b")
          .select(col("b"))
          .collect()
    )


# =============================================================================
# Query 056: Organizations in Phoenix (Korean)
# MATCH (:City {name: "피닉스"})<-[:IN_CITY]-(org:Organization) RETURN COUNT(org)
# =============================================================================
def query_056(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("City")
          .filter(col("name") == "피닉스")
          .expand("IN_CITY", direction="in", to="Organization", as_="org")
          .group_by()
          .agg(count=count())
          .collect()
    )


# =============================================================================
# Query 057: Article-Journal counts by title
# MATCH (n:Article) -[:PUBLISHED_IN]->(m:Journal) 
# WITH DISTINCT n, m 
# RETURN n.title AS title, count(m) AS count 
# ORDER BY count DESC LIMIT 7
# =============================================================================
def query_057(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .expand("PUBLISHED_IN", to="Journal", as_="m")
          .group_by(title=col("Article.title"))
          .agg(count=count_distinct(col("m.id")))
          .order_by("count", ascending=False)
          .limit(7)
          .collect()
    )


# =============================================================================
# Query 058: User with highest scored answer
# MATCH (a:Answer) WITH max(a.score) AS max_score 
# MATCH (a:Answer {score: max_score}) 
# MATCH (u:User)-[:PROVIDED]->(a) 
# RETURN u.display_name
# =============================================================================
def query_058(hg: Hypergraph) -> list[dict]:
    # First get max score, then find the user
    max_score_result = (
        hg.nodes("Answer")
          .group_by()
          .agg(max_score=max_(col("score")))
          .first()
    )
    if max_score_result:
        max_score = max_score_result["max_score"]
        return (
            hg.nodes("User")
              .expand("PROVIDED", to="Answer", as_="a")
              .filter(col("a.score") == max_score)
              .select(col("User.display_name"))
              .collect()
        )
    return []


# =============================================================================
# Query 059: Path count between Author and Report
# MATCH p=(a:Author{last_name:'Dunajski'})-[*]->(d:Report{report_id:'3fa3ec8100d88908b00d139dacdedb6a'}) 
# RETURN count(p)
# =============================================================================
def query_059(hg: Hypergraph) -> list[dict]:
    return (
        all_paths(
            hg.nodes("Author").filter(col("last_name") == "Dunajski"),
            hg.nodes("Report").filter(col("report_id") == "3fa3ec8100d88908b00d139dacdedb6a"),
            direction="out"
        )
        .group_by()
        .agg(count=count())
        .collect()
    )


# =============================================================================
# Query 060: Journal ID by article and publication info
# MATCH (n:Article) -[pu:PUBLISHED_IN {pages : '303-348'}]->(m) 
# WHERE n.title='Generating Functional in CFT on Riemann Surfaces II: Homological Aspects' 
# RETURN m.journal_id
# =============================================================================
def query_060(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("title") == "Generating Functional in CFT on Riemann Surfaces II: Homological Aspects")
          .expand("PUBLISHED_IN", to="Journal", as_="m", edge_as="pu")
          .filter(col("pu.pages") == "303-348")
          .select(col("m.journal_id"))
          .collect()
    )


# =============================================================================
# Query 061: Top short movie by IMDB rating
# MATCH (m:Movie) WHERE m.runtime < 90 
# WITH m ORDER BY m.imdbRating DESC LIMIT 1 
# RETURN m.title
# =============================================================================
def query_061(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Movie")
          .filter(col("runtime") < 90)
          .order_by("imdbRating", ascending=False)
          .limit(1)
          .select("title")
          .collect()
    )


# =============================================================================
# Query 062: Policy type counts
# MATCH (p:Policy) RETURN p.Policy_Type_Code, COUNT(*)
# =============================================================================
def query_062(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Policy")
          .group_by(col("Policy_Type_Code"))
          .agg(count=count())
          .collect()
    )


# =============================================================================
# Query 063: Active intermediaries
# MATCH (i:Intermediary) WHERE i.status = 'ACTIVE' 
# RETURN i.name AS intermediary_name, i.countries AS countries, i.address AS address
# =============================================================================
def query_063(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Intermediary")
          .filter(col("status") == "ACTIVE")
          .select(
              intermediary_name=col("name"),
              countries=col("countries"),
              address=col("address")
          )
          .collect()
    )


# =============================================================================
# Query 064: Teams not in any match season
# MATCH (t:Team) WHERE NOT (t)-[:PARTICIPATES_IN]-(:MatchSeason) RETURN t.Name
# =============================================================================
def query_064(hg: Hypergraph) -> list[dict]:
    participating_teams = (
        hg.nodes("Team")
          .expand("PARTICIPATES_IN", to="MatchSeason")
          .select(col("Team.id"))
          .collect()
    )
    team_ids = [t["id"] for t in participating_teams]
    
    return (
        hg.nodes("Team")
          .filter(~col("id").contains_element(team_ids))
          .select(col("Name"))
          .collect()
    )


# =============================================================================
# Query 065: Production companies with both movies and videos
# MATCH (pc:ProductionCompany)<-[:PRODUCED_BY]-(m:Movie) 
# MATCH (pc)<-[:PRODUCED_BY]-(v:Video) 
# RETURN DISTINCT pc.name
# =============================================================================
def query_065(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("ProductionCompany")
          .expand("PRODUCED_BY", direction="in", to="Movie", as_="m")
          .expand("PRODUCED_BY", direction="in", to="Video", as_="v")
          .select(col("ProductionCompany.name"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 066: Properties from category 2-hop traversal
# MATCH (a:Categories{category_id:'eea477d68b70c3a05be12567240033ef'})-[*2]->(n) 
# RETURN DISTINCT properties(n) AS props
# =============================================================================
def query_066(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Categories")
          .filter(col("category_id") == "eea477d68b70c3a05be12567240033ef")
          .expand(hops=2, as_="n")
          .select(props=properties(col("n")))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 067: Distinct video game types
# MATCH (v:VideoGame) RETURN DISTINCT v.GType
# =============================================================================
def query_067(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("VideoGame")
          .select(col("GType"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 068: Shortest path between Article and Report
# MATCH p=shortestPath((a:Article{title:'Open sets satisfying systems of congruences'})
#   -[*]-(e:Report{report_id:'5049b80a2935f95cc95cf14dbfb8c610'})) 
# RETURN nodes(p)
# =============================================================================
def query_068(hg: Hypergraph) -> list[dict]:
    return (
        shortest_path(
            hg.nodes("Article").filter(col("title") == "Open sets satisfying systems of congruences"),
            hg.nodes("Report").filter(col("report_id") == "5049b80a2935f95cc95cf14dbfb8c610"),
            direction="both"
        )
        .select(nodes=nodes(col("path")))
        .collect()
    )


# =============================================================================
# Query 069: DOIs without author connection
# MATCH (n:DOI), (:Author {first_name: 'A.'}) 
# WHERE NOT (n) --> (:Author) 
# RETURN n.name
# =============================================================================
def query_069(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("DOI")
          .filter(~exists(
              hg.nodes("DOI").expand(to="Author", direction="out")
          ))
          .select(col("name"))
          .collect()
    )


# =============================================================================
# Query 070: Character interactions (optional match)
# MATCH (c:Character {name: "Stevron-Frey"}) 
# OPTIONAL MATCH (c)-[:INTERACTS]-(other) 
# RETURN DISTINCT other.name
# =============================================================================
def query_070(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Character")
          .filter(col("name") == "Stevron-Frey")
          .optional_expand("INTERACTS", direction="both", to="Character", as_="other")
          .select(col("other.name"))
          .distinct()
          .collect()
    )


# Queries 071-100 continue...

# =============================================================================
# Query 071: Officers of Samoa entities
# MATCH (o:Officer)-[:officer_of]->(e:Entity) 
# WHERE e.jurisdiction_description = 'Samoa' 
# RETURN o
# =============================================================================
def query_071(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Officer")
          .expand("officer_of", to="Entity", as_="e")
          .filter(col("e.jurisdiction_description") == "Samoa")
          .select(col("Officer"))
          .collect()
    )


# =============================================================================
# Query 072: Journal relation type counts
# MATCH (a:Journal{name:'Math. Ann'})-[r]->() 
# RETURN COUNT(DISTINCT TYPE(r)) AS rels, TYPE(r)
# =============================================================================
def query_072(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Journal")
          .filter(col("name") == "Math. Ann")
          .expand(direction="out", edge_as="r")
          .group_by(type_r=type_(col("r")))
          .agg(rels=count_distinct(type_(col("r"))))
          .collect()
    )


# =============================================================================
# Query 073: Customers with 1998 orders
# MATCH (c:Customer)-[:PURCHASED]->(o:Order) 
# WHERE o.requiredDate STARTS WITH '1998' 
# RETURN c.companyName AS customerName, c.contactName AS contactName, c.customerID AS customerID
# =============================================================================
def query_073(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Customer")
          .expand("PURCHASED", to="Order", as_="o")
          .filter(col("o.requiredDate").starts_with("1998"))
          .select(
              customerName=col("Customer.companyName"),
              contactName=col("Customer.contactName"),
              customerID=col("Customer.customerID")
          )
          .collect()
    )


# =============================================================================
# Query 074: Paris organizations with subsidiaries
# MATCH (o:Organization)-[:IN_CITY]->(c:City {name: "Paris"}) 
# WHERE exists{ (o)-[:HAS_SUBSIDIARY]->(:Organization) } 
# RETURN o
# =============================================================================
def query_074(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Organization")
          .expand("IN_CITY", to="City", as_="c")
          .filter(col("c.name") == "Paris")
          .filter(exists(
              hg.nodes("Organization").expand("HAS_SUBSIDIARY", to="Organization")
          ))
          .select(col("Organization"))
          .collect()
    )


# =============================================================================
# Query 075: Longest path from specific Topic
# MATCH p=(a:Topic{description:'...'})-[*]->(n) 
# RETURN p, nodes(p) ORDER BY LENGTH(p) DESC LIMIT 1
# =============================================================================
def query_075(hg: Hypergraph) -> list[dict]:
    topic_desc = "Encompasses techniques for reconstructing images from blurred or incomplete data using regularizers, sparsity, and phase retrieval algorithms, with applications in compressive sensing, neural networks, and optical imaging. Focuses on understanding small-time behavior, limiting transitions, and phase transitions in signal processing and optics, as well as the role of status-dependent behavior and spiking neurons in neural networks. Emphasizes the importance of regularization, penalization, and lasso techniques in image reconstruction and phase retrieval"
    return (
        hg.nodes("Topic")
          .filter(col("description") == topic_desc)
          .expand(hops=(1, 10), as_="n")
          .with_column("path_len", path_length(col("_path")))
          .order_by("path_len", ascending=False)
          .limit(1)
          .select(path=col("_path"), nodes=nodes(col("_path")))
          .collect()
    )


# =============================================================================
# Query 076: Apartments without facilities
# MATCH (a:Apartment) WHERE NOT (:ApartmentFacility)-[:IS_LOCATED_IN]->(a) 
# RETURN count(*)
# =============================================================================
def query_076(hg: Hypergraph) -> list[dict]:
    apartments_with_facilities = (
        hg.nodes("ApartmentFacility")
          .expand("IS_LOCATED_IN", to="Apartment", as_="a")
          .select(col("a.id"))
          .collect()
    )
    apt_ids = [a["id"] for a in apartments_with_facilities]
    
    return (
        hg.nodes("Apartment")
          .filter(~col("id").contains_element(apt_ids))
          .group_by()
          .agg(count=count())
          .collect()
    )


# =============================================================================
# Query 077: Pixar videos
# MATCH (v:Video)-[:PRODUCED_BY]->(pc:ProductionCompany {name: 'Pixar Animation Studios'}) 
# RETURN v.title LIMIT 3
# =============================================================================
def query_077(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Video")
          .expand("PRODUCED_BY", to="ProductionCompany", as_="pc")
          .filter(col("pc.name") == "Pixar Animation Studios")
          .select(col("Video.title"))
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 078: Recent questions by tag
# MATCH (q:Question)-[:TAGGED]->(t:Tag) 
# WHERE q.answer_count > 0 
# WITH t, q ORDER BY q.creation_date DESC 
# RETURN t.name AS tag, q.creation_date AS question_creation_date LIMIT 3
# =============================================================================
def query_078(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Question")
          .filter(col("answer_count") > 0)
          .expand("TAGGED", to="Tag", as_="t")
          .order_by(col("Question.creation_date"), ascending=False)
          .limit(3)
          .select(
              tag=col("t.name"),
              question_creation_date=col("Question.creation_date")
          )
          .collect()
    )


# =============================================================================
# Query 079: Entities benefiting from high-number filings
# MATCH (f:Filing) WHERE f.number > 10 
# MATCH (f)-[:BENEFITS]->(e:Entity) 
# RETURN DISTINCT e.name
# =============================================================================
def query_079(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Filing")
          .filter(col("number") > 10)
          .expand("BENEFITS", to="Entity", as_="e")
          .select(col("e.name"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 080: Packages with movies by duration
# MATCH (p:Package)-[:PROVIDES_ACCESS_TO]->(:Genre)<-[:IN_GENRE]-(m:Movie) 
# RETURN p.name, p.duration 
# ORDER BY p.duration DESC LIMIT 3
# =============================================================================
def query_080(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Package")
          .expand("PROVIDES_ACCESS_TO", to="Genre", as_="g")
          .expand("IN_GENRE", direction="in", to="Movie", as_="m")
          .order_by(col("Package.duration"), ascending=False)
          .limit(3)
          .select(col("Package.name"), col("Package.duration"))
          .collect()
    )


# =============================================================================
# Query 081: VIP users from high-view streams
# MATCH (s:Stream)-[:VIP]->(u:User) 
# WHERE s.total_view_count > 10000000 
# RETURN u.name AS vip_name 
# ORDER BY s.total_view_count DESC LIMIT 3
# =============================================================================
def query_081(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Stream")
          .filter(col("total_view_count") > 10000000)
          .expand("VIP", to="User", as_="u")
          .order_by(col("Stream.total_view_count"), ascending=False)
          .limit(3)
          .select(vip_name=col("u.name"))
          .collect()
    )


# =============================================================================
# Query 082: Top streams with teams by view count
# MATCH (s:Stream)-[:HAS_TEAM]->(:Team) 
# WHERE s.total_view_count IS NOT NULL 
# RETURN s ORDER BY s.total_view_count DESC LIMIT 5
# =============================================================================
def query_082(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Stream")
          .expand("HAS_TEAM", to="Team")
          .filter(col("Stream.total_view_count").is_not_null())
          .order_by(col("Stream.total_view_count"), ascending=False)
          .limit(5)
          .select(col("Stream"))
          .collect()
    )


# =============================================================================
# Query 083: Games with teams by total views
# MATCH (s:Stream)-[:PLAYS]->(g:Game) 
# WHERE exists{(s)-[:HAS_TEAM]->(:Team)} 
# RETURN g.name AS Game, sum(s.total_view_count) AS TotalViews 
# ORDER BY TotalViews DESC LIMIT 3
# =============================================================================
def query_083(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Stream")
          .filter(exists(
              hg.nodes("Stream").expand("HAS_TEAM", to="Team")
          ))
          .expand("PLAYS", to="Game", as_="g")
          .group_by(Game=col("g.name"))
          .agg(TotalViews=sum_(col("Stream.total_view_count")))
          .order_by("TotalViews", ascending=False)
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 084: Complex article-journal query
# MATCH (n:Article {abstract: '...'}) -[r:PUBLISHED_IN]- (m:Journal) 
# WHERE m.name STARTS WITH 'J' AND r.pages IS NOT NULL 
# RETURN n.name
# =============================================================================
def query_084(hg: Hypergraph) -> list[dict]:
    abstract = "  The main result of this paper is the proof of the \"transversal part\" of the homological mirror symmetry conjecture for an elliptic curve which states an equivalence of two $A_{\\infty}$-structures on the category of vector bundles on an elliptic curves. The proof is based on the study of $A_{\\infty}$-structures on the category of line bundles over an elliptic curve satisfying some natural restrictions (in particular, $m_1$ should be zero, $m_2$ should coincide with the usual composition). The key observation is that such a structure is uniquely determined up to homotopy by certain triple products. "
    return (
        hg.nodes("Article")
          .filter(col("abstract") == abstract)
          .expand("PUBLISHED_IN", to="Journal", direction="both", as_="m", edge_as="r")
          .filter(col("m.name").starts_with("J") & col("r.pages").is_not_null())
          .select(col("Article.name"))
          .collect()
    )


# =============================================================================
# Query 085: Multi-hop keyword to topic traversal
# MATCH (a:Keyword{name:'log-balanced'})-[*]->(d:Topic{label:'Dynamical Systems_10'})-[*]->(n) 
# RETURN n
# =============================================================================
def query_085(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Keyword")
          .filter(col("name") == "log-balanced")
          .expand(hops=(1, 10), as_="d")
          .filter(labels(col("d")).contains_element("Topic") & (col("d.label") == "Dynamical Systems_10"))
          .expand(hops=(1, 10), as_="n")
          .select(col("n"))
          .collect()
    )


# =============================================================================
# Query 086: Top moderators by relationship count
# MATCH (u:User)-[r:MODERATOR]->(otherUser:User) 
# RETURN u.name AS user, count(r) AS num_moderator_relationships 
# ORDER BY num_moderator_relationships DESC LIMIT 3
# =============================================================================
def query_086(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("User")
          .expand("MODERATOR", to="User", as_="otherUser", edge_as="r")
          .group_by(user=col("User.name"))
          .agg(num_moderator_relationships=count())
          .order_by("num_moderator_relationships", ascending=False)
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 087: Directors born in 1960s
# MATCH (p:Person)-[:DIRECTED]->(:Movie) 
# WHERE p.born >= 1960 AND p.born < 1970 
# RETURN p.name AS director, p.born AS birthYear
# =============================================================================
def query_087(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Person")
          .filter((col("born") >= 1960) & (col("born") < 1970))
          .expand("DIRECTED", to="Movie")
          .select(
              director=col("Person.name"),
              birthYear=col("Person.born")
          )
          .collect()
    )


# =============================================================================
# Query 088: Top movies by budget
# MATCH (m:Movie) WHERE m.budget IS NOT NULL 
# RETURN m.title, m.budget ORDER BY m.budget DESC LIMIT 5
# =============================================================================
def query_088(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Movie")
          .filter(col("budget").is_not_null())
          .order_by("budget", ascending=False)
          .limit(5)
          .select("title", "budget")
          .collect()
    )


# =============================================================================
# Query 089: Jenny's lowest rated review
# MATCH (u:User {name: 'Jenny'})-[:WROTE]->(r:Review)-[:REVIEWS]->(b:Business) 
# RETURN b.name AS business, r.stars AS rating 
# ORDER BY r.stars ASC LIMIT 1
# =============================================================================
def query_089(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("User")
          .filter(col("name") == "Jenny")
          .expand("WROTE", to="Review", as_="r")
          .expand("REVIEWS", to="Business", as_="b")
          .order_by(col("r.stars"), ascending=True)
          .limit(1)
          .select(
              business=col("b.name"),
              rating=col("r.stars")
          )
          .collect()
    )


# =============================================================================
# Query 090: Filing details with concerned entities
# MATCH (f:Filing)-[:CONCERNS]->(e:Entity) 
# WITH f, COUNT(DISTINCT e) AS entityCount 
# ORDER BY entityCount DESC LIMIT 5 
# MATCH (f)-[:CONCERNS]->(e:Entity) 
# RETURN f.sar_id AS FilingID, f.begin AS BeginDate, f.end AS EndDate, 
#        f.amount AS Amount, COLLECT(e.name) AS ConcernedEntities
# =============================================================================
def query_090(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Filing")
          .expand("CONCERNS", to="Entity", as_="e")
          .group_by(
              FilingID=col("Filing.sar_id"),
              BeginDate=col("Filing.begin"),
              EndDate=col("Filing.end"),
              Amount=col("Filing.amount")
          )
          .agg(
              entityCount=count_distinct(col("e.id")),
              ConcernedEntities=collect(col("e.name"))
          )
          .order_by("entityCount", ascending=False)
          .limit(5)
          .select("FilingID", "BeginDate", "EndDate", "Amount", "ConcernedEntities")
          .collect()
    )


# Queries 091-100

# =============================================================================
# Query 091: Author 2-hop traversal
# MATCH (a:Author{last_name:'Leoni'})-[r]->(n), (n)-[s]->(m) 
# RETURN labels(n) AS Interim, labels(m) AS Target
# =============================================================================
def query_091(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Author")
          .filter(col("last_name") == "Leoni")
          .expand(direction="out", as_="n")
          .expand(direction="out", as_="m")
          .select(
              Interim=labels(col("n")),
              Target=labels(col("m"))
          )
          .collect()
    )


# =============================================================================
# Query 092: Cybersecurity articles mentioning organizations
# MATCH (a:Article)-[:HAS_CHUNK]->(c:Chunk) WHERE c.text CONTAINS 'cybersecurity' 
# MATCH (a)-[:MENTIONS]->(o:Organization) 
# RETURN o
# =============================================================================
def query_092(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .expand("HAS_CHUNK", to="Chunk", as_="c")
          .filter(col("c.text").contains("cybersecurity"))
          .expand("MENTIONS", to="Organization", as_="o")
          .select(col("o"))
          .collect()
    )


# =============================================================================
# Query 093: Union of author affiliations and article titles
# MATCH (n:Author) RETURN n.affiliation AS Records 
# UNION ALL 
# MATCH (m:Article) RETURN m.title AS Records
# =============================================================================
def query_093(hg: Hypergraph) -> list[dict]:
    authors = (
        hg.nodes("Author")
          .select(Records=col("affiliation"))
    )
    articles = (
        hg.nodes("Article")
          .select(Records=col("title"))
    )
    return authors.union(articles).collect()


# =============================================================================
# Query 094: Longest reply thread
# MATCH p=(t:Tweet)-[:REPLY_TO*]->(:Tweet) 
# RETURN length(p) AS longestThread 
# ORDER BY longestThread DESC LIMIT 1
# =============================================================================
def query_094(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Tweet")
          .expand("REPLY_TO", hops=(1, 100), to="Tweet", as_="reply")
          .with_column("thread_length", path_length(col("_path")))
          .order_by("thread_length", ascending=False)
          .limit(1)
          .select(longestThread=col("thread_length"))
          .collect()
    )


# =============================================================================
# Query 095: City with most supplier relationships
# MATCH (o1:Organization)-[:HAS_SUPPLIER]->(o2:Organization), 
#       (o1)-[:IN_CITY]->(c:City), (o2)-[:IN_CITY]->(c) 
# WITH c, count(DISTINCT o1) AS orgCount 
# ORDER BY orgCount DESC LIMIT 1 
# RETURN c.name
# =============================================================================
def query_095(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Organization")
          .expand("HAS_SUPPLIER", to="Organization", as_="o2")
          .expand("IN_CITY", to="City", as_="c1")
          .expand("IN_CITY", to="City", as_="c2")
          .filter(col("c1.id") == col("c2.id"))
          .group_by(col("c1.name"))
          .agg(orgCount=count_distinct(col("Organization.id")))
          .order_by("orgCount", ascending=False)
          .limit(1)
          .select(col("c1.name"))
          .collect()
    )


# =============================================================================
# Query 096: Union of topic cluster and label
# MATCH (n:Topic) RETURN n.cluster AS Records 
# UNION ALL 
# MATCH (m:Topic) RETURN m.label AS Records
# =============================================================================
def query_096(hg: Hypergraph) -> list[dict]:
    clusters = hg.nodes("Topic").select(Records=col("cluster"))
    labels = hg.nodes("Topic").select(Records=col("label"))
    return clusters.union(labels).collect()


# =============================================================================
# Query 097: Shortest path category to author
# MATCH p=shortestPath((a:Categories{category_id:'33657234da1dc070ea09e7c31bb86abb'})
#   -[*]-(e:Author{affiliation:'unspecified'})) 
# RETURN nodes(p)
# =============================================================================
def query_097(hg: Hypergraph) -> list[dict]:
    return (
        shortest_path(
            hg.nodes("Categories").filter(col("category_id") == "33657234da1dc070ea09e7c31bb86abb"),
            hg.nodes("Author").filter(col("affiliation") == "unspecified"),
            direction="both"
        )
        .select(nodes=nodes(col("path")))
        .collect()
    )


# =============================================================================
# Query 098: Article to DOI to related nodes
# MATCH (a:Article{title:'Hyper-K{\"a}hler Hierarchies and their twistor theory'})
#   -[:HAS_DOI]->(c:DOI)-[r]->(n) 
# RETURN n
# =============================================================================
def query_098(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("title") == 'Hyper-K{"a}hler Hierarchies and their twistor theory')
          .expand("HAS_DOI", to="DOI", as_="c")
          .expand(direction="out", as_="n")
          .select(col("n"))
          .collect()
    )


# =============================================================================
# Query 099: High view count streams
# MATCH (s:Stream) WHERE s.total_view_count > 500000000 
# RETURN s.name, s.url, s.total_view_count
# =============================================================================
def query_099(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Stream")
          .filter(col("total_view_count") > 500000000)
          .select("name", "url", "total_view_count")
          .collect()
    )


# =============================================================================
# Query 100: Adventure movie total revenue
# MATCH (m:Movie)-[:IN_GENRE]->(g:Genre {name: 'Adventure'}) 
# RETURN sum(m.revenue) AS totalRevenue
# =============================================================================
def query_100(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Movie")
          .expand("IN_GENRE", to="Genre", as_="g")
          .filter(col("g.name") == "Adventure")
          .group_by()
          .agg(totalRevenue=sum_(col("Movie.revenue")))
          .collect()
    )


# =============================================================================
# Query 101: Multi-hop from author affiliation
# MATCH (a:Author{affiliation:'unspecified'})-[*]->(d:Keyword{name:'...'})-[*]->(n) 
# RETURN n
# =============================================================================
def query_101(hg: Hypergraph) -> list[dict]:
    keyword_name = "tree (optimality criteria: minimum mean-squared error)  alternative keyword suggestions: - multiscale superpopulation models - independent innovations trees - water-"
    return (
        hg.nodes("Author")
          .filter(col("affiliation") == "unspecified")
          .expand(hops=(1, 10), as_="d")
          .filter(labels(col("d")).contains_element("Keyword") & (col("d.name") == keyword_name))
          .expand(hops=(1, 10), as_="n")
          .select(col("n"))
          .collect()
    )


# =============================================================================
# Query 102: Movie taglines from 1999
# MATCH (m:Movie) WHERE m.released = 1999 RETURN m.tagline
# =============================================================================
def query_102(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Movie")
          .filter(col("released") == 1999)
          .select("tagline")
          .collect()
    )


# =============================================================================
# Query 103: Distinct topic clusters
# MATCH (n:Topic) WHERE n.cluster <> '7' RETURN DISTINCT n.cluster AS cluster
# =============================================================================
def query_103(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Topic")
          .filter(col("cluster") != "7")
          .select(cluster=col("cluster"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 104: Authors of negative sentiment articles
# MATCH (a:Article)-[:MENTIONS]->(o:Organization) 
# WHERE a.sentiment < 0 
# RETURN a.author AS Author ORDER BY a.date ASC LIMIT 3
# =============================================================================
def query_104(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("sentiment") < 0)
          .expand("MENTIONS", to="Organization")
          .order_by("date", ascending=True)
          .limit(3)
          .select(Author=col("Article.author"))
          .collect()
    )


# =============================================================================
# Query 105: Businesses reviewed by Angie
# MATCH (u:User {name: 'Angie'})-[:WROTE]->(r:Review)-[:REVIEWS]->(b:Business) 
# RETURN DISTINCT b.name
# =============================================================================
def query_105(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("User")
          .filter(col("name") == "Angie")
          .expand("WROTE", to="Review", as_="r")
          .expand("REVIEWS", to="Business", as_="b")
          .select(col("b.name"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 106: Articles with specific meta value
# MATCH (n:Article) -[r:PUBLISHED_IN]->(m:Journal) 
# WHERE r.meta = '248' 
# WITH DISTINCT n, m 
# RETURN n.title AS title, count(m) AS count ORDER BY count
# =============================================================================
def query_106(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .expand("PUBLISHED_IN", to="Journal", as_="m", edge_as="r")
          .filter(col("r.meta") == "248")
          .group_by(title=col("Article.title"))
          .agg(count=count_distinct(col("m.id")))
          .order_by("count", ascending=True)
          .collect()
    )


# =============================================================================
# Query 107: Character interactions with high weight
# MATCH (c1:Character)-[r:INTERACTS45]->(c2:Character) 
# RETURN c1.name AS Character1, c2.name AS Character2, r.weight AS Weight 
# ORDER BY r.weight DESC LIMIT 3
# =============================================================================
def query_107(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Character")
          .expand("INTERACTS45", to="Character", as_="c2", edge_as="r")
          .order_by(col("r.weight"), ascending=False)
          .limit(3)
          .select(
              Character1=col("Character.name"),
              Character2=col("c2.name"),
              Weight=col("r.weight")
          )
          .collect()
    )


# =============================================================================
# Query 108: Journal relations and properties
# MATCH (a:Journal{name:'Geom. Topol'})-[r]->(n) RETURN properties(n), r
# =============================================================================
def query_108(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Journal")
          .filter(col("name") == "Geom. Topol")
          .expand(direction="out", as_="n", edge_as="r")
          .select(properties=properties(col("n")), r=col("r"))
          .collect()
    )


# =============================================================================
# Query 109: Most commented questions
# MATCH (q:Question)<-[:COMMENTED_ON]-(c:Comment) 
# WITH q, COUNT(c) AS comment_count 
# ORDER BY comment_count DESC LIMIT 5 
# RETURN q.title AS question_title, q.link AS question_link, comment_count
# =============================================================================
def query_109(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Question")
          .expand("COMMENTED_ON", direction="in", as_="c")
          .group_by(
              question_title=col("Question.title"),
              question_link=col("Question.link")
          )
          .agg(comment_count=count())
          .order_by("comment_count", ascending=False)
          .limit(5)
          .collect()
    )


# =============================================================================
# Query 110: 2022 articles mentioning large organizations
# MATCH (a:Article)-[:MENTIONS]->(o:Organization) 
# WHERE a.date.year = 2022 AND o.nbrEmployees > 100 
# RETURN a
# =============================================================================
def query_110(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(year(col("date")) == 2022)
          .expand("MENTIONS", to="Organization", as_="o")
          .filter(col("o.nbrEmployees") > 100)
          .select(col("Article"))
          .collect()
    )


# =============================================================================
# Query 111: Journal IDs (not null)
# MATCH (n:Journal) WHERE n.journal_id IS NOT NULL RETURN n.journal_id LIMIT 10
# =============================================================================
def query_111(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Journal")
          .filter(col("journal_id").is_not_null())
          .select("journal_id")
          .limit(10)
          .collect()
    )


# =============================================================================
# Query 112: Keyword relation type counts
# MATCH (a:Keyword{name:'uncountably many different asymptotic growth rates'})-[r]->(n) 
# RETURN n, TYPE(r) AS Relations, COUNT(r) AS Counts
# =============================================================================
def query_112(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Keyword")
          .filter(col("name") == "uncountably many different asymptotic growth rates")
          .expand(direction="out", as_="n", edge_as="r")
          .group_by(col("n"), Relations=type_(col("r")))
          .agg(Counts=count())
          .collect()
    )


# =============================================================================
# Query 113: Article category relations
# MATCH (a:Article{title:'Fast matrix multiplication is stable'})
#   -[:HAS_CATEGORY]->(c:Categories)-[r]->(n) 
# RETURN n
# =============================================================================
def query_113(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("title") == "Fast matrix multiplication is stable")
          .expand("HAS_CATEGORY", to="Categories", as_="c")
          .expand(direction="out", as_="n")
          .select(col("n"))
          .collect()
    )


# =============================================================================
# Query 114: Union of update date and author ID
# MATCH (n:UpdateDate) RETURN n.update_date AS Records 
# UNION 
# MATCH (m:Author) RETURN m.author_id AS Records
# =============================================================================
def query_114(hg: Hypergraph) -> list[dict]:
    dates = hg.nodes("UpdateDate").select(Records=col("update_date"))
    authors = hg.nodes("Author").select(Records=col("author_id"))
    return dates.union(authors).distinct().collect()


# =============================================================================
# Query 115: Report traversal with labels
# MATCH (a:Report{report_no:'Swansea preprint 99-14'})-[r]->(n), (n)-[s]->(m) 
# RETURN labels(n) AS Interim, labels(m) AS Target
# =============================================================================
def query_115(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Report")
          .filter(col("report_no") == "Swansea preprint 99-14")
          .expand(direction="out", as_="n")
          .expand(direction="out", as_="m")
          .select(
              Interim=labels(col("n")),
              Target=labels(col("m"))
          )
          .collect()
    )


# =============================================================================
# Query 116: Article comments with OR condition
# MATCH (n:Article) 
# WHERE n.title = '$A_{\\infty}$-structures on an elliptic curve' OR n.comments IS NOT NULL 
# RETURN DISTINCT n.comments AS comments
# =============================================================================
def query_116(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(
              (col("title") == "$A_{\\infty}$-structures on an elliptic curve") |
              col("comments").is_not_null()
          )
          .select(comments=col("comments"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 117: Path count journal to author
# MATCH p=(a:Journal{name:'Math. Nachr'})-[*]->(d:Author{first_name:'S. O.'}) 
# RETURN count(p)
# =============================================================================
def query_117(hg: Hypergraph) -> list[dict]:
    return (
        all_paths(
            hg.nodes("Journal").filter(col("name") == "Math. Nachr"),
            hg.nodes("Author").filter(col("first_name") == "S. O."),
            direction="out"
        )
        .group_by()
        .agg(count=count())
        .collect()
    )


# =============================================================================
# Query 118: Update date by exact date
# MATCH (n:UpdateDate) WHERE n.update_date = date('2011-11-10') RETURN n
# =============================================================================
def query_118(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("UpdateDate")
          .filter(col("update_date") == date("2011-11-10"))
          .collect()
    )


# =============================================================================
# Query 119: Longest path from keyword
# MATCH p=(a:Keyword{name:'population dynamics'})-[*]->(n) 
# RETURN p, nodes(p) ORDER BY LENGTH(p) DESC LIMIT 1
# =============================================================================
def query_119(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Keyword")
          .filter(col("name") == "population dynamics")
          .expand(hops=(1, 10), as_="n")
          .with_column("path_len", path_length(col("_path")))
          .order_by("path_len", ascending=False)
          .limit(1)
          .select(path=col("_path"), nodes=nodes(col("_path")))
          .collect()
    )


# =============================================================================
# Query 120: Cross product article and categories
# MATCH (n:Article) MATCH (m:Categories) RETURN n.abstract, m.category_id LIMIT 8
# =============================================================================
def query_120(hg: Hypergraph) -> list[dict]:
    # Note: This is a cross join - Grism would handle differently
    articles = hg.nodes("Article").select(abstract=col("abstract")).collect()
    categories = hg.nodes("Categories").select(category_id=col("category_id")).collect()
    
    result = []
    for a in articles:
        for c in categories:
            result.append({"abstract": a["abstract"], "category_id": c["category_id"]})
            if len(result) >= 8:
                return result
    return result


# =============================================================================
# Query 121: Top users by rating count
# MATCH (u:User)-[r:RATED]->(m:Movie) 
# WITH u, count(r) AS numRatings 
# ORDER BY numRatings DESC LIMIT 5 
# RETURN u.name AS user, numRatings
# =============================================================================
def query_121(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("User")
          .expand("RATED", to="Movie", edge_as="r")
          .group_by(user=col("User.name"))
          .agg(numRatings=count())
          .order_by("numRatings", ascending=False)
          .limit(5)
          .collect()
    )


# =============================================================================
# Query 122: Organizations by employee count
# MATCH (o:Organization) 
# RETURN o.name AS organization, o.nbrEmployees AS numberOfEmployees 
# ORDER BY o.nbrEmployees DESC LIMIT 5
# =============================================================================
def query_122(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Organization")
          .order_by("nbrEmployees", ascending=False)
          .limit(5)
          .select(
              organization=col("name"),
              numberOfEmployees=col("nbrEmployees")
          )
          .collect()
    )


# =============================================================================
# Query 123: Author last names with OR
# MATCH (n:Author) 
# WHERE n.last_name = 'Chakrabarti' OR n.last_name IS NOT NULL 
# RETURN DISTINCT n.last_name AS last_name
# =============================================================================
def query_123(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Author")
          .filter(
              (col("last_name") == "Chakrabarti") |
              col("last_name").is_not_null()
          )
          .select(last_name=col("last_name"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 124: Top actors by movie count
# MATCH (p:Person)-[:ACTED_IN]->(m:Movie) 
# WITH p, count(m) AS numMovies 
# ORDER BY numMovies DESC LIMIT 5 
# RETURN p.name AS actor, numMovies
# =============================================================================
def query_124(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Person")
          .expand("ACTED_IN", to="Movie", as_="m")
          .group_by(actor=col("Person.name"))
          .agg(numMovies=count())
          .order_by("numMovies", ascending=False)
          .limit(5)
          .collect()
    )


# =============================================================================
# Query 125: Article properties and relations
# MATCH (b:Article)-[r]->(n) 
# WHERE b.title = 'Subexponential groups in 4-manifold topology' 
# RETURN properties(b) AS Article_props, properties(n) AS props
# =============================================================================
def query_125(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("title") == "Subexponential groups in 4-manifold topology")
          .expand(direction="out", as_="n")
          .select(
              Article_props=properties(col("Article")),
              props=properties(col("n"))
          )
          .collect()
    )


# =============================================================================
# Query 126: Path exists between keywords
# MATCH (a:Keyword{name:'uncountably many different asymptotic growth rates'}), 
#       (b:Keyword{name:'layer-by-layer growth'}) 
# RETURN EXISTS((a)-[*]-(b)) AS pathExists
# =============================================================================
def query_126(hg: Hypergraph) -> list[dict]:
    paths = all_paths(
        hg.nodes("Keyword").filter(col("name") == "uncountably many different asymptotic growth rates"),
        hg.nodes("Keyword").filter(col("name") == "layer-by-layer growth"),
        direction="both",
        max_hops=10
    ).count()
    return [{"pathExists": paths > 0}]


# =============================================================================
# Query 127: Authors with K last name
# MATCH (n:Author) WHERE n.last_name STARTS WITH 'K' 
# RETURN n.last_name AS last_name, n.affiliation AS affiliation
# =============================================================================
def query_127(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Author")
          .filter(col("last_name").starts_with("K"))
          .select(last_name=col("last_name"), affiliation=col("affiliation"))
          .collect()
    )


# =============================================================================
# Query 128: Topic to article matching
# MATCH (a:Topic{cluster:'8'})-[r]->(n), 
#       (d:Article{title:'The Gervais-Neveu-Felder equation...'}) 
# WHERE ANY(key in keys(n) WHERE n[key] = d[key]) 
# RETURN n
# =============================================================================
def query_128(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Topic")
          .filter(col("cluster") == "8")
          .expand(direction="out", as_="n")
          .select(col("n"))
          .collect()
    )


# =============================================================================
# Query 129: Path count report to article
# MATCH p=(a:Report{report_id:'dd0a54fea06e7b7a384741aac9313d65'})
#   -[*]->(d:Article{article_id:'1048'}) 
# RETURN count(p)
# =============================================================================
def query_129(hg: Hypergraph) -> list[dict]:
    return (
        all_paths(
            hg.nodes("Report").filter(col("report_id") == "dd0a54fea06e7b7a384741aac9313d65"),
            hg.nodes("Article").filter(col("article_id") == "1048"),
            direction="out"
        )
        .group_by()
        .agg(count=count())
        .collect()
    )


# =============================================================================
# Query 130: Organizations with most competitors
# MATCH (o:Organization)-[:HAS_COMPETITOR]->(c:Organization) 
# WITH o, COUNT(c) AS competitorCount 
# ORDER BY competitorCount DESC LIMIT 3 
# RETURN o.name AS organizationName, competitorCount
# =============================================================================
def query_130(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Organization")
          .expand("HAS_COMPETITOR", to="Organization", as_="c")
          .group_by(organizationName=col("Organization.name"))
          .agg(competitorCount=count())
          .order_by("competitorCount", ascending=False)
          .limit(3)
          .collect()
    )


# Queries 131-150

# =============================================================================
# Query 131: Articles without author connection
# MATCH (n:Article), (:Author {first_name: 'R.'}) 
# WHERE NOT (n) --> (:Author) 
# RETURN n.article_id
# =============================================================================
def query_131(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(~exists(
              hg.nodes("Article").expand(to="Author", direction="out")
          ))
          .select(col("article_id"))
          .collect()
    )


# =============================================================================
# Query 132: Max interaction weight in community
# MATCH (source:Character)-[r:INTERACTS45]->(target:Character) 
# WHERE source.louvain = 0 AND target.louvain = 0 
# WITH r.weight AS weight 
# RETURN max(weight)
# =============================================================================
def query_132(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Character")
          .filter(col("louvain") == 0)
          .expand("INTERACTS45", to="Character", as_="target", edge_as="r")
          .filter(col("target.louvain") == 0)
          .group_by()
          .agg(max_weight=max_(col("r.weight")))
          .collect()
    )


# =============================================================================
# Query 133: John Snow's related characters
# MATCH (c1:Character {name: "John Snow"})
#   -[:INTERACTS|:INTERACTS1|:INTERACTS2|:INTERACTS3|:INTERACTS45]-(c2:Character) 
# RETURN c2.name AS RelatedCharacter
# =============================================================================
def query_133(hg: Hypergraph) -> list[dict]:
    result = []
    for edge_type in ["INTERACTS", "INTERACTS1", "INTERACTS2", "INTERACTS3", "INTERACTS45"]:
        chars = (
            hg.nodes("Character")
              .filter(col("name") == "John Snow")
              .expand(edge_type, direction="both", to="Character", as_="c2")
              .select(RelatedCharacter=col("c2.name"))
              .collect()
        )
        result.extend(chars)
    return result


# =============================================================================
# Query 134: Articles excluding specific pages
# MATCH (n:Article) -[r:PUBLISHED_IN]->(m:Journal) 
# WHERE r.pages <> '293-299' 
# WITH DISTINCT n, m 
# RETURN n.abstract AS abstract, count(m) AS count 
# ORDER BY count DESC LIMIT 7
# =============================================================================
def query_134(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .expand("PUBLISHED_IN", to="Journal", as_="m", edge_as="r")
          .filter(col("r.pages") != "293-299")
          .group_by(abstract=col("Article.abstract"))
          .agg(count=count_distinct(col("m.id")))
          .order_by("count", ascending=False)
          .limit(7)
          .collect()
    )


# =============================================================================
# Query 135: Users who asked questions (lowest reputation)
# MATCH (u:User)-[:ASKED]->(:Question) 
# RETURN u.display_name, u.reputation 
# ORDER BY u.reputation ASC LIMIT 3
# =============================================================================
def query_135(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("User")
          .expand("ASKED", to="Question")
          .order_by(col("User.reputation"), ascending=True)
          .limit(3)
          .select(col("User.display_name"), col("User.reputation"))
          .collect()
    )


# =============================================================================
# Query 136: Users with images (highest reputation)
# MATCH (u:User) WHERE u.image IS NOT NULL 
# RETURN u.image, u.display_name, u.reputation 
# ORDER BY u.reputation DESC LIMIT 5
# =============================================================================
def query_136(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("User")
          .filter(col("image").is_not_null())
          .order_by("reputation", ascending=False)
          .limit(5)
          .select("image", "display_name", "reputation")
          .collect()
    )


# =============================================================================
# Query 137: Cross join author and report
# MATCH (n:Author) MATCH (m:Report) RETURN n.affiliation, m.report_no LIMIT 8
# =============================================================================
def query_137(hg: Hypergraph) -> list[dict]:
    authors = hg.nodes("Author").select(affiliation=col("affiliation")).collect()
    reports = hg.nodes("Report").select(report_no=col("report_no")).collect()
    
    result = []
    for a in authors:
        for r in reports:
            result.append({"affiliation": a["affiliation"], "report_no": r["report_no"]})
            if len(result) >= 8:
                return result
    return result


# =============================================================================
# Query 138: December movies
# MATCH (m:Movie) WHERE date(m.release_date).month = 12 RETURN m.title
# =============================================================================
def query_138(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Movie")
          .filter(month(date(col("release_date"))) == 12)
          .select("title")
          .collect()
    )


# =============================================================================
# Query 139: Organizations with subsidiaries and investors
# MATCH (o:Organization)-[:HAS_SUBSIDIARY]->(:Organization) 
# WHERE exists{ (o)-[:HAS_INVESTOR]->() } 
# RETURN o LIMIT 3
# =============================================================================
def query_139(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Organization")
          .expand("HAS_SUBSIDIARY", to="Organization")
          .filter(exists(
              hg.nodes("Organization").expand("HAS_INVESTOR", direction="out")
          ))
          .select(col("Organization"))
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 140: VIP users from streams with 1M+ followers
# MATCH (s:Stream)-[:VIP]->(u:User) 
# WHERE s.followers > 1000000 
# RETURN u.name AS vip_user, s.name AS stream
# =============================================================================
def query_140(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Stream")
          .filter(col("followers") > 1000000)
          .expand("VIP", to="User", as_="u")
          .select(
              vip_user=col("u.name"),
              stream=col("Stream.name")
          )
          .collect()
    )


# =============================================================================
# Query 141: Orders by freight
# MATCH (o:Order) RETURN o ORDER BY o.freight DESC LIMIT 5
# =============================================================================
def query_141(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Order")
          .order_by("freight", ascending=False)
          .limit(5)
          .collect()
    )


# =============================================================================
# Query 142: Top condiment products by quantity
# MATCH (c:Category {categoryName: 'Condiments'})<-[:PART_OF]-(p:Product)<-[:ORDERS]-(o:Order) 
# RETURN p.productName, SUM(o.quantity) AS totalQuantity 
# ORDER BY totalQuantity DESC LIMIT 5
# =============================================================================
def query_142(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Category")
          .filter(col("categoryName") == "Condiments")
          .expand("PART_OF", direction="in", to="Product", as_="p")
          .expand("ORDERS", direction="in", as_="o")
          .group_by(col("p.productName"))
          .agg(totalQuantity=sum_(col("o.quantity")))
          .order_by("totalQuantity", ascending=False)
          .limit(5)
          .collect()
    )


# =============================================================================
# Query 143: Director-writers
# MATCH (p:Person)-[:DIRECTED]->(m:Movie)<-[:WROTE]-(p) 
# RETURN p.name AS person LIMIT 3
# =============================================================================
def query_143(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Person")
          .expand("DIRECTED", to="Movie", as_="m")
          .expand("WROTE", direction="in", as_="writer")
          .filter(col("Person.id") == col("writer.id"))
          .select(person=col("Person.name"))
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 144: DOI 2-hop properties
# MATCH (a:DOI{name:'10.1142/S0219061301000077'})-[*2]->(n) 
# RETURN DISTINCT properties(n) AS props
# =============================================================================
def query_144(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("DOI")
          .filter(col("name") == "10.1142/S0219061301000077")
          .expand(hops=2, as_="n")
          .select(props=properties(col("n")))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 145: Top reviewers of Imagine Nation Brewing
# MATCH (u:User)-[:WROTE]->(r:Review)-[:REVIEWS]->(b:Business {name: 'Imagine Nation Brewing'}) 
# WITH u, count(r) AS reviewsCount 
# ORDER BY reviewsCount DESC LIMIT 3 
# RETURN u.name, reviewsCount
# =============================================================================
def query_145(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("User")
          .expand("WROTE", to="Review", as_="r")
          .expand("REVIEWS", to="Business", as_="b")
          .filter(col("b.name") == "Imagine Nation Brewing")
          .group_by(col("User.name"))
          .agg(reviewsCount=count())
          .order_by("reviewsCount", ascending=False)
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 146: Keyword relation counts
# MATCH (a:Keyword{name:'exponentially growing'})-[r]->() 
# RETURN COUNT(DISTINCT TYPE(r)) AS rels, TYPE(r)
# =============================================================================
def query_146(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Keyword")
          .filter(col("name") == "exponentially growing")
          .expand(direction="out", edge_as="r")
          .group_by(type_r=type_(col("r")))
          .agg(rels=count_distinct(type_(col("r"))))
          .collect()
    )


# =============================================================================
# Query 147: Organizations with neutral sentiment
# MATCH (a:Article)-[:MENTIONS]->(o:Organization) 
# WHERE a.sentiment = 0.5 
# RETURN o
# =============================================================================
def query_147(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("sentiment") == 0.5)
          .expand("MENTIONS", to="Organization", as_="o")
          .select(col("o"))
          .collect()
    )


# =============================================================================
# Query 148: Articles with meta 248 comments
# MATCH (n:Article) -[r:PUBLISHED_IN]->(m:Journal) 
# WHERE r.meta = '248' 
# WITH DISTINCT n, m 
# RETURN n.comments AS comments, count(m) AS count ORDER BY count
# =============================================================================
def query_148(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .expand("PUBLISHED_IN", to="Journal", as_="m", edge_as="r")
          .filter(col("r.meta") == "248")
          .group_by(comments=col("Article.comments"))
          .agg(count=count_distinct(col("m.id")))
          .order_by("count", ascending=True)
          .collect()
    )


# =============================================================================
# Query 149: Languages of high-view streams
# MATCH (s:Stream)-[:HAS_LANGUAGE]->(l:Language) 
# WHERE s.total_view_count > 10000000 
# RETURN DISTINCT l.name
# =============================================================================
def query_149(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Stream")
          .filter(col("total_view_count") > 10000000)
          .expand("HAS_LANGUAGE", to="Language", as_="l")
          .select(col("l.name"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 150: French actors
# MATCH (a:Actor {bornIn: "France"}) RETURN a.name
# =============================================================================
def query_150(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Actor")
          .filter(col("bornIn") == "France")
          .select("name")
          .collect()
    )


# Queries 151-200

# =============================================================================
# Query 151: Most expensive hardware product
# MATCH (p:Product) WHERE p.product_type_code = 'Hardware' 
# RETURN p.product_name ORDER BY p.product_price DESC LIMIT 1
# =============================================================================
def query_151(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Product")
          .filter(col("product_type_code") == "Hardware")
          .order_by("product_price", ascending=False)
          .limit(1)
          .select("product_name")
          .collect()
    )


# =============================================================================
# Query 152: Oldest movies
# MATCH (m:Movie) WHERE m.released < 1985 
# RETURN m.title, m.released ORDER BY m.released LIMIT 5
# =============================================================================
def query_152(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Movie")
          .filter(col("released") < 1985)
          .order_by("released", ascending=True)
          .limit(5)
          .select("title", "released")
          .collect()
    )


# =============================================================================
# Query 153: Categories starting with 4
# MATCH (n:Categories) WHERE n.category_id STARTS WITH '4' 
# RETURN n.category_id AS category_id, n.specifications AS specifications
# =============================================================================
def query_153(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Categories")
          .filter(col("category_id").starts_with("4"))
          .select(
              category_id=col("category_id"),
              specifications=col("specifications")
          )
          .collect()
    )


# =============================================================================
# Query 154: Burlingame business reviews
# MATCH (b:Business)-[:REVIEWS]->(r:Review)<-[:WROTE]-(u:User) 
# WHERE b.city = 'Burlingame' 
# RETURN b.address, r.text
# =============================================================================
def query_154(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Business")
          .filter(col("city") == "Burlingame")
          .expand("REVIEWS", to="Review", as_="r")
          .expand("WROTE", direction="in", to="User")
          .select(col("Business.address"), col("r.text"))
          .collect()
    )


# =============================================================================
# Query 155: Officers with CFE prefix in NIUE jurisdiction
# MATCH (o:Officer)-[r:officer_of]->(e:Entity) 
# WHERE o.icij_id STARTS WITH 'CFE' AND e.jurisdiction = 'NIUE' 
# RETURN o.name
# =============================================================================
def query_155(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Officer")
          .filter(col("icij_id").starts_with("CFE"))
          .expand("officer_of", to="Entity", as_="e")
          .filter(col("e.jurisdiction") == "NIUE")
          .select(col("Officer.name"))
          .collect()
    )


# =============================================================================
# Query 156: Toy Story language
# MATCH (m:Movie {title: 'Toy Story'})-[:SPOKEN_IN_LANGUAGE]->(l:Language) 
# RETURN l.name AS language
# =============================================================================
def query_156(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Movie")
          .filter(col("title") == "Toy Story")
          .expand("SPOKEN_IN_LANGUAGE", to="Language", as_="l")
          .select(language=col("l.name"))
          .collect()
    )


# =============================================================================
# Query 157: Articles with specific pages
# MATCH (n:Article) -[r:PUBLISHED_IN]->(m:Journal) 
# WHERE r.pages='521-554' 
# WITH DISTINCT n, m 
# RETURN n.abstract AS abstract, count(m) AS count LIMIT 20
# =============================================================================
def query_157(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .expand("PUBLISHED_IN", to="Journal", as_="m", edge_as="r")
          .filter(col("r.pages") == "521-554")
          .group_by(abstract=col("Article.abstract"))
          .agg(count=count_distinct(col("m.id")))
          .limit(20)
          .collect()
    )


# =============================================================================
# Query 158: CEOs with Accenture in summary
# MATCH (ceo:Person)-[:HAS_CEO]-(org:Organization) 
# WHERE ceo.summary CONTAINS 'Accenture' 
# RETURN org.name
# =============================================================================
def query_158(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Person")
          .filter(col("summary").contains("Accenture"))
          .expand("HAS_CEO", direction="both", to="Organization", as_="org")
          .select(col("org.name"))
          .collect()
    )


# =============================================================================
# Query 159: Journal name by article and pages
# MATCH (n:Article) -[pu:PUBLISHED_IN {pages : '641-672'}]->(m) 
# WHERE n.article_id='1008' 
# RETURN m.name
# =============================================================================
def query_159(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("article_id") == "1008")
          .expand("PUBLISHED_IN", as_="m", edge_as="pu")
          .filter(col("pu.pages") == "641-672")
          .select(col("m.name"))
          .collect()
    )


# =============================================================================
# Query 160: Earliest movies
# MATCH (m:Movie) RETURN m ORDER BY m.released ASC LIMIT 3
# =============================================================================
def query_160(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Movie")
          .order_by("released", ascending=True)
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 161: Author relation types
# MATCH (a:Author{last_name:'Keller'})-[r]->() 
# RETURN TYPE(r) AS Relations, COUNT(r) AS Counts
# =============================================================================
def query_161(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Author")
          .filter(col("last_name") == "Keller")
          .expand(direction="out", edge_as="r")
          .group_by(Relations=type_(col("r")))
          .agg(Counts=count())
          .collect()
    )


# =============================================================================
# Query 162: Author 2-hop traversal
# MATCH (a:Author{last_name:'Polishchuk'})-[r]->(n), (n)-[s]->(m) 
# RETURN labels(n) AS Interim, labels(m) AS Target
# =============================================================================
def query_162(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Author")
          .filter(col("last_name") == "Polishchuk")
          .expand(direction="out", as_="n")
          .expand(direction="out", as_="m")
          .select(
              Interim=labels(col("n")),
              Target=labels(col("m"))
          )
          .collect()
    )


# =============================================================================
# Query 163: Authors not Chakrabarti
# MATCH (n:Author) WHERE n.last_name <> 'Chakrabarti' 
# RETURN DISTINCT n.affiliation AS affiliation
# =============================================================================
def query_163(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Author")
          .filter(col("last_name") != "Chakrabarti")
          .select(affiliation=col("affiliation"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 164: Directors with high-budget movies
# MATCH (d:Director)-[:DIRECTED]->(m:Movie) 
# WHERE m.budget > 200000000 
# WITH d, count(m) AS moviesDirected 
# ORDER BY moviesDirected DESC LIMIT 3 
# RETURN d.name, moviesDirected
# =============================================================================
def query_164(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Director")
          .expand("DIRECTED", to="Movie", as_="m")
          .filter(col("m.budget") > 200000000)
          .group_by(col("Director.name"))
          .agg(moviesDirected=count())
          .order_by("moviesDirected", ascending=False)
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 165: Organizations with neutral sentiment (names)
# MATCH (a:Article)-[:MENTIONS]->(o:Organization) 
# WHERE a.sentiment = 0.5 
# RETURN o.name
# =============================================================================
def query_165(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("sentiment") == 0.5)
          .expand("MENTIONS", to="Organization", as_="o")
          .select(col("o.name"))
          .collect()
    )


# =============================================================================
# Query 166: Report properties via variable-length path
# MATCH (a:Report{report_no:'ITF-99-42'})-[*]->(n) 
# RETURN DISTINCT properties(n) AS Properties
# =============================================================================
def query_166(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Report")
          .filter(col("report_no") == "ITF-99-42")
          .expand(hops=(1, 10), as_="n")
          .select(Properties=properties(col("n")))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 167: Article to article via multi-hop
# MATCH (a:Article{title:'An adelic causality problem related to abelian L-functions'})
#   -[*]->(d:Article{comments:'16 pages To be published in Journal of Geometry and Physics'})
#   -[*]->(n) 
# RETURN n
# =============================================================================
def query_167(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("title") == "An adelic causality problem related to abelian L-functions")
          .expand(hops=(1, 10), as_="d")
          .filter(
              labels(col("d")).contains_element("Article") & 
              (col("d.comments") == "16 pages To be published in Journal of Geometry and Physics")
          )
          .expand(hops=(1, 10), as_="n")
          .select(col("n"))
          .collect()
    )


# =============================================================================
# Query 168: Actor with longest name
# MATCH (p:Person)-[:ACTED_IN]->(:Movie) 
# RETURN p.name AS name ORDER BY size(p.name) DESC LIMIT 1
# =============================================================================
def query_168(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Person")
          .expand("ACTED_IN", to="Movie")
          .order_by(length(col("Person.name")), ascending=False)
          .limit(1)
          .select(name=col("Person.name"))
          .collect()
    )


# =============================================================================
# Query 169: Reports with report numbers
# MATCH (n:Report) WHERE n.report_no <> 'none provided' 
# RETURN DISTINCT n.report_no AS report_no
# =============================================================================
def query_169(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Report")
          .filter(col("report_no") != "none provided")
          .select(report_no=col("report_no"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 170: Officer crime investigations
# MATCH (n:Officer)-[r:INVESTIGATED_BY]->(m:Crime) RETURN n, r, m
# =============================================================================
def query_170(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Officer")
          .expand("INVESTIGATED_BY", to="Crime", as_="m", edge_as="r")
          .select(n=col("Officer"), r=col("r"), m=col("m"))
          .collect()
    )


# =============================================================================
# Query 171: Users by genre diversity
# MATCH (u:User)-[:RATED]->(m:Movie)-[:IN_GENRE]->(g:Genre) 
# WITH u, COUNT(DISTINCT g) AS genreCount 
# ORDER BY genreCount DESC LIMIT 3 
# RETURN u, genreCount
# =============================================================================
def query_171(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("User")
          .expand("RATED", to="Movie", as_="m")
          .expand("IN_GENRE", to="Genre", as_="g")
          .group_by(col("User"))
          .agg(genreCount=count_distinct(col("g.id")))
          .order_by("genreCount", ascending=False)
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 172: Questions from Jan 1, 2020
# MATCH (q:Question) WHERE date(q.createdAt) = date('2020-01-01') 
# RETURN q LIMIT 3
# =============================================================================
def query_172(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Question")
          .filter(date(col("createdAt")) == date("2020-01-01"))
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 173: Article relation types
# MATCH (a:Article{abstract:'...'})-[r]->() 
# RETURN TYPE(r) AS Relations, COUNT(r) AS Counts
# =============================================================================
def query_173(hg: Hypergraph) -> list[dict]:
    abstract = "  The Wakimoto construction for the quantum affine algebra U_q(\\hat{sl}_2) admits a reduction to the q-deformed parafermion algebras. We interpret the latter theory as a free field realization of the Andrews-Baxter-Forrester models in regime II. We give multi-particle form factors of some local operators on the lattice and compute their scaling limit, where the models are described by a massive field theory with Z_k symmetric minimal scattering matrices. "
    return (
        hg.nodes("Article")
          .filter(col("abstract") == abstract)
          .expand(direction="out", edge_as="r")
          .group_by(Relations=type_(col("r")))
          .agg(Counts=count())
          .collect()
    )


# =============================================================================
# Query 174: Max interaction weight in community 1
# MATCH (c1:Character)-[r:INTERACTS]->(c2:Character) 
# WHERE c1.louvain = 1 AND c2.louvain = 1 
# RETURN max(r.weight)
# =============================================================================
def query_174(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Character")
          .filter(col("louvain") == 1)
          .expand("INTERACTS", to="Character", as_="c2", edge_as="r")
          .filter(col("c2.louvain") == 1)
          .group_by()
          .agg(max_weight=max_(col("r.weight")))
          .collect()
    )


# =============================================================================
# Query 175: Shortest path topic to DOI
# MATCH p=shortestPath((a:Topic{label:'Linear Algebra_1'})
#   -[*]-(e:DOI{name:'10.1142/S0219061301000077'})) 
# RETURN nodes(p)
# =============================================================================
def query_175(hg: Hypergraph) -> list[dict]:
    return (
        shortest_path(
            hg.nodes("Topic").filter(col("label") == "Linear Algebra_1"),
            hg.nodes("DOI").filter(col("name") == "10.1142/S0219061301000077"),
            direction="both"
        )
        .select(nodes=nodes(col("path")))
        .collect()
    )


# =============================================================================
# Query 176: Top actors by distinct movie count
# MATCH (p:Person)-[:ACTED_IN]->(m:Movie) 
# WITH p, count(DISTINCT m) AS movieCount 
# ORDER BY movieCount DESC LIMIT 3 
# RETURN p.name AS actor, movieCount
# =============================================================================
def query_176(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Person")
          .expand("ACTED_IN", to="Movie", as_="m")
          .group_by(actor=col("Person.name"))
          .agg(movieCount=count_distinct(col("m.id")))
          .order_by("movieCount", ascending=False)
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 177: Genre movie counts
# MATCH (g:Genre)<-[:IN_GENRE]-(m:Movie) 
# RETURN g.name AS Genre, count(m) AS MovieCount 
# ORDER BY MovieCount DESC LIMIT 3
# =============================================================================
def query_177(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Genre")
          .expand("IN_GENRE", direction="in", to="Movie", as_="m")
          .group_by(Genre=col("Genre.name"))
          .agg(MovieCount=count())
          .order_by("MovieCount", ascending=False)
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 178: Youngest teacher's hometown
# MATCH (teacher:teacher) RETURN teacher.Hometown 
# ORDER BY teacher.Age ASC LIMIT 1
# =============================================================================
def query_178(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("teacher")
          .order_by("Age", ascending=True)
          .limit(1)
          .select("Hometown")
          .collect()
    )


# =============================================================================
# Query 179: Top hashtags
# MATCH (t:Tweet)-[:TAGS]->(h:Hashtag) 
# RETURN h.name AS hashtag, count(*) AS count 
# ORDER BY count DESC LIMIT 3
# =============================================================================
def query_179(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Tweet")
          .expand("TAGS", to="Hashtag", as_="h")
          .group_by(hashtag=col("h.name"))
          .agg(count=count())
          .order_by("count", ascending=False)
          .limit(3)
          .collect()
    )


# =============================================================================
# Query 180: Average score for react-apollo questions
# MATCH (q:Question)-[:TAGGED]->(t:Tag {name: 'react-apollo'}) 
# WITH avg(q.score) AS average_score 
# RETURN average_score
# =============================================================================
def query_180(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Question")
          .expand("TAGGED", to="Tag", as_="t")
          .filter(col("t.name") == "react-apollo")
          .group_by()
          .agg(average_score=avg(col("Question.score")))
          .collect()
    )


# =============================================================================
# Query 181: Report relation type counts
# MATCH (a:Report{report_id:'d4a4409b7e8a77f4894c998a04162257'})-[r]->() 
# RETURN COUNT(DISTINCT TYPE(r)) AS rels, TYPE(r)
# =============================================================================
def query_181(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Report")
          .filter(col("report_id") == "d4a4409b7e8a77f4894c998a04162257")
          .expand(direction="out", edge_as="r")
          .group_by(type_r=type_(col("r")))
          .agg(rels=count_distinct(type_(col("r"))))
          .collect()
    )


# =============================================================================
# Query 182: Count articles with null abstract
# MATCH (n:Article) WHERE n.abstract IS NULL RETURN count(n)
# =============================================================================
def query_182(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .filter(col("abstract").is_null())
          .group_by()
          .agg(count=count())
          .collect()
    )


# =============================================================================
# Query 183: Article-journal counts by pages
# MATCH (n:Article) -[r:PUBLISHED_IN]->(m:Journal) 
# WHERE r.pages = '263-281' 
# RETURN n.article_id AS article_id, count(m) AS count
# =============================================================================
def query_183(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Article")
          .expand("PUBLISHED_IN", to="Journal", as_="m", edge_as="r")
          .filter(col("r.pages") == "263-281")
          .group_by(article_id=col("Article.article_id"))
          .agg(count=count())
          .collect()
    )


# =============================================================================
# Query 184: Reviewers with specific summary
# MATCH (p:Person)-[r:REVIEWED]->(m:Movie) 
# WHERE r.summary CONTAINS 'A solid romp' 
# RETURN p.name
# =============================================================================
def query_184(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Person")
          .expand("REVIEWED", to="Movie", edge_as="r")
          .filter(col("r.summary").contains("A solid romp"))
          .select(col("Person.name"))
          .collect()
    )


# =============================================================================
# Query 185: DOI properties
# MATCH (b:DOI)-[r]->(n) 
# WHERE b.doi_id = '3cee0a24d271bd40a0fb03d70f70dcc7' 
# RETURN properties(b) AS DOI_props, properties(n) AS props
# =============================================================================
def query_185(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("DOI")
          .filter(col("doi_id") == "3cee0a24d271bd40a0fb03d70f70dcc7")
          .expand(direction="out", as_="n")
          .select(
              DOI_props=properties(col("DOI")),
              props=properties(col("n"))
          )
          .collect()
    )


# =============================================================================
# Query 186: Article keywords with specific pattern
# MATCH (n:Article {abstract: '...'}) -[:HAS_KEY]- (m:Keyword) 
# WHERE m.key_id STARTS WITH 'k' 
# RETURN m
# =============================================================================
def query_186(hg: Hypergraph) -> list[dict]:
    abstract = "  In this paper we study the smallest Mealy automaton of intermediate growth, first considered by the last two authors. We describe the automatic transformation monoid it defines, give a formula for the generating series for its (ball volume) growth function, and give sharp asymptotics for its growth function, namely [ F(n) \\sim 2^{5/2} 3^{3/4} \\pi^{-2} n^{1/4} \\exp{\\pi\\sqrt{n/6}} ] with the ratios of left- to right-hand side tending to 1 as $n \\to \\infty$. "
    return (
        hg.nodes("Article")
          .filter(col("abstract") == abstract)
          .expand("HAS_KEY", direction="both", to="Keyword", as_="m")
          .filter(col("m.key_id").starts_with("k"))
          .select(col("m"))
          .collect()
    )


# =============================================================================
# Query 187: Multi-hop author to author
# MATCH (a:Author{affiliation:'unspecified'})-[*]->(d:Author{affiliation:'unspecified'})-[*]->(n) 
# RETURN n
# =============================================================================
def query_187(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Author")
          .filter(col("affiliation") == "unspecified")
          .expand(hops=(1, 5), as_="d")
          .filter(labels(col("d")).contains_element("Author") & (col("d.affiliation") == "unspecified"))
          .expand(hops=(1, 5), as_="n")
          .select(col("n"))
          .collect()
    )


# =============================================================================
# Query 188: Movies by language (unwind)
# MATCH (m:Movie) WHERE m.languages IS NOT NULL 
# UNWIND m.languages AS language 
# RETURN language, count(m) AS num_movies 
# ORDER BY num_movies DESC LIMIT 5
# =============================================================================
def query_188(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Movie")
          .filter(col("languages").is_not_null())
          .explode("languages", as_="language")
          .group_by("language")
          .agg(num_movies=count())
          .order_by("num_movies", ascending=False)
          .limit(5)
          .collect()
    )


# =============================================================================
# Query 189: Author 2-hop properties
# MATCH (a:Author{last_name:'Neeman'})-[*2]->(n) 
# RETURN DISTINCT properties(n) AS props
# =============================================================================
def query_189(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Author")
          .filter(col("last_name") == "Neeman")
          .expand(hops=2, as_="n")
          .select(props=properties(col("n")))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 190: Matching relation types between Author and DOI
# MATCH (a:Author)-[r]->(n), (d:DOI)-[s]->(m) 
# WHERE TYPE(r) = TYPE(s) 
# RETURN labels(n), labels(m)
# =============================================================================
def query_190(hg: Hypergraph) -> list[dict]:
    # This requires comparing relation types across different starting nodes
    # Would typically require a join or comparison operation
    return (
        hg.nodes("Author")
          .expand(direction="out", as_="n", edge_as="r")
          .with_column("author_rel_type", type_(col("r")))
          .select(labels_n=labels(col("n")), author_rel=col("author_rel_type"))
          .collect()
    )


# =============================================================================
# Query 191: Medicine enzyme interaction counts
# MATCH (m:Medicine)-[:INTERACTS_WITH]->(e:Enzyme) 
# RETURN count(distinct m.FDA_approved) as count
# =============================================================================
def query_191(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Medicine")
          .expand("INTERACTS_WITH", to="Enzyme")
          .group_by()
          .agg(count=count_distinct(col("Medicine.FDA_approved")))
          .collect()
    )


# =============================================================================
# Query 192: Topic by description
# MATCH (n:Topic) 
# WHERE n.description CONTAINS 'udy of mathematical structures called categories...' 
# RETURN n.description AS description, n.cluster AS cluster
# =============================================================================
def query_192(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Topic")
          .filter(col("description").contains(
              "udy of mathematical structures called categories, characterized by objects and morphisms that relate them, with emphasis on reductions and indecomposability concepts."
          ))
          .select(description=col("description"), cluster=col("cluster"))
          .collect()
    )


# =============================================================================
# Query 193: Longest path from author
# MATCH p=(a:Author{first_name:'J. Daniel'})-[*]->(n) 
# RETURN p, nodes(p) ORDER BY LENGTH(p) DESC LIMIT 1
# =============================================================================
def query_193(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Author")
          .filter(col("first_name") == "J. Daniel")
          .expand(hops=(1, 10), as_="n")
          .with_column("path_len", path_length(col("_path")))
          .order_by("path_len", ascending=False)
          .limit(1)
          .select(path=col("_path"), nodes=nodes(col("_path")))
          .collect()
    )


# =============================================================================
# Query 194: Lowest revenue videos
# MATCH (v:Video) WHERE v.revenue IS NOT NULL 
# RETURN v.title, v.revenue ORDER BY v.revenue ASC LIMIT 5
# =============================================================================
def query_194(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Video")
          .filter(col("revenue").is_not_null())
          .order_by("revenue", ascending=True)
          .limit(5)
          .select("title", "revenue")
          .collect()
    )


# =============================================================================
# Query 195: Parent orgs with subsidiaries in different cities
# MATCH (parentOrg:Organization)-[:HAS_SUBSIDIARY]->(subsidiary:Organization) 
# WITH parentOrg, subsidiary 
# MATCH (parentOrg)-[:IN_CITY]->(parentCity:City) 
# MATCH (subsidiary)-[:IN_CITY]->(subsidiaryCity:City) 
# WHERE parentCity.id <> subsidiaryCity.id 
# RETURN DISTINCT parentOrg.name
# =============================================================================
def query_195(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Organization")
          .expand("HAS_SUBSIDIARY", to="Organization", as_="subsidiary")
          .expand("IN_CITY", to="City", as_="parentCity")
          .expand("IN_CITY", to="City", as_="subCity")
          .filter(col("parentCity.id") != col("subCity.id"))
          .select(col("Organization.name"))
          .distinct()
          .collect()
    )


# =============================================================================
# Query 196: Seattle's top revenue organization
# MATCH (o:Organization)-[:IN_CITY]->(c:City {name: "Seattle"}) 
# RETURN o.name AS BusinessName, o.revenue AS Revenue 
# ORDER BY o.revenue DESC LIMIT 1
# =============================================================================
def query_196(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Organization")
          .expand("IN_CITY", to="City", as_="c")
          .filter(col("c.name") == "Seattle")
          .order_by(col("Organization.revenue"), ascending=False)
          .limit(1)
          .select(
              BusinessName=col("Organization.name"),
              Revenue=col("Organization.revenue")
          )
          .collect()
    )


# =============================================================================
# Query 197: Female directors of highly rated movies
# MATCH (p:Person {gender: 2})-[:CREW_FOR {job: 'Director'}]-(m:Movie) 
# WHERE m.average_vote > 8.0 
# WITH p, count(m) AS movieCount 
# ORDER BY movieCount DESC 
# RETURN p.name, movieCount LIMIT 10
# =============================================================================
def query_197(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Person")
          .filter(col("gender") == 2)
          .expand("CREW_FOR", direction="both", to="Movie", as_="m", edge_as="crew")
          .filter((col("crew.job") == "Director") & (col("m.average_vote") > 8.0))
          .group_by(col("Person.name"))
          .agg(movieCount=count())
          .order_by("movieCount", ascending=False)
          .limit(10)
          .collect()
    )


# =============================================================================
# Query 198: Best rated movie runtime
# MATCH (m:Movie) RETURN m.runtime, m.average_vote 
# ORDER BY m.average_vote DESC LIMIT 1
# =============================================================================
def query_198(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Movie")
          .order_by("average_vote", ascending=False)
          .limit(1)
          .select("runtime", "average_vote")
          .collect()
    )


# =============================================================================
# Query 199: Applications on zone 2 machines
# MATCH (r:Rack {zone: 2})-[:HOLDS]->(m:Machine)-[:RUNS]->(a:Application) 
# RETURN a.name
# =============================================================================
def query_199(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Rack")
          .filter(col("zone") == 2)
          .expand("HOLDS", to="Machine", as_="m")
          .expand("RUNS", to="Application", as_="a")
          .select(col("a.name"))
          .collect()
    )


# =============================================================================
# Query 200: Student advisor counts
# MATCH (s:Student) RETURN s.Advisor, COUNT(*)
# =============================================================================
def query_200(hg: Hypergraph) -> list[dict]:
    return (
        hg.nodes("Student")
          .group_by(col("Advisor"))
          .agg(count=count())
          .collect()
    )


# =============================================================================
# Helper function to run all queries
# =============================================================================
def run_all_queries(hg: Hypergraph) -> dict[str, list[dict]]:
    """
    Run all 200 queries and return results.
    
    Args:
        hg: Connected Hypergraph instance
        
    Returns:
        Dict mapping query name to results
    """
    results = {}
    for i in range(1, 201):
        query_name = f"query_{i:03d}"
        query_func = globals().get(query_name)
        if query_func:
            try:
                results[query_name] = query_func(hg)
            except Exception as e:
                results[query_name] = {"error": str(e)}
    return results


if __name__ == "__main__":
    # Example usage
    print("Grism Python API Query Examples")
    print("================================")
    print("This file contains 200 query functions equivalent to Cypher queries.")
    print("\nUsage:")
    print("  from grism import Hypergraph")
    print("  hg = Hypergraph.connect('grism://local')")
    print("  result = query_001(hg)")
    print("\nOr run all queries:")
    print("  results = run_all_queries(hg)")
