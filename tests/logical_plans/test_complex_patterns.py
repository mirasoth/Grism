"""Tests for Complex query patterns.

This module tests the logical plan generation for complex multi-stage queries
that combine multiple operators in realistic scenarios.

Patterns covered:
- Full query chain (scan + expand + filter + group + sort + limit + select)
- Query 004 pattern (articles with keyword counts)
- Query 001 pattern (country entity counts from filings)
- Union of frames
- Multiple expands with aggregation
"""

import grism as gr


class TestFullQueryChains:
    """Tests for complete end-to-end query patterns."""

    def test_query_004_pattern(self, hg, plan_validator):
        """Full query: Articles with keyword counts.

        Pattern from query_004:
            MATCH (n:Article) -[:HAS_KEY]->(m:Keyword)
            WITH DISTINCT n, m
            RETURN n.abstract AS abstract, count(m) AS count
            ORDER BY count DESC LIMIT 7

        Operators: Scan -> Expand -> Aggregate -> Sort -> Limit -> Project
        """
        frame = (
            hg.nodes("Article")
            .expand("HAS_KEY", to="Keyword", as_="m")
            .group_by(gr.col("Article.abstract"))
            .agg(count=gr.count_distinct(gr.col("m.id")))
            .order_by("count", ascending=False)
            .limit(7)
            .select(abstract=gr.col("Article.abstract"), count=gr.col("count"))
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        # Verify all operators present
        assert validator.has_operator("Scan")
        assert validator.has_operator("Expand")
        assert validator.has_operator("Aggregate")
        assert validator.has_operator("Sort")
        assert validator.has_operator("Limit")
        assert validator.has_operator("Project")

        # Verify correct nesting (outer to inner)
        assert validator.operator_order("Project", "Limit", "Sort", "Aggregate", "Expand", "Scan")

        # Verify key parameters
        assert validator.has_text("Article")
        assert validator.has_text("HAS_KEY")
        assert validator.has_text("Keyword")
        assert validator.has_text("COUNT_DISTINCT") or validator.has_text("DISTINCT")

    def test_query_001_pattern(self, hg, plan_validator):
        """Full query: Country entity counts from filings.

        Pattern from query_001:
            MATCH (f:Filing)-[:BENEFITS]->(e:Entity)-[:COUNTRY]->(c:Country)
            WITH c.name AS country, COUNT(e) AS entityCount
            ORDER BY entityCount DESC LIMIT 3
            RETURN country, entityCount

        Operators: Scan -> Expand -> Expand -> Aggregate -> Sort -> Limit -> Project
        """
        frame = (
            hg.nodes("Filing")
            .expand("BENEFITS", to="Entity", as_="e")
            .expand("COUNTRY", to="Country", as_="c")
            .group_by(country=gr.col("c.name"))
            .agg(entityCount=gr.count())
            .order_by("entityCount", ascending=False)
            .limit(3)
            .select("country", "entityCount")
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        # Verify all operators present
        assert validator.has_operator("Scan")
        assert validator.count_operators("Expand") == 2
        assert validator.has_operator("Aggregate")
        assert validator.has_operator("Sort")
        assert validator.has_operator("Limit")
        assert validator.has_operator("Project")

        # Verify key parameters
        assert validator.has_text("Filing")
        assert validator.has_text("BENEFITS")
        assert validator.has_text("COUNTRY")
        assert validator.has_text("COUNT")

    def test_query_015_pattern(self, hg, plan_validator):
        """Full query: Top actors in pre-1980 movies.

        Pattern from query_015:
            MATCH (a:Actor)-[r:ACTED_IN]->(m:Movie)
            WHERE m.year < 1980
            RETURN a.name, count(r) AS roles
            ORDER BY roles DESC LIMIT 5

        Operators: Scan -> Expand -> Filter -> Aggregate -> Sort -> Limit -> Project
        """
        frame = (
            hg.nodes("Actor")
            .expand("ACTED_IN", to="Movie", as_="m", edge_as="r")
            .filter(gr.col("m.year") < 1980)
            .group_by(gr.col("Actor.name"))
            .agg(roles=gr.count())
            .order_by("roles", ascending=False)
            .limit(5)
            .select(gr.col("Actor.name"), "roles")
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        # Verify all operators present
        assert validator.has_operator("Scan")
        assert validator.has_operator("Expand")
        assert validator.has_operator("Filter")
        assert validator.has_operator("Aggregate")
        assert validator.has_operator("Sort")
        assert validator.has_operator("Limit")
        assert validator.has_operator("Project")

        # Verify correct ordering
        assert validator.operator_order(
            "Project", "Limit", "Sort", "Aggregate", "Filter", "Expand", "Scan"
        )


class TestUnionPatterns:
    """Tests for union operations."""

    def test_union_two_frames(self, hg, plan_validator):
        """Union of two node frames.

        Pattern: frame1.union(frame2)
        """
        frame1 = hg.nodes("Person").filter(gr.col("type") == "employee")
        frame2 = hg.nodes("Person").filter(gr.col("type") == "contractor")
        frame = frame1.union(frame2)
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Union")
        # Should have two Scan operators (one for each branch)
        assert validator.count_operators("Scan") == 2
        assert validator.count_operators("Filter") == 2


class TestMultiExpandAggregation:
    """Tests for queries with multiple expands and aggregation."""

    def test_triple_expand_with_filter(self, hg, plan_validator):
        """Multiple expands with filter and aggregation.

        Pattern from query_199 extended:
            MATCH (r:Rack)-[:HOLDS]->(m:Machine)-[:RUNS]->(a:Application)
            WHERE r.zone = 2
            RETURN count(a)
        """
        frame = (
            hg.nodes("Rack")
            .filter(gr.col("zone") == 2)
            .expand("HOLDS", to="Machine", as_="m")
            .expand("RUNS", to="Application", as_="a")
            .group_by()
            .agg(count=gr.count())
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Aggregate")
        assert validator.count_operators("Expand") == 2
        assert validator.has_operator("Filter")
        assert validator.has_operator("Scan")

        # Verify expand edge types
        assert validator.has_text("HOLDS")
        assert validator.has_text("RUNS")

    def test_expand_with_edge_filter_and_agg(self, hg, plan_validator):
        """Expand with edge property filter and aggregation.

        Pattern from query_197:
            Female directors of highly rated movies
        """
        frame = (
            hg.nodes("Person")
            .filter(gr.col("gender") == 2)
            .expand("CREW_FOR", direction="both", to="Movie", as_="m", edge_as="crew")
            .filter((gr.col("crew.job") == "Director") & (gr.col("m.average_vote") > 8.0))
            .group_by(gr.col("Person.name"))
            .agg(movieCount=gr.count())
            .order_by("movieCount", ascending=False)
            .limit(10)
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        # Verify key operators
        assert validator.has_operator("Limit")
        assert validator.has_operator("Sort")
        assert validator.has_operator("Aggregate")
        assert validator.has_operator("Expand")
        # Multiple filters
        assert validator.count_operators("Filter") >= 2
        assert validator.has_operator("Scan")


class TestComplexFilters:
    """Tests for queries with complex filter conditions."""

    def test_filter_with_or_and_null_check(self, hg, plan_validator):
        """Complex filter with OR and null check.

        Pattern from query_043:
            WHERE n.author_id = '...' OR n.affiliation IS NOT NULL
        """
        frame = (
            hg.nodes("Author")
            .filter(
                (gr.col("author_id") == "d83c43e5b1cf398c4e549843f497694b")
                | gr.col("affiliation").is_not_null()
            )
            .select(affiliation=gr.col("affiliation"))
            .distinct()
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        assert validator.has_text("OR")
        assert validator.has_text("NOT") and validator.has_text("NULL")
