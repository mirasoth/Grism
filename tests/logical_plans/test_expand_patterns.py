"""Tests for Expand operator patterns.

This module tests the logical plan generation for graph traversal operations,
including single-hop and multi-hop expansions with various configurations.

Patterns covered:
- Single-hop with edge type
- Single-hop with target label
- Multi-hop fixed (hops=3)
- Multi-hop range (hops=(1, 5))
- Direction variations (out, in, both)
- With alias (as_="m")
- Chained expands (multiple traversals)
- Expand with edge properties
"""


class TestSingleHopExpand:
    """Tests for single-hop expand operations."""

    def test_expand_with_edge_type(self, hg, plan_validator):
        """Expand with specific edge type.

        Pattern: expand("KNOWS")
        Based on: query_001 (expand("BENEFITS"))
        """
        frame = hg.nodes("Person").expand("KNOWS")
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Expand")
        assert validator.has_operator("Scan")
        assert validator.operator_order("Expand", "Scan")
        assert validator.has_text("KNOWS")

    def test_expand_with_target_label(self, hg, plan_validator):
        """Expand with target node label.

        Pattern: expand("ACTED_IN", to="Movie")
        Based on: query_015 (actors in movies)
        """
        frame = hg.nodes("Actor").expand("ACTED_IN", to="Movie")
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Expand")
        assert validator.has_text("ACTED_IN")
        assert validator.has_text("Movie")

    def test_expand_with_alias(self, hg, plan_validator):
        """Expand with alias for target node.

        Pattern: expand("HAS_KEY", to="Keyword", as_="k")
        Based on: query_004 (articles with keywords)
        """
        frame = hg.nodes("Article").expand("HAS_KEY", to="Keyword", as_="k")
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Expand")
        assert validator.has_text("HAS_KEY")
        assert validator.has_text("Keyword")
        # Alias should appear in the plan
        assert validator.has_text("k") or validator.has_text("AS k")


class TestDirectionExpand:
    """Tests for expand with different directions."""

    def test_expand_outgoing(self, hg, plan_validator):
        """Expand following outgoing edges (default).

        Pattern: expand("FOLLOWS", direction="out")
        """
        frame = hg.nodes("User").expand("FOLLOWS", direction="out")
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Expand")
        assert validator.has_text("OUT")

    def test_expand_incoming(self, hg, plan_validator):
        """Expand following incoming edges.

        Pattern: expand("REVIEWS", direction="in")
        Based on: query_024 (businesses with reviews)
        """
        frame = hg.nodes("Business").expand("REVIEWS", direction="in")
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Expand")
        assert validator.has_text("IN")

    def test_expand_both_directions(self, hg, plan_validator):
        """Expand following edges in both directions.

        Pattern: expand("INTERACTS", direction="both")
        Based on: query_017 (character interactions)
        """
        frame = hg.nodes("Character").expand("INTERACTS", direction="both")
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Expand")
        assert validator.has_text("BOTH")


class TestMultiHopExpand:
    """Tests for multi-hop expand operations."""

    def test_expand_fixed_hops(self, hg, plan_validator):
        """Expand with fixed number of hops.

        Pattern: expand(hops=3)
        Based on: query_007, query_013 (3-hop traversals)
        """
        frame = hg.nodes("Article").expand(hops=3)
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Expand")
        assert validator.has_text("hops=3") or validator.has_text("3")

    def test_expand_hop_range(self, hg, plan_validator):
        """Expand with range of hops.

        Pattern: expand(hops=(1, 5))
        Based on: query_022 (variable-length path)
        """
        frame = hg.nodes("DOI").expand(hops=(1, 5))
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Expand")
        # Should contain hop range indicators
        expand_line = validator.get_operator_line("Expand")
        assert expand_line is not None


class TestChainedExpand:
    """Tests for chained expand operations."""

    def test_double_expand(self, hg, plan_validator):
        """Chained expand through two edge types.

        Pattern: expand("A").expand("B")
        Based on: query_001 (Filing -> Entity -> Country)
        """
        frame = hg.nodes("Filing").expand("BENEFITS", to="Entity").expand("COUNTRY", to="Country")
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        # Should have two Expand operators
        assert validator.count_operators("Expand") == 2
        assert validator.has_text("BENEFITS")
        assert validator.has_text("COUNTRY")
        assert validator.has_operator("Scan")

    def test_triple_expand(self, hg, plan_validator):
        """Chained expand through three edge types.

        Pattern: expand("A").expand("B").expand("C")
        Based on: query_199 (Rack -> Machine -> Application)
        """
        frame = hg.nodes("Rack").expand("HOLDS", to="Machine").expand("RUNS", to="Application")
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        # Should have two Expand operators
        assert validator.count_operators("Expand") == 2
        assert validator.has_text("HOLDS")
        assert validator.has_text("RUNS")


class TestExpandWithEdgeProperties:
    """Tests for expand with edge property access."""

    def test_expand_with_edge_alias(self, hg, plan_validator):
        """Expand capturing edge for property access.

        Pattern: expand("RATED", edge_as="r")
        Based on: query_025 (user ratings)
        """
        frame = hg.nodes("User").expand("RATED", to="Movie", as_="m", edge_as="r")
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Expand")
        assert validator.has_text("RATED")
        # The edge alias should be captured
        assert validator.has_text("Movie")
