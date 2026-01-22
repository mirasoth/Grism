"""Tests for Aggregation operator patterns.

This module tests the logical plan generation for group-by and aggregation
operations, which are essential for analytics queries.

Patterns covered:
- Simple count (no group-by)
- Count with group-by key
- Count distinct
- Multiple aggregates (sum, avg, max, min)
- Aggregation after expand
- Having clause (filter after aggregation)
- Named aggregation outputs
- Multi-key group-by
"""

import grism as gr


class TestSimpleAggregation:
    """Tests for simple aggregation without group-by."""

    def test_count_all(self, hg, plan_validator):
        """Count all entities.

        Pattern: group_by().agg(count=count())
        Based on: query_021 (count drivers)
        """
        frame = hg.nodes("Driver").group_by().agg(count=gr.count())
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Aggregate")
        assert validator.has_operator("Scan")
        assert validator.operator_order("Aggregate", "Scan")
        assert validator.has_text("COUNT")

    def test_max_aggregation(self, hg, plan_validator):
        """Find maximum value.

        Pattern: group_by().agg(max_val=max_(col("score")))
        """
        frame = hg.nodes("Answer").group_by().agg(max_score=gr.max_(gr.col("score")))
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Aggregate")
        assert validator.has_text("MAX")
        assert validator.has_text("score")


class TestGroupByAggregation:
    """Tests for aggregation with group-by keys."""

    def test_count_with_group_by(self, hg, plan_validator):
        """Count with single group-by key.

        Pattern: group_by("city").agg(count=count())
        Based on: query_035 (character community sizes)
        """
        frame = hg.nodes("Character").group_by(gr.col("louvain")).agg(count=gr.count())
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Aggregate")
        # Should have group key
        agg_line = validator.get_operator_line("Aggregate")
        assert agg_line is not None
        assert "keys=" in agg_line or "louvain" in agg_line
        assert validator.has_text("COUNT")

    def test_count_distinct(self, hg, plan_validator):
        """Count distinct values.

        Pattern: group_by().agg(n=count_distinct(col("ip")))
        Based on: query_023 (count distinct switch IPs)
        """
        frame = (
            hg.nodes("Rack")
            .filter(gr.col("name") == "DC1-RCK-1-1")
            .expand("HOLDS", to="Switch", as_="switch")
            .group_by()
            .agg(numberOfSwitchTypes=gr.count_distinct(gr.col("switch.ip")))
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Aggregate")
        assert validator.has_text("COUNT_DISTINCT") or validator.has_text("DISTINCT")

    def test_multi_key_group_by(self, hg, plan_validator):
        """Group by multiple keys.

        Pattern: group_by(col("a"), col("b")).agg(...)
        Based on: query_051 (moderators by user name and id)
        """
        frame = (
            hg.nodes("User")
            .expand("MODERATOR", to="Stream", as_="s")
            .group_by(gr.col("User.name"), gr.col("User.id"))
            .agg(num_streams=gr.count())
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Aggregate")
        # Should show multiple keys
        agg_line = validator.get_operator_line("Aggregate")
        assert agg_line is not None


class TestMultipleAggregates:
    """Tests for multiple aggregation functions."""

    def test_sum_and_count(self, hg, plan_validator):
        """Multiple aggregates: sum and count.

        Pattern: group_by().agg(total=sum_(...), count=count())
        """
        frame = hg.nodes("Order").group_by().agg(total=gr.sum_(gr.col("amount")), count=gr.count())
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Aggregate")
        assert validator.has_text("SUM")
        assert validator.has_text("COUNT")

    def test_avg_aggregation(self, hg, plan_validator):
        """Average aggregation.

        Pattern: group_by().agg(avg_val=avg(col("value")))
        Based on: query_019 (average properties)
        """
        frame = hg.nodes("Item").group_by(gr.col("category")).agg(avg_price=gr.avg(gr.col("price")))
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Aggregate")
        assert validator.has_text("AVG")

    def test_min_max_aggregation(self, hg, plan_validator):
        """Min and max aggregates together.

        Pattern: group_by().agg(min=min_(...), max=max_(...))
        """
        frame = (
            hg.nodes("Product")
            .group_by(gr.col("category"))
            .agg(min_price=gr.min_(gr.col("price")), max_price=gr.max_(gr.col("price")))
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Aggregate")
        assert validator.has_text("MIN")
        assert validator.has_text("MAX")


class TestAggregationWithExpand:
    """Tests for aggregation after expand operations."""

    def test_count_after_expand(self, hg, plan_validator):
        """Count after expand traversal.

        Pattern: expand(...).group_by().agg(count=count())
        Based on: query_037 (machine count in zone 2 racks)
        """
        frame = (
            hg.nodes("Rack")
            .filter(gr.col("zone") == 2)
            .expand("HOLDS", to="Machine", as_="m")
            .group_by()
            .agg(count=gr.count())
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Aggregate")
        assert validator.has_operator("Expand")
        assert validator.has_operator("Filter")
        assert validator.has_operator("Scan")
        # Order: Aggregate -> Expand -> Filter -> Scan
        assert validator.operator_order("Aggregate", "Expand", "Filter", "Scan")

    def test_count_distinct_after_expand(self, hg, plan_validator):
        """Count distinct after expand.

        Pattern: expand(...).group_by(key).agg(count=count_distinct(...))
        Based on: query_004, query_009 (articles with keyword counts)
        """
        frame = (
            hg.nodes("Article")
            .expand("HAS_KEY", to="Keyword", as_="m")
            .group_by(gr.col("Article.title"))
            .agg(count=gr.count_distinct(gr.col("m.id")))
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Aggregate")
        assert validator.has_operator("Expand")
        assert validator.has_text("COUNT_DISTINCT") or validator.has_text("DISTINCT")


class TestHavingClause:
    """Tests for filter after aggregation (HAVING clause equivalent)."""

    def test_filter_after_aggregation(self, hg, plan_validator):
        """Filter on aggregated values (HAVING clause).

        Pattern: group_by(...).agg(...).filter(col("count") > n)
        Based on: query_051 (moderators with count > 1)
        """
        frame = (
            hg.nodes("User")
            .expand("MODERATOR", to="Stream", as_="s")
            .group_by(gr.col("User.name"))
            .agg(num_streams=gr.count())
            .filter(gr.col("num_streams") > 1)
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        # Should have Filter after Aggregate
        assert validator.has_operator("Filter")
        assert validator.has_operator("Aggregate")
        # Filter should come before (wrap) Aggregate in the plan
        assert validator.operator_order("Filter", "Aggregate")
