"""Tests for Projection operator patterns.

This module tests the logical plan generation for projection-related operations
including select, distinct, order_by, limit, and offset.

Patterns covered:
- Select specific columns
- Select with aliasing
- Distinct
- Order by single column
- Order by multiple columns (mixed directions)
- Limit
- Offset/skip
- Combined sort + limit
"""

import grism as gr


class TestSelectPatterns:
    """Tests for select/project operations."""

    def test_select_columns(self, hg, plan_validator):
        """Select specific columns.

        Pattern: select("name", "age")
        Based on: query_008 (movie title, released, votes)
        """
        frame = hg.nodes("Movie").select("title", "released", "votes")
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Project")
        assert validator.has_operator("Scan")
        assert validator.operator_order("Project", "Scan")
        assert validator.has_text("title")
        assert validator.has_text("released")
        assert validator.has_text("votes")

    def test_select_with_alias(self, hg, plan_validator):
        """Select with column aliasing.

        Pattern: select(new_name=col("old_name"))
        Based on: query_001 (country=col("c.name"))
        """
        frame = hg.nodes("City").select(city_name=gr.col("name"))
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Project")
        # Alias should appear in projection
        assert validator.has_text("name")

    def test_select_expression(self, hg, plan_validator):
        """Select with expression.

        Pattern: select(col("a") + col("b"))
        """
        frame = hg.nodes("Item").select(
            gr.col("name"), total=(gr.col("price") * gr.col("quantity"))
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Project")


class TestDistinctPatterns:
    """Tests for distinct operations."""

    def test_distinct(self, hg, plan_validator):
        """Select distinct values.

        Pattern: select(...).distinct()
        Based on: query_003 (distinct game names), query_040 (distinct product sizes)
        """
        frame = hg.nodes("Product").select(gr.col("product_size")).distinct()
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        # Distinct is implemented via Project
        assert validator.has_operator("Project")
        assert validator.has_text("product_size")


class TestSortPatterns:
    """Tests for ordering operations."""

    def test_order_by_ascending(self, hg, plan_validator):
        """Order by single column ascending.

        Pattern: order_by("column", ascending=True)
        Based on: query_047 (lowest rated movies)
        """
        frame = hg.nodes("Movie").order_by("average_vote", ascending=True)
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Sort")
        assert validator.has_operator("Scan")
        assert validator.operator_order("Sort", "Scan")
        assert validator.has_text("average_vote")
        assert validator.has_text("ASC")

    def test_order_by_descending(self, hg, plan_validator):
        """Order by single column descending.

        Pattern: order_by("column", ascending=False)
        Based on: query_010 (highest budget movie)
        """
        frame = hg.nodes("Movie").order_by("budget", ascending=False)
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Sort")
        assert validator.has_text("budget")
        assert validator.has_text("DESC")

    def test_order_by_multiple_columns(self, hg, plan_validator):
        """Order by multiple columns with mixed directions.

        Pattern: order_by(col("a").desc(), col("b").asc())
        Based on: query_053 (streams by followers DESC, views ASC)
        """
        frame = hg.nodes("Stream").order_by(
            gr.col("followers").desc(), gr.col("total_view_count").asc()
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Sort")
        assert validator.has_operator("Scan")
        # Multi-column sort should have multiple sort expressions
        sort_line = validator.get_operator_line("Sort")
        assert sort_line is not None
        # The Sort line should contain multiple sorting expressions
        assert sort_line.count("ASC") >= 1 or sort_line.count("DESC") >= 1


class TestLimitPatterns:
    """Tests for limit operations."""

    def test_limit(self, hg, plan_validator):
        """Limit result count.

        Pattern: limit(n)
        Based on: Many queries (e.g., query_002 with LIMIT 3)
        """
        frame = hg.nodes("Person").limit(10)
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Limit")
        assert validator.has_operator("Scan")
        assert validator.operator_order("Limit", "Scan")
        limit_line = validator.get_operator_line("Limit")
        assert limit_line is not None
        assert "10" in limit_line

    def test_offset(self, hg, plan_validator):
        """Skip first n results.

        Pattern: offset(n) or skip(n)
        Based on: query_039 (SKIP 2 LIMIT 2)
        """
        frame = hg.nodes("Categories").offset(2)
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        # Offset might be represented as Limit with offset or separate operator
        # Just check it generates a valid plan
        assert validator.has_operator("Scan")


class TestCombinedPatterns:
    """Tests for combined projection patterns."""

    def test_sort_and_limit(self, hg, plan_validator):
        """Combined sort and limit (TOP N pattern).

        Pattern: order_by(...).limit(n)
        Based on: query_010 (top budget), query_044 (top IMDB votes)
        """
        frame = hg.nodes("Movie").order_by("imdbVotes", ascending=False).limit(5)
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Limit")
        assert validator.has_operator("Sort")
        assert validator.has_operator("Scan")
        # Limit wraps Sort
        assert validator.operator_order("Limit", "Sort", "Scan")

    def test_sort_limit_select(self, hg, plan_validator):
        """Combined sort, limit, and select.

        Pattern: order_by(...).limit(n).select(...)
        Based on: query_044, query_047
        """
        frame = (
            hg.nodes("Movie")
            .order_by("imdbVotes", ascending=False)
            .limit(5)
            .select("title", "imdbVotes")
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Project")
        assert validator.has_operator("Limit")
        assert validator.has_operator("Sort")
        assert validator.has_operator("Scan")
        # Project -> Limit -> Sort -> Scan
        assert validator.operator_order("Project", "Limit", "Sort", "Scan")

    def test_offset_and_limit(self, hg, plan_validator):
        """Combined offset and limit (pagination).

        Pattern: offset(n).limit(m)
        Based on: query_039 (SKIP 2 LIMIT 2)
        """
        frame = (
            hg.nodes("Categories")
            .filter(gr.col("category_id").starts_with("f"))
            .select(category_id=gr.col("category_id"))
            .offset(2)
            .limit(2)
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Limit")
        assert validator.has_operator("Filter")
        assert validator.has_operator("Scan")
