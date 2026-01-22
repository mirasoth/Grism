"""Tests for Filter operator patterns.

This module tests the logical plan generation for various filter predicates,
including comparison operators, logical operators, string functions, and null checks.

Patterns covered:
- Comparison operators (>, <, >=, <=, ==, !=)
- Logical operators (AND, OR, NOT)
- String predicates (starts_with, contains, ends_with, matches)
- Null checks (is_null, is_not_null)
- Range filters (combined conditions)
"""

import grism as gr


class TestComparisonFilters:
    """Tests for comparison operator filters."""

    def test_greater_than(self, hg, plan_validator):
        """Filter with greater than comparison.

        Pattern: filter(col("rating") > 7)
        Expected: Filter containing "rating > ..."
        """
        frame = hg.nodes("Movie").filter(gr.col("rating") > 7)
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        assert validator.has_operator("Scan")
        assert validator.operator_order("Filter", "Scan")
        assert validator.has_text("rating")
        assert validator.has_text(">")

    def test_less_than(self, hg, plan_validator):
        """Filter with less than comparison.

        Pattern: filter(col("age") < 18)
        """
        frame = hg.nodes("Person").filter(gr.col("age") < 18)
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        assert validator.has_text("age")
        assert validator.has_text("<")

    def test_greater_equal(self, hg, plan_validator):
        """Filter with greater than or equal comparison.

        Pattern: filter(col("year") >= 2000)
        """
        frame = hg.nodes("Movie").filter(gr.col("year") >= 2000)
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        assert validator.has_text("year")
        assert validator.has_text(">=")

    def test_less_equal(self, hg, plan_validator):
        """Filter with less than or equal comparison.

        Pattern: filter(col("score") <= 100)
        """
        frame = hg.nodes("Test").filter(gr.col("score") <= 100)
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        assert validator.has_text("score")
        assert validator.has_text("<=")

    def test_equality(self, hg, plan_validator):
        """Filter with equality comparison.

        Pattern: filter(col("status") == "active")
        """
        frame = hg.nodes("User").filter(gr.col("status") == "active")
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        assert validator.has_text("status")
        assert validator.has_text("==") or validator.has_text("=")
        assert validator.has_text("active")

    def test_inequality(self, hg, plan_validator):
        """Filter with inequality comparison.

        Pattern: filter(col("type") != "deleted")
        """
        frame = hg.nodes("Item").filter(gr.col("type") != "deleted")
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        assert validator.has_text("type")
        assert validator.has_text("!=") or validator.has_text("<>")


class TestLogicalOperatorFilters:
    """Tests for logical operator filters."""

    def test_and_filter(self, hg, plan_validator):
        """Filter with AND logical operator.

        Pattern: filter((col("age") > 18) & (col("age") < 65))
        Expected: Filter containing AND
        """
        frame = hg.nodes("Person").filter((gr.col("age") > 18) & (gr.col("age") < 65))
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        assert validator.has_text("age")
        assert validator.has_text("AND")

    def test_or_filter(self, hg, plan_validator):
        """Filter with OR logical operator.

        Pattern: filter((col("status") == "active") | (col("status") == "pending"))
        Expected: Filter containing OR
        """
        frame = hg.nodes("Task").filter(
            (gr.col("status") == "active") | (gr.col("status") == "pending")
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        assert validator.has_text("status")
        assert validator.has_text("OR")

    def test_not_filter(self, hg, plan_validator):
        """Filter with NOT logical operator.

        Pattern: filter(~(col("deleted") == True))
        Expected: Filter containing NOT
        """
        frame = hg.nodes("Record").filter(~(gr.col("deleted") == True))  # noqa: E712
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        assert validator.has_text("NOT")


class TestStringFilters:
    """Tests for string predicate filters."""

    def test_starts_with(self, hg, plan_validator):
        """Filter with STARTS_WITH predicate.

        Pattern: filter(col("name").starts_with("Dr."))
        Expected: Filter with STARTS_WITH
        """
        frame = hg.nodes("Author").filter(gr.col("name").starts_with("Dr."))
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        assert validator.has_text("STARTS_WITH")
        assert validator.has_text("name")

    def test_contains(self, hg, plan_validator):
        """Filter with CONTAINS predicate.

        Pattern: filter(col("description").contains("important"))
        Expected: Filter with CONTAINS
        """
        frame = hg.nodes("Document").filter(gr.col("description").contains("important"))
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        assert validator.has_text("CONTAINS")

    def test_ends_with(self, hg, plan_validator):
        """Filter with ENDS_WITH predicate.

        Pattern: filter(col("email").ends_with("@example.com"))
        Expected: Filter with ENDS_WITH
        """
        frame = hg.nodes("User").filter(gr.col("email").ends_with("@example.com"))
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        assert validator.has_text("ENDS_WITH")

    def test_regex_match(self, hg, plan_validator):
        """Filter with regex MATCHES predicate.

        Pattern: filter(col("code").matches("^[A-Z]{3}$"))
        Expected: Filter with MATCHES or REGEX
        """
        frame = hg.nodes("Product").filter(gr.col("code").matches("^[A-Z]{3}$"))
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        # Could be MATCHES, REGEX, or similar
        assert validator.has_text("MATCH") or validator.has_text("REGEX")


class TestNullFilters:
    """Tests for null check filters."""

    def test_is_null(self, hg, plan_validator):
        """Filter for null values.

        Pattern: filter(col("optional_field").is_null())
        Expected: Filter with IS_NULL or IS NULL
        """
        frame = hg.nodes("Record").filter(gr.col("optional_field").is_null())
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        assert validator.has_text("NULL")

    def test_is_not_null(self, hg, plan_validator):
        """Filter for non-null values.

        Pattern: filter(col("required_field").is_not_null())
        Expected: Filter with IS_NOT_NULL or IS NOT NULL
        """
        frame = hg.nodes("Record").filter(gr.col("required_field").is_not_null())
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        assert validator.has_text("NOT") and validator.has_text("NULL")


class TestRangeFilters:
    """Tests for range and combined filters."""

    def test_range_filter(self, hg, plan_validator):
        """Filter with range condition (between).

        Pattern: filter((col("value") >= 10) & (col("value") <= 100))
        Based on: query_008 (movies from 1990-2000)
        """
        frame = hg.nodes("Movie").filter(
            (gr.col("released") >= 1990) & (gr.col("released") <= 2000) & (gr.col("votes") > 5000)
        )
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        assert validator.has_operator("Filter")
        assert validator.has_text("released")
        assert validator.has_text("votes")
        assert validator.has_text("AND")
        assert validator.operator_order("Filter", "Scan")
