"""Tests for expression system."""

import grism as gr


class TestColumnExpressions:
    """Tests for column reference expressions."""

    def test_col(self):
        """Test column reference."""
        expr = gr.col("name")
        assert expr is not None

    def test_prop(self):
        """Test property reference (alias for col)."""
        expr = gr.prop("name")
        assert expr is not None

    def test_qualified_column(self):
        """Test qualified column reference."""
        expr = gr.col("Person.name")
        assert expr is not None


class TestLiteralExpressions:
    """Tests for literal expressions."""

    def test_lit_int(self):
        """Test integer literal."""
        expr = gr.lit(42)
        assert expr is not None

    def test_lit_float(self):
        """Test float literal."""
        expr = gr.lit(3.14)
        assert expr is not None

    def test_lit_string(self):
        """Test string literal."""
        expr = gr.lit("hello")
        assert expr is not None

    def test_lit_bool(self):
        """Test boolean literal."""
        expr = gr.lit(True)
        assert expr is not None

    def test_lit_none(self):
        """Test None literal."""
        expr = gr.lit(None)
        assert expr is not None


class TestComparisonOperators:
    """Tests for comparison operators."""

    def test_eq(self):
        """Test equality."""
        expr = gr.col("age") == 30
        assert expr is not None

    def test_ne(self):
        """Test inequality."""
        expr = gr.col("age") != 30
        assert expr is not None

    def test_gt(self):
        """Test greater than."""
        expr = gr.col("age") > 18
        assert expr is not None

    def test_ge(self):
        """Test greater than or equal."""
        expr = gr.col("age") >= 18
        assert expr is not None

    def test_lt(self):
        """Test less than."""
        expr = gr.col("age") < 65
        assert expr is not None

    def test_le(self):
        """Test less than or equal."""
        expr = gr.col("age") <= 65
        assert expr is not None


class TestLogicalOperators:
    """Tests for logical operators."""

    def test_and(self):
        """Test logical AND."""
        expr = (gr.col("age") > 18) & (gr.col("age") < 65)
        assert expr is not None

    def test_or(self):
        """Test logical OR."""
        expr = (gr.col("status") == "active") | (gr.col("status") == "pending")
        assert expr is not None

    def test_not(self):
        """Test logical NOT."""
        expr = ~(gr.col("active") == True)  # noqa: E712
        assert expr is not None


class TestArithmeticOperators:
    """Tests for arithmetic operators."""

    def test_add(self):
        """Test addition."""
        expr = gr.col("price") + 10
        assert expr is not None

    def test_sub(self):
        """Test subtraction."""
        expr = gr.col("price") - 10
        assert expr is not None

    def test_mul(self):
        """Test multiplication."""
        expr = gr.col("quantity") * gr.col("price")
        assert expr is not None

    def test_div(self):
        """Test division."""
        expr = gr.col("total") / gr.col("count")
        assert expr is not None

    def test_mod(self):
        """Test modulo."""
        expr = gr.col("value") % 10
        assert expr is not None

    def test_neg(self):
        """Test negation."""
        expr = -gr.col("value")
        assert expr is not None


class TestNullHandling:
    """Tests for null handling."""

    def test_is_null(self):
        """Test is_null."""
        expr = gr.col("email").is_null()
        assert expr is not None

    def test_is_not_null(self):
        """Test is_not_null."""
        expr = gr.col("email").is_not_null()
        assert expr is not None

    def test_fill_null(self):
        """Test fill_null."""
        expr = gr.col("value").fill_null(0)
        assert expr is not None

    def test_coalesce_method(self):
        """Test coalesce method."""
        expr = gr.col("a").coalesce(gr.col("b"), gr.lit(0))
        assert expr is not None

    def test_coalesce_function(self):
        """Test coalesce function."""
        expr = gr.coalesce(gr.col("a"), gr.col("b"), gr.lit(0))
        assert expr is not None


class TestStringOperations:
    """Tests for string operations."""

    def test_contains(self):
        """Test contains."""
        expr = gr.col("name").contains("alice")
        assert expr is not None

    def test_starts_with(self):
        """Test starts_with."""
        expr = gr.col("name").starts_with("Dr.")
        assert expr is not None

    def test_ends_with(self):
        """Test ends_with."""
        expr = gr.col("email").ends_with("@example.com")
        assert expr is not None

    def test_matches(self):
        """Test regex match."""
        expr = gr.col("phone").matches(r"\d{3}-\d{4}")
        assert expr is not None

    def test_like(self):
        """Test SQL-style LIKE."""
        expr = gr.col("name").like("Alice%")
        assert expr is not None

    def test_lower(self):
        """Test lower."""
        expr = gr.col("name").lower()
        assert expr is not None

    def test_upper(self):
        """Test upper."""
        expr = gr.col("name").upper()
        assert expr is not None

    def test_trim(self):
        """Test trim."""
        expr = gr.col("name").trim()
        assert expr is not None

    def test_replace(self):
        """Test replace."""
        expr = gr.col("text").replace("old", "new")
        assert expr is not None

    def test_substring(self):
        """Test substring."""
        expr = gr.col("name").substring(0, 5)
        assert expr is not None

    def test_split(self):
        """Test split."""
        expr = gr.col("tags").split(",")
        assert expr is not None

    def test_concat_method(self):
        """Test concat method."""
        expr = gr.col("first").concat(gr.lit(" "), gr.col("last"))
        assert expr is not None

    def test_concat_function(self):
        """Test concat function."""
        expr = gr.concat(gr.col("first"), gr.lit(" "), gr.col("last"))
        assert expr is not None

    def test_length_method(self):
        """Test length method."""
        expr = gr.col("name").length()
        assert expr is not None

    def test_length_function(self):
        """Test length function."""
        expr = gr.length(gr.col("name"))
        assert expr is not None


class TestMathFunctions:
    """Tests for math functions."""

    def test_abs(self):
        """Test abs function."""
        expr = gr.abs(gr.col("value"))
        assert expr is not None

    def test_ceil(self):
        """Test ceil function."""
        expr = gr.ceil(gr.col("value"))
        assert expr is not None

    def test_floor(self):
        """Test floor function."""
        expr = gr.floor(gr.col("value"))
        assert expr is not None

    def test_round(self):
        """Test round function."""
        expr = gr.round(gr.col("value"))
        assert expr is not None

    def test_sqrt(self):
        """Test sqrt function."""
        expr = gr.sqrt(gr.col("value"))
        assert expr is not None

    def test_power(self):
        """Test power function."""
        expr = gr.power(gr.col("value"), 2)
        assert expr is not None


class TestDateTimeFunctions:
    """Tests for date/time functions."""

    def test_year(self):
        """Test year extraction."""
        expr = gr.year(gr.col("created_at"))
        assert expr is not None

    def test_month(self):
        """Test month extraction."""
        expr = gr.month(gr.col("created_at"))
        assert expr is not None

    def test_day(self):
        """Test day extraction."""
        expr = gr.day(gr.col("created_at"))
        assert expr is not None


class TestConditionalFunctions:
    """Tests for conditional functions."""

    def test_if(self):
        """Test if_ function."""
        expr = gr.if_(gr.col("age") >= 18, gr.lit("adult"), gr.lit("minor"))
        assert expr is not None


class TestGraphFunctions:
    """Tests for graph-specific functions."""

    def test_labels(self):
        """Test labels function."""
        expr = gr.labels(gr.col("n"))
        assert expr is not None

    def test_type(self):
        """Test type function."""
        expr = gr.type(gr.col("r"))
        assert expr is not None

    def test_id(self):
        """Test id function."""
        expr = gr.id(gr.col("n"))
        assert expr is not None

    def test_properties(self):
        """Test properties function."""
        expr = gr.properties(gr.col("n"))
        assert expr is not None


class TestAliasing:
    """Tests for expression aliasing."""

    def test_alias(self):
        """Test alias method."""
        expr = gr.col("name").alias("person_name")
        assert expr is not None

    def test_as_(self):
        """Test as_ method."""
        expr = (gr.col("age") + 1).as_("next_age")
        assert expr is not None


class TestSorting:
    """Tests for sort expressions."""

    def test_asc(self):
        """Test ascending sort."""
        expr = gr.col("name").asc()
        assert expr is not None

    def test_desc(self):
        """Test descending sort."""
        expr = gr.col("age").desc()
        assert expr is not None


class TestAggregationFunctions:
    """Tests for aggregation functions."""

    def test_count(self):
        """Test count aggregation."""
        agg = gr.count()
        assert agg is not None

    def test_count_column(self):
        """Test count with column."""
        agg = gr.count(gr.col("id"))
        assert agg is not None

    def test_count_distinct(self):
        """Test count distinct."""
        agg = gr.count_distinct(gr.col("category"))
        assert agg is not None

    def test_sum(self):
        """Test sum aggregation."""
        agg = gr.sum(gr.col("amount"))
        assert agg is not None

    def test_avg(self):
        """Test avg aggregation."""
        agg = gr.avg(gr.col("score"))
        assert agg is not None

    def test_min(self):
        """Test min aggregation."""
        agg = gr.min(gr.col("price"))
        assert agg is not None

    def test_max(self):
        """Test max aggregation."""
        agg = gr.max(gr.col("price"))
        assert agg is not None

    def test_collect_agg(self):
        """Test collect aggregation."""
        agg = gr.collect(gr.col("tag"))
        assert agg is not None

    def test_collect_distinct(self):
        """Test collect_distinct aggregation."""
        agg = gr.collect_distinct(gr.col("category"))
        assert agg is not None

    def test_first(self):
        """Test first aggregation."""
        agg = gr.first(gr.col("name"))
        assert agg is not None

    def test_last(self):
        """Test last aggregation."""
        agg = gr.last(gr.col("name"))
        assert agg is not None

    def test_agg_alias(self):
        """Test aggregation aliasing."""
        agg = gr.count().alias("total")
        assert agg is not None

    def test_agg_as_(self):
        """Test aggregation as_ method."""
        agg = gr.sum(gr.col("amount")).as_("total_amount")
        assert agg is not None


class TestVectorFunctions:
    """Tests for vector/AI functions."""

    def test_sim(self):
        """Test similarity function."""
        expr = gr.sim(gr.col("embedding"), [1.0, 0.0, 0.0])
        assert expr is not None
