"""Tests for Hypergraph and Frame types."""

import pytest
import grism as gr


class TestHypergraph:
    """Tests for Hypergraph class."""

    def test_connect(self):
        """Test basic connection."""
        hg = gr.connect("grism://local")
        assert hg is not None

    def test_connect_with_namespace(self):
        """Test connection with namespace."""
        hg = gr.connect("grism://local", namespace="test_ns")
        assert hg is not None

    def test_connect_static_method(self):
        """Test Hypergraph.connect() static method."""
        hg = gr.Hypergraph.connect("grism://local")
        assert hg is not None

    def test_create(self):
        """Test creating a new hypergraph."""
        hg = gr.create("grism://local")
        assert hg is not None

    def test_with_namespace(self):
        """Test namespace scoping."""
        hg = gr.connect("grism://local")
        hg_ns = hg.with_namespace("my_namespace")
        assert hg_ns is not None

    def test_with_config(self):
        """Test execution configuration."""
        hg = gr.connect("grism://local")
        hg_cfg = hg.with_config(parallelism=4, memory_limit=1024 * 1024, batch_size=2048)
        assert hg_cfg is not None


class TestNodeFrame:
    """Tests for NodeFrame class."""

    def test_nodes_all(self):
        """Test getting all nodes."""
        hg = gr.connect("grism://local")
        nf = hg.nodes()
        assert nf is not None

    def test_nodes_with_label(self):
        """Test getting nodes with label filter."""
        hg = gr.connect("grism://local")
        nf = hg.nodes("Person")
        assert nf is not None

    def test_filter(self):
        """Test filter operation."""
        hg = gr.connect("grism://local")
        nf = hg.nodes("Person").filter(gr.col("age") > 18)
        assert nf is not None

    def test_where(self):
        """Test where alias."""
        hg = gr.connect("grism://local")
        nf = hg.nodes("Person").where(gr.col("name") == "Alice")
        assert nf is not None

    def test_limit(self):
        """Test limit operation."""
        hg = gr.connect("grism://local")
        nf = hg.nodes("Person").limit(10)
        assert nf is not None

    def test_head(self):
        """Test head operation."""
        hg = gr.connect("grism://local")
        nf = hg.nodes("Person").head(5)
        assert nf is not None

    def test_offset(self):
        """Test offset operation."""
        hg = gr.connect("grism://local")
        nf = hg.nodes("Person").offset(10)
        assert nf is not None

    def test_skip(self):
        """Test skip alias."""
        hg = gr.connect("grism://local")
        nf = hg.nodes("Person").skip(5)
        assert nf is not None

    def test_order_by(self):
        """Test order_by operation."""
        hg = gr.connect("grism://local")
        nf = hg.nodes("Person").order_by(gr.col("age"))
        assert nf is not None

    def test_sort(self):
        """Test sort alias."""
        hg = gr.connect("grism://local")
        nf = hg.nodes("Person").sort(gr.col("name"))
        assert nf is not None

    def test_expand(self):
        """Test expand operation."""
        hg = gr.connect("grism://local")
        nf = hg.nodes("Person").expand("KNOWS", direction="out")
        assert nf is not None

    def test_expand_with_hops(self):
        """Test expand with hop range."""
        hg = gr.connect("grism://local")
        nf = hg.nodes("Person").expand("KNOWS", hops=(1, 3))
        assert nf is not None

    def test_group_by(self):
        """Test group_by operation."""
        hg = gr.connect("grism://local")
        gf = hg.nodes("Person").group_by("city")
        assert gf is not None

    def test_groupby(self):
        """Test groupby alias."""
        hg = gr.connect("grism://local")
        gf = hg.nodes("Person").groupby(gr.col("city"))
        assert gf is not None

    def test_explain(self):
        """Test explain operation."""
        hg = gr.connect("grism://local")
        nf = hg.nodes("Person").filter(gr.col("age") > 18)
        explain = nf.explain()
        assert isinstance(explain, str)
        assert "Scan" in explain or "Filter" in explain

    def test_schema(self):
        """Test schema introspection."""
        hg = gr.connect("grism://local")
        nf = hg.nodes("Person")
        schema = nf.schema()
        assert schema is not None
        assert schema.entity_type() == "node"

    def test_collect(self):
        """Test collect operation."""
        hg = gr.connect("grism://local")
        nf = hg.nodes("Person")
        result = nf.collect()
        assert isinstance(result, list)

    def test_count(self):
        """Test count operation."""
        hg = gr.connect("grism://local")
        nf = hg.nodes("Person")
        count = nf.count()
        assert isinstance(count, int)
        assert count >= 0

    def test_exists(self):
        """Test exists operation."""
        hg = gr.connect("grism://local")
        nf = hg.nodes("Person")
        exists = nf.exists()
        assert isinstance(exists, bool)


class TestEdgeFrame:
    """Tests for EdgeFrame class."""

    def test_edges_all(self):
        """Test getting all edges."""
        hg = gr.connect("grism://local")
        ef = hg.edges()
        assert ef is not None

    def test_edges_with_label(self):
        """Test getting edges with label filter."""
        hg = gr.connect("grism://local")
        ef = hg.edges("KNOWS")
        assert ef is not None

    def test_filter(self):
        """Test filter operation."""
        hg = gr.connect("grism://local")
        ef = hg.edges("KNOWS").filter(gr.col("since") > 2020)
        assert ef is not None

    def test_source(self):
        """Test source operation."""
        hg = gr.connect("grism://local")
        nf = hg.edges("KNOWS").source()
        assert nf is not None

    def test_target(self):
        """Test target operation."""
        hg = gr.connect("grism://local")
        nf = hg.edges("KNOWS").target()
        assert nf is not None

    def test_endpoints(self):
        """Test endpoints operation."""
        hg = gr.connect("grism://local")
        nf = hg.edges("KNOWS").endpoints("source")
        assert nf is not None

    def test_limit(self):
        """Test limit operation."""
        hg = gr.connect("grism://local")
        ef = hg.edges("KNOWS").limit(10)
        assert ef is not None

    def test_explain(self):
        """Test explain operation."""
        hg = gr.connect("grism://local")
        ef = hg.edges("KNOWS")
        explain = ef.explain()
        assert isinstance(explain, str)


class TestHyperedgeFrame:
    """Tests for HyperedgeFrame class."""

    def test_hyperedges_all(self):
        """Test getting all hyperedges."""
        hg = gr.connect("grism://local")
        hef = hg.hyperedges()
        assert hef is not None

    def test_hyperedges_with_label(self):
        """Test getting hyperedges with label filter."""
        hg = gr.connect("grism://local")
        hef = hg.hyperedges("Collaboration")
        assert hef is not None

    def test_filter(self):
        """Test filter operation."""
        hg = gr.connect("grism://local")
        hef = hg.hyperedges("Collaboration").filter(gr.col("year") == 2024)
        assert hef is not None

    def test_expand(self):
        """Test expand with role."""
        hg = gr.connect("grism://local")
        nf = hg.hyperedges("Collaboration").expand("author")
        assert nf is not None

    def test_limit(self):
        """Test limit operation."""
        hg = gr.connect("grism://local")
        hef = hg.hyperedges().limit(10)
        assert hef is not None


class TestGroupedFrame:
    """Tests for GroupedFrame class."""

    def test_agg_count(self):
        """Test count aggregation."""
        hg = gr.connect("grism://local")
        result = hg.nodes("Person").group_by("city").count()
        assert result is not None

    def test_agg_multiple(self):
        """Test multiple aggregations."""
        hg = gr.connect("grism://local")
        result = (
            hg.nodes("Person")
            .group_by("city")
            .agg(gr.count().alias("count"), gr.avg(gr.col("age")).alias("avg_age"))
        )
        assert result is not None


class TestSchemaIntrospection:
    """Tests for schema introspection."""

    def test_node_labels(self):
        """Test getting node labels."""
        hg = gr.connect("grism://local")
        labels = hg.node_labels()
        assert isinstance(labels, list)

    def test_edge_types(self):
        """Test getting edge types."""
        hg = gr.connect("grism://local")
        types = hg.edge_types()
        assert isinstance(types, list)

    def test_hyperedge_labels(self):
        """Test getting hyperedge labels."""
        hg = gr.connect("grism://local")
        labels = hg.hyperedge_labels()
        assert isinstance(labels, list)

    def test_node_count(self):
        """Test node count."""
        hg = gr.connect("grism://local")
        count = hg.node_count()
        assert isinstance(count, int)
        assert count >= 0

    def test_edge_count(self):
        """Test edge count."""
        hg = gr.connect("grism://local")
        count = hg.edge_count()
        assert isinstance(count, int)
        assert count >= 0

    def test_hyperedge_count(self):
        """Test hyperedge count."""
        hg = gr.connect("grism://local")
        count = hg.hyperedge_count()
        assert isinstance(count, int)
        assert count >= 0
