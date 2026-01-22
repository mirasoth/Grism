"""Tests for Scan operator patterns.

This module tests the logical plan generation for various scan operations,
which form the leaf nodes of all query plans.

Patterns covered:
- Node scan without label (all nodes)
- Node scan with label filter
- Edge scan without type
- Edge scan with type filter
- Hyperedge scan
"""


class TestNodeScanPatterns:
    """Tests for node scan operations."""

    def test_node_scan_all(self, hg, plan_validator):
        """Scan all nodes without label filter.

        Pattern: hg.nodes()
        Expected plan: Scan(Node)
        """
        frame = hg.nodes()
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        # Validate structure
        assert validator.has_operator("Scan")
        assert validator.has_text("Node")

        # Should not have a label filter
        scan_line = validator.get_operator_line("Scan")
        assert scan_line is not None
        assert "label=" not in scan_line or "label=None" in scan_line

    def test_node_scan_with_label(self, hg, plan_validator):
        """Scan nodes with a specific label.

        Pattern: hg.nodes("Person")
        Expected plan: Scan(Node), label=Person
        """
        frame = hg.nodes("Person")
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        # Validate structure
        assert validator.has_operator("Scan")
        assert validator.has_text("Node")
        assert validator.has_text("Person")

        # Should have the label in scan
        scan_line = validator.get_operator_line("Scan")
        assert scan_line is not None
        assert "Person" in scan_line


class TestEdgeScanPatterns:
    """Tests for edge scan operations."""

    def test_edge_scan_all(self, hg, plan_validator):
        """Scan all edges without type filter.

        Pattern: hg.edges()
        Expected plan: Scan(Edge)
        """
        frame = hg.edges()
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        # Validate structure
        assert validator.has_operator("Scan")
        assert validator.has_text("Edge")

    def test_edge_scan_with_type(self, hg, plan_validator):
        """Scan edges with a specific type.

        Pattern: hg.edges("KNOWS")
        Expected plan: Scan(Edge), label=KNOWS
        """
        frame = hg.edges("KNOWS")
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        # Validate structure
        assert validator.has_operator("Scan")
        assert validator.has_text("Edge")
        assert validator.has_text("KNOWS")


class TestHyperedgeScanPatterns:
    """Tests for hyperedge scan operations."""

    def test_hyperedge_scan_all(self, hg, plan_validator):
        """Scan all hyperedges without label filter.

        Pattern: hg.hyperedges()
        Expected plan: Scan(Hyperedge)
        """
        frame = hg.hyperedges()
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        # Validate structure
        assert validator.has_operator("Scan")
        assert validator.has_text("Hyperedge")

    def test_hyperedge_scan_with_label(self, hg, plan_validator):
        """Scan hyperedges with a specific label.

        Pattern: hg.hyperedges("Collaboration")
        Expected plan: Scan(Hyperedge), label=Collaboration
        """
        frame = hg.hyperedges("Collaboration")
        plan = frame.explain("logical")
        validator = plan_validator(plan)

        # Validate structure
        assert validator.has_operator("Scan")
        assert validator.has_text("Hyperedge")
        assert validator.has_text("Collaboration")
