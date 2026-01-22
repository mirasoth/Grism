"""Shared pytest fixtures for Grism tests."""

import pytest

import grism as gr


@pytest.fixture
def hg():
    """Provide a connected Hypergraph instance."""
    return gr.connect("grism://local")


class PlanValidator:
    """Helper class to validate logical plan structure.

    Provides methods to check operator presence, order, and parameters
    in a logical plan string.
    """

    def __init__(self, plan: str):
        """Initialize with a logical plan string.

        Args:
            plan: The logical plan string from frame.explain()
        """
        self.plan = plan
        self.lines = [line.strip() for line in plan.split("\n") if line.strip()]

    def has_operator(self, op: str) -> bool:
        """Check if an operator is present in the plan.

        Args:
            op: Operator name to search for (e.g., "Scan", "Filter")

        Returns:
            True if the operator is found in any line
        """
        return any(op in line for line in self.lines)

    def has_text(self, text: str) -> bool:
        """Check if specific text is present in the plan.

        Args:
            text: Text to search for

        Returns:
            True if the text is found
        """
        return text in self.plan

    def operator_order(self, *ops: str) -> bool:
        """Verify operators appear in order (outer to inner / top to bottom).

        In a logical plan, outer operators appear first (top of the tree),
        and inner operators (like Scan) appear later (bottom).

        Args:
            ops: Operator names in expected order (outer to inner)

        Returns:
            True if all operators are found in the expected order
        """
        positions = []
        for op in ops:
            for i, line in enumerate(self.lines):
                if op in line:
                    positions.append(i)
                    break
            else:
                return False  # Operator not found
        return positions == sorted(positions)

    def get_operator_line(self, op: str) -> str | None:
        """Get the full line containing an operator.

        Args:
            op: Operator name to search for

        Returns:
            The full line containing the operator, or None if not found
        """
        for line in self.lines:
            if op in line:
                return line
        return None

    def count_operators(self, op: str) -> int:
        """Count occurrences of an operator in the plan.

        Args:
            op: Operator name to count

        Returns:
            Number of lines containing the operator
        """
        return sum(1 for line in self.lines if op in line)


@pytest.fixture
def plan_validator():
    """Provide a PlanValidator factory.

    Usage:
        def test_something(hg, plan_validator):
            plan = hg.nodes("Person").explain()
            validator = plan_validator(plan)
            assert validator.has_operator("Scan")
    """
    return PlanValidator
