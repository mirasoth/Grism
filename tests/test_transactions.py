"""Tests for transaction support."""

import pytest

import grism as gr


class TestTransaction:
    """Tests for Transaction class."""

    def test_transaction_creation(self):
        """Test transaction creation."""
        hg = gr.connect("grism://local")
        tx = hg.transaction()
        assert tx is not None
        assert tx.is_active()
        assert tx.pending_count() == 0

    def test_create_node(self):
        """Test creating a node."""
        hg = gr.connect("grism://local")
        tx = hg.transaction()
        node_id = tx.create_node("Person", name="Alice", age=30)
        assert node_id is not None
        assert tx.pending_count() == 1

    def test_create_edge(self):
        """Test creating an edge."""
        hg = gr.connect("grism://local")
        tx = hg.transaction()
        node1 = tx.create_node("Person", name="Alice")
        node2 = tx.create_node("Person", name="Bob")
        edge_id = tx.create_edge("KNOWS", node1, node2, since=2020)
        assert edge_id is not None
        assert tx.pending_count() == 3

    def test_create_hyperedge(self):
        """Test creating a hyperedge."""
        hg = gr.connect("grism://local")
        tx = hg.transaction()
        node1 = tx.create_node("Person", name="Alice")
        node2 = tx.create_node("Paper", title="Graph Databases")
        he_id = tx.create_hyperedge(
            "Authored",
            bindings={"author": node1, "paper": node2},
            year=2024,
        )
        assert he_id is not None
        assert tx.pending_count() == 3

    def test_update(self):
        """Test updating properties."""
        hg = gr.connect("grism://local")
        tx = hg.transaction()
        node_id = tx.create_node("Person", name="Alice", age=30)
        tx.update(node_id, age=31, city="NYC")
        assert tx.pending_count() == 2

    def test_delete_node(self):
        """Test deleting a node."""
        hg = gr.connect("grism://local")
        tx = hg.transaction()
        node_id = tx.create_node("Person", name="Alice")
        tx.delete_node(node_id)
        assert tx.pending_count() == 2

    def test_delete_edge(self):
        """Test deleting an edge."""
        hg = gr.connect("grism://local")
        tx = hg.transaction()
        tx.delete_edge("edge_123")
        assert tx.pending_count() == 1

    def test_delete_hyperedge(self):
        """Test deleting a hyperedge."""
        hg = gr.connect("grism://local")
        tx = hg.transaction()
        tx.delete_hyperedge("he_123")
        assert tx.pending_count() == 1

    def test_commit(self):
        """Test committing a transaction."""
        hg = gr.connect("grism://local")
        tx = hg.transaction()
        tx.create_node("Person", name="Alice")
        tx.commit()
        assert not tx.is_active()
        assert tx.pending_count() == 0

    def test_rollback(self):
        """Test rolling back a transaction."""
        hg = gr.connect("grism://local")
        tx = hg.transaction()
        tx.create_node("Person", name="Alice")
        tx.rollback()
        assert not tx.is_active()
        assert tx.pending_count() == 0

    def test_inactive_transaction_error(self):
        """Test that inactive transactions raise errors."""
        hg = gr.connect("grism://local")
        tx = hg.transaction()
        tx.commit()

        with pytest.raises(RuntimeError):
            tx.create_node("Person", name="Alice")

    def test_inactive_commit_error(self):
        """Test that committing inactive transaction raises error."""
        hg = gr.connect("grism://local")
        tx = hg.transaction()
        tx.commit()

        with pytest.raises(RuntimeError):
            tx.commit()


class TestTransactionContextManager:
    """Tests for transaction context manager protocol."""

    def test_context_manager_commit(self):
        """Test automatic commit on success."""
        hg = gr.connect("grism://local")

        with hg.transaction() as tx:
            tx.create_node("Person", name="Alice")
            assert tx.is_active()

        # Transaction should be committed and inactive
        assert not tx.is_active()

    def test_context_manager_rollback_on_exception(self):
        """Test automatic rollback on exception."""
        hg = gr.connect("grism://local")

        try:
            with hg.transaction() as tx:
                tx.create_node("Person", name="Alice")
                raise ValueError("Test error")
        except ValueError:
            pass

        # Transaction should be rolled back
        assert not tx.is_active()


class TestTransactionPatterns:
    """Tests for common transaction patterns."""

    def test_build_graph(self):
        """Test building a small graph."""
        hg = gr.connect("grism://local")

        with hg.transaction() as tx:
            # Create nodes
            alice = tx.create_node("Person", name="Alice", age=30)
            bob = tx.create_node("Person", name="Bob", age=25)
            company = tx.create_node("Company", name="TechCorp")

            # Create edges
            tx.create_edge("KNOWS", alice, bob, since=2020)
            tx.create_edge("WORKS_AT", alice, company, role="Engineer")
            tx.create_edge("WORKS_AT", bob, company, role="Designer")

            assert tx.pending_count() == 6

    def test_batch_create_nodes(self):
        """Test batch creating nodes."""
        hg = gr.connect("grism://local")

        with hg.transaction() as tx:
            for i in range(10):
                tx.create_node("Person", name=f"Person_{i}", index=i)

            assert tx.pending_count() == 10

    def test_hyperedge_collaboration(self):
        """Test creating collaboration hyperedge."""
        hg = gr.connect("grism://local")

        with hg.transaction() as tx:
            # Create participants
            alice = tx.create_node("Person", name="Alice")
            bob = tx.create_node("Person", name="Bob")
            charlie = tx.create_node("Person", name="Charlie")
            paper = tx.create_node("Paper", title="Graph Theory")

            # Create collaboration hyperedge
            tx.create_hyperedge(
                "Collaboration",
                bindings={
                    "lead_author": alice,
                    "co_author_1": bob,
                    "co_author_2": charlie,
                    "output": paper,
                },
                year=2024,
                venue="SIGMOD",
            )

            assert tx.pending_count() == 5
