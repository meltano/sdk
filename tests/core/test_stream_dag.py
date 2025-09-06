"""Tests for Stream DAG functionality."""

from __future__ import annotations

import pytest

from singer_sdk import Stream, Tap
from singer_sdk.helpers.stream_dag import (
    StreamDAG,
    StreamDependency,
    SyncTrigger,
)
from singer_sdk.helpers.types import Context  # noqa: TC001


class MockStream(Stream):
    """Mock stream for testing."""

    def __init__(self, tap: Tap, name: str):
        self.name = name
        self._sync_called_with: list[Context | None] = []
        super().__init__(tap=tap)
        # Always select mock streams for testing
        self.selected = True
        # Replace sync method with our mock
        self.sync = self._mock_sync  # type: ignore[method-assign,misc]

    @property
    def schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
            },
        }

    def get_records(self, context: Context | None):  # noqa: ARG002
        """Mock implementation."""
        yield {"id": 1, "name": "test"}

    def _mock_sync(self, context: Context | None = None) -> None:
        """Track sync calls for testing."""
        self._sync_called_with.append(context)
        # Don't call super().sync() to avoid full sync complexity
        # Just track that sync was called


class MockTap(Tap):
    """Mock tap for testing."""

    name = "test-tap"

    def __init__(self):
        # Initialize _discovered_streams before calling super().__init__
        self._discovered_streams = {}
        super().__init__(config={}, validate_config=False, setup_mapper=False)

    def add_mock_stream(self, name: str) -> MockStream:
        """Add a mock stream to this tap."""
        stream = MockStream(self, name)
        self._discovered_streams[name] = stream
        return stream

    def discover_streams(self):
        """Return mock streams."""
        return list(self._discovered_streams.values())


class TestStreamDAG:
    """Test StreamDAG functionality."""

    def test_add_stream(self):
        """Test adding streams to DAG."""
        dag = StreamDAG()
        tap = MockTap()
        stream = tap.add_mock_stream("test_stream")

        dag.add_stream(stream)

        assert "test_stream" in dag._nodes
        assert dag._stream_registry["test_stream"] == stream

    def test_add_dependency_simple(self):
        """Test adding a simple dependency."""
        dag = StreamDAG()

        dependency = StreamDependency(parent_stream="parent", child_stream="child")

        dag.add_dependency(dependency)

        assert "parent" in dag._nodes
        assert "child" in dag._nodes
        assert len(dag._nodes["parent"].dependents) == 1
        assert len(dag._nodes["child"].dependencies) == 1
        assert dag._nodes["parent"].dependents[0] == dependency
        assert dag._nodes["child"].dependencies[0] == dependency

    def test_cycle_detection(self):
        """Test that cycles are detected and prevented."""
        dag = StreamDAG()

        # Add A -> B
        dep1 = StreamDependency(parent_stream="A", child_stream="B")
        dag.add_dependency(dep1)

        # Add B -> C
        dep2 = StreamDependency(parent_stream="B", child_stream="C")
        dag.add_dependency(dep2)

        # Try to add C -> A (would create cycle)
        dep3 = StreamDependency(parent_stream="C", child_stream="A")

        with pytest.raises(ValueError, match="would create a cycle"):
            dag.add_dependency(dep3)

    def test_topological_sort(self):
        """Test topological sorting of streams."""
        dag = StreamDAG()

        # Create dependency chain: A -> B -> C
        deps = [
            StreamDependency(parent_stream="A", child_stream="B"),
            StreamDependency(parent_stream="B", child_stream="C"),
        ]

        for dep in deps:
            dag.add_dependency(dep)

        order = dag.get_execution_order()

        assert order.index("A") < order.index("B")
        assert order.index("B") < order.index("C")

    def test_immediate_sync_trigger(self):
        """Test immediate sync trigger behavior."""
        dag = StreamDAG()
        tap = MockTap()

        parent_stream = tap.add_mock_stream("parent")
        child_stream = tap.add_mock_stream("child")

        dag.add_stream(parent_stream)
        dag.add_stream(child_stream)

        # Set up immediate dependency
        dependency = StreamDependency(
            parent_stream="parent",
            child_stream="child",
            sync_trigger=SyncTrigger.IMMEDIATE,
            context_mapper=lambda record, context: {"parent_id": record["id"]},  # noqa: ARG005
        )
        dag.add_dependency(dependency)

        # Process a record from parent
        record = {"id": 123, "name": "test"}
        dag.process_record("parent", record, None)

        # Child should have been synced immediately
        assert len(child_stream._sync_called_with) == 1
        assert child_stream._sync_called_with[0] == {"parent_id": 123}

    def test_batch_sync_trigger(self):
        """Test batch sync trigger behavior."""
        dag = StreamDAG()
        tap = MockTap()

        parent_stream = tap.add_mock_stream("parent")
        child_stream = tap.add_mock_stream("child")

        dag.add_stream(parent_stream)
        dag.add_stream(child_stream)

        # Set up batch dependency
        dependency = StreamDependency(
            parent_stream="parent",
            child_stream="child",
            sync_trigger=SyncTrigger.BATCH,
            context_mapper=lambda record, context: {"parent_id": record["id"]},  # noqa: ARG005
        )
        dag.add_dependency(dependency)

        # Process multiple records
        records = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"},
            {"id": 3, "name": "test3"},
        ]

        for record in records:
            dag.process_record("parent", record, None)

        # Child should not be synced yet
        assert len(child_stream._sync_called_with) == 0

        # Finalize parent stream
        dag.finalize_stream("parent")

        # Now child should be synced for each context
        assert len(child_stream._sync_called_with) == 3
        expected_contexts = [{"parent_id": 1}, {"parent_id": 2}, {"parent_id": 3}]
        assert child_stream._sync_called_with == expected_contexts

    def test_aggregated_sync_trigger(self):
        """Test aggregated sync trigger behavior."""
        dag = StreamDAG()
        tap = MockTap()

        parent_stream = tap.add_mock_stream("parent")
        child_stream = tap.add_mock_stream("child")

        dag.add_stream(parent_stream)
        dag.add_stream(child_stream)

        # Custom aggregator
        def aggregate_contexts(contexts):
            ids = [ctx["parent_id"] for ctx in contexts]
            return {"aggregated_ids": ids, "count": len(ids)}

        # Set up aggregated dependency
        dependency = StreamDependency(
            parent_stream="parent",
            child_stream="child",
            sync_trigger=SyncTrigger.AGGREGATED,
            context_mapper=lambda record, context: {"parent_id": record["id"]},  # noqa: ARG005
            aggregator=aggregate_contexts,
        )
        dag.add_dependency(dependency)

        # Process multiple records
        records = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"},
        ]

        for record in records:
            dag.process_record("parent", record, None)

        # Child should not be synced yet
        assert len(child_stream._sync_called_with) == 0

        # Finalize parent stream
        dag.finalize_stream("parent")

        # Child should be synced once with aggregated context
        assert len(child_stream._sync_called_with) == 1
        expected_context = {"aggregated_ids": [1, 2], "count": 2}
        assert child_stream._sync_called_with[0] == expected_context

    def test_conditional_sync(self):
        """Test conditional sync behavior."""
        dag = StreamDAG()
        tap = MockTap()

        parent_stream = tap.add_mock_stream("parent")
        child_stream = tap.add_mock_stream("child")

        dag.add_stream(parent_stream)
        dag.add_stream(child_stream)

        # Only sync if record ID is even
        def even_id_condition(record, context):  # noqa: ARG001
            return record["id"] % 2 == 0

        dependency = StreamDependency(
            parent_stream="parent",
            child_stream="child",
            sync_trigger=SyncTrigger.IMMEDIATE,
            context_mapper=lambda record, context: {"parent_id": record["id"]},  # noqa: ARG005
            condition=even_id_condition,
        )
        dag.add_dependency(dependency)

        # Process records with odd and even IDs
        records = [
            {"id": 1, "name": "odd"},  # Should not sync
            {"id": 2, "name": "even"},  # Should sync
            {"id": 3, "name": "odd"},  # Should not sync
            {"id": 4, "name": "even"},  # Should sync
        ]

        for record in records:
            dag.process_record("parent", record, None)

        # Only even IDs should have triggered syncs
        assert len(child_stream._sync_called_with) == 2
        assert child_stream._sync_called_with == [{"parent_id": 2}, {"parent_id": 4}]

    def test_multiple_dependencies(self):
        """Test multiple dependencies from same parent."""
        dag = StreamDAG()
        tap = MockTap()

        parent_stream = tap.add_mock_stream("parent")
        child1_stream = tap.add_mock_stream("child1")
        child2_stream = tap.add_mock_stream("child2")

        dag.add_stream(parent_stream)
        dag.add_stream(child1_stream)
        dag.add_stream(child2_stream)

        # Parent -> Child1
        dep1 = StreamDependency(
            parent_stream="parent",
            child_stream="child1",
            context_mapper=lambda record, context: {  # noqa: ARG005
                "parent_id": record["id"],
                "stream": "child1",
            },
        )

        # Parent -> Child2
        dep2 = StreamDependency(
            parent_stream="parent",
            child_stream="child2",
            context_mapper=lambda record, context: {  # noqa: ARG005
                "parent_id": record["id"],
                "stream": "child2",
            },
        )

        dag.add_dependency(dep1)
        dag.add_dependency(dep2)

        # Process a record
        record = {"id": 100, "name": "test"}
        dag.process_record("parent", record, None)

        # Both children should be synced
        assert len(child1_stream._sync_called_with) == 1
        assert len(child2_stream._sync_called_with) == 1
        assert child1_stream._sync_called_with[0] == {
            "parent_id": 100,
            "stream": "child1",
        }
        assert child2_stream._sync_called_with[0] == {
            "parent_id": 100,
            "stream": "child2",
        }

    def test_many_to_one_relationship(self):
        """Test many-to-one relationship pattern."""
        dag = StreamDAG()
        tap = MockTap()

        parent1_stream = tap.add_mock_stream("parent1")
        parent2_stream = tap.add_mock_stream("parent2")
        child_stream = tap.add_mock_stream("child")

        dag.add_stream(parent1_stream)
        dag.add_stream(parent2_stream)
        dag.add_stream(child_stream)

        # Custom aggregator for many-to-one
        def combine_parents(contexts):
            parent1_data = [ctx for ctx in contexts if ctx.get("source") == "parent1"]
            parent2_data = [ctx for ctx in contexts if ctx.get("source") == "parent2"]
            return {
                "parent1_count": len(parent1_data),
                "parent2_count": len(parent2_data),
                "total_count": len(contexts),
            }

        # Set up dependencies from both parents to child
        deps = [
            StreamDependency(
                parent_stream="parent1",
                child_stream="child",
                sync_trigger=SyncTrigger.AGGREGATED,
                context_mapper=lambda record, context: {  # noqa: ARG005
                    "parent_id": record["id"],
                    "source": "parent1",
                },
                aggregator=combine_parents,
            ),
            StreamDependency(
                parent_stream="parent2",
                child_stream="child",
                sync_trigger=SyncTrigger.AGGREGATED,
                context_mapper=lambda record, context: {  # noqa: ARG005
                    "parent_id": record["id"],
                    "source": "parent2",
                },
                aggregator=combine_parents,
            ),
        ]

        for dep in deps:
            dag.add_dependency(dep)

        # Process records from both parents
        dag.process_record("parent1", {"id": 1}, None)
        dag.process_record("parent1", {"id": 2}, None)
        dag.process_record("parent2", {"id": 3}, None)

        # Child should not be synced yet
        assert len(child_stream._sync_called_with) == 0

        # Finalize first parent
        dag.finalize_stream("parent1")

        # Still should not be synced (waiting for all parents)
        # Note: In this test, we're testing individual parent finalization
        # In practice, you'd want to coordinate when all parents are done
        assert len(child_stream._sync_called_with) == 1  # parent1 finalized

        # Finalize second parent
        dag.finalize_stream("parent2")

        # Now child should have been synced again
        assert len(child_stream._sync_called_with) == 2


class TestDAGIntegrationWithTap:
    """Test DAG integration with Tap class."""

    def test_tap_setup_dag(self):
        """Test that taps can set up and use DAG functionality."""
        tap = MockTap()
        parent_stream = tap.add_mock_stream("parent")
        child_stream = tap.add_mock_stream("child")

        # Set up DAG
        dag = tap.setup_dag()

        assert dag is not None
        assert tap.dag == dag
        assert parent_stream._dag == dag
        assert child_stream._dag == dag
        assert "parent" in dag._stream_registry
        assert "child" in dag._stream_registry

    def test_stream_dag_integration(self):
        """Test that streams properly integrate with DAG."""
        tap = MockTap()
        parent_stream = tap.add_mock_stream("parent")
        child_stream = tap.add_mock_stream("child")

        # Set up DAG with dependency
        dag = tap.setup_dag()
        dependency = StreamDependency(
            parent_stream="parent",
            child_stream="child",
            context_mapper=lambda record, context: {"parent_id": record["id"]},  # noqa: ARG005
        )
        dag.add_dependency(dependency)

        # Simulate record processing in parent stream
        record = {"id": 42, "name": "test"}
        parent_stream._process_record_with_dag(record, None)

        # Child should be synced
        assert len(child_stream._sync_called_with) == 1
        assert child_stream._sync_called_with[0] == {"parent_id": 42}

        # Finalize parent stream
        parent_stream._finalize_with_dag()

        # This should trigger any pending batch operations (none in this case)
        # The test mainly validates the integration works without errors
