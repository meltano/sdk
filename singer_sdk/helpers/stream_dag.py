"""Stream DAG (Directed Acyclic Graph) functionality for the Singer SDK.

This module provides support for complex stream relationships beyond simple parent-child
structures, enabling scenarios like many-to-one merging, conditional syncing, and
complex data flow orchestration.
"""

from __future__ import annotations

import logging
import typing as t
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record
    from singer_sdk.streams.core import Stream


class SyncTrigger(Enum):
    """Defines when a stream should be synced in relation to its dependencies."""

    IMMEDIATE = "immediate"  # Sync immediately when parent record is processed
    BATCH = "batch"  # Wait for all parent records, then sync once
    CONDITIONAL = "conditional"  # Sync based on custom logic
    AGGREGATED = "aggregated"  # Aggregate all parent contexts into single sync


class StreamRelationType(Enum):
    """Types of relationships between streams."""

    ONE_TO_ONE = "one_to_one"  # One parent record spawns one child sync
    ONE_TO_MANY = "one_to_many"  # One parent record spawns multiple child syncs
    MANY_TO_ONE = "many_to_one"  # Multiple parent records merge into single child sync
    MANY_TO_MANY = "many_to_many"  # Multiple parents spawn syncs for same child stream


@dataclass
class StreamDependency:
    """Represents a dependency relationship between streams."""

    parent_stream: str  # Parent stream name
    child_stream: str  # Child stream name
    relation_type: StreamRelationType = StreamRelationType.ONE_TO_ONE
    sync_trigger: SyncTrigger = SyncTrigger.IMMEDIATE
    context_mapper: t.Callable[[Record, Context | None], Context | None] | None = None
    condition: t.Callable[[Record, Context | None], bool] | None = None
    aggregator: t.Callable[[list[Context]], Context] | None = None
    priority: int = 0  # Higher priority streams sync first


@dataclass
class StreamNode:
    """Represents a stream node in the DAG."""

    stream_name: str
    dependencies: list[StreamDependency] = field(default_factory=list)
    dependents: list[StreamDependency] = field(default_factory=list)
    pending_contexts: dict[str, list[Context]] = field(
        default_factory=lambda: defaultdict(list)
    )
    sync_state: dict[str, t.Any] = field(default_factory=dict)


class StreamDAG:
    """Manages stream relationships as a Directed Acyclic Graph."""

    def __init__(self) -> None:  # noqa: D107
        self._nodes: dict[str, StreamNode] = {}
        self._stream_registry: dict[str, Stream] = {}
        self.logger = logging.getLogger(__name__)

    def add_stream(self, stream: Stream) -> None:
        """Add a stream to the DAG registry.

        Args:
            stream: Stream instance to add to the registry.
        """
        if stream.name not in self._nodes:
            self._nodes[stream.name] = StreamNode(stream_name=stream.name)
        self._stream_registry[stream.name] = stream

    def add_dependency(self, dependency: StreamDependency) -> None:
        """Add a dependency relationship to the DAG.

        Args:
            dependency: StreamDependency defining the relationship.

        Raises:
            ValueError: If the dependency would create a cycle.
        """
        # Ensure nodes exist
        if dependency.parent_stream not in self._nodes:
            self._nodes[dependency.parent_stream] = StreamNode(dependency.parent_stream)
        if dependency.child_stream not in self._nodes:
            self._nodes[dependency.child_stream] = StreamNode(dependency.child_stream)

        # Check for cycles before adding
        if self._would_create_cycle(dependency.parent_stream, dependency.child_stream):
            raise ValueError(  # noqa: TRY003
                f"Adding dependency from {dependency.parent_stream} to "  # noqa: EM102
                f"{dependency.child_stream} would create a cycle"
            )

        # Add the dependency
        parent_node = self._nodes[dependency.parent_stream]
        child_node = self._nodes[dependency.child_stream]

        parent_node.dependents.append(dependency)
        child_node.dependencies.append(dependency)

    def _would_create_cycle(self, parent: str, child: str) -> bool:
        """Check if adding an edge would create a cycle.

        Args:
            parent: Parent stream name.
            child: Child stream name.

        Returns:
            True if adding the edge would create a cycle.
        """
        # Use DFS to check if there's already a path from child to parent
        visited = set()

        def has_path_to_parent(current: str) -> bool:
            if current == parent:
                return True
            if current in visited:
                return False

            visited.add(current)
            node = self._nodes.get(current)
            if not node:
                return False

            for dep in node.dependents:  # noqa: SIM110
                if has_path_to_parent(dep.child_stream):
                    return True
            return False

        return has_path_to_parent(child)

    def get_execution_order(self) -> list[str]:
        """Get topological ordering of streams for execution.

        Returns:
            List of stream names in execution order.

        Raises:
            ValueError: If the graph contains cycles.
        """
        # Kahn's algorithm for topological sorting
        in_degree = dict.fromkeys(self._nodes, 0)

        # Calculate in-degrees
        for node in self._nodes.values():
            for dep in node.dependents:
                in_degree[dep.child_stream] += 1

        # Start with nodes that have no dependencies
        queue = deque([name for name, degree in in_degree.items() if degree == 0])
        result = []

        while queue:
            current = queue.popleft()
            result.append(current)

            node = self._nodes[current]
            for dep in node.dependents:
                child = dep.child_stream
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(result) != len(self._nodes):
            raise ValueError("Cycle detected in stream DAG")  # noqa: EM101, TRY003

        return result

    def process_record(
        self, stream_name: str, record: Record, context: Context | None = None
    ) -> None:
        """Process a record from a stream and trigger dependent streams.

        Args:
            stream_name: Name of the stream producing the record.
            record: The record that was processed.
            context: Stream context.
        """
        node = self._nodes.get(stream_name)
        if not node:
            return

        for dependency in node.dependents:
            self._handle_dependency(dependency, record, context)

    def _handle_dependency(
        self, dependency: StreamDependency, record: Record, context: Context | None
    ) -> None:
        """Handle a specific dependency relationship.

        Args:
            dependency: The dependency to process.
            record: The parent record.
            context: Parent stream context.
        """
        # Apply condition check if specified
        if dependency.condition and not dependency.condition(record, context):
            return

        # Create child context using mapper
        child_context = None
        if dependency.context_mapper:
            child_context = dependency.context_mapper(record, context)
        else:
            child_context = context or record

        if child_context is None:
            return

        child_node = self._nodes[dependency.child_stream]

        # Handle different sync triggers
        if dependency.sync_trigger == SyncTrigger.IMMEDIATE:
            self._sync_stream_with_context(dependency.child_stream, child_context)

        elif dependency.sync_trigger == SyncTrigger.BATCH:
            # Store context for batch processing
            parent_key = f"{dependency.parent_stream}"
            child_node.pending_contexts[parent_key].append(child_context)

        elif dependency.sync_trigger == SyncTrigger.AGGREGATED:
            # Store context for aggregation
            parent_key = f"{dependency.parent_stream}_agg"
            child_node.pending_contexts[parent_key].append(child_context)

        elif dependency.sync_trigger == SyncTrigger.CONDITIONAL:
            # Store for later processing based on custom logic
            parent_key = f"{dependency.parent_stream}_conditional"
            child_node.pending_contexts[parent_key].append(child_context)

    def _sync_stream_with_context(self, stream_name: str, context: Context) -> None:
        """Sync a stream with the given context.

        Args:
            stream_name: Name of the stream to sync.
            context: Context to pass to the stream.
        """
        stream = self._stream_registry.get(stream_name)
        if stream and stream.selected:
            try:
                stream.sync(context=context)
            except Exception as e:  # noqa: BLE001
                self.logger.error(  # noqa: TRY400
                    "Error syncing stream %s with context %s: %s",
                    stream_name,
                    context,
                    e,
                )

    def finalize_stream(self, stream_name: str) -> None:
        """Finalize a stream and process any pending batch syncs.

        Args:
            stream_name: Name of the stream that finished processing.
        """
        node = self._nodes.get(stream_name)
        if not node:
            return

        # Process dependencies that were waiting for this stream to finish
        for dependency in node.dependents:
            child_node = self._nodes[dependency.child_stream]
            parent_key = f"{stream_name}"

            if dependency.sync_trigger == SyncTrigger.BATCH:
                # Sync once for each collected context
                contexts = child_node.pending_contexts.get(parent_key, [])
                for context in contexts:
                    self._sync_stream_with_context(dependency.child_stream, context)
                child_node.pending_contexts[parent_key].clear()

            elif dependency.sync_trigger == SyncTrigger.AGGREGATED:
                # Aggregate all contexts and sync once
                agg_key = f"{stream_name}_agg"
                contexts = child_node.pending_contexts.get(agg_key, [])
                if contexts and dependency.aggregator:
                    aggregated_context = dependency.aggregator(contexts)
                    self._sync_stream_with_context(
                        dependency.child_stream, aggregated_context
                    )
                child_node.pending_contexts[agg_key].clear()

    def get_stream_dependencies(self, stream_name: str) -> list[StreamDependency]:
        """Get all dependencies for a stream.

        Args:
            stream_name: Name of the stream.

        Returns:
            List of dependencies where this stream is the child.
        """
        node = self._nodes.get(stream_name)
        return node.dependencies if node else []

    def get_stream_dependents(self, stream_name: str) -> list[StreamDependency]:
        """Get all dependents for a stream.

        Args:
            stream_name: Name of the stream.

        Returns:
            List of dependencies where this stream is the parent.
        """
        node = self._nodes.get(stream_name)
        return node.dependents if node else []


class DAGAwareStream:
    """Mixin for streams that participate in DAG relationships."""

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:  # noqa: D107
        super().__init__(*args, **kwargs)
        self._dag: StreamDAG | None = None

    def set_dag(self, dag: StreamDAG) -> None:
        """Set the DAG instance for this stream.

        Args:
            dag: The StreamDAG instance managing relationships.
        """
        self._dag = dag

    def _process_record_with_dag(
        self, record: Record, context: Context | None = None
    ) -> None:
        """Process a record and notify the DAG.

        This should be called after processing each record in the stream.

        Args:
            record: The processed record.
            context: Stream context.
        """
        if self._dag:
            self._dag.process_record(self.name, record, context)  # type: ignore[attr-defined]

    def _finalize_with_dag(self) -> None:
        """Notify the DAG that this stream has finished processing."""
        if self._dag:
            self._dag.finalize_stream(self.name)  # type: ignore[attr-defined]
