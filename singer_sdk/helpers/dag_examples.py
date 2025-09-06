"""Example implementations of common DAG patterns for stream relationships.

This module provides concrete examples of how to implement the use cases
mentioned in the GitHub discussion for stream DAG functionality.
"""

from __future__ import annotations

import typing as t

from singer_sdk.helpers.stream_dag import (
    StreamDAG,
    StreamDependency,
    StreamRelationType,
    SyncTrigger,
)

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record


def create_one_to_one_dependency(
    parent_stream: str,
    child_stream: str,
    context_mapper: t.Callable[[Record, Context | None], Context | None] | None = None,
) -> StreamDependency:
    """Create a simple one-to-one parent-child relationship.

    Use case: One parent record spawns one child sync
    Example: User -> UserProfile (each user has one profile)

    Args:
        parent_stream: Name of the parent stream.
        child_stream: Name of the child stream.
        context_mapper: Optional function to transform parent record into child context.

    Returns:
        StreamDependency configured for one-to-one relationship.
    """

    def default_mapper(record: Record, context: Context | None) -> Context | None:
        return {"parent_id": record.get("id"), **(context or {})}

    return StreamDependency(
        parent_stream=parent_stream,
        child_stream=child_stream,
        relation_type=StreamRelationType.ONE_TO_ONE,
        sync_trigger=SyncTrigger.IMMEDIATE,
        context_mapper=context_mapper or default_mapper,
    )


def create_one_to_many_dependency(
    parent_stream: str,
    child_stream: str,
    child_id_extractor: t.Callable[[Record], list[t.Any]],
    context_mapper: t.Callable[[Record, t.Any, Context | None], Context | None]
    | None = None,
) -> StreamDependency:
    """Create a one-to-many parent-child relationship.

    Use case: One parent record spawns multiple child syncs
    Example: Project -> Tasks (each project has many tasks)

    Args:
        parent_stream: Name of the parent stream.
        child_stream: Name of the child stream.
        child_id_extractor: Function to extract list of child IDs from parent record.
        context_mapper: Function to create child context from parent and child ID.

    Returns:
        StreamDependency configured for one-to-many relationship.
    """

    def default_mapper(record: Record, context: Context | None) -> Context | None:
        # This will be called multiple times via DAG's handling
        child_ids = child_id_extractor(record)
        contexts = []
        for child_id in child_ids:
            child_context = {
                "parent_id": record.get("id"),
                "child_id": child_id,
                **(context or {}),
            }
            if context_mapper:
                mapped_context = context_mapper(record, child_id, context)
                if mapped_context is not None:
                    child_context = dict(mapped_context)
            contexts.append(child_context)

        # Store contexts for the DAG to process
        return {"_child_contexts": contexts}

    return StreamDependency(
        parent_stream=parent_stream,
        child_stream=child_stream,
        relation_type=StreamRelationType.ONE_TO_MANY,
        sync_trigger=SyncTrigger.IMMEDIATE,
        context_mapper=default_mapper,
    )


def create_many_to_one_dependency(
    parent_streams: list[str],
    child_stream: str,
    aggregator: t.Callable[[list[Context]], Context] | None = None,
) -> list[StreamDependency]:
    """Create many-to-one parent-child relationships.

    Use case: Multiple parent records merge into single child sync
    Example: Orders + Customers -> OrderAnalytics (aggregate data from both)

    Args:
        parent_streams: Names of the parent streams.
        child_stream: Name of the child stream.
        aggregator: Function to aggregate multiple parent contexts into one.

    Returns:
        List of StreamDependencies configured for many-to-one relationship.
    """

    def default_aggregator(contexts: list[Context]) -> Context:
        """Default aggregator that merges contexts and collects unique parent IDs."""  # noqa: DOC201
        merged: dict[str, t.Any] = {}
        parent_ids: list[t.Any] = []

        for ctx in contexts:
            merged.update(ctx)
            if "parent_id" in ctx:
                parent_ids.append(ctx["parent_id"])

        merged["parent_ids"] = list(set(parent_ids))  # Remove duplicates
        return merged

    dependencies = []
    for parent_stream in parent_streams:
        dep = StreamDependency(
            parent_stream=parent_stream,
            child_stream=child_stream,
            relation_type=StreamRelationType.MANY_TO_ONE,
            sync_trigger=SyncTrigger.AGGREGATED,
            context_mapper=lambda record, context: {
                "parent_id": record.get("id"),
                **(context or {}),
            },
            aggregator=aggregator or default_aggregator,
        )
        dependencies.append(dep)

    return dependencies


def create_many_to_many_dependency(
    parent_streams: list[str],
    child_stream: str,
    batch_sync: bool = False,  # noqa: FBT001, FBT002
) -> list[StreamDependency]:
    """Create many-to-many parent-child relationships.

    Use case: Multiple parent records each spawn syncs for same child stream
    Example: Multiple API endpoints -> Events stream (events from different sources)

    Args:
        parent_streams: Names of the parent streams.
        child_stream: Name of the child stream.
        batch_sync: If True, wait for all parents to finish before syncing child.

    Returns:
        List of StreamDependencies configured for many-to-many relationship.
    """
    sync_trigger = SyncTrigger.BATCH if batch_sync else SyncTrigger.IMMEDIATE

    dependencies = []
    for parent_stream in parent_streams:
        dep = StreamDependency(
            parent_stream=parent_stream,
            child_stream=child_stream,
            relation_type=StreamRelationType.MANY_TO_MANY,
            sync_trigger=sync_trigger,
            context_mapper=lambda record, context: {
                "parent_stream": parent_stream,  # noqa: B023
                "parent_id": record.get("id"),
                **(context or {}),
            },
        )
        dependencies.append(dep)

    return dependencies


def create_conditional_dependency(
    parent_stream: str,
    child_stream: str,
    condition: t.Callable[[Record, Context | None], bool],
    context_mapper: t.Callable[[Record, Context | None], Context | None] | None = None,
) -> StreamDependency:
    """Create a conditional parent-child relationship.

    Use case: Child stream syncs only when certain conditions are met
    Example: Users -> PremiumFeatures (only sync premium users)

    Args:
        parent_stream: Name of the parent stream.
        child_stream: Name of the child stream.
        condition: Function that determines if child should be synced.
        context_mapper: Optional function to transform parent record into child context.

    Returns:
        StreamDependency with conditional logic.
    """

    def default_mapper(record: Record, context: Context | None) -> Context | None:
        return {"parent_id": record.get("id"), **(context or {})}

    return StreamDependency(
        parent_stream=parent_stream,
        child_stream=child_stream,
        relation_type=StreamRelationType.ONE_TO_ONE,
        sync_trigger=SyncTrigger.CONDITIONAL,
        context_mapper=context_mapper or default_mapper,
        condition=condition,
    )


class DAGPatterns:
    """Collection of common DAG setup patterns."""

    @staticmethod
    def setup_hierarchical_sync(dag: StreamDAG) -> None:
        """Set up a hierarchical sync pattern: Company -> Department -> Employee.

        This demonstrates a multi-level hierarchy where each level depends on the previous.

        Args:
            dag: StreamDAG instance to configure.
        """  # noqa: E501
        # Level 1: Company -> Department
        company_dept_dep = create_one_to_many_dependency(
            parent_stream="companies",
            child_stream="departments",
            child_id_extractor=lambda record: record.get("department_ids", []),
        )
        dag.add_dependency(company_dept_dep)

        # Level 2: Department -> Employee
        dept_emp_dep = create_one_to_many_dependency(
            parent_stream="departments",
            child_stream="employees",
            child_id_extractor=lambda record: record.get("employee_ids", []),
        )
        dag.add_dependency(dept_emp_dep)

    @staticmethod
    def setup_fan_out_pattern(dag: StreamDAG) -> None:
        """Set up a fan-out pattern: Order -> [OrderItems, OrderTracking, OrderPayments].

        This demonstrates one parent spawning multiple independent child streams.

        Args:
            dag: StreamDAG instance to configure.
        """  # noqa: E501
        order_streams = ["order_items", "order_tracking", "order_payments"]

        for child_stream in order_streams:
            dep = create_one_to_one_dependency(
                parent_stream="orders",
                child_stream=child_stream,
                context_mapper=lambda record, ctx: {"order_id": record.get("id")},  # noqa: ARG005
            )
            dag.add_dependency(dep)

    @staticmethod
    def setup_fan_in_pattern(dag: StreamDAG) -> None:
        """Set up a fan-in pattern: [Users, Orders, Products] -> Analytics.

        This demonstrates multiple parents feeding into one analytical child stream.

        Args:
            dag: StreamDAG instance to configure.
        """

        def analytics_aggregator(contexts: list[Context]) -> Context:
            """Aggregate data from multiple sources for analytics."""  # noqa: DOC201
            user_count = sum(
                1 for ctx in contexts if ctx.get("parent_stream") == "users"
            )
            order_count = sum(
                1 for ctx in contexts if ctx.get("parent_stream") == "orders"
            )
            product_count = sum(
                1 for ctx in contexts if ctx.get("parent_stream") == "products"
            )

            return {
                "analytics_batch_id": f"batch_{len(contexts)}",
                "user_count": user_count,
                "order_count": order_count,
                "product_count": product_count,
                "total_records": len(contexts),
            }

        dependencies = create_many_to_one_dependency(
            parent_streams=["users", "orders", "products"],
            child_stream="analytics",
            aggregator=analytics_aggregator,
        )

        for dep in dependencies:
            dag.add_dependency(dep)

    @staticmethod
    def setup_conditional_pattern(dag: StreamDAG) -> None:
        """Set up conditional syncing: Users -> PremiumFeatures (only premium users).

        Args:
            dag: StreamDAG instance to configure.
        """

        def is_premium_user(record: Record, context: Context | None) -> bool:  # noqa: ARG001
            """Check if user has premium status."""  # noqa: DOC201
            return record.get("subscription_type") == "premium"

        premium_dep = create_conditional_dependency(
            parent_stream="users",
            child_stream="premium_features",
            condition=is_premium_user,
            context_mapper=lambda record, ctx: {  # noqa: ARG005
                "user_id": record.get("id"),
                "subscription_type": record.get("subscription_type"),
            },
        )
        dag.add_dependency(premium_dep)


def demonstrate_all_patterns() -> StreamDAG:
    """Demonstrate all DAG patterns in a single configuration.

    Returns:
        A fully configured StreamDAG with multiple relationship patterns.
    """
    dag = StreamDAG()

    # Add hierarchical pattern
    DAGPatterns.setup_hierarchical_sync(dag)

    # Add fan-out pattern
    DAGPatterns.setup_fan_out_pattern(dag)

    # Add fan-in pattern
    DAGPatterns.setup_fan_in_pattern(dag)

    # Add conditional pattern
    DAGPatterns.setup_conditional_pattern(dag)

    return dag
