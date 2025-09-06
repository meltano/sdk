# Stream DAG (Directed Acyclic Graph) Functionality

The Meltano SDK now supports complex stream relationships beyond simple parent-child patterns through a Directed Acyclic Graph (DAG) system. This enables sophisticated data synchronization scenarios where streams can have multiple dependencies, conditional syncing, and aggregated processing.

## Overview

The Stream DAG functionality addresses several use cases mentioned in [GitHub Discussion #2846](https://github.com/meltano/sdk/discussions/2846):

1. **One parent record spawning one child sync** - Traditional parent-child relationship
1. **One parent record spawning multiple child syncs** - Fan-out pattern
1. **Multiple parent records merging into single child sync** - Fan-in/aggregation pattern
1. **Multiple parent records each spawning sync for same child stream** - Many-to-many pattern
1. **All records from parent stream aggregating into single context** - Batch aggregation

## Key Components

### StreamDAG

The central orchestrator that manages stream relationships and execution order.

```python
from singer_sdk.helpers.stream_dag import StreamDAG

dag = StreamDAG()
```

### StreamDependency

Defines a relationship between two streams with configurable behavior.

```python
from singer_sdk.helpers.stream_dag import StreamDependency, StreamRelationType, SyncTrigger

dependency = StreamDependency(
    parent_stream="users",
    child_stream="user_profiles",
    relation_type=StreamRelationType.ONE_TO_ONE,
    sync_trigger=SyncTrigger.IMMEDIATE,
    context_mapper=lambda record, context: {"user_id": record["id"]}
)
```

### Sync Triggers

Control when child streams are synchronized:

- **`IMMEDIATE`** - Sync immediately when parent record is processed
- **`BATCH`** - Wait for parent stream completion, then sync for each context
- **`AGGREGATED`** - Aggregate all parent contexts into single child sync
- **`CONDITIONAL`** - Sync based on custom logic

### Relation Types

Define the nature of the relationship:

- **`ONE_TO_ONE`** - One parent record → one child sync
- **`ONE_TO_MANY`** - One parent record → multiple child syncs
- **`MANY_TO_ONE`** - Multiple parent records → single child sync
- **`MANY_TO_MANY`** - Multiple parents → multiple syncs for same child

## Basic Usage

### 1. Enable DAG in Your Tap

```python
from singer_sdk import Tap
from singer_sdk.helpers.stream_dag import StreamDAG, StreamDependency

class MyTap(Tap):
    def setup_dag(self) -> StreamDAG:
        """Configure stream relationships."""
        dag = super().setup_dag()  # Initialize base DAG

        # Define your dependencies here
        dependency = StreamDependency(
            parent_stream="companies",
            child_stream="departments",
            context_mapper=lambda record, context: {"company_id": record["id"]}
        )
        dag.add_dependency(dependency)

        return dag
```

### 2. DAG-Aware Streams

The base `Stream` class automatically supports DAG functionality. No changes needed in existing stream implementations.

## Usage Examples

### One-to-One Relationship

```python
# User → UserProfile (each user has one profile)
dependency = StreamDependency(
    parent_stream="users",
    child_stream="user_profiles",
    context_mapper=lambda record, context: {"user_id": record["id"]}
)
dag.add_dependency(dependency)
```

### One-to-Many Relationship

```python
# Project → Tasks (each project has multiple tasks)
from singer_sdk.helpers.dag_examples import create_one_to_many_dependency

dependency = create_one_to_many_dependency(
    parent_stream="projects",
    child_stream="tasks",
    child_id_extractor=lambda record: record.get("task_ids", [])
)
dag.add_dependency(dependency)
```

### Many-to-One Relationship (Aggregation)

```python
# Orders + Customers → Analytics (aggregate data from both)
from singer_sdk.helpers.dag_examples import create_many_to_one_dependency

def analytics_aggregator(contexts):
    order_count = sum(1 for ctx in contexts if ctx.get("source") == "orders")
    customer_count = sum(1 for ctx in contexts if ctx.get("source") == "customers")
    return {"order_count": order_count, "customer_count": customer_count}

dependencies = create_many_to_one_dependency(
    parent_streams=["orders", "customers"],
    child_stream="analytics",
    aggregator=analytics_aggregator
)

for dep in dependencies:
    dag.add_dependency(dep)
```

### Conditional Relationships

```python
# Users → PremiumFeatures (only for premium users)
from singer_sdk.helpers.dag_examples import create_conditional_dependency

def is_premium_user(record, context):
    return record.get("subscription_type") == "premium"

dependency = create_conditional_dependency(
    parent_stream="users",
    child_stream="premium_features",
    condition=is_premium_user
)
dag.add_dependency(dependency)
```

### Batch Processing

```python
# Process all parent records, then sync child once per context
dependency = StreamDependency(
    parent_stream="orders",
    child_stream="order_summary",
    sync_trigger=SyncTrigger.BATCH,
    context_mapper=lambda record, context: {"batch_id": "daily_batch"}
)
dag.add_dependency(dependency)
```

## Advanced Patterns

### Hierarchical Relationships

```python
# Company → Department → Employee
from singer_sdk.helpers.dag_examples import DAGPatterns

def setup_dag(self) -> StreamDAG:
    dag = super().setup_dag()
    DAGPatterns.setup_hierarchical_sync(dag)
    return dag
```

### Fan-Out Pattern

```python
# Order → [OrderItems, OrderTracking, OrderPayments]
DAGPatterns.setup_fan_out_pattern(dag)
```

### Fan-In Pattern

```python
# [Users, Orders, Products] → Analytics
DAGPatterns.setup_fan_in_pattern(dag)
```

## Context Mapping

Context mappers transform parent records into child stream contexts:

```python
def custom_context_mapper(record, parent_context):
    """Transform parent record into child context."""
    return {
        "parent_id": record["id"],
        "parent_type": record["type"],
        "timestamp": record.get("updated_at"),
        **parent_context  # Include parent context if needed
    }

dependency = StreamDependency(
    parent_stream="parent",
    child_stream="child",
    context_mapper=custom_context_mapper
)
```

## Error Handling

The DAG system includes several safety mechanisms:

### Cycle Detection

```python
# This will raise ValueError: "would create a cycle"
dag.add_dependency(StreamDependency("A", "B"))
dag.add_dependency(StreamDependency("B", "C"))
dag.add_dependency(StreamDependency("C", "A"))  # ❌ Creates cycle
```

### Execution Ordering

```python
# Get topologically sorted execution order
execution_order = dag.get_execution_order()
print(f"Streams will execute in this order: {execution_order}")
```

## Performance Considerations

### Stream Selection

- Only selected streams participate in DAG relationships
- Child streams are automatically synced when parent triggers them
- Use conditions to avoid unnecessary syncing

### Memory Usage

- Batch and aggregated triggers store contexts in memory until parent completes
- For large datasets, consider using immediate triggers or smaller batches

### Execution Order

- DAG automatically determines optimal execution order
- Independent streams can run in parallel
- Dependencies create sequential constraints

## Migration from Parent-Child Streams

Existing parent-child relationships continue to work unchanged. To migrate to DAG:

### Before (Traditional Parent-Child)

```python
class ChildStream(Stream):
    parent_stream_type = ParentStream

    def get_records(self, context):
        parent_id = context["parent_id"]
        # ... fetch child records
```

### After (DAG-based)

```python
class MyTap(Tap):
    def setup_dag(self) -> StreamDAG:
        dag = super().setup_dag()

        # Same functionality, but more flexible
        dependency = StreamDependency(
            parent_stream="parent_stream",
            child_stream="child_stream",
            context_mapper=lambda record, ctx: {"parent_id": record["id"]}
        )
        dag.add_dependency(dependency)

        return dag
```

## Testing

The DAG functionality includes comprehensive test utilities:

```python
from tests.core.test_stream_dag import MockStream, MockTap

def test_my_dag_setup():
    tap = MockTap()
    parent = tap.add_mock_stream("parent")
    child = tap.add_mock_stream("child")

    dag = tap.setup_dag()
    # ... configure dependencies

    # Simulate record processing
    dag.process_record("parent", {"id": 123}, None)

    # Verify child was synced
    assert len(child._sync_called_with) == 1
```

## Examples

See the complete working example in [`examples/dag_usage_example.py`](../examples/dag_usage_example.py) which demonstrates:

- Hierarchical relationships (Company → Department → Employee)
- One-to-one relationships (Order → OrderItems)
- Conditional relationships (Customer → PremiumFeatures)
- Many-to-one aggregation (Customer + Order → Analytics)
- Enterprise-only features

## Best Practices

1. **Start Simple** - Begin with basic one-to-one relationships before adding complexity
1. **Use Helper Functions** - Leverage `dag_examples.py` for common patterns
1. **Test Dependencies** - Always test your DAG configuration with sample data
1. **Monitor Performance** - Watch memory usage with batch/aggregated triggers
1. **Document Relationships** - Clear documentation helps maintain complex DAGs
1. **Validate Early** - DAG validation happens at setup time, catching errors early

## API Reference

### StreamDAG Methods

- `add_stream(stream)` - Register a stream with the DAG
- `add_dependency(dependency)` - Add a relationship between streams
- `get_execution_order()` - Get topologically sorted stream order
- `process_record(stream_name, record, context)` - Process a record and trigger dependents
- `finalize_stream(stream_name)` - Complete processing for a stream

### StreamDependency Properties

- `parent_stream` - Name of parent stream
- `child_stream` - Name of child stream
- `relation_type` - Type of relationship
- `sync_trigger` - When to sync child stream
- `context_mapper` - Function to create child context
- `condition` - Function to determine if sync should occur
- `aggregator` - Function to combine multiple contexts
- `priority` - Execution priority (higher = earlier)

This DAG functionality provides a powerful foundation for complex data synchronization scenarios while maintaining backward compatibility with existing parent-child patterns.
