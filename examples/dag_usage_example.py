"""Example demonstrating Stream DAG usage in a Meltano SDK tap.

This example shows how to implement the use cases mentioned in the GitHub discussion
for complex stream relationships using the new DAG functionality.
"""  # noqa: INP001

from __future__ import annotations

from singer_sdk import Stream, Tap
from singer_sdk.helpers.dag_examples import (
    create_conditional_dependency,
    create_many_to_one_dependency,
    create_one_to_many_dependency,
    create_one_to_one_dependency,
)
from singer_sdk.helpers.stream_dag import StreamDAG  # noqa: TC001
from singer_sdk.helpers.types import Context  # noqa: TC001


class CompanyStream(Stream):
    """Stream for company data."""

    name = "companies"
    schema = {  # noqa: RUF012
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "department_ids": {"type": "array", "items": {"type": "integer"}},
            "is_enterprise": {"type": "boolean"},
        },
    }

    def get_records(self, context: Context | None):  # noqa: ANN201, ARG002, PLR6301
        """Return sample company records."""  # noqa: DOC402
        yield {
            "id": 1,
            "name": "TechCorp",
            "department_ids": [1, 2, 3],
            "is_enterprise": True,
        }
        yield {
            "id": 2,
            "name": "SmallBiz",
            "department_ids": [4],
            "is_enterprise": False,
        }


class DepartmentStream(Stream):
    """Stream for department data."""

    name = "departments"
    schema = {  # noqa: RUF012
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "company_id": {"type": "integer"},
            "name": {"type": "string"},
            "employee_ids": {"type": "array", "items": {"type": "integer"}},
        },
    }

    def get_records(self, context: Context | None):  # noqa: ANN201, PLR6301
        """Return sample department records."""  # noqa: DOC402
        if context and "parent_id" in context:
            company_id = context["parent_id"]
            dept_id = context.get("child_id", 1)
            yield {
                "id": dept_id,
                "company_id": company_id,
                "name": f"Department {dept_id}",
                "employee_ids": [
                    dept_id * 10 + i for i in range(1, 4)
                ],  # 3 employees per dept
            }


class EmployeeStream(Stream):
    """Stream for employee data."""

    name = "employees"
    schema = {  # noqa: RUF012
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "department_id": {"type": "integer"},
            "name": {"type": "string"},
            "email": {"type": "string"},
        },
    }

    def get_records(self, context: Context | None):  # noqa: ANN201, PLR6301
        """Return sample employee records."""  # noqa: DOC402
        if context and "parent_id" in context:
            dept_id = context["parent_id"]
            emp_id = context.get("child_id", 1)
            yield {
                "id": emp_id,
                "department_id": dept_id,
                "name": f"Employee {emp_id}",
                "email": f"employee{emp_id}@company.com",
            }


class OrderStream(Stream):
    """Stream for order data."""

    name = "orders"
    schema = {  # noqa: RUF012
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "customer_id": {"type": "integer"},
            "total": {"type": "number"},
            "status": {"type": "string"},
        },
    }

    def get_records(self, context: Context | None):  # noqa: ANN201, ARG002, PLR6301
        """Return sample order records."""  # noqa: DOC402
        yield {"id": 1, "customer_id": 101, "total": 299.99, "status": "completed"}
        yield {"id": 2, "customer_id": 102, "total": 149.50, "status": "pending"}


class OrderItemStream(Stream):
    """Stream for order item data."""

    name = "order_items"
    schema = {  # noqa: RUF012
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "order_id": {"type": "integer"},
            "product_id": {"type": "integer"},
            "quantity": {"type": "integer"},
            "price": {"type": "number"},
        },
    }

    def get_records(self, context: Context | None):  # noqa: ANN201, PLR6301
        """Return sample order item records."""  # noqa: DOC402
        if context and "order_id" in context:
            order_id = context["order_id"]
            yield {
                "id": order_id * 10 + 1,
                "order_id": order_id,
                "product_id": 201,
                "quantity": 2,
                "price": 99.99,
            }


class CustomerStream(Stream):
    """Stream for customer data."""

    name = "customers"
    schema = {  # noqa: RUF012
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "email": {"type": "string"},
            "subscription_type": {"type": "string"},
        },
    }

    def get_records(self, context: Context | None):  # noqa: ANN201, ARG002, PLR6301
        """Return sample customer records."""  # noqa: DOC402
        yield {
            "id": 101,
            "name": "John Doe",
            "email": "john@example.com",
            "subscription_type": "premium",
        }
        yield {
            "id": 102,
            "name": "Jane Smith",
            "email": "jane@example.com",
            "subscription_type": "basic",
        }


class PremiumFeatureStream(Stream):
    """Stream for premium customer features."""

    name = "premium_features"
    schema = {  # noqa: RUF012
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "customer_id": {"type": "integer"},
            "feature_name": {"type": "string"},
            "enabled": {"type": "boolean"},
        },
    }

    def get_records(self, context: Context | None):  # noqa: ANN201, PLR6301
        """Return premium features for customer."""  # noqa: DOC402
        if context and "user_id" in context:
            customer_id = context["user_id"]
            yield {
                "id": customer_id * 100 + 1,
                "customer_id": customer_id,
                "feature_name": "advanced_analytics",
                "enabled": True,
            }


class AnalyticsStream(Stream):
    """Stream for aggregated analytics."""

    name = "analytics"
    schema = {  # noqa: RUF012
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "metric_name": {"type": "string"},
            "metric_value": {"type": "number"},
            "source_streams": {"type": "array", "items": {"type": "string"}},
        },
    }

    def get_records(self, context: Context | None):  # noqa: ANN201, PLR6301
        """Return analytics based on aggregated data."""  # noqa: DOC402
        if context and "total_records" in context:
            yield {
                "id": context.get("analytics_batch_id", "batch_1"),
                "metric_name": "total_records_processed",
                "metric_value": context["total_records"],
                "source_streams": ["customers", "orders"],
            }


class EnterpriseFeatureStream(Stream):
    """Stream for enterprise-only features."""

    name = "enterprise_features"
    schema = {  # noqa: RUF012
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "company_id": {"type": "integer"},
            "feature_name": {"type": "string"},
        },
    }

    def get_records(self, context: Context | None):  # noqa: ANN201, PLR6301
        """Return enterprise features."""  # noqa: DOC402
        if context and "parent_id" in context:
            company_id = context["parent_id"]
            yield {
                "id": company_id * 1000 + 1,
                "company_id": company_id,
                "feature_name": "advanced_security_suite",
            }


class ExampleDAGTap(Tap):
    """Example tap demonstrating DAG functionality."""

    name = "example-dag-tap"

    def discover_streams(self) -> list[Stream]:
        """Return list of discovered streams."""
        return [
            CompanyStream(self),
            DepartmentStream(self),
            EmployeeStream(self),
            OrderStream(self),
            OrderItemStream(self),
            CustomerStream(self),
            PremiumFeatureStream(self),
            AnalyticsStream(self),
            EnterpriseFeatureStream(self),
        ]

    def setup_dag(self) -> StreamDAG:
        """Configure complex stream relationships using DAG functionality."""  # noqa: DOC201
        dag = super().setup_dag()

        # Use case 1: Hierarchical relationship (Company -> Department -> Employee)
        self._setup_hierarchical_relationships(dag)

        # Use case 2: One-to-one relationship (Order -> OrderItems)
        self._setup_order_relationships(dag)

        # Use case 3: Conditional relationship (Customer -> PremiumFeatures, only premium)  # noqa: E501
        self._setup_conditional_relationships(dag)

        # Use case 4: Many-to-one relationship (Customer + Order -> Analytics)
        self._setup_analytics_relationships(dag)

        # Use case 5: Enterprise-only features
        self._setup_enterprise_relationships(dag)

        return dag

    def _setup_hierarchical_relationships(self, dag: StreamDAG) -> None:  # noqa: PLR6301
        """Set up Company -> Department -> Employee hierarchy."""
        # Company -> Department (one-to-many)
        company_dept_dep = create_one_to_many_dependency(
            parent_stream="companies",
            child_stream="departments",
            child_id_extractor=lambda record: record.get("department_ids", []),
        )
        dag.add_dependency(company_dept_dep)

        # Department -> Employee (one-to-many)
        dept_emp_dep = create_one_to_many_dependency(
            parent_stream="departments",
            child_stream="employees",
            child_id_extractor=lambda record: record.get("employee_ids", []),
        )
        dag.add_dependency(dept_emp_dep)

    def _setup_order_relationships(self, dag: StreamDAG) -> None:  # noqa: PLR6301
        """Set up Order -> OrderItems relationship."""
        order_items_dep = create_one_to_one_dependency(
            parent_stream="orders",
            child_stream="order_items",
            context_mapper=lambda record, context: {"order_id": record["id"]},  # noqa: ARG005
        )
        dag.add_dependency(order_items_dep)

    def _setup_conditional_relationships(self, dag: StreamDAG) -> None:  # noqa: PLR6301
        """Set up conditional Customer -> PremiumFeatures relationship."""

        def is_premium_customer(record, context):  # noqa: ANN001, ANN202, ARG001
            return record.get("subscription_type") == "premium"

        premium_dep = create_conditional_dependency(
            parent_stream="customers",
            child_stream="premium_features",
            condition=is_premium_customer,
            context_mapper=lambda record, context: {  # noqa: ARG005
                "user_id": record["id"],
                "subscription_type": record.get("subscription_type"),
            },
        )
        dag.add_dependency(premium_dep)

    def _setup_analytics_relationships(self, dag: StreamDAG) -> None:  # noqa: PLR6301
        """Set up many-to-one relationship feeding analytics."""

        def analytics_aggregator(contexts: list[Context]) -> Context:
            """Aggregate data from multiple streams for analytics."""  # noqa: DOC201
            customer_contexts = [
                ctx for ctx in contexts if ctx.get("source_stream") == "customers"
            ]
            order_contexts = [
                ctx for ctx in contexts if ctx.get("source_stream") == "orders"
            ]

            return {
                "analytics_batch_id": f"batch_{len(contexts)}",
                "customer_count": len(customer_contexts),
                "order_count": len(order_contexts),
                "total_records": len(contexts),
            }

        # Create many-to-one dependencies
        dependencies = create_many_to_one_dependency(
            parent_streams=["customers", "orders"],
            child_stream="analytics",
            aggregator=analytics_aggregator,
        )

        # Customize context mappers to identify source streams
        for dep in dependencies:
            if dep.parent_stream == "customers":
                dep.context_mapper = lambda record, context: {  # noqa: ARG005
                    "source_stream": "customers",
                    "customer_id": record["id"],
                }
            elif dep.parent_stream == "orders":
                dep.context_mapper = lambda record, context: {  # noqa: ARG005
                    "source_stream": "orders",
                    "order_id": record["id"],
                }

            dag.add_dependency(dep)

    def _setup_enterprise_relationships(self, dag: StreamDAG) -> None:  # noqa: PLR6301
        """Set up enterprise-only features."""

        def is_enterprise_company(record, context):  # noqa: ANN001, ANN202, ARG001
            return record.get("is_enterprise", False)

        enterprise_dep = create_conditional_dependency(
            parent_stream="companies",
            child_stream="enterprise_features",
            condition=is_enterprise_company,
        )
        dag.add_dependency(enterprise_dep)


def main():  # noqa: ANN201
    """Demonstrate the DAG tap in action."""
    # Create and configure the tap
    tap = ExampleDAGTap()

    # Set up the DAG relationships
    dag = tap.setup_dag()

    print("=== DAG Configuration ===")  # noqa: T201
    print(f"Streams: {list(dag._stream_registry.keys())}")  # noqa: SLF001, T201
    print(f"Execution order: {dag.get_execution_order()}")  # noqa: T201

    print("\n=== Stream Dependencies ===")  # noqa: T201
    for stream_name, node in dag._nodes.items():  # noqa: SLF001
        if node.dependencies:
            deps = [
                f"{dep.parent_stream} ({dep.relation_type.value})"
                for dep in node.dependencies
            ]
            print(f"{stream_name} <- {', '.join(deps)}")  # noqa: T201
        if node.dependents:
            deps = [
                f"{dep.child_stream} ({dep.relation_type.value})"
                for dep in node.dependents
            ]
            print(f"{stream_name} -> {', '.join(deps)}")  # noqa: T201

    print("\n=== Simulating Sync ===")  # noqa: T201

    # This would normally be called by the tap framework
    # Here we're just demonstrating the DAG structure

    print("In a real sync, the DAG would:")  # noqa: T201
    print("1. Process company records")  # noqa: T201
    print("2. For each company, immediately sync relevant departments")  # noqa: T201
    print("3. For each department, immediately sync relevant employees")  # noqa: T201
    print("4. Process order records and sync order items")  # noqa: T201
    print(  # noqa: T201
        "5. Process customer records, sync premium features for premium customers only"
    )
    print(  # noqa: T201
        "6. After customers and orders complete, aggregate data into analytics stream"
    )
    print("7. Sync enterprise features only for enterprise companies")  # noqa: T201

    print(  # noqa: T201
        f"\nTotal configured dependencies: {sum(len(node.dependencies) for node in dag._nodes.values())}"  # noqa: E501, SLF001
    )


if __name__ == "__main__":
    main()
