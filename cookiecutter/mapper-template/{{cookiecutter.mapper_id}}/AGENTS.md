# AGENTS.md - AI Agent Development Guide for {{ cookiecutter.mapper_id }}

This document provides guidance for AI coding agents and developers working on this Singer mapper.

## Project Overview

- **Project Type**: Singer Mapper (Inline Stream Map)
- **Mapper Name**: {{ cookiecutter.name }}
- **Framework**: Meltano Singer SDK

## Architecture

This mapper sits between Singer taps and targets, transforming data in-flight using the Singer SDK's inline stream mapping capabilities.

### Key Components

1. **Mapper Class** (`{{ cookiecutter.library_name }}/mapper.py`): Main transformation logic
1. **Configuration**: Defines stream mappings and transformation rules

## Development Guidelines for AI Agents

### Understanding Singer Mapper Concepts

Before making changes, ensure you understand these concepts:

- **Stream Maps**: Transform records from one schema to another
- **Inline Transformation**: Data transformed in-memory during pipeline
- **Pass-through**: Records can pass through unchanged
- **Aliasing**: Rename streams
- **Filtering**: Remove records based on conditions
- **Property Mapping**: Transform individual fields

### Mapper Capabilities

Singer mappers can:

1. **Rename fields**: `{"old_name": "new_name"}`
1. **Add computed fields**: `{"total": "price * quantity"}`
1. **Filter records**: `{"__filter__": "price > 100"}`
1. **Exclude fields**: `{"__else__": null}` or use `null` for specific fields
1. **Alias streams**: Rename entire streams
1. **Clone streams**: Create multiple outputs from one input
1. **Flatten objects**: Convert nested JSON to flat structure
1. **Hash/mask data**: Anonymize sensitive fields

### Common Tasks

#### Adding Stream Transformations

Define mappings in mapper configuration:

```python
class {{ cookiecutter.name }}Mapper(InlineMapper):
    name = "{{ cookiecutter.mapper_id }}"

    # Example: Transform user records
    stream_maps = {
        "users": {
            "user_id": "id",  # Rename id to user_id
            "full_name": "first_name + ' ' + last_name",  # Compute
            "email_hash": "md5(email)",  # Hash sensitive data
            "age": "(today() - birth_date).days / 365",  # Calculate age
            "__filter__": "is_active == true",  # Only active users
        },
        "orders": {
            "order_id": "id",
            "customer_id": "user_id",
            "total": "subtotal + tax + shipping",
            "items": None,  # Exclude items array
        },
    }
```

#### Expression Syntax

Stream maps support Python-like expressions:

**Arithmetic**:

```python
"total": "price * quantity"
"discount_percent": "(original_price - sale_price) / original_price * 100"
```

**String operations**:

```python
"full_name": "first_name + ' ' + last_name"
"upper_name": "name.upper()"
"domain": "email.split('@')[1]"
```

**Date/time**:

```python
"today": "today()"
"age_days": "(today() - created_at).days"
"formatted_date": "created_at.strftime('%Y-%m-%d')"
```

**Conditional logic**:

```python
"status": "'active' if is_enabled else 'inactive'"
"category": "'high' if price > 1000 else 'low'"
```

**Functions**:

```python
"email_hash": "md5(email)"
"uuid": "uuid4()"
"length": "len(description)"
```

#### Filtering Records

Use `__filter__` to include only matching records:

```python
stream_maps = {
    "transactions": {
        "__filter__": "amount > 0 and status == 'completed'",
        # ... field mappings
    }
}
```

Complex filters:

```python
"__filter__": "country in ['US', 'CA'] and age >= 18"
"__filter__": "created_at > '2024-01-01' and not is_deleted"
```

#### Flattening Nested Objects

Flatten nested JSON structures:

```python
stream_maps = {
    "orders": {
        "order_id": "id",
        "customer_name": "customer.name",
        "customer_email": "customer.email",
        "shipping_address": "shipping.address.street",
        "shipping_city": "shipping.address.city",
    }
}
```

#### Excluding Sensitive Fields

Remove PII or sensitive data:

```python
stream_maps = {
    "users": {
        "id": "id",
        "username": "username",
        "email": None,  # Exclude
        "password": None,  # Exclude
        "ssn": None,  # Exclude
        "created_at": "created_at",
    }
}
```

Or hash sensitive fields:

```python
stream_maps = {
    "users": {
        "email_hash": "md5(email)",
        "ssn_hash": "sha256(ssn)",
        # Original fields excluded implicitly if not mapped
        "__else__": "pass",  # Pass through other fields
    }
}
```

#### Cloning Streams

Create multiple output streams from one input:

```python
stream_maps = {
    "transactions": {
        "transaction_id": "id",
        "amount": "amount",
        # ... other fields
    },
    "transactions__high_value": {
        "__source__": "transactions",
        "__filter__": "amount > 10000",
        "transaction_id": "id",
        "amount": "amount",
        "flagged": "'high_value'",
    }
}
```

#### Dynamic Configuration

Allow users to configure mappings:

```python
class {{ cookiecutter.name }}Mapper(InlineMapper):
    config_jsonschema = PropertiesList(
        Property("hash_email", BooleanType, default=False),
        Property("filter_active_only", BooleanType, default=True),
        Property("min_amount", NumberType, default=0),
    ).to_dict()

    @property
    def stream_maps(self):
        """Generate stream maps based on config."""
        maps = {}
        if self.config.get("hash_email"):
            maps["users"]["email"] = "md5(email)"
        if self.config.get("filter_active_only"):
            maps["users"]["__filter__"] = "is_active == true"
        return maps
```

### Testing

Test your mapper with sample data:

```bash
# Install dependencies
uv sync

# Test mapper with sample input
cat sample_input.singer | {{ cookiecutter.mapper_id }} --config config.json

# Full pipeline test
tap-something | {{ cookiecutter.mapper_id }} --config mapper_config.json | target-something --config target_config.json
```

Create test cases:

```python
# tests/test_mapper.py
def test_field_rename():
    mapper = {{ cookiecutter.name }}Mapper(config={})
    record = {"old_name": "value"}
    result = mapper.map_record(record, "stream_name")
    assert result["new_name"] == "value"
    assert "old_name" not in result

def test_filtering():
    mapper = {{ cookiecutter.name }}Mapper(config={})
    # Should include
    record1 = {"amount": 100}
    assert mapper.should_include(record1, "transactions")

    # Should exclude
    record2 = {"amount": -10}
    assert not mapper.should_include(record2, "transactions")
```

### Common Use Cases

#### Data Privacy Compliance

Remove or hash PII for GDPR/CCPA:

```python
stream_maps = {
    "customers": {
        "customer_id": "id",
        "email_hash": "md5(email)",
        "name_hash": "md5(name)",
        "signup_date": "created_at",
        # Exclude direct PII
        "email": None,
        "phone": None,
        "address": None,
    }
}
```

#### Data Enrichment

Add computed fields:

```python
stream_maps = {
    "orders": {
        "*": "record",  # Include all original fields
        "total_with_tax": "subtotal * 1.08",
        "is_large_order": "item_count > 10",
        "days_since_order": "(today() - order_date).days",
    }
}
```

#### Schema Standardization

Normalize fields across sources:

```python
stream_maps = {
    "contacts": {
        "id": "contact_id || customer_id || user_id",  # Handle variations
        "email": "email_address || email || contact_email",
        "name": "full_name || name",
        "created": "created_at || creation_date || date_added",
    }
}
```

#### Splitting Large Objects

Break apart nested objects:

```python
stream_maps = {
    "orders__items": {
        "__source__": "orders",
        "order_id": "id",
        "item": "items[]",  # Expand array to separate records
    }
}
```

### Performance Considerations

1. **Memory**: Mappers process records in-memory

   - Keep transformations lightweight
   - Avoid storing large amounts of state

1. **CPU**: Complex expressions add overhead

   - Profile transformations
   - Simplify expressions where possible

1. **Throughput**:

   - Filtering reduces downstream load
   - Removing fields reduces message size

### Keeping meltano.yml and Mapper Settings in Sync

When this mapper is used with Meltano, the settings defined in `meltano.yml` must stay in sync with the `config_jsonschema` in the mapper class. Configuration drift between these two sources causes confusion and runtime errors.

**When to sync:**

- Adding new configuration properties to the mapper
- Removing or renaming existing properties
- Changing property types, defaults, or descriptions
- Marking properties as required or secret

**How to sync:**

1. Update `config_jsonschema` in `{{ cookiecutter.library_name }}/mapper.py`
1. Update the corresponding `settings` block in `meltano.yml`
1. Update `.env.example` with the new environment variable

Example - adding a new `hash_algorithm` setting:

```python
# {{ cookiecutter.library_name }}/mapper.py
config_jsonschema = th.PropertiesList(
    th.Property("hash_email", th.BooleanType, default=False),
    th.Property("hash_algorithm", th.StringType, default="md5"),  # New setting
).to_dict()
```

```yaml
# meltano.yml
plugins:
  mappers:
    - name: {{ cookiecutter.mapper_id }}
      settings:
        - name: hash_email
          kind: boolean
          value: false
        - name: hash_algorithm  # New setting
          kind: string
          value: md5
```

```bash
# .env.example
{{ cookiecutter.mapper_id | upper | replace('-', '_') }}_HASH_EMAIL=false
{{ cookiecutter.mapper_id | upper | replace('-', '_') }}_HASH_ALGORITHM=md5  # New setting
```

**Setting kind mappings:**

| Python Type | Meltano Kind |
|-------------|--------------|
| `StringType` | `string` |
| `IntegerType` | `integer` |
| `BooleanType` | `boolean` |
| `NumberType` | `number` |
| `DateTimeType` | `date_iso8601` |
| `ArrayType` | `array` |
| `ObjectType` | `object` |

Any properties with `secret=True` should be marked with `sensitive: true` in `meltano.yml`.

**Best practices:**

- Always update all three files (`mapper.py`, `meltano.yml`, `.env.example`) in the same commit
- Use the same default values in all locations
- Keep descriptions consistent between code docstrings and `meltano.yml` `description` fields

> **Note:** This guidance is consistent with tap and target templates in the Singer SDK. See the [SDK documentation](https://sdk.meltano.com) for canonical reference.

### Common Pitfalls

1. **Null Handling**: Check for null before operations

   ```python
   "safe_value": "field if field is not None else 0"
   ```

1. **Type Mismatches**: Ensure compatible types

   ```python
   "string_value": "str(numeric_field)"
   ```

1. **Missing Fields**: Handle optional fields

   ```python
   "value": "field.get('nested', 'default')"
   ```

1. **Circular References**: Don't create infinite loops

1. **State Preservation**: Mappers should be stateless

1. **Schema Changes**: Handle schema evolution gracefully

### SDK Resources

- [Stream Maps Documentation](https://sdk.meltano.com/en/latest/stream_maps.html)
- [Singer SDK Documentation](https://sdk.meltano.com)
- [Expression Functions Reference](https://sdk.meltano.com/en/latest/stream_maps.html#functions)

### Best Practices

1. **Documentation**: Document transformation logic
1. **Testing**: Test with edge cases (nulls, empty strings, etc.)
1. **Configuration**: Make transformations configurable
1. **Validation**: Validate output schema
1. **Logging**: Log transformation errors
1. **Idempotency**: Ensure consistent results
1. **Reversibility**: Consider if transformations can be undone
1. **Privacy**: Be careful with PII handling

## File Structure

```
{{ cookiecutter.mapper_id }}/
├── {{ cookiecutter.library_name }}/
│   ├── __init__.py
│   └── mapper.py       # Mapper implementation
├── tests/
│   ├── __init__.py
│   └── test_mapper.py
├── config.json         # Example configuration
├── pyproject.toml      # Dependencies and metadata
└── README.md          # User documentation
```

## Additional Resources

- Project README: See `README.md` for setup and usage
- Singer SDK: https://sdk.meltano.com
- Stream Maps Guide: https://sdk.meltano.com/en/latest/stream_maps.html
- Meltano: https://meltano.com

## Making Changes

When implementing changes:

1. Understand the data flowing through the mapper
1. Test transformations with real data
1. Handle edge cases (nulls, missing fields, type mismatches)
1. Document transformation logic
1. Update configuration schema
1. Ensure backward compatibility
1. Run tests and type checking

## Questions?

If you're uncertain about an implementation:

- Check stream maps documentation for expression syntax
- Review other mappers for transformation patterns
- Test incrementally with sample data
- Consider performance implications
- Validate output schema matches expectations
