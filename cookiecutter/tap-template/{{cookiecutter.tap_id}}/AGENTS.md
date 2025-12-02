# AGENTS.md - AI Agent Development Guide for {{ cookiecutter.tap_id }}

This document provides guidance for AI coding agents and developers working on this Singer tap.

## Project Overview

- **Project Type**: Singer Tap
- **Source**: {{ cookiecutter.source_name }}
- **Stream Type**: {{ cookiecutter.stream_type }}
- **Authentication**: {{ cookiecutter.auth_method }}
- **Framework**: Meltano Singer SDK

## Architecture

This tap follows the Singer specification and uses the Meltano Singer SDK to extract data from {{ cookiecutter.source_name }}.

### Key Components

1. **Tap Class** (`{{ cookiecutter.library_name }}/tap.py`): Main entry point, defines streams and configuration
1. **Client** (`{{ cookiecutter.library_name }}/client.py`): Handles API communication and authentication
1. **Streams** (`{{ cookiecutter.library_name }}/streams.py`): Define data streams and their schemas
{% if cookiecutter.auth_method in ['OAuth2', 'JWT'] -%}
1. **Authentication** (`{{ cookiecutter.library_name }}/auth.py`): Implements {{ cookiecutter.auth_method }} authentication logic
{% endif -%}

## Development Guidelines for AI Agents

### Understanding Singer Concepts

Before making changes, ensure you understand these Singer concepts:

- **Streams**: Individual data endpoints (e.g., users, orders, transactions)
- **State**: Tracks incremental sync progress using bookmarks
- **Catalog**: Metadata about available streams and their schemas
- **Records**: Individual data items emitted by the tap
- **Schemas**: JSON Schema definitions for stream data

### Common Tasks

#### Adding a New Stream

1. Define stream class in `{{ cookiecutter.library_name }}/streams.py`
1. Set `name`, `path`, `primary_keys`, and `replication_key`
1. Define schema using `PropertiesList` or JSON Schema
1. Register stream in the tap's `discover_streams()` method

Example:

```python
class MyNewStream({{ cookiecutter.source_name }}Stream):
    name = "my_new_stream"
    path = "/api/v1/my_resource"
    primary_keys = ["id"]
    replication_key = "updated_at"

    schema = PropertiesList(
        Property("id", StringType, required=True),
        Property("name", StringType),
        Property("updated_at", DateTimeType),
    ).to_dict()
```

#### Modifying Authentication

{% if cookiecutter.auth_method == 'API Key' -%}

- Update `authenticator` in client class
- API key should be passed via headers or query parameters
- Configuration defined in `tap.py` config schema
  {% elif cookiecutter.auth_method == 'Bearer Token' -%}
- Token stored in `auth_token` config property
- Passed via Authorization header: `Bearer {token}`
  {% elif cookiecutter.auth_method == 'Basic Auth' -%}
- Username/password in config
- Automatically encoded to base64
  {% elif cookiecutter.auth_method == 'OAuth2' -%}
- Implements OAuth2 flow in `auth.py`
- Handles token refresh automatically
- Requires client_id, client_secret, and redirect_uri
  {% elif cookiecutter.auth_method == 'JWT' -%}
- JWT token generation in `auth.py`
- Token included in Authorization header
  {% endif -%}

#### Handling Pagination

The SDK provides built-in pagination classes. **Use these instead of overriding `get_next_page_token()` directly.**

**Built-in Paginator Classes:**

1. **SimpleHeaderPaginator**: For APIs using Link headers (RFC 5988)

   ```python
   from singer_sdk.pagination import SimpleHeaderPaginator

   class MyStream({{ cookiecutter.source_name }}Stream):
       def get_new_paginator(self):
           return SimpleHeaderPaginator()
   ```

1. **HeaderLinkPaginator**: For APIs with `Link: <url>; rel="next"` headers

   ```python
   from singer_sdk.pagination import HeaderLinkPaginator

   class MyStream({{ cookiecutter.source_name }}Stream):
       def get_new_paginator(self):
           return HeaderLinkPaginator()
   ```

1. **JSONPathPaginator**: For cursor/token in response body

   ```python
   from singer_sdk.pagination import JSONPathPaginator

   class MyStream({{ cookiecutter.source_name }}Stream):
       def get_new_paginator(self):
           return JSONPathPaginator("$.pagination.next_token")
   ```

1. **SinglePagePaginator**: For non-paginated endpoints

   ```python
   from singer_sdk.pagination import SinglePagePaginator

   class MyStream({{ cookiecutter.source_name }}Stream):
       def get_new_paginator(self):
           return SinglePagePaginator()
   ```

**Creating Custom Paginators:**

For complex pagination logic, create a custom paginator class:

```python
from singer_sdk.pagination import BasePageNumberPaginator

class MyCustomPaginator(BasePageNumberPaginator):
    def has_more(self, response):
        """Check if there are more pages."""
        data = response.json()
        return data.get("has_more", False)

    def get_next_url(self, response):
        """Get the next page URL."""
        data = response.json()
        if self.has_more(response):
            return data.get("next_url")
        return None

# Use in stream
class MyStream({{ cookiecutter.source_name }}Stream):
    def get_new_paginator(self):
        return MyCustomPaginator(start_value=1)
```

**Common Pagination Patterns:**

- **Offset-based**: Extend `BaseOffsetPaginator`
- **Page-based**: Extend `BasePageNumberPaginator`
- **Cursor-based**: Extend `BaseAPIPaginator` with custom logic
- **HATEOAS/HAL**: Use `JSONPathPaginator` with appropriate JSON path

Only override `get_next_page_token()` as a last resort for very simple cases.

#### State and Incremental Sync

- Set `replication_key` to enable incremental sync (e.g., "updated_at")
- Override `get_starting_timestamp()` to set initial sync point
- State automatically managed by SDK
- Access current state via `get_context_state()`

#### Schema Evolution

- Use flexible schemas during development
- Add new properties without breaking changes
- Consider making fields optional when unsure
- Use `th.Property("field", th.StringType)` for basic types
- Nest objects with `th.ObjectType(...)`

### Testing

Run tests to verify your changes:

```bash
# Install dependencies
uv sync

# Run all tests
uv run pytest

# Run specific test
uv run pytest tests/test_core.py -k test_name
```

### Configuration

Configuration properties are defined in the tap class:

- Required vs optional properties
- Secret properties (passwords, tokens)
- Mark sensitive data with `secret=True` parameter
- Defaults specified in config schema

Example configuration schema:

```python
from singer_sdk import typing as th

config_jsonschema = th.PropertiesList(
    th.Property("api_url", th.StringType, required=True),
    th.Property("api_key", th.StringType, required=True, secret=True),
    th.Property("start_date", th.DateTimeType),
    th.Property("user_agent", th.StringType, default="tap-mysource"),
).to_dict()
```

Example test with config:

```bash
{{ cookiecutter.tap_id }} --config config.json --discover
{{ cookiecutter.tap_id }} --config config.json --catalog catalog.json
```

### Common Pitfalls

1. **Rate Limiting**: Implement backoff using `RESTStream` built-in retry logic
1. **Large Responses**: Use pagination, don't load entire dataset into memory
1. **Schema Mismatches**: Validate data matches schema, handle null values
1. **State Management**: Don't modify state directly, use SDK methods
1. **Timezone Handling**: Use UTC, parse ISO 8601 datetime strings
1. **Error Handling**: Let SDK handle retries, log warnings for data issues

### SDK Resources

- [Singer SDK Documentation](https://sdk.meltano.com)
- [Singer Spec](https://hub.meltano.com/singer/spec)
- [SDK Reference](https://sdk.meltano.com/en/latest/reference.html)
- [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html)

### Best Practices

1. **Logging**: Use `self.logger` for structured logging
1. **Validation**: Validate API responses before emitting records
1. **Documentation**: Update README with new streams and config options
1. **Type Hints**: Add type hints to improve code clarity
1. **Testing**: Write tests for new streams and edge cases
1. **Performance**: Profile slow streams, optimize API calls
1. **Error Messages**: Provide clear, actionable error messages

## File Structure

```
{{ cookiecutter.tap_id }}/
├── {{ cookiecutter.library_name }}/
│   ├── __init__.py
│   ├── tap.py          # Main tap class
│   ├── client.py       # API client
│   └── streams.py      # Stream definitions
├── tests/
│   ├── __init__.py
│   └── test_core.py
├── config.json         # Example configuration
├── pyproject.toml      # Dependencies and metadata
└── README.md          # User documentation
```

## Additional Resources

- Project README: See `README.md` for setup and usage
- Singer SDK: https://sdk.meltano.com
- Meltano: https://meltano.com
- Singer Specification: https://hub.meltano.com/singer/spec

## Making Changes

When implementing changes:

1. Understand the existing code structure
1. Follow Singer and SDK patterns
1. Test thoroughly with real API credentials
1. Update documentation and docstrings
1. Ensure backward compatibility when possible
1. Run linting and type checking

## Questions?

If you're uncertain about an implementation:

- Check SDK documentation for similar examples
- Review other Singer taps for patterns
- Test incrementally with small changes
- Validate against the Singer specification
