# Schema Sources

The Singer SDK provides an extensible API for loading schemas from various sources through the `SchemaSource` system. This enables loading schemas from:

- **File directories** - JSON schema files in a directory structure
- **OpenAPI specifications** - Schema components from OpenAPI 3.1 specs (local or remote)
- **Custom sources** - Implement your own schema loading logic

## Basic Usage

```python
from typing import ClassVar

from singer_sdk import RESTStream, SchemaDirectory, StreamSchema

from myproject import schemas

# Create a schema source from a directory.
# For example, if the package has the following directory structure:
# myproject
# └── schemas
#     ├── __init__.py
#     ├── projects.json
#     └── users.json
# then the following code will load the schemas from the schemas subpackage:
SCHEMAS_DIR = SchemaDirectory(schemas)

class ProjectsStream(RESTStream):
    name = "projects"
    schema: ClassVar[StreamSchema] = StreamSchema(SCHEMAS_DIR)  # Loads from projects.json
```

## OpenAPI Integration

```python
from typing import ClassVar

from singer_sdk import OpenAPISchema, StreamSchema

# Load from OpenAPI spec
openapi_source = OpenAPISchema("https://api.example.com/openapi.json")

class UsersStream(RESTStream):
    name = "users"
    schema: ClassVar[StreamSchema] = StreamSchema(openapi_source, key="User")  # Load "User" component
```

## Migrating from File-Path Based Schemas

If you're upgrading from an older version of the Singer SDK that used file paths for schema loading, here's how to migrate your streams to use the new schema sources system.

### Before: Using `schema_filepath`

```python
from pathlib import Path
from singer_sdk import RESTStream

SCHEMAS_DIR = Path(__file__).parent / "schemas"

class ProjectsStream(RESTStream):
    """Projects stream with file-path based schema."""

    name = "projects"
    schema_filepath = SCHEMAS_DIR / "projects.json"  # Deprecated approach
```

### After: Using `StreamSchema` with `SchemaDirectory`

```python
from typing import ClassVar
from singer_sdk import RESTStream, SchemaDirectory, StreamSchema
from myproject import schemas  # Your schemas module/package

SCHEMAS_DIR = SchemaDirectory(schemas)

class ProjectsStream(RESTStream):
    """Projects stream with schema source."""

    name = "projects"
    schema: ClassVar[StreamSchema] = StreamSchema(SCHEMAS_DIR)  # New approach
```

### Migration Steps

1. **Organize your schemas**: Ensure your schema files are in a Python package with an `__init__.py` file:

   ```
   myproject/
   ├── __init__.py
   ├── streams.py
   └── schemas/
       ├── __init__.py
       ├── projects.json
       └── users.json
   ```

1. **Update imports**: Add the new schema source imports:

   ```python
   from singer_sdk import SchemaDirectory, StreamSchema
   from myproject import schemas
   ```

1. **Create schema directory**: Replace file paths with a schema directory:

   ```python
   # Before
   SCHEMAS_DIR = Path(__file__).parent / "schemas"

   # After
   SCHEMAS_DIR = SchemaDirectory(schemas)
   ```

1. **Update stream classes**: Replace `schema_filepath` with the `StreamSchema` descriptor:

   ```python
   # Before
   class MyStream(RESTStream):
       schema_filepath = SCHEMAS_DIR / "my_stream.json"

   # After
   class MyStream(RESTStream):
       schema: ClassVar[StreamSchema] = StreamSchema(SCHEMAS_DIR)
   ```

1. **Handle custom schema keys** (if needed):

   ```python
   # If your stream name doesn't match the schema file name
   class ProjectDetailsStream(RESTStream):
       name = "project_details"
       schema: ClassVar[StreamSchema] = StreamSchema(SCHEMAS_DIR, key="ProjectDetail")  # Uses ProjectDetail.json
   ```

### Benefits of Migration

- **Better performance**: Schema caching reduces file I/O
- **Type safety**: Improved type hints and validation
- **Flexibility**: Support for multiple schema sources (files, OpenAPI, custom)
- **Future-proof**: Access to new schema source features
