# Schema Sources

The Singer SDK provides an extensible API for loading schemas from various sources through the `SchemaSource` system. This enables loading schemas from:

- **File directories** - JSON schema files in a directory structure
- **OpenAPI specifications** - Schema components from OpenAPI 3.1 specs (local or remote)
- **Custom sources** - Implement your own schema loading logic

## Basic Usage

```python
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
    schema = StreamSchema(SCHEMAS_DIR)  # Loads from projects.json
```

## OpenAPI Integration

```python
from singer_sdk import OpenAPISchema, StreamSchema

# Load from OpenAPI spec
openapi_source = OpenAPISchema("https://api.example.com/openapi.json")

class UsersStream(RESTStream):
    name = "users"
    schema = StreamSchema(openapi_source, key="User")  # Load "User" component
```
