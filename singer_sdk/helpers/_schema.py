"""Provides an object model for JSON Schema."""

from dataclasses import dataclass
from typing import Any, List, Optional, Union

from singer import Schema

# These are keys defined in the JSON Schema spec that do not themselves contain
# schemas (or lists of schemas)
STANDARD_KEYS = [
    "title",
    "description",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "maxLength",
    "minLength",
    "format",
    "type",
    "required",
    "enum",
    # These are NOT simple keys (they can contain schemas themselves). We could
    # consider adding extra handling to them.
    "additionalProperties",
    "anyOf",
    "patternProperties",
]


@dataclass
class SchemaPlus(Schema):
    """Object model for JSON Schema.

    Tap and Target authors may find this to be more convenient than
    working directly with JSON Schema data structures.

    This is based on, and overwrites
    https://github.com/transferwise/pipelinewise-singer-python/blob/master/singer/schema.py.
    This is because we wanted to expand it with extra STANDARD_KEYS.

    """

    type: Optional[Union[str, List[str]]] = None
    properties: Optional[dict] = None
    items: Optional[Any] = None
    description: Optional[str] = None
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    exclusiveMinimum: Optional[float] = None
    exclusiveMaximum: Optional[float] = None
    multipleOf: Optional[float] = None
    maxLength: Optional[int] = None
    minLength: Optional[int] = None
    anyOf: Optional[Any] = None
    format: Optional[str] = None
    additionalProperties: Optional[Any] = None
    patternProperties: Optional[Any] = None
    required: Optional[List[str]] = None
    enum: Optional[List[Any]] = None
    title: Optional[str] = None

    def to_dict(self):
        """Return the raw JSON Schema as a (possibly nested) dict."""
        result = {}

        if self.properties is not None:
            result["properties"] = {k: v.to_dict() for k, v in self.properties.items()}

        if self.items is not None:
            result["items"] = self.items.to_dict()

        for key in STANDARD_KEYS:
            if self.__dict__.get(key) is not None:
                result[key] = self.__dict__[key]

        return result

    @classmethod
    def from_dict(cls, data, **schema_defaults):
        """Initialize a Schema object based on the JSON Schema structure.

        :param schema_defaults: The default values to the Schema constructor.
        """
        kwargs = schema_defaults.copy()
        properties = data.get("properties")
        items = data.get("items")

        if properties is not None:
            kwargs["properties"] = {
                k: SchemaPlus.from_dict(v, **schema_defaults)
                for k, v in properties.items()
            }
        if items is not None:
            kwargs["items"] = SchemaPlus.from_dict(items, **schema_defaults)
        for key in STANDARD_KEYS:
            if key in data:
                kwargs[key] = data[key]
        return SchemaPlus(**kwargs)
