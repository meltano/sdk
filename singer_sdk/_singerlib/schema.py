"""Provides an object model for JSON Schema."""

from __future__ import annotations

import typing as t
from dataclasses import dataclass

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
class Schema:
    """Object model for JSON Schema.

    Tap and Target authors may find this to be more convenient than
    working directly with JSON Schema data structures.

    This is based on, and overwrites
    https://github.com/transferwise/pipelinewise-singer-python/blob/master/singer/schema.py.
    This is because we wanted to expand it with extra STANDARD_KEYS.
    """

    type: str | list[str] | None = None
    properties: dict | None = None
    items: t.Any | None = None
    description: str | None = None
    minimum: float | None = None
    maximum: float | None = None
    exclusiveMinimum: float | None = None
    exclusiveMaximum: float | None = None
    multipleOf: float | None = None
    maxLength: int | None = None
    minLength: int | None = None
    anyOf: t.Any | None = None
    format: str | None = None
    additionalProperties: t.Any | None = None
    patternProperties: t.Any | None = None
    required: list[str] | None = None
    enum: list[t.Any] | None = None
    title: str | None = None

    def to_dict(self) -> dict[str, t.Any]:
        """Return the raw JSON Schema as a (possibly nested) dict.

        Returns:
            The raw JSON Schema as a (possibly nested) dict.
        """
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
    def from_dict(cls: t.Type[Schema], data: dict, **schema_defaults: t.Any) -> Schema:
        """Initialize a Schema object based on the JSON Schema structure.

        Args:
            data: The JSON Schema structure.
            schema_defaults: Default values for the schema.

        Returns:
            The initialized Schema object.
        """
        kwargs = schema_defaults.copy()
        properties = data.get("properties")
        items = data.get("items")

        if properties is not None:
            kwargs["properties"] = {
                k: cls.from_dict(v, **schema_defaults) for k, v in properties.items()
            }
        if items is not None:
            kwargs["items"] = cls.from_dict(items, **schema_defaults)
        for key in STANDARD_KEYS:
            if key in data:
                kwargs[key] = data[key]
        return cls(**kwargs)
