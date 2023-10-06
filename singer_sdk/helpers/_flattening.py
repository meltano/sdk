"""Internal helper library for record flatteting functions."""

from __future__ import annotations

import collections
import itertools
import re
import typing as t
from copy import deepcopy

import inflection
import simplejson as json

DEFAULT_FLATTENING_SEPARATOR = "__"


class FlatteningOptions(t.NamedTuple):
    """A stream map which performs the flattening role."""

    max_level: int
    flattening_enabled: bool = True
    separator: str = DEFAULT_FLATTENING_SEPARATOR


def get_flattening_options(
    plugin_config: t.Mapping,
) -> FlatteningOptions | None:
    """Get flattening options, if flattening is enabled.

    Args:
        plugin_config: The tap or target config dictionary.

    Returns:
        A new FlatteningOptions object or None if flattening is disabled.
    """
    if "flattening_enabled" in plugin_config and plugin_config["flattening_enabled"]:
        return FlatteningOptions(max_level=int(plugin_config["flattening_max_depth"]))

    return None


def flatten_key(key_name: str, parent_keys: list[str], separator: str = "__") -> str:
    """Concatenate `key_name` with its `parent_keys` using `separator`.

    Args:
        key_name: The node's key.
        parent_keys: A list of parent keys which are ancestors to this node.
        separator: The separator used during concatenation. Defaults to "__".

    Returns:
        The flattened key name as a string.

    >>> flatten_key("foo", ["bar", "baz"])
    'bar__baz__foo'

    >>> flatten_key("foo", ["bar", "baz"], separator=".")
    'bar.baz.foo'
    """
    full_key = [*parent_keys, key_name]
    inflected_key = full_key.copy()
    reducer_index = 0
    while len(
        separator.join(inflected_key),
    ) >= 255 and reducer_index < len(  # noqa: PLR2004
        inflected_key,
    ):
        reduced_key = re.sub(
            r"[a-z]",
            "",
            inflection.camelize(inflected_key[reducer_index]),
        )
        inflected_key[reducer_index] = (
            reduced_key if len(reduced_key) > 1 else inflected_key[reducer_index][0:3]
        ).lower()
        reducer_index += 1

    return separator.join(inflected_key)


def flatten_schema(
    schema: dict,
    max_level: int,
    separator: str = "__",
) -> dict:
    """Flatten the provided schema up to a depth of max_level.

    Args:
        schema: The schema definition to flatten.
        separator: The string to use when concatenating key names.
        max_level: The max recursion level (zero-based, exclusive).

    Returns:
        A flattened version of the provided schema definition.

    >>> import json
    >>> schema = {
    ...     "type": "object",
    ...     "properties": {
    ...         "id": {
    ...             "type": "string"
    ...         },
    ...         "foo": {
    ...             "type": "object",
    ...             "properties": {
    ...                 "bar": {
    ...                     "type": "object",
    ...                     "properties": {
    ...                         "baz": {
    ...                             "type": "object",
    ...                             "properties": {
    ...                                 "qux": {
    ...                                     "type": "string"
    ...                                 }
    ...                             }
    ...                         }
    ...                     }
    ...                 }
    ...             }
    ...         }
    ...     }
    ... }
    >>> print(json.dumps(flatten_schema(schema, 0), indent=2))
    {
      "type": "object",
      "properties": {
        "id": {
          "type": "string"
        },
        "foo": {
          "type": "object",
          "properties": {
            "bar": {
              "type": "object",
              "properties": {
                "baz": {
                  "type": "object",
                  "properties": {
                    "qux": {
                      "type": "string"
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    >>> print(json.dumps(flatten_schema(schema, 1), indent=2))
    {
      "type": "object",
      "properties": {
        "id": {
          "type": "string"
        },
        "foo__bar": {
          "type": "string"
        }
      }
    }

    >>> print(json.dumps(flatten_schema(schema, 2), indent=2))
    {
      "type": "object",
      "properties": {
        "id": {
          "type": "string"
        },
        "foo__bar__baz": {
          "type": "string"
        }
      }
    }

    >>> print(json.dumps(flatten_schema(schema, 3), indent=2))
    {
      "type": "object",
      "properties": {
        "id": {
          "type": "string"
        },
        "foo__bar__baz__qux": {
          "type": "string"
        }
      }
    }

    >>> nullable_leaves_schema = {
    ...     "type": "object",
    ...     "properties": {
    ...         "id": {
    ...             "type": "string"
    ...         },
    ...         "foo": {
    ...             "type": ["object", "null"],
    ...             "properties": {
    ...                 "bar": {
    ...                     "type": ["object", "null"],
    ...                     "properties": {
    ...                         "baz": {
    ...                             "type": ["object", "null"],
    ...                             "properties": {
    ...                                 "qux": {
    ...                                     "type": "string"
    ...                                 }
    ...                             }
    ...                         }
    ...                     }
    ...                 }
    ...             }
    ...         }
    ...     }
    ... }
    >>> print(json.dumps(flatten_schema(nullable_leaves_schema, 0), indent=2))
    {
      "type": "object",
      "properties": {
        "id": {
          "type": "string"
        },
        "foo": {
          "type": [
            "object",
            "null"
          ],
          "properties": {
            "bar": {
              "type": [
                "object",
                "null"
              ],
              "properties": {
                "baz": {
                  "type": [
                    "object",
                    "null"
                  ],
                  "properties": {
                    "qux": {
                      "type": "string"
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    >>> print(json.dumps(flatten_schema(nullable_leaves_schema, 1), indent=2))
    {
      "type": "object",
      "properties": {
        "id": {
          "type": "string"
        },
        "foo__bar": {
          "type": [
            "string",
            "null"
          ]
        }
      }
    }

    >>> print(json.dumps(flatten_schema(nullable_leaves_schema, 2), indent=2))
    {
      "type": "object",
      "properties": {
        "id": {
          "type": "string"
        },
        "foo__bar__baz": {
          "type": [
            "string",
            "null"
          ]
        }
      }
    }
    """
    new_schema = deepcopy(schema)
    new_schema["properties"] = _flatten_schema(
        schema_node=new_schema,
        max_level=max_level,
        separator=separator,
    )
    return new_schema


def _flatten_schema(  # noqa: C901, PLR0912
    schema_node: dict,
    parent_keys: list[str] | None = None,
    separator: str = "__",
    level: int = 0,
    max_level: int = 0,
) -> dict:
    """Flatten the provided schema node, recursively up to depth of `max_level`.

    Args:
        schema_node: The schema node to flatten.
        parent_keys: The parent's key, provided as a list of node names.
        separator: The string to use when concatenating key names.
        level: The current recursion level (zero-based).
        max_level: The max recursion level (zero-based, exclusive).

    Returns:
        A flattened version of the provided node.
    """
    if parent_keys is None:
        parent_keys = []

    items: list[tuple[str, dict]] = []
    if "properties" not in schema_node:
        return {}

    for field_name, field_schema in schema_node["properties"].items():
        new_key = flatten_key(field_name, parent_keys, separator)
        if "type" in field_schema:
            if (
                "object" in field_schema["type"]
                and "properties" in field_schema
                and level < max_level
            ):
                items.extend(
                    _flatten_schema(
                        field_schema,
                        [*parent_keys, field_name],
                        separator=separator,
                        level=level + 1,
                        max_level=max_level,
                    ).items(),
                )
            elif (
                "array" in field_schema["type"]
                or "object" in field_schema["type"]
                and max_level > 0
            ):
                types = (
                    ["string", "null"] if "null" in field_schema["type"] else "string"
                )
                items.append((new_key, {"type": types}))
            else:
                items.append((new_key, field_schema))
        # TODO: Figure out what this really does, try breaking it.
        # If it's not needed, remove it.
        elif len(field_schema.values()) > 0:
            if next(iter(field_schema.values()))[0]["type"] == "string":
                next(iter(field_schema.values()))[0]["type"] = ["null", "string"]
                items.append((new_key, next(iter(field_schema.values()))[0]))
            elif next(iter(field_schema.values()))[0]["type"] == "array":
                next(iter(field_schema.values()))[0]["type"] = ["null", "array"]
                items.append((new_key, next(iter(field_schema.values()))[0]))
            elif next(iter(field_schema.values()))[0]["type"] == "object":
                next(iter(field_schema.values()))[0]["type"] = ["null", "object"]
                items.append((new_key, next(iter(field_schema.values()))[0]))

    # Sort and check for duplicates
    def _key_func(item):
        return item[0]  # first item is tuple is the key name.

    sorted_items = sorted(items, key=_key_func)
    for field_name, g in itertools.groupby(sorted_items, key=_key_func):
        if len(list(g)) > 1:
            msg = f"Duplicate column name produced in schema: {field_name}"
            raise ValueError(msg)

    # Return the (unsorted) result as a dict.
    return dict(items)


def flatten_record(
    record: dict,
    flattened_schema: dict,
    max_level: int,
    separator: str = "__",
) -> dict:
    """Flatten a record up to max_level.

    Args:
        record: The record to flatten.
        flattened_schema: The already flattened schema.
        separator: The string used to separate concatenated key names. Defaults to "__".
        max_level: The maximum depth of keys to flatten recursively.

    Returns:
        A flattened version of the record.
    """
    return _flatten_record(
        record_node=record,
        flattened_schema=flattened_schema,
        separator=separator,
        max_level=max_level,
    )


def _flatten_record(
    record_node: t.MutableMapping[t.Any, t.Any],
    *,
    flattened_schema: dict | None = None,
    parent_key: list[str] | None = None,
    separator: str = "__",
    level: int = 0,
    max_level: int = 0,
) -> dict:
    """This recursive function flattens the record node.

    The current invocation is expected to be at `level` and will continue recursively
    until the provided `max_level` is reached.

    Args:
        record_node: The record node to flatten.
        flattened_schema: The already flattened full schema for the record.
        parent_key: The parent's key, provided as a list of node names.
        separator: The string to use when concatenating key names.
        level: The current recursion level (zero-based).
        max_level: The max recursion level (zero-based, exclusive).

    Returns:
        A flattened version of the provided node.
    """
    if parent_key is None:
        parent_key = []

    items: list[tuple[str, t.Any]] = []
    for k, v in record_node.items():
        new_key = flatten_key(k, parent_key, separator)
        if isinstance(v, collections.abc.MutableMapping) and level < max_level:
            items.extend(
                _flatten_record(
                    v,
                    flattened_schema=flattened_schema,
                    parent_key=[*parent_key, k],
                    separator=separator,
                    level=level + 1,
                    max_level=max_level,
                ).items(),
            )
        else:
            items.append(
                (
                    new_key,
                    json.dumps(v, use_decimal=True)
                    if _should_jsondump_value(k, v, flattened_schema)
                    else v,
                ),
            )

    return dict(items)


def _should_jsondump_value(key: str, value: t.Any, flattened_schema=None) -> bool:
    """Return True if json.dump() should be used to serialize the value.

    Args:
        key: [description]
        value: [description]
        flattened_schema: [description]. Defaults to None.

    Returns:
        [description]
    """
    if isinstance(value, (dict, list)):
        return True

    if (
        flattened_schema
        and key in flattened_schema
        and "type" in flattened_schema[key]
        and set(flattened_schema[key]["type"]) == {"null", "object", "array"}
    ):
        return True

    return False
