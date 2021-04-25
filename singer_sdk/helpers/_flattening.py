"""Internal helper library for record flatteting functions."""

import json
import re
import collections
import inflection
from typing import Optional


class RecordFlattener:
    """Flattens hierarchical records into 2-dimensional ones."""

    sep: str
    max_level: Optional[int]

    def __init__(self, sep: str = "__", max_level: int = None):
        """Initialize flattener."""
        self.sep = sep
        self.max_level = max_level

    def flatten_key(self, k, parent_key):
        """Return a flattened version of the key."""
        full_key = parent_key + [k]
        inflected_key = full_key.copy()
        reducer_index = 0
        while len(self.sep.join(inflected_key)) >= 255 and reducer_index < len(
            inflected_key
        ):
            reduced_key = re.sub(
                r"[a-z]", "", inflection.camelize(inflected_key[reducer_index])
            )
            inflected_key[reducer_index] = (
                reduced_key
                if len(reduced_key) > 1
                else inflected_key[reducer_index][0:3]
            ).lower()
            reducer_index += 1
        return self.sep.join(inflected_key)

    def flatten_record(self, d, flatten_schema=None, parent_key=[], level=0):
        """Return a flattened version of the record."""
        items = []
        for k, v in d.items():
            new_key = self._flatten_key(k, parent_key)
            if isinstance(v, collections.MutableMapping) and level < self.max_level:
                items.extend(
                    self._flatten_record(
                        v,
                        flatten_schema,
                        parent_key + [k],
                        sep=self.sep,
                        level=level + 1,
                    ).items()
                )
            else:
                items.append(
                    (
                        new_key,
                        json.dumps(v)
                        if self._should_json_dump_value(k, v, flatten_schema)
                        else v,
                    )
                )
        return dict(items)

    @staticmethod
    def _should_json_dump_value(key, value, flatten_schema=None) -> bool:
        if isinstance(value, (dict, list)):
            return True
        if (
            flatten_schema
            and key in flatten_schema
            and "type" in flatten_schema[key]
            and set(flatten_schema[key]["type"]) == {"null", "object", "array"}
        ):
            return True
        return False
