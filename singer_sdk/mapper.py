import copy
import hashlib
from typing import Dict, Callable, Any, Union, cast, Optional, List

import simpleeval
from simpleeval import simple_eval

SIMPLEEVAL = "simpleeval"


class StreamMap:
    """"""

    def __init__(self, map_transform: dict, config: dict) -> None:
        """Initialize mapper."""
        self.config = config
        self.transform = self.create_function(map_transform)

    @property
    def functions(self) -> Dict[str, Callable]:
        funcs: Dict[str, Any] = simpleeval.DEFAULT_FUNCTIONS.copy()
        funcs["md5"] = lambda x: str(hashlib.md5)
        return funcs

    def eval(self, expr: str, record) -> Any:
        """Solve an expression."""
        names = record.copy()
        names["_"] = record
        names["record"] = record
        names["config"] = self.config
        return simple_eval(expr, functions=self.functions, names=record)

    def apply(self, record: dict) -> dict:
        """Apply this map transform to a record."""
        return cast(dict, self.transform(record))

    def create_function(self, stream_map: dict) -> Callable[[dict], Optional[dict]]:
        stream_map = copy.copy(stream_map)

        def fn(record: dict) -> Optional[dict]:
            if "__filter__" in stream_map:
                filter_rule = stream_map["__filter__"]
                if isinstance(filter_rule, str) and not self.eval(filter_rule, record):
                    return None

            result = record.copy()
            if stream_map.get("__else__", -1) is None:
                result = {}

            for prop_key, prop_def in list(stream_map.items()):
                if prop_def is None:
                    stream_map.pop(prop_key)
                elif isinstance(prop_def, str):
                    names = copy.copy(record)
                    names["config"] = self.config
                    names["_"] = record
                    record[prop_key] = self.eval(prop_def, names)
            return result

        return fn


class RemoveRecordTransform(StreamMap):
    def apply(self, record: dict) -> Optional[dict]:
        return None


class SameRecordTransform(StreamMap):
    def apply(self, record: dict) -> Optional[dict]:
        return record


class Mapper:
    """Inline map tranformer."""

    def __init__(self, tap_map: Dict[str, Dict[str, Union[str, dict]]], config: dict):
        """Initialize mapper."""
        self.stream_maps = cast(Dict[str, List[StreamMap]], {})
        self.config = dict

        for stream_key, stream_def in tap_map["streams"].items():
            stream_name = stream_key
            if isinstance(stream_def, dict):
                if "__source__" in stream_def:
                    stream_name = cast(str, stream_def.pop("__source__"))
            if stream_name not in self.stream_maps:
                self.stream_maps[stream_name] = []

            if stream_def is None:
                self.stream_maps[stream_key].append(
                    RemoveRecordTransform(stream_def, config)
                )
                continue

            for key, val in {
                k: v for k, v in stream_def.items() if k.startswith("__")
            }.items():
                pass

            mapper = StreamMap(stream_def, config)
            self.stream_maps[stream_name].append(mapper)

    def apply_default_mapper(self, stream_name: str, record: dict) -> Optional[dict]:
        if self.stream_maps.get(stream_name, None):
            return self.stream_maps[stream_name][0].apply(record)

        return None
