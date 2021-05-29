import copy
import hashlib
import logging
from typing import Dict, Callable, Any, Union, cast, Optional, List

import simpleeval
from simpleeval import simple_eval

SIMPLEEVAL = "simpleeval"


def md5(input: str) -> str:
    return hashlib.md5(input.encode("utf-8")).hexdigest()


class StreamMap:
    """"""

    def __init__(self, map_transform: dict, config: dict) -> None:
        """Initialize mapper."""
        self.config = config
        self.transform = self.create_function(map_transform)

    @property
    def functions(self) -> Dict[str, Callable]:
        funcs: Dict[str, Any] = simpleeval.DEFAULT_FUNCTIONS.copy()
        funcs["md5"] = md5
        return funcs

    def eval(self, expr: str, record) -> Any:
        """Solve an expression."""
        names = record.copy()
        names["_"] = record
        names["record"] = record
        names["config"] = self.config
        result = simple_eval(expr, functions=self.functions, names=record)
        logging.info(f"Eval result: {expr} = {result}")
        return result

    def apply(self, record: dict) -> dict:
        """Apply this map transform to a record."""
        return cast(dict, self.transform(record))

    def create_function(self, stream_map: dict) -> Callable[[dict], Optional[dict]]:
        stream_map = copy.copy(stream_map)

        filter_rule: Optional[str] = None
        include_by_default = True
        if stream_map and "__filter__" in stream_map:
            filter_rule = stream_map.pop("__filter__")
            logging.info(f"Found filter rule: {filter_rule}")

        if stream_map and "__else__" in stream_map:
            if stream_map["__else__"] is None:
                logging.info(
                    "Detected `__else__=None` rule. "
                    "Only explicitly mapped properties will be included in output."
                )
                include_by_default = False
            else:
                raise NotImplementedError(
                    f"Option '{stream_map['__else__']}' is not supported for __else__"
                )
            stream_map.pop("__else__")

        def fn(record: dict) -> Optional[dict]:
            if isinstance(filter_rule, str):
                filter_result = self.eval(filter_rule, record)
                logging.info(f"Filter result: {filter_result}")
                if not filter_result:
                    logging.info(f"Excluding record due to filter.")
                    return None

            if include_by_default:
                result = record.copy()
            else:
                result = {}

            for prop_key, prop_def in list(stream_map.items()):
                if prop_def is None:
                    stream_map.pop(prop_key)
                elif isinstance(prop_def, str):
                    names = copy.copy(record)
                    names["config"] = self.config
                    names["_"] = record
                    result[prop_key] = self.eval(prop_def, names)
            return result

        return fn


class RemoveRecordTransform(StreamMap):
    def __init__(self, config: dict) -> None:
        """Initialize mapper."""
        self.config = config

    def apply(self, record: dict) -> Optional[dict]:
        return None


class SameRecordTransform(StreamMap):
    def __init__(self, config: dict) -> None:
        """Initialize mapper."""
        self.config = config

    def apply(self, record: dict) -> Optional[dict]:
        return record


class Mapper:
    """Inline map tranformer."""

    def __init__(self, tap_map: Dict[str, Dict[str, Union[str, dict]]], config: dict):
        """Initialize mapper."""
        self.stream_maps = cast(Dict[str, List[StreamMap]], {})
        self.config = dict
        self.default_mapper: StreamMap = SameRecordTransform(config)

        if "__else__" in tap_map["streams"]:
            if tap_map["streams"]["__else__"] is None:
                self.default_mapper = RemoveRecordTransform(config)
                tap_map["streams"].pop("__else__")
            else:
                raise RuntimeError(
                    f"Undefined transform for '__else__' case: {tap_map['__else__']}"
                )

        for stream_key, stream_def in tap_map["streams"].items():
            stream_name = stream_key
            logging.info(stream_name)

            if stream_key.startswith("__"):
                raise NotImplementedError(
                    f"Option '{stream_key}:{stream_def}' is not expected."
                )

            if stream_name not in self.stream_maps:
                self.stream_maps[stream_name] = []

            if stream_def is None:
                self.stream_maps[stream_key].append(RemoveRecordTransform(config))
                logging.info(f"Add null tansform for {stream_name}")
                continue

            if isinstance(stream_def, str):
                # Handle expected cases

                raise NotImplementedError(
                    f"Option '{stream_key}:{stream_def}' is not expected."
                )

            if "__source__" in stream_def:
                stream_name = cast(str, stream_def.pop("__source__"))

            if stream_name not in self.stream_maps:
                self.stream_maps[stream_name] = []

            mapper = StreamMap(stream_def, config)
            self.stream_maps[stream_name].append(mapper)

    def apply_default_mapper(self, stream_name: str, record: dict) -> Optional[dict]:
        if stream_name not in self.stream_maps:
            return self.default_mapper.apply(record)

        if self.stream_maps.get(stream_name, None):
            return self.stream_maps[stream_name][0].apply(record)

        return None
