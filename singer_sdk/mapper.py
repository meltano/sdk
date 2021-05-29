import copy
import hashlib
import logging
from typing import Dict, Callable, Any, Tuple, Type, Union, cast, Optional, List

from singer import Catalog, CatalogEntry

import simpleeval
from simpleeval import simple_eval
from singer_sdk.typing import (
    IntegerType,
    JSONTypeHelper,
    NumberType,
    PropertiesList,
    Property,
    StringType,
)

SIMPLEEVAL = "simpleeval"


def md5(input: str) -> str:
    return hashlib.md5(input.encode("utf-8")).hexdigest()


class StreamMap:
    """"""

    def __init__(self, map_transform: dict, config: dict, raw_schema: dict) -> None:
        """Initialize mapper."""
        self.config = config
        self._transform_fn: Callable[[dict], Optional[dict]]
        self._transform_fn, self.schema = self._init_transform_and_schema(
            map_transform, raw_schema
        )

    def transform(self, record: dict) -> Optional[dict]:
        return self._transform_fn(record)

    @property
    def functions(self) -> Dict[str, Callable]:
        funcs: Dict[str, Any] = simpleeval.DEFAULT_FUNCTIONS.copy()
        funcs["md5"] = md5
        return funcs

    def _eval(self, expr: str, record) -> Any:
        """Solve an expression."""
        names = record.copy()
        names["_"] = record
        names["record"] = record
        names["config"] = self.config
        result = simple_eval(expr, functions=self.functions, names=record)
        logging.info(f"Eval result: {expr} = {result}")
        return result

    def _eval_type(self, expr: str) -> JSONTypeHelper:
        """Solve an expression."""
        assert expr is not None, "Expression should be str, not None"

        if expr.startswith("float("):
            return NumberType()

        if expr.startswith("int("):
            return IntegerType()

        return StringType()

    def _init_transform_and_schema(
        self, stream_map: dict, original_schema: dict
    ) -> Tuple[Callable[[dict], Optional[dict]], dict]:
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
                    "Unmapped properties will be excluded from output."
                )
                include_by_default = False
            else:
                raise NotImplementedError(
                    f"Option '{stream_map['__else__']}' is not supported for __else__"
                )
            stream_map.pop("__else__")

        if not include_by_default:
            transformed_properties_list = PropertiesList()

        for prop_key, prop_def in list(stream_map.items()):
            if prop_def is None:
                stream_map.pop(prop_key)
            elif isinstance(prop_def, str):
                transformed_properties_list.append(
                    Property(prop_key, self._eval_type(prop_def))
                )

        def fn(record: dict) -> Optional[dict]:
            if isinstance(filter_rule, str):
                filter_result = self._eval(filter_rule, record)
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
                    result[prop_key] = self._eval(prop_def, names)
            return result

        return fn, transformed_properties_list.to_dict()


class RemoveRecordTransform(StreamMap):
    def __init__(self, config: dict, raw_schema: dict) -> None:
        """Initialize mapper."""
        _ = raw_schema
        self.config = config

    def transform(self, record: dict) -> Optional[dict]:
        _ = record  # Drop the record
        return None


class SameRecordTransform(StreamMap):
    def __init__(self, config: dict, raw_schema) -> None:
        """Initialize mapper."""
        self.config = config
        self.schema = raw_schema

    def transform(self, record: dict) -> Optional[dict]:
        return record


class Mapper:
    """Inline map tranformer."""

    def __init__(
        self,
        tap_map: Dict[str, Dict[str, Union[str, dict]]],
        config: dict,
        raw_catalog: dict,
    ):
        """Initialize mapper."""
        self.stream_maps = cast(Dict[str, List[StreamMap]], {})
        self.config = dict
        self.raw_catalog = raw_catalog
        self.raw_catalog_obj = Catalog.from_dict(raw_catalog)
        self.default_mapper_type: Type[StreamMap]

        if "__else__" in tap_map["streams"]:
            if tap_map["streams"]["__else__"] is None:
                logging.info(
                    "Found '__else__=None' default mapper. "
                    "Unmapped streams will be excluded from output."
                )
                self.default_mapper_type = RemoveRecordTransform
                tap_map["streams"].pop("__else__")
            else:
                raise RuntimeError(
                    f"Undefined transform for '__else__' case: {tap_map['__else__']}"
                )
        else:
            logging.info(
                "Operator '__else__=None' was not found. "
                "Unmapped streams will be included in output."
            )
            self.default_mapper_type = SameRecordTransform

        for catalog_entry in self.raw_catalog_obj.streams:
            if catalog_entry.stream not in self.stream_maps:
                # The 0th mapper should be the same-named treatment.
                # Additional items may be added for aliasing or multi projections.
                self.stream_maps[catalog_entry.stream] = [
                    self.default_mapper_type(
                        self.config, catalog_entry.schema.to_dict()
                    )
                ]

        for stream_key, stream_def in tap_map["streams"].items():
            stream_name = stream_key
            logging.info(stream_name)

            if stream_key.startswith("__"):
                raise NotImplementedError(
                    f"Option '{stream_key}:{stream_def}' is not expected."
                )

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

            mapper = StreamMap(
                stream_def, config, self.get_original_stream_schema(stream_name)
            )
            if stream_name == stream_key:
                # Zero-th mapper should be the same-named mapper:
                self.stream_maps[stream_name][0] = mapper
            else:
                # Additional mappers for aliasing and multi-projection:
                self.stream_maps[stream_name].append(mapper)

    def get_default_mapper(self, stream_name: str) -> StreamMap:
        if stream_name not in self.stream_maps:
            raise RuntimeError(f"No mapper found for {stream_name}. ")

        return self.stream_maps[stream_name][0]

    def get_original_stream_schema(self, stream_name) -> dict:
        catalog = Catalog.from_dict(self.raw_catalog)
        catalog_entry: CatalogEntry = catalog.get_stream(stream_name)
        return catalog_entry.schema.to_dict()

    def apply_default_mapper(self, stream_name: str, record: dict) -> Optional[dict]:
        return self.get_default_mapper(stream_name).transform(record)
