"""Stream Mapper classes.

Mappers allow inline stream transformation, filtering, aliasing, and duplication.
"""

import abc
import copy
import hashlib
import logging
from typing import Dict, Callable, Any, Tuple, Type, Union, cast, Optional, List

from singer import Catalog, CatalogEntry

import simpleeval
from simpleeval import simple_eval

from singer_sdk.helpers._catalog import get_selected_schema
from singer_sdk.typing import (
    IntegerType,
    JSONTypeHelper,
    NumberType,
    PropertiesList,
    Property,
    StringType,
)

SIMPLEEVAL = "simpleeval"
MAPPER_ELSE_OPTION = "__else__"
MAPPER_FILTER_OPTION = "__filter__"
MAPPER_SOURCE_OPTION = "__source__"
MAPPER_ALIAS_OPTION = "__alias__"


def md5(input: str) -> str:
    """Return the md5 as a string. This is a function for inline calculations."""
    return hashlib.md5(input.encode("utf-8")).hexdigest()


class StreamMap(metaclass=abc.ABCMeta):
    """Abstract base class for all map classes."""

    def __init__(self, stream_alias: str, raw_schema: dict) -> None:
        """Initialize mapper."""
        self.stream_alias = stream_alias
        self.raw_schema = raw_schema
        self.transformed_schema = raw_schema

    @abc.abstractmethod
    def transform(self, record: dict) -> Optional[dict]:
        """Transform a record and return the result."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_filter_result(self, record: dict) -> bool:
        """Return True to include the record or False to exclude."""
        raise NotImplementedError


class DefaultStreamMap(StreamMap):
    """Abstract base class for default maps which do not require custom config."""


class RemoveRecordTransform(DefaultStreamMap):
    """Default mapper which simply excludes any records."""

    def transform(self, record: dict) -> Optional[dict]:
        """Return None (always exclude)."""
        _ = record  # Drop the record
        return None

    def get_filter_result(self, record: dict) -> bool:
        """Return False (always exclude)."""
        return False


class SameRecordTransform(DefaultStreamMap):
    """Default mapper which simply returns the original records."""

    def transform(self, record: dict) -> Optional[dict]:
        """Return original record unchanged."""
        return record

    def get_filter_result(self, record: dict) -> bool:
        """Return True (always include)."""
        return True


class CustomStreamMap(StreamMap):
    """Defines transformation logic for a singer stream map."""

    def __init__(
        self,
        alias: str,
        map_config: dict,
        raw_schema: dict,
        map_transform: dict,
    ) -> None:
        """Initialize mapper."""
        super().__init__(stream_alias=alias, raw_schema=raw_schema)

        self.map_config = map_config
        self._transform_fn: Callable[[dict], Optional[dict]]
        self._filter_fn: Callable[[dict], bool]
        (
            self._filter_fn,
            self._transform_fn,
            self.transformed_schema,
        ) = self._init_functions_and_schema(
            stream_map=map_transform, original_schema=raw_schema
        )

    def transform(self, record: dict) -> Optional[dict]:
        """Return a transformed record."""
        return self._transform_fn(record)

    def get_filter_result(self, record: dict) -> bool:
        """Return True to include or False to exclude."""
        return self._filter_fn(record)

    @property
    def functions(self) -> Dict[str, Callable]:
        """Return the functions which should be available for expression evaluation."""
        funcs: Dict[str, Any] = simpleeval.DEFAULT_FUNCTIONS.copy()
        funcs["md5"] = md5
        return funcs

    def _eval(self, expr: str, record: dict, property_name: Optional[str]) -> Any:
        """Solve an expression."""
        names = record.copy()  # Start with names from record properties
        names["_"] = record  # Add a shorthand alias in case of reserved words in names
        names["record"] = record  # ...and a longhand alias
        names["config"] = self.map_config  # Allow map config access within transform
        if property_name and property_name in record:
            # Allow access to original property value if applicable
            names["self"] = record[property_name]
        result = simple_eval(expr, functions=self.functions, names=names)
        logging.info(f"Eval result: {expr} = {result}")
        return result

    def _eval_type(self, expr: str) -> JSONTypeHelper:
        """Evaluate an expression's type."""
        assert expr is not None, "Expression should be str, not None"

        if expr.startswith("float("):
            return NumberType()

        if expr.startswith("int("):
            return IntegerType()

        return StringType()

    def _init_functions_and_schema(
        self, stream_map: dict, original_schema: dict
    ) -> Tuple[Callable[[dict], bool], Callable[[dict], Optional[dict]], dict]:
        """Return a tuple: filter_fn, transform_fn, transformed_schema."""
        stream_map = copy.copy(stream_map)

        filter_rule: Optional[str] = None
        include_by_default = True
        if stream_map and MAPPER_FILTER_OPTION in stream_map:
            filter_rule = stream_map.pop(MAPPER_FILTER_OPTION)
            logging.info(f"Found filter rule: {filter_rule}")

        if stream_map and MAPPER_ELSE_OPTION in stream_map:
            if stream_map[MAPPER_ELSE_OPTION] is None:
                logging.info(
                    f"Detected `{MAPPER_ELSE_OPTION}=None` rule. "
                    "Unmapped properties will be excluded from output."
                )
                include_by_default = False
            else:
                raise NotImplementedError(
                    f"Option '{MAPPER_ELSE_OPTION}={stream_map[MAPPER_ELSE_OPTION]}' "
                    "is not supported."
                )
            stream_map.pop(MAPPER_ELSE_OPTION)

        # Transform the schema as needed

        transformed_schema = copy.copy(original_schema)
        if not include_by_default:
            transformed_schema = PropertiesList().to_dict()
        if "properties" not in transformed_schema:
            transformed_schema["properties"] = {}

        for prop_key, prop_def in list(stream_map.items()):
            if prop_def is None:
                stream_map.pop(prop_key)
            elif (
                isinstance(prop_def, str)
                and prop_key not in transformed_schema["properties"]
            ):
                transformed_schema["properties"].update(
                    Property(prop_key, self._eval_type(prop_def)).to_dict()
                )

        # Declare function variables

        def eval_filter(record: dict) -> bool:
            nonlocal filter_rule

            filter_result = self._eval(
                expr=cast(str, filter_rule), record=record, property_name=None
            )
            logging.debug(f"Filter result: {filter_result}")
            if not filter_result:
                logging.debug("Excluding record due to filter.")
                return False

            return True

        def always_true(record: dict) -> bool:
            _ = record
            return True

        if isinstance(filter_rule, str):
            filter_fn = eval_filter
        elif filter_rule is None:
            filter_fn = always_true
        else:
            raise ValueError(
                f"Unexpected filter rule type '{type(filter_rule).__name__}' in "
                f"expression {str(filter_rule)}. Expected 'str' or 'None'."
            )

        def transform_fn(record: dict) -> Optional[dict]:
            nonlocal include_by_default, stream_map

            if not self.get_filter_result(record):
                return None

            if include_by_default:
                result = record.copy()
            else:
                result = {}

            for prop_key, prop_def in list(stream_map.items()):
                if prop_def is None:
                    # Remove property from result
                    result.pop(prop_key, None)
                    continue

                if isinstance(prop_def, str):
                    # Apply property transform
                    result[prop_key] = self._eval(
                        expr=prop_def, record=record, property_name=prop_key
                    )
                    continue

                raise ValueError(
                    f"Unexpected mapping type '{type(prop_def).__name__}' in "
                    f"map expression '{prop_def}'. Expected 'str' or 'None'."
                )

            return result

        return filter_fn, transform_fn, transformed_schema


class Mapper:
    """Inline map tranformer."""

    def __init__(
        self,
        tap_map: Dict[str, Dict[str, Union[str, dict]]],
        map_config: dict,
        raw_catalog: dict,
        logger: logging.Logger,
    ):
        """Initialize mapper."""
        self.stream_maps = cast(Dict[str, List[StreamMap]], {})
        self.map_config = map_config
        self.raw_catalog = raw_catalog
        self.raw_catalog_obj = Catalog.from_dict(raw_catalog)
        self.default_mapper_type: Type[DefaultStreamMap]
        self.logger = logger

        if MAPPER_ELSE_OPTION in tap_map["streams"]:
            if tap_map["streams"][MAPPER_ELSE_OPTION] is None:
                logging.info(
                    f"Found '{MAPPER_ELSE_OPTION}=None' default mapper. "
                    "Unmapped streams will be excluded from output."
                )
                self.default_mapper_type = RemoveRecordTransform
                tap_map["streams"].pop(MAPPER_ELSE_OPTION)
            else:
                raise RuntimeError(
                    f"Undefined transform for '{MAPPER_ELSE_OPTION}'' case: "
                    f"{tap_map[MAPPER_ELSE_OPTION]}"
                )
        else:
            logging.info(
                "Operator '{MAPPER_ELSE_OPTION}=None' was not found. "
                "Unmapped streams will be included in output."
            )
            self.default_mapper_type = SameRecordTransform

        for catalog_entry in self.raw_catalog_obj.streams:
            if catalog_entry.stream not in self.stream_maps:
                # The 0th mapper should be the same-named treatment.
                # Additional items may be added for aliasing or multi projections.
                self.stream_maps[catalog_entry.stream] = [
                    self.default_mapper_type(
                        catalog_entry.stream,
                        self.get_original_stream_schema(catalog_entry.stream),
                    )
                ]

        for stream_key, stream_def in tap_map["streams"].items():
            stream_name = stream_key
            stream_alias = stream_key
            logging.info(stream_name)

            if stream_key.startswith("__"):
                raise NotImplementedError(
                    f"Option '{stream_key}:{stream_def}' is not expected."
                )

            if stream_def is None:
                self.stream_maps[stream_key].append(
                    RemoveRecordTransform(
                        stream_alias=stream_key,
                        raw_schema=self.get_original_stream_schema(stream_key),
                    )
                )
                logging.info(f"Add null tansform for {stream_name}")
                continue

            if isinstance(stream_def, str):
                # Handle expected cases

                raise NotImplementedError(
                    f"Option '{stream_key}:{stream_def}' is not expected."
                )

            if MAPPER_SOURCE_OPTION in stream_def:
                stream_name = cast(str, stream_def.pop(MAPPER_SOURCE_OPTION))

            if MAPPER_ALIAS_OPTION in stream_def:
                stream_alias = cast(str, stream_def.pop(MAPPER_ALIAS_OPTION))

            mapper = CustomStreamMap(
                alias=stream_alias,
                map_transform=stream_def,
                map_config=self.map_config,
                raw_schema=self.get_original_stream_schema(stream_name),
            )
            if stream_name == stream_key:
                # Zero-th mapper should be the same-named mapper:
                self.stream_maps[stream_name][0] = mapper
            else:
                # Additional mappers for aliasing and multi-projection:
                self.stream_maps[stream_name].append(mapper)

    def get_primary_mapper(self, stream_name: str) -> StreamMap:
        """Return the primary mapper for the specified stream name."""
        if stream_name not in self.stream_maps:
            raise RuntimeError(f"No mapper found for {stream_name}. ")

        return self.stream_maps[stream_name][0]

    def get_original_stream_schema(
        self, stream_name: str, with_selection_filter: bool = True
    ) -> dict:
        """Return the unchanged schema definition for the specified stream name."""
        if with_selection_filter:
            return get_selected_schema(self.raw_catalog, stream_name, self.logger)

        catalog = Catalog.from_dict(self.raw_catalog)
        catalog_entry: CatalogEntry = catalog.get_stream(stream_name)
        return cast(dict, catalog_entry.schema.to_dict())

    def apply_primary_mapper(self, stream_name: str, record: dict) -> Optional[dict]:
        """Apply the primary mapper for the stream and return the result."""
        return self.get_primary_mapper(stream_name).transform(record)
