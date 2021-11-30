"""Stream Mapper classes.

Mappers allow inline stream transformation, filtering, aliasing, and duplication.
"""

import abc
import copy
import hashlib
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union, cast

from singer_sdk.exceptions import MapExpressionError, StreamMapConfigError
from singer_sdk.helpers import _simpleeval as simpleeval
from singer_sdk.helpers._catalog import get_selected_schema
from singer_sdk.helpers._singer import Catalog
from singer_sdk.typing import (
    CustomType,
    IntegerType,
    JSONTypeHelper,
    NumberType,
    PropertiesList,
    Property,
    StringType,
)

MAPPER_ELSE_OPTION = "__else__"
MAPPER_FILTER_OPTION = "__filter__"
MAPPER_SOURCE_OPTION = "__source__"
MAPPER_ALIAS_OPTION = "__alias__"
MAPPER_KEY_PROPERTIES_OPTION = "__key_properties__"


def md5(input: str) -> str:
    """Digest a string using MD5. This is a function for inline calculations.

    Args:
        input: String to digest.

    Returns:
        A string digested into MD5.
    """
    return hashlib.md5(input.encode("utf-8")).hexdigest()


class StreamMap(metaclass=abc.ABCMeta):
    """Abstract base class for all map classes."""

    def __init__(
        self, stream_alias: str, raw_schema: dict, key_properties: Optional[List[str]]
    ) -> None:
        """Initialize mapper.

        Args:
            stream_alias: Stream name.
            raw_schema: Original strem JSON schema.
            key_properties: Primary key of the source stream.
        """
        self.stream_alias = stream_alias
        self.raw_schema = raw_schema
        self.raw_key_properties = key_properties
        self.transformed_schema = raw_schema
        self.transformed_key_properties = key_properties

    @abc.abstractmethod
    def transform(self, record: dict) -> Optional[dict]:
        """Transform a record and return the result.

        Args:
            record: An individual record dictionary in a stream.

        Return:
            A new dictionary representing a transformed record.

        Raises:
            NotImplementedError: If the derived class doesn't override this method.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_filter_result(self, record: dict) -> bool:
        """Exclude records from a stream.

        Args:
            record: An individual record dictionary in a stream.

        Return:
            True to include the record or False to exclude.

        Raises:
            NotImplementedError: If the derived class doesn't override this method.
        """
        raise NotImplementedError


class DefaultStreamMap(StreamMap):
    """Abstract base class for default maps which do not require custom config."""


class RemoveRecordTransform(DefaultStreamMap):
    """Default mapper which simply excludes any records."""

    def transform(self, record: dict) -> None:
        """Return None (always exclude).

        Args:
            record: An individual record dictionary in a stream.

        Returns:
            None
        """
        _ = record  # Drop the record
        return None

    def get_filter_result(self, record: dict) -> bool:
        """Exclude all records.

        Args:
            record: An individual record dictionary in a stream.

        Returns:
            Always `False`.
        """
        return False


class SameRecordTransform(DefaultStreamMap):
    """Default mapper which simply returns the original records."""

    def transform(self, record: dict) -> dict:
        """Return original record unchanged.

        Args:
            record: An individual record dictionary in a stream.

        Returns:
            The original record unchanged.
        """
        return record

    def get_filter_result(self, record: dict) -> bool:
        """Return True (always include).

        Args:
            record: An individual record dictionary in a stream.

        Returns:
            Always `True`.
        """
        return True


class CustomStreamMap(StreamMap):
    """Defines transformation logic for a singer stream map."""

    def __init__(
        self,
        stream_alias: str,
        map_config: dict,
        raw_schema: dict,
        key_properties: Optional[List[str]],
        map_transform: dict,
    ) -> None:
        """Initialize mapper.

        Args:
            stream_alias: Stream name.
            map_config: Stream map configuration.
            raw_schema: Original strem JSON schema.
            key_properties: Primary key of the source stream.
            map_transform: Dictionary of transformations to apply to the stream.
        """
        super().__init__(
            stream_alias=stream_alias,
            raw_schema=raw_schema,
            key_properties=key_properties,
        )

        self.map_config = map_config
        self._transform_fn: Callable[[dict], Optional[dict]]
        self._filter_fn: Callable[[dict], bool]
        (
            self._filter_fn,
            self._transform_fn,
            self.transformed_schema,
        ) = self._init_functions_and_schema(stream_map=map_transform)

    def transform(self, record: dict) -> Optional[dict]:
        """Return a transformed record.

        Args:
            record: An individual record dictionary in a stream.

        Returns:
            The transformed record.
        """
        return self._transform_fn(record)

    def get_filter_result(self, record: dict) -> bool:
        """Return True to include or False to exclude.

        Args:
            record: An individual record dictionary in a stream.

        Returns:
            Boolean flag for record selection.
        """
        return self._filter_fn(record)

    @property
    def functions(self) -> Dict[str, Callable]:
        """Get avaibale transformation functions.

        Returns:
            Functions which should be available for expression evaluation.
        """
        funcs: Dict[str, Any] = simpleeval.DEFAULT_FUNCTIONS.copy()
        funcs["md5"] = md5
        return funcs

    def _eval(self, expr: str, record: dict, property_name: Optional[str]) -> Any:
        """Solve an expression.

        Args:
            expr: String expression to evaluate.
            record: Individual stream record.
            property_name: Name of property to transform in the record.

        Returns:
            Evaluated expression.

        Raises:
            MapExpressionError: If the mapping expression failed to evaluate.
        """
        names = record.copy()  # Start with names from record properties
        names["_"] = record  # Add a shorthand alias in case of reserved words in names
        names["record"] = record  # ...and a longhand alias
        names["config"] = self.map_config  # Allow map config access within transform
        if property_name and property_name in record:
            # Allow access to original property value if applicable
            names["self"] = record[property_name]
        try:
            result = simpleeval.simple_eval(expr, functions=self.functions, names=names)
            logging.debug(f"Eval result: {expr} = {result}")
        except Exception as ex:
            raise MapExpressionError(
                f"Failed to evaluate simpleeval expressions {expr}."
            ) from ex
        return result

    def _eval_type(
        self, expr: str, default: Optional[JSONTypeHelper] = None
    ) -> JSONTypeHelper:
        """Evaluate an expression's type.

        Args:
            expr: String expression to evaluate.
            default: TODO.

        Returns:
            TODO
        """
        assert expr is not None, "Expression should be str, not None"

        default = default or StringType()

        if expr.startswith("float("):
            return NumberType()

        if expr.startswith("int("):
            return IntegerType()

        if expr.startswith("str("):
            return StringType()

        if expr[0] == "'" and expr[-1] == "'":
            return StringType()

        return default

    def _init_functions_and_schema(
        self, stream_map: dict
    ) -> Tuple[Callable[[dict], bool], Callable[[dict], Optional[dict]], dict]:
        """Return a tuple: filter_fn, transform_fn, transformed_schema.

        Args:
            stream_map: TODO

        Returns:
            TODO.

        Raises:
            NotImplementedError: TODO
            StreamMapConfigError: TODO
        """
        stream_map = copy.copy(stream_map)

        filter_rule: Optional[str] = None
        include_by_default = True
        if stream_map and MAPPER_FILTER_OPTION in stream_map:
            filter_rule = stream_map.pop(MAPPER_FILTER_OPTION)
            logging.info(f"Found '{self.stream_alias}' filter rule: {filter_rule}")

        if stream_map and MAPPER_KEY_PROPERTIES_OPTION in stream_map:
            self.transformed_key_properties: List[str] = stream_map.pop(
                MAPPER_KEY_PROPERTIES_OPTION
            )
            logging.info(
                f"Found stream map override for '{self.stream_alias}' key properties: "
                f"{str(self.transformed_key_properties)}"
            )

        if stream_map and MAPPER_ELSE_OPTION in stream_map:
            if stream_map[MAPPER_ELSE_OPTION] is None:
                logging.info(
                    f"Detected `{MAPPER_ELSE_OPTION}=None` rule. "
                    "Unmapped, non-key properties will be excluded from output."
                )
                include_by_default = False
            else:
                raise NotImplementedError(
                    f"Option '{MAPPER_ELSE_OPTION}={stream_map[MAPPER_ELSE_OPTION]}' "
                    "is not supported."
                )
            stream_map.pop(MAPPER_ELSE_OPTION)

        # Transform the schema as needed

        transformed_schema = copy.copy(self.raw_schema)
        if not include_by_default:
            # Start with only the defined (or transformed) key properties
            transformed_schema = PropertiesList().to_dict()
            for key_property in self.transformed_key_properties or []:
                transformed_schema["properties"][key_property] = self.raw_schema[
                    "properties"
                ][key_property]

        if "properties" not in transformed_schema:
            transformed_schema["properties"] = {}

        for prop_key, prop_def in list(stream_map.items()):
            if prop_def is None:
                if prop_key in (self.transformed_key_properties or []):
                    raise StreamMapConfigError(
                        f"Removing key property '{prop_key}' is not permitted in "
                        f"'{self.stream_alias}' stream map config. To remove a key "
                        "property, use the `__key_properties__` operator "
                        "to specify either a new list of key property names or `null` "
                        "to replicate with no key properties in the stream."
                    )

                stream_map.pop(prop_key)
            elif isinstance(prop_def, str):
                default_type: JSONTypeHelper = StringType()  # Fallback to string
                existing_schema: dict = transformed_schema["properties"].get(
                    prop_key, {}
                )
                if existing_schema:
                    # Set default type if property exists already in JSON Schema
                    default_type = CustomType(existing_schema)

                transformed_schema["properties"].update(
                    Property(
                        prop_key, self._eval_type(prop_def, default=default_type)
                    ).to_dict()
                )
            else:
                raise StreamMapConfigError(
                    f"Unexpected type '{type(prop_def).__name__}' in stream map "
                    f"for '{self.stream_alias}:{prop_key}'."
                )

        # Declare function variables

        def eval_filter(record: dict) -> bool:
            nonlocal filter_rule

            filter_result = self._eval(
                expr=cast(str, filter_rule), record=record, property_name=None
            )
            logging.debug(
                f"Filter result for '{filter_rule}' "
                "in '{self.name}' stream: {filter_result}"
            )
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
            raise StreamMapConfigError(
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
                # Start with only the defined (or transformed) key properties
                result = {}
                for key_property in self.transformed_key_properties or []:
                    if key_property in record:
                        result[key_property] = record[key_property]

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

                raise StreamMapConfigError(
                    f"Unexpected mapping type '{type(prop_def).__name__}' in "
                    f"map expression '{prop_def}'. Expected 'str' or 'None'."
                )

            return result

        return filter_fn, transform_fn, transformed_schema


class PluginMapper:
    """Inline map tranformer."""

    def __init__(
        self,
        plugin_config: Dict[str, Dict[str, Union[str, dict]]],
        logger: logging.Logger,
    ) -> None:
        """Initialize mapper.

        Args:
            plugin_config: TODO
            logger: TODO

        Raises:
            StreamMapConfigError: TODO
        """
        self.stream_maps = cast(Dict[str, List[StreamMap]], {})
        self.map_config = plugin_config.get("stream_map_config", {})
        self.default_mapper_type: Type[DefaultStreamMap] = SameRecordTransform
        self.logger = logger

        self.stream_maps_dict = cast(
            Dict[str, Union[str, dict, None]], plugin_config.get("stream_maps", {})
        )
        if MAPPER_ELSE_OPTION in self.stream_maps_dict:
            if self.stream_maps_dict[MAPPER_ELSE_OPTION] is None:
                logging.info(
                    f"Found '{MAPPER_ELSE_OPTION}=None' default mapper. "
                    "Unmapped streams will be excluded from output."
                )
                self.default_mapper_type = RemoveRecordTransform
                self.stream_maps_dict.pop(MAPPER_ELSE_OPTION)
            else:
                raise StreamMapConfigError(
                    f"Undefined transform for '{MAPPER_ELSE_OPTION}'' case: "
                    f"{self.stream_maps_dict[MAPPER_ELSE_OPTION]}"
                )
        else:
            logging.info(
                f"Operator '{MAPPER_ELSE_OPTION}=None' was not found. "
                "Unmapped streams will be included in output."
            )
        for stream_map_key, stream_def in self.stream_maps_dict.items():
            if stream_map_key.startswith("__"):
                raise StreamMapConfigError(
                    f"Option '{stream_map_key}:{stream_def}' is not expected."
                )

    def register_raw_streams_from_catalog(self, catalog: Catalog) -> None:
        """Register all streams as described in the catalog dict.

        Args:
            catalog: TODO
        """
        for catalog_entry in catalog.streams:
            self.register_raw_stream_schema(
                catalog_entry.stream or catalog_entry.tap_stream_id,
                get_selected_schema(
                    catalog_entry.stream or catalog_entry.tap_stream_id,
                    catalog_entry.schema.to_dict(),
                    catalog_entry.metadata.resolve_selection(),
                    self.logger,
                ),
                catalog_entry.key_properties,
            )

    def register_raw_stream_schema(
        self, stream_name: str, schema: dict, key_properties: Optional[List[str]]
    ) -> None:
        """Register a new stream as described by its name and schema.

        Args:
            stream_name: TODO
            schema: TODO
            key_properties: TODO

        Raises:
            StreamMapConfigError: TODO
        """
        if stream_name not in self.stream_maps:
            # The 0th mapper should be the same-named treatment.
            # Additional items may be added for aliasing or multi projections.
            self.stream_maps[stream_name] = [
                self.default_mapper_type(stream_name, schema, key_properties)
            ]

        for stream_map_key, stream_def in self.stream_maps_dict.items():
            stream_alias: str = stream_map_key
            if isinstance(stream_def, str):
                if stream_name == stream_map_key:
                    # TODO: Add any expected cases for str expressions (currently none)
                    pass

                raise StreamMapConfigError(
                    f"Option '{stream_map_key}:{stream_def}' is not expected."
                )

            if stream_def is None:
                if stream_name != stream_map_key:
                    continue

                self.stream_maps[stream_map_key][0] = RemoveRecordTransform(
                    stream_alias=stream_map_key,
                    raw_schema=schema,
                    key_properties=None,
                )
                logging.info(f"Set null tansform as default for '{stream_name}'")
                continue

            if not isinstance(stream_def, dict):
                raise StreamMapConfigError(
                    "Unexpected stream definition type. Expected str, dict, or None. "
                    f"Got '{type(stream_def).__name__}'."
                )

            if MAPPER_SOURCE_OPTION in stream_def:
                if stream_name != cast(str, stream_def.pop(MAPPER_SOURCE_OPTION)):
                    # Not a match
                    continue

            elif stream_name != stream_map_key:
                # Not a match
                continue

            if MAPPER_ALIAS_OPTION in stream_def:
                stream_alias = cast(str, stream_def.pop(MAPPER_ALIAS_OPTION))

            mapper = CustomStreamMap(
                stream_alias=stream_alias,
                map_transform=stream_def,
                map_config=self.map_config,
                raw_schema=schema,
                key_properties=key_properties,
            )
            if stream_name == stream_alias:
                # Zero-th mapper should be the same-named mapper.
                # Override the default mapper with this custom map
                self.stream_maps[stream_name][0] = mapper
            else:
                # Additional mappers for aliasing and multi-projection:
                self.stream_maps[stream_name].append(mapper)

    def get_primary_mapper(self, stream_name: str) -> StreamMap:
        """Return the primary mapper for the specified stream name.

        Args:
            stream_name: TODO

        Returns:
            TODO

        Raises:
            StreamMapConfigError: TODO
        """
        if stream_name not in self.stream_maps:
            raise StreamMapConfigError(f"No mapper found for {stream_name}. ")

        return self.stream_maps[stream_name][0]

    def apply_primary_mapper(self, stream_name: str, record: dict) -> Optional[dict]:
        """Apply the primary mapper for the stream and return the result.

        Args:
            stream_name: TODO
            record: TODO

        Returns:
            TODO
        """
        return self.get_primary_mapper(stream_name).transform(record)
