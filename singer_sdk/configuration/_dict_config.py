"""Helpers for parsing and wrangling configuration dictionaries."""

from __future__ import annotations

import logging
import os
import typing as t
from pathlib import Path

from dotenv import find_dotenv
from dotenv.main import DotEnv

from singer_sdk.helpers import _typing
from singer_sdk.helpers._util import load_json, read_json_file

logger = logging.getLogger(__name__)

TRUTHY = ("true", "1", "yes", "on")


def _parse_array(value: str) -> list[str]:
    return load_json(value)  # type: ignore[return-value]


def _legacy_parse_array_of_strings(value: str) -> list[str]:
    return value.split(",")


def parse_environment_config(
    config_schema: dict[str, t.Any],
    prefix: str,
    dotenv_path: str | None = None,
) -> dict[str, t.Any]:
    """Parse configuration from environment variables.

    Args:
        config_schema: A JSON Schema dictionary for the configuration.
        prefix: Prefix for environment variables.
        dotenv_path: Path to a .env file. If None, will try to find one in increasingly
            higher folders.

    Returns:
        A configuration dictionary.
    """
    result: dict[str, t.Any] = {}

    if not dotenv_path:
        dotenv_path = find_dotenv()

    logger.debug("Loading configuration from %s", dotenv_path)
    DotEnv(dotenv_path).set_as_environment_variables()

    for config_key, schema in config_schema.get("properties", {}).items():
        env_var_name = prefix + config_key.upper().replace("-", "_")
        if env_var_name in os.environ:
            env_var_value = os.environ[env_var_name]
            logger.info(
                "Parsing '%s' config from env variable '%s'.",
                config_key,
                env_var_name,
            )
            if _typing.is_integer_type(schema):
                result[config_key] = int(env_var_value)
            elif _typing.is_boolean_type(schema):
                result[config_key] = env_var_value.lower() in TRUTHY
            elif _typing.is_string_array_type(schema):
                try:
                    result[config_key] = _parse_array(env_var_value)
                except Exception:  # noqa: BLE001
                    # TODO(edgarrmondragon): Make this a deprecation warning.
                    # https://github.com/meltano/sdk/issues/2724
                    logger.warning(
                        "Parsing array of the form 'x,y,z' is deprecated and will be "
                        "removed in future versions.",
                    )
                    result[config_key] = _legacy_parse_array_of_strings(env_var_value)
            elif _typing.is_array_type(schema):
                result[config_key] = _parse_array(env_var_value)
            elif _typing.is_object_type(schema):
                result[config_key] = load_json(env_var_value)
            else:
                result[config_key] = env_var_value
    return result


def merge_config_sources(
    inputs: t.Iterable[str],
    config_schema: dict[str, t.Any],
    env_prefix: str,
) -> dict[str, t.Any]:
    """Merge configuration from multiple sources into a single dictionary.

    Args:
        inputs: A sequence of configuration sources (file paths or ENV).
        config_schema: A JSON Schema dictionary for the configuration.
        env_prefix: Prefix for environment variables.

    Raises:
        FileNotFoundError: If any of config files does not exist.

    Returns:
        A single configuration dictionary.
    """
    config: dict[str, t.Any] = {}
    for config_input in inputs:
        if config_input == "ENV":
            env_config = parse_environment_config(config_schema, prefix=env_prefix)
            config.update(env_config)
            continue

        config_path = Path(config_input)

        if not config_path.is_file():
            msg = (
                f"Could not locate config file at '{config_path}'.Please check that "
                "the file exists."
            )
            raise FileNotFoundError(msg)

        config.update(read_json_file(config_path))

    return config


def merge_missing_config_jsonschema(
    source_jsonschema: dict,
    target_jsonschema: dict,
) -> None:
    """Append any missing properties in the target with those from source.

    Args:
        source_jsonschema: The source json schema from which to import.
        target_jsonschema: The json schema to update.
    """
    for k, v in source_jsonschema["properties"].items():
        if k not in target_jsonschema["properties"]:
            target_jsonschema["properties"][k] = v
