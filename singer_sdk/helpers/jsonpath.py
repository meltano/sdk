"""JSONPath helpers."""

from __future__ import annotations

import logging
import typing as t

import memoization
from jsonpath_ng.ext import parse

if t.TYPE_CHECKING:
    import jsonpath_ng


logger = logging.getLogger(__name__)


def extract_jsonpath(
    expression: str,
    input: dict | list,  # noqa: A002
) -> t.Generator[t.Any, None, None]:
    """Extract records from an input based on a JSONPath expression.

    Args:
        expression: JSONPath expression to match against the input.
        input: JSON object or array to extract records from.

    Yields:
        Records matched with JSONPath expression.
    """
    compiled_jsonpath = _compile_jsonpath(expression)

    match: jsonpath_ng.DatumInContext
    matches = compiled_jsonpath.find(input)

    logger.info("JSONPath matches: %d", len(matches))

    for match in matches:
        yield match.value


@memoization.cached
def _compile_jsonpath(expression: str) -> jsonpath_ng.JSONPath:
    """Parse a JSONPath expression and cache the result.

    Args:
        expression: A string representing a JSONPath expression.

    Returns:
        A compiled JSONPath object.
    """
    return parse(expression)
