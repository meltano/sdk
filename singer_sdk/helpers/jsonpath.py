"""JSONPath helpers."""

from typing import Any, Generator, Union

import jsonpath_ng
import memoization
from jsonpath_ng.ext import parse


def extract_jsonpath(
    expression: str, input: Union[dict, list]
) -> Generator[Any, None, None]:
    """Extract records from an input based on a JSONPath expression.

    Args:
        expression: JSONPath expression to match against the input.
        input: JSON object or array to extract records from.

    Yields:
        Records matched with JSONPath expression.
    """
    compiled_jsonpath = _compile_jsonpath(expression)

    match: jsonpath_ng.DatumInContext
    for match in compiled_jsonpath.find(input):
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
