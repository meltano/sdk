"""JSONPath helpers."""

from typing import Any, Generator, Union
import memoization

import jsonpath_ng


def extract_jsonpath(
    expression: str, input: Union[dict, list]
) -> Generator[Any, None, None]:
    """Extract records from an input based on a JSONPath expression."""
    compiled_jsonpath = _compile_jsonpath(expression)

    match: jsonpath_ng.DatumInContext
    for match in compiled_jsonpath.find(input):
        yield match.value


@memoization.cached
def _compile_jsonpath(expression: str) -> jsonpath_ng.JSONPath:
    """Parse a JSONPath expression and cache the result."""
    return jsonpath_ng.parse(expression)
