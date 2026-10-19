"""JSONPath helpers."""

from __future__ import annotations

import logging
import re
import typing as t
import warnings
from functools import lru_cache

from jsonpath_ng import Child, Slice
from jsonpath_ng.ext import parse

from singer_sdk.helpers._compat import SingerSDKPendingDeprecationWarning
from singer_sdk.helpers._packaging import get_sdk_version

if t.TYPE_CHECKING:
    import jsonpath_ng


logger = logging.getLogger(__name__)

_RELEASE_VERSION = re.compile(r"\d+\.\d+\.\d+")


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

    if _is_wildcard_child(compiled_jsonpath, expression):
        # jsonpath-ng>=1.11 no longer coerces scalars and objects into single-item
        # arrays for `[*]`, so a lone object would yield its values instead of
        # itself. Evaluate the parent path and keep the legacy behavior.
        for match in compiled_jsonpath.left.find(input):
            value = match.value
            if isinstance(value, list):
                yield from value
            elif value is not None:
                warnings.warn(
                    f"JSONPath {expression!r} matched a non-array value. Wrapping it "
                    "in an array is deprecated and will stop working in a future "
                    "version. Use a path that selects the object itself instead. See "
                    f"{_docs_url()}#jsonpath-wildcards-on-a-single-object",
                    SingerSDKPendingDeprecationWarning,
                    stacklevel=3,
                )
                yield value
        return

    matches = compiled_jsonpath.find(input)

    logger.debug("JSONPath %s match count: %d", expression, len(matches))

    for match in matches:
        yield match.value


def _docs_url() -> str:
    """Get the URL of the code samples docs matching the installed SDK release.

    Returns:
        A versioned URL for release builds, or the latest docs otherwise.
    """
    version = get_sdk_version()
    ref = f"v{version}" if _RELEASE_VERSION.fullmatch(version) else "latest"
    return f"https://sdk.meltano.com/en/{ref}/code_samples.html"


def _is_wildcard_child(
    path: jsonpath_ng.JSONPath,
    expression: str,
) -> t.TypeGuard[Child]:
    """Check whether a compiled JSONPath ends in an unbounded ``[*]``.

    jsonpath-ng parses both ``[*]`` and ``[:]`` as an unbounded slice, so the original
    expression is checked to only match the former.

    Args:
        path: A compiled JSONPath expression.
        expression: The JSONPath expression ``path`` was compiled from.

    Returns:
        True if the expression is a child with a ``[*]`` wildcard, False otherwise.
    """
    right = getattr(path, "right", None)
    return (
        expression.rstrip().endswith("[*]")
        and isinstance(path, Child)
        and isinstance(right, Slice)
        and right.start is None
        and right.end is None
        and right.step is None
    )


@lru_cache
def _compile_jsonpath(expression: str) -> jsonpath_ng.JSONPath:
    """Parse a JSONPath expression and cache the result.

    Args:
        expression: A string representing a JSONPath expression.

    Returns:
        A compiled JSONPath object.
    """
    return parse(expression)  # type: ignore[no-any-return]
