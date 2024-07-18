from __future__ import annotations

import functools
import typing as t
import warnings


def deprecate_row_param(func: t.Callable) -> t.Callable:
    @functools.wraps(func)
    def wrapper(*args: t.Any, **kwargs: t.Any) -> t.Any:  # noqa: ANN401
        if "row" in kwargs:
            warnings.warn(
                f"The 'row' parameter for '{func.__qualname__}' is deprecated. "
                "Use 'record' instead.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            kwargs["record"] = kwargs.pop("row")
        return func(*args, **kwargs)

    return wrapper
