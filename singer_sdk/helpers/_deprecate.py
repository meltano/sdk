from __future__ import annotations

import contextlib
import typing as t
import warnings

from typing_extensions import ParamSpec

P = ParamSpec("P")
T = t.TypeVar("T")

NOT_PROVIDED = object()


def deprecate_param(position: int, name: str) -> None:
    """Decorator to mark a parameter as deprecated.

    Args:
        position: The position of the parameter if passed as a positional argument.
        name: The name of the parameter if passed as a keyword argument.
        acceptable_values: A set of values that don't trigger a deprecation warning.
    """

    def _deprecate_param(func: t.Callable[P, T]) -> t.Callable[P, T]:
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            value = NOT_PROVIDED

            # Check if the parameter is being passed as a positional argument
            with contextlib.suppress(IndexError):
                # Pop the parameter from the arguments tuple
                value = args[position]
                args = args[:position] + args[position + 1 :]

            # Check if the parameter is being passed as a keyword argument
            value = kwargs.pop(name, NOT_PROVIDED)

            if value is not NOT_PROVIDED and value is not None:
                warnings.warn(
                    f"Parameter '{name}' is deprecated and will be removed in a future "
                    "release. Please use 'None' instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )
            else:
                value = None

            kwargs[name] = value

            return func(*args, **kwargs)

        return wrapper

    return _deprecate_param
