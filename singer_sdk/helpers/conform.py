"""Record conforming helpers, for both the tap and target."""

from __future__ import annotations

from enum import Enum

__all__ = [
    "DatetimeErrorTreatmentEnum",
    "TypeConformanceLevel",
]


class DatetimeErrorTreatmentEnum(Enum):
    """Enum for treatment options for date parsing error."""

    ERROR = "error"
    """
    Raise an error for invalid time values.
    """

    MAX = "max"
    """
    Replace invalid time values with the maximum timestamp value.
    """

    NULL = "null"
    """
    Replace invalid time values with a null value.
    """


class TypeConformanceLevel(Enum):
    """Used to configure how data is conformed to json compatible types.

    Before outputting data as JSON, it is conformed to types that are valid in json,
    based on the current types and the schema. For example, dates are converted to
    strings.

    By default, all data is conformed recursively. If this is not necessary (because
    data is already valid types, or you are manually converting it) then it may be more
    performant to use a lesser conformance level.
    """

    RECURSIVE = 1
    """
    All data is recursively conformed
    """

    ROOT_ONLY = 2
    """
    Only properties on the root object, excluding array elements, are conformed
    """

    NONE = 3
    """
    No conformance is performed
    """
