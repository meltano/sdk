"""Sink classes for targets."""

from __future__ import annotations

import typing as t
import warnings

from singer_sdk.helpers._compat import SingerSDKDeprecationWarning
from singer_sdk.sinks.batch import BatchSink
from singer_sdk.sinks.batch_context import BatchContext
from singer_sdk.sinks.batch_strategies import (
    BatchStrategy,
    HybridBatchStrategy,
    NoBatchStrategy,
    RecordCountBatchStrategy,
    SizeBatchStrategy,
    TimeWindowBatchStrategy,
)
from singer_sdk.sinks.core import Sink
from singer_sdk.sinks.protocols import (
    IBatchable,
    IBatchLoader,
    IPreprocessable,
    IRecordLoader,
    IVersionable,
)
from singer_sdk.sinks.record import RecordSink

if t.TYPE_CHECKING:
    from singer_sdk.sql import SQLSink  # noqa: F401

__all__ = [
    # Core sink classes
    "BatchSink",
    "RecordSink",
    "Sink",
    # Type-safe batch context
    "BatchContext",
    # Batch strategies (for custom batching behavior)
    "BatchStrategy",
    "NoBatchStrategy",
    "RecordCountBatchStrategy",
    "TimeWindowBatchStrategy",
    "HybridBatchStrategy",
    "SizeBatchStrategy",
    # Protocols (for type hints and interfaces)
    "IRecordLoader",
    "IBatchLoader",
    "IPreprocessable",
    "IVersionable",
    "IBatchable",
]


def __getattr__(name: str) -> t.Any:  # noqa: ANN401
    """Provide backward compatibility for moved SQL classes.

    Args:
        name: The name of the attribute to import.

    Returns:
        The imported attribute.

    Raises:
        AttributeError: If the attribute is not found.
    """
    if name == "SQLSink":
        warnings.warn(
            f"Importing {name} from singer_sdk.sinks is deprecated. "
            f"Please import from singer_sdk.sql instead: "
            f"from singer_sdk.sql import {name}",
            SingerSDKDeprecationWarning,
            stacklevel=2,
        )
        from singer_sdk.sql import SQLSink  # noqa: PLC0415

        return SQLSink

    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
