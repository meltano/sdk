"""tap-base library for building singer-compliant taps."""

from tap_base.streams.core import TapStreamBase
from tap_base.streams.rest import RESTStreamBase
from tap_base.streams.database import DatabaseStreamBase


__all__ = [
    "TapStreamBase",
    "RESTStreamBase",
    "DatabaseStreamBase",
]
