"""tap-base library for building singer-compliant taps."""

# from tap_base.streams.generic import GenericStreamBase
# from tap_base.streams.syncable import TapStreamBase

# from tap_base.streams.discoverable import DiscoverableStreamBase, DatabaseStreamBase
from tap_base.streams.discoverable import DatabaseStreamBase


__all__ = [
    # "GenericStreamBase",
    # "TapStreamBase",
    # "DiscoverableStreamBase",
    "DatabaseStreamBase",
]
