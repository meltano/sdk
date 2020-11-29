"""TapStreamBase abstract class"""

import abc
import time

import singer


class TapStreamBase(metaclass=abc.ABCMeta):
    """Abstract base class for tap streams."""

    _id: str
    _state: singer.StateMessage

    def __init__(
        self, tap_stream_id: str, state: singer.StateMessage = None,
    ):
        """Initialize tap stream."""
        self._id = tap_stream_id
        self._state = state

    def get_stream_version(self):
        """Get stream version from bookmark."""
        stream_version = singer.get_bookmark(
            state=self._state, tap_stream_id=self._id, key="version"
        )
        if stream_version is None:
            stream_version = int(time.time() * 1000)
        return stream_version
