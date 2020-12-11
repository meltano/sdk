"""Shared parent class for TapBase, TargetBase, and TransformBase."""

import abc
import logging
import sys

from typing import Any

import backoff

from tap_base.exceptions import TapStreamConnectionFailure
from tap_base.helpers import classproperty


class GenericStreamBase(metaclass=abc.ABCMeta):
    """Abstract base class for generic tap streams."""

    MAX_CONNECT_RETRIES = 0

    _config: dict
    _conn: Any

    logger: logging.Logger

    @classproperty
    def discoverable(self):
        """Set to true if stream can discover its own metadata."""
        return False

    def __init__(self, config: dict, logger: logging.Logger):
        """Initialize stream."""
        self._config = config
        self.logger = logger

    def get_config(self, config_key: str, default: Any = None) -> Any:
        """Return config value or a default value."""
        return self._config.get(config_key, default)

    def open_stream_connection(self) -> Any:
        """Perform any needed tasks to initialize the tap stream connection."""
        pass

    def log_backoff_attempt(self, details):
        """Log backoff attempts used by stream retry_pattern()."""
        self.logger.info(
            "Error communicating with source, "
            f"triggering backoff: {details.get('tries')} try"
        )

    def connect_with_retries(self) -> Any:
        """Run open_stream_connection(), retry automatically a few times if failed."""
        return backoff.on_exception(
            backoff.expo,
            exception=TapStreamConnectionFailure,
            max_tries=self.MAX_CONNECT_RETRIES,
            on_backoff=self.log_backoff_attempt,
            factor=2,
        )(self.open_stream_connection)()

    def is_connected(self) -> bool:
        """Return True if connected."""
        return self._conn is not None

    def ensure_connected(self):
        """Connect if not yet connected."""
        if not self.is_connected():
            self.connect_with_retries()

    def fatal(self):
        """Fatal error. Abort stream."""
        sys.exit(1)
