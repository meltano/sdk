"""Performance timers which deal with dynamic timing events."""

from __future__ import annotations

import time


class PerfTimerError(Exception):
    """A custom exception used to report errors in use of BatchPerfTimer class."""


class PerfTimer:
    """A Basic Performance Timer Class."""

    _start_time: float | None = None
    _stop_time: float | None = None
    _lap_time: float | None = None

    @property
    def start_time(self) -> float | None:
        return self._start_time

    @property
    def stop_time(self) -> float | None:
        return self._stop_time

    @property
    def lap_time(self) -> float | None:
        return self._lap_time

    def start(self) -> None:
        """Start the timer."""
        if self._start_time is not None:
            msg = "Timer is running. Use .stop() to stop it"
            raise PerfTimerError(msg)

        self._start_time = time.perf_counter()

    def on_the_clock(self) -> float:
        """Give the time on the clock."""
        if self._start_time is None:
            msg = "Timer is not running. Use .start() to start it"
            raise PerfTimerError(msg)

        return (
            time.perf_counter() - self._start_time
            if self._start_time is not None
            else 0.0
        )

    def stop(self) -> None:
        """Stop the timer, Stores the elapsed time, and reset."""
        if self._start_time is None:
            msg = "Timer is not running. Use .start() to start it"
            raise PerfTimerError(msg)

        self._stop_time = time.perf_counter()
        self._lap_time = self._stop_time - self._start_time
        self._start_time = None
        self._stop_time = None


class BatchPerfTimer(PerfTimer):
    """The Performance Timer for Target bulk inserts."""

    def __init__(
        self,
        max_size: int,
        max_perf_counter: float,
    ) -> None:
        self.SINK_MAX_SIZE_CEILING: int = max_size
        self._max_perf_counter: float = max_perf_counter

    SINK_MAX_SIZE_CEILING: int
    """The max size a bulk insert can be"""

    _sink_max_size: int = 100
    """Hold the calculated batch size"""

    @property
    def sink_max_size(self) -> int:
        """The current MAX_SIZE_DEFAULT."""
        return self._sink_max_size

    @property
    def max_perf_counter(self) -> float:
        """How many seconds can pass before a insert."""
        return self._max_perf_counter

    @property
    def perf_diff_allowed_min(self) -> float:
        """The minimum negative variance allowed, 1/3 worse than wanted."""
        return -1.0 * (self.max_perf_counter * 0.33)

    @property
    def perf_diff_allowed_max(self) -> float:
        """The maximum positive variance allowed, 1/4 better than wanted."""
        return self.max_perf_counter * 0.25

    @property
    def perf_diff(self) -> float:
        """Difference between wanted elapsed time and actual elapsed time."""
        diff = self.max_perf_counter - self.lap_time if self.lap_time else 0
        return float(diff)

    def counter_based_max_size(self) -> None:  # noqa: C901
        """Calculate performance based batch size."""
        correction = 0
        if self.perf_diff < self.perf_diff_allowed_min:
            if self.sink_max_size >= 15000:  # noqa: PLR2004
                correction = -5000
            elif self.sink_max_size >= 10000:  # noqa: PLR2004
                correction = -1000
            elif self.sink_max_size >= 1000:  # noqa: PLR2004
                correction = -100
            elif self.sink_max_size > 10:  # noqa: PLR2004
                correction = -10
        if (
            self.perf_diff >= self.perf_diff_allowed_max
            and self.sink_max_size < self.SINK_MAX_SIZE_CEILING
        ):
            if self.sink_max_size >= 10000:  # noqa: PLR2004
                correction = 10000
            elif self.sink_max_size >= 1000:  # noqa: PLR2004
                correction = 1000
            elif self.sink_max_size >= 100:  # noqa: PLR2004
                correction = 100
            elif self.sink_max_size >= 10:  # noqa: PLR2004
                correction = 10
        self._sink_max_size += correction
