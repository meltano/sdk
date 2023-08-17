"""Perf Timer tests."""

from __future__ import annotations

import time

import pytest

from singer_sdk.helpers._perftimer import BatchPerfTimer, PerfTimer, PerfTimerError


def test_perftimer_properties():
    timer: PerfTimer = PerfTimer()
    timer._start_time = 1.1
    timer._stop_time = 1.10
    timer._lap_time = 0.09
    assert timer.start_time is timer._start_time
    assert timer.stop_time is timer.stop_time
    assert timer._lap_time is timer.lap_time
    assert timer.start_time == 1.1
    assert timer.stop_time == 1.10
    assert timer.lap_time == 0.09


def test_perftimer_actions():
    timer: PerfTimer = PerfTimer()
    timer.start()
    assert timer.start_time is not None
    assert timer.stop_time is None
    assert timer.lap_time is None
    time.sleep(1.1)
    assert timer.on_the_clock() >= 1
    timer.stop()
    assert timer.lap_time >= 1
    assert timer.lap_time < 1.5
    assert timer.start_time is None
    assert timer.stop_time is None


def test_perftimer_errors():
    timer: PerfTimer = PerfTimer()
    with pytest.raises(
        PerfTimerError,
        match=r"Timer is not running. Use .start\(\) to start it",
    ):
        timer.stop()
    with pytest.raises(
        PerfTimerError,
        match=r"Timer is not running. Use .start\(\) to start it",
    ):
        timer.on_the_clock()
    # starting a timer to test start() error
    timer.start()
    with pytest.raises(
        PerfTimerError,
        match=r"Timer is running. Use .stop\(\) to stop it",
    ):
        timer.start()
    # stopping the timer at the end of the test
    timer.stop()


def test_batchperftimer_properties():
    batchtimer: BatchPerfTimer = BatchPerfTimer(100, 1)
    batchtimer._lap_time = 0.10
    assert batchtimer._sink_max_size is batchtimer.sink_max_size
    assert batchtimer._max_perf_counter is batchtimer.max_perf_counter
    assert batchtimer.sink_max_size == 100
    assert batchtimer.max_perf_counter == 1
    assert batchtimer.perf_diff_allowed_max == 0.25
    assert batchtimer.perf_diff_allowed_min == -0.33
    assert batchtimer.perf_diff == 0.90


def test_batchperftimer_counter_based_max_size_additive():
    batchtimer: BatchPerfTimer = BatchPerfTimer(10, 1)
    batchtimer._lap_time = 0.24
    assert batchtimer.perf_diff > batchtimer.perf_diff_allowed_max
    assert batchtimer.counter_based_max_size() == 20
    batchtimer._sink_max_size = 100
    assert batchtimer.counter_based_max_size() == 200
    batchtimer._sink_max_size = 1000
    assert batchtimer.counter_based_max_size() == 2000
    batchtimer._sink_max_size = 10000
    assert batchtimer.counter_based_max_size() == 20000
    batchtimer._sink_max_size = 100000
    assert batchtimer.sink_max_size == batchtimer.SINK_MAX_SIZE_CEILING
    assert batchtimer.counter_based_max_size() == 100000


def test_batchperftimer_counter_based_max_size_reductive():
    batchtimer: BatchPerfTimer = BatchPerfTimer(100000, 1)
    batchtimer._lap_time = 1.5
    assert batchtimer.perf_diff < batchtimer.perf_diff_allowed_min
    assert batchtimer.sink_max_size == batchtimer.SINK_MAX_SIZE_CEILING
    assert batchtimer.counter_based_max_size() == 95000
    batchtimer._sink_max_size = 10000
    assert batchtimer.counter_based_max_size() == 9000
    batchtimer._sink_max_size = 1000
    assert batchtimer.counter_based_max_size() == 900
    batchtimer._sink_max_size = 100
    assert batchtimer.counter_based_max_size() == 90
    batchtimer._sink_max_size = 10
    assert batchtimer.counter_based_max_size() == 10
