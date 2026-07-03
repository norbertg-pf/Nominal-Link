"""Sample-feed tests: per-sample encoding + upload-failure detection.

Imports only ``nominal_link``; the write-stream is faked.
"""

from __future__ import annotations

import logging

import numpy as np

from nominal_link import (
    UPLOAD_FAILURE_LEVEL,
    UPLOAD_FAILURE_LOGGER_NAME,
    enqueue_block,
    upload_failure_detail,
)


class _RecordingStream:
    def __init__(self, fail_on: set[tuple[str, float]] | None = None):
        self.calls: list[tuple[str, int, float]] = []
        self._fail_on = fail_on or set()

    def enqueue(self, channel_name, timestamp, value):
        if (channel_name, value) in self._fail_on:
            raise ConnectionError("sdk queue rejected the sample")
        self.calls.append((channel_name, timestamp, value))


def test_enqueue_block_encodes_int_ns_and_feeds_every_channel():
    stream = _RecordingStream()
    run_start_ns = 1_750_000_000_000_000_000
    t_view = np.array([0.0, 0.001, 0.002])
    val_view = np.array([[1.0, 2.0, 3.0], [10.0, 20.0, 30.0]])

    enqueued, dropped = enqueue_block(
        stream, ["chA", "chB"], t_view, val_view, run_start_ns=run_start_ns
    )

    assert (enqueued, dropped) == (6, 0)
    # Exact integer-nanosecond timestamps: rint(t*1e9) + run_start_ns.
    expected_ts = [run_start_ns, run_start_ns + 1_000_000, run_start_ns + 2_000_000]
    assert stream.calls[:3] == [("chA", expected_ts[i], v) for i, v in enumerate([1.0, 2.0, 3.0])]
    assert stream.calls[3:] == [("chB", expected_ts[i], v) for i, v in enumerate([10.0, 20.0, 30.0])]
    # Timestamps are native ints and values native floats (not numpy scalars).
    assert all(type(ts) is int and type(v) is float for _, ts, v in stream.calls)


def test_enqueue_block_counts_drops_without_aborting_the_block():
    stream = _RecordingStream(fail_on={("chA", 2.0)})
    enqueued, dropped = enqueue_block(
        stream, ["chA"], np.array([0.0, 0.001, 0.002]), np.array([[1.0, 2.0, 3.0]]),
        run_start_ns=0,
    )
    # The failing middle sample is dropped; the rest still go through.
    assert (enqueued, dropped) == (2, 1)
    assert [v for _, _, v in stream.calls] == [1.0, 3.0]


def test_enqueue_block_rint_rounds_rather_than_truncates():
    stream = _RecordingStream()
    # 0.0015 s = 1_500_000 ns exactly on the .5 boundary; rint rounds to even.
    enqueue_block(stream, ["c"], np.array([0.0000000015]), np.array([[1.0]]), run_start_ns=0)
    assert stream.calls[0][1] == 2  # 1.5 ns -> rint -> 2, not truncated to 1


def test_upload_failure_constants_name_the_sdk_logger():
    assert UPLOAD_FAILURE_LOGGER_NAME == "nominal"
    assert UPLOAD_FAILURE_LEVEL == logging.ERROR


def test_upload_failure_detail_prefers_the_attached_exception():
    try:
        raise TimeoutError("socket timed out")
    except TimeoutError:
        import sys
        record = logging.LogRecord(
            "nominal", logging.ERROR, __file__, 1, "batch upload failed", None, sys.exc_info()
        )
    assert upload_failure_detail(record) == "TimeoutError: socket timed out"


def test_upload_failure_detail_falls_back_to_the_message():
    record = logging.LogRecord(
        "nominal", logging.ERROR, __file__, 1, "dropped %d rows", (42,), None
    )
    assert upload_failure_detail(record) == "dropped 42 rows"
