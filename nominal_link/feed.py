"""Sample feed: per-sample write-stream encoding + upload-failure detection.

The two remaining pieces of Nominal-SDK knowledge on a streaming host's hot
path, extracted so Nominal can own and optimise them:

- HOW samples enter an open write-stream (:func:`enqueue_block`): the SDK wants
  integer-nanosecond timestamps and one ``enqueue(channel_name, timestamp,
  value)`` call per sample. The encoding is vectorised per block (one call per
  chunk from the host), so owning it here adds no per-sample overhead.
- HOW a dropped batch is detected (:data:`UPLOAD_FAILURE_LOGGER_NAME` /
  :data:`UPLOAD_FAILURE_LEVEL` / :func:`upload_failure_detail`): the SDK's
  default JSON write-stream never raises on a dead network -- it logs ``ERROR``
  exactly once per failed batch upload on the ``nominal`` logger and silently
  drops the batch. Each such record therefore means real data was lost; the
  host attaches its own counting handler using these constants and keeps the
  counting/throttling/reporting mechanism.

Functions/Constants:
- enqueue_block: feed one block of samples into an open write-stream.
- UPLOAD_FAILURE_LOGGER_NAME / UPLOAD_FAILURE_LEVEL: where dropped batches show up.
- upload_failure_detail: concise human-readable detail for a failure record.
"""

from __future__ import annotations

import logging
from typing import Sequence

import numpy as np

# The logger the SDK's write-stream reports upload failures on, and the level it
# uses. The default JSON write-stream logs ERROR exactly once per failed batch
# upload and then silently drops that batch (no retry, no resend), so each ERROR
# record on this logger means data did not reach Nominal.
UPLOAD_FAILURE_LOGGER_NAME = "nominal"
UPLOAD_FAILURE_LEVEL = logging.ERROR


def upload_failure_detail(record: logging.LogRecord) -> str:
    """Concise human-readable detail for a dropped-batch log record.

    Prefers the attached exception (type + message) when the record carries
    ``exc_info``, else the record's formatted message.

    Args:
        record: A log record seen on the :data:`UPLOAD_FAILURE_LOGGER_NAME`
            logger at :data:`UPLOAD_FAILURE_LEVEL`.

    Returns:
        A one-line description suitable for an operator-facing note.
    """
    exc = record.exc_info[1] if record.exc_info else None
    return f"{type(exc).__name__}: {exc}" if exc is not None else record.getMessage()


def enqueue_block(
    stream, channel_names: Sequence[str], t_view, val_view, *, run_start_ns: int
) -> tuple[int, int]:
    """Feed one block of samples into an open write-stream.

    Encodes the block's timestamps as integer nanoseconds -- ``t_view`` holds
    seconds since the run start (small values), so ``rint`` to int64 is exact
    and adding ``run_start_ns`` stays within int64. The vectorised int path is
    far cheaper than building a datetime per sample, and ``.tolist()`` yields
    native Python floats, which iterate and enqueue faster than numpy scalars.

    A failing ``enqueue`` for one sample never aborts the block: it is counted
    as dropped and the remaining samples still go through (matching the SDK's
    own per-batch drop semantics).

    Args:
        stream: An open write-stream handle (exposes ``enqueue``).
        channel_names: Stream channel name per row of ``val_view``.
        t_view: 1-D array of per-sample times, in seconds since run start.
        val_view: 2-D array of samples, shape ``(len(channel_names), len(t_view))``.
        run_start_ns: The run start as integer nanoseconds since the epoch.

    Returns:
        ``(enqueued, dropped)`` sample counts for this block.
    """
    ts_ns = (np.rint(np.asarray(t_view) * 1e9).astype(np.int64) + int(run_start_ns)).tolist()
    enqueued = 0
    dropped = 0
    for ch_idx, ch_name in enumerate(channel_names):
        for ts, val in zip(ts_ns, val_view[ch_idx].tolist()):
            try:
                stream.enqueue(channel_name=ch_name, timestamp=ts, value=val)
                enqueued += 1
            except Exception:
                dropped += 1
    return enqueued, dropped
