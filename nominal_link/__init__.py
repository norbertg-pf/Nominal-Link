"""nominal_link -- the Nominal integration boundary for DAQUniversal.

A self-contained, host-agnostic layer that owns everything *Nominal-specific*
about uploading and streaming: the data-model mapping (presets -> assets), the
write-stream session handshake, the reconnect tunables, and the upload-CLI
contract. It imports nothing from DAQUniversal, so it can be lifted into a
standalone, Nominal-owned package (see README) without touching this code.

The split, by design:
- This package: HOW to talk to Nominal (SDK calls, data model, CLI grammar).
- The host (DAQUniversal): WHEN and WHAT to send -- the acquisition pipeline,
  the streaming child-process lifecycle and CPU pinning, the inter-process
  counters, the progress/event-log surfaces, the upload gating and retries.

Public API is re-exported here so callers can ``from nominal_link import ...``.
"""

from __future__ import annotations

from nominal_link.availability import nominal_sdk_available
from nominal_link.model import (
    REQUIRED_PRESET_FIELDS,
    build_asset_key_for_preset,
    build_run_metadata,
)
from nominal_link.reconnect import ReconnectPolicy
from nominal_link.streaming import (
    MIN_SAFE_MAX_WAIT_MS,
    RECONNECT_BACKOFF_S,
    RECONNECT_CLOSE_TIMEOUT_S,
    RECOVERY_QUIET_S,
    close_stream_ctx,
    create_stream_run,
    open_stream_session,
)
from nominal_link.upload import (
    UPLOADER_DISTRIBUTION,
    UPLOADER_PIP_SPEC,
    output_indicates_partial_failure,
    output_indicates_uploader_missing,
    tdms_subcommand_argv,
)

__version__ = "0.2.0"

__all__ = [
    "nominal_sdk_available",
    "REQUIRED_PRESET_FIELDS",
    "build_asset_key_for_preset",
    "build_run_metadata",
    "MIN_SAFE_MAX_WAIT_MS",
    "RECONNECT_BACKOFF_S",
    "RECONNECT_CLOSE_TIMEOUT_S",
    "RECOVERY_QUIET_S",
    "ReconnectPolicy",
    "close_stream_ctx",
    "create_stream_run",
    "open_stream_session",
    "UPLOADER_DISTRIBUTION",
    "UPLOADER_PIP_SPEC",
    "output_indicates_partial_failure",
    "output_indicates_uploader_missing",
    "tdms_subcommand_argv",
]
