"""Nominal write-stream protocol layer: open a session, create the run.

This module owns the Nominal-SDK-specific knowledge for live streaming -- the
client/asset/dataset/write-stream handshake, the bounded close of a dead stream,
and the run-creation call -- plus the tunables that govern outage recovery (the
decision policy that consumes them lives in :mod:`nominal_link.reconnect`). It
does NOT own the host's loop, the inter-process counters, or the CPU pinning:
those are host (DAQUniversal) concerns. The host imports ``open_stream_session``
and drives it; this layer is the part Nominal can own and optimise without
seeing the host's acquisition code.

The ``nominal`` SDK is imported lazily by the caller (the host passes the
``NominalClient`` class into ``open_stream_session``), so importing this module
never requires the SDK to be installed.

Functions/Constants:
- MIN_SAFE_MAX_WAIT_MS: smallest write-stream max-wait the SDK honours sanely.
- RECONNECT_BACKOFF_S / RECOVERY_QUIET_S / RECONNECT_CLOSE_TIMEOUT_S: reconnect tunables.
- open_stream_session: open a fresh client/asset/dataset/write-stream session.
- close_stream_ctx: bounded close of a (possibly dead) write-stream context.
- create_stream_run: create the Nominal run that frames a finished stream.
"""

from __future__ import annotations

import threading
from datetime import datetime
from typing import Callable

# The Nominal SDK's timeout-flush thread derives its sleep from
# ``max_wait.seconds`` (the whole-seconds component) rather than
# ``total_seconds()``. Any value below 1000 ms truncates to 0, so the flush
# thread busy-spins a CPU core and hammers the batch lock -- which starves the
# enqueue loop. Sub-second waits buy nothing at speed (the batch flushes on
# ``batch_size`` long before the timeout), so the host clamps up to this floor.
MIN_SAFE_MAX_WAIT_MS = 1000

# Self-healing reconnect schedule after a network/Wi-Fi outage. ``enqueue``
# never raises on a dead network -- the SDK drops batches and logs an ERROR -- so
# a *sustained* failure escalates to a full session rebuild (fresh client +
# write-stream). Recovers gently on a long overnight outage; capped at the final
# entry. These are the knobs to tune for a given site's network behaviour.
RECONNECT_BACKOFF_S = (30.0, 60.0, 120.0, 300.0, 600.0, 900.0)

# Uploads going quiet for this long while the stream is still open means the
# failure cleared on its own: the SDK resumes uploading on the same stream once
# the network returns, so a brief blip needs no rebuild.
RECOVERY_QUIET_S = 5.0

# Bounded wait for closing a dead write-stream during reconnect: ``__exit__``
# flushes outstanding batches and joins the SDK upload thread, which can block on
# a dead socket, so the host runs it on a daemon thread and abandons it past this.
RECONNECT_CLOSE_TIMEOUT_S = 15.0


def open_stream_session(
    nominal_client_cls, *, profile, asset_key, asset_name, dataset_scope, batch_size, max_wait_td
):
    """Open a fresh Nominal write-stream session and return its handles.

    Re-resolves the client and asset on every call (not just the write-stream),
    so an auth token that expired during a long outage is refreshed on reconnect.
    Each step makes a network call, so on a dead network this raises -- which lets
    the host's reconnect loop treat a failure as "still down" and back off.

    Args:
        nominal_client_cls: The ``NominalClient`` class (imported by the caller,
            so this module never hard-depends on the ``nominal`` SDK).
        profile: Nominal CLI profile name to authenticate with.
        asset_key: Property dict identifying/creating the Nominal asset.
        asset_name: Human-readable asset name used on create.
        dataset_scope: Data-scope name of the stream dataset under the asset.
        batch_size: SDK write-stream batch size (samples per upload).
        max_wait_td: SDK write-stream max batch-fill wait as a ``timedelta``.

    Returns:
        Tuple ``(client, asset, dataset, stream_ctx, stream)`` for an open stream.

    Raises:
        Exception: Any SDK/network failure while resolving the client, asset,
            dataset, or opening the write-stream (treated as "network down").
    """
    client = nominal_client_cls.from_profile(profile)
    asset = client.get_or_create_asset_by_properties(asset_key, name=asset_name)
    try:
        dataset = asset.get_dataset(dataset_scope)
    except ValueError:
        dataset = client.create_dataset(asset.name + " Stream Dataset")
        asset.add_dataset(data_scope_name=dataset_scope, dataset=dataset)
    stream_ctx = dataset.get_write_stream(batch_size=int(batch_size), max_wait=max_wait_td)
    stream = stream_ctx.__enter__()
    return client, asset, dataset, stream_ctx, stream


def _emit_warning(on_warning: Callable[[str], None] | None, message: str) -> None:
    """Deliver a close warning to the host's callback; never raise back in.

    A raising callback must not break the close path (nor kill the daemon close
    thread mid-flight), so callback failures are swallowed here by design -- the
    close itself still completes/times out either way.

    Args:
        on_warning: Host callback taking the formatted message, or None.
        message: The fully formatted warning text.
    """
    if on_warning is None:
        return
    try:
        on_warning(message)
    except Exception:
        pass


def close_stream_ctx(stream_ctx, *, timeout_s: float, on_warning: Callable[[str], None] | None = None) -> bool:
    """Close a write-stream context with a bounded wait, never blocking forever.

    The SDK's ``__exit__`` flushes outstanding batches and joins its upload
    thread; on a dead network that flush can block until the socket times out.
    So the close runs on a daemon thread and is abandoned past ``timeout_s`` --
    a hung close can never stall the host's reconnect loop, and the orphaned
    thread dies with the process. This is Nominal-SDK behaviour knowledge, which
    is why the mechanism lives here rather than in the host.

    Args:
        stream_ctx: The write-stream context manager to exit (no-op if None).
        timeout_s: Seconds to wait for the close before abandoning it
            (:data:`RECONNECT_CLOSE_TIMEOUT_S` is the tuned default to pass).
        on_warning: Optional callback receiving a formatted warning message when
            the close errors or times out (the host routes this to its logs).

    Returns:
        True when the close finished (or there was nothing to close); False when
        it was abandoned after ``timeout_s``.
    """
    if stream_ctx is None:
        return True
    done = threading.Event()

    def _close_worker() -> None:
        try:
            stream_ctx.__exit__(None, None, None)
        except Exception as exc:
            _emit_warning(on_warning, f"Error closing Nominal write_stream during reconnect: {exc}")
        finally:
            done.set()

    threading.Thread(target=_close_worker, name="nominal-stream-close", daemon=True).start()
    if not done.wait(timeout=timeout_s):
        _emit_warning(
            on_warning,
            f"Nominal write_stream close did not finish within {timeout_s:.0f}s during reconnect; abandoning it.",
        )
        return False
    return True


def create_stream_run(client, asset, *, run_start: datetime, run_end: datetime, run_metadata: dict):
    """Create the Nominal run that frames a finished streaming session.

    Args:
        client: An open ``NominalClient``.
        asset: The asset the stream wrote to (provides ``rid``).
        run_start: Run start time (UTC); also stamped as the ``Start_Time`` property.
        run_end: Run end time (UTC).
        run_metadata: Extra run properties to merge in (e.g. Test_Site, scale factors).

    Returns:
        The created run object (exposes ``rid``).
    """
    start_iso = run_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    return client.create_run(
        name=f"Stream Run - {start_iso}",
        start=run_start,
        end=run_end,
        assets=[asset.rid],
        properties={**run_metadata, "Start_Time": start_iso},
    )
