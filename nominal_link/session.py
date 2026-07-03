"""StreamSession: supervised lifecycle of one Nominal write-stream session.

The session object that owns the SDK handles (client, asset, dataset,
write-stream) and their lifecycle -- open, self-healing rebuild on an outage
(bounded close of the dead stream + fresh reopen, scheduled by
:class:`~nominal_link.reconnect.ReconnectPolicy`), the final flushing close, and
framing the finished stream with a run. It composes the primitives this package
already owns (``open_stream_session`` / ``close_stream_ctx`` /
``create_stream_run`` + the policy) into one supervised unit, so a host's loop
shrinks to: feed :meth:`StreamSession.observe` once per tick, call
:meth:`StreamSession.rebuild` when told to, and enqueue on
:attr:`StreamSession.stream`.

The host still owns everything around the session: its process/thread model and
priorities, the loop itself, its shared error counter and flags, and every
operator-facing message (the session returns outcomes and exposes state; it
never logs). The primitives are injectable (``open_session`` / ``bounded_close``)
so a host can route them through its own patchable seams or fakes; by default
the session uses this package's implementations.

Functions/Classes:
- StreamSession: open / observe / rebuild / shutdown_close / create_run.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Callable

from nominal_link.reconnect import ReconnectAction, ReconnectPolicy
from nominal_link.streaming import (
    RECONNECT_CLOSE_TIMEOUT_S,
    _emit_warning,
    close_stream_ctx,
    create_stream_run,
    open_stream_session,
)


class StreamSession:
    """Owns one Nominal write-stream session and its self-healing rebuilds.

    One instance per streaming run. Typical host loop::

        session = StreamSession(NominalClient, profile=..., asset_key=..., ...)
        stream = session.open()                      # raises on failure
        while running:
            action = session.observe(time.monotonic(), error_count)
            if action == "reconnect":
                ok, exc, delay = session.rebuild()
                stream = session.stream              # fresh stream or None
        session.shutdown_close()                     # final flushing close
        session.create_run(run_start=..., run_end=..., run_metadata=...)

    The session performs no logging and reads no shared state of the host's --
    outcomes come back as return values and the host renders its own messages
    from them (plus ``session.policy`` for attempt counts and downtime).
    """

    def __init__(
        self,
        nominal_client_cls,
        *,
        profile: str,
        asset_key: dict,
        asset_name: str,
        dataset_scope: str,
        batch_size: int,
        max_wait_td,
        policy: ReconnectPolicy | None = None,
        close_timeout_s: float = RECONNECT_CLOSE_TIMEOUT_S,
        clock: Callable[[], float] = time.monotonic,
        read_error_count: Callable[[], int] | None = None,
        open_session: Callable[..., tuple] = open_stream_session,
        bounded_close: Callable[..., object] | None = None,
        on_warning: Callable[[str], None] | None = None,
    ) -> None:
        """Configure a session (nothing is opened until :meth:`open`).

        Args:
            nominal_client_cls: The ``NominalClient`` class (imported by the
                caller, so this module never hard-depends on the SDK).
            profile: Nominal CLI profile name to authenticate with.
            asset_key: Property dict identifying/creating the Nominal asset.
            asset_name: Human-readable asset name used on create.
            dataset_scope: Data-scope name of the stream dataset under the asset.
            batch_size: SDK write-stream batch size (samples per upload).
            max_wait_td: SDK write-stream max batch-fill wait as a ``timedelta``.
            policy: Reconnect decision policy; a default-tuned
                :class:`ReconnectPolicy` when omitted.
            close_timeout_s: Bound on closing a dead stream during a rebuild.
            clock: Monotonic clock, injectable for deterministic tests. Used
                only to timestamp rebuild outcomes for the policy.
            read_error_count: Optional callable returning the host's cumulative
                upload-error count, used to re-baseline the policy after a
                successful rebuild (errors counted during the outage must not
                re-arm against the fresh stream). A raising/missing reader
                falls back to the policy's last observed count.
            open_session: The session-opening primitive; defaults to
                :func:`nominal_link.streaming.open_stream_session`. Hosts can
                inject a wrapper (e.g. their own patchable seam). Called as
                ``open_session(nominal_client_cls, **open_kwargs)``.
            bounded_close: The dead-stream close primitive, called as
                ``bounded_close(stream_ctx, timeout_s)``; defaults to
                :func:`nominal_link.streaming.close_stream_ctx` routing
                warnings to ``on_warning``.
            on_warning: Warning sink for the default ``bounded_close`` and for
                a raising injected ``bounded_close`` during :meth:`rebuild`.
        """
        self._client_cls = nominal_client_cls
        self._open_kwargs = dict(
            profile=profile,
            asset_key=asset_key,
            asset_name=asset_name,
            dataset_scope=dataset_scope,
            batch_size=batch_size,
            max_wait_td=max_wait_td,
        )
        self.policy = policy if policy is not None else ReconnectPolicy()
        self._close_timeout_s = float(close_timeout_s)
        self._clock = clock
        self._read_error_count = read_error_count
        self._open_session = open_session
        self._on_warning = on_warning
        if bounded_close is None:
            def bounded_close(ctx, timeout_s: float):
                return close_stream_ctx(ctx, timeout_s=timeout_s, on_warning=on_warning)
        self._bounded_close = bounded_close
        self._client = None
        self._asset = None
        self._dataset = None
        self._stream_ctx = None
        self._stream = None

    # -- state ---------------------------------------------------------------

    @property
    def client(self):
        """The current ``NominalClient`` (None before the first open)."""
        return self._client

    @property
    def asset(self):
        """The resolved asset (None before the first open)."""
        return self._asset

    @property
    def dataset(self):
        """The stream dataset (None before the first open)."""
        return self._dataset

    @property
    def stream(self):
        """The open write-stream handle, or None while down/closed.

        Hot-path note: hosts should re-bind a local reference after
        :meth:`open` / :meth:`rebuild` rather than dereferencing this property
        per sample.
        """
        return self._stream

    @property
    def stream_ctx(self):
        """The write-stream context manager, or None while down/closed."""
        return self._stream_ctx

    @property
    def is_open(self) -> bool:
        """True while an open write-stream is held."""
        return self._stream is not None

    # -- lifecycle -----------------------------------------------------------

    def open(self):
        """Open a fresh client/asset/dataset/write-stream session.

        Returns:
            The open write-stream handle (also available as :attr:`stream`).

        Raises:
            Exception: Any SDK/network failure while opening (the host treats
                the initial open failing as fatal for the run; a rebuild-time
                failure goes through :meth:`rebuild` instead and backs off).
        """
        (
            self._client,
            self._asset,
            self._dataset,
            self._stream_ctx,
            self._stream,
        ) = self._open_session(self._client_cls, **self._open_kwargs)
        return self._stream

    def observe(self, now_mono: float, error_count: int) -> ReconnectAction | None:
        """Feed one supervision tick to the reconnect policy.

        Delegates to :meth:`ReconnectPolicy.observe` with ``stream_open`` taken
        from this session's own state, so the host cannot desynchronise the two.

        Args:
            now_mono: Current monotonic time (same clock family as ``clock``).
            error_count: The host's cumulative upload-error count.

        Returns:
            The policy's action for this tick (``"arm"`` / ``"recovered"`` /
            ``"reconnect"``) or None. On ``"reconnect"`` the host should call
            :meth:`rebuild` (after emitting whatever note it wants first).
        """
        return self.policy.observe(now_mono, error_count, stream_open=self.is_open)

    def rebuild(self) -> tuple[bool, Exception | None, float]:
        """Tear the (presumed dead) session down and open a fresh one.

        Closes the old write-stream with the bounded close (a flush to a dead
        socket must not stall the host's loop), drops the stream handles, then
        reopens from scratch -- fresh client, so an auth token that expired
        during the outage is refreshed. The previous client/asset/dataset
        handles are intentionally KEPT on a failed reopen, so a run stopped
        mid-outage can still be framed by :meth:`create_run`. The rebuild
        outcome is reported to the policy with a freshly sampled clock (the
        close + reopen can take many seconds, so scheduling from the tick's
        timestamp would fire the next attempt early).

        A close-path failure never prevents the reopen: the close is
        best-effort teardown of an already-dead stream (the default
        ``bounded_close`` cannot raise; a raising injected one is surfaced via
        ``on_warning`` and the rebuild proceeds).

        Returns:
            ``(True, None, 0.0)`` on success -- the new stream is on
            :attr:`stream` and the policy is reset + re-baselined.
            ``(False, exc, delay_s)`` on failure -- :attr:`stream` stays None
            and the policy has scheduled the next attempt ``delay_s`` from now.
        """
        try:
            self._bounded_close(self._stream_ctx, self._close_timeout_s)
        except Exception as exc:
            _emit_warning(self._on_warning, f"Nominal stream close before rebuild failed: {exc}")
        self._stream_ctx = self._stream = None
        try:
            self.open()
        except Exception as exc:
            delay = self.policy.on_open_failed(self._clock())
            return False, exc, delay
        error_count = self.policy.last_error_count
        if self._read_error_count is not None:
            try:
                error_count = int(self._read_error_count())
            except Exception:
                pass
        self.policy.on_open_succeeded(self._clock(), error_count)
        return True, None, 0.0

    def shutdown_close(self) -> None:
        """Final flushing close of the write-stream (end of the run).

        Unlike the rebuild-time bounded close, this one is allowed to block:
        the run is over and the flush is the last chance to deliver the tail
        samples, so waiting on it is the point. No-op when no stream is held
        (e.g. the run stopped mid-outage).

        Raises:
            Exception: Whatever the SDK's ``__exit__`` raises; the host wraps
                this in its own error surface.
        """
        stream_ctx = self._stream_ctx
        self._stream_ctx = self._stream = None
        if stream_ctx is not None:
            stream_ctx.__exit__(None, None, None)

    def create_run(self, *, run_start: datetime, run_end: datetime, run_metadata: dict):
        """Create the Nominal run that frames this finished session.

        Args:
            run_start: Run start time (UTC).
            run_end: Run end time (UTC).
            run_metadata: Extra run properties (e.g. Test_Site, scale factors).

        Returns:
            The created run object (exposes ``rid``).

        Raises:
            RuntimeError: If the session was never opened (no client/asset).
            Exception: Any SDK/network failure from the run-creation call.
        """
        if self._client is None or self._asset is None:
            raise RuntimeError("StreamSession.create_run called before a successful open")
        return create_stream_run(
            self._client,
            self._asset,
            run_start=run_start,
            run_end=run_end,
            run_metadata=run_metadata,
        )
