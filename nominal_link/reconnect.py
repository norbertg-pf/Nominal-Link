"""Reconnect decision policy for a Nominal write-stream outage.

The pure state machine that decides WHEN a streaming host should rebuild a dead
write-stream session. The host owns the mechanism -- reading its inter-process
error counter, closing and reopening the session, flags, and log surfaces -- and
drives this policy once per loop tick. Keeping the decisions here (beside the
tunables in :mod:`nominal_link.streaming`) lets Nominal own and tune the
*behaviour* of outage recovery, not just its constants, without ever seeing the
host's process model.

Background (why the policy looks like this): the Nominal SDK's ``enqueue`` never
raises on a dead network -- batches are dropped and an ERROR is logged, which the
host counts. A *rise* in that count is therefore the only reliable "data is not
reaching Nominal" signal. The first rise arms a capped-backoff rebuild; a blip
that goes quiet again before the timer fires is cancelled (the SDK resumes on
the same stream); only a persisting failure tears the stream down.

Functions/Classes:
- ReconnectPolicy: per-session decision state machine (observe / on_open_failed /
  on_open_succeeded).
"""

from __future__ import annotations

from typing import Literal, Sequence

from nominal_link.streaming import RECONNECT_BACKOFF_S, RECOVERY_QUIET_S

# The action a host must take after a tick, in decision priority order:
# - "arm":       an outage was just detected; the host should surface it (flag,
#                operator note) and wait -- the rebuild is scheduled, not immediate.
# - "recovered": uploads went quiet again while the stream is still held; the
#                pending rebuild is cancelled and the host should clear its flag.
# - "reconnect": the backoff timer fired while still down; the host should close
#                the dead session and try to open a fresh one now.
ReconnectAction = Literal["arm", "recovered", "reconnect"]


class ReconnectPolicy:
    """Decides when to rebuild a Nominal write-stream after upload failures.

    One instance per streaming session. The host calls :meth:`observe` once per
    loop tick with a monotonic clock and its cumulative upload-error count, then
    performs the returned action; after a rebuild attempt it reports the outcome
    via :meth:`on_open_failed` / :meth:`on_open_succeeded`.

    The policy is pure state: no threads, no I/O, no clock reads of its own --
    every timestamp comes in as a caller-supplied monotonic ``now``. That keeps
    it deterministic and directly unit-testable.

    Example (host loop skeleton)::

        policy = ReconnectPolicy()
        while streaming:
            action = policy.observe(time.monotonic(), error_count, stream_open=stream is not None)
            if action == "reconnect":
                close_dead_session()
                try:
                    stream = open_fresh_session()
                except Exception:
                    delay = policy.on_open_failed(time.monotonic())
                else:
                    policy.on_open_succeeded(time.monotonic(), error_count)
    """

    def __init__(
        self,
        *,
        backoff_s: Sequence[float] = RECONNECT_BACKOFF_S,
        recovery_quiet_s: float = RECOVERY_QUIET_S,
    ) -> None:
        """Create a fresh policy (no outage in progress, error baseline 0).

        Args:
            backoff_s: Rebuild schedule in seconds: the first entry is the delay
                between detecting an outage and the first rebuild; each failed
                rebuild advances one entry; the final entry repeats (cap).
            recovery_quiet_s: Uploads staying quiet for this long while the
                stream is still open cancels a pending rebuild.

        Raises:
            ValueError: If ``backoff_s`` is empty.
        """
        backoff = tuple(float(delay) for delay in backoff_s)
        if not backoff:
            raise ValueError("backoff_s must contain at least one delay")
        self._backoff = backoff
        self._recovery_quiet_s = float(recovery_quiet_s)
        self._last_error_count = 0
        self._last_rise_mono = 0.0
        self._down_since: float | None = None
        self._attempt = 0
        self._next_reconnect_at: float | None = None

    @property
    def is_down(self) -> bool:
        """True while an outage is being tracked (a rebuild is armed or running)."""
        return self._down_since is not None

    @property
    def attempt(self) -> int:
        """Failed rebuild attempts so far in the current outage (0 before the first)."""
        return self._attempt

    @property
    def last_error_count(self) -> int:
        """The most recent error count observed (the rise-detection baseline).

        Hosts whose counter read can fail (e.g. a shared-memory value) can feed
        this back into :meth:`observe` to make a failed read a no-op tick.
        """
        return self._last_error_count

    @property
    def first_delay_s(self) -> float:
        """Delay between detecting an outage and the first rebuild attempt."""
        return self._backoff[0]

    def downtime_s(self, now_mono: float) -> float:
        """Seconds since the current outage was detected (0.0 when not down).

        Args:
            now_mono: Current monotonic time, from the same clock as ``observe``.
        """
        return 0.0 if self._down_since is None else now_mono - self._down_since

    def observe(
        self, now_mono: float, error_count: int, *, stream_open: bool
    ) -> ReconnectAction | None:
        """Advance the state machine one tick and return the action to take.

        Args:
            now_mono: Current monotonic time.
            error_count: The host's cumulative upload-error count. Only a RISE
                versus the previous tick matters; the absolute value is opaque.
            stream_open: Whether the host still holds an open write-stream.
                Quiet-window recovery is only offered while it does -- with the
                stream already torn down there is nothing to resume on.

        Returns:
            ``"arm"`` when an outage was just detected, ``"recovered"`` when a
            pending rebuild was cancelled because uploads went quiet, or
            ``"reconnect"`` when the host should rebuild the session now.
            ``None`` when nothing is due this tick.
        """
        errors_rose = int(error_count) > self._last_error_count
        if errors_rose:
            self._last_rise_mono = now_mono
        self._last_error_count = int(error_count)

        if errors_rose and self._down_since is None:
            self._down_since = now_mono
            self._attempt = 0
            self._next_reconnect_at = now_mono + self._backoff[0]
            return "arm"

        if (
            self._down_since is not None
            and stream_open
            and now_mono - self._last_rise_mono >= self._recovery_quiet_s
        ):
            # Uploads went quiet while the stream is still held: the failure
            # cleared on its own (the SDK resumes on the same stream). Cancel
            # the rebuild; a later rise re-arms, so a truly-dead stream is
            # still rebuilt.
            self._reset_outage()
            return "recovered"

        if (
            self._down_since is not None
            and self._next_reconnect_at is not None
            and now_mono >= self._next_reconnect_at
        ):
            return "reconnect"

        return None

    def on_open_failed(self, now_mono: float) -> float:
        """Record a failed rebuild attempt and schedule the next one.

        Args:
            now_mono: Current monotonic time, sampled AFTER the failed close +
                reopen (which can take many seconds on a dead network) so the
                next delay is not measured from a stale tick timestamp.

        Returns:
            The delay in seconds until the next rebuild attempt (the backoff
            schedule advanced by one, capped at its final entry).
        """
        self._attempt += 1
        delay = self._backoff[min(self._attempt, len(self._backoff) - 1)]
        self._next_reconnect_at = now_mono + delay
        return delay

    def on_open_succeeded(self, now_mono: float, error_count: int) -> None:
        """Record a successful rebuild: the outage is over.

        Clears the outage state and re-baselines rise detection, so errors
        counted during the outage don't immediately re-arm against the fresh
        stream.

        Args:
            now_mono: Current monotonic time, sampled after the reopen.
            error_count: The host's error count after the reopen (the new
                rise-detection baseline).
        """
        self._reset_outage()
        self._last_error_count = int(error_count)
        self._last_rise_mono = now_mono

    def _reset_outage(self) -> None:
        """Clear the outage-tracking state (baseline is kept by the callers)."""
        self._down_since = None
        self._attempt = 0
        self._next_reconnect_at = None
