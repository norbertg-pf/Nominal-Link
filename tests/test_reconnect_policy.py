"""ReconnectPolicy tests: the pure outage/rebuild decision state machine.

Deterministic by construction -- the policy never reads a clock, so every test
drives it with hand-picked monotonic timestamps. Imports only ``nominal_link``.
"""

from __future__ import annotations

import pytest

from nominal_link import RECONNECT_BACKOFF_S, RECOVERY_QUIET_S, ReconnectPolicy

_BACKOFF = (10.0, 20.0, 40.0)


def _policy(**overrides) -> ReconnectPolicy:
    kwargs = {"backoff_s": _BACKOFF, "recovery_quiet_s": 5.0}
    kwargs.update(overrides)
    return ReconnectPolicy(**kwargs)


def test_defaults_come_from_the_streaming_tunables():
    policy = ReconnectPolicy()
    assert policy.first_delay_s == RECONNECT_BACKOFF_S[0]
    # The quiet window is honoured: a rise at t=0 must not recover before
    # RECOVERY_QUIET_S even with the stream open.
    policy.observe(0.0, 1, stream_open=True)
    assert policy.observe(RECOVERY_QUIET_S - 0.1, 1, stream_open=True) is None
    assert policy.observe(RECOVERY_QUIET_S + 0.1, 1, stream_open=True) == "recovered"


def test_empty_backoff_rejected():
    with pytest.raises(ValueError):
        ReconnectPolicy(backoff_s=())


def test_arms_on_first_error_rise_only():
    policy = _policy()
    assert policy.observe(0.0, 0, stream_open=True) is None
    assert policy.observe(1.0, 3, stream_open=True) == "arm"
    assert policy.is_down is True
    # Further rises while already down keep tracking but never re-arm.
    assert policy.observe(2.0, 5, stream_open=True) is None


def test_quiet_recovery_cancels_a_pending_rebuild():
    policy = _policy()
    policy.observe(0.0, 1, stream_open=True)
    # Uploads stay quiet past the window while the stream is still held.
    assert policy.observe(5.5, 1, stream_open=True) == "recovered"
    assert policy.is_down is False
    # A later rise re-arms, so a truly-dead stream is still rebuilt.
    assert policy.observe(6.0, 2, stream_open=True) == "arm"


def test_no_quiet_recovery_without_an_open_stream():
    policy = _policy()
    policy.observe(0.0, 1, stream_open=True)
    # Stream already torn down (mid-rebuild): quiet means nothing to resume on,
    # so the pending reconnect must fire instead of a false recovery.
    assert policy.observe(5.5, 1, stream_open=False) is None
    assert policy.observe(10.0, 1, stream_open=False) == "reconnect"


def test_ongoing_errors_defer_recovery_until_quiet():
    policy = _policy()
    policy.observe(0.0, 1, stream_open=True)
    policy.observe(4.0, 2, stream_open=True)  # rise resets the quiet window
    assert policy.observe(8.0, 2, stream_open=True) is None  # only 4s quiet
    assert policy.observe(9.5, 2, stream_open=True) == "recovered"


def test_reconnect_due_after_first_delay():
    policy = _policy()
    policy.observe(0.0, 1, stream_open=True)
    assert policy.observe(9.9, 2, stream_open=True) is None  # rises keep quiet-reset
    assert policy.observe(10.0, 2, stream_open=True) == "reconnect"
    # Still "reconnect" on the next tick until the host reports an outcome.
    assert policy.observe(10.1, 2, stream_open=True) == "reconnect"


def test_failed_opens_walk_the_backoff_schedule_and_cap():
    policy = _policy()
    policy.observe(0.0, 1, stream_open=True)
    # Failure n consumes backoff[min(n, last)]: 20, 40, then capped at 40.
    assert policy.on_open_failed(100.0) == 20.0
    assert policy.attempt == 1
    assert policy.observe(119.9, 1, stream_open=False) is None
    assert policy.observe(120.0, 1, stream_open=False) == "reconnect"
    assert policy.on_open_failed(120.5) == 40.0
    assert policy.on_open_failed(200.0) == 40.0  # capped at the final entry
    assert policy.attempt == 3


def test_successful_open_resets_and_rebaselines_error_count():
    policy = _policy()
    policy.observe(0.0, 1, stream_open=True)
    policy.observe(10.0, 4, stream_open=True)  # errors kept rising while down
    policy.on_open_succeeded(11.0, 4)
    assert policy.is_down is False
    assert policy.attempt == 0
    assert policy.last_error_count == 4
    # The outage-era count is the new baseline: no phantom re-arm...
    assert policy.observe(12.0, 4, stream_open=True) is None
    # ...but a genuine new failure on the fresh stream arms again.
    assert policy.observe(13.0, 5, stream_open=True) == "arm"


def test_last_error_count_supports_failed_counter_reads():
    policy = _policy()
    policy.observe(0.0, 7, stream_open=True)
    # A host whose shared-counter read fails feeds the baseline back in,
    # making the tick a no-op for rise detection.
    assert policy.last_error_count == 7
    assert policy.observe(1.0, policy.last_error_count, stream_open=True) is None


def test_downtime_reports_seconds_since_detection():
    policy = _policy()
    assert policy.downtime_s(50.0) == 0.0
    policy.observe(100.0, 1, stream_open=True)
    assert policy.downtime_s(130.0) == 30.0
