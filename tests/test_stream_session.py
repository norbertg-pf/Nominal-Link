"""StreamSession tests: supervised open / rebuild / shutdown / create_run.

Fully deterministic: a fake client class stands in for the SDK and a fake clock
is injected, so no thread sleeps or wall-clock reads are involved (the bounded
close's daemon thread is exercised separately in test_streaming_session.py).
Imports only ``nominal_link``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from nominal_link import ReconnectPolicy, StreamSession

_BACKOFF = (10.0, 20.0, 40.0)


class _FakeStream:
    pass


class _FakeStreamCtx:
    def __init__(self, ctrl):
        self._ctrl = ctrl
        self.stream = _FakeStream()

    def __enter__(self):
        return self.stream

    def __exit__(self, *exc):
        self._ctrl.closed.append(self)
        if self._ctrl.fail_close:
            raise RuntimeError("flush failed")
        return False


class _FakeDataset:
    def __init__(self, ctrl):
        self._ctrl = ctrl

    def get_write_stream(self, *, batch_size, max_wait):
        self._ctrl.open_calls += 1
        return _FakeStreamCtx(self._ctrl)


class _FakeAsset:
    name = "Fake Asset"
    rid = "asset-rid"

    def __init__(self, ctrl):
        self._ctrl = ctrl

    def get_dataset(self, scope):
        return _FakeDataset(self._ctrl)


class _Ctrl:
    def __init__(self):
        self.profile_calls = 0
        self.open_calls = 0
        self.closed = []
        self.fail_open = False
        self.fail_close = False
        self.create_run_kwargs = None


def _client_cls(ctrl: _Ctrl):
    class _FakeClient:
        @classmethod
        def from_profile(cls, profile):
            ctrl.profile_calls += 1
            if ctrl.fail_open:
                raise ConnectionError("network down")
            return cls()

        def get_or_create_asset_by_properties(self, key, *, name):
            return _FakeAsset(ctrl)

        def create_run(self, **kwargs):
            ctrl.create_run_kwargs = kwargs
            return type("Run", (), {"rid": "run-rid"})()

    return _FakeClient


class _FakeClock:
    def __init__(self, start: float = 0.0):
        self.now = start

    def __call__(self) -> float:
        return self.now


def _session(ctrl: _Ctrl, clock: _FakeClock, **overrides) -> StreamSession:
    kwargs = dict(
        profile="p",
        asset_key={"Asset Type": "Cable", "Name": "C1"},
        asset_name="Cable C1",
        dataset_scope="stream",
        batch_size=10,
        max_wait_td=timedelta(seconds=1),
        policy=ReconnectPolicy(backoff_s=_BACKOFF, recovery_quiet_s=5.0),
        clock=clock,
    )
    kwargs.update(overrides)
    return StreamSession(_client_cls(ctrl), **kwargs)


def test_open_populates_handles_and_returns_stream():
    ctrl, clock = _Ctrl(), _FakeClock()
    session = _session(ctrl, clock)
    assert session.is_open is False
    stream = session.open()
    assert session.is_open is True
    assert stream is session.stream
    assert session.client is not None and session.asset is not None and session.dataset is not None
    assert ctrl.profile_calls == 1 and ctrl.open_calls == 1


def test_observe_delegates_with_the_sessions_own_stream_state():
    ctrl, clock = _Ctrl(), _FakeClock()
    session = _session(ctrl, clock)
    session.open()
    assert session.observe(0.0, 1) == "arm"
    # Quiet recovery is offered because the SESSION still holds the stream.
    assert session.observe(5.5, 1) == "recovered"


def test_rebuild_success_reopens_fresh_client_and_rebaselines_policy():
    ctrl, clock = _Ctrl(), _FakeClock()
    counter = {"value": 1}
    session = _session(ctrl, clock, read_error_count=lambda: counter["value"])
    old_stream = session.open()
    session.observe(0.0, 1)                      # arm
    # Errors keep RISING during a real outage (each batch fails), which is what
    # holds the quiet-window recovery off until the backoff fires.
    assert session.observe(10.0, 2) == "reconnect"
    counter["value"] = 3                         # and rose again mid-rebuild
    clock.now = 12.0
    ok, exc, delay = session.rebuild()
    assert (ok, exc, delay) == (True, None, 0.0)
    # Old stream torn down, new one held; the client was re-resolved (token refresh).
    assert ctrl.closed and session.stream is not old_stream
    assert ctrl.profile_calls == 2
    # Policy reset + re-baselined to the post-reopen count: no phantom re-arm.
    assert session.policy.is_down is False
    assert session.policy.last_error_count == 3
    assert session.observe(13.0, 3) is None
    assert session.observe(14.0, 4) == "arm"


def test_rebuild_failure_backs_off_and_leaves_session_closed():
    ctrl, clock = _Ctrl(), _FakeClock()
    session = _session(ctrl, clock)
    session.open()
    session.observe(0.0, 1)
    # Rising count keeps quiet-recovery at bay until the backoff fires.
    assert session.observe(10.0, 2) == "reconnect"
    ctrl.fail_open = True
    clock.now = 15.0                             # close + reopen took 5s
    ok, exc, delay = session.rebuild()
    assert ok is False and isinstance(exc, ConnectionError)
    assert delay == 20.0                         # backoff advanced to the 2nd entry
    assert session.is_open is False and session.stream is None
    assert session.policy.attempt == 1
    # Next attempt is scheduled from the FRESH clock (15.0), not the tick time.
    assert session.observe(34.9, 2) is None
    assert session.observe(35.0, 2) == "reconnect"


def test_rebuild_uses_injected_seams():
    ctrl, clock = _Ctrl(), _FakeClock()
    seam_calls = {"open": 0, "close": []}

    def _open_seam(client_cls, **kwargs):
        seam_calls["open"] += 1
        from nominal_link import open_stream_session
        return open_stream_session(client_cls, **kwargs)

    def _close_seam(ctx, timeout_s):
        seam_calls["close"].append((ctx, timeout_s))

    session = _session(ctrl, clock, open_session=_open_seam, bounded_close=_close_seam,
                       close_timeout_s=7.5)
    session.open()
    session.observe(0.0, 1)
    session.observe(10.0, 1)
    old_ctx = session.stream_ctx
    ok, _, _ = session.rebuild()
    assert ok is True
    assert seam_calls["open"] == 2               # initial open + rebuild
    assert seam_calls["close"] == [(old_ctx, 7.5)]


def test_rebuild_proceeds_to_reopen_when_bounded_close_raises():
    ctrl, clock = _Ctrl(), _FakeClock()
    warnings: list[str] = []

    def _raising_close(ctx, timeout_s):
        raise RuntimeError("close adapter exploded")

    session = _session(ctrl, clock, bounded_close=_raising_close, on_warning=warnings.append)
    session.open()
    session.observe(0.0, 1)
    session.observe(10.0, 2)
    # A close-path failure must not prevent the reopen (best-effort teardown).
    ok, exc, delay = session.rebuild()
    assert (ok, exc, delay) == (True, None, 0.0)
    assert session.is_open is True
    assert any("close before rebuild failed" in message for message in warnings)


def test_rebuild_read_error_count_failure_falls_back_to_baseline():
    ctrl, clock = _Ctrl(), _FakeClock()

    def _broken_reader():
        raise OSError("shared value gone")

    session = _session(ctrl, clock, read_error_count=_broken_reader)
    session.open()
    session.observe(0.0, 2)
    session.observe(10.0, 2)
    ok, _, _ = session.rebuild()
    assert ok is True
    # Falls back to the policy's last observed count instead of raising.
    assert session.policy.last_error_count == 2


def test_shutdown_close_flushes_directly_and_clears_handles():
    ctrl, clock = _Ctrl(), _FakeClock()
    session = _session(ctrl, clock)
    session.open()
    session.shutdown_close()
    assert ctrl.closed                            # __exit__ ran (flush)
    assert session.is_open is False and session.stream_ctx is None
    session.shutdown_close()                      # idempotent no-op afterwards
    assert len(ctrl.closed) == 1


def test_shutdown_close_raises_through_for_the_host_to_surface():
    ctrl, clock = _Ctrl(), _FakeClock()
    ctrl.fail_close = True
    session = _session(ctrl, clock)
    session.open()
    with pytest.raises(RuntimeError):
        session.shutdown_close()


def test_create_run_frames_the_session_and_requires_an_open():
    ctrl, clock = _Ctrl(), _FakeClock()
    session = _session(ctrl, clock)
    with pytest.raises(RuntimeError):
        session.create_run(
            run_start=datetime(2026, 7, 1, tzinfo=timezone.utc),
            run_end=datetime(2026, 7, 2, tzinfo=timezone.utc),
            run_metadata={},
        )
    session.open()
    start = datetime(2026, 7, 1, 12, 30, 0, tzinfo=timezone.utc)
    run = session.create_run(
        run_start=start,
        run_end=datetime(2026, 7, 1, 13, 0, 0, tzinfo=timezone.utc),
        run_metadata={"Test_Site": "SiteA"},
    )
    assert run.rid == "run-rid"
    assert ctrl.create_run_kwargs["assets"] == ["asset-rid"]
    assert ctrl.create_run_kwargs["properties"]["Test_Site"] == "SiteA"
    # create_run works even after the stream was closed (run framing happens last).
    session.shutdown_close()
    session.create_run(run_start=start, run_end=start, run_metadata={})


def test_full_outage_scenario_down_retry_recover():
    ctrl, clock = _Ctrl(), _FakeClock()
    session = _session(ctrl, clock)
    session.open()
    # Outage: arm at t=0, errors keep rising, rebuild due at t=10 while still down.
    session.observe(0.0, 1)
    assert session.observe(10.0, 2) == "reconnect"
    ctrl.fail_open = True
    clock.now = 10.5
    ok, _, delay = session.rebuild()
    assert ok is False and delay == 20.0
    # Still down at the next due time; second failure caps toward the schedule end.
    assert session.observe(30.5, 2) == "reconnect"
    clock.now = 31.0
    ok, _, delay = session.rebuild()
    assert ok is False and delay == 40.0
    # Network returns before the third attempt.
    ctrl.fail_open = False
    assert session.observe(71.0, 2) == "reconnect"
    clock.now = 71.5
    ok, _, _ = session.rebuild()
    assert ok is True and session.is_open is True
    assert session.policy.is_down is False and session.policy.attempt == 0
