"""Write-stream session tests using a FAKE Nominal client.

The real ``nominal`` SDK is never imported here: ``open_stream_session`` takes
the client *class* as an argument, so a fake stands in. Imports only
``nominal_link``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from nominal_link import create_stream_run, open_stream_session


class _FakeStreamCtx:
    def __init__(self):
        self.entered = False

    def __enter__(self):
        self.entered = True
        return "stream-handle"

    def __exit__(self, *exc):
        return False


class _FakeDataset:
    def __init__(self, scope):
        self.scope = scope
        self.write_stream_calls = []

    def get_write_stream(self, *, batch_size, max_wait):
        self.write_stream_calls.append((batch_size, max_wait))
        return _FakeStreamCtx()


class _FakeAsset:
    name = "asset-name"
    rid = "asset-rid-123"

    def __init__(self):
        self.get_dataset_calls = []

    def get_dataset(self, scope):
        self.get_dataset_calls.append(scope)
        return _FakeDataset(scope)


class _FakeClient:
    """Records the calls open_stream_session / create_stream_run make on it."""

    from_profile_calls = 0

    def __init__(self, profile):
        self.profile = profile
        self.asset = _FakeAsset()
        self.create_run_kwargs = None

    @classmethod
    def from_profile(cls, profile):
        cls.from_profile_calls += 1
        return cls(profile)

    def get_or_create_asset_by_properties(self, asset_key, *, name):
        self.last_asset_key = asset_key
        self.last_asset_name = name
        return self.asset

    def create_dataset(self, name):  # pragma: no cover - not hit on the happy path
        return _FakeDataset("created")

    def create_run(self, **kwargs):
        self.create_run_kwargs = kwargs
        return type("Run", (), {"rid": "run-rid-999"})()


def test_open_stream_session_reresolves_client_each_call():
    _FakeClient.from_profile_calls = 0
    kw = dict(
        profile="p",
        asset_key={"Asset Type": "Cable", "Name": "C1"},
        asset_name="Cable C1",
        dataset_scope="stream",
        batch_size=500,
        max_wait_td=timedelta(seconds=1),
    )
    open_stream_session(_FakeClient, **kw)
    open_stream_session(_FakeClient, **kw)
    # Re-resolving the client each call is what refreshes an expired token on reconnect.
    assert _FakeClient.from_profile_calls == 2


def test_open_stream_session_returns_open_handles_and_forwards_batch_params():
    client, asset, dataset, stream_ctx, stream = open_stream_session(
        _FakeClient,
        profile="p",
        asset_key={"Asset Type": "Cable", "Name": "C1"},
        asset_name="Cable C1",
        dataset_scope="stream",
        batch_size=250,
        max_wait_td=timedelta(seconds=2),
    )
    assert client.last_asset_key == {"Asset Type": "Cable", "Name": "C1"}
    assert client.last_asset_name == "Cable C1"
    assert dataset.write_stream_calls == [(250, timedelta(seconds=2))]
    assert stream_ctx.entered is True
    assert stream == "stream-handle"


class _DeadNetworkClient:
    @classmethod
    def from_profile(cls, profile):
        raise ConnectionError("network is down")


def test_open_stream_session_raises_when_network_down():
    # Each step is a network call; on a dead network open_stream_session must
    # raise so the host's reconnect loop treats it as "still down" and backs off.
    with pytest.raises(ConnectionError):
        open_stream_session(
            _DeadNetworkClient,
            profile="p",
            asset_key={},
            asset_name="a",
            dataset_scope="stream",
            batch_size=1,
            max_wait_td=timedelta(seconds=1),
        )


def test_create_stream_run_passes_frame_and_stamps_start_time():
    client = _FakeClient("p")
    asset = client.asset
    start = datetime(2026, 7, 1, 12, 30, 0, tzinfo=timezone.utc)
    end = datetime(2026, 7, 1, 13, 0, 0, tzinfo=timezone.utc)
    run = create_stream_run(
        client, asset, run_start=start, run_end=end, run_metadata={"Test_Site": "SiteA"}
    )
    kwargs = client.create_run_kwargs
    assert kwargs["name"] == "Stream Run - 2026-07-01T12:30:00Z"
    assert kwargs["start"] == start
    assert kwargs["end"] == end
    assert kwargs["assets"] == [asset.rid]
    # run_metadata is merged in and Start_Time is stamped from run_start.
    assert kwargs["properties"] == {"Test_Site": "SiteA", "Start_Time": "2026-07-01T12:30:00Z"}
    assert run.rid == "run-rid-999"
