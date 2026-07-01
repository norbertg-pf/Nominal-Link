"""Data-model tests: presets -> asset keys, channels -> run metadata.

Imports only ``nominal_link`` -- no DAQUniversal, no live Nominal.
"""

from __future__ import annotations

from nominal_link import (
    REQUIRED_PRESET_FIELDS,
    build_asset_key_for_preset,
    build_run_metadata,
)


class _FakeChannelConfig:
    """Duck-typed stand-in for the host's ChannelConfig (name/custom_name/kind/scale)."""

    def __init__(self, name, custom_name="", kind="ANALOG", scale=1.0):
        self.name = name
        self.custom_name = custom_name
        self.kind = kind
        self.scale = scale


def test_required_preset_fields_contents():
    assert REQUIRED_PRESET_FIELDS["Tape"] == ("Supplier_ID", "Tape_ID", "Sample_ID", "Test_Instance")
    assert REQUIRED_PRESET_FIELDS["Cable"] == ("Name", "Test_Instance")
    assert REQUIRED_PRESET_FIELDS["Magnet"] == ("Name", "Test_Instance")
    assert set(REQUIRED_PRESET_FIELDS) == {"Tape", "Cable", "Magnet"}


def test_asset_key_tape_fully_populated():
    key, name = build_asset_key_for_preset(
        "Tape",
        {"Supplier_ID": " S1 ", "Tape_ID": "T2", "Sample_ID": "X3", "Test_Instance": "run"},
    )
    # Whitespace is stripped; only the keying fields land in the asset key.
    assert key == {"Asset Type": "Tape", "Supplier_id": "S1", "Tape_id": "T2", "Sample_id": "X3"}
    assert name == "Tape Supplier S1 Tape T2 Sample X3"


def test_asset_key_tape_missing_field_returns_none():
    # A blank required keying field -> (None, None).
    key, name = build_asset_key_for_preset("Tape", {"Supplier_ID": "S1", "Tape_ID": "", "Sample_ID": "X3"})
    assert (key, name) == (None, None)


def test_asset_key_cable():
    assert build_asset_key_for_preset("Cable", {"Name": "C7"}) == (
        {"Asset Type": "Cable", "Name": "C7"},
        "Cable C7",
    )
    assert build_asset_key_for_preset("Cable", {"Name": "  "}) == (None, None)


def test_asset_key_magnet():
    assert build_asset_key_for_preset("Magnet", {"Name": "M9"}) == (
        {"Asset Type": "Magnet", "Name": "M9"},
        "Magnet M9",
    )
    assert build_asset_key_for_preset("Magnet", {}) == (None, None)


def test_asset_key_unknown_preset_type():
    assert build_asset_key_for_preset("Widget", {"Name": "z"}) == (None, None)


def test_run_metadata_test_site_and_scale_factor():
    cfgs = [
        _FakeChannelConfig("ai0", custom_name="Voltage", kind="ANALOG", scale=2.5),
        _FakeChannelConfig("ai1", custom_name="", kind="ANALOG", scale="bad"),
    ]
    meta = build_run_metadata(
        {"Test_Site": "SiteA", "Other": "ignored"},
        cfgs,
        ["ai0", "ai1"],
    )
    assert meta["Test_Site"] == "SiteA"
    # custom_name wins the prefix; a non-numeric scale falls back to "1.0".
    assert meta["Voltage_Scale_Factor"] == "2.5"
    assert meta["ai1_Scale_Factor"] == "1.0"


def test_run_metadata_filter_channel_marker():
    cfgs = [_FakeChannelConfig("FLT0", custom_name="Filtered", kind="FILTER", scale=3.0)]
    meta = build_run_metadata({}, cfgs, ["FLT0"])
    # A FILTER channel carries a marker, not a scale factor.
    assert meta == {"Filtered_Filter_Channel": "true"}


def test_run_metadata_omits_absent_test_site_and_unknown_signals():
    cfgs = [_FakeChannelConfig("ai0", custom_name="V", scale=1.0)]
    meta = build_run_metadata({}, cfgs, ["ai0", "does_not_exist"])
    assert "Test_Site" not in meta
    assert meta == {"V_Scale_Factor": "1.0"}
