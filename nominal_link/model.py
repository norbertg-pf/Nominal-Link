"""Nominal data-model mapping: presets -> assets, channels -> run metadata.

This is the single place that encodes Nominal's data model for this rig: which
preset fields key each asset type, how an asset name is composed, and how the
per-run property bag is built. It is intentionally free of any DAQUniversal
imports -- it operates on plain mappings and duck-typed channel-config objects
(anything exposing ``name``, ``custom_name``, ``kind`` and ``scale``) -- so it
can be lifted into a standalone, Nominal-owned package without change.

Functions/Classes:
- REQUIRED_PRESET_FIELDS: the keying fields each asset type needs non-empty.
- build_asset_key_for_preset: preset_type + fields -> (asset_key, asset_name).
- build_run_metadata: per-run property dict (Test_Site + per-channel markers).
"""

from __future__ import annotations

from typing import Mapping

# TDMS root-property keys the Nominal uploader needs present AND non-empty for
# each preset -- the "required keying properties" per the Nominal data model:
# the bold core properties for each asset type. ``Type`` and ``Start_Time`` are
# always written by the TDMS writer, so they are omitted here.
#   * Tape   -> Supplier_ID, Tape_ID, Sample_ID, Test_Instance
#   * Cable  -> Name, Test_Instance
#   * Magnet -> Name, Test_Instance
# Other model fields (Level, Level_ID, N_Stacks, ...) are non-keying context and
# may be left blank by the operator.
REQUIRED_PRESET_FIELDS: dict[str, tuple[str, ...]] = {
    "Tape": ("Supplier_ID", "Tape_ID", "Sample_ID", "Test_Instance"),
    "Cable": ("Name", "Test_Instance"),
    "Magnet": ("Name", "Test_Instance"),
}


def build_asset_key_for_preset(
    preset_type: str, preset_file_fields: Mapping[str, str]
) -> tuple[dict[str, str] | None, str | None]:
    """Map a preset to a Nominal asset key + display name.

    Args:
        preset_type: One of ``"Tape"``, ``"Cable"``, ``"Magnet"``.
        preset_file_fields: The preset's field values (whitespace is stripped).

    Returns:
        ``(asset_key, asset_name)`` for a known, fully-populated preset, or
        ``(None, None)`` when the type is unknown or a required field is blank.
    """
    fields = {k: str(v).strip() for k, v in (preset_file_fields or {}).items()}
    if preset_type == "Tape":
        supplier, tape, sample = fields.get("Supplier_ID", ""), fields.get("Tape_ID", ""), fields.get("Sample_ID", "")
        if not (supplier and tape and sample):
            return None, None
        return (
            {"Asset Type": "Tape", "Supplier_id": supplier, "Tape_id": tape, "Sample_id": sample},
            f"Tape Supplier {supplier} Tape {tape} Sample {sample}",
        )
    if preset_type == "Cable":
        name = fields.get("Name", "")
        return ({"Asset Type": "Cable", "Name": name}, f"Cable {name}") if name else (None, None)
    if preset_type == "Magnet":
        name = fields.get("Name", "")
        return ({"Asset Type": "Magnet", "Name": name}, f"Magnet {name}") if name else (None, None)
    return None, None


def build_run_metadata(preset_file_fields, active_channel_configs, selected_signals) -> dict[str, str]:
    """Build the Nominal run property bag for a streaming run.

    Emits ``Test_Site`` (when present in the preset) plus one
    ``{custom_name}_Scale_Factor`` per selected channel. Filter (FLT*) channels
    carry a ``{custom_name}_Filter_Channel`` marker instead of a scale factor:
    their values are already in engineering units, so a Nominal consumer can
    tell a derived channel apart from a raw scaled one.

    Args:
        preset_file_fields: The preset's field values.
        active_channel_configs: Iterable of channel-config objects, each exposing
            ``name``, ``custom_name``, ``kind`` and ``scale`` (duck-typed).
        selected_signals: The signal names selected for streaming.

    Returns:
        A flat ``str -> str`` property dict suitable for Nominal run properties.
    """
    cfg_by_name = {cfg.name: cfg for cfg in active_channel_configs}
    fields = {k: str(v).strip() for k, v in (preset_file_fields or {}).items()}
    metadata: dict[str, str] = {}
    test_site = fields.get("Test_Site", "")
    if test_site:
        metadata["Test_Site"] = test_site
    for sig in selected_signals:
        cfg = cfg_by_name.get(sig)
        if cfg is None:
            continue
        custom = (getattr(cfg, "custom_name", "") or "").strip() or sig
        if str(getattr(cfg, "kind", "")) == "FILTER":
            metadata[f"{custom}_Filter_Channel"] = "true"
            continue
        try:
            scale_str = str(float(getattr(cfg, "scale", 1.0)))
        except (TypeError, ValueError):
            scale_str = "1.0"
        metadata[f"{custom}_Scale_Factor"] = scale_str
    return metadata
