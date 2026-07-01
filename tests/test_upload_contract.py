"""Upload-CLI contract tests: argument grammar + output semantics.

Imports only ``nominal_link``; no subprocess is ever launched here (the host owns
launching -- this only encodes the CLI grammar and how to read its output).
"""

from __future__ import annotations

from nominal_link import (
    UPLOADER_DISTRIBUTION,
    UPLOADER_PIP_SPEC,
    output_indicates_partial_failure,
    output_indicates_uploader_missing,
    tdms_subcommand_argv,
)


def test_tdms_subcommand_argv_shape():
    assert tdms_subcommand_argv("myprofile", "/data/run.tdms") == [
        "tdms",
        "--profile",
        "myprofile",
        "--path",
        "/data/run.tdms",
    ]


def test_uploader_distribution_and_pip_spec():
    assert UPLOADER_DISTRIBUTION == "proxima_fusion"
    assert UPLOADER_PIP_SPEC.startswith("proxima_fusion @ git+")


def test_partial_failure_detected_on_processing_complete_marker():
    assert output_indicates_partial_failure("Processing complete with 3 failure(s)") is True
    assert output_indicates_partial_failure("Processing complete with 1 failure(s)") is True


def test_partial_failure_false_on_clean_output():
    assert output_indicates_partial_failure("Run created: rid=abc123\nAll files uploaded.") is False


def test_uploader_missing_source_markers():
    # Source checkout: uv could not find the upload_tdms console script.
    assert output_indicates_uploader_missing("error: upload_tdms program not found") is True
    assert output_indicates_uploader_missing("Failed to spawn: upload_tdms") is True


def test_uploader_missing_frozen_markers():
    # Frozen .exe: the build was packaged without proxima_fusion.
    assert output_indicates_uploader_missing("upload_tdms not bundled in this build") is True
    assert output_indicates_uploader_missing("Nominal upload unavailable in this build") is True


def test_uploader_missing_requires_both_source_tokens():
    # "program not found" without the upload_tdms mention is NOT the missing-uploader case.
    assert output_indicates_uploader_missing("some other program not found") is False
    # A genuine upload failure is not a missing-uploader skip.
    assert output_indicates_uploader_missing("Processing complete with 2 failure(s)") is False
