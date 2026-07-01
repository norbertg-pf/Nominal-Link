"""Nominal TDMS-upload CLI contract: argument grammar + output semantics.

The post-acquisition ``.tdms`` push shells out to the ``upload_tdms`` console
script from the private ``proxima_fusion`` package (Nominal's
``ext-proxima-fusion`` repo). This module owns the parts of that contract that
are Nominal's: how the CLI's arguments are spelled and how to read its output.
It deliberately does NOT own *how* the host launches the process (``uv run`` in a
source checkout vs the frozen-exe re-exec, ``shutil.which``, timeouts, retries) --
that is host execution-environment plumbing and stays in DAQUniversal.

Because a zero exit code does NOT prove delivery (the CLI catches each file's
exception, logs it, and still returns 0), the host relies on
``output_indicates_partial_failure`` to treat "Processing complete with N
failure(s)" as a failure, and on ``output_indicates_uploader_missing`` to tell a
genuine failure apart from "the optional uploader isn't installed/bundled".

Functions/Constants:
- UPLOADER_DISTRIBUTION / UPLOADER_PIP_SPEC: the package + install spec to surface.
- tdms_subcommand_argv: the ``upload_tdms`` argument vector (program name excluded).
- output_indicates_partial_failure: True when the CLI reported per-file failures.
- output_indicates_uploader_missing: True when the uploader is absent (a skip, not a failure).
"""

from __future__ import annotations

# The optional uploader's distribution name (the package that actually provides
# the ``upload_tdms`` console script) and the recommended pip spec to install it.
# Kept here so the "not installed" guidance has a single source of truth.
#
# The recommended install is this package's own ``[upload]`` extra, which pulls
# ``proxima_fusion`` in -- one command gives a host both the integration boundary
# and the uploader. (Installing ``proxima_fusion`` directly from its own git URL
# also works; this is just the single-command form the host surfaces to users.)
UPLOADER_DISTRIBUTION = "proxima_fusion"
UPLOADER_PIP_SPEC = (
    "nominal_link[upload] @ git+https://github.com/norbertg-pf/Nominal-Link.git@main"
)


def tdms_subcommand_argv(profile: str, path: str) -> list[str]:
    """Return the ``upload_tdms`` argument vector for a single-file TDMS push.

    The program name is excluded so the host can prepend its own launcher
    (``uv run --no-sync upload_tdms`` in a source checkout, or the frozen-exe
    ``--upload-tdms-cli`` re-exec).

    Args:
        profile: Nominal CLI profile name.
        path: Absolute path to the ``.tdms`` file to upload.

    Returns:
        ``["tdms", "--profile", profile, "--path", path]``.
    """
    return ["tdms", "--profile", profile, "--path", path]


def output_indicates_partial_failure(output: str) -> bool:
    """True when the CLI's output reports one or more per-file upload failures.

    The CLI returns exit code 0 even when individual files fail, printing
    ``"Processing complete with N failure(s)"``. The host treats that as a
    failure despite the zero exit code.
    """
    return "Processing complete with" in output


def output_indicates_uploader_missing(output: str) -> bool:
    """True when the output shows the uploader itself is unavailable.

    Distinguishes "the optional uploader isn't installed/bundled" (a skip the
    host should not retry) from a genuine upload failure. Covers both runtimes:

    * source checkout -- ``uv`` could not find the ``upload_tdms`` console script;
    * frozen ``.exe`` -- the build was packaged without ``proxima_fusion``.
    """
    source_missing = "upload_tdms" in output and ("program not found" in output or "Failed to spawn" in output)
    frozen_missing = "not bundled in this build" in output or "unavailable in this build" in output
    return source_missing or frozen_missing
