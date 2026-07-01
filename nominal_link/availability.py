"""Runtime availability probes for the Nominal integration.

Lets the host decide -- without a try/except scattered through its code -- whether
live streaming can run (the public ``nominal`` SDK is importable). The TDMS-upload
path probes the uploader differently (it shells out to a console script), so its
availability is determined from the subprocess output via :mod:`nominal_link.upload`.
"""

from __future__ import annotations


def nominal_sdk_available() -> tuple[bool, str]:
    """Return whether the public ``nominal`` SDK can be imported in this Python.

    Returns:
        ``(True, "")`` when ``nominal.core`` imports, else ``(False, reason)``
        with an operator-facing reason string.
    """
    try:
        import nominal.core  # noqa: F401
    except Exception as exc:
        return False, f"`nominal` package not importable in this Python: {exc}"
    return True, ""
