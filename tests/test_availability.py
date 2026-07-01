"""SDK-availability probe tests.

``nominal_sdk_available`` only asks whether ``nominal.core`` imports. These tests
drive both branches by faking / breaking that import via ``sys.modules`` so they
need neither the real Nominal SDK installed nor a live connection.
"""

from __future__ import annotations

import sys
import types

from nominal_link import nominal_sdk_available


def test_sdk_available_true_when_core_imports(monkeypatch):
    # Inject a fake nominal.core so the probe succeeds without the real SDK.
    nominal_mod = types.ModuleType("nominal")
    core_mod = types.ModuleType("nominal.core")
    nominal_mod.core = core_mod
    monkeypatch.setitem(sys.modules, "nominal", nominal_mod)
    monkeypatch.setitem(sys.modules, "nominal.core", core_mod)

    ok, reason = nominal_sdk_available()
    assert ok is True
    assert reason == ""


def test_sdk_available_false_when_core_import_fails(monkeypatch):
    # A None entry in sys.modules makes `import nominal.core` raise ImportError.
    monkeypatch.setitem(sys.modules, "nominal.core", None)

    ok, reason = nominal_sdk_available()
    assert ok is False
    assert "nominal" in reason
