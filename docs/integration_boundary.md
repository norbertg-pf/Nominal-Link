# Integration boundary (`nominal_link`)

`nominal_link` is the boundary between a data-acquisition **host** and **Nominal**.
It owns *how to talk to Nominal*; the host owns *when and what to send*. The
package imports nothing host-specific, so Nominal can own, review, and optimise it
independently — it never sees the host's acquisition, quench-detection, or plotting
code.

The reference host is Proxima Fusion's DAQUniversal, which consumes this package
optionally (see *How a host consumes this package* below). This document describes
only the boundary — the public surface and the contract — not any host internals.

## Two integration paths

| Path | When | Nominal dependency | How it's used |
|---|---|---|---|
| **Live streaming** | continuous, during a recording | public `nominal` SDK (`nominal.core.NominalClient`) | the host runs a low-priority producer that calls `open_stream_session` and enqueues samples |
| **`.tdms` file upload** | one-shot, after a recording stops | private `upload_tdms` CLI (`proxima_fusion`, the `ext-proxima-fusion` repo) | the host shells out to the CLI as a subprocess |

Streaming imports the SDK (lazily — see below); the file-upload path only shells
out to the CLI and **never imports it**.

## What this package owns

```
nominal_link/
  model.py         preset → asset key/name; run-metadata property bag; REQUIRED_PRESET_FIELDS
  streaming.py     open_stream_session / create_stream_run + reconnect tunables
  upload.py        upload_tdms argument grammar + output semantics + install spec
  availability.py  nominal_sdk_available()
```

- **Data model** — `build_asset_key_for_preset`, `build_run_metadata`,
  `REQUIRED_PRESET_FIELDS`: the mapping from rig presets (Tape / Cable / Magnet)
  to Nominal asset keys, names, and per-run properties.
- **Write-stream session** — `open_stream_session` (client → asset → dataset →
  write-stream handshake, re-resolving the client each call so an auth token that
  expired during an outage refreshes on reconnect) and `create_stream_run`.
- **Reconnect tunables** — `RECONNECT_BACKOFF_S`, `RECOVERY_QUIET_S`,
  `RECONNECT_CLOSE_TIMEOUT_S`, `MIN_SAFE_MAX_WAIT_MS`. The knobs governing outage
  recovery — the part Nominal is best placed to tune for a given site's network.
- **Upload-CLI contract** — `tdms_subcommand_argv` (argument grammar),
  `output_indicates_partial_failure` ("Processing complete with N failure(s)" ⇒
  failure despite exit 0), `output_indicates_uploader_missing` (uploader absent ⇒
  a skip, not a failure), `UPLOADER_DISTRIBUTION`, `UPLOADER_PIP_SPEC`.
- **Availability** — `nominal_sdk_available()`.

## What the host owns (deliberately not in this package)

- **Streaming process lifecycle, CPU pinning, and priority.** The host runs
  streaming as its lowest-priority producer so a reconnect storm can never starve
  higher-priority work (on the reference host, quench detection). That safety
  property is the host's.
- **The reconnect *loop* and inter-process counters.** This package provides the
  primitives and tunables; the host drives them — the state machine, the metrics,
  the control channel, and any block averaging.
- **Upload gating, retries, progress, and logging.** Preset validation, retry with
  backoff, the cancellable/timed subprocess, and all operator-facing surfaces.
- **Process launching.** How the `upload_tdms` CLI is invoked (e.g. `uv run
  --no-sync upload_tdms …` from a source checkout, or a re-exec from a frozen
  build), `PATH` resolution, and subprocess management.

Rule of thumb: **this package knows HOW to talk to Nominal; the host decides WHEN
and WHAT to send.**

## Public API

```python
from nominal_link import (
    # data model
    REQUIRED_PRESET_FIELDS, build_asset_key_for_preset, build_run_metadata,
    # write-stream session + reconnect tunables
    open_stream_session, create_stream_run,
    MIN_SAFE_MAX_WAIT_MS, RECONNECT_BACKOFF_S, RECOVERY_QUIET_S, RECONNECT_CLOSE_TIMEOUT_S,
    # upload-CLI contract
    tdms_subcommand_argv, output_indicates_partial_failure, output_indicates_uploader_missing,
    UPLOADER_DISTRIBUTION, UPLOADER_PIP_SPEC,
    # availability
    nominal_sdk_available,
)
```

## Streaming flow

1. The host builds the asset key + run metadata (`build_asset_key_for_preset`,
   `build_run_metadata`) and starts its streaming producer.
2. The producer calls `open_stream_session(NominalClient, …)`, then loops:
   consume a chunk → (optionally) block-average → `stream.enqueue(name, ts_ns, value)`.
3. On a sustained rise in the SDK's dropped-batch error count, the host tears the
   stream down and re-opens via `open_stream_session` on the `RECONNECT_BACKOFF_S`
   schedule; a blip that clears within `RECOVERY_QUIET_S` cancels the rebuild.
4. On stop: flush, then `create_stream_run(...)` frames the run.

## `.tdms` upload flow

1. After a recording stops, the host validates `REQUIRED_PRESET_FIELDS` for the
   active preset.
2. The host builds the command with `tdms_subcommand_argv(profile, path)`, wrapped
   in whatever launcher its runtime uses, and runs it as a cancellable, timed
   subprocess with retry/backoff.
3. The host reads the result via `output_indicates_partial_failure` /
   `output_indicates_uploader_missing`. Because the CLI returns exit 0 even when
   individual files fail, `output_indicates_partial_failure` is what turns
   "Processing complete with N failure(s)" into a real failure; a missing uploader
   is a skip (the recording is safe on disk regardless).

## Nominal SDK assumptions

The `nominal` SDK is imported **lazily by the caller**: the host passes the
`NominalClient` *class* into `open_stream_session`, so merely importing
`nominal_link` never requires the SDK to be installed. `nominal_sdk_available()`
probes whether `nominal.core` imports in the current interpreter.

The write-stream path expects this SDK surface:

- `NominalClient.from_profile(profile)`
- `client.get_or_create_asset_by_properties(asset_key, name=…)`
- `asset.get_dataset(scope)` / `client.create_dataset(name)` / `asset.add_dataset(data_scope_name=…, dataset=…)`
- `dataset.get_write_stream(batch_size=…, max_wait=<timedelta>)` (a context manager)
- `stream.enqueue(channel_name, timestamp_ns, value)`
- `client.create_run(name=…, start=…, end=…, assets=[…], properties={…})`

`MIN_SAFE_MAX_WAIT_MS` exists because the SDK's timeout-flush thread derives its
sleep from `max_wait.seconds` (the whole-seconds component), so a sub-second
`max_wait` truncates to 0 and busy-spins a core — the host clamps up to this floor.

Targets Python `>=3.11,<3.12` and `nominal>=0.5` (see `pyproject.toml`).

## How a host consumes this package

`nominal_link` is **optional** and installed from git (it is deliberately not a
declared dependency of the host, so the host's dependency resolution and CI never
need access to this private repo). One command installs everything Nominal — the
streaming boundary + SDK **and** the `upload_tdms` uploader (via the `[upload]`
extra):

```bash
uv pip install "nominal_link[upload] @ git+https://github.com/norbertg-pf/Nominal-Link.git@main"
```

Drop `[upload]` for streaming only. A well-behaved host imports this package
through its own optional-dependency shim and disables its Nominal features cleanly
when the package is absent — nothing else in the host should break.

## Ownership & tests

This repository is Nominal's to own and optimise. Its tests
(`tests/`) cover **this package's** contract in isolation — they import only
`nominal_link` and fake the SDK, so they run without a live Nominal connection.
The **host-behaviour** tests (the reconnect loop, retry policy, inter-process
counters, and preset/metadata wiring) live in the host's repository, since they
exercise host code that drives these primitives.
