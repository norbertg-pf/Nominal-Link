# nominal_link

The **Nominal integration boundary** for DAQUniversal. A small, host-agnostic
layer that owns everything *Nominal-specific* about uploading and streaming, and
nothing about the DAQ application itself.

This is the package **owned and optimised by Nominal**. It imports nothing from
DAQUniversal, so it can be reviewed, tuned, and released independently without
exposing the host's acquisition, quench-detection, or plotting code.

## The split

| Concern | Lives in | Why |
|---|---|---|
| Nominal SDK client/asset/dataset/write-stream handshake (+ bounded close of a dead stream) | `nominal_link` | Nominal protocol knowledge |
| Reconnect/backoff tunables + the decision policy (`ReconnectPolicy`) | `nominal_link` | Outage-recovery behaviour Nominal owns |
| Session lifecycle supervisor (`StreamSession`: open / rebuild / final close / run framing) | `nominal_link` | Composes the protocol + policy into one owned unit |
| Data model: preset → asset key/name, run metadata | `nominal_link` | The Nominal data model |
| Upload-CLI argument grammar + output semantics | `nominal_link` | The `upload_tdms` contract |
| SDK availability probe | `nominal_link` | One place to ask "can we stream?" |
| Streaming child-process lifecycle + CPU pinning | DAQUniversal | Host priority contract (safety) |
| Inter-process counters, the reconnect loop *mechanism*, block averaging | DAQUniversal | Host data pipeline |
| Upload gating, retries, progress bars, event log | DAQUniversal | Host UX + policy |
| Process launching (`uv run` vs frozen re-exec), `shutil.which`, timeouts | DAQUniversal | Host execution environment |

Rule of thumb: **this package knows HOW to talk to Nominal; the host decides
WHEN and WHAT to send.**

## Public API

```python
from nominal_link import (
    # data model
    REQUIRED_PRESET_FIELDS, build_asset_key_for_preset, build_run_metadata,
    # write-stream session, supervisor, reconnect tunables + decision policy
    open_stream_session, close_stream_ctx, create_stream_run, ReconnectPolicy, StreamSession,
    MIN_SAFE_MAX_WAIT_MS, RECONNECT_BACKOFF_S, RECOVERY_QUIET_S, RECONNECT_CLOSE_TIMEOUT_S,
    # upload-CLI contract
    tdms_subcommand_argv, output_indicates_partial_failure, output_indicates_uploader_missing,
    UPLOADER_DISTRIBUTION, UPLOADER_PIP_SPEC,
    # availability
    nominal_sdk_available,
)
```

The `nominal` SDK is imported **lazily** by the caller: the host passes the
`NominalClient` class into `open_stream_session`, so merely importing
`nominal_link` never requires the SDK to be installed.

The `upload_tdms` CLI (from the private `proxima_fusion` / `ext-proxima-fusion`
package) is invoked as an **external subprocess by the host** — it is never
imported here. This module only encodes its argument grammar and how to read its
output.

## Install

```bash
pip install -e .                 # runtime: streaming boundary (pulls the `nominal` SDK)
pip install -e ".[dev]"          # + pytest / ruff for the test suite
pip install -e ".[upload]"       # + the private `upload_tdms` CLI (proxima_fusion)
```

### Extras

| Extra | Adds | For |
|---|---|---|
| *(base)* | `nominal_link` + `nominal` SDK | Live streaming |
| `[upload]` | + `proxima_fusion` (`upload_tdms` CLI) | Post-acquisition `.tdms` upload |
| `[dev]` | pytest, ruff | Running the test suite |

The `[upload]` extra is a **direct git reference** to the private
`ext-proxima-fusion` repo. It is resolved **only** when `[upload]` is explicitly
requested — the base install, `[dev]`, and this repo's CI (`uv run --no-sync`)
never fetch it, so none of them need private-repo access.

DAQUniversal consumes this package **optionally** and directly from git (never a
declared dependency there — a private git source would break its `uv`
resolution/CI). One command installs everything Nominal — the streaming boundary,
the SDK, and the uploader:

```bash
uv pip install "nominal_link[upload] @ git+https://github.com/norbertg-pf/Nominal-Link.git@main"
```

Drop the `[upload]` for streaming only. When the package is absent, DAQUniversal
still runs: Nominal streaming and the Nominal `.tdms` upload simply grey out, and
every other feature (including Google Drive uploads) is unaffected.

## Tests

```bash
pytest
```

The suite imports **only** `nominal_link` and fakes the Nominal SDK where a test
needs it, so it runs without a live Nominal connection or the real SDK.

## Layout

```
pyproject.toml       # this package's build + deps
README.md
nominal_link/
  __init__.py        # re-exports the public API
  model.py           # preset → asset key/name; run-metadata; REQUIRED_PRESET_FIELDS
  streaming.py       # open_stream_session / close_stream_ctx / create_stream_run + reconnect tunables
  reconnect.py       # ReconnectPolicy: the outage/rebuild decision state machine
  session.py         # StreamSession: supervised open / rebuild / final close / run framing
  upload.py          # upload_tdms argument grammar + output semantics + install spec
  availability.py    # nominal_sdk_available()
tests/               # package-level tests (import only nominal_link)
docs/
  integration_boundary.md   # the full integration spec (ownership split, API, flows, SDK surface)
```
