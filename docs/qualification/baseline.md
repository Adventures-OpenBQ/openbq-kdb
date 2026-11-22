# v10 baseline and protocol qualification — 2026-11-22

This receipt covers task 1, before full data operations and chart integration.
The user selected v10 backend and subsequently requested the v10 lightweight-
charts Live Chart as the first consumer; charts/v12 follows later.

## Sources

- KDB execution base: `6d2035bf58007b33d950f8c272fd0b718c432ac5`, v10 review-fix
  merge, exactly the commit named by HOST's source.json. Local main `c249571`
  was stale, and tag v10.0.0 resolves to older `9519bbcc`; neither is the base.
- HOST baseline: `e5e848833dc7371bb827ac4868806f11c5c6ead9`, the v10.0.0
  line `8c9c361bef4f323fa8df4d439e12694f1c666fdf` plus multi-architecture image pin.
- CLIENT execution base: v10.0.0 `1667ada996146d9d1d164e98607890c61e04442a`.
- Later charts/v12 reference only: `b52dc1b6a5ddc42ceda30b9fa5bbca73bf477cd0`.
- HOST pins q image index
  `ghcr.io/artcashin/kdb-x:1.1@sha256:b7fe12489fb255c23273609b6c878889cb249a011b34a132d46a1b2801443894`.
  The image was not downloaded or launched for this native qualification.

The machine-readable `v10-vendor-drift.json` records drift in HOST's vendored
session and two tests versus its declared KDB source commit. The session
change reports a non-loopback bind endpoint as host:port. This is why running
HOST tests against the upstream editable KDB initially produced two endpoint-
label failures. Running them with the actual shipped vendor source resolves
both. No recovery/data logic was changed to conceal those failures.

## Reproducible environment and baselines

An isolated Python 3.12.13 environment was created for this work. Versions match
the locally present v10 live-grid image where relevant: PyKX 4.2.0, pandas 3.1.1,
FastAPI 0.162.5, httpx 0.28.1, deltalake 1.6.6 and polars 2.1.0.
`python-environment.txt` records the complete qualification environment; it is
not a replacement production dependency lock. No shared interpreter was changed.

| Check | Result |
|---|---|
| KDB `python -m pytest tests q/tests -q`, before changes | 160 passed, 1 skipped |
| HOST tests/test_live_profile.py, test_lake_profile.py, test_kdb_build.py | 35 passed |
| HOST live-grid full default suite, actual vendored KDB on PYTHONPATH | 579 passed, 14 network tests deselected |
| HOST scripts/tests/test_lake_scripts.py | 15 passed |
| KDB full suite with task-1 additions | 199 passed, 1 skipped |
| Installed wheel, tests copied outside checkout, no PYTHONPATH | 39 passed |
| Ruff on changed Python files | passed |

The existing skipped KDB multiset test requires licensed embedded PyKX. This is
distinct from the licensed external q runtime used by the socket tests. Full
Ruff exposes pre-existing F811 duplicate `import math` in aggregate.py, present
in the execution base; it is not fixed as part of protocol qualification.
Existing asyncio-fixture, Starlette/httpx and OpenBB ClientSession deprecation
warnings were recorded; no tests were suppressed to hide them.

## Real runtime results

Native macOS arm64 KDB-X 5.0, release 2026.07.23 (`m64`), was launched in
disposable processes with disposable machine tokens. No provider calls or
production tick data were used and no license contents were collected.

RED evidence: bare q accepted wrong, missing and unknown credentials, failing
all three native-auth rejection tests. The new credential loader and packaged
bootstrap tests also failed before implementation. The secured server passes
real socket tests for token rejection, symbolic ping, expression rejection,
async handler replacement denial, no listener before initialization, nonzero
exit on local initialization/policy failure, version mismatch with no downgrade,
and explicit HTTP/WS denial including valid Basic credentials.

Two runtime details changed the implementation:

1. PyKX context introspection must be disabled with no_ctx=True before connection
   construction. Otherwise the constructor itself sends forbidden expressions.
2. HTTP auth `.z.ac` requires a two-item result `(0;"")` for denial, not boolean
   false; the stricter test caught a 500 type error and now observes 401/403.

The package wheel includes bootstrap.q and all protocol JSON assets. Installed-
wheel tests import the installed module from a separate virtual environment
and directory, then repeat real authentication, schema and credential tests.
Source-tree resource presence alone was not treated as packaging proof.

`workload-native.json` records twenty 100,000-row scan and aggregation proxies:
scan 0 ms at q's millisecond timer resolution, aggregation 0–1 ms. This supports
the proposed 100,000-row scan admission cap / 10 ms target for further testing;
it is not a benchmark of serialized chart responses or concurrent ingestion.

## Still unqualified

Only authenticated ping is executable in the task-1 bootstrap. Role/data API,
existing v10 recorder migration, log persistence under restricted callbacks,
chart snapshots/resume/candle state, slow browsers, Linux amd64/arm64 containers,
and rendered v10/v12 clients are not certified by this receipt. The frozen
schema/examples are contracts, not evidence of live chart implementation.

Application payload checks are post-deserialization. KX's `.z.bm` is a malformed-
message callback, not a proven preallocation limit. No untrusted-sandbox or
power-loss durability guarantee is made. Runtime/OS stdout backpressure remains
separate from the planned bounded application diagnostic path.
