# Native named data / chart source evidence

Date: 2026-11-22. Base: v10 vendored source commit `6d2035b`, plus stage-1
qualification commit `08d603d`. Runtime: operator-supplied KDB-X 5.0
2026.07.23 m64 (macOS arm64), PyKX 4.2.0, Python 3.12.13. All credentials and
log directories were disposable. No provider/production data or deployment.

RED: the first real authenticated data test failed because `--data-dir` and
`writeTicks` were unavailable in stage 1. Subsequent native tests exposed q
canonical argument forms, replay through `.z.ps`, empty-batch identity handling,
prune/resnapshot floor behavior and midbucket candle-floor handling; all have
regressions in `tests/integration/test_named_data.py`.

Final command:

```sh
PYKX_UNLICENSED=true OPENBQ_TEST_Q=/Users/artcashin/.kx/bin/q \
  .superpowers/sdd/2026-11-22-kdb-finos-authorization-logging/venv/bin/python \
  -m pytest tests q/tests -q
```

Result: **220 passed, 1 skipped**. Fourteen native named-source tests cover
role grant/deny checks for every implemented operation; symbolic authenticated
write/read/probe/cache/export; exact duplicate occurrences and nanoseconds;
filtered global progress; late historical OHLCV replacements; structural schema
conformance; empty/null/oversized batches; original accepted batch identity;
retention/scan limits; pruning and fresh snapshot floors; persistence restart;
fail-stop recovery after private-stream application failure; native auth error
classification; secured session/store owner-thread integration. Existing stage-1
native tests cover denied expressions/async mutation/HTTP/WebSockets and startup
readiness. The nonloopback endpoint reporting matches shipped HOST v10 behavior.

`uv build --wheel --out-dir /tmp/openbq-named-wheel` succeeded. The wheel contains
all four q assets. An extracted installed wheel, launched from outside the
checkout with **Python `-S`** (no third-party server dependencies), successfully
booted a disposable authenticated tickplant and accepted a write followed by a
chart snapshot over real IPC.

Ruff on changed Python/test files and `git diff --check` pass. Full-package Ruff
still reports the pre-existing duplicate `math` import (F811) in `aggregate.py`;
that unrelated source drift is unchanged.

Not qualified here: Linux amd64/arm64 images, full browser/WebSocket delivery,
rendering, normal-load p95 latency/ingestion regression, slow consumers, FINOS
structured diagnostics and bounded runtime-output draining. `readDiagnostics`
remains unavailable and is not advertised. The 10 ms call target and <=10%
ingestion regression gate remain unmeasured targets. The source's incomplete
coverage flag never claims provider completeness or an untrusted IPC sandbox.
