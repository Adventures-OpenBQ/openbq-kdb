# Bounded diagnostics and runtime output qualification

2026-11-22, native macOS arm64: KDB-X 5.0/2026.07.23 m64, PyKX 4.2.0,
Python 3.12.13. Disposable machine credentials, synthetic ticks and temporary
logs only; no deployment or provider data.

RED: new Python tests could not import the absent diagnostics/drain module.
The native diagnostics read was unavailable, then tests exposed dropped error
records and reserved-field override before FINOS processing. A separately seeded
whole binary-log chunk whose application failed demonstrated a ready server
serving partial replay; this now fails closed before listening. Torn-tail valid
prefix recovery remains qualified independently and returns exactly the original
two occurrences without duplicates.

Final command (set `Q_BINARY` to an operator-supplied licensed q executable):

```sh
PYKX_UNLICENSED=true OPENBQ_TEST_Q="$Q_BINARY" \
  .superpowers/sdd/2026-11-22-kdb-finos-authorization-logging/venv/bin/python \
  -m pytest tests q/tests -q
```

Result: **231 passed, 1 skipped**. Native coverage includes role-protected bounded
reads, UTC nanoseconds, 1,100-event ring overwrite, per-event-class denial rate
limits and observable suppression, reserved-field/secret-payload filtering,
changed boot ID/cursor reset, logger failure preserving original tick-log append
failure/no mutation, unrecoverable replay refusal, valid torn-tail repair and
bootstrap raw runtime/license-output redaction. Python tests drain a multi-MiB
noisy child with a 32 KiB redacted-marker tail, block an output logger while
polling/overflow continue, bound shutdown, recover boot cursors and exercise
plain connection lifecycle records. All PyKX calls/conversions remain on the
session owner thread.

The wheel build includes `diagnostics.q` and the exact unmodified FINOS
`q/structlog/structlog.q` from commit
`e54032a57fd4d9c1288ef4807f2c9f7c22d1c290`, SHA-256
`196040a6ed75f6979fa525b5a02f05d45d97f0797b191121ce810de1f6183b30`, plus
Apache license and provenance. An installed-wheel server launched from outside
the checkout with Python **`-S`**, using only stdlib server dependencies,
accepted an authenticated synthetic tick and returned FINOS-processed
`server_ready` diagnostics over native IPC. No raw token/license output appeared
on its wrapper output. FINOS's default table is never populated.

Full-package Ruff and `git diff --check` pass. The unrelated duplicated inner
`math` import noted in the historical named-source receipt was removed by the
parent's integration cleanup; its behavior is unchanged.

Defaults/interfaces: q ring 1,000 records × <=4 KiB, diagnostic reads <=100,
INFO threshold (tick successes DEBUG), rate 10/second/fixed event class.
`DiagnosticPump(store, logger=None, interval=1.0, queue_size=1000).start()` uses a
separate bounded output worker; `close(timeout=.5)` is bounded. Counters:
`pending,dropped,overwritten,failures`. Named store method:
`read_diagnostics(after_sequence=0,limit=100)`. Explicitly grant diagnostics role.
Bootstrap and session-spawned children both continuously drain raw bytes and keep
only bounded safe redaction markers; no uncontrolled startup text is retained.

Limits: native `-U` rejections can precede q's application callback, so this is
not a complete credential-rejection audit. A permanently blocked Python logger
can leave its single daemon worker until process exit; queue/poller/ingestion
remain independent. Direct q launches outside the wrapper can still block on a
container output driver. Linux amd64/arm64 and the normal-load ingestion/latency
regression gate remain separate, unmeasured qualification work. No guarantee of
durable audit retention is made.
