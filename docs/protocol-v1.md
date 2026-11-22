# OpenBQ q protocol v1 — qualification contract

Backend target: v10. Charts/v12 consumes this API later, after the data and
subscription operations are implemented. This stage implements **ping only**;
it does not change v10's running session, recorder, exports or Compose stack.

The normative machine manifest is
[`protocol-v1.json`](../openbq_kdb/resources/protocol-v1.json).
Browser structural schemas and golden transcripts are
[`chart-stream-v1.schema.json`](../openbq_kdb/resources/chart-stream-v1.schema.json)
and [`chart-stream-v1.examples.json`](../openbq_kdb/resources/chart-stream-v1.examples.json).
They are packaged in the wheel so later Python and TypeScript consumers can
pin identical bytes. Golden transcripts are specification examples, not proof
that the future chart data path already works.

## Native envelope and qualification

The q IPC message is exactly a general list:

```
(`.openbq.rpc; `ping; (); "request-id")
```

The first two values are **symbol atoms**, not q expression strings. Arguments
are a general list even when empty. Request ID is an ASCII character vector of
1–128 bytes. No input is passed to `value`, `eval`, or a client-selected
function lookup. In this stage arguments must be empty and operation must be
`ping`; data-operation names in the manifest are unavailable until later tasks.

PyKX 4.2.0 serializes the intended envelope with:

```python
conn(kx.SymbolAtom(".openbq.rpc"), kx.SymbolAtom("ping"),
     kx.List([]), kx.CharVector("handshake"), debug=False, skip_debug=True)
```

Use `SyncQConnection(no_ctx=True, reconnection_attempts=-1, ...)`. The default
context-enabled constructor sends anonymous context-inspection expressions and
is incompatible with the restricted endpoint. Debug wrapping also changes the
message shape; disable it. The supplied `connect_qualified` helper verifies a
ready v1 response and closes on mismatch, without probing another endpoint or
downgrading to expression execution. This helper must run on the existing
session owner thread when integrated in task 3.

`ping` returns q dictionary `protocol_version=1i`, `ready=1b`, and a symbol vector
of actually implemented capabilities (`ping` only now). The qualification helper
is intentionally stage-specific; capability expansion and role checks accompany
the later full client migration. Never advertise chart_stream_v1 now.

## Credentials and startup

`KDB_AUTH_PASSWORDS_FILE` names a regular, permission-restricted file, at most
64 KiB, with one `user:token` per line. A single final newline is accepted; blank
lines, duplicate identities, colons in tokens and malformed lines are rejected.
Tokens are 32–256 characters from `[A-Za-z0-9_-]`, intended to be randomly
generated machine tokens. Length alone is not an entropy test. POSIX secret
files must grant no group/other permissions. No credentials are created for
operators by the test harness; tests generate and destroy disposable values.

`KDB_AUTH_ROLES_FILE` contains exactly `schema_version:1` and `principals`, an
object mapping identities to nonempty unique arrays of known roles. The identity
sets must match the password file. Repeated JSON keys, unknown roles, extra
top-level keys and malformed values fail closed. Identity syntax is
`[A-Za-z][A-Za-z0-9_.-]{0,63}`. Role unions and future operation grants are in the
manifest. The current server permits authenticated ping for every configured
identity; it does not pretend to enforce yet-unimplemented operations.

Clients use process-owned `KDB_USERNAME` and `KDB_PASSWORD_FILE`; per-request
credentials do not select identities. Client files accept exactly one token
with an optional single final newline. Secret values are omitted from object
reprs and configuration errors. This loader is not yet wired into legacy v10
KdbConfig/KdbSession.

The opt-in launcher is `python -m openbq_kdb.bootstrap --q /absolute/path/to/q
--port PORT`. It snapshots the validated credentials/policy into an owner-only
temporary directory, starts q with `-p 0 -U PASSWORD_FILE`, and opens loopback
only after trusted local initialization and handler installation succeed. Its
optional `--init-script` is an operator-controlled local file, never a remote
RPC parameter. There is no console on a network listener and no unrestricted
fallback. Failure of local initialization exits nonzero without opening the
port. The wrapper forwards termination and cleans up its owned child and
temporary credentials.

q's native `-U` verifies the token before `.z.pw` checks readiness and membership.
It also prevents hook deletion via `\\x`. No `-u 1` is added: its effect on remote
contexts performing legitimate tick-log file writes needs task-2 qualification.
HTTP GET/POST/other methods and WebSocket upgrades are denied in this minimal
qualification mode, even with valid HTTP Basic credentials. Future tick-plant
handlers require separate authenticated transport qualification.

## Data operations reserved for later tasks

The manifest fixes every operation's positional arguments, q result shape,
roles and proposed bounds, including prototypes, bars/coverage/LRU, tick writes,
multiset probes/pruning, REST snapshots, EOD chunks, diagnostics and chart reads.
Timestamps are native q timestamp atoms for native RPC. Existing bar reads and
coverage use inclusive endpoints; EOD and chart view end times are exclusive.
No operation accepts arbitrary table names or executable source.

Tick writes/probes preserve the existing 100,000-row maximum live batch and
5,000-row replay acknowledgment units; no partial batch splitting occurs in
KdbStore. Bar payloads allow at most 100,000 rows, 128 columns, and 8 MiB of
encoded payload; dynamic provider columns require a data-only type validator in
task 2. REST snapshot JSON is at most 64 KiB. EOD slices are at most 250,000 rows
within one UTC day. Limit/schema errors must be rejected before mutation.
Zero rows are a legitimate result, not evidence of complete provider coverage.

## Browser stream contract reserved for charts/v12

`/chart_stream_ws` is an authenticated live-grid/gateway endpoint, not public q.
It is distinct from legacy coalesced quote rows and TA figure/delta sockets.
Authentication and symbol access are checked on each admission/resume; a cursor
does not grant access. Fresh single-use gateway tickets are required per socket.
The selected HOST exposes ticket minting through `/grid/ws_ticket`; its current
socket allowlist does not yet admit the proposed endpoint.

The schema defines subscribe/resume/ack/unsubscribe/pong and snapshot begin,
chunks/end, ticks, candles, candle_status, resumed, resync_required, heartbeat,
unsubscribed and error. All frames carry protocol_version=1 and subscription_id;
post-admission frames carry an opaque generation, fresh on each admission.
Client controls carry request_id. Decimal strings encode sequences/counters;
consumers must additionally enforce int64 range, ordering and generation/epoch
binding. The structural JSON schema alone is not a state-machine validator.

Cursors are `{epoch,seq}`; tick IDs use the same shape, even for byte-identical
trades. Native restart creates a new epoch; binary logs do not change. Snapshot
and watermark H come from one q turn; readChartChanges scans the full retained
interval to H or returns resync_required. There is no partial-interval candle
aggregate at a later watermark. A bounded accepted-data buffer retains at most
1,000,000 ticks/128 MiB/60 seconds; chart reads scan at most 100,000 accepted rows
per call. Backlogs exceeding that scan cap resync. The 10 ms per-call work target
is a measured admission/qualification target, not a hard q preemption promise.

Snapshot caps: 10,000 ticks or 2,000 candles, 8 MiB per logical response and
512 KiB per snapshot chunk, counting envelopes. The atomic model is installed
only at snapshot_end. `view_floor` is `{"kind":"tick","time":UTC,"seq":SEQ|null}`
or `{"kind":"candle","bucket_start":UTC}`. Preserve it with cursor/view_key in
the checkpoint; spare max_items capacity after byte truncation does not permit
older omitted history to reappear. Merge and trim newest items while the floor
only advances. `view_key` is SHA-256 of UTF-8 JSON for the normalized view with
sorted keys, no whitespace, ASCII escaping, explicit normalized defaults;
timestamps normalize to UTC with nine fractional digits. Missing end stays
absent, ticks omit interval, and supplied values override max_items defaults
(10,000 ticks/2,000 candles) before hashing. This is specification binding, not
a signature. Golden examples contain normalized views and their actual hashes;
task 6 must consume these same fixtures when implementing normalization.

Changes carry from_cursor/through_cursor so interleaved symbols and empty
filtered intervals advance coherently. Partial overlap applies only unseen
tick IDs/newer candle revisions. A forward hole, expired/ahead/wrong-epoch
cursor, unavailable coverage, wrong view, oversized catch-up or slow consumer
requires explicit reset. Ack means fully applied model progress. Quiet markets
may send empty progress frames, never fabricated trades.

Full candle replacements use key `(symbol,interval,bucket_start)`, revision H,
server-calculated OHLCV/VWAP/tickCount, and open/closed status. Late ticks revise
old buckets. Equal event times tie-break by accepted sequence. Never add
replacement volume to previous volume. Status-only transitions use status_seq,
reset each generation, and do not advance data cursor. Resume includes an
authoritative status baseline for every retained candle key in the checkpoint,
even with no new trades. Status changes do not conflict with equal data revisions.

Per-connection pending bytes (unsent plus unacknowledged, including transport
buffers and staged snapshots) are capped at 8 MiB or 100 messages; process-wide
pending bytes at 128 MiB, with 64 connections and 8 subscriptions/connection.
Control messages: 64 KiB and 10/second/connection. Each connection reserves part
of its budget for bounded control output; no unlimited heartbeat queue. Send
deadline is 5 seconds; pending-data application progress and heartbeat liveness
must be demonstrated within 15 seconds. On overload, reset explicitly or close
4009 when reset cannot be delivered; never block q ingestion behind a browser.
Reconnect uses jittered 0.5–15 second backoff, reset after 30 stable seconds.

## Runtime resource limits and remaining qualification

Application validation happens after q deserialization. q's documented `.z.bm`
reports malformed IPC structures; it is not a preallocation byte ceiling or
application authorization hook. No total deserialization-allocation bound has
been qualified here. Keep controlled machine credentials, private networking
and runtime/container memory containment; do not advertise an untrusted sandbox.
References: [KX callback behavior](https://code.kx.com/q/ref/dotz/) and
[native password files](https://code.kx.com/q/basics/cmdline/).

The frozen normal-load fixture is 4 symbols, 1,000 ticks/second aggregate,
250 ms flushes (250-row normal batches), 8 responsive clients with one view each,
10 seconds warmup and 60 seconds measured duration. Final targets remain p95
accept-to-model <=250 ms and <=10% ingestion p95 regression. The native 100,000-row
scan/aggregation proxy informs the 10 ms work target, but does not establish
serialization, retention-buffer, concurrent-client or full chart API performance.
Both Linux architectures, full role/data API, persistence integration, slow
clients and rendered charts/v12 remain later qualification gates.
