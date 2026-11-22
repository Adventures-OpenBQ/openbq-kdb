# OpenBQ q protocol v1

The packaged [manifest](../openbq_kdb/resources/protocol-v1.json) declares the
named operations, role unions, argument/result shapes and limits. The shared
[browser schema](../openbq_kdb/resources/chart-stream-v1.schema.json) and
[golden transcripts](../openbq_kdb/resources/chart-stream-v1.examples.json)
remain the browser contract. A source API does not establish WebSocket,
rendering, deployment or load qualification.

## Local startup and authenticated transport

Use `python -m openbq_kdb.bootstrap --q /absolute/path/to/q --port 5000
--bind 0.0.0.0 --mode tickplant --data-dir /data`. `--bind` defaults to loopback;
keep q on the private service network. `--workspace-mb` optionally sets q workspace containment and is preserved by
spawned KdbSession. Tickplant requires an explicit local data directory; logs remain in `DATA_DIR/logs/tlog_YYYY.MM.DD` with the existing
binary message format. `--mode qualification` without a data directory retains
the ping-only qualification endpoint. Supplying `--data-dir` enables the full
data source for compatibility with qualification harnesses. No browser talks
directly to q.

The launcher uses only Python's standard library. It validates and snapshots the
password/role files, starts q with `-p 0 -U PASSWORD_FILE`, loads trusted local
assets, then opens the configured listener. `--init-script` is an optional
operator-controlled local file, never an RPC argument. Failures before readiness
exit nonzero without opening the port. No network console, HTTP evaluator or
WebSocket evaluator is installed. Both synchronous and asynchronous IPC use the
same exact envelope validator. Async errors are contained; they do not replace
handlers. Native `-U` authenticates the token before `.z.pw` checks readiness and
principal membership, and prevents hook removal through `\x`.

`KDB_AUTH_PASSWORDS_FILE` is a regular permission-restricted file, at most 64 KiB,
containing `identity:token` lines. Tokens have 32–256 `[A-Za-z0-9_-]` characters;
operators generate random machine tokens. One final newline is accepted. Empty
lines, duplicate identities and malformed tokens fail closed. POSIX group/other
permissions are forbidden. `KDB_AUTH_ROLES_FILE` contains exactly
`{"schema_version":1,"principals":{"identity":["role"]}}`; principal sets must
match, repeated JSON keys and unknown/duplicate roles are rejected. Identities
match `[A-Za-z][A-Za-z0-9_.-]{0,63}`. No secrets are created by the launcher.

Clients use process-owned `KDB_USERNAME` and `KDB_PASSWORD_FILE`. Either variable
selects secured mode, and incomplete/invalid configuration fails closed. The
secret file contains one token with an optional final newline. KdbSession loads
it once and uses authenticated qualification for every endpoint/reconnect, with
no expression fallback. Authentication, authorization and protocol failures are
classified with safe stable exception codes and stop endpoint selection
immediately; only transport failures enter the ordinary backoff/bypass path. Unconfigured sessions retain v10 legacy behavior.

## Native envelope and owner-thread client

The native envelope is `(`.openbq.rpc; `operation; args; "request-id")`, with
symbol atoms in its first two positions. Arguments are data, never expressions.
The request ID is an ASCII char vector of 1–128 bytes. Operation membership,
principal role grants, arity and per-operation payload types/limits are checked
before work. The fixed handler registry is created locally; there is no remote
registration, setter, table-name selector, `value`, `eval` or source argument.
Schema/prototypes are created locally before listening.

q canonicalizes a homogeneous list: a singleton view dictionary becomes a
one-row table, and timestamp-only arguments become a timestamp vector. Those
canonical forms are admitted **only** for `readChartSnapshot`, `pruneTicks`, `dayCount` and
`readDiagnostics`, at their declared arity and with full semantic validation. Other
operations still require a general list. This narrow exception is not a remote
query/table execution path.

`connect_qualified` uses `SyncQConnection(no_ctx=True,
reconnection_attempts=-1, connection_timeout=5, timeout=5)` and symbolic ping
with `debug=False, skip_debug=True`. Context introspection and debug expression
wrappers are incompatible with this endpoint. The handshake requires v1, ready
true and a unique textual capability list containing ping; it accepts expanded
capabilities. `KdbSession.capabilities` is empty before qualification and contains
the peer's advertised capability set afterward. `session.rpc(operation,*args)`
constructs arguments and converts replies on the permanent owner thread.
KdbStore selects NamedKdbStore in secured mode; no schema expressions or temporary
remote variables are sent. Chart methods return JSON-like Python dictionaries,
including recursive UTF-8 text decoding and q table-to-record-list conversion.

## Named data and accepted chart source

Tick writes/probes permit one batch of up to 100,000 rows. The exact schema is
`time:timestamp,sym:symbol,price:float,size:float`; public/exported trades retain
these four columns. Multiset probes count matching occurrences, preserving v10's
uncertain-write spool behavior. A successful probe does not accept another
batch or advance chart sequence. Existing cache dtype conformity and inclusive
bar/coverage endpoints remain; chart/EOD end bounds are exclusive. Bars permit
100,000 rows, 128 primitive data columns, 8 MiB encoded payload. Executable and
nested provider columns are rejected. Columns beginning `qw` are reserved to
prevent q SQL parameter shadowing. REST JSON is capped at 64 KiB; tick reads at
1,000 rows; EOD slices at 250,000 within one day. The diagnostics role may read the bounded operational event ring through
`readDiagnostics`; it cannot change logging configuration.

Persistence retains append-before-apply semantics, torn-tail repair and daily
roll from v10. An unrecoverable whole-chunk replay/application error now aborts
bootstrap instead of admitting a partially replayed ready plant. Trusted boot replay temporarily uses the local replay handler
before networking is possible; authentication applies only to network ingress.
The private stream records accepted batches only after durable append and public
apply. A fresh nondeterministic epoch is generated after every restart; replayed
rows receive new-epoch identities without changing the binary logs.

`streamStatus()` returns `epoch`, `low_cursor`, `high_cursor`, `current_cursor`
(an alias of high), `coverage`, server `clock` and `limits`.
`readChartSnapshot(view)` returns `view,cursor,view_floor,coverage,truncated,items`.
`readChartChanges(cursor,view)` returns
`from_cursor,through_cursor,coverage,unavailable_buckets,batches|candles`, or
`reason,current_cursor,coverage` for an explicit reset. Cursors and candle
revisions are `{epoch,seq}`; sequence values and tickCount are decimal strings.
Every chart time is UTC ISO with nine fractional digits, produced in q before
PyKX conversion. Candle items and tick items match the shared structural schema.

Snapshots capture data and H in one q turn, order ticks by `(event_time,seq)`,
and retain the newest max_items (10,000 ticks/2,000 candles) within an 8 MiB
logical response budget. A truncated view reports the first retained key as its
floor; an untruncated view keeps the requested start. HOST preserves per-view
floors during subsequent merges and handles view-key/checkpoint binding.
Accepted changes retain original batch identity, preserve duplicate occurrences,
and scan the entire global (C,H] interval even when symbols filter all rows out.
A response never pages part of an interval while aggregating future ticks.
Late ticks return full touched-candle replacements at response H; equal-time
open/close uses accepted sequence as the tie-break. Closed candles remain
revisable. Clock/status transitions are handled by HOST without fabricated ticks.

The accepted ring retains at most 1,000,000 rows, 128 MiB and 60 seconds. A
conservative 128-byte/row charge covers its bounded symbol/numeric payload
without serializing the whole ring during ingestion. Catch-up scans at most
100,000 accepted rows and returns explicit reset when the response cannot fit.
Epoch changes, expired/ahead cursors, catch-up excess and pruning coverage loss
reset explicitly. Pruning aligns private identity metadata with the four-column
public table. Cursors predating a prune reset; fresh snapshots advance their
effective floor and continue using the original requested view. Partial pruned
candle buckets are omitted and late revisions report unavailable bucket keys.
A private-stream apply failure after durable public apply marks the server
unready and exits; restart reconstructs coherent metadata from the old log.
Coverage is always marked incomplete: retained ticks are not proof that an
upstream provider supplied every trade. No continuous-market completeness is
inferred from an empty result.

## Qualification limits

Native macOS arm64 KDB-X/PyKX socket tests cover authenticated operations,
expression denial, cache/tick/export paths, duplicate identities, filtered
progress, late candle revisions, expiry/pruning, restart/replay and secured
owner-thread session/store integration. Existing simulation/regression tests
remain useful but do not establish Linux deployment safety. Linux amd64/arm64,
full slow-browser transport, rendered chart and normal-load latency gates remain
separate qualification. The 10 ms q-call target and <=10% p95 ingestion regression
are targets, not measured claims in this milestone.

Application limits apply after IPC deserialization; no preallocation bound is
claimed. Keep controlled machine credentials, private networking and runtime
memory containment. [KX callback documentation](https://code.kx.com/q/ref/dotz/)
and [native password files](https://code.kx.com/q/basics/cmdline/) describe the
runtime transport mechanisms.

## Bounded structured diagnostics

Tickplant loads the unmodified FINOS structlog source pinned at
`e54032a57fd4d9c1288ef4807f2c9f7c22d1c290` from packaged vendor assets. The
adjacent provenance records its SHA-256 and Apache license. Production config
replaces all default processors before `getLogger`; the upstream default table
stays empty. UTC JSON events are appended only to a 1,000-record q ring, at
most 4 KiB per record. No operational logging writes stdout/files/network on
the tick path. Default INFO excludes successful-batch DEBUG events. The fixed
event vocabulary and typed field whitelist prevent payload/credential fields
or reserved metadata overrides. Rate limiting admits 10 events/second/fixed
event class; suppression totals are emitted on the next event or diagnostic
read after the second changes. Counters and limiter keys are bounded.

`readDiagnostics(after_sequence,limit)` requires diagnostics role, returns
`boot_id,events,next_sequence,overwritten`, and permits at most 100 encoded JSON
records per read. Reads are non-destructive. A changed boot ID requires a cursor
reset; the Python pump rereads from zero. Ring overwrites are explicit, so this
is operational visibility rather than a durable audit archive. Native `-U`
credential rejection can occur before q's application callback and is not
claimed as a complete application-level authentication audit.

Enable external output explicitly:
`DiagnosticPump(store, logger=None, interval=1.0, queue_size=1000).start()` from
`openbq_kdb.diagnostics`. It polls through NamedKdbStore/session owner-thread RPC,
passes only plain bounded records to a separate output worker, and emits JSON
through Python's existing logging API. `close(timeout=.5)` is bounded;
`pending,dropped,overwritten,failures` expose overload/output failures. Give the
process identity diagnostics role explicitly. A blocked logger may leave one
daemon output worker until process exit; it cannot block the poller or q owner.
The optional pump also accepts fixed client connection lifecycle records through
a nonblocking session sink. No Python structlog dependency is added.

Spawned-q stdout/stderr is continuously consumed by a byte-only daemon drain.
It retains at most 32 KiB of **fixed redaction markers**, never raw runtime or
license text, and exposes a safe byte-count startup classification. It never
calls an output logger. This deliberately sacrifices raw startup excerpts to
avoid guessing a secret grammar for uncontrolled q/OS output. The bootstrap launcher uses the same drain and never forwards raw child output.
A q process launched directly outside this wrapper can still block on its own
container output driver; that residual path is not controlled by this package. Container log rotation/memory containment remain
operator configuration, separate from the bounded application event ring.
