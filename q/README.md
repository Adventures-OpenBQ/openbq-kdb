<!-- Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# openbq-kdb q — the tick cache speaks websocket

`startup.q` turns the Ep. 10 kdb+ tick cache into a live publisher: any
websocket + JSON client can subscribe to trades and pull OHLCV bars from the
same q that live-grid records into — no pykx, no q client library. The
consumer this was built for is **bdobb-v2's live chart**; `live-chart.html`
here is the reference implementation of that client.

The pattern is Jonathon McMurray's chained-tickerplant-over-websockets
([blog post](https://jonathonmcmurray.github.io/kdb/q/websockets/2019/08/12/ws-server.html),
[ws.q](https://github.com/jonathonmcmurray/ws.q), MIT). His library drags in
the qutil package system, so the ~40 lines that matter are inlined in
`startup.q` instead — and co-located in the cache's own q rather than run as
a separate chain process, because the cache IS the tickerplant here.

## Protocol

One websocket, on the same port q already serves IPC on:

    client -> {"type":"sub","syms":["AAPL","BTC-USD"]}
    server -> {"table":"trades","data":[{"time":"...","sym":"AAPL","price":1.0,"size":2.0},...]}
              (immediately: a snapshot of the last 500 cached ticks per sym;
               then: every batch live-grid flushes, filtered to your syms)

    client -> {"type":"bars","sym":"AAPL","interval":60}      # seconds
    server -> {"table":"bars","sym":"AAPL","data":[{"barTime":...,"open":...,"high":...,
               "low":...,"close":...,"vwap":...,"volume":...,"tickCount":...},...]}

    client -> {"type":"avwap","sym":"AAPL","anchor":"2026-08-28T14:30:00"}
    server -> {"table":"avwap","sym":"AAPL","anchor":"...","pv":...,"vol":...,
               "data":[{"time":...,"avwap":...},...]}

    client -> {"type":"stats"}
    server -> {"table":"stats","memory":{"used":...,"heap":...,"peak":...,
                                         "wmax":...,"mphy":...},"rows":N,
               "syms":[{"sym":...,"time":...,"price":...,"n":...},...]}
              (.Q.w[]'s ledger -- the numbers the -w limit judges -- plus
               last-per-sym from a by-clause, kdb's signature keyed shape.
               wmax is the -w ceiling, 0 when none is set; mphy is the host's
               physical memory. Both are what `used`/`heap`/`peak` have to be
               judged against -- without them a client can read the numbers
               but not know how much room is left.)

    client -> {"type":"asof","sym":"AAPL","times":["2026-08-28T18:30:00",...]}
    server -> {"table":"asof","sym":"AAPL","data":[{"time":...,"price":...,"size":...},...]}
              (aj: the last trade AS OF each requested time -- the as-of
               join, kdb's canonical primitive; null before the first trade)

Two read-only HTTP endpoints (q's .z.ph -- defining it also kills q's default
browser console, which evaluates code) feed the NAS-side EOD flush through
tailscale Serve, which proxies only HTTP and so never exposes raw q IPC:

    GET /syms                          -> ["AAPL",...]
    GET /day?date=2026-08-28&sym=AAPL  -> that UTC day's ticks, JSON

Anchored VWAP is the cumulative `(sums price*size)%sums size` from the anchor
forward — computed in q because the ticks are already here; recomputing it in
a client-side dataframe would mean shipping every tick since the anchor on
each refresh. `pv`/`vol` are the running totals, so a subscribed client
extends the series per tick without re-requesting:
`avwap = (pv += p*s) / (vol += s)`.

**Per-bar `vwap` is the composition primitive.** Each bar's `vwap * volume`
is exactly that bar's `sum(price*size)`, so a client can build VWAP variants
from bars alone, trade-true, without ever touching ticks:

    anchored from bar k:   Σ(vwap_i·vol_i, i≥k) / Σ(vol_i, i≥k)
    rolling over N bars:   Σ(vwap_i·vol_i, last N) / Σ(vol_i, last N)
                           (a 30-minute rolling vwap = six 5-minute bars)

The same primitive rides through every aggregation layer: this websocket's
`bars`, live-grid's tick-derived `/series` bars, and the ArcticDB provider's
resample (`vwap=true` query param) over flushed ticks in the HDB.

A second `sub` replaces the first; closing the socket unsubscribes. Defining
`.z.ws` is also a hardening step: q's default evaluates whatever a websocket
client sends.

## How ticks get here

live-grid's recorder flushes tick batches every ~250 ms through
`openbq-kdb`'s `write_ticks`, which calls `upd` when the server defines it
(this script does: insert + publish) and falls back to a bare insert on a
plain q. Nothing else in the stack changes.

## Deploy

> **openbq-docker v10:** nothing to copy. The `live` profile mounts this directory's
> `startup.q` and `persist.q` read-only at `/opt/openbq-kdb/` and runs
> `q /opt/openbq-kdb/startup.q`; `KDB_PERSIST_Q` points startup.q at persist.q, and the tick
> logs live on the named volume `kdb-data`. Start it with
> `docker compose --profile live up -d kdb`. There is no host port on this line (the
> donor text below describes the donor's `127.0.0.1:5999` publish).
> `tests/test_persist.py` pins persist.q's behaviour and `tests/tickplant_sim.py`
> models it for the crash-recovery tests in openbq-kdb and live-grid. The donor's
> instructions follow.

    cp q/startup.q kdb-data/startup.q
    docker compose up -d kdb

The compose file publishes the port at `127.0.0.1:5999` for bdobb-v2.
**Read the warning on the kdb service in docker-compose.yml first** — q IPC
shares that port and executes arbitrary code with no auth. Loopback only,
never serve/funnel it.

## Test

    ./run-test.sh        # throwaway KDB-X on :5998, full protocol smoke test

Checks: pub with no subscribers, snapshot on subscribe, sym-filtered live
push, the exact guarded expression `write_ticks` sends, cache accumulation,
and the bars query. Needs docker, node ≥ 22, and a licence in ../kdb-license.

## Demo

    docker exec -d openbb-kdb bash -c 'q /data/gen.q < <(tail -f /dev/null)'  # synthetic ticks, or use the real feed
    python3 -m http.server 8123 --bind 127.0.0.1 --directory q
    open http://127.0.0.1:8123/live-chart.html

Candlesticks build live from the stream: `bars` request for history, `sub`
for the tail, last bar updated in place per tick — the exact shape bdobb-v2's
real-time chart widget needs.

The toolbar previews the v10 tool model. **⚓ anchor** (default): click the
chart to anchor a new AVWAP at that time — served trade-true by kdb, then
extended live from its `pv`/`vol` totals; up to six, distinct colors.
**╱ segment** and **↗ ray**: two clicks draw a line — pure client geometry,
nothing goes to the server. A segment is closed (ends at its points); a ray
is open, defined by its first point plus the **recorded slope** (price per
second) and ridden out to the latest bar as ticks arrive. **ƒ fib**: two
clicks (swing start = 100%, swing end = 0%) draw labeled retracement levels
(23.6/38.2/50/61.8/78.6), extended to the live edge. **∠ gann**: one click
fans 30/45/60° lines — up from a relative low, down from a relative high
(judged against the surrounding bars' range); the angles are measured on
screen at draw time and recorded as price-per-second slopes, so redraws
reproduce the same fan after any rescale. **─ level**: one click drops a
horizontal support/resistance line at that price — recorded as the single
price, rendered by the platform's own price-line primitive. Drawings never
drive the price autoscale — a fan's far end would squash the candles.

Everything is recorded in **chart coordinates** — anchors by their start
time, drawings by (time, price) points and slope, never pixels — and
persisted per symbol in `localStorage`, so a refresh replaces each series
exactly: anchors re-request their trade-true series from kdb, drawings
redraw from their recorded geometry. **🗑 clear** forgets it all.

The trades table carries the `` `g# `` grouped attribute on `sym`: sym-filtered
queries use a group index instead of scanning (measured ~25% on 641k rows
with two symbols; the win grows with watchlist breadth). Inserts maintain it;
`upd` re-applies it after a prune's table reassignment drops it.

## q traps this file already paid for

- A lone `/` on its own line in a q script opens a block comment that
  silently swallows the rest of the file.
- `-500#table` on a table shorter than 500 rows cycles rows instead of
  clamping — snapshot takes must clamp to `count`.
- Bars must `time xasc` before `first`/`last` — ticks land out of order.
- `.j.j` serializes floats at display precision, default 7 significant
  digits — enough to corrupt a vwap on the wire. `startup.q` sets `\P 17`.
