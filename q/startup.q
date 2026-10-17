/ Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
/ SPDX-License-Identifier: Apache-2.0

/ startup.q -- tick cache with websocket pub/sub.
/ Minimal standalone take on jonathonmcmurray's ws.q .wsu namespace
/ (https://github.com/jonathonmcmurray/ws.q, MIT) -- the library itself
/ drags in the qutil package system, so the ~30 lines are inlined here.
/ NB: never leave a lone "/" on its own line in this file -- q reads it as
/ the start of a block comment and silently swallows everything after it.
/ Protocol (JSON over websocket, same port q already listens on):
/   client -> {"type":"sub","syms":["AAPL","BTC-USD"]}
/   server -> {"table":"trades","data":[{"time":"2026-...","sym":"AAPL","price":1.0,"size":2.0},...]}
/   client -> {"type":"bars","sym":"AAPL","interval":60}     (interval in seconds)
/   server -> {"table":"bars","sym":"AAPL","data":[{"barTime":...,"open":...,"high":...,"low":...,"close":...,"vwap":...,"volume":...,"tickCount":...},...]}
/   client -> {"type":"avwap","sym":"AAPL","anchor":"2026-08-28T14:30:00"}
/   server -> {"table":"avwap","sym":"AAPL","anchor":"...","data":[{"time":...,"avwap":...},...],"pv":...,"vol":...}
/             (pv/vol are the running sum(price*size) and sum(size), so a live
/              client can extend the series per tick: avwap=(pv+p*s)%(vol+s))
/   client -> {"type":"stats"}
/   server -> {"table":"stats","memory":{"used":...,"heap":...,"peak":...,
/                                        "wmax":...,"mphy":...},"rows":N,
/              "syms":[{"sym":...,"time":...,"price":...,"n":...},...]}
/             (wmax is the -w ceiling, 0 when none is set; mphy is the host's
/              physical memory. Both are what `used`/`heap`/`peak` have to be
/              judged against -- without them a client can read the numbers
/              but not know how much room is left.)
/   client -> {"type":"asof","sym":"AAPL","times":["2026-08-28T18:30:00",...]}
/   server -> {"table":"asof","sym":"AAPL","data":[{"time":...,"price":...,"size":...},...]}
/             (the last trade AS OF each requested time; null before the first)
/ A second sub replaces the first; closing the socket unsubscribes.

/ .j.j serializes floats at display precision, which defaults to 7
/ significant digits -- enough to corrupt a vwap on the wire. Full doubles:
system"P 17";

/ schema matches openbq-kdb/_INIT_SCHEMA -- idempotent, same guard
if[not `trades in key `.; trades:([] time:`timestamp$(); sym:`symbol$(); price:`float$(); size:`float$())];

/ grouped attribute on sym: sym-filtered queries (subscription filter,
/ avwap, bars) use a group index instead of scanning every row. Inserts
/ maintain it incrementally; the prune's table reassignment drops it, so
/ upd re-applies when it is missing (O(1) to check, O(n) only after a prune)
@[`trades;`sym;`g#];

/ websocket handle -> subscribed syms
.wsu.subs:(`int$())!();

/ ticks per sym pushed as snapshot on subscribe
.wsu.snapN:500;

.wsu.send:{[h;d] (neg h) .j.j `table`data!(`trades;0!d)};

.wsu.sub:{[h;syms]
  syms:(),syms;
  .wsu.subs[h]:syms;
  / neg-take cycles rows when the table is shorter than N, so clamp to count
  snap:raze{[s]t:select from trades where sym=s;(neg .wsu.snapN&count t)#t}each syms;
  if[count snap; .wsu.send[h;snap]];
 };

/ OHLCV+VWAP bars from the tick cache; `time xasc first because ticks land
/ out of order and first/last on an unsorted table give wrong open/close
.wsu.bars:{[s;iv]
  0!select open:first price, high:max price, low:min price, close:last price,
    vwap:size wavg price, volume:sum size, tickCount:count i
    by barTime:iv xbar time from `time xasc select from trades where sym=s};

/ anchored VWAP: the cumulative vwap series for one sym from an anchor
/ time forward, straight off the tick cache; pv/vol let a client extend
/ the series live without re-requesting
.wsu.avwap:{[s;t0;iv]
  t:`time xasc select from trades where sym=s, time>=t0;
  d:0!select time, avwap:(sums price*size)%sums size from t;
  / thin to one point per bucket (client sends its bar interval; default
  / 1s): a full day of BTC ticks is a ~150MB JSON response at tick
  / resolution, which kills the websocket under the reply
  d:0!select time:last time, avwap:last avwap by bkt:iv xbar time from d;
  `data`pv`vol!(delete bkt from d; sum t[`price]*t`size; sum t`size)};

.z.ws:{
  msg:@[.j.k;x;{()!()}];
  if["sub"~msg`type; .wsu.sub[.z.w;`$msg`syms]];
  if["bars"~msg`type;
    (neg .z.w) .j.j `table`sym`data!(`bars;msg`sym;.wsu.bars[`$msg`sym;`long$1e9*msg`interval])];
  if["avwap"~msg`type;
    iv:`long$1e9*$[`interval in key msg; msg`interval; 1];
    (neg .z.w) .j.j (`table`sym`anchor!(`avwap;msg`sym;msg`anchor)),.wsu.avwap[`$msg`sym;"P"$msg`anchor;iv]];
  if["stats"~msg`type;
    / .Q.w[] is q's own memory ledger (the numbers the -w limit judges);
    / the by-clause returns kdb's signature shape, a keyed table.
    /
    / `wmax` and `mphy` travel with them deliberately. Reporting `used`,
    / `heap` and `peak` alone tells a client how much memory is in use but
    / not what it is in use AGAINST, so "is this instance near its limit?"
    / could not be answered from the wire -- only guessed at, or looked up
    / in whatever licence the operator happens to remember. `wmax` is the
    / -w ceiling (0 when none is set, in which case the licence or `mphy`
    / is the real bound) and `mphy` is the host's physical memory.
    /
    / Nothing else can answer it either: .z.ph is defined below precisely to
    / kill q's default handler, which evaluates code, so there is no remote
    / path to .z.l or to a bare .Q.w[]. That is the right trade -- but it
    / does mean a fact a client legitimately needs has to be published here
    / or not at all.
    (neg .z.w) .j.j `table`memory`rows`syms!
      (`stats; `used`heap`peak`wmax`mphy#.Q.w[]; count trades;
       0!select last time, last price, n:count i by sym from trades)];
  if["asof"~msg`type;
    / aj: the last trade AS OF each requested timestamp -- the as-of join,
    / kdb's canonical primitive. The right side must be time-sorted and
    / ticks land out of order, so sort per call (cheap at cache scale)
    t:msg`times;
    ts:$[10h=type t; enlist "P"$t; "P"$'t];
    (neg .z.w) .j.j `table`sym`data!(`asof; msg`sym;
      select time, price, size from aj[`sym`time;
        ([] sym:count[ts]#`$msg`sym; time:ts);
        `time xasc select sym, time, price, size from trades])];
 };

.z.wc:{.wsu.subs::.wsu.subs _ x;};

.wsu.pub:{[d]
  {[d;h;syms] p:select from d where sym in syms; if[count p; .wsu.send[h;p]]}[d]'[key .wsu.subs; value .wsu.subs];
 };

/ HTTP (.z.ph): read-only endpoints the donor's NAS-side EOD flush used over
/ tailscale Serve. openbq-docker does not carry that flush (eod-dump reads
/ over IPC) and publishes no kdb port or Serve entry, so nothing off
/ live-private reaches them. Defining .z.ph also kills q's default handler,
/ a browser console that EVALUATES code.
/   GET /syms                              -> ["AAPL",...]
/   GET /day?date=2026-08-28&sym=AAPL      -> that UTC day's ticks, JSON
.z.ph:{
  full:first x;
  base:first parts:"?" vs full;
  q:$[1<count parts; (!/) "S=&" 0: last parts; (`$())!()];
  $[base~"syms";
      .h.hy[`json] .j.j string distinct trades`sym;
    base~"day";
      [st:`timestamp$"D"$q`date; en:`timestamp$1+"D"$q`date;
       .h.hy[`json] .j.j select time, price, size from trades
         where sym=`$q`sym, time within (st;en-1)];
    .h.hn["404 Not Found";`txt;"not here"]]
 };

/ openbq-kdb's write_ticks calls upd when it exists, plain insert otherwise
upd:{[t;d]
  t insert d;
  if[`trades=t;
    if[not `g=attr trades`sym; @[`trades;`sym;`g#]];
    .wsu.pub[d]];
 };

/ persistence (2026-09-01): tick log + -11! replay -- see persist.q
/ openbq-docker v10: KDB_PERSIST_Q names persist.q when the compose file mounts the
/ repository copy read-only (/opt/openbq-kdb/persist.q); unset, the donor's /data/persist.q.
kdbPersistQ:$[count kdbPersistQ:getenv`KDB_PERSIST_Q; kdbPersistQ; "/data/persist.q"];
-1 "loading ",kdbPersistQ;
system"l ",kdbPersistQ;
