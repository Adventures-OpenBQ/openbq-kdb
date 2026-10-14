/ Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
/ SPDX-License-Identifier: Apache-2.0

/ persist.q v2 (2026-09-01) -- tick-log durability. Replaces v1's 5-minute
/ full-table mirror (O(n) per timer tick) with the classic tickerplant log:
/ O(1) append per update, -11! replay on boot, so a restart loses nothing.
/ EOD flush to the Delta HDB is the eod-dump sidecar (scripts/eod_dump.py);
/ live-grid's rolling window prunes RAM, so there is no clear here.
/ Idempotent to re-load into a live server: the replay is boot-only.

system"mkdir -p /data/logs";
.tlog.dir:`:/data/logs;
.tlog.file:{` sv .tlog.dir,`$"tlog_",string x};
.tlog.h:0N;
.tlog.day:.z.d;

/ wrap upd exactly once: append to the log, then the original insert+publish
if[not `updCore in key `.; updCore:upd];
upd:{[t;d] if[not null .tlog.h; .tlog.h enlist(`upd;t;d)]; updCore[t;d]};

/ boot replay: every log still on disk (unflushed days + today), oldest
/ first -- lexical sort of tlog_YYYY.MM.DD is chronological. Hot-load guard:
/ a live server (rows present) must not replay duplicates. .tlog.h is still
/ null here, so replayed upds are not re-logged.
if[not count trades;
  {[f] -1 "tlog: replaying ",string f; -11! ` sv .tlog.dir,f} each asc key .tlog.dir];

/ open today's log for appending (create empty if new)
.tlog.open:{[d]
  f:.tlog.file d;
  if[()~key f; f set ()];
  .tlog.h::hopen f; .tlog.day::d;};
.tlog.open .z.d;

/ roll at UTC date change; prune logs older than 7 days (eod-dump has
/ flushed them to Delta long since -- and replays them on boot if not)
.tlog.roll:{
  if[.z.d>.tlog.day;
    @[hclose;.tlog.h;::]; .tlog.open .z.d;
    old:{x where x<`$"tlog_",string .z.d-7} key .tlog.dir;
    {hdel ` sv .tlog.dir,x} each old]};
.z.ts:{@[.tlog.roll;::;{-2 "tlog roll failed: ",x;}]};
system"t 60000";
.z.exit:{@[hclose;.tlog.h;::]};
-1 "tick log active: ",string[.tlog.file .tlog.day]," (replay on boot, daily roll)";
