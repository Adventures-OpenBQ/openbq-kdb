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
.tlog.torn:0b;

/ Cut a log with a torn tail back to its whole chunks, crash-safely: -11!(-2;p)
/ answers a chunk count for a whole log, or (chunks;bytes) for the valid prefix
/ of a corrupt one. The prefix is written to tlog_<d>.tmp and renamed over the
/ log (Q5, KX's "Replacing a corrupt log"), so a crash mid-rewrite leaves the
/ original whole. Returns the chunk count to replay.
.tlog.cut:{[p]
  n:-11!(-2;p);
  if[0h<type n;
    -2 "tlog: ",string[p]," has a bad tail; keeping its first ",string[n 0]," chunks (",string[n 1]," bytes)";
    tmp:`$string[p],".tmp";
    tmp 1: read1(p;0;n 1);
    system "mv ",(1_string tmp)," ",1_string p;
    n:n 0];
  n};

/ wrap upd exactly once: append to the log, then the original insert+publish.
/ A failed append (ENOSPC mid-run; Q4) may leave a partial chunk that every
/ later append would land behind, unreplayable. So it is trapped: the handle is
/ closed, the log marked torn, and the error signalled -- updCore is NOT run,
/ so nothing is applied unlogged and live-grid's spool keeps the batch. The
/ next upd cuts the log back and reopens it before appending; until that
/ succeeds, every upd signals the same way.
.tlog.append:{[x]
  if[.tlog.torn; f:.tlog.file .tlog.day; .tlog.cut f; .tlog.h::hopen f; .tlog.torn::0b];
  .tlog.h x};
.tlog.fail:{[e] @[hclose;.tlog.h;::]; .tlog.torn::1b; -2 "tlog: append failed: ",e; 'e};
if[not `updCore in key `.; updCore:upd];
upd:{[t;d] if[.tlog.torn or not null .tlog.h; @[.tlog.append;enlist(`upd;t;d);.tlog.fail]]; updCore[t;d]};

/ boot replay: the logs of today and the .tlog.replayDays days before it,
/ oldest first -- lexical sort of tlog_YYYY.MM.DD is chronological. Older logs
/ stay on disk until the roll deletes them, but are not replayed: the prune
/ (openbq-kdb's `trades:: delete`) is not logged, so they would only come back
/ to be pruned a minute later, a memory spike (D25). 2 days covers live-grid's
/ 48 h window in openbq-docker's compose. Hot-load guard: a live server (rows
/ present) must not replay duplicates. .tlog.h is still null here, so replayed
/ upds are not re-logged.
/ A torn tail (host crash, power loss, ENOSPC; D4) is cut back first
/ (.tlog.cut), so today's appends follow a whole chunk, then replayed. Each file
/ is trapped: one bad log must not abort this script, or the handle, the roll
/ and .z.exit below never load and q runs on without logging. Only
/ tlog_YYYY.MM.DD names are logs: a tlog_<d>.tmp a crash left mid-cut is not.
.tlog.replayDays:2;
.tlog.replay:{[f]
  p:` sv .tlog.dir,f;
  n:.tlog.cut p;
  -1 "tlog: replaying ",string f; -11!(n;p)};
if[not count trades;
  {[f] @[.tlog.replay;f;{[f;e] -2 "tlog: replay of ",string[f]," failed: ",e}f]} each
    {x where (x>=`$"tlog_",string .z.d-.tlog.replayDays) and not x like "*.tmp"} asc key .tlog.dir];

/ open today's log for appending (create empty if new)
.tlog.open:{[d]
  f:.tlog.file d;
  if[()~key f; f set ()];
  .tlog.h::hopen f; .tlog.day::d; .tlog.torn::0b;};
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
