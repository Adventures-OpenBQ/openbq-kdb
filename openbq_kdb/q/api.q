/ Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
/ SPDX-License-Identifier: Apache-2.0
/ Named data API. Loaded locally before the listener opens; never evaluated remotely.
system "P 17";
.cache.cov:([] sym:`symbol$();iv:`symbol$();s:`timestamp$();e:`timestamp$());
.cache.lru:([sym:`symbol$();iv:`symbol$()] atime:`timestamp$());
.openbq.bars:()!();
trades:([] time:`timestamp$();sym:`symbol$();price:`float$();size:`float$());
snap:([sym:`symbol$()] fetched:`timestamp$();payload:());
.openbq.tickProto:0#trades;
.openbq.text:{[v;cap] if[not 10h=type v;'"KDB_ARGUMENT"];if[(0=count v)|cap<count v;'"KDB_ARGUMENT"];if[not all v in .Q.an,"_.-/^=:";'"KDB_ARGUMENT"];v};
.openbq.sym:{`$.openbq.text[x;64]};
.openbq.iv:{`$.openbq.text[x;16]};
.openbq.stamp:{if[not -12h=type x;'"KDB_ARGUMENT"];if[null x;'"KDB_ARGUMENT"];x};
.openbq.long:{if[not -7h=type x;'"KDB_ARGUMENT"];if[x<0;'"KDB_ARGUMENT"];x};
.openbq.key:{[s;iv] `$string[.openbq.sym s],"|",string .openbq.iv iv};
.openbq.validateTicks:{[d]
 if[not 98h=type d;'"KDB_ARGUMENT"];
 if[not `time`sym`price`size~cols d;'"KDB_ARGUMENT"];
 if[not 12 11 9 9h~type each value flip d;'"KDB_ARGUMENT"];
 if[100000<count d;'"KDB_LIMIT"];
 if[any null d`time;'"KDB_ARGUMENT"];
 if[any 64<count each string d`sym;'"KDB_ARGUMENT"];
 d};
.openbq.barPrototype:{[s;iv] k:.openbq.key[s;iv];$[k in key .openbq.bars;0#.openbq.bars k;()]};
.openbq.readBars:{[s;iv;qwlo;qwhi] k:.openbq.key[s;iv];.openbq.stamp qwlo;.openbq.stamp qwhi;if[not k in key .openbq.bars;:()];d:.openbq.bars k;select from d where t within (qwlo;qwhi)};
.openbq.writeBars:{[s;iv;d]
 k:.openbq.key[s;iv];if[not 98h=type d;'"KDB_ARGUMENT"];
 if[(100000<count d)|(128<count cols d)|8388608<count -8!d;'"KDB_LIMIT"];
 if[not `t in cols d;'"KDB_ARGUMENT"];if[not 12h=type d`t;'"KDB_ARGUMENT"];
 if[not all (type each value flip d) in 1 2 4 5 6 7 8 9 11 12 13 14 15 16 17 18 19h;'"KDB_ARGUMENT"];
 if[any {any x like "qw*"} cols d;'"KDB_ARGUMENT"];
 old:$[k in key .openbq.bars;.openbq.bars k;0#d];
 .openbq.bars[k]:`t xasc 0!(`t xkey old) upsert d;::};
.openbq.readCoverage:{[s;iv] qwsym:.openbq.sym s;qwiv:.openbq.iv iv;select s,e from .cache.cov where sym=qwsym,iv=qwiv};
.openbq.recordCoverage:{[s;iv;lo;hi] qwsym:.openbq.sym s;qwiv:.openbq.iv iv;.openbq.stamp lo;.openbq.stamp hi;if[lo>hi;'"KDB_ARGUMENT"];.cache.cov: .cache.cov upsert (qwsym;qwiv;lo;hi);::};
.openbq.touch:{[s;iv] .cache.lru: .cache.lru upsert (.openbq.sym s;.openbq.iv iv;.z.p);::};
.openbq.drop:{[s;iv] k:.openbq.key[s;iv];qwsym:.openbq.sym s;qwiv:.openbq.iv iv;.openbq.bars:k _ .openbq.bars;delete from `.cache.cov where sym=qwsym,iv=qwiv;delete from `.cache.lru where sym=qwsym,iv=qwiv;.Q.gc[];.openbq.log[`info;`cache_eviction;enlist[`operation]!enlist `drop];::};
.openbq.writeTicks:{[d] .openbq.validateTicks d;if[(count d)>9223372036854775807j-.chart.seq;'"KDB_LIMIT"];upd[`trades;d];.openbq.log[`debug;`tick_batch_written;`operation`principal`rows`outcome!(`writeTicks;.z.u;"j"$count d;`ok)];"j"$count d};
.openbq.ticksPresent:{[d] .openbq.validateTicks d;if[not count d;:0j];w:select from trades where time within (min d`time;max d`time);p:count each group d;$[count w;sum (value p)&0^(count each group w) key p;0j]};
.openbq.pruneTicks:{[qwcut] .openbq.stamp qwcut;.chart.ids::.chart.ids where trades[`time]>=qwcut;trades::delete from trades where time<qwcut;.openbq.pruned::$[null .openbq.pruned;qwcut;.openbq.pruned|qwcut];.chart.pruneH::.chart.seq;.chart.availableFrom::max (.chart.availableFrom;.openbq.pruned);.Q.gc[];"j"$count trades};
.openbq.tickSpan:{[s] qwsym:.openbq.sym s;d:select from trades where sym=qwsym;$[count d;select lo:min time,hi:max time from d;([] lo:`timestamp$();hi:`timestamp$())]};
.openbq.latestTick:{[s] qwsym:.openbq.sym s;d:select time,price,size from trades where sym=qwsym;$[count d;select from d where time=max time;d]};
.openbq.readTicks:{[s;n] qwsym:.openbq.sym s;.openbq.long n;if[n>1000;'"KDB_LIMIT"];n sublist `time xdesc reverse select time,price,size from trades where sym=qwsym};
.openbq.aggregateFrame:{[s;lo;hi;w] qwsym:.openbq.sym s;.openbq.stamp lo;.openbq.stamp hi;.openbq.long w;if[0=w;'"KDB_ARGUMENT"];0!select open:first price,high:max price,low:min price,close:last price,volume:sum size,vwap:size wavg price by t:w xbar time from `time xasc select from trades where sym=qwsym,time within (lo;hi)};
.openbq.writeSnapshot:{[s;p] qwsym:.openbq.sym s;if[not 10h=type p;'"KDB_ARGUMENT"];if[65536<count p;'"KDB_LIMIT"];.j.k p;`snap upsert (qwsym;.z.p;p);::};
.openbq.readSnapshot:{[s] qwsym:.openbq.sym s;select fetched,payload from snap where sym=qwsym};
.openbq.day:{[lo;hi] .openbq.stamp lo;.openbq.stamp hi;if[(hi<lo)|86400000000000<hi-lo;'"KDB_ARGUMENT"];::};
.openbq.dayCount:{[lo;hi] .openbq.day[lo;hi];"j"$count select from trades where time>=lo,time<hi};
.openbq.daySlice:{[lo;hi;off;n] .openbq.day[lo;hi];.openbq.long off;.openbq.long n;if[n>250000;'"KDB_LIMIT"];n sublist off _ select from trades where time>=lo,time<hi};
system "l ",(getenv `OPENBQ_Q_ASSETS),"/diagnostics.q";
/ Persist's original append-before-apply ordering and binary log message shape.
upd:{[t;d] if[not t~`trades;'"KDB_ARGUMENT"];`trades insert d;::};
.z.ps:value;
system "l ",(getenv `OPENBQ_Q_ASSETS),"/persist.q";
.z.ps:.openbq.deny;
system "l ",(getenv `OPENBQ_Q_ASSETS),"/chartstream.q";
.openbq.ops:`memory`barPrototype`tickPrototype`readBars`writeBars`readCoverage`recordCoverage`touch`drop`lruRows`writeTicks`ticksPresent`pruneTicks`tickSpan`latestTick`readTicks`aggregateFrame`writeSnapshot`readSnapshot`dayCount`daySlice`streamStatus`readChartSnapshot`readChartChanges;
.openbq.ops:.openbq.ops,`readDiagnostics;
.openbq.arity:.openbq.ops!0 2 0 4 3 2 4 2 2 0 1 1 1 1 1 2 4 2 1 2 4 0 1 2 2;
.openbq.handlers:.openbq.ops!({[] .Q.w[]};.openbq.barPrototype;{[] .openbq.tickProto};.openbq.readBars;.openbq.writeBars;.openbq.readCoverage;.openbq.recordCoverage;.openbq.touch;.openbq.drop;{[] 0!.cache.lru};.openbq.writeTicks;.openbq.ticksPresent;.openbq.pruneTicks;.openbq.tickSpan;.openbq.latestTick;.openbq.readTicks;.openbq.aggregateFrame;.openbq.writeSnapshot;.openbq.readSnapshot;.openbq.dayCount;.openbq.daySlice;.openbq.streamStatus;.openbq.readChartSnapshot;.openbq.readChartChanges;.openbq.readDiagnostics);
.openbq.call:{[op;args] if[not count[args]=.openbq.arity op;'"KDB_ARGUMENT"];if[op~`readDiagnostics;:.openbq.readDiagnostics[args 0;args 1]];if[op~`pruneTicks;:.openbq.pruneTicks first args];if[op~`dayCount;:.openbq.dayCount[args 0;args 1]];if[not count args;:(.openbq.handlers op)[]];(.openbq.handlers op) . args};
.openbq.capabilities:enlist[`ping],.openbq.ops,`chart_stream_v1;
