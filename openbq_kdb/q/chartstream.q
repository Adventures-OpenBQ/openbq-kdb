/ Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
/ SPDX-License-Identifier: Apache-2.0
/ Accepted-data identities are private and do not change four-column trades/logs.
.chart.epoch:string first -1?0Ng;
.chart.seq:"j"$count trades;
.chart.ids:1+til count trades;
.chart.low:.chart.seq;
.chart.ring:([] time:`timestamp$();sym:`symbol$();price:`float$();size:`float$();seq:`long$();accepted:`timestamp$();batch:`long$());
.chart.maxRows:1000000;
.chart.maxBytes:134217728;
.chart.keep:0D00:01:00;
.chart.scan:100000;
.chart.responseBytes:8388608;
.openbq.pruned:0Np;
.chart.pruneH:0j;
.chart.availableFrom:$[count trades;min trades`time;0Np];
.chart.iso:{[t] s:string t;ssr[10#s;".";"-"],"T",11_s,"Z"};
.chart.cursor:{`epoch`seq!(.chart.epoch;string x)};
.chart.coverage:{[] `available_from`complete_from`available_to`complete!($[null .chart.availableFrom;::;.chart.iso .chart.availableFrom];::;.chart.iso .z.p;0b)};
.chart.trim:{[]
 d:.chart.ring;
 d:select from d where accepted>=.z.p-.chart.keep;
 if[count[d]>.chart.maxRows;d:(count[d]-.chart.maxRows)_d];
 if[count[d]>.chart.maxBytes div 128;d:(count[d]-.chart.maxBytes div 128)_d];
 if[count d;.chart.low::first[d`seq]-1];
 if[not count d;.chart.low::.chart.seq];
 .chart.ring::d;::};
.chart.accept:{[d]
 n:count d;if[not n;:0b];
 ids:.chart.seq+1+til n;
 .chart.availableFrom::max (.openbq.pruned;min (.chart.availableFrom;min d`time));
 .chart.ids::.chart.ids,ids;
 .chart.seq::last ids;
 .chart.ring::.chart.ring upsert d,'([] seq:ids;accepted:n#.z.p;batch:n#last ids);
 .chart.trim[];::};
/ Persist has already wrapped upd: durably append -> public apply -> private stream.
.chart.persistUpd:upd;
.chart.failed:{[e] .openbq.ready::0b;exit 1};
upd:{[t;d] .chart.persistUpd[t;d];@[.chart.accept;d;.chart.failed];::};
.chart.intervals:`1s`1m`5m`15m`30m`1h`1d!1000000000 60000000000 300000000000 900000000000 1800000000000 3600000000000 86400000000000j;
.chart.parseTime:{if[not 10h=type x;'"KDB_ARGUMENT"];if[not 30=count x;'"KDB_ARGUMENT"];t:"P"$x;.openbq.stamp t};
.chart.view:{[v]
 if[not 99h=type v;'"KDB_ARGUMENT"];
 if[not all `symbol`mode`start`max_items in key v;'"KDB_ARGUMENT"];
 if[not all key[v] in `symbol`mode`start`end`interval`max_items;'"KDB_ARGUMENT"];
 .openbq.sym v`symbol;
 if[not v[`mode] in ("ticks";"candles");'"KDB_ARGUMENT"];
 lo:.chart.parseTime v`start;hi:$[`end in key v;.chart.parseTime v`end;0Wp];
 if[hi<=lo;'"KDB_ARGUMENT"];
 n:v`max_items;if[not type[n] in -6 -7h;'"KDB_ARGUMENT"];if[(n<1)|n>$[v[`mode]~"ticks";10000;2000];'"KDB_LIMIT"];
 if[v[`mode]~"candles";if[not `interval in key v;'"KDB_ARGUMENT"];if[not (`$v`interval) in key .chart.intervals;'"KDB_ARGUMENT"]];
 if[(v[`mode]~"ticks")&`interval in key v;'"KDB_ARGUMENT"];
 v};
.chart.floorTime:{[v]
 lo:.chart.parseTime v`start;
 if[null .openbq.pruned;:lo];
 qwcut:.openbq.pruned;
 if[v[`mode]~"candles";w:.chart.intervals `$v`interval;qwcut:$[qwcut=w xbar qwcut;qwcut;(w xbar qwcut)+w]];
 lo|qwcut};
.chart.rows:{[v]
 qwsym:`$v`symbol;lo:.chart.floorTime v;hi:$[`end in key v;.chart.parseTime v`end;0Wp];
 d:trades,'([] seq:.chart.ids);
 `time`seq xasc select from d where sym=qwsym,time>=lo,time<hi};
.chart.tick:{[r] `tick_id`time`sym`price`size!(.chart.cursor r`seq;.chart.iso r`time;string r`sym;r`price;r`size)};
.chart.candle:{[v;h;r]
 w:.chart.intervals `$v`interval;
 `key`revision`open`high`low`close`volume`vwap`tickCount`status!(`symbol`interval`bucket_start!(v`symbol;v`interval;.chart.iso r`t);.chart.cursor h;r`open;r`high;r`low;r`close;r`volume;r`vwap;string r`tickCount;$[.z.p>=r[`t]+w;"closed";"open"])};
.chart.aggregate:{[v;d]
 w:.chart.intervals `$v`interval;
 0!select open:first price,high:max price,low:min price,close:last price,volume:sum size,vwap:size wavg price,tickCount:count i by t:w xbar time from d};
.chart.candles:{[v;d;h] .chart.candle[v;h] each .chart.aggregate[v;d]};
.openbq.streamStatus:{[] .chart.trim[];h:.chart.cursor .chart.seq;`epoch`low_cursor`high_cursor`current_cursor`coverage`clock`limits!(.chart.epoch;.chart.cursor .chart.low;h;h;.chart.coverage[];.chart.iso .z.p;`retained_ticks`scan_rows`response_bytes!(.chart.maxRows;.chart.scan;.chart.responseBytes))};
.chart.reset:{[reason] `reason`current_cursor`coverage!(reason;.chart.cursor .chart.seq;.chart.coverage[])};
.openbq.readChartSnapshot:{[v]
 .chart.view v;
 h:.chart.seq;d:.chart.rows v;
 n:v`max_items;
 rows:$[v[`mode]~"ticks";d;.chart.aggregate[v;d]];
 truncated:count[rows]>n;
 if[truncated;rows:(count[rows]-n)_rows];
 items:$[v[`mode]~"ticks";.chart.tick each rows;.chart.candle[v;h] each rows];
 while[(count items)&(.chart.responseBytes-4096)<count .j.j items;items:1_items;truncated:1b];
 floorTime:.chart.floorTime v;if[v[`mode]~"candles";floorTime:(.chart.intervals `$v`interval) xbar floorTime];
 startFloor:.chart.iso floorTime;
 flr:$[v[`mode]~"ticks";`kind`time`seq!("tick";startFloor;::);`kind`bucket_start!("candle";startFloor)];
 if[truncated&count items;flr:$[v[`mode]~"ticks";`kind`time`seq!("tick";first[items]`time;first[items][`tick_id]`seq);`kind`bucket_start!("candle";first[items][`key]`bucket_start)]];
 `view`cursor`view_floor`coverage`truncated`items!(v;.chart.cursor h;flr;.chart.coverage[];truncated;items)};
.openbq.readChartChanges:{[c;v]
 .chart.view v;
 if[not 99h=type c;'"KDB_ARGUMENT"];if[not `epoch`seq~key c;'"KDB_ARGUMENT"];
 if[(not 10h=type c`epoch)|not 10h=type c`seq;'"KDB_ARGUMENT"];
 if[not c[`epoch]~.chart.epoch;:.chart.reset "epoch_changed"];
 s:"J"$c`seq;if[(null s)|(s<0)|not c[`seq]~string s;'"KDB_ARGUMENT"];
 .chart.trim[];h:.chart.seq;
 if[s>h;:.chart.reset "cursor_ahead"];
 if[s<.chart.low;:.chart.reset "cursor_expired"];
 if[(h-s)>.chart.scan;:.chart.reset "catchup_limit"];
 if[s<.chart.pruneH;:.chart.reset "coverage_lost"];
 qwsym:`$v`symbol;lo:.chart.floorTime v;hi:$[`end in key v;.chart.parseTime v`end;0Wp];
 raw:select from .chart.ring where seq>s,seq<=h,sym=qwsym,time>=(.chart.parseTime v`start),time<hi;
 d:select from raw where time>=lo;
 base:`from_cursor`through_cursor`coverage`unavailable_buckets!(c;.chart.cursor h;.chart.coverage[];());
 if[v[`mode]~"ticks";
   batches:{[rows;b] t:select from rows where batch=b;`batch_id`accepted_at`ticks!(.chart.epoch,"-",string b;.chart.iso first t`accepted;.chart.tick each t)}[d] each distinct d`batch;
   out:base,enlist[`batches]!enlist batches;
   if[.chart.responseBytes<count .j.j out;:.chart.reset "catchup_limit"];:out];
 w:.chart.intervals `$v`interval;
 missing:distinct w xbar exec time from raw where time<lo;
 if[(not null .openbq.pruned)&(.chart.parseTime v`start)<lo;missing:distinct missing,enlist lo-w];
 base[`unavailable_buckets]:{[v;t] `symbol`interval`bucket_start!(v`symbol;v`interval;.chart.iso t)}[v] each missing;
 affected:distinct w xbar d`time;
 allRows:.chart.rows v;allRows:select from allRows where (w xbar time) in affected;
 out:base,enlist[`candles]!enlist .chart.candles[v;allRows;h];
 if[.chart.responseBytes<count .j.j out;:.chart.reset "catchup_limit"];
 out};
