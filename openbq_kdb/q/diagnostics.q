/ Copyright 2026 SecretoftheUniverse.com LLC. Apache-2.0.
/ FINOS pipeline, configured before getLogger: no table insertion or local time.
system "l ",(getenv `OPENBQ_Q_ASSETS),"/vendor/finos/structlog.q";
.diag.boot:string first -1?0Ng;
.diag.seq:0j;
.diag.events:([] seq:`long$();payload:());
.diag.capacity:1000;
.diag.maxBytes:4096;
.diag.overwritten:0j;
.diag.minLevel:20;
.diag.vocabulary:`server_ready`auth_denied`rpc_failed`tick_batch_written`replay_started`replay_completed`replay_failed`log_roll_failed`cache_eviction`connection_failed`connection_recovered`log_events_suppressed;
.diag.second:"j"$.z.p div 1000000000;
.diag.counts:.diag.vocabulary!count[.diag.vocabulary]#0j;
.diag.suppressed:.diag.counts;
.diag.iso:{[t] s:string t;ssr[10#s;".";"-"],"T",11_s,"Z"};
.diag.write:{[d]
 payload:.j.j d;
 if[count[payload]>.diag.maxBytes;d:`request_id _ d;d[`truncated]:1b;payload:.j.j d];
 if[count[payload]>.diag.maxBytes;:0b];
 .diag.seq::.diag.seq+1;
 `.diag.events insert (.diag.seq;payload);
 if[count[.diag.events]>.diag.capacity;.diag.events::1_.diag.events;.diag.overwritten::.diag.overwritten+1];
 1b};
.diag.roll:{[]
 now:"j"$.z.p div 1000000000;
 if[now<>.diag.second;
   missed:sum .diag.suppressed;
   .diag.counts::key[.diag.counts]!count[.diag.counts]#0j;
   .diag.suppressed::.diag.counts;.diag.second::now;
   if[missed;.diag.write `schema_version`timestamp`level`event`component`pid`rows!(1;.diag.iso .z.p;30;"log_events_suppressed";"kdb";.z.i;missed)]];
 ::};
.diag.process:{[d]
 event:d`event;
 if[not event in .diag.vocabulary;:0b];
 if[d[`level]<.diag.minLevel;:0b];
 .diag.roll[];
 if[10<=.diag.counts event;.diag.suppressed[event]+:1;:0b];
 .diag.counts[event]+:1;
 safe:`schema_version`timestamp`level`event`component`pid!(1;.diag.iso .z.p;d`level;string event;"kdb";.z.i);
 / Only these typed, non-reserved application fields may survive the processor.
 if[`operation in key d;if[-11h=type d`operation;safe[`operation]:$[d[`operation] in .openbq.capabilities;string d`operation;"unknown"]]];
 if[`principal in key d;if[d[`principal] in .openbq.principals;safe[`principal]:string d`principal]];
 if[`request_id in key d;if[(10h=type d`request_id)&128>=count d`request_id;safe[`request_id]:d`request_id]];
 if[`duration_ms in key d;if[type[d`duration_ms] in -6 -7 -8 -9h;safe[`duration_ms]:d`duration_ms]];
 if[`rows in key d;if[type[d`rows] in -6 -7h;safe[`rows]:d`rows]];
 if[`outcome in key d;if[d[`outcome] in `ok`denied`failed;safe[`outcome]:string d`outcome]];
 if[`error_code in key d;if[d[`error_code] in `KDB_ARGUMENT`KDB_LIMIT`KDB_AUTHORIZATION`KDB_ENVELOPE`KDB_NOT_READY`KDB_OPERATION_UNAVAILABLE`KDB_OPERATION_FAILED`TLOG_APPEND`TLOG_REPLAY`TLOG_TORN`TLOG_ROLL;safe[`error_code]:string d`error_code]];
 .diag.write safe};
.finos.structlog.configure enlist[`processors]!enlist enlist .diag.process;
.diag.logger:.finos.structlog.getLogger[];
.diag.emit:{[level;event;fields] fields:`schema_version`timestamp`event`level`component`pid _ fields; .[.diag.logger level;(event;fields);{::}];::};
.diag.info:{[event;fields] .diag.emit[`info;event;fields]};
.openbq.log:{[level;event;fields] @[{[a] .diag.emit[a 0;a 1;a 2]};(level;event;fields);{::}];::};
.diag.failure:{[message;e]
 fields:`outcome`error_code!(`failed;"KDB_OPERATION_FAILED");
 if[99h=type message;::];
 if[0h=type message;if[4=count message;
   op:message 1;
   if[-11h=type op;if[op in .openbq.capabilities;fields[`operation]:op]];
   r:message 3;if[10h=type r;if[128>=count r;fields[`request_id]:r]]]];
 if[.z.u in .openbq.principals;fields[`principal]:.z.u];
 if[e in ("KDB_ARGUMENT";"KDB_LIMIT";"KDB_AUTHORIZATION";"KDB_ENVELOPE";"KDB_NOT_READY";"KDB_OPERATION_UNAVAILABLE");fields[`error_code]:`$e];
 .openbq.log[`warning;$[e~"KDB_AUTHORIZATION";`auth_denied;`rpc_failed];fields];::};
.openbq.readDiagnostics:{[after;n]
 .diag.roll[];
 .openbq.long after;.openbq.long n;if[(n<1)|n>100;'"KDB_LIMIT"];
 if[after>.diag.seq;after:0j];
 d:n sublist select from .diag.events where seq>after;
 `boot_id`events`next_sequence`overwritten!(.diag.boot;d`payload;$[count d;last d`seq;after];.diag.overwritten)};
