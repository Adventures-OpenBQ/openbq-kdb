/ Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
/ SPDX-License-Identifier: Apache-2.0
/ Authenticated named dispatcher. No expression evaluation from the network.
.openbq.ready:0b;
.openbq.principals:`symbol$();
.openbq.grants:()!();
.openbq.capabilities:enlist `ping;
.openbq.log:{[level;event;fields] ::};
.diag.failure:{[message;e] ::};
.z.pw:{[u;p] .openbq.ready & u in .openbq.principals};
.openbq.deny:{'"KDB_ENVELOPE"};
.z.pg:.openbq.deny;
.z.ps:{@[.openbq.deny;x;{::}];};
.z.pq:.openbq.deny;
.z.ph:{"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"};
.z.pp:.z.ph;
.z.pm:.z.ph;
.z.ac:{(0;"")};
.z.wo:{hclose x};
.z.ws:{hclose .z.w};
.openbq.rpc:{[op;args;requestId]
  if[not .openbq.ready;'"KDB_NOT_READY"];
  if[not .z.u in .openbq.principals;'"KDB_AUTHORIZATION"];
  if[not -11h=type op;'"KDB_ARGUMENT"];
  if[not (0h=type args)|((op~`readChartSnapshot)&(98h=type args)&1=count args)|((op in `pruneTicks`dayCount)&12h=type args)|((op~`readDiagnostics)&7h=type args);'"KDB_ARGUMENT"];
  if[not 10h=type requestId;'"KDB_ARGUMENT"];
  if[(not all 128>"i"$requestId)|(0=count requestId)|128<count requestId;'"KDB_ARGUMENT"];
  if[not op in .openbq.capabilities except enlist `chart_stream_v1;'"KDB_OPERATION_UNAVAILABLE"];
  if[not op in .openbq.grants .z.u;'"KDB_AUTHORIZATION"];
  if[op~`ping; if[count args;'"KDB_ARGUMENT"]; :`protocol_version`ready`capabilities!(1i;1b;.openbq.capabilities)];
  .openbq.call[op;args]};
.openbq.rawDispatch:{[message]
  if[not 0h=type message;'"KDB_ENVELOPE"];
  if[not 4=count message;'"KDB_ENVELOPE"];
  if[not message[0]~`.openbq.rpc;'"KDB_ENVELOPE"];
  .openbq.rpc[message 1;message 2;message 3]};
.openbq.dispatch:{[message]
 @[.openbq.rawDispatch;message;{[m;e] .[.diag.failure;(m;e);{::}];'e}[message]]};
.openbq.init:{[]
  config:.j.k raze read0 hsym `$getenv `OPENBQ_BOOTSTRAP_POLICY;
  .openbq.principals::`$config`principals;
  .openbq.grants::(key config`grants)!{`$x} each value config`grants;
  if[count getenv `OPENBQ_DATA_DIR;system "l ",(getenv `OPENBQ_Q_ASSETS),"/api.q"];
  localScript:getenv `OPENBQ_BOOTSTRAP_INIT;
  if[count localScript;system "l ",localScript];
  .z.pg::.openbq.dispatch;
  .z.ps::{@[.openbq.dispatch;x;{::}];};
  .openbq.ready::1b;
  .openbq.log[`info;`server_ready;()!()];
  system "p ",getenv `OPENBQ_BOOTSTRAP_LISTEN;
  };
@[.openbq.init;();{-2 "KDB_BOOTSTRAP_FAILED";exit 1}];
