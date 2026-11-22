/ Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
/ SPDX-License-Identifier: Apache-2.0
/ Stage 1: authenticated ping only. No expression evaluation from the network.
.openbq.ready:0b;
.openbq.principals:`symbol$();
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
  if[not (0h=type args)&0=count args;'"KDB_ARGUMENT"];
  if[not 10h=type requestId;'"KDB_ARGUMENT"];
  if[(0=count requestId)|128<count requestId;'"KDB_ARGUMENT"];
  if[not op~`ping;'"KDB_OPERATION_UNAVAILABLE"];
  `protocol_version`ready`capabilities!(1i;1b;enlist `ping)};
.openbq.dispatch:{[message]
  if[not 0h=type message;'"KDB_ENVELOPE"];
  if[not 4=count message;'"KDB_ENVELOPE"];
  if[not message[0]~`.openbq.rpc;'"KDB_ENVELOPE"];
  .openbq.rpc[message 1;message 2;message 3]};
.openbq.init:{[]
  config:.j.k raze read0 hsym `$getenv `OPENBQ_BOOTSTRAP_POLICY;
  .openbq.principals::`$config`principals;
  localScript:getenv `OPENBQ_BOOTSTRAP_INIT;
  if[count localScript;system "l ",localScript];
  .z.pg::.openbq.dispatch;
  .z.ps::{@[.openbq.dispatch;x;{::}];};
  .openbq.ready::1b;
  system "p ",getenv `OPENBQ_BOOTSTRAP_LISTEN;
  };
@[.openbq.init;();{-2 "KDB_BOOTSTRAP_FAILED";exit 1}];
