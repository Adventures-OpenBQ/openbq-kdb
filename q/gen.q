/ Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
/ SPDX-License-Identifier: Apache-2.0

/ gen.q -- synthetic AAPL tick generator, run as a sidecar inside the
/ container: q gen.q with stdin held open. Random-walk, one tick / 250ms
/ (live-grid's real flush cadence).
h:hopen 5000;
p:185.0;
.z.ts:{
  p+::.05-rand .1;
  h(`upd;`trades;([] time:enlist .z.p; sym:enlist `AAPL; price:enlist p; size:enlist `float$1+rand 100));
 };
system "t 250";
