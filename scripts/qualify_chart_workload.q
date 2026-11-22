/ Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
/ SPDX-License-Identifier: Apache-2.0
/ Disposable, deterministic single-turn work proxy; not a chart API benchmark.
n:100000;
trades:([] time:2026.11.20D14:30:00+1000000*til n;
  sym:n#`AAPL`MSFT`SPY`BTC;price:100f+0.01*n#til 100;size:n#1f);
qualifyScan:{select from trades where sym=`AAPL};
qualifyAggregate:{0!select open:first price,high:max price,low:min price,close:last price,
  volume:sum size,vwap:size wavg price by t:1000000000 xbar time
  from `time xasc select from trades where sym=`AAPL};
qualifyScan[];qualifyAggregate[];
scanMs:{first system "ts qualifyScan[]"}each til 20;
aggregateMs:{first system "ts qualifyAggregate[]"}each til 20;
-1 "QUALIFY_WORK=",.j.j `rows`scan_ms`aggregate_ms!(n;scanMs;aggregateMs);
exit 0;
