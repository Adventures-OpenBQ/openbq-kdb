// Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
// SPDX-License-Identifier: Apache-2.0

// Smoke test: KDB-X websocket pub/sub tick cache (startup.q).
// Requires the openbq-kdb-test container running (see run-test.sh).
import { execSync } from 'node:child_process';
import assert from 'node:assert';

const WS_URL = process.env.WS_URL ?? 'ws://127.0.0.1:5998/';

function injectQ(lines) {
  const input = ['h:hopen 5000', ...lines.map((l) => `h"${l.replaceAll('"', '\\"')}"`), 'exit 0', ''].join('\n');
  execSync('docker exec -i openbq-kdb-test q', { input, stdio: ['pipe', 'pipe', 'inherit'] });
}

const messages = [];
let resolveNext;
function nextMessage(timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    if (messages.length) return resolve(messages.shift());
    const t = setTimeout(() => reject(new Error('timeout waiting for ws message')), timeoutMs);
    resolveNext = (m) => { clearTimeout(t); resolve(m); };
  });
}

// 1. Pre-insert ticks BEFORE any subscriber exists -- pub with no subs must not error.
injectQ([
  'upd[`trades; ([] time:3#.z.p; sym:3#`AAPL; price:101.1 101.2 101.3; size:10 20 30f)]',
  'upd[`trades; ([] time:enlist .z.p; sym:enlist `MSFT; price:enlist 200.5; size:enlist 5f)]',
]);
console.log('ok: pre-insert with no subscribers');

// 2. Connect + subscribe to AAPL only.
const ws = new WebSocket(WS_URL);
ws.onmessage = (ev) => {
  const m = JSON.parse(ev.data);
  if (resolveNext) { const r = resolveNext; resolveNext = null; r(m); } else messages.push(m);
};
await new Promise((resolve, reject) => { ws.onopen = resolve; ws.onerror = (e) => reject(new Error('ws connect failed')); });
ws.send(JSON.stringify({ type: 'sub', syms: ['AAPL'] }));

// 3. Snapshot: the 3 AAPL rows, no MSFT.
const snap = await nextMessage();
assert.equal(snap.table, 'trades');
assert.equal(snap.data.length, 3);
assert.ok(snap.data.every((r) => r.sym === 'AAPL'), 'snapshot leaked non-subscribed syms');
assert.equal(typeof snap.data[0].price, 'number');
assert.ok(snap.data[0].time.startsWith('202'), `time not ISO-ish: ${snap.data[0].time}`);
console.log('ok: snapshot on subscribe (3 AAPL rows, MSFT filtered)', snap.data[0]);

// 4. Live push via upd: mixed batch, only AAPL should arrive.
injectQ(['upd[`trades; ([] time:2#.z.p; sym:`AAPL`MSFT; price:102.0 201.0; size:1 2f)]']);
const live = await nextMessage();
assert.equal(live.table, 'trades');
assert.equal(live.data.length, 1);
assert.equal(live.data[0].sym, 'AAPL');
assert.equal(live.data[0].price, 102.0);
console.log('ok: live push filtered to subscription', live.data[0]);

// 5. The exact guarded expression store.py will use (upd defined -> publishes).
injectQ(['incoming_ticks:([] time:enlist .z.p; sym:enlist `AAPL; price:enlist 103.0; size:enlist 7f); $[`upd in key `.; upd[`trades;incoming_ticks]; `trades insert incoming_ticks]']);
const guarded = await nextMessage();
assert.equal(guarded.data[0].price, 103.0);
console.log('ok: store.py guarded expression routes through upd');

// 6. Table still accumulates (cache role intact): 3+1+2+1 = 7 rows.
const out = execSync('docker exec -i openbq-kdb-test q', { input: 'h:hopen 5000\n-1 string h"count trades";\nexit 0\n', stdio: ['pipe', 'pipe', 'inherit'] }).toString();
assert.ok(out.includes('7'), `expected 7 rows, got: ${out}`);
console.log('ok: trades table holds all 7 rows');

// 7. Bars request over the same socket: OHLCV+VWAP from the cache.
ws.send(JSON.stringify({ type: 'bars', sym: 'AAPL', interval: 60 }));
const bars = await nextMessage();
assert.equal(bars.table, 'bars');
assert.equal(bars.sym, 'AAPL');
assert.ok(bars.data.length >= 1);
const b0 = bars.data[0];
for (const k of ['barTime', 'open', 'high', 'low', 'close', 'vwap', 'volume', 'tickCount']) assert.ok(k in b0, `missing ${k}`);
assert.ok(b0.high >= b0.low);
console.log('ok: bars request (OHLCV+VWAP)', b0);

// 8. Anchored VWAP: cumulative series from an anchor, plus running pv/vol
// so a client can extend it live. All-AAPL pv = 6897, vol = 68.
ws.send(JSON.stringify({ type: 'avwap', sym: 'AAPL', anchor: '2000-01-01T00:00:00' }));
const av = await nextMessage();
assert.equal(av.table, 'avwap');
// bucketed to 1s by default: same-second ticks collapse to one point
assert.ok(av.data.length >= 1 && av.data.length <= 5, `bad point count ${av.data.length}`);
assert.ok(Math.abs(av.data.at(-1).avwap - 6897 / 68) < 1e-9, `bad final avwap ${av.data.at(-1).avwap}`);
assert.ok(Math.abs(av.pv - 6897) < 1e-9 && Math.abs(av.vol - 68) < 1e-9, `bad pv/vol ${av.pv}/${av.vol}`);
console.log('ok: anchored VWAP series + running pv/vol', { points: av.data.length, last: av.data.at(-1).avwap, pv: av.pv, vol: av.vol });

// 9. stats: q's own memory ledger plus last-per-sym from a by-clause.
ws.send(JSON.stringify({ type: 'stats' }));
const stats = await nextMessage();
assert.equal(stats.table, 'stats');
assert.ok(stats.memory.used > 0 && stats.memory.heap >= stats.memory.used);
assert.equal(stats.rows, 7);
const bySym = Object.fromEntries(stats.syms.map((s) => [s.sym, s]));
assert.equal(bySym.AAPL.n, 5);
assert.equal(bySym.MSFT.n, 2);
assert.equal(bySym.AAPL.price, 103);
console.log('ok: stats (memory + last-per-sym)', stats.memory);

// 10. asof: the last trade as of each timestamp, via aj.
const future = new Date(Date.now() + 3600e3).toISOString().slice(0, 19);
ws.send(JSON.stringify({ type: 'asof', sym: 'AAPL', times: ['2000-01-01T00:00:00', future] }));
const asof = await nextMessage();
assert.equal(asof.table, 'asof');
assert.equal(asof.data.length, 2);
assert.ok(asof.data[0].price == null, 'no trade existed in 2000');
assert.equal(asof.data[1].price, 103, 'as-of the future = the last trade');
console.log('ok: as-of join', asof.data[1]);

// 11. the g# attribute is applied and survives the inserts above.
const attrOut = execSync('docker exec -i openbq-kdb-test q', { input: 'h:hopen 5000\n-1 "attr=",string h"attr trades`sym";\nexit 0\n', stdio: ['pipe', 'pipe', 'inherit'] }).toString();
assert.ok(attrOut.includes('attr=g'), `expected g attribute, got: ${attrOut}`);
console.log('ok: g# attribute on sym');

ws.close();
console.log('\nALL CHECKS PASSED');
process.exit(0);
