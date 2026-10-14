# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0

"""Real-q check for tick recording and aggregation. Needs a licence; not in CI."""

import sys
from datetime import datetime, timedelta

import pandas as pd

from openbq_kdb.aggregate import aggregate_ticks
from openbq_kdb.config import resolve_config
from openbq_kdb.session import KdbSession
from openbq_kdb.store import KdbStore

failures = []


def check(name, ok, detail=""):
    print(f"{'PASS' if ok else 'FAIL'}  {name} {detail}")
    if not ok:
        failures.append(name)


session = KdbSession(resolve_config())
store = KdbStore(session)
base = datetime(2025, 6, 10, 14, 0, 0)

# Force a connection now so the resolved endpoint -- the link of the chain
# that actually answered -- is known before anything else runs.
session.connection()
print(f"endpoint: {session.endpoint}")

# Deliberately out of order: true open is 100 (at :05), true close is 101 (at :55).
ticks = pd.DataFrame({
    "time": [base + timedelta(seconds=55), base + timedelta(seconds=5),
             base + timedelta(seconds=30), base + timedelta(seconds=70)],
    "sym": ["TICKCHK"] * 4,
    "price": [101.0, 100.0, 103.0, 99.0],
    "size": [5.0, 10.0, 1.0, 7.0],
})
check("write_ticks", store.write_ticks(ticks) == 4)

bars = aggregate_ticks(store, "TICKCHK", "1m", base, base + timedelta(minutes=5))
check("two buckets", len(bars) == 2, f"got {len(bars)}")
if bars:
    first = bars[0]
    check("open is time-ordered, not insertion-ordered", first["open"] == 100.0,
          f"open={first['open']} (100.0 expected; 101.0 means the sort was dropped)")
    check("close is time-ordered", first["close"] == 101.0, f"close={first['close']}")
    check("high", first["high"] == 103.0)
    check("low", first["low"] == 100.0)
    check("volume", first["volume"] == 16.0)

span = store.tick_span("TICKCHK")
check("tick_span", span is not None and span[0] <= span[1])

# --- second batch, dtypes deliberately re-inferred -------------------------
#
# This is the `'type` bug class that broke every live chart once. q's upsert
# matches column types EXACTLY -- it raises rather than coercing -- while
# pandas re-infers each column's dtype from the values of whichever batch it
# is handed. The first batch above fixed nothing (the schema is declared), but
# a batch whose columns infer DIFFERENTLY is what actually goes over the wire
# in production: a live feed's second flush is small, and a small flush is
# exactly the one whose size column is all-null (object, not float64) and
# whose prices happen to be whole numbers (int64, not float64).
#
# No mock can catch this: a fake connection accepts any dtype. Only a real q
# says 'type.
batch2 = pd.DataFrame({
    "time": [base + timedelta(minutes=10), base + timedelta(minutes=10, seconds=30)],
    "sym": ["TICKCHK", "TICKCHK"],
    "price": [104, 105],   # int64 -- the stored column is float
    "size": [None, None],  # object -- an all-null column has no dtype to infer
})
check("second batch infers different dtypes",
      str(batch2["price"].dtype) != "float64" and str(batch2["size"].dtype) != "float64",
      f"price={batch2['price'].dtype} size={batch2['size'].dtype}")
try:
    written2 = store.write_ticks(batch2)
except Exception as exc:  # noqa: BLE001 - the whole point of this check
    written2 = -1
    check("write_ticks conforms a re-inferred batch", False, f"raised {exc!r}")
else:
    check("write_ticks conforms a re-inferred batch", written2 == 2, f"wrote {written2}")

bars2 = aggregate_ticks(store, "TICKCHK", "1m", base, base + timedelta(minutes=15))
check("second batch is queryable alongside the first", len(bars2) == 3, f"got {len(bars2)}")
late = [b for b in bars2 if b["date"] == base + timedelta(minutes=10)]
check("the re-inferred batch kept its values", bool(late) and late[0]["open"] == 104.0
      and late[0]["close"] == 105.0,
      f"got {late[0] if late else 'no 14:10 bucket'}")

# --- snapshots -------------------------------------------------------------
#
# `snap`'s payload column is a q GENERAL LIST -- the one column type in this
# schema that no other code path exercises, and it has never run against a
# real q. A char vector going into a general list, and coming back out of
# .pd() as something json.loads can read, are both assumptions until proven.
payload = {"symbol": "TICKCHK", "prev_close": 99.5,
           "nested": {"venues": ["A", "B"], "n": 3}, "unicode": "café"}
store.write_snapshot("TICKCHK", payload)
got = store.read_snapshot("TICKCHK", 3600.0)
check("read_snapshot round-trips the payload", got == payload, f"got {got!r}")
check("read_snapshot honours the TTL", store.read_snapshot("TICKCHK", 0.0) is None)
check("read_snapshot on an unknown symbol is None",
      store.read_snapshot("NOSUCHSYM", 3600.0) is None)

store.write_snapshot("TICKCHK", {"replaced": True})
check("write_snapshot upserts rather than duplicating",
      store.read_snapshot("TICKCHK", 3600.0) == {"replaced": True})

# --- read_ticks tie-break ---------------------------------------------------
#
# read_ticks orders newest-first via `xdesc reverse`, relying on q's `xdesc`
# being a STABLE sort so that ticks sharing one `time` come out in arrival
# order. No mocked test can check this -- the test fake substring-matches
# query text rather than actually sorting. Two ticks, one timestamp, written
# in arrival order: the later arrival must come back first.
tied_time = base + timedelta(minutes=20)
tied = pd.DataFrame({
    "time": [tied_time, tied_time],
    "sym": ["TICKCHK", "TICKCHK"],
    "price": [201.0, 202.0],  # 201 arrives first, 202 second
    "size": [1.0, 1.0],
})
store.write_ticks(tied)
newest_two = store.read_ticks("TICKCHK", 2)
check("read_ticks breaks a time tie with the later arrival first",
      bool(newest_two) and newest_two[0]["price"] == 202.0,
      f"got {newest_two[:2] if newest_two else newest_two}")

# --- ticks_present: live-grid's in-doubt probe (openbq-docker v10) -----------
#
# The tick spool asks this before re-sending a batch whose write raised after
# it was sent. Only the simulator has answered it in the cloud; this is the
# real q's table find (`qwin ? probe_ticks`) on the shapes live-grid sends.
check("ticks_present: a written batch is all present", store.ticks_present(ticks) == 4,
      f"got {store.ticks_present(ticks)}")
absent = ticks.assign(price=ticks["price"] + 0.5)
check("ticks_present: an unwritten batch is absent", store.ticks_present(absent) == 0,
      f"got {store.ticks_present(absent)}")
mixed = pd.concat([ticks.iloc[:2], absent.iloc[:1]], ignore_index=True)
check("ticks_present counts row by row", store.ticks_present(mixed) == 2,
      f"got {store.ticks_present(mixed)}")
from datetime import timezone  # noqa: E402 - live-grid's stamps are tz-aware UTC

utc = pd.DataFrame({
    "time": [datetime(2025, 6, 10, 15, 0, 0, 123456, tzinfo=timezone.utc),
             datetime(2025, 6, 10, 15, 0, 1, tzinfo=timezone.utc)],
    "sym": ["TICKCHK", "TICKCHK"],
    "price": [62000.123456789, 62000.25],
    "size": [0.00012, 0.0],
})
check("ticks_present before a tz-aware write", store.ticks_present(utc) == 0,
      f"got {store.ticks_present(utc)}")
store.write_ticks(utc)
check("ticks_present after a tz-aware write (live-grid's shape)", store.ticks_present(utc) == 2,
      f"got {store.ticks_present(utc)}")

store.prune_ticks(datetime(2030, 1, 1))
check("prune clears the window", store.tick_span("TICKCHK") is None)

session.close()
print()
print("FAILURES:", failures if failures else "none")
sys.exit(1 if failures else 0)
