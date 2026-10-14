# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0

"""Every q statement the cache issues.

Kept in one module so the q surface is auditable and mockable in one place.

Five measured facts shape this file:
  * `heap`, not `used`, is what approaches `wmax` and kills q, so eviction
    watches heap.
  * `delete` frees `used` but leaves `heap` untouched; only `.Q.gc[]` returns
    it. Every eviction therefore ends in a collect.
  * `upsert` matches column types EXACTLY -- an incoming column whose q type
    differs from the stored one raises `'type`, it is not coerced -- while
    pandas re-infers each column's dtype from the values of whichever batch
    it is handed. Every write therefore conforms the batch to the stored
    table's schema first; see `_conform_dtypes`.
  * q binds `x`/`y`/`z` as implicit parameters only INSIDE a lambda. A bare
    `conn("... where sym = x", sym)` sends an expression that references an
    unbound `x` and q answers `'x` -- which the cache catches and degrades to
    a bypass, so the whole cache silently did nothing while looking healthy.
    Every parameterised statement here is therefore an explicit-parameter
    lambda `{[qwlo;qwhi] ...}`. Inside a `select`/`delete`, a lambda
    parameter whose name matches a column is silently SHADOWED by that
    column -- the query keeps running and returns wrong rows, no error. This
    isn't just `.cache.cov`/`.cache.lru` (`sym`, `iv`, `s`, `e`, `atime`,
    fixed schemas this code controls): bar-table columns come from whatever
    an upstream data provider returns (`cache.py` renames only the timestamp
    column to `t`), so a short parameter name like `a` or `b` can collide
    with a real provider column with no warning. Every parameter name here
    is therefore namespaced under the `qw` prefix (`_PARAM_PREFIX` below) --
    a string no market-data provider would ever emit as a column name -- so
    the whole class of collision is closed, not patched case by case.
  * An UNGROUPED aggregate (`min`/`max` with no `by`) always returns exactly
    one row, even over zero matching input rows, and PyKX's `.pd()` turns that
    row's q null timestamp into a bogus 1700s-era `Timestamp` rather than
    `NaT` -- so `pd.isna()` cannot detect "no data" after the fact. Found live
    by `scripts/tick_check.py` against a real q: `prune_ticks` correctly
    emptied `trades`, but the next `tick_span` call, on an empty table,
    returned a fabricated span instead of `None`. See `tick_span` for the fix
    (check the row count in q before aggregating, not the pandas result after).

Nothing in here may run off the session's owner thread -- see session.py.
Every query, every K-object construction (`_q_symbol`, `_q_timestamp`) and
every conversion back to Python (`.py()`, `.pd()`) happens inside a callable
handed to `session.run()`.
"""

import logging
import re
from datetime import datetime

from openbq_kdb.ranges import Range

logger = logging.getLogger(__name__)

_SAFE = re.compile(r"[^A-Za-z0-9_]")

# Every lambda parameter in this module is namespaced under this prefix so
# that no bar-table column -- which comes from an arbitrary upstream data
# provider, not a schema this code controls -- can ever collide with, and
# silently shadow, a parameter inside a select/delete. See the module
# docstring for the full hazard.
_PARAM_PREFIX = "qw"

# `.cache.cov` / `.cache.lru` don't exist on a fresh q process, and a q that
# crosses its -w gets silently respawned empty (see session.py) -- so this
# can't be a one-time Python-side flag. `if[not X in key `.cache; ...]` is
# idempotent on the q side (a no-op, not a wipe, if the tables already
# exist), and _conn() re-issues it whenever the session hands back a
# connection object we haven't initialised yet.
_INIT_SCHEMA = (
    "if[not `cov in key `.cache; .cache.cov: "
    "([] sym:`symbol$(); iv:`symbol$(); s:`timestamp$(); e:`timestamp$())]; "
    "if[not `lru in key `.cache; .cache.lru: "
    "([sym:`symbol$(); iv:`symbol$()] atime:`timestamp$())]; "
    "if[not `trades in key `.; trades: "
    "([] time:`timestamp$(); sym:`symbol$(); price:`float$(); size:`float$())]; "
    "if[not `snap in key `.; snap: "
    "([sym:`symbol$()] fetched:`timestamp$(); payload:())]"
)


class KdbStore:
    """Typed access to the cache's q state."""

    def __init__(self, session):
        self.session = session
        self._schema_conn = None  # the connection object last initialised

    def _conn(self):
        conn = self.session.connection()
        if conn is not self._schema_conn:
            conn(_INIT_SCHEMA)
            self._schema_conn = conn
        return conn

    def _call(self, fn):
        """Run `fn(conn)` on the session's q owner thread and return its result.

        `fn` must do ALL of its PyKX work inside itself -- returning a K object
        for the caller to convert would move the conversion back off the owner
        thread, which is exactly the thing that corrupts the heap.
        """
        return self.session.run(lambda: fn(self._conn()))

    @staticmethod
    def table_name(symbol: str, interval: str) -> str:
        """A valid q identifier for one (symbol, interval) pair."""
        sym = _SAFE.sub("_", str(symbol).strip().upper())
        iv = _SAFE.sub("_", str(interval).strip())
        return f"bars_{sym}_{iv}"

    def memory(self) -> dict:
        """`.Q.w[]` as a plain dict with str keys (used, heap, wmax, ...)."""
        return self._call(lambda conn: dict(conn(".Q.w[]").py()))

    def read_bars(self, symbol: str, interval: str, start: datetime, end: datetime):
        """Bars within [start, end] as a pandas DataFrame; empty if absent."""
        import pandas as pd

        name = self.table_name(symbol, interval)

        def read(conn):
            if not conn(f"`{name} in key `.").py():
                return None
            # qwlo/qwhi, not a/b: bar-table columns are whatever the upstream
            # provider returned, and a provider-emitted column named `a` or
            # `b` would silently shadow a same-named parameter here.
            return conn(
                f"{{[qwlo;qwhi] select from {name} where t >= qwlo, t <= qwhi}}",
                _q_timestamp(start),
                _q_timestamp(end),
            ).pd()

        out = self._call(read)
        return out if out is not None else pd.DataFrame()

    def write_bars(self, symbol: str, interval: str, df) -> None:
        """Upsert bars, keeping the table sorted and free of duplicate stamps."""
        name = self.table_name(symbol, interval)

        def write(conn):
            # q's upsert demands the incoming column types match the stored
            # ones exactly, and pandas re-infers them from each batch's
            # values -- so the batch is bent to the stored table's schema
            # before it goes over the wire. See _conform_dtypes.
            incoming = df
            if conn(f"`{name} in key `.").py():
                incoming = _conform_dtypes(df, conn(f"0#{name}").pd())
            # A leading underscore is q's drop/cut operator, not a valid
            # identifier start -- `incoming_bars` can't collide with a
            # `bars_<SYMBOL>_<INTERVAL>` cache table.
            conn["incoming_bars"] = incoming
            conn(
                f"{name}: `t xasc 0!(`t xkey $[`{name} in key `.; {name}; 0#incoming_bars]) "
                "upsert incoming_bars"
            )
            conn("delete incoming_bars from `.")

        self._call(write)

    def read_coverage(self, symbol: str, interval: str) -> list[Range]:
        """Ranges already fetched for this (symbol, interval)."""
        rows = self._call(lambda conn: conn(
            "{[qwsym;qwiv] select s, e from .cache.cov where sym = qwsym, iv = qwiv}",
            _q_symbol(symbol), _q_symbol(interval),
        ).py())
        if not rows:
            return []
        if isinstance(rows, dict):  # column-oriented result
            return list(zip(rows["s"], rows["e"]))
        return [(r[0], r[1]) for r in rows]

    def record_coverage(self, symbol: str, interval: str, r: Range) -> None:
        """Append a covered range. Coalescing happens on read, in Python.

        `.cache.cov` is a dotted (fully qualified) name, so assigning to it
        inside a lambda amends the global table, not a function local.
        """
        self._call(lambda conn: conn(
            "{[qwsym;qwiv;qws;qwe] .cache.cov: .cache.cov upsert (qwsym; qwiv; qws; qwe)}",
            _q_symbol(symbol), _q_symbol(interval),
            _q_timestamp(r[0]), _q_timestamp(r[1]),
        ))

    def touch(self, symbol: str, interval: str) -> None:
        """Record an access for LRU ordering."""
        self._call(lambda conn: conn(
            "{[qwsym;qwiv] .cache.lru: .cache.lru upsert (qwsym; qwiv; .z.p)}",
            _q_symbol(symbol), _q_symbol(interval),
        ))

    def drop(self, symbol: str, interval: str) -> None:
        """Remove a table, its coverage, and its LRU entry, then collect."""
        name = self.table_name(symbol, interval)

        def do_drop(conn):
            conn(f"if[`{name} in key `.; delete {name} from `.]")
            # `delete from `.cache.cov` amends the global table in place, which
            # is why these lambdas need no assignment back.
            conn(
                "{[qwsym;qwiv] delete from `.cache.cov where sym = qwsym, iv = qwiv}",
                _q_symbol(symbol), _q_symbol(interval),
            )
            conn(
                "{[qwsym;qwiv] delete from `.cache.lru where sym = qwsym, iv = qwiv}",
                _q_symbol(symbol), _q_symbol(interval),
            )
            conn(".Q.gc[]")

        self._call(do_drop)

    def evict_until_below(self, budget_bytes: int) -> list[str]:
        """Drop least-recently-used entries until heap is under budget.

        Preventive by necessity: crossing q's -w does not raise, it kills the
        process. Returns the table names actually evicted -- a name is only
        appended once its table is confirmed to have existed, so a stale LRU
        row naming an already-gone table isn't counted as an eviction.

        Logs a warning (heap left over budget) if the LRU runs out before the
        budget is reached, including if the LRU was empty to begin with:
        crossing q's -w kills the process, so "we couldn't get under budget"
        is exactly the condition an operator needs surfaced.
        """
        evicted: list[str] = []
        heap = self.memory().get("heap", 0)
        if heap <= budget_bytes:
            return evicted
        rows = self._call(
            lambda conn: conn("select sym, iv, atime from .cache.lru").py()
        ) or []
        if isinstance(rows, dict):
            rows = list(zip(rows["sym"], rows["iv"], rows["atime"]))
        for sym, iv, _ in sorted(rows, key=lambda r: r[2]):
            sym = sym.decode() if isinstance(sym, bytes) else str(sym)
            iv = iv.decode() if isinstance(iv, bytes) else str(iv)
            name = self.table_name(sym, iv)
            existed = bool(self._call(lambda conn: conn(f"`{name} in key `.").py()))
            self.drop(sym, iv)
            if existed:
                evicted.append(name)
            heap = self.memory().get("heap", 0)
            if heap <= budget_bytes:
                return evicted
        logger.warning(
            "evict_until_below exhausted the LRU without reaching budget "
            "(heap=%s budget=%s)", heap, budget_bytes,
        )
        return evicted

    def write_ticks(self, frame) -> int:
        """Batch-insert ticks. One IPC round-trip per flush, never per tick.

        The batch is conformed to the stored column types first: q's `insert`
        rejects a type mismatch outright rather than coercing, and pandas
        re-infers dtypes per batch (an all-null size column arrives as object
        where the stored column is float).
        """
        if frame is None or getattr(frame, "empty", True):
            return 0

        def write(conn):
            prototype = conn("0#trades").pd()
            conn["incoming_ticks"] = _conform_dtypes(frame, prototype)
            # A q loaded with q/startup.q defines `upd`, which inserts AND
            # publishes the batch to websocket subscribers; a plain q has no
            # upd, so fall back to the bare insert.
            conn("$[`upd in key `.; upd[`trades;incoming_ticks]; `trades insert incoming_ticks]")
            conn("delete incoming_ticks from `.")
            return len(frame)

        return self._call(write)

    def ticks_present(self, frame) -> int:
        """How many rows of `frame` `trades` already holds, matching all four columns.

        live-grid's tick spool (openbq-docker v10) asks this before it replays a
        batch whose earlier write is IN DOUBT: one sent to q whose reply never
        came back (the connection died mid-call). `write_ticks` applies a batch
        all or nothing -- it lands in q in ONE `upd`/`insert` message, and
        persist.q logs that message before applying it, so a q killed half way
        replays it whole at boot -- so "every row present" means the batch was
        applied and must not be written again, and anything less means it was
        not. Exact float and timestamp equality is right here: the probe is the
        very values the first attempt sent.

        Only the time window the batch spans is searched (`within` on the
        batch's own min and max), so the cost is proportional to the ticks in
        that window, not the whole day. A probe sent to a q whose `trades` is
        empty answers 0. Rows are found with q's table find (`qwin ? probe`): the
        index of each probe row in the window, or `count qwin` when absent.
        """
        if frame is None or getattr(frame, "empty", True):
            return 0

        def probe(conn):
            prototype = conn("0#trades").pd()
            conn["probe_ticks"] = _conform_dtypes(frame, prototype)
            try:
                found = conn(
                    "{qwin: select time, sym, price, size from trades"
                    " where time within (min probe_ticks`time; max probe_ticks`time);"
                    " sum (count qwin) > qwin ? probe_ticks}[]"
                ).py()
            finally:
                conn("delete probe_ticks from `.")
            return int(found or 0)

        return self._call(probe)

    def prune_ticks(self, cutoff: datetime) -> int:
        """Drop ticks older than `cutoff` and return the row count remaining.

        `delete` frees `used` but not `heap`; only `.Q.gc[]` returns it.
        """
        def prune(conn):
            conn("{[qwcut] trades:: delete from trades where time < qwcut}", _q_timestamp(cutoff))
            conn(".Q.gc[]")
            remaining = conn("count trades").py()
            return int(remaining) if remaining is not None else 0

        return self._call(prune)

    def tick_span(self, symbol: str):
        """Earliest and latest tick held for a symbol, or None if there are none.

        An UNGROUPED q aggregate (no `by`) always answers with exactly one row,
        even over zero matching input rows -- there is no group-by key to be
        absent, so `min`/`max` of an empty selection is a row of q nulls (`0Nt`),
        not an empty table. `got.empty` therefore can't detect "no ticks" here
        the way it does for a keyed/grouped result.
        Worse: PyKX's `.pd()` does not turn that q null into pandas `NaT` -- it
        reinterprets the null's raw int64 bit pattern as a nanosecond offset from
        the q epoch (2000-01-01) and hands back a real-looking `Timestamp` deep in
        the 1700s. `pd.isna()` never fires because the value isn't NaN/NaT, just
        wrong -- caught live by `tick_check.py` against a real q, invisible to
        every mocked test. So the emptiness check has to happen in q, before the
        aggregate ever runs: 0 matching rows returns an explicitly empty table
        instead of an aggregate over nothing.
        """
        import pandas as pd

        def span(conn):
            got = conn(
                "{[qwsym] $[0 = count select from trades where sym = qwsym;"
                " ([] lo:`timestamp$(); hi:`timestamp$());"
                " select lo: min time, hi: max time from trades where sym = qwsym]}",
                _q_symbol(symbol),
            ).pd()
            if got is None or got.empty:
                return None
            lo, hi = got["lo"].iloc[0], got["hi"].iloc[0]
            if pd.isna(lo) or pd.isna(hi):
                return None
            return (lo.to_pydatetime(), hi.to_pydatetime())

        return self._call(span)

    def latest_tick(self, symbol: str):
        """The newest tick held for a symbol, or None if there are none.

        The emptiness check runs in q, before the aggregate, for the reason
        `tick_span` spells out: an ungrouped q aggregate over zero rows yields
        a row of q nulls, and PyKX's `.pd()` turns a null timestamp into a
        plausible-looking 1700s `Timestamp` rather than `NaT`, so no pandas-side
        check can catch it.
        """
        import pandas as pd

        def newest(conn):
            got = conn(
                "{[qwsym] $[0 = count select from trades where sym = qwsym;"
                " ([] time:`timestamp$(); price:`float$(); size:`float$());"
                " select time, price, size from trades"
                " where sym = qwsym, time = max time]}",
                _q_symbol(symbol),
            ).pd()
            if got is None or got.empty:
                return None
            # On a `time` tie, iloc[-1] picks the last row in table order --
            # correct because kdb preserves insertion order and TickRecorder
            # appends ticks in arrival order, so the last row is the newest
            # arrival, not an arbitrary one.
            row = got.iloc[-1]
            if pd.isna(row["time"]):
                return None
            return {
                "time": row["time"].to_pydatetime(),
                "price": float(row["price"]),
                "size": float(row["size"]),
            }

        return self._call(newest)

    def read_ticks(self, symbol: str, limit: int) -> list[dict]:
        """The newest `limit` ticks held for a symbol, newest first.

        Unlike `tick_span` and `latest_tick` this needs no in-q emptiness
        guard: those are ungrouped AGGREGATES, which over zero rows answer one
        row of q nulls that PyKX misreads as a 1700s timestamp. This is a plain
        selection -- zero matching rows is an empty table, and `.pd()` gives an
        empty frame, which iterates zero times.

        `xdesc` then `sublist` sorts the symbol's whole tick history per call.
        That is the same trade `startup.q` makes for its as-of join and is
        cheap at cache scale; kdb is a one-day rolling window here, not a
        historical database.

        `xdesc` is a STABLE sort: ticks sharing one `time` come out in their
        original, arrival order, not reversed. `reverse` runs first so that,
        after the stable sort, arrival order among tied ticks reads newest
        first -- the same tie-break `latest_tick` documents above.

        A non-positive `limit` returns an empty list. q's `sublist` takes
        from the *end* of the list for a negative left argument, which would
        silently hand back the OLDEST ticks instead -- guarded here rather
        than relying on the one caller (`/ticks`) that happens to reject it.
        """
        if limit <= 0:
            return []

        def read(conn):
            got = conn(
                "{[qwsym;qwlim] qwlim sublist `time xdesc reverse"
                " select time, price, size from trades where sym = qwsym}",
                _q_symbol(symbol),
                limit,
            ).pd()
            if got is None or got.empty:
                return []
            return [
                {
                    "time": row["time"].to_pydatetime(),
                    "price": float(row["price"]),
                    "size": float(row["size"]),
                }
                for _, row in got.iterrows()
            ]

        return self._call(read)

    def aggregate_frame(self, symbol: str, interval: str, start, end):
        """OHLCV buckets for one symbol, aggregated in q.

        `time xasc` is REQUIRED: ticks arrive out of order, and `first`/`last`
        on an unsorted table silently produce the wrong open and close. The
        result comes back keyed, so `0!` it before .pd().
        """
        from openbq_kdb.aggregate import bucket_ns

        width = bucket_ns(interval)

        def agg(conn):
            return conn(
                "{[qwsym;qwlo;qwhi;qwbucket]"
                " 0!select open:first price, high:max price, low:min price,"
                " close:last price, volume:sum size, vwap:size wavg price"
                " by t: qwbucket xbar time"
                " from `time xasc select from trades"
                " where sym=qwsym, time within (qwlo;qwhi)}",
                _q_symbol(symbol),
                _q_timestamp(start),
                _q_timestamp(end),
                width,
            ).pd()

        return self._call(agg)

    def write_snapshot(self, symbol: str, payload: dict) -> None:
        """Store a REST snapshot with its fetch time, for TTL reuse."""
        import json

        def write(conn):
            conn(
                "{[qwsym;qwpayload] snap:: snap upsert (qwsym; .z.p; qwpayload)}",
                _q_symbol(symbol),
                json.dumps(payload),
            )

        self._call(write)

    def read_snapshot(self, symbol: str, max_age: float) -> dict | None:
        """Return a snapshot fetched within `max_age` seconds, else None."""
        import json

        def read(conn):
            got = conn(
                "{[qwsym] select fetched, payload from snap where sym = qwsym}",
                _q_symbol(symbol),
            ).pd()
            if got is None or got.empty:
                return None
            import pandas as pd

            fetched = got["fetched"].iloc[0]
            if pd.isna(fetched):
                return None
            # `fetched` was written from q's `.z.p`, which is UTC -- but PyKX's
            # .pd() hands back a tz-NAIVE pandas Timestamp (it drops the zone,
            # it does not convert to local time). Comparing that naive-but-UTC
            # value against a naive-but-LOCAL `pd.Timestamp.now()` mixes the
            # two clocks: on UTC-5 the resulting "age" starts negative and
            # stays under any sane TTL for hours (the cache never refreshes,
            # serving stale data as fresh); on UTC+1 "age" is already past the
            # TTL the instant the row is written (the cache never hits). Pin
            # both sides to UTC explicitly rather than assuming either one's
            # tz-awareness.
            fetched = pd.Timestamp(fetched)
            fetched = (
                fetched.tz_localize("UTC")
                if fetched.tzinfo is None
                else fetched.tz_convert("UTC")
            )
            age = (pd.Timestamp.now(tz="UTC") - fetched).total_seconds()
            if age > max_age:
                return None
            raw = got["payload"].iloc[0]
            if isinstance(raw, bytes):
                raw = raw.decode()
            return json.loads(raw)

        return self._call(read)


def _conform_dtypes(df, prototype):
    """`df` with the column types the cached table already has.

    q's `upsert` requires the incoming column's type to MATCH the stored
    column's type; a mismatch raises `'type` rather than coercing. pandas, on
    the other side of the boundary, infers each column's dtype from the values
    in ONE batch -- so the same provider column arrives as float64 in a batch
    that carries values and as object in a batch where every value is None
    (an all-None column has no dtype to infer), and an int64 volume column
    arrives as float64 in any batch holding a NaN. Whichever batch is written
    first therefore fixes the q type of every column, and any later batch that
    infers differently is rejected outright.

    A purely historical window hides this completely: its second request is a
    full cache hit that writes nothing. A window reaching the present cannot --
    the forming bar is never cached, so every repeat request writes a second,
    much smaller batch into the same table, and a small batch is exactly the
    one whose columns come back empty and untyped.

    Casting is one-directional on purpose. The STORED column keeps its type and
    the incoming batch is bent to fit; recasting the stored column to follow the
    newest batch would let one integer-valued batch truncate a float price
    column. A column that cannot be cast is left alone -- the upsert then fails
    exactly as it does today and the request degrades to a bypass, which is no
    worse than before and never silently wrong.
    """
    import numpy as np

    columns = getattr(df, "columns", None)
    prototype_columns = getattr(prototype, "columns", None)
    if columns is None or prototype_columns is None:
        # Nothing frame-shaped to conform, or nothing to conform against:
        # hand the batch straight through, exactly as before this existed.
        return df

    out = df
    for column in columns:
        if column not in prototype.columns:
            continue
        want = prototype[column].dtype
        values = df[column]
        if values.dtype == want:
            continue
        try:
            if want.kind == "i" and values.isna().any():
                # q casts a float null to the integer null of that width;
                # pandas raises instead, so the mapping is made explicit.
                values = values.fillna(np.iinfo(want.type).min)
                converted = values.astype(want)
            else:
                converted = values.astype(want)
                if want.kind == "i" and not (converted == values).all():
                    # astype silently TRUNCATES float->int (133610200.7 ->
                    # 133610200) rather than raising, so the except clause
                    # below never sees it. Checking the round-trip makes the
                    # loss explicit and falls through to the same bypass path
                    # as any other unconvertible column, instead of writing a
                    # value the provider never sent.
                    continue
        except (TypeError, ValueError, OverflowError) as exc:
            logger.debug("cannot conform column %r to %s: %s", column, want, exc)
            continue
        if out is df:
            out = df.copy()
        out[column] = converted
    return out


def _q_symbol(value: str):
    import pykx as kx

    return kx.SymbolAtom(str(value))


def _q_timestamp(value: datetime):
    import pykx as kx

    return kx.TimestampAtom(value)
