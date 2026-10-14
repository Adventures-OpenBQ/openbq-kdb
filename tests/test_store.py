# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0

"""Store: q statement construction and eviction policy, against a fake connection."""

import re
from datetime import datetime, timedelta

from openbq_kdb.store import _INIT_SCHEMA, _PARAM_PREFIX, KdbStore

D = lambda s: datetime.fromisoformat(s)  # noqa: E731


def _ticks_frame(rows: int = 3):
    """A trades-shaped batch."""
    import pandas as pd

    base = D("2025-06-10T14:00:00")
    return pd.DataFrame({
        "time": [base + timedelta(seconds=i) for i in range(rows)],
        "sym": ["AAPL"] * rows,
        "price": [100.0 + i for i in range(rows)],
        "size": [1.0] * rows,
    })


class FakeConn:
    """Records queries; returns canned values by substring match."""

    def __init__(self, responses=None):
        self.queries = []
        self.calls = []  # (query, args) -- args matter for the lambda-binding check
        self.responses = responses or {}
        self.data = {}

    def __call__(self, query, *args):
        self.queries.append(query)
        self.calls.append((query, args))
        for needle, value in self.responses.items():
            if needle in query:
                return _Wrapped(value)
        return _Wrapped(None)

    def __setitem__(self, key, value):
        self.data[key] = value


class _Wrapped:
    def __init__(self, value):
        self._value = value

    def py(self):
        return self._value

    def pd(self):
        return self._value


class FakeSession:
    def __init__(self, conn):
        self._conn = conn
        self.runs = 0

    def connection(self):
        return self._conn

    def run(self, fn):
        """Stand-in for the real owner-thread marshalling: run it here, now."""
        self.runs += 1
        return fn()


def store_with(responses=None):
    conn = FakeConn(responses)
    return KdbStore(FakeSession(conn)), conn


def test_table_name_is_symbol_and_interval():
    s, _ = store_with()
    assert s.table_name("AAPL", "1d") == "bars_AAPL_1d"


def test_table_name_sanitizes_punctuation():
    """BTC-USD and EUR.FOREX must not produce invalid q identifiers."""
    s, _ = store_with()
    assert s.table_name("BTC-USD", "1d") == "bars_BTC_USD_1d"
    assert s.table_name("BRK.B", "5m") == "bars_BRK_B_5m"


def test_memory_reads_heap_not_used():
    """heap is what approaches wmax and kills q; used trails it."""
    s, _ = store_with({".Q.w[]": {"used": 100, "heap": 500, "wmax": 1000}})
    assert s.memory()["heap"] == 500


def test_evict_stops_once_below_budget():
    s, conn = store_with({
        ".Q.w[]": {"used": 100, "heap": 100, "wmax": 1000},
        ".cache.lru": [("AAPL", "1d", 1.0)],
    })
    assert s.evict_until_below(500) == []
    assert not any(".Q.gc" in q for q in conn.queries)


def test_evict_drops_oldest_first_and_collects_garbage():
    """delete alone frees `used` but not `heap`; .Q.gc[] is what reclaims it."""
    calls = {"n": 0}

    class ShrinkingConn(FakeConn):
        def __call__(self, query, *args):
            self.queries.append(query)
            if ".Q.w[]" in query:
                calls["n"] += 1
                heap = 1000 if calls["n"] == 1 else 100
                return _Wrapped({"used": heap, "heap": heap, "wmax": 4000})
            if ".cache.lru" in query and "select" in query:
                return _Wrapped([("OLD", "1d", 1.0), ("NEW", "1d", 99.0)])
            if "in key" in query:  # bars_OLD_1d exists, so its eviction counts
                return _Wrapped(True)
            return _Wrapped(None)

    conn = ShrinkingConn()
    s = KdbStore(FakeSession(conn))
    evicted = s.evict_until_below(500)
    assert evicted == ["bars_OLD_1d"]
    assert any(".Q.gc" in q for q in conn.queries)
    assert any("delete" in q and "bars_OLD_1d" in q for q in conn.queries)


def test_evict_warns_and_returns_empty_when_lru_is_empty_but_heap_over_budget(caplog):
    """An empty LRU can't tell us anything to evict -- but crossing q's -w kills
    the process, so failing to reach budget must be visible, not silent."""
    s, conn = store_with({
        ".Q.w[]": {"used": 900, "heap": 900, "wmax": 1000},
        ".cache.lru": [],
    })
    with caplog.at_level("WARNING"):
        evicted = s.evict_until_below(500)
    assert evicted == []
    assert any("budget" in r.message.lower() for r in caplog.records)
    assert not any(".Q.gc" in q for q in conn.queries)


def test_evict_does_not_count_a_stale_lru_row_whose_table_is_already_gone():
    """A row in .cache.lru naming a table that no longer exists must not be
    reported as an eviction -- callers use the return value to account for
    freed memory, and a phantom entry would overstate what was actually freed."""
    calls = {"n": 0}

    class StaleRowConn(FakeConn):
        def __call__(self, query, *args):
            self.queries.append(query)
            if ".Q.w[]" in query:
                calls["n"] += 1
                # heap never drops -- the phantom table was never really there
                # to free, so this also exercises the "exhausted the LRU
                # without reaching budget" warning path.
                return _Wrapped({"used": 900, "heap": 900, "wmax": 4000})
            if ".cache.lru" in query and "select" in query:
                return _Wrapped([("GHOST", "1d", 1.0)])
            if "in key" in query:  # bars_GHOST_1d does not exist
                return _Wrapped(False)
            return _Wrapped(None)

    conn = StaleRowConn()
    s = KdbStore(FakeSession(conn))
    evicted = s.evict_until_below(500)
    assert evicted == []


def test_read_coverage_returns_ranges():
    s, _ = store_with({
        ".cache.cov": [(D("2024-01-01"), D("2024-06-30"))],
    })
    assert s.read_coverage("AAPL", "1d") == [(D("2024-01-01"), D("2024-06-30"))]


def test_read_coverage_empty_for_unknown_symbol():
    s, _ = store_with({".cache.cov": []})
    assert s.read_coverage("NOPE", "1d") == []


def test_drop_removes_table_and_coverage():
    s, conn = store_with()
    s.drop("AAPL", "1d")
    joined = " ".join(conn.queries)
    assert "bars_AAPL_1d" in joined
    assert ".cache.cov" in joined


def test_schema_init_runs_before_first_coverage_read():
    """.cache.cov/.cache.lru must exist before anything queries or writes them."""
    s, conn = store_with({".cache.cov": []})
    s.read_coverage("AAPL", "1d")
    assert conn.queries[0] == _INIT_SCHEMA
    assert "select s, e from .cache.cov" in conn.queries[1]


def test_lru_schema_is_keyed_so_touch_upsert_replaces_not_appends():
    """.cache.lru must be keyed on (sym, iv): on an unkeyed table `upsert`
    appends, so repeated touches of the same symbol pile up stale rows that
    can outrank genuinely hot data during eviction."""
    s, conn = store_with()
    s.touch("AAPL", "1d")
    assert "([sym:`symbol$(); iv:`symbol$()] atime:`timestamp$())" in conn.queries[0]


def test_schema_init_runs_before_first_write():
    s, conn = store_with()
    s.touch("AAPL", "1d")
    assert conn.queries[0] == _INIT_SCHEMA
    assert ".cache.lru" in conn.queries[1]


def test_schema_init_reruns_on_new_connection_but_not_redundantly():
    """A respawned q is empty -- init must re-run for a new connection object,
    but not on every call against the connection already initialised."""
    conn1 = FakeConn()
    session = FakeSession(conn1)
    s = KdbStore(session)

    s.touch("AAPL", "1d")
    s.touch("AAPL", "1d")
    assert conn1.queries.count(_INIT_SCHEMA) == 1  # no redundant re-init

    conn2 = FakeConn()
    session._conn = conn2  # simulate session.connection() handing back a respawned q
    s.touch("AAPL", "1d")
    assert conn2.queries.count(_INIT_SCHEMA) == 1  # re-ran for the new connection
    assert conn1.queries.count(_INIT_SCHEMA) == 1  # old connection's log untouched


def test_no_statement_uses_a_leading_underscore_identifier():
    """`_incoming` was a q parse error (`_` is the drop/cut operator); guard
    against any recorded statement reintroducing an identifier like it."""
    s, conn = store_with({".cache.cov": [], ".cache.lru": []})
    s.write_bars("AAPL", "1d", object())
    s.read_bars("AAPL", "1d", D("2024-01-01"), D("2024-01-02"))
    s.read_coverage("AAPL", "1d")
    s.record_coverage("AAPL", "1d", (D("2024-01-01"), D("2024-01-02")))
    s.touch("AAPL", "1d")
    s.drop("AAPL", "1d")

    leading_underscore_ident = re.compile(r"(?<![\w.])_[A-Za-z]")
    for q in conn.queries:
        assert not leading_underscore_ident.search(q), q


def exercise_every_statement(store):
    """Drive every store method that issues q, on one fake connection."""
    store.write_bars("AAPL", "1d", object())
    store.read_bars("AAPL", "1d", D("2024-01-01"), D("2024-01-02"))
    store.read_coverage("AAPL", "1d")
    store.record_coverage("AAPL", "1d", (D("2024-01-01"), D("2024-01-02")))
    store.touch("AAPL", "1d")
    store.drop("AAPL", "1d")
    store.memory()
    store.evict_until_below(10**9)
    store.aggregate_frame("AAPL", "1m", D("2024-01-01"), D("2024-01-02"))
    store.write_snapshot("AAPL", {"close": 100.0, "volume": 10.0})
    store.read_snapshot("AAPL", 60.0)


def test_every_parameterised_statement_is_a_lambda():
    """q binds x/y/z/w as implicit parameters only INSIDE {...}. A bare
    expression referencing them raises 'x, which the cache swallows as a
    bypass -- so the whole cache silently did nothing against a real q."""
    s, conn = store_with({
        ".cache.cov": [], ".cache.lru": [],
        ".Q.w[]": {"used": 1, "heap": 1, "wmax": 10},
        "in key": True,
    })
    exercise_every_statement(s)

    parameterised = [(q, args) for q, args in conn.calls if args]
    assert parameterised, "no statement passed arguments -- the check is vacuous"
    for query, args in parameterised:
        assert query.startswith("{["), f"unbound implicit args in: {query}"
        # The lambda must actually declare as many parameters as are sent.
        declared = query[2:query.index("]")].split(";")
        assert len(declared) == len(args), f"{len(args)} args, {declared} declared: {query}"


def test_lambda_parameters_never_shadow_a_column_name():
    """Inside select/delete, a table's own columns shadow the lambda's
    parameters -- the query keeps running and returns wrong rows, no error.

    A hardcoded list of "known" columns can't prove a parameter is safe:
    bar-table columns are NOT a fixed schema, they come from whatever the
    upstream data provider returns (`cache.py` renames only the timestamp
    column to `t`), so a provider emitting a column literally named `a`
    would shadow a same-named parameter with no warning. (Verified against a
    real q before this fix: a bar table carrying a column named `a` made
    `read_bars` return 9 rows instead of 3 -- the wrong window, silently.)

    The only thing that provably can't collide with an arbitrary provider
    column is a name reserved under a prefix no provider would plausibly
    emit -- so this asserts every declared parameter lives under
    `_PARAM_PREFIX`, not merely that it avoids a list of columns seen so
    far. A future edit reintroducing a short name like `a` or `s` fails
    this test immediately, regardless of what table it touches.
    """
    s, conn = store_with({
        ".cache.cov": [], ".cache.lru": [],
        ".Q.w[]": {"used": 1, "heap": 1, "wmax": 10},
        "in key": True,
    })
    exercise_every_statement(s)

    parameterised = [(q, args) for q, args in conn.calls if args]
    assert parameterised, "no statement passed arguments -- the check is vacuous"
    for query, args in parameterised:
        declared = [p.strip() for p in query[2:query.index("]")].split(";")]
        for p in declared:
            assert p.startswith(_PARAM_PREFIX), (
                f"parameter {p!r} is not namespaced under the reserved prefix "
                f"{_PARAM_PREFIX!r} -- it could collide with a provider-supplied "
                f"column: {query}"
            )
            assert len(p) > len(_PARAM_PREFIX), f"parameter {p!r} is just the prefix: {query}"


def test_every_q_call_is_marshalled_onto_the_session_owner_thread():
    """PyKX is unusable from more than one thread -- not merely unserialised --
    so no query, K-object construction or conversion may happen outside
    session.run()."""
    conn = FakeConn({
        ".cache.cov": [], ".cache.lru": [],
        ".Q.w[]": {"used": 1, "heap": 1, "wmax": 10},
    })

    class StrictSession(FakeSession):
        """Refuses to hand out a connection except from inside run()."""

        def __init__(self, conn):
            super().__init__(conn)
            self.inside = False

        def connection(self):
            assert self.inside, "connection() reached outside run()"
            return self._conn

        def run(self, fn):
            self.runs += 1
            self.inside = True
            try:
                return fn()
            finally:
                self.inside = False

    session = StrictSession(conn)
    exercise_every_statement(KdbStore(session))
    assert session.runs >= 8


def test_schema_init_creates_the_trades_table():
    s, conn = store_with()
    s.write_ticks(_ticks_frame())
    joined = " ".join(conn.queries)
    assert "trades" in joined


def test_write_ticks_sends_one_batch_not_one_insert_per_row():
    """Per-tick IPC cannot keep up with a live feed."""
    s, conn = store_with()
    n = s.write_ticks(_ticks_frame(rows=50))
    assert n == 50
    inserts = [q for q in conn.queries if "insert" in q]
    assert len(inserts) == 1, f"expected one batched insert, got {len(inserts)}"


def test_write_ticks_routes_through_upd_when_the_server_defines_it():
    """q/startup.q's upd inserts AND publishes to websocket subscribers;
    the write must stay a bare insert on a q with no upd defined."""
    s, conn = store_with()
    s.write_ticks(_ticks_frame())
    query = next(q for q in conn.queries if "insert" in q)
    assert "`upd in key `." in query
    assert "upd[`trades;incoming_ticks]" in query
    assert "`trades insert incoming_ticks" in query


def test_prune_ticks_deletes_below_the_cutoff_and_collects():
    s, conn = store_with({"count trades": 3})
    s.prune_ticks(D("2025-06-10T14:00:00"))
    joined = " ".join(conn.queries)
    assert "delete" in joined and "trades" in joined
    assert any(".Q.gc" in q for q in conn.queries)


def test_tick_span_returns_none_when_no_ticks():
    s, _ = store_with({"select min time": None})
    assert s.tick_span("NOPE") is None


def test_tick_span_query_guards_the_empty_case_in_q_not_python():
    """Pins the presence of the `0 = count ...` guard in the query tick_span
    sends, so a regression can't silently drop it.

    What this proves: the guard clause is still in the outgoing q statement.
    What this does NOT prove: that the guard actually prevents the bug it was
    written for. FakeConn matches replies by substring and always hands back
    `None` for an unmatched query, so this suite passes identically whether or
    not the guard is present in a query whose result is never inspected here --
    it can only catch the guard's absence, not its effect. The real defect
    lived in PyKX: `.pd()` reinterprets a q null timestamp's raw int64 as
    nanoseconds since the q epoch and returns a real-looking (wrong) Timestamp
    instead of NaT, so `pd.isna()` never fired. Only a real q process can show
    that the guard actually suppresses that -- see openbq-kdb/scripts/tick_check.py.
    """
    s, conn = store_with()
    s.tick_span("AAPL")
    assert any("0 = count" in q for q in conn.queries), (
        "tick_span's query must guard the zero-row case in q -- an ungrouped "
        "aggregate over zero rows returns a row of nulls that PyKX silently "
        "mis-converts instead of an empty/absent result"
    )


def test_lambda_parameters_in_tick_queries_never_shadow_a_column():
    """A q lambda parameter matching a column name silently returns wrong rows."""
    import re

    s, conn = store_with()
    s.write_ticks(_ticks_frame())
    s.prune_ticks(D("2025-06-10T14:00:00"))
    s.tick_span("AAPL")
    columns = {"time", "sym", "price", "size", "t", "open", "high", "low", "close", "volume"}
    for query in conn.queries:
        for params in re.findall(r"\{\[([^\]]*)\]", query):
            for name in (p.strip() for p in params.split(";") if p.strip()):
                assert name not in columns, f"parameter {name!r} shadows a column in: {query}"
                assert name.startswith("qw"), f"parameter {name!r} lacks the qw prefix"


def test_latest_tick_checks_emptiness_in_q_not_in_pandas():
    """A q null timestamp survives .pd() as a real-looking 1700s Timestamp, so
    `pd.isna` cannot detect "no ticks" -- the guard has to run inside q, before
    the aggregate. Same trap tick_span documents."""
    conn = FakeConn()
    store = KdbStore(FakeSession(conn))
    store.latest_tick("AAPL")
    q = conn.queries[-1]
    assert "0 = count select from trades where sym = " in q, q
    assert q.index("0 = count") < q.index("max time"), "guard must precede the aggregate"


def test_latest_tick_returns_none_when_there_are_no_ticks():
    import pandas as pd

    conn = FakeConn()
    conn.responses["max time"] = pd.DataFrame({"time": [], "price": [], "size": []})
    store = KdbStore(FakeSession(conn))
    assert store.latest_tick("AAPL") is None


def test_latest_tick_returns_the_newest_row():
    import pandas as pd

    conn = FakeConn()
    conn.responses["max time"] = pd.DataFrame({
        "time": [pd.Timestamp("2026-08-26T15:14:00")],
        "price": [312.95],
        "size": [40.0],
    })
    store = KdbStore(FakeSession(conn))
    got = store.latest_tick("AAPL")
    assert got["price"] == 312.95
    assert got["size"] == 40.0
    assert got["time"] == D("2026-08-26T15:14:00")


def test_latest_tick_binds_the_symbol_as_a_parameter_not_by_interpolation():
    """Interpolating the symbol into the q string would let a symbol containing
    q syntax change the statement."""
    conn = FakeConn()
    store = KdbStore(FakeSession(conn))
    store.latest_tick("AAPL")
    query, args = conn.calls[-1]
    assert "AAPL" not in query
    assert args, "symbol must be passed as a bound argument"


def test_read_ticks_returns_newest_first():
    import pandas as pd

    s, conn = store_with({
        "xdesc": pd.DataFrame({
            "time": [
                pd.Timestamp("2026-09-03T15:00:02"),
                pd.Timestamp("2026-09-03T15:00:01"),
            ],
            "price": [191.5, 191.0],
            "size": [100.0, 50.0],
        }),
    })
    got = s.read_ticks("AAPL", 10)
    assert [r["price"] for r in got] == [191.5, 191.0]
    assert got[0]["time"] == D("2026-09-03T15:00:02")
    assert got[0]["size"] == 100.0


def test_read_ticks_on_an_empty_cache_is_an_empty_list():
    """A restarted kdb holds no ticks. That is empty, never an error."""
    import pandas as pd

    s, _ = store_with({
        "xdesc": pd.DataFrame({"time": [], "price": [], "size": []}),
    })
    assert s.read_ticks("AAPL", 10) == []


def test_read_ticks_with_a_non_positive_limit_is_an_empty_list():
    """q's `sublist` takes from the end of the list for a negative left
    argument, which would silently hand back the OLDEST ticks instead of the
    newest -- guarded in Python before any query is issued."""
    s, conn = store_with()
    assert s.read_ticks("AAPL", -5) == []
    assert s.read_ticks("AAPL", 0) == []
    assert conn.calls == [], "a non-positive limit must not reach q at all"


def test_read_ticks_binds_the_symbol_as_a_parameter_not_by_interpolation():
    """Interpolating the symbol into the q string would let a symbol containing
    q syntax change the statement."""
    conn = FakeConn()
    store = KdbStore(FakeSession(conn))
    store.read_ticks("AAPL", 10)
    query, args = conn.calls[-1]
    assert "AAPL" not in query
    assert args, "symbol must be passed as a bound argument"


def test_read_ticks_breaks_time_ties_with_the_later_arrival_first():
    """Two ticks sharing one `time`, recorded old-then-new: read_ticks must
    return the later arrival first -- the same tie-break `latest_tick`
    documents two methods above.

    `xdesc` is a STABLE sort: ties keep their original (arrival) order, so a
    bare `` `time xdesc `` would present the earlier arrival as the newer
    one, backwards. The fix reverses the selection before that stable sort.

    FakeConn can't execute q, so this pins the fix at the query-routing
    level instead of the data level: the canned response below only matches
    a query containing "xdesc reverse". A regression that drops `reverse`
    sends a query this response won't match; FakeConn then falls through to
    its default `None` reply, and read_ticks returns `[]` instead of the two
    ticks below -- so this fails on the regression itself, not merely on the
    query text's shape.
    """
    import pandas as pd

    tied = pd.Timestamp("2026-09-03T15:00:00")
    s, _ = store_with({
        "xdesc reverse": pd.DataFrame({
            "time": [tied, tied],
            "price": [102.0, 101.0],  # later arrival first, as the fix produces
            "size": [2.0, 1.0],
        }),
    })
    got = s.read_ticks("AAPL", 10)
    assert [r["price"] for r in got] == [102.0, 101.0]


def test_write_snapshot_stores_a_fetch_time():
    s, conn = store_with()
    s.write_snapshot("AAPL", {"close": 100.0, "volume": 10.0})
    joined = " ".join(conn.queries)
    assert "snap" in joined


def test_read_snapshot_returns_none_when_absent():
    s, _ = store_with({"select from snap": None})
    assert s.read_snapshot("NOPE", 60.0) is None


def test_read_snapshot_ttl_is_judged_against_utc_not_local_wall_clock():
    """`fetched` comes back from q's `.z.p`, which is UTC -- but PyKX hands it
    to Python as a tz-NAIVE pandas Timestamp. Comparing that against local
    wall-clock time (`pd.Timestamp.now()` with no tz) mixes clocks by however
    many hours the host is offset from UTC: on UTC-5 "age" goes negative and
    the cache never expires (stale data served as fresh, the dangerous
    direction); on UTC+1 "age" is already past any TTL the instant the row is
    written (the cache never hits at all).

    The host running this test may itself sit at UTC+0, where the bug happens
    not to manifest -- so the local TZ is pinned to America/New_York (a large,
    real offset) for the duration of the test, making the result the same
    regardless of what timezone the test machine actually runs in.
    """
    import os
    import time as time_module

    import pandas as pd

    old_tz = os.environ.get("TZ")
    os.environ["TZ"] = "America/New_York"
    time_module.tzset()
    try:
        elapsed = 30.0
        # A UTC instant `elapsed` seconds ago, stored tz-naive -- exactly the
        # shape PyKX's .pd() hands back for a q timestamp column.
        fetched = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(seconds=elapsed)).tz_localize(None)
        s, _ = store_with({
            "select fetched, payload from snap": pd.DataFrame({
                "fetched": [fetched],
                "payload": ['{"close": 1.0}'],
            }),
        })
        assert s.read_snapshot("AAPL", max_age=elapsed + 10) == {"close": 1.0}, (
            "an entry younger than the TTL must be judged fresh"
        )
        assert s.read_snapshot("AAPL", max_age=elapsed - 10) is None, (
            "an entry older than the TTL must be judged stale"
        )
    finally:
        if old_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = old_tz
        time_module.tzset()


def test_ticks_present_probes_only_the_batch_window_and_cleans_up():
    """The in-doubt probe of live-grid's tick spool (openbq-docker v10)."""
    s, conn = store_with({"sum (count qwin)": 3})
    assert s.ticks_present(_ticks_frame(rows=3)) == 3
    assert "probe_ticks" in conn.data
    probe = next(q for q in conn.queries if "probe_ticks`time" in q)
    assert "within (min probe_ticks`time; max probe_ticks`time)" in probe
    assert "qwin ? probe_ticks" in probe
    assert conn.queries[-1] == "delete probe_ticks from `."
    # no parameters: nothing for the lambda-binding rules to bind
    assert all(not args for q, args in conn.calls if "probe_ticks`time" in q)


def test_ticks_present_of_an_empty_frame_never_dials_q():
    import pandas as pd

    s, conn = store_with()
    assert s.ticks_present(pd.DataFrame(columns=["time", "sym", "price", "size"])) == 0
    assert conn.queries == []
