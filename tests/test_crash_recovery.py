# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0
"""The real KdbSession and KdbStore across a kdb restart (openbq-docker v10).

The q at the other end is q/tests/tickplant_sim.py, a model of startup.q +
persist.q (its tick log and -11! replay); there is no q binary in the cloud.
Only `KdbSession._connect` is replaced -- it dials the simulated host instead
of PyKX -- so the liveness check, the reconnect chain, the expiring retry latch
and the schema re-initialisation are the shipped code.
"""
import importlib.util
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from openbq_kdb import session as session_mod
from openbq_kdb.config import resolve_config
from openbq_kdb.session import KdbSession, KdbUnavailable
from openbq_kdb.store import KdbStore

_SIM = Path(__file__).resolve().parents[1] / "q" / "tests" / "tickplant_sim.py"
_spec = importlib.util.spec_from_file_location("tickplant_sim", _SIM)
sim = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sim)


def _ticks(n, start=datetime(2026, 10, 9, 14, 30), sym="AAPL"):
    return pd.DataFrame({
        "time": [start + timedelta(seconds=i) for i in range(n)],
        "sym": [sym] * n,
        "price": [100.0 + i for i in range(n)],
        "size": [1.0] * n,
    })


@pytest.fixture
def host(tmp_path, monkeypatch):
    h = sim.Host(tmp_path, today=lambda: date(2026, 10, 9))
    monkeypatch.setattr(KdbSession, "_connect", lambda self, host=None: h.connect())
    monkeypatch.setattr(session_mod, "_RETRY_AFTER_S", 0.0)
    # live-grid's environment in docker-compose.yml
    for key, value in {"KDB_EMBEDDED": "false", "KDB_BIND_HOST": "kdb", "KDB_PORT": "5000"}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("KDB_HOST", raising=False)
    return h


def _store():
    # The compose wiring: live-grid never spawns, it dials kdb:5000 (link 2).
    cfg = resolve_config()
    assert not cfg.may_spawn and cfg.bind_host == "kdb" and cfg.port == 5000
    return KdbStore(KdbSession(cfg))


def test_openbq_kdb_reconnects_to_a_restarted_q_and_reads_the_replayed_day(host):
    host.start()
    store = _store()
    assert store.write_ticks(_ticks(120)) == 120
    lo, hi = store.tick_span("AAPL")
    host.kill()
    with pytest.raises(Exception):
        store.tick_span("AAPL")  # the dead connection fails its 1+1 and nothing answers
    host.start()
    assert store.tick_span("AAPL") == (lo, hi)
    assert store.session.endpoint == "loopback"  # link 2: bind_host
    assert len(store.read_ticks("AAPL", 1000)) == 120
    # writes after the restart land in the new log and survive the next restart
    store.write_ticks(_ticks(30, start=datetime(2026, 10, 9, 16)))
    host.kill()
    host.start()
    assert len(store.read_ticks("AAPL", 1000)) == 150
    store.session.close()


def test_the_retry_latch_holds_off_then_picks_up_a_q_that_came_back(host, monkeypatch):
    monkeypatch.setattr(session_mod, "_RETRY_AFTER_S", 60.0)
    store = _store()
    with pytest.raises(KdbUnavailable):
        store.tick_span("AAPL")  # no q yet: latched for 60 s
    host.start()
    with pytest.raises(KdbUnavailable, match="not retrying"):
        store.tick_span("AAPL")
    store.session._retry_after = 0.0  # the interval elapses
    assert store.tick_span("AAPL") is None
    store.session.close()


def test_schema_is_reissued_on_the_new_connection(host):
    host.start()
    store = _store()
    store.write_ticks(_ticks(1))
    first = store._schema_conn
    host.kill()
    host.start()
    store.tick_span("AAPL")
    assert store._schema_conn is not first
    store.session.close()


def test_an_in_doubt_write_is_resolved_by_ticks_present_after_the_restart(host):
    """A batch applied and logged by a q that died before replying (openbq-docker v10).

    The caller saw an exception, so it cannot know whether the batch landed;
    persist.q's replay brings it back with the day, and `ticks_present` says
    so -- which is what lets live-grid's spool skip it instead of writing it twice.
    """
    host.start()
    store = _store()
    store.write_ticks(_ticks(10))
    host.plant.die_after_next_upd = True
    batch = _ticks(5, start=datetime(2026, 10, 9, 15))
    with pytest.raises(Exception):
        store.write_ticks(batch)
    host.start()
    assert store.ticks_present(batch) == 5
    assert store.ticks_present(_ticks(5, start=datetime(2026, 10, 9, 16))) == 0
    assert len(store.read_ticks("AAPL", 1000)) == 15
    store.session.close()
