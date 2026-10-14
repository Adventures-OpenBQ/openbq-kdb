# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0
"""Crash recovery in the kdb service: persist.q's tick log and its -11! replay.

Two halves. The static half pins the q source to the behaviour the
simulation models (tickplant_sim.py), so the model cannot drift from the
script. The simulation half kills and restarts the modelled q and checks that
the day comes back. A real q run is Mac-pending.
"""
import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from tickplant_sim import Host, QDown, log_name

KDB_WS = Path(__file__).resolve().parents[1]
PERSIST = (KDB_WS / "persist.q").read_text()
STARTUP = (KDB_WS / "startup.q").read_text()


def _ticks(n, start=datetime(2026, 10, 9, 14, 30), sym="AAPL"):
    return pd.DataFrame({
        "time": [start + timedelta(seconds=i) for i in range(n)],
        "sym": [sym] * n,
        "price": [100.0 + i for i in range(n)],
        "size": [1.0] * n,
    })


# -- the q source ----------------------------------------------------------------

def test_startup_loads_persist_from_KDB_PERSIST_Q_or_the_donor_default():
    assert 'kdbPersistQ:$[count kdbPersistQ:getenv`KDB_PERSIST_Q; kdbPersistQ; "/data/persist.q"];' in STARTUP
    assert STARTUP.rstrip().endswith('system"l ",kdbPersistQ;')
    # persist.q wraps upd, so it must load AFTER startup.q defines upd.
    assert STARTUP.index("upd:{[t;d]") < STARTUP.index('system"l ",kdbPersistQ;')
    # The donor's warning: a "/" alone at the start of a line opens a block comment
    # that swallows the rest of the file. (Indented, inside a function, it is a comment.)
    assert not any(line.rstrip() == "/" for line in STARTUP.splitlines())
    assert not any(line.rstrip() == "/" for line in PERSIST.splitlines())


def test_persist_logs_every_upd_under_data_logs_once():
    assert 'system"mkdir -p /data/logs";' in PERSIST
    assert ".tlog.dir:`:/data/logs;" in PERSIST
    assert '.tlog.file:{` sv .tlog.dir,`$"tlog_",string x};' in PERSIST
    # upd is wrapped exactly once (re-loading persist.q must not double-log)...
    assert "if[not `updCore in key `.; updCore:upd];" in PERSIST
    # ...and the wrapper appends to the log only while a handle is open.
    assert "upd:{[t;d] if[not null .tlog.h; .tlog.h enlist(`upd;t;d)]; updCore[t;d]};" in PERSIST


def test_persist_replays_on_boot_only_and_before_opening_the_log():
    replay = PERSIST.index("if[not count trades;")
    assert "-11! ` sv .tlog.dir,f} each asc key .tlog.dir" in PERSIST
    # the handle is still null during the replay, so replayed upds are not re-logged
    assert PERSIST.index(".tlog.h:0N;") < replay < PERSIST.index(".tlog.open .z.d;")


def test_persist_rolls_daily_and_prunes_after_seven_days():
    assert "if[.z.d>.tlog.day;" in PERSIST
    assert 'old:{x where x<`$"tlog_",string .z.d-7} key .tlog.dir;' in PERSIST
    assert 'system"t 60000";' in PERSIST
    assert ".z.exit:{@[hclose;.tlog.h;::]};" in PERSIST


# -- the simulation --------------------------------------------------------------

def test_a_killed_q_comes_back_with_the_day(tmp_path):
    host = Host(tmp_path, today=lambda: date(2026, 10, 9))
    q = host.start()
    q.upd("trades", _ticks(300))
    q.upd("trades", _ticks(200, start=datetime(2026, 10, 9, 15, 0)))
    assert len(q.trades) == 500
    host.kill()
    try:
        host.connect()
        raise AssertionError("a killed q must refuse connections")
    except QDown:
        pass
    q2 = host.start()
    assert q2.replayed == [log_name(date(2026, 10, 9))]
    assert len(q2.trades) == 500
    assert list(q2.trades["price"]) == list(_ticks(300)["price"]) + list(_ticks(200)["price"])
    # Replayed updates were not logged a second time: one more restart, same count.
    host.kill()
    assert len(host.start().trades) == 500


def test_ticks_after_the_restart_are_logged_and_survive_the_next_one(tmp_path):
    host = Host(tmp_path, today=lambda: date(2026, 10, 9))
    host.start().upd("trades", _ticks(10))
    host.kill()
    host.start().upd("trades", _ticks(5, start=datetime(2026, 10, 9, 16)))
    host.kill()
    assert len(host.start().trades) == 15


def test_replay_covers_every_unpruned_day_oldest_first(tmp_path):
    today = {"d": date(2026, 10, 8)}
    host = Host(tmp_path, today=lambda: today["d"])
    q = host.start()
    q.upd("trades", _ticks(3, start=datetime(2026, 10, 8, 20)))
    today["d"] = date(2026, 10, 9)
    q.roll()
    q.upd("trades", _ticks(4, start=datetime(2026, 10, 9, 1)))
    host.kill()
    q2 = host.start()
    assert q2.replayed == ["tlog_2026.10.08", "tlog_2026.10.09"]
    assert list(q2.trades["time"]) == sorted(q2.trades["time"])
    assert len(q2.trades) == 7


def test_the_roll_deletes_logs_older_than_seven_days(tmp_path):
    today = {"d": date(2026, 10, 1)}
    host = Host(tmp_path, today=lambda: today["d"])
    q = host.start()
    for offset in range(1, 10):
        today["d"] = date(2026, 10, 1) + timedelta(days=offset)
        q.roll()
    names = sorted(p.name for p in (Path(tmp_path) / "logs").iterdir())
    assert names[0] == log_name(today["d"] - timedelta(days=7))
    assert names[-1] == log_name(today["d"])


def test_the_log_is_one_record_per_upd(tmp_path):
    host = Host(tmp_path, today=lambda: date(2026, 10, 9))
    q = host.start()
    q.upd("trades", _ticks(3))
    q.upd("trades", _ticks(2))
    lines = (Path(tmp_path) / "logs" / "tlog_2026.10.09").read_text().splitlines()
    assert [len(json.loads(line)["rows"]) for line in lines] == [3, 2]
    assert re.fullmatch(r"tlog_\d{4}\.\d{2}\.\d{2}", log_name(date(2026, 10, 9)))
