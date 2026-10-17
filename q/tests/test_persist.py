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
    # ...and the wrapper appends to the log only while a handle is open (or one is to be
    # reopened after a failed append, Q4), trapped, before the insert.
    assert ("upd:{[t;d] if[.tlog.torn or not null .tlog.h; "
            "@[.tlog.append;enlist(`upd;t;d);.tlog.fail]]; updCore[t;d]};") in PERSIST


def test_persist_replays_on_boot_only_and_before_opening_the_log():
    replay = PERSIST.index("if[not count trades;")
    # D25: today and the two days before; D4: each file trapped, a torn tail cut back.
    assert ".tlog.replayDays:2;" in PERSIST
    assert ('{x where (x>=`$"tlog_",string .z.d-.tlog.replayDays) and not x like "*.tmp"}'
            ' asc key .tlog.dir];') in PERSIST
    assert "@[.tlog.replay;f;" in PERSIST and "n:-11!(-2;p);" in PERSIST
    # Q5: the prefix goes to a temp file renamed over the log, never rewritten in place
    assert "tmp 1: read1(p;0;n 1);" in PERSIST and " p 1: read1" not in PERSIST
    assert "n:.tlog.cut p;" in PERSIST and "-11!(n;p)}" in PERSIST
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


def test_the_model_replays_only_the_window_and_cuts_a_torn_tail(tmp_path):
    host = Host(tmp_path, today=lambda: date(2026, 10, 9))
    host.start().upd("trades", _ticks(4))
    host.kill()
    logs = Path(tmp_path) / "logs"
    today = logs / log_name(date(2026, 10, 9))
    for back in (2, 3):
        (logs / log_name(date(2026, 10, 9) - timedelta(days=back))).write_text(today.read_text())
    with open(today, "a", encoding="utf-8") as fh:
        fh.write('{"t": "trades", "rows": [["2026-10')  # torn
    q = host.start()
    assert q.replayed == ["tlog_2026.10.07", "tlog_2026.10.09"] and len(q.trades) == 8
    q.upd("trades", _ticks(1))
    host.kill()
    assert len(host.start().trades) == 9


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


# -- a real q, when one is installed (QBIN, or q on PATH; skipped otherwise) ----------

import os
import shutil
import subprocess

import pytest

QBIN = os.getenv("QBIN") or shutil.which("q") or str(Path.home() / ".kx/bin/q")
_HARNESS = """trades:([] time:`timestamp$(); sym:`symbol$(); price:`float$(); size:`float$());
upd:{[t;d] t insert d};
system"l ",getenv`PERSIST;
-1 .j.j `rows`logging`timer!(count trades; not null .tlog.h; system"t");
if[count getenv`PRE; value getenv`PRE];
if[count getenv`ADD; upd[`trades;([] time:enlist .z.p; sym:enlist`X; price:enlist 1f; size:enlist 1f)]];
exit 0
"""
real_q = pytest.mark.skipif(not Path(QBIN).is_file(), reason="no q binary (Mac gate)")


def _q(tmp_path, add=False, pre=""):
    """One boot of a real q running persist.q, its log directory moved under tmp_path."""
    logs = tmp_path / "logs"
    persist = tmp_path / "persist.q"
    # hsym, not a `: literal: the temporary path has characters a symbol literal cannot hold.
    persist.write_text(PERSIST.replace("`:/data/logs", f'(hsym`$"{logs}")')
                       .replace("mkdir -p /data/logs", f"mkdir -p {logs}"))
    (tmp_path / "harness.q").write_text(_HARNESS)
    env = dict(os.environ, PERSIST=str(persist), ADD="1" if add else "", PRE=pre)
    run = subprocess.run([QBIN, str(tmp_path / "harness.q"), "-q"], env=env, cwd=tmp_path,
                         capture_output=True, text=True, timeout=60)
    status = [json.loads(line) for line in run.stdout.splitlines() if line.startswith("{")]
    assert status, run.stdout + run.stderr
    return (status[0] if pre else status[-1]), run.stderr, status


@real_q
def test_a_torn_log_tail_is_cut_back_and_logging_stays_on(tmp_path):
    """D4 (2026-10-16): a host crash or ENOSPC leaves a partial chunk at the end of the log.
    The replay keeps the whole chunks, and the rest of persist.q (the handle, the roll timer)
    still loads, so later ticks are logged and survive the next restart."""
    _q(tmp_path, add=True)
    _q(tmp_path, add=True)
    (log,) = (tmp_path / "logs").iterdir()
    with open(log, "ab") as fh:
        fh.write(b"\x01\x00\x00\x00\x40\x00\x00\x00torn")  # a chunk header with no body
    status, err, _ = _q(tmp_path, add=True)
    assert status == {"rows": 2, "logging": True, "timer": 60000}, err
    assert "bad tail" in err
    assert _q(tmp_path)[0]["rows"] == 3


@real_q
def test_only_the_days_the_window_holds_are_replayed(tmp_path):
    """D25: the prune is not logged, so replaying every kept log would reload days live-grid
    drops a minute later. Only today and the two days before it are replayed."""
    _q(tmp_path, add=True)
    (log,) = (tmp_path / "logs").iterdir()
    today = datetime.strptime(log.name, "tlog_%Y.%m.%d").date()
    for back in (2, 3, 6):
        shutil.copy(log, log.with_name(log_name(today - timedelta(days=back))))
    assert _q(tmp_path)[0]["rows"] == 2  # today's and the day-before-yesterday's


@real_q
def test_an_append_that_fails_mid_run_is_cut_back_and_not_applied_unlogged(tmp_path):
    """Q4 (re-review 2026-10-16): ENOSPC mid-run leaves a partial chunk, and every later append
    would land after it, unreplayable. A failed append now closes the handle and signals, so
    the batch is NOT applied (live-grid's spool keeps it); the next upd cuts the log back to
    its whole chunks, reopens it and logs. Modelled by a torn tail written into the live log
    and a dead handle (999i), which makes the next append fail as a full disk would."""
    _q(tmp_path, add=True)
    pre = ("f:.tlog.file .tlog.day; f 1: read1[f],0x0100000040000000746f726e; .tlog.h:999i;"
           "r:@[upd[`trades];([] time:enlist .z.p; sym:enlist`X; price:enlist 2f; size:enlist 1f);{`failed}];"
           "upd[`trades;([] time:enlist .z.p; sym:enlist`X; price:enlist 3f; size:enlist 1f)];"
           "-1 .j.j `r`rows`torn!(r;count trades;.tlog.torn);")
    _, err, status = _q(tmp_path, pre=pre)
    assert status[1] == {"r": "failed", "rows": 2, "torn": False}, err
    # the restart replays all of it: the first run's row and the one logged after the cut
    status, err, _ = _q(tmp_path)
    assert status["rows"] == 2 and status["logging"] is True, err
    assert "bad tail" not in err


@real_q
def test_the_boot_cut_back_goes_through_a_temp_file_and_a_leftover_is_ignored(tmp_path):
    """Q5: the cut-back writes tlog_<d>.tmp and renames it over the log, so a crash mid-rewrite
    leaves the original whole. A .tmp a crash left behind is never replayed as a log."""
    assert '.tmp' in PERSIST and 'system "mv ",' in PERSIST
    _q(tmp_path, add=True)
    (log,) = (tmp_path / "logs").iterdir()
    shutil.copy(log, log.with_name(log.name + ".tmp"))  # a crash's leftover, whole chunks
    with open(log, "ab") as fh:
        fh.write(b"\x01\x00\x00\x00\x40\x00\x00\x00torn")
    status, err, _ = _q(tmp_path)
    assert status["rows"] == 1 and "bad tail" in err
    assert sorted(p.name for p in (tmp_path / "logs").iterdir()) == [log.name]
