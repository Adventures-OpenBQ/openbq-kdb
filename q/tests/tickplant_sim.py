# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0
"""A Python model of the kdb service: q/startup.q plus q/persist.q.

There is no q binary in the cloud, so the crash-recovery tests drive this
model instead. It reproduces what persist.q does, step for step, and answers
the exact IPC statements openbq-kdb sends (the same strings, matched verbatim),
so the real KdbSession, KdbStore and live-grid TickRecorder run unmodified
against it:

- boot: `mkdir -p /data/logs`; if `trades` is empty, replay every
  `tlog_<YYYY.MM.DD>` oldest first (lexical order), with the log handle still
  null so replayed updates are not logged twice; then open today's log;
- `upd`: append the update to today's log (one record per call, written
  through immediately, as q's handle write is), then insert;
- the daily roll: at a UTC date change open the new day's log and delete logs
  older than 7 days;
- `kill()` is SIGKILL: memory is lost, the files stay;
- `die_after_next_upd`: fault injection for live-grid's tick spool. The next
  `upd` is logged and applied, then the process dies before it can reply --
  the in-doubt write whose ticks must not be replayed twice.

`q/tests/test_persist.py` pins this model to the q source, so a change to
persist.q that the model does not follow fails there rather than passing here.
A real q run of the same procedure is Mac-pending (docs/v10/live-grid-kdb-port.md).
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

# The statements openbq-kdb issues (openbq_kdb/store.py), matched verbatim.
_UPD = "$[`upd in key `.; upd[`trades;incoming_ticks]; `trades insert incoming_ticks]"
_PRUNE = "{[qwcut] trades:: delete from trades where time < qwcut}"
_SPAN_PREFIX = "{[qwsym] $[0 = count select from trades where sym = qwsym;"
_READ_PREFIX = "{[qwsym;qwlim] qwlim sublist `time xdesc reverse"
_AGG_PREFIX = "{[qwsym;qwlo;qwhi;qwbucket]"
_PROBE = ("{qwin: select time, sym, price, size from trades"
          " where time within (min probe_ticks`time; max probe_ticks`time);"
          " sum (count qwin) > qwin ? probe_ticks}[]")


class QDown(ConnectionError):
    """The simulated q is not running (killed, or not yet restarted)."""


class _K:
    """Just enough of a PyKX K object: `.py()` and `.pd()`."""

    def __init__(self, value):
        self._value = value

    def py(self):
        return self._value

    def pd(self):
        return self._value


def _py(value):
    """A PyKX atom (or a plain Python value) as Python."""
    return value.py() if hasattr(value, "py") else value


def log_name(day: date) -> str:
    """persist.q's `.tlog.file`: `tlog_` and q's `string .z.d` (YYYY.MM.DD)."""
    return "tlog_" + day.strftime("%Y.%m.%d")


class TickPlant:
    """One q process running startup.q + persist.q, with its /data directory."""

    def __init__(self, data_dir, today=None):
        self.logs = Path(data_dir) / "logs"
        self._today = today or (lambda: datetime.utcnow().date())
        self.alive = True
        self.die_after_next_upd = False
        self.replayed: list[str] = []
        self.trades = self._empty()
        self._handle = None  # .tlog.h: null until after the replay
        os.makedirs(self.logs, exist_ok=True)  # system"mkdir -p /data/logs"
        if not len(self.trades):  # if[not count trades; ... -11! each asc key .tlog.dir]
            for name in sorted(os.listdir(self.logs)):
                self.replayed.append(name)
                self._replay(self.logs / name)
        self.day = self._today()
        self._open(self.day)

    # -- persist.q --------------------------------------------------------------
    @staticmethod
    def _empty():
        return pd.DataFrame({
            "time": pd.Series(dtype="datetime64[ns]"),
            "sym": pd.Series(dtype="object"),
            "price": pd.Series(dtype="float64"),
            "size": pd.Series(dtype="float64"),
        })

    def _replay(self, path: Path) -> None:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                record = json.loads(line)
                frame = pd.DataFrame(record["rows"], columns=["time", "sym", "price", "size"])
                # ISO8601: isoformat() omits a zero microsecond part, so one
                # log record can mix "...:00" and "...:00.040000" stamps.
                frame["time"] = pd.to_datetime(frame["time"], format="ISO8601")
                self.upd(record["t"], frame)

    def _open(self, day: date) -> None:
        path = self.logs / log_name(day)
        if not path.exists():  # if[()~key f; f set ()]
            path.write_text("", encoding="utf-8")
        self._handle = path
        self.day = day

    def upd(self, table: str, frame: pd.DataFrame) -> None:
        if self._handle is not None:  # if[not null .tlog.h; .tlog.h enlist(`upd;t;d)]
            rows = [[t.isoformat(), s, float(p), float(z)] for t, s, p, z in
                    zip(pd.to_datetime(frame["time"]), frame["sym"], frame["price"], frame["size"])]
            with open(self._handle, "a", encoding="utf-8") as fh:
                fh.write(json.dumps({"t": table, "rows": rows}) + "\n")
        if table == "trades":  # updCore: insert (and publish)
            frame = frame[["time", "sym", "price", "size"]].copy()
            frame["time"] = pd.to_datetime(frame["time"]).astype("datetime64[ns]")
            self.trades = frame if not len(self.trades) else pd.concat(
                [self.trades, frame], ignore_index=True)

    def roll(self) -> None:
        """`.tlog.roll`, run by .z.ts every 60 s."""
        today = self._today()
        if today > self.day:
            self._open(today)
            cutoff = log_name(today - timedelta(days=7))
            for name in sorted(os.listdir(self.logs)):
                if name < cutoff:
                    os.remove(self.logs / name)

    def kill(self) -> None:
        """SIGKILL: the process and its memory are gone; /data stays."""
        self.alive = False
        self.trades = None
        self._handle = None

    # -- IPC ---------------------------------------------------------------------
    def connect(self) -> "SimConn":
        if not self.alive:
            raise QDown("connection refused")
        return SimConn(self)


class SimConn:
    """A PyKX SyncQConnection to a TickPlant, for the statements openbq-kdb sends."""

    def __init__(self, plant: TickPlant):
        self.plant = plant
        self.globals: dict = {}

    def _check(self):
        if not self.plant.alive:
            raise QDown("the q at the other end has gone away")

    def __setitem__(self, name, value):
        self._check()
        self.globals[name] = value

    def close(self):
        pass

    def __call__(self, query: str, *args):
        self._check()
        p = self.plant
        if query == "1+1":
            return _K(2)
        if query.startswith("if[not `cov in key `.cache;"):
            return _K(None)  # _INIT_SCHEMA: trades already exists on startup.q's q
        if query == "0#trades":
            return _K(p._empty())
        if query == _UPD:
            p.upd("trades", self.globals["incoming_ticks"])
            if p.die_after_next_upd:  # applied and logged; the reply never leaves
                p.die_after_next_upd = False
                p.kill()
                raise QDown("the q at the other end has gone away")
            return _K(None)
        if query == "delete incoming_ticks from `.":
            self.globals.pop("incoming_ticks", None)
            return _K(None)
        if query == _PROBE:  # openbq-kdb's ticks_present: rows of the probe found in trades
            probe = self.globals["probe_ticks"][["time", "sym", "price", "size"]].copy()
            probe["time"] = pd.to_datetime(probe["time"]).astype("datetime64[ns]")
            lo, hi = probe["time"].min(), probe["time"].max()
            held = p.trades[(p.trades["time"] >= lo) & (p.trades["time"] <= hi)]
            keys = set(map(tuple, held[["time", "sym", "price", "size"]].itertuples(index=False)))
            return _K(sum(1 for row in probe.itertuples(index=False) if tuple(row) in keys))
        if query == "delete probe_ticks from `.":
            self.globals.pop("probe_ticks", None)
            return _K(None)
        if query == _PRUNE:
            cut = pd.Timestamp(_py(args[0]))
            p.trades = p.trades[p.trades["time"] >= cut].reset_index(drop=True)
            return _K(None)
        if query in (".Q.gc[]",):
            return _K(0)
        if query == "count trades":
            return _K(len(p.trades))
        sym = _py(args[0]) if args else None
        rows = p.trades[p.trades["sym"] == sym] if args else p.trades
        if query.startswith(_SPAN_PREFIX):
            if not len(rows):
                return _K(pd.DataFrame({"lo": [], "hi": []}))
            return _K(pd.DataFrame({"lo": [rows["time"].min()], "hi": [rows["time"].max()]}))
        if query.startswith(_READ_PREFIX):
            limit = int(_py(args[1]))
            ordered = rows.iloc[::-1].sort_values("time", ascending=False, kind="stable")
            return _K(ordered[["time", "price", "size"]].head(limit).reset_index(drop=True))
        if query.startswith(_AGG_PREFIX):
            lo, hi, width = pd.Timestamp(_py(args[1])), pd.Timestamp(_py(args[2])), int(_py(args[3]))
            sel = rows[(rows["time"] >= lo) & (rows["time"] <= hi)].sort_values("time", kind="stable")
            return _K(aggregate(sel, width))
        raise NotImplementedError(f"the simulated q does not model: {query[:80]!r}")


def aggregate(ticks: pd.DataFrame, width_ns: int) -> pd.DataFrame:
    """startup.q/openbq-kdb's `xbar` OHLCV: time-sorted ticks, bucket = width xbar time."""
    if not len(ticks):
        return pd.DataFrame(columns=["t", "open", "high", "low", "close", "volume", "vwap"])
    ns = pd.to_datetime(ticks["time"]).astype("datetime64[ns]").astype("int64")
    frame = ticks.assign(t=pd.to_datetime((ns // width_ns) * width_ns), pv=ticks["price"] * ticks["size"])
    grouped = frame.groupby("t", sort=True)
    out = pd.DataFrame({
        "open": grouped["price"].first(), "high": grouped["price"].max(),
        "low": grouped["price"].min(), "close": grouped["price"].last(),
        "volume": grouped["size"].sum(),
    })
    out["vwap"] = grouped["pv"].sum() / out["volume"].where(out["volume"] != 0)
    return out.reset_index()


class Host:
    """The docker host: one /data volume that outlives every q process on it."""

    def __init__(self, data_dir, today=None):
        self.data_dir = data_dir
        self.today = today
        self.plant: TickPlant | None = None

    def start(self) -> TickPlant:
        self.plant = TickPlant(self.data_dir, today=self.today)
        return self.plant

    def kill(self) -> None:
        if self.plant is not None:
            self.plant.kill()

    def connect(self, *_args, **_kwargs) -> SimConn:
        if self.plant is None:
            raise QDown("connection refused")
        return self.plant.connect()
