# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0

"""Session lifecycle: spawn, reuse, notice death, respawn, give up cleanly."""

import subprocess
import threading
import time

import pytest

import openbq_kdb.session as session
from openbq_kdb.config import KdbConfig
from openbq_kdb.session import KdbSession, KdbUnavailable


def cfg(**kw) -> KdbConfig:
    base = dict(host="127.0.0.1", port=5000, bind_host="127.0.0.1", may_spawn=True,
                memory_mb=1024, watermark=0.75, upstream="eodhd", qhome="/opt/kx",
                qlic="/opt/kx", local_qhome="/opt/kx")
    base.update(kw)
    return KdbConfig(**base)


class FakeConn:
    """Stands in for pykx.SyncQConnection."""

    def __init__(self, alive=True):
        self.alive = alive
        self.calls = []
        self.threads = []

    def __call__(self, query, *args):
        if not self.alive:
            raise RuntimeError("Attempted to use a closed IPC connection")
        self.calls.append(query)
        self.threads.append(threading.get_ident())
        return 2

    def close(self):
        self.alive = False


class FakeProc:
    """Stands in for subprocess.Popen -- records terminate/wait/kill calls."""

    def __init__(self, alive=True):
        self.alive = alive
        self.terminated = False
        self.killed = False
        self.wait_calls = 0

    def poll(self):
        return None if self.alive else 0

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True
        self.alive = False

    def wait(self, timeout=None):
        self.wait_calls += 1
        if self.alive:
            raise subprocess.TimeoutExpired(cmd="q", timeout=timeout)
        return 0


def test_connects_and_reuses_one_connection(monkeypatch):
    made = []
    monkeypatch.setattr(KdbSession, "_spawn", lambda self: None)
    monkeypatch.setattr(KdbSession, "_connect", lambda self: made.append(1) or FakeConn())
    s = KdbSession(cfg())
    first, second = s.connection(), s.connection()
    assert first is second
    assert len(made) == 1


def test_health_check_detects_a_dead_q(monkeypatch):
    monkeypatch.setattr(KdbSession, "_spawn", lambda self: None)
    monkeypatch.setattr(KdbSession, "_connect", lambda self: FakeConn())
    s = KdbSession(cfg())
    conn = s.connection()
    assert s.is_alive() is True
    conn.alive = False
    assert s.is_alive() is False


def test_respawns_after_death(monkeypatch):
    """q dying is a normal state, not an exception -- the next call gets a new one."""
    conns = [FakeConn(), FakeConn()]
    spawns = []
    monkeypatch.setattr(KdbSession, "_spawn", lambda self: spawns.append(1))
    monkeypatch.setattr(KdbSession, "_connect", lambda self: conns.pop(0))
    s = KdbSession(cfg())
    first = s.connection()
    first.alive = False
    second = s.connection()
    assert second is not first
    assert len(spawns) == 2


def test_external_server_is_never_spawned(monkeypatch):
    spawns = []
    monkeypatch.setattr(KdbSession, "_spawn", lambda self: spawns.append(1))
    monkeypatch.setattr(KdbSession, "_connect", lambda self, host=None: FakeConn())
    KdbSession(cfg(host="kdb.internal", may_spawn=False)).connection()
    assert spawns == []


def test_connect_failure_raises_kdb_unavailable(monkeypatch):
    def boom(self):
        raise OSError("connection refused")

    monkeypatch.setattr(KdbSession, "_spawn", lambda self: None)
    monkeypatch.setattr(KdbSession, "_connect", boom)
    with pytest.raises(KdbUnavailable):
        KdbSession(cfg()).connection()


def test_repeated_failure_is_not_retried_every_call(monkeypatch):
    """A missing license must not cost a spawn attempt on every single request."""
    attempts = []

    def boom(self):
        attempts.append(1)
        raise OSError("connection refused")

    monkeypatch.setattr(KdbSession, "_spawn", lambda self: None)
    monkeypatch.setattr(KdbSession, "_connect", boom)
    s = KdbSession(cfg())
    for _ in range(5):
        with pytest.raises(KdbUnavailable):
            s.connection()
    assert len(attempts) == 1


def test_a_session_that_failed_once_can_connect_later(monkeypatch):
    """The give-up latch must EXPIRE, not last for the process lifetime.

    On a cold `docker compose up`, live-grid's drain loop reaches q about
    0.2s in -- before openbb-api, which owns the spawn, has bound :5000. That
    single refused connect used to latch tick recording (and the tick half of
    the chart) off permanently, recoverable only by restarting live-grid by
    hand after something else warmed q. Within the interval the latch still
    holds, so a hot caller never re-pays a doomed spawn; past it, a q that has
    since come up is picked up on its own.
    """
    attempts = []
    conn = FakeConn()

    def refuse_once(self):
        attempts.append(1)
        if len(attempts) == 1:
            raise OSError("connection refused")
        return conn

    monkeypatch.setattr(KdbSession, "_spawn", lambda self: None)
    monkeypatch.setattr(KdbSession, "_connect", refuse_once)
    monkeypatch.setattr(session, "_RETRY_AFTER_S", 0.05)

    s = KdbSession(cfg())
    with pytest.raises(KdbUnavailable):
        s.connection()
    with pytest.raises(KdbUnavailable):  # inside the interval: still suppressed
        s.connection()
    assert len(attempts) == 1, "the latch must suppress a retry inside the interval"

    time.sleep(0.06)
    assert s.connection() is conn, "q came up later; the session must find it"
    assert len(attempts) == 2
    s.close()


def test_q_command_binds_loopback_and_sets_workspace():
    """The bind is load-bearing: 0.0.0.0 would expose an unauthenticated q."""
    argv = KdbSession(cfg(memory_mb=1024))._q_argv()
    assert argv[1] == "-p"
    assert argv[2] == "127.0.0.1:5000"
    assert "0.0.0.0" not in " ".join(argv)
    assert argv[argv.index("-w") + 1] == "1280"  # 1024 * 1.25


def test_q_command_bind_follows_bind_host():
    # KDB_BIND_HOST (config.py) is the prerequisite for the bridge-network
    # migration -- the spawn bind must move with it, with no code change.
    argv = KdbSession(cfg(bind_host="kdb"))._q_argv()
    assert argv[2] == "kdb:5000"


def test_orphaned_process_terminated_on_connect_failure(monkeypatch):
    """A q we spawned must never keep running unsupervised after connect fails."""
    fake_proc = FakeProc(alive=True)

    def fake_spawn(self):
        self._proc = fake_proc

    def fake_connect(self):
        raise OSError("connection refused")

    # Keep the retry loop's total budget tiny so the test doesn't burn real time.
    monkeypatch.setattr(session, "_CONNECT_BUDGET_S", 0.05)
    monkeypatch.setattr(session, "_CONNECT_POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr(KdbSession, "_spawn", fake_spawn)
    monkeypatch.setattr(KdbSession, "_connect", fake_connect)

    s = KdbSession(cfg())
    with pytest.raises(KdbUnavailable):
        s.connection()

    assert fake_proc.terminated is True
    assert s._proc is None


def test_close_terminates_and_reaps_process_falling_back_to_kill():
    """close() must wait() after terminate(), and escalate to kill() if it hangs."""
    fake_proc = FakeProc(alive=True)
    s = KdbSession(cfg())
    s._proc = fake_proc

    s.close()

    assert fake_proc.terminated is True
    assert fake_proc.wait_calls >= 1
    assert fake_proc.killed is True
    assert s._proc is None


def test_spawn_env_uses_qlic_not_qhome(monkeypatch, tmp_path):
    """The mounted licence dir must reach q's environment, not get overwritten
    by qhome -- that was the bug that made the bring-your-own-licence mount
    inert."""
    captured = {}

    def fake_popen(argv, stdin=None, stdout=None, stderr=None, env=None, cwd=None):
        captured["env"] = env
        return FakeProc(alive=True)

    monkeypatch.setattr(session.subprocess, "Popen", fake_popen)
    s = KdbSession(cfg(qhome="/opt/kx", qlic="/opt/kx-license"))
    s._spawn()

    assert captured["env"]["QHOME"] == "/opt/kx"
    assert captured["env"]["QLIC"] == "/opt/kx-license"
    assert captured["env"]["QLIC"] != captured["env"]["QHOME"]


def test_every_q_call_lands_on_one_thread_that_is_never_the_caller(monkeypatch):
    """PyKX is unusable from more than one thread: four strictly sequential
    write/read rounds abort with `free(): invalid size` when each round runs on
    a fresh thread, with no concurrency at all. Affinity, not a mutex, is the
    fix -- so every call must land on the SAME thread, whoever asked."""
    conn = FakeConn()
    monkeypatch.setattr(KdbSession, "_spawn", lambda self: None)
    monkeypatch.setattr(KdbSession, "_connect", lambda self: conn)
    s = KdbSession(cfg())

    idents = []

    def caller():
        idents.append(s.run(lambda: s._conn("1+1")))

    s.connection()
    for _ in range(4):  # a fresh thread each round -- the shape that aborted
        t = threading.Thread(target=caller)
        t.start()
        t.join()

    assert len(set(conn.threads)) == 1
    assert threading.get_ident() not in conn.threads
    s.close()


def test_the_connection_is_created_on_the_owner_thread_too(monkeypatch):
    """Affinity that begins after the handle was opened elsewhere is not
    affinity: the spawn and the connect must run on the owner thread."""
    seen = {}
    monkeypatch.setattr(KdbSession, "_spawn",
                        lambda self: seen.__setitem__("spawn", threading.get_ident()))
    monkeypatch.setattr(KdbSession, "_connect",
                        lambda self: seen.__setitem__("connect", threading.get_ident())
                        or FakeConn())
    s = KdbSession(cfg())
    conn = s.connection()
    s.run(lambda: conn("1+1"))

    assert seen["spawn"] == seen["connect"] == conn.threads[0]
    assert seen["connect"] != threading.get_ident()
    s.close()


def test_run_from_inside_the_owner_thread_does_not_deadlock(monkeypatch):
    """One worker means re-entrant work queued behind itself would wait
    forever -- `connection()` and `is_alive()` both route through run()."""
    monkeypatch.setattr(KdbSession, "_spawn", lambda self: None)
    monkeypatch.setattr(KdbSession, "_connect", lambda self: FakeConn())
    s = KdbSession(cfg())

    def nested():
        # connection() -> run() from inside a run() task.
        return s.run(lambda: s.connection() is not None) and s.is_alive()

    assert s.run(nested) is True
    s.close()


def test_close_retires_the_owner_thread_and_never_raises(monkeypatch):
    monkeypatch.setattr(KdbSession, "_spawn", lambda self: None)
    monkeypatch.setattr(KdbSession, "_connect", lambda self: FakeConn())
    s = KdbSession(cfg())
    conn = s.connection()

    s.close()
    s.close()  # idempotent: a second close must not raise either

    assert conn.alive is False
    with pytest.raises(KdbUnavailable):  # the owner thread is gone; no more q
        s.run(lambda: None)


def test_close_does_not_kill_a_process_that_exits_on_terminate():
    """A clean SIGTERM exit must not escalate to SIGKILL."""

    class ExitsOnTerminate(FakeProc):
        def terminate(self):
            super().terminate()
            self.alive = False

    fake_proc = ExitsOnTerminate(alive=True)
    s = KdbSession(cfg())
    s._proc = fake_proc

    s.close()

    assert fake_proc.terminated is True
    assert fake_proc.wait_calls == 1
    assert fake_proc.killed is False


def test_chain_spawns_first_when_a_local_q_is_available(monkeypatch):
    calls = []
    monkeypatch.setattr(KdbSession, "_spawn", lambda self: calls.append("spawn"))
    monkeypatch.setattr(KdbSession, "_connect_with_retry", lambda self: FakeConn())
    monkeypatch.setattr(KdbSession, "_connect", lambda self, host=None: FakeConn())
    s = KdbSession(cfg(may_spawn=True, host="elsewhere"))
    s.connection()
    assert calls == ["spawn"]
    assert s.endpoint == "spawned"


def test_chain_tries_loopback_before_the_external_host(monkeypatch):
    """live-grid picks up the q that openbb-api spawned in the shared namespace."""
    tried = []

    def fake_connect(self, host=None):
        tried.append(host)
        return FakeConn()

    monkeypatch.setattr(KdbSession, "_connect", fake_connect)
    s = KdbSession(cfg(may_spawn=False, host="host.docker.internal"))
    s.connection()
    assert tried == ["127.0.0.1"]
    assert s.endpoint == "loopback"


def test_link_two_follows_bind_host_off_loopback(monkeypatch):
    # Once services no longer share one network namespace, link 2 has to try
    # wherever the spawning sibling's KDB_BIND_HOST actually put q -- not a
    # hardcoded "127.0.0.1" that stops being reachable.
    tried = []

    def fake_connect(self, host=None):
        tried.append(host)
        return FakeConn()

    monkeypatch.setattr(KdbSession, "_connect", fake_connect)
    s = KdbSession(cfg(may_spawn=False, bind_host="kdb", host="host.docker.internal"))
    s.connection()
    assert tried == ["kdb"]


def test_chain_falls_through_to_the_external_host(monkeypatch):
    tried = []

    def fake_connect(self, host=None):
        tried.append(host)
        if host == "127.0.0.1":
            raise OSError("connection refused")
        return FakeConn()

    monkeypatch.setattr(KdbSession, "_connect", fake_connect)
    s = KdbSession(cfg(may_spawn=False, host="host.docker.internal"))
    s.connection()
    assert tried == ["127.0.0.1", "host.docker.internal"]
    assert s.endpoint == "host.docker.internal:5000"


def test_a_failed_spawn_still_falls_through_to_the_external_host(monkeypatch):
    """A broken local q must not cost the operator their external one."""
    tried = []

    def boom(self):
        raise OSError("q exited immediately")

    def fake_connect(self, host=None):
        tried.append(host)
        if host == "127.0.0.1":
            raise OSError("connection refused")
        return FakeConn()

    monkeypatch.setattr(KdbSession, "_spawn", boom)
    monkeypatch.setattr(KdbSession, "_connect", fake_connect)
    s = KdbSession(cfg(may_spawn=True, host="host.docker.internal"))
    s.connection()
    assert s.endpoint == "host.docker.internal:5000"


def test_no_local_q_and_no_host_raises(monkeypatch):
    def fake_connect(self, host=None):
        raise OSError("connection refused")

    monkeypatch.setattr(KdbSession, "_connect", fake_connect)
    s = KdbSession(cfg(may_spawn=False, host=None))
    with pytest.raises(KdbUnavailable):
        s.connection()


def test_a_failed_spawn_leaves_no_orphan_process(monkeypatch):
    stopped = []
    monkeypatch.setattr(KdbSession, "_spawn", lambda self: None)
    monkeypatch.setattr(KdbSession, "_stop_proc", lambda self: stopped.append(1))
    monkeypatch.setattr(
        KdbSession, "_connect_with_retry",
        lambda self: (_ for _ in ()).throw(OSError("no listener")),
    )
    monkeypatch.setattr(
        KdbSession, "_connect",
        lambda self, host=None: (_ for _ in ()).throw(OSError("refused")),
    )
    s = KdbSession(cfg(may_spawn=True, host=None))
    with pytest.raises(KdbUnavailable):
        s.connection()
    assert stopped, "a q we started must never be left running unsupervised"
