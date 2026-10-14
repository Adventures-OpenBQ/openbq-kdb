# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0

"""The reconnect backoff that replaced the fixed 60 s latch (openbq-docker v10).

On the Mac, a 3 s kdb restart cost live-grid a whole minute of recording: the
first failed connect latched every attempt off for 60 s. The latch is now a
backoff -- 1 s, doubling, capped at 15 s -- and these tests drive it with an
injected clock, so no test sleeps.
"""

import pytest

import openbq_kdb.session as session
from openbq_kdb.session import KdbSession, KdbUnavailable, retry_delay
from tests.test_session import FakeConn, cfg


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.fixture(autouse=True)
def _no_env(monkeypatch):
    monkeypatch.delenv("KDB_RETRY_INITIAL_S", raising=False)
    monkeypatch.delenv("KDB_RETRY_MAX_S", raising=False)


def _down_then(monkeypatch, up):
    """`_connect` refuses while `up()` is false, then answers with one FakeConn."""
    attempts = []
    conn = FakeConn()

    def connect(self, host=None):
        attempts.append(host)
        if not up():
            raise OSError("connection refused")
        return conn

    monkeypatch.setattr(KdbSession, "_connect", connect)
    return attempts, conn


def test_delay_starts_at_one_second_doubles_and_caps_at_fifteen():
    assert [retry_delay(n) for n in range(1, 8)] == [1.0, 2.0, 4.0, 8.0, 15.0, 15.0, 15.0]
    assert retry_delay(10_000) == 15.0  # no overflow on a very long outage


def test_env_vars_override_initial_and_cap(monkeypatch):
    monkeypatch.setenv("KDB_RETRY_INITIAL_S", "0.5")
    monkeypatch.setenv("KDB_RETRY_MAX_S", "3")
    assert [retry_delay(n) for n in range(1, 5)] == [0.5, 1.0, 2.0, 3.0]


def test_bad_env_values_fall_back_to_the_defaults(monkeypatch):
    monkeypatch.setenv("KDB_RETRY_INITIAL_S", "soon")
    monkeypatch.setenv("KDB_RETRY_MAX_S", "-4")
    assert retry_delay(1) == 1.0 and retry_delay(9) == 15.0


def test_a_zero_cap_disables_the_latch(monkeypatch):
    monkeypatch.setenv("KDB_RETRY_MAX_S", "0")
    assert retry_delay(1) == 0.0


def test_backoff_timing_with_an_injected_clock(monkeypatch):
    """Attempts land at +1, +2, +4, +8 and then every 15 s, never in between."""
    clock = Clock()
    attempts, _ = _down_then(monkeypatch, lambda: False)
    s = KdbSession(cfg(may_spawn=False, host=None), clock=clock)
    t0 = clock.now
    attempted_at = []
    # Poll every 0.25 s for 60 s, as hot as live-grid's drain loop.
    for step in range(int(60 / 0.25) + 1):
        clock.now = t0 + step * 0.25
        before = len(attempts)
        with pytest.raises(KdbUnavailable):
            s.connection()
        if len(attempts) > before:
            attempted_at.append(clock.now - t0)
    assert attempted_at == [0.0, 1.0, 3.0, 7.0, 15.0, 30.0, 45.0, 60.0]
    s.close()


def test_calls_inside_the_window_fail_fast_without_dialling(monkeypatch):
    clock = Clock()
    attempts, _ = _down_then(monkeypatch, lambda: False)
    s = KdbSession(cfg(may_spawn=False, host=None), clock=clock)
    with pytest.raises(KdbUnavailable):
        s.connection()
    for _ in range(100):
        with pytest.raises(KdbUnavailable, match="not retrying"):
            s.connection()
    assert len(attempts) == 1
    s.close()


def test_a_kdb_back_within_a_second_is_picked_up_after_one_second(monkeypatch):
    """The Mac drill: kdb restarted in ~3 s used to cost 60 s of recording."""
    clock = Clock()
    state = {"up": False}
    attempts, conn = _down_then(monkeypatch, lambda: state["up"])
    s = KdbSession(cfg(may_spawn=False, host=None), clock=clock)
    with pytest.raises(KdbUnavailable):
        s.connection()
    state["up"] = True
    clock.now += 0.9
    with pytest.raises(KdbUnavailable):
        s.connection()
    clock.now += 0.1
    assert s.connection() is conn
    s.close()


def test_success_resets_the_backoff(monkeypatch):
    clock = Clock()
    state = {"up": False}
    attempts, conn = _down_then(monkeypatch, lambda: state["up"])
    s = KdbSession(cfg(may_spawn=False, host=None), clock=clock)
    for wait in (0.0, 1.0, 2.0, 4.0):  # four failures: the next delay would be 8 s
        clock.now += wait
        with pytest.raises(KdbUnavailable):
            s.connection()
    assert s._failures == 4
    clock.now += 8.0
    state["up"] = True
    assert s.connection() is conn
    assert s._failures == 0
    # q dies again: the first retry is 1 s out, not 16 s
    conn.alive = False
    state["up"] = False
    with pytest.raises(KdbUnavailable):
        s.connection()
    assert s._retry_after - clock.now == pytest.approx(1.0)
    s.close()


def test_the_module_cap_is_still_the_seam_the_older_tests_patch(monkeypatch):
    monkeypatch.setattr(session, "_RETRY_AFTER_S", 0.0)
    assert retry_delay(1) == 0.0
    monkeypatch.setattr(session, "_RETRY_AFTER_S", 60.0)
    assert retry_delay(7) == 60.0


def test_retry_in_reports_the_remaining_window(monkeypatch):
    clock = Clock()
    _down_then(monkeypatch, lambda: False)
    s = KdbSession(cfg(may_spawn=False, host=None), clock=clock)
    assert s.retry_in() == 0.0
    with pytest.raises(KdbUnavailable):
        s.connection()
    assert s.retry_in() == pytest.approx(1.0)
    clock.now += 0.75
    assert s.retry_in() == pytest.approx(0.25)
    clock.now += 5
    assert s.retry_in() == 0.0
    s.close()
