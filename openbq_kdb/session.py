# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0

"""Ownership of the q process, its IPC connection, and the thread that may touch it.

q is a child of THIS container, bound to `KdbConfig.bind_host` ("127.0.0.1"
by default -- see config.py). Today every service in this stack shares the
tailscale container's network namespace, so that loopback bind is reachable
by every sibling service and by no tailnet peer; binding 0.0.0.0 would
publish an unauthenticated q -- which executes arbitrary q -- to the whole
tailnet. `bind_host` exists so that address can move without a code change
once services no longer share one namespace.

Crossing q's -w kills the process outright (verified against kdb-x 5.0), so a
dead q is treated as an ordinary state: detect, respawn, carry on.

PyKX is not merely un-serialised across threads, it is unusable from more than
one thread: four STRICTLY SEQUENTIAL write/read rounds (no concurrency at all)
survive on one thread and abort with `free(): invalid size` when each round
runs on a fresh thread. A mutex therefore cannot fix it -- serialising the
calls leaves the same heap corruption, it only hides the visible symptom of
replies crossing between threads. What the C layer wants is thread AFFINITY,
so this module owns a single-worker executor and every PyKX interaction --
spawning q, opening the connection, every query, every K-object conversion --
happens on that one thread for the process lifetime. Callers hand work in
through `run()`.
"""

import logging
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from openbq_kdb.config import KdbConfig

logger = logging.getLogger(__name__)

# poll() only proves a child hasn't exited -- not that it's listening yet --
# so a freshly spawned q gets a bounded window of connect attempts rather
# than one blind fixed sleep. Checking poll() between attempts lets a q that
# exits immediately (e.g. a missing license) fail fast instead of waiting
# out the whole budget.
_CONNECT_BUDGET_S = 5.0
_CONNECT_POLL_INTERVAL_S = 0.15

# Bounded shutdown: give q a chance to exit cleanly on SIGTERM, then force it.
_TERMINATE_WAIT_S = 3.0
_KILL_WAIT_S = 2.0

# How long a failed connect suppresses the next attempt.
#
# A failed connect must not be retried per request: the common cause is a
# missing licence, and re-paying a doomed spawn plus the 5s connect budget on
# EVERY call would make openbb-kdb's hot path unusable for a reader who simply
# has no licence -- the configuration this stack is explicitly built to
# tolerate. That is what the original permanent latch protected.
#
# But permanent was too strong, and it cost a real failure: on a cold
# `docker compose up`, live-grid's drain loop reaches q ~0.2s in, before
# openbb-api (which owns the spawn) has bound :5000. One refused connect
# latched tick recording off for the whole process lifetime, recoverable only
# by restarting live-grid by hand.
#
# An EXPIRING latch keeps both properties and needs no extra state: attempts
# are capped at one per interval no matter how hot the caller's loop is, and a
# q that appears later is picked up on the next request after the interval.
# The choice is a deadline rather than an attempt COUNT because the hazard is
# a rate (spawns per second), not a total -- a counter would either exhaust
# itself during the very startup race it exists to survive, or never expire.
#
# openbq-docker v10 (user ruling 2026-10-12, docs/v10/live-grid-kdb-port.md,
# "Tick spool and fast reconnect"): the interval is no longer a fixed 60 s but
# an exponential BACKOFF -- 1 s after the first failure, doubling per
# consecutive failure, capped at 15 s. A fixed 60 s latch made live-grid sit
# out a whole minute after a 3 s kdb restart (measured on the Mac: ~100 s of
# crypto ticks dropped). Everything the latch protected still holds: it is
# still a deadline (a rate bound, never a count), every call inside it still
# fails fast without touching q or the socket, and in steady failure attempts
# converge to one per cap. What the shorter cap costs, worst case: a reader
# with no licence pays a doomed spawn once per 15 s instead of once per 60 s
# (poll() makes that a few milliseconds), and a loopback port squatted by a
# non-q listener (macOS AirPlay on :5000) holds the owner thread for the 5 s
# connect budget once per 20 s instead of once per 65 s. The first success
# resets the backoff.
#
# KDB_RETRY_INITIAL_S and KDB_RETRY_MAX_S override the two numbers; they are
# read at each failure, so the module constants below stay the defaults (and
# the seam the tests patch). A cap of 0 disables the latch entirely.
_RETRY_INITIAL_S = 1.0
_RETRY_AFTER_S = 15.0  # the cap


def _env_seconds(name: str, default: float) -> float:
    """A non-negative float from the environment, else `default` (logged)."""
    import os

    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning("%s=%r is not a number; using %.1f", name, raw, default)
        return default
    if value < 0 or value != value:  # negative or NaN
        logger.warning("%s=%r is negative; using %.1f", name, raw, default)
        return default
    return value


def retry_delay(failures: int) -> float:
    """Seconds to hold off after `failures` consecutive failed connects (>= 1)."""
    initial = _env_seconds("KDB_RETRY_INITIAL_S", _RETRY_INITIAL_S)
    cap = _env_seconds("KDB_RETRY_MAX_S", _RETRY_AFTER_S)
    exponent = min(max(failures, 1) - 1, 30)  # 2**30 s is past any sane cap
    return min(cap, initial * (2 ** exponent))


class KdbUnavailable(Exception):
    """No usable kdb+ connection. Callers degrade to upstream pass-through."""


class KdbSession:
    """Owns at most one q process, one IPC connection, and the thread that may use them."""

    def __init__(self, config: KdbConfig, clock=time.monotonic):
        self.config = config
        # Injected for the backoff tests; production uses time.monotonic.
        self._clock = clock
        # Consecutive failed connects since the last success (drives the backoff).
        self._failures = 0
        self._conn = None
        self.endpoint: str | None = None
        self._proc: subprocess.Popen | None = None
        self._proc_log_path: str | None = None
        # Monotonic deadline before which a connect is not re-attempted. 0.0
        # means "never failed yet". See _RETRY_AFTER_S and retry_delay().
        self._retry_after = 0.0
        # One worker, forever: PyKX needs the SAME thread every time, not just
        # one thread at a time. `_owner_ident` is stamped by the worker itself
        # on its first task so `run()` can recognise a re-entrant call.
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="openbb-kdb-q")
        self._owner_ident: int | None = None
        self._closed = False

    def run(self, fn):
        """Run `fn()` on the q owner thread and return its result.

        Everything that touches PyKX must go through here, including creating
        the connection and spawning q -- affinity that starts after the
        connection was opened elsewhere is not affinity.

        A call already ON the owner thread runs inline: `connection()` and
        `is_alive()` route through `run()` themselves, so a store method that
        asks for the connection from inside its own submitted task would
        otherwise queue work behind itself on a one-worker pool and deadlock.
        """
        if threading.get_ident() == self._owner_ident:
            return fn()
        if self._closed:
            raise KdbUnavailable("kdb+ session is closed")
        return self._executor.submit(self._own, fn).result()

    def _own(self, fn):
        """Body of every submitted task: claim ownership, then do the work."""
        self._owner_ident = threading.get_ident()
        return fn()

    def _q_argv(self) -> list[str]:
        """Argument vector for the q server."""
        cfg = self.config
        return [
            f"{cfg.local_qhome}/bin/q",
            "-p", f"{cfg.bind_host}:{cfg.port}",
            "-w", str(cfg.q_workspace_mb),
            "-q",
        ]

    def _spawn(self) -> None:
        """Start q as a child process.

        stdin is a pipe we never close: q reads its console from stdin and
        exits on EOF, which in a detached container happens immediately.

        stdout/stderr go to a temp file, not a pipe: nothing ever reads a
        PIPE for a long-lived child, so once q writes enough output to fill
        the OS pipe buffer its write() blocks forever -- a live, undetectable
        hang instead of the clean death this module is built to handle. The
        file is read back only if q exits immediately, to preserve the
        startup diagnostic (e.g. a missing license).
        """
        import os
        import tempfile

        if self._proc is not None and self._proc.poll() is None:
            return
        self._cleanup_log_file()
        env = dict(os.environ, QHOME=self.config.local_qhome, QLIC=self.config.qlic)
        log_file = tempfile.NamedTemporaryFile(
            prefix="openbb-kdb-q-", suffix=".log", delete=False
        )
        self._proc_log_path = log_file.name
        try:
            self._proc = subprocess.Popen(  # noqa: S603
                self._q_argv(),
                stdin=subprocess.PIPE,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                env=env,
                cwd=self.config.local_qhome,
            )
        finally:
            log_file.close()

    def _read_proc_log(self) -> str:
        """Best-effort read of the spawned q's captured stdout/stderr."""
        if not self._proc_log_path:
            return ""
        try:
            with open(self._proc_log_path, "rb") as fh:
                return fh.read().decode(errors="replace")
        except OSError:
            return ""

    def _cleanup_log_file(self) -> None:
        """Remove the previous spawn's log file, if any. Best-effort."""
        import os

        if self._proc_log_path is None:
            return
        try:
            os.remove(self._proc_log_path)
        except OSError:
            pass
        self._proc_log_path = None

    def _connect(self, host: str | None = None):
        """Open a PyKX IPC connection (unlicensed client mode -- no licence needed).

        `connection_timeout` is load-bearing, not decorative: PyKX's default
        (`None`) never times out the socket. The chain's step 2 always probes
        loopback, and on a machine where something else already owns that
        port (observed in the wild: macOS's AirPlay receiver squats on 5000)
        a bare TCP accept with no kdb+ handshake reply hangs `_init` forever
        -- on the session's ONE owner thread, wedging every future call, kdb+
        or not, for the rest of the process. Bounding it to the same budget
        as the spawn retry loop turns that hang into an ordinary fall-through
        to the next link in the chain.
        """
        import pykx as kx

        return kx.SyncQConnection(
            host or "127.0.0.1", self.config.port, connection_timeout=_CONNECT_BUDGET_S
        )

    def _connect_with_retry(self):
        """Connect to a just-spawned q, retrying until it's listening or we give up.

        A successful _spawn() only means q hasn't exited -- it may not be
        listening yet. Retry the connect itself within a bounded total
        budget, and bail out immediately (without waiting for the budget) if
        poll() shows the process has already died.
        """
        deadline = time.monotonic() + _CONNECT_BUDGET_S
        while True:
            if self._proc is not None and self._proc.poll() is not None:
                out = self._read_proc_log()[:500]
                raise OSError(f"q exited immediately: {out}")
            try:
                return self._connect()
            except Exception:
                if self._proc is None or time.monotonic() >= deadline:
                    raise
                time.sleep(_CONNECT_POLL_INTERVAL_S)

    def is_alive(self) -> bool:
        """True when the current connection still answers."""
        return self.run(self._is_alive)

    def _is_alive(self) -> bool:
        if self._conn is None:
            return False
        try:
            self._conn("1+1")
            return True
        except Exception:
            return False

    def retry_in(self) -> float:
        """Seconds until the backoff allows the next connect attempt (0.0: now).

        Plain float arithmetic on the caller's thread, no PyKX: live-grid's
        tick spool asks this so it does not stage a replay (and fsync its
        cursor) on every 0.2 s flush while the session would refuse anyway.
        """
        return max(0.0, self._retry_after - self._clock())

    def connection(self):
        """Return a live connection, spawning or respawning as needed.

        The object handed back belongs to the owner thread: use it only from
        inside a `run()` callable, never directly from the caller's thread.
        """
        return self.run(self._connection)

    def _connection(self):
        remaining = self._retry_after - self._clock()
        if remaining > 0:
            raise KdbUnavailable(
                f"kdb+ unreachable; not retrying for another {remaining:.1f}s"
            )
        if self._conn is not None and self._is_alive():
            return self._conn
        self._conn = None
        self.endpoint = None
        spawned = False
        errors: list[str] = []

        # 1. A q the operator supplied and we can run ourselves.
        if self.config.may_spawn:
            try:
                self._spawn()
                spawned = True
                self._conn = self._connect_with_retry()
                self.endpoint = "spawned"
            except Exception as exc:  # noqa: BLE001 - fall through to the next link
                errors.append(f"spawn: {exc}")
                if spawned:
                    # We own this process; never leave a q we started running
                    # unsupervised just because we could not connect to it.
                    self._stop_proc()
                    spawned = False

        # 2. bind_host: a q another service already spawned, co-located at
        #    the configured bind address (today's default, "127.0.0.1", is
        #    right here only because this stack shares one network
        #    namespace -- see KdbConfig.bind_host).
        if self._conn is None:
            try:
                self._conn = self._connect(self.config.bind_host)
                self.endpoint = "loopback"
            except Exception as exc:  # noqa: BLE001
                errors.append(f"loopback: {exc}")

        # 3. A kdb container the operator runs themselves, same machine.
        if self._conn is None and self.config.host:
            try:
                self._conn = self._connect(self.config.host)
                self.endpoint = f"{self.config.host}:{self.config.port}"
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{self.config.host}: {exc}")

        if self._conn is None:
            self._failures += 1
            delay = retry_delay(self._failures)
            self._retry_after = self._clock() + delay
            detail = "; ".join(errors) or "no kdb+ endpoint configured"
            logger.warning(
                "kdb+ unavailable, falling back to upstream (attempt %d, retrying in %.1fs): %s",
                self._failures, delay, detail,
            )
            raise KdbUnavailable(detail)

        # A connect that succeeded clears the latch and the backoff: the next
        # failure starts again at the initial interval rather than inheriting a
        # stale deadline or a long doubled one.
        self._retry_after = 0.0
        self._failures = 0
        return self._conn

    def _stop_proc(self) -> None:
        """Terminate and reap the child process, escalating to SIGKILL. Never raises."""
        proc = self._proc
        self._proc = None
        if proc is None or proc.poll() is not None:
            self._cleanup_log_file()
            return
        try:
            proc.terminate()
            try:
                proc.wait(timeout=_TERMINATE_WAIT_S)
            except subprocess.TimeoutExpired:
                logger.warning("q did not exit within %.1fs of SIGTERM; sending SIGKILL",
                                _TERMINATE_WAIT_S)
                proc.kill()
                try:
                    proc.wait(timeout=_KILL_WAIT_S)
                except subprocess.TimeoutExpired:
                    logger.warning("q did not exit within %.1fs of SIGKILL", _KILL_WAIT_S)
        except Exception:
            logger.debug("error while stopping q process", exc_info=True)
        self._cleanup_log_file()

    def close(self) -> None:
        """Close the connection, stop a q we started, retire the owner thread.

        Never raises. The connection is closed ON the owner thread -- closing a
        PyKX handle is as much a PyKX call as querying it -- and the executor is
        shut down only afterwards, so no task can outlive the process it drives.
        """
        try:
            self.run(self._shutdown)
        except Exception:
            # The owner thread is already gone (or never started): the child
            # process still has to be reaped, and _shutdown is idempotent.
            logger.debug("kdb+ shutdown could not run on the owner thread", exc_info=True)
            try:
                self._shutdown()
            except Exception:
                logger.debug("error during kdb+ shutdown", exc_info=True)
        self._closed = True
        try:
            self._executor.shutdown(wait=True)
        except Exception:
            logger.debug("error shutting down the kdb+ owner thread", exc_info=True)
        # Thread identities are recycled once a thread is gone: a stale ident
        # would let some unrelated future thread take the re-entrant path.
        self._owner_ident = None

    def _shutdown(self) -> None:
        """Close the IPC connection and reap the child. Never raises."""
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception as exc:
            logger.debug("error closing kdb+ IPC connection: %s", exc)
        self._conn = None
        self._stop_proc()
