# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0
"""Real authenticated sockets, never an in-process simulation of .z.pg.

OPENBQ_TEST_Q selects an operator-provided licensed executable. Missing q skips
this suite; an executable that fails startup is a qualification failure.
OPENBQ_QUALIFY_LEGACY=1 demonstrates RED against an unmodified bare q server.
"""
from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import sys
import time

import pytest


@pytest.fixture
def server(tmp_path):
    q = os.environ.get("OPENBQ_TEST_Q") or shutil.which("q")
    if not q:
        pytest.skip("licensed q executable unavailable; runtime gate NOT qualified")
    token = secrets.token_urlsafe(48)
    passwords = tmp_path / "passwords"
    passwords.write_text(f"reader:{token}\n")
    passwords.chmod(0o600)
    roles = tmp_path / "roles.json"
    roles.write_text(json.dumps({"schema_version": 1, "principals": {"reader": ["query"]}}))
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = dict(os.environ, PYKX_UNLICENSED="true", QHOME=str(Path(q).parent.parent),
               KDB_AUTH_PASSWORDS_FILE=str(passwords), KDB_AUTH_ROLES_FILE=str(roles))
    command = [sys.executable, "-m", "openbq_kdb.bootstrap", "--q", q, "--port", str(port)]
    if os.environ.get("OPENBQ_QUALIFY_LEGACY") == "1":
        command = [q, "-q", "-p", f"127.0.0.1:{port}"]
    processes = []

    def start(init=None, *, wait=True):
        args = command + (["--init-script", str(init)] if init else [])
        proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, env=env)
        processes.append(proc)
        if wait:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                assert proc.poll() is None, "server exited before authenticated readiness"
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.05):
                        break
                except OSError:
                    time.sleep(0.02)
            else:
                pytest.fail("server did not become reachable within five seconds")
        return proc

    def connect(username="reader", password=None):
        import pykx as kx
        return kx.SyncQConnection("127.0.0.1", port, username=username,
                                 password=token if password is None else password,
                                 connection_timeout=0.3, timeout=0.5,
                                 reconnection_attempts=-1, no_ctx=True)

    yield start, connect, port, tmp_path, roles, passwords
    for proc in processes:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
        if proc.stdin:
            proc.stdin.close()


def ping(conn):
    import pykx as kx
    return conn(kx.SymbolAtom(".openbq.rpc"), kx.SymbolAtom("ping"),
                kx.List([]), kx.CharVector("qualification")).py()


@pytest.mark.parametrize("username,password", [("reader", "wrong"), ("", ""),
                                                ("unknown", "wrong")])
def test_native_auth_rejects_wrong_missing_and_unknown_credentials(server, username, password):
    start, connect, *_ = server
    start()
    with pytest.raises(Exception, match="(?i)access|authentication|credentials"):
        with connect(username, password):
            pass


def test_symbolic_ping_reports_only_implemented_capability(server):
    start, connect, *_ = server
    start()
    with connect() as conn:
        assert ping(conn) == {"protocol_version": 1, "ready": True,
                              "capabilities": ["ping"]}


def test_qualified_client_rejects_protocol_mismatch_without_downgrading(server):
    from openbq_kdb.credentials import ClientCredentials
    from openbq_kdb.protocol import KdbProtocolError, connect_qualified
    start, _, port, tmp, _, passwords = server
    init = tmp / "version-two.q"
    init.write_text('.openbq.rpc:{[op;args;r] `protocol_version`ready`capabilities!'
                    '(2i;1b;enlist `ping)};\n')
    start(init)
    token = passwords.read_text().strip().split(":", 1)[1]
    with pytest.raises(KdbProtocolError, match="KDB_PROTOCOL_VERSION"):
        connect_qualified("127.0.0.1", port, ClientCredentials("reader", token))


def test_qualified_client_uses_authenticated_symbolic_ping(server):
    from openbq_kdb.credentials import ClientCredentials
    from openbq_kdb.protocol import connect_qualified
    start, _, port, _, _, passwords = server
    start()
    token = passwords.read_text().strip().split(":", 1)[1]
    with connect_qualified("127.0.0.1", port, ClientCredentials("reader", token)) as conn:
        assert ping(conn)["protocol_version"] == 1


@pytest.mark.parametrize("query", ["1+1", ".openbq.rpc[`ping;();\"x\"]",
                                    "{x+1}", "escape_marker:1", "system \"pwd\""])
def test_authenticated_client_cannot_evaluate_expressions(server, query):
    start, connect, *_ = server
    start()
    with connect() as conn:
        with pytest.raises(Exception, match="KDB_ENVELOPE"):
            conn(query)
        assert ping(conn)["ready"] is True


def test_async_expression_cannot_disable_authorization(server):
    start, connect, *_ = server
    start()
    with connect() as conn:
        conn(".z.pg:value", wait=False)
        assert ping(conn)["ready"] is True
        with pytest.raises(Exception, match="KDB_ENVELOPE"):
            conn("1+1")


def test_server_does_not_listen_until_local_initialization_finishes(server):
    start, connect, port, tmp, *_ = server
    init = tmp / "slow.q"
    init.write_text('system "sleep 0.6";\n')
    proc = start(init, wait=False)
    time.sleep(0.15)
    assert proc.poll() is None
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", port), timeout=0.05)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            with connect() as conn:
                assert ping(conn)["ready"] is True
            return
        except Exception:
            time.sleep(0.05)
    pytest.fail("server never admitted authenticated ping after initialization")


def test_failed_local_bootstrap_exits_nonzero_without_listener(server):
    start, _, port, tmp, *_ = server
    init = tmp / "failed.q"
    init.write_text("missing_bootstrap_function[];\n")
    proc = start(init, wait=False)
    assert proc.wait(timeout=5) != 0
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", port), timeout=0.05)


def test_principal_present_in_password_file_but_not_policy_prevents_startup(server):
    start, _, port, _, roles, _ = server
    roles.write_text('{"schema_version":1,"principals":{"other":["query"]}}')
    proc = start(wait=False)
    assert proc.wait(timeout=5) != 0
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", port), timeout=0.05)


def test_http_and_websocket_evaluation_are_disabled_in_qualification_mode(server):
    start, _, port, _, _, passwords = server
    start()
    authorization = base64.b64encode(passwords.read_bytes().strip())
    for request in [b"GET /?1%2b1 HTTP/1.1\r\nHost: localhost\r\n\r\n",
                    b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\n\r\n1+1",
                    b"GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n"
                    b"Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n"
                    b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"]:
        for credentials in (b"", b"Authorization: Basic " + authorization + b"\r\n"):
            authenticated = request.replace(b"Host: localhost\r\n",
                                            b"Host: localhost\r\n" + credentials)
            with socket.create_connection(("127.0.0.1", port), timeout=0.5) as sock:
                sock.sendall(authenticated)
                response = sock.recv(4096)
                assert response.startswith((b"HTTP/1.1 401", b"HTTP/1.1 403"))
