# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0
"""Qualification handshake. Call only from the session's PyKX owner thread.

No context introspection, expression fallback, or alternate-endpoint retry.
Existing v10 session/store migration is a later task.
"""
from __future__ import annotations

from openbq_kdb.credentials import ClientCredentials


class KdbProtocolError(RuntimeError):
    """The peer did not satisfy the explicitly requested named protocol."""


def connect_qualified(host: str, port: int, credentials: ClientCredentials):
    import pykx as kx

    conn = kx.SyncQConnection(host, port, username=credentials.username,
                             password=credentials.password, no_ctx=True,
                             reconnection_attempts=-1, connection_timeout=5, timeout=5)
    try:
        result = conn(kx.SymbolAtom(".openbq.rpc"), kx.SymbolAtom("ping"),
                      kx.List([]), kx.CharVector("protocol-handshake"),
                      debug=False, skip_debug=True).py()
        if (not isinstance(result, dict) or type(result.get("protocol_version")) is not int
                or result["protocol_version"] != 1 or result.get("ready") is not True
                or result.get("capabilities") != ["ping"]):
            raise KdbProtocolError("KDB_PROTOCOL_VERSION")
        return conn
    except Exception:
        conn.close()
        raise
