# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0
"""Named protocol and qualification; call only on the session's PyKX owner thread.

No context introspection, expression fallback, or policy-failure target retry.
"""
from __future__ import annotations

from openbq_kdb.credentials import ClientCredentials


class KdbAuthenticationError(RuntimeError):
    """Machine credentials were rejected; never retry another endpoint."""


class KdbAuthorizationError(RuntimeError):
    """Authenticated identity lacks permission for this operation."""


class KdbArgumentError(RuntimeError):
    """The named operation rejected its data arguments."""


class KdbProtocolError(RuntimeError):
    """The peer did not satisfy the explicitly requested named protocol."""


def validate_handshake(result):
    """Validate readiness on initial connect and every secured health check."""
    if (not isinstance(result, dict) or type(result.get("protocol_version")) is not int
            or result["protocol_version"] != 1 or result.get("ready") is not True
            or not isinstance(result.get("capabilities"), list)
            or "ping" not in result["capabilities"]
            or any(not isinstance(cap, str) for cap in result["capabilities"])
            or len(set(result["capabilities"])) != len(result["capabilities"])):
        raise KdbProtocolError("KDB_PROTOCOL_VERSION")
    return frozenset(result["capabilities"])


def connect_qualified(host: str, port: int, credentials: ClientCredentials):
    import pykx as kx

    try:
        conn = kx.SyncQConnection(host, port, username=credentials.username,
                             password=credentials.password, no_ctx=True,
                             reconnection_attempts=-1, connection_timeout=5, timeout=5)
    except Exception as exc:
        message = str(exc).lower()
        if any(word in message for word in ('access', 'authentication', 'credentials', 'password')):
            raise KdbAuthenticationError('KDB_AUTHENTICATION') from None
        raise
    try:
        result = named_call(conn, "ping").py()
        validate_handshake(result)
        return conn
    except Exception:
        conn.close()
        raise


def _argument(value):
    """Build explicit data values; Python strings are q char vectors, never symbols."""
    import pykx as kx

    if isinstance(value, str):
        return kx.CharVector(value)
    if isinstance(value, dict):
        return kx.toq({str(key): _argument(item) for key, item in value.items()})
    return value


def named_call(conn, operation: str, *args):
    """Issue only the exact symbolic dispatcher envelope, on the owner thread."""
    import pykx as kx

    try:
        return conn(kx.SymbolAtom('.openbq.rpc'), kx.SymbolAtom(operation),
                    kx.List([_argument(arg) for arg in args]), kx.CharVector('openbq-client'),
                    debug=False, skip_debug=True)
    except Exception as exc:
        code = str(exc)
        if 'KDB_AUTHORIZATION' in code:
            raise KdbAuthorizationError('KDB_AUTHORIZATION') from None
        if code in {'KDB_ARGUMENT', 'KDB_LIMIT'}:
            raise KdbArgumentError(code) from None
        if code in {'KDB_NOT_READY', 'KDB_OPERATION_UNAVAILABLE', 'KDB_ENVELOPE',
                    'KDB_PROTOCOL_VERSION'}:
            raise KdbProtocolError(code) from None
        raise


def chart_result(value):
    """Decode text and q's canonical table-of-records inside chart dictionaries."""
    record_fields = {'items', 'batches', 'ticks', 'candles', 'unavailable_buckets'}

    def decode(item, field=None):
        if isinstance(item, bytes):
            return item.decode('utf-8')
        if isinstance(item, dict):
            if field in record_fields:
                keys = list(item)
                columns = list(item.values())
                if not columns:
                    return []
                return [decode(dict(zip(keys, row))) for row in zip(*columns)]
            return {decode(key): decode(val, decode(key)) for key, val in item.items()}
        if isinstance(item, (tuple, list)):
            return [decode(val) for val in item]
        if isinstance(item, float):
            import math
            return item if math.isfinite(item) else None
        return item

    return decode(value)
