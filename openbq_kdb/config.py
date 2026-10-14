# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0

"""Connection and cache configuration.

Precedence for most settings: OpenBB credential > environment variable >
default.

Two settings are deliberately outside that chain and take NO credential:

- ``qhome`` is read from ``QHOME`` (else ``/opt/kx``) exactly once per process,
  before ``import pykx`` can rewrite the variable -- see ``_qhome_once``. A
  per-request credential could not be honoured without reintroducing the
  ambiguity that resolution exists to remove.
- ``qlic`` is read from ``QLIC``, falling back to ``qhome``. It points at the
  operator's bring-your-own licence mount, which is a property of the container
  rather than of any caller's credentials.
"""

import os
from dataclasses import dataclass

_DEFAULTS = {
    "port": 5000,
    "bind_host": "127.0.0.1",
    "memory_mb": 8192,
    "watermark": 0.75,
    "upstream": "eodhd",
    "qhome": "/opt/kx",
    "local_qhome": "/opt/kx",
}

# q is given headroom above the cache budget. Crossing -w kills the process
# outright (no catchable 'wsfull), so -w is containment that protects the rest
# of the container -- the real budget is enforced by eviction well below it.
_WORKSPACE_HEADROOM = 1.25

# `import pykx` REWRITES os.environ["QHOME"] to PyKX's own bundled q directory.
# So the same call, made twice in one process, can resolve two different qhomes
# depending on whether PyKX had loaded in between -- and the second answer is
# the wrong one: it points at PyKX's lib, not the operator's kdb-x install.
# The first resolution therefore wins for the lifetime of the process.
#
# `_qhome` holds that resolution, but a bare `None` cannot double as both "not
# decided yet" and "decided: unset" -- so `_qhome_decided` tracks the decision
# itself, separately from its (possibly absent) result.
_qhome: str | None = None
_qhome_decided = False


def _qhome_once() -> str | None:
    """QHOME as it read BEFORE PyKX could rewrite it, or None if unset.

    Decided once per process; callers apply whatever default suits their
    field.
    """
    global _qhome, _qhome_decided  # noqa: PLW0603
    if not _qhome_decided:
        _qhome = os.getenv("QHOME")
        _qhome_decided = True
    return _qhome


def has_local_q(local_qhome: str) -> bool:
    """True when `local_qhome` holds a q we could actually execute.

    The operator mounts their own q here; this repo ships none, because the
    licence does not permit redistributing KX's binary.
    """
    candidate = os.path.join(local_qhome, "bin", "q")
    return os.path.isfile(candidate) and os.access(candidate, os.X_OK)


@dataclass(frozen=True)
class KdbConfig:
    """Resolved kdb+ settings.

    ``host`` and ``bind_host`` name different things: ``host`` is the
    OPERATOR's own external kdb+ container (link 3 of the connection chain,
    unset by default -- nothing to fall back to). ``bind_host`` is the
    address THIS session spawns q on, and the address a non-spawning sibling
    tries first to find a co-located q (link 1's bind, link 2's connect) --
    "127.0.0.1" while every service shares one network namespace, something
    else once they don't.
    """

    host: str | None
    port: int
    bind_host: str
    may_spawn: bool
    memory_mb: int
    watermark: float
    upstream: str
    qhome: str
    qlic: str
    local_qhome: str

    @property
    def q_workspace_mb(self) -> int:
        """The `-w` value: the cache budget plus containment headroom."""
        return int(self.memory_mb * _WORKSPACE_HEADROOM)


def _pick(key: str, env: str, credentials: dict | None):
    """Resolve one setting: credential > env var > absent (``None``).

    Presence must be judged by containment/identity, not truthiness --
    otherwise a legitimately falsy credential (``False``, ``0``, ``0.0``)
    reads as "not set" and silently falls through to the env var or
    default, which is exactly the bug this function exists to avoid.
    So a credential counts as present if the key exists and its value is
    not ``None``, regardless of whether that value is falsy.

    Environment variables are different: they're always strings, and a
    shell/compose env file has no way to represent "unset" other than an
    empty string. So ``KDB_HOST=""`` is treated as absent, not as an
    explicit empty value -- there's no falsy-but-meaningful env value to
    protect the way there is for credentials.
    """
    creds = credentials or {}
    cred_key = f"kdb_{key}"
    if cred_key in creds and creds[cred_key] is not None:
        return creds[cred_key]
    env_val = os.getenv(env)
    if env_val:
        return env_val
    return None


def resolve_config(credentials: dict | None = None) -> KdbConfig:
    """Resolve configuration from credentials, environment, then defaults."""
    # No KDB_HOST means there is simply no external kdb to fall back to.
    host = _pick("host", "KDB_HOST", credentials) or None

    raw_port = _pick("port", "KDB_PORT", credentials)
    if raw_port is None:
        raw_port = _DEFAULTS["port"]
    try:
        port = int(raw_port)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid kdb+ port: {raw_port!r}. Must be an integer.") from exc
    if not 1 <= port <= 65535:
        raise ValueError(f"KDB_PORT {port} out of range (1-65535).")

    bind_host = (
        _pick("bind_host", "KDB_BIND_HOST", credentials) or _DEFAULTS["bind_host"]
    )

    # Where the operator mounted their own q. QHOME is accepted as a fallback
    # for anyone carrying the older variable, but it is read ONCE per process
    # (see _qhome_once) because `import pykx` rewrites QHOME in place.
    local_qhome = (
        _pick("local_qhome", "KDB_LOCAL_QHOME", credentials)
        or _qhome_once()
        or _DEFAULTS["local_qhome"]
    )

    raw_embedded = _pick("embedded", "KDB_EMBEDDED", credentials)
    if raw_embedded is None:
        # Spawn only if the operator actually supplied a q. Otherwise the
        # chain falls through to KDB_HOST.
        may_spawn = has_local_q(local_qhome)
    else:
        may_spawn = str(raw_embedded).strip().lower() in ("1", "true", "yes", "on")

    raw_mem = _pick("memory_mb", "KDB_MEMORY_MB", credentials)
    if raw_mem is None:
        raw_mem = _DEFAULTS["memory_mb"]
    try:
        memory_mb = int(raw_mem)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid KDB_MEMORY_MB: {raw_mem!r}.") from exc
    if memory_mb < 64:
        raise ValueError(f"KDB_MEMORY_MB {memory_mb} is too small (minimum 64).")

    raw_wm = _pick("cache_watermark", "KDB_CACHE_WATERMARK", credentials)
    if raw_wm is None:
        raw_wm = _DEFAULTS["watermark"]
    try:
        watermark = float(raw_wm)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid KDB_CACHE_WATERMARK: {raw_wm!r}.") from exc
    if not 0.1 <= watermark <= 0.95:
        raise ValueError(f"KDB_CACHE_WATERMARK {watermark} out of range (0.1-0.95).")

    upstream = _pick("upstream", "KDB_UPSTREAM", credentials)
    if upstream is None:
        upstream = _DEFAULTS["upstream"]
    qhome = _qhome_once() or _DEFAULTS["qhome"]
    # A licence dir separate from QHOME lets the operator bind-mount kc.lic
    # without it living inside qhome (which the Dockerfile strips clean).
    # Falling back to qhome preserves today's behaviour for anyone who does
    # keep their licence in QHOME.
    qlic = os.getenv("QLIC") or qhome

    return KdbConfig(
        host=host, port=port, bind_host=bind_host, may_spawn=may_spawn,
        memory_mb=memory_mb, watermark=watermark, upstream=str(upstream),
        qhome=qhome, qlic=qlic, local_qhome=local_qhome,
    )
