# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0

"""Config resolution: env vars and OpenBB credentials into one frozen object."""

import pytest

import openbq_kdb.config as config
from openbq_kdb.config import resolve_config


@pytest.fixture(autouse=True)
def _unmade_qhome_decision(monkeypatch):
    """QHOME is decided once per PROCESS; give each test that decision unmade."""
    monkeypatch.setattr(config, "_qhome", None)
    monkeypatch.setattr(config, "_qhome_decided", False)


def test_defaults(monkeypatch, tmp_path):
    for var in ("KDB_HOST", "KDB_PORT", "KDB_BIND_HOST", "KDB_EMBEDDED",
                "KDB_MEMORY_MB", "KDB_CACHE_WATERMARK", "KDB_UPSTREAM"):
        monkeypatch.delenv(var, raising=False)
    # No local q at this path: isolates the default from whatever the host
    # machine happens to have sitting at /opt/kx.
    monkeypatch.setenv("KDB_LOCAL_QHOME", str(tmp_path))
    cfg = resolve_config()
    assert cfg.host is None
    assert cfg.port == 5000
    assert cfg.bind_host == "127.0.0.1"
    assert cfg.may_spawn is False
    assert cfg.memory_mb == 8192
    assert cfg.watermark == 0.75
    assert cfg.upstream == "eodhd"


def test_bind_host_from_env(monkeypatch):
    # KDB_BIND_HOST is the prerequisite for the bridge-network migration
    # (docs/superpowers/specs/2026-08-27-bridge-network-design.md) -- it lets
    # the q spawn bind (and a sibling's fallback connect) move off loopback
    # without a code change.
    monkeypatch.setenv("KDB_BIND_HOST", "0.0.0.0")
    cfg = resolve_config()
    assert cfg.bind_host == "0.0.0.0"


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("KDB_HOST", "kdb.internal")
    monkeypatch.setenv("KDB_PORT", "5010")
    monkeypatch.setenv("KDB_UPSTREAM", "yfinance")
    cfg = resolve_config()
    assert (cfg.host, cfg.port, cfg.upstream) == ("kdb.internal", 5010, "yfinance")


def test_credentials_beat_env(monkeypatch):
    monkeypatch.setenv("KDB_UPSTREAM", "yfinance")
    cfg = resolve_config({"kdb_upstream": "fmp"})
    assert cfg.upstream == "fmp"


def test_pointing_at_an_external_server_disables_spawning(monkeypatch, tmp_path):
    """No local q means nothing to spawn, regardless of where KDB_HOST points."""
    monkeypatch.setenv("KDB_HOST", "kdb.internal")
    monkeypatch.setenv("KDB_LOCAL_QHOME", str(tmp_path))
    monkeypatch.delenv("KDB_EMBEDDED", raising=False)
    assert resolve_config().may_spawn is False


def test_explicit_embedded_false(monkeypatch):
    monkeypatch.setenv("KDB_EMBEDDED", "false")
    assert resolve_config().may_spawn is False


def test_workspace_is_25_percent_above_budget(monkeypatch):
    monkeypatch.setenv("KDB_MEMORY_MB", "8192")
    assert resolve_config().q_workspace_mb == 10240


def test_rejects_bad_port(monkeypatch):
    monkeypatch.setenv("KDB_PORT", "notanumber")
    with pytest.raises(ValueError):
        resolve_config()


def test_rejects_out_of_range_watermark(monkeypatch):
    monkeypatch.setenv("KDB_CACHE_WATERMARK", "1.5")
    with pytest.raises(ValueError):
        resolve_config()


def test_config_is_frozen():
    cfg = resolve_config()
    with pytest.raises(Exception):
        cfg.host = "elsewhere"  # type: ignore[misc]


def test_explicit_embedded_false_credential_is_honored(monkeypatch):
    """A credential of False must not be mistaken for 'not set'."""
    monkeypatch.delenv("KDB_EMBEDDED", raising=False)
    cfg = resolve_config({"kdb_embedded": False})
    assert cfg.may_spawn is False


def test_explicit_zero_memory_credential_raises(monkeypatch):
    """A credential of 0 must not be mistaken for 'not set' and silently defaulted."""
    monkeypatch.delenv("KDB_MEMORY_MB", raising=False)
    with pytest.raises(ValueError):
        resolve_config({"kdb_memory_mb": 0})


def test_explicit_zero_watermark_credential_raises(monkeypatch):
    """A credential of 0.0 must not be mistaken for 'not set' and silently defaulted."""
    monkeypatch.delenv("KDB_CACHE_WATERMARK", raising=False)
    with pytest.raises(ValueError):
        resolve_config({"kdb_cache_watermark": 0.0})


def test_empty_env_host_falls_back_to_default(monkeypatch):
    """An empty string is how a shell/compose env file expresses 'unset' --
    and with no KDB_HOST there is no external kdb to fall back to."""
    monkeypatch.setenv("KDB_HOST", "")
    assert resolve_config().host is None


def test_ipv6_loopback_host_is_treated_as_embedded(monkeypatch, tmp_path):
    """Host no longer gates spawning -- an ipv6-loopback KDB_HOST does not
    disable it when a runnable local q is present."""
    qbin = tmp_path / "bin" / "q"
    qbin.parent.mkdir(parents=True)
    qbin.write_text("#!/bin/sh\n")
    qbin.chmod(0o755)
    monkeypatch.setenv("KDB_HOST", "::1")
    monkeypatch.setenv("KDB_LOCAL_QHOME", str(tmp_path))
    monkeypatch.delenv("KDB_EMBEDDED", raising=False)
    assert resolve_config().may_spawn is True


def test_qlic_env_var_is_carried_onto_config(monkeypatch):
    """The mounted licence directory must survive into KdbConfig.qlic."""
    monkeypatch.setenv("QLIC", "/opt/kx-license")
    assert resolve_config().qlic == "/opt/kx-license"


def test_qhome_survives_pykx_rewriting_the_environment(monkeypatch):
    """`import pykx` REWRITES os.environ["QHOME"] to PyKX's own bundled q. A
    second resolve_config() must not therefore hand back a different qhome --
    spawning PyKX's q instead of the operator's kdb-x install."""
    monkeypatch.setenv("QHOME", "/opt/kx")
    first = resolve_config()

    monkeypatch.setenv("QHOME", "/usr/local/lib/python3.13/site-packages/pykx/lib")
    second = resolve_config()

    assert first.qhome == second.qhome == "/opt/kx"


def test_qlic_falls_back_to_qhome_when_unset(monkeypatch):
    """No QLIC set: preserve today's behaviour of looking in QHOME."""
    monkeypatch.delenv("QLIC", raising=False)
    monkeypatch.setenv("QHOME", "/opt/kx")
    cfg = resolve_config()
    assert cfg.qlic == cfg.qhome == "/opt/kx"


def test_local_qhome_defaults_to_opt_kx(monkeypatch):
    monkeypatch.delenv("KDB_LOCAL_QHOME", raising=False)
    monkeypatch.delenv("QHOME", raising=False)
    assert resolve_config().local_qhome == "/opt/kx"


def test_local_qhome_from_env(monkeypatch):
    monkeypatch.setenv("KDB_LOCAL_QHOME", "/opt/mine")
    assert resolve_config().local_qhome == "/opt/mine"


def test_host_is_none_when_unset(monkeypatch):
    """No KDB_HOST means there is no external kdb to fall back to."""
    monkeypatch.delenv("KDB_HOST", raising=False)
    assert resolve_config().host is None


def test_host_from_env(monkeypatch):
    monkeypatch.setenv("KDB_HOST", "host.docker.internal")
    assert resolve_config().host == "host.docker.internal"


def test_may_spawn_is_false_when_no_local_q(monkeypatch, tmp_path):
    """Nothing to spawn: the chain should fall through to KDB_HOST."""
    monkeypatch.setenv("KDB_LOCAL_QHOME", str(tmp_path))
    monkeypatch.delenv("KDB_EMBEDDED", raising=False)
    assert resolve_config().may_spawn is False


def test_may_spawn_is_true_when_a_runnable_q_is_present(monkeypatch, tmp_path):
    qbin = tmp_path / "bin" / "q"
    qbin.parent.mkdir(parents=True)
    qbin.write_text("#!/bin/sh\n")
    qbin.chmod(0o755)
    monkeypatch.setenv("KDB_LOCAL_QHOME", str(tmp_path))
    monkeypatch.delenv("KDB_EMBEDDED", raising=False)
    assert resolve_config().may_spawn is True


def test_a_present_but_non_executable_q_does_not_count(monkeypatch, tmp_path):
    qbin = tmp_path / "bin" / "q"
    qbin.parent.mkdir(parents=True)
    qbin.write_text("not executable")
    qbin.chmod(0o644)
    monkeypatch.setenv("KDB_LOCAL_QHOME", str(tmp_path))
    monkeypatch.delenv("KDB_EMBEDDED", raising=False)
    assert resolve_config().may_spawn is False


def test_kdb_embedded_false_overrides_a_present_q(monkeypatch, tmp_path):
    """live-grid sets this: two spawners in one namespace race for the port."""
    qbin = tmp_path / "bin" / "q"
    qbin.parent.mkdir(parents=True)
    qbin.write_text("#!/bin/sh\n")
    qbin.chmod(0o755)
    monkeypatch.setenv("KDB_LOCAL_QHOME", str(tmp_path))
    monkeypatch.setenv("KDB_EMBEDDED", "false")
    assert resolve_config().may_spawn is False


def test_kdb_embedded_true_forces_spawn_even_with_no_q(monkeypatch, tmp_path):
    """An explicit true is a deliberate choice; let the spawn fail loudly."""
    monkeypatch.setenv("KDB_LOCAL_QHOME", str(tmp_path))
    monkeypatch.setenv("KDB_EMBEDDED", "true")
    assert resolve_config().may_spawn is True
