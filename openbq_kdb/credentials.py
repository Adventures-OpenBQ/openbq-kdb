# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0
"""Process-owned credential files; errors never include file contents or tokens."""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import re
import stat
from typing import Mapping

ROLES = frozenset({"query", "cache", "recorder", "export", "diagnostics", "chart"})
_IDENTITY = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,63}\Z")
_TOKEN = re.compile(r"[A-Za-z0-9_-]{32,256}\Z")
_MAX_FILE = 65_536


class CredentialConfigurationError(ValueError):
    """Safe configuration classification, without source paths or secret values."""


def _read(path: Path, *, secret: bool) -> str:
    try:
        # Nonblocking open lets us reject a FIFO before any read can hang.
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0))
        with os.fdopen(descriptor, "rb") as source:
            info = os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > _MAX_FILE:
                raise CredentialConfigurationError("KDB_CREDENTIAL_FILE")
            if secret and os.name == "posix" and info.st_mode & 0o077:
                raise CredentialConfigurationError("KDB_CREDENTIAL_PERMISSIONS")
            data = source.read(_MAX_FILE + 1)
        if len(data) > _MAX_FILE:
            raise CredentialConfigurationError("KDB_CREDENTIAL_FILE")
        return data.decode("ascii")
    except (OSError, UnicodeError):
        raise CredentialConfigurationError("KDB_CREDENTIAL_FILE") from None


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise CredentialConfigurationError("KDB_POLICY_DUPLICATE")
        result[key] = value
    return result


@dataclass(frozen=True)
class ClientCredentials:
    username: str
    password: str = field(repr=False)


@dataclass(frozen=True)
class ServerCredentials:
    principals: dict[str, tuple[str, ...]]
    password_file: str = field(repr=False)


def load_client_credentials(environ: Mapping[str, str] | None = None) -> ClientCredentials:
    env = os.environ if environ is None else environ
    user = env.get("KDB_USERNAME", "")
    source = env.get("KDB_PASSWORD_FILE", "")
    if not _IDENTITY.fullmatch(user) or not source:
        raise CredentialConfigurationError("KDB_CLIENT_CREDENTIALS")
    token = _read(Path(source), secret=True).removesuffix("\n")
    if not _TOKEN.fullmatch(token):
        raise CredentialConfigurationError("KDB_CLIENT_CREDENTIALS")
    return ClientCredentials(user, token)


def load_server_credentials(passwords: Path, roles: Path) -> ServerCredentials:
    text = _read(passwords, secret=True)
    entries = {}
    for line in text.removesuffix("\n").split("\n"):
        parts = line.split(":")
        if (len(parts) != 2 or not _IDENTITY.fullmatch(parts[0])
                or not _TOKEN.fullmatch(parts[1]) or parts[0] in entries):
            raise CredentialConfigurationError("KDB_SERVER_CREDENTIALS")
        entries[parts[0]] = parts[1]
    try:
        policy = json.loads(_read(roles, secret=False), object_pairs_hook=_object)
    except (ValueError, RecursionError):
        raise CredentialConfigurationError("KDB_POLICY") from None
    if (not isinstance(policy, dict) or set(policy) != {"schema_version", "principals"}
            or type(policy["schema_version"]) is not int or policy["schema_version"] != 1
            or not isinstance(policy["principals"], dict)
            or set(policy["principals"]) != set(entries)):
        raise CredentialConfigurationError("KDB_POLICY")
    principals = {}
    for user, grants in policy["principals"].items():
        if (not isinstance(grants, list) or not grants
                or any(not isinstance(role, str) or role not in ROLES for role in grants)
                or len(set(grants)) != len(grants)):
            raise CredentialConfigurationError("KDB_POLICY")
        principals[user] = tuple(grants)
    return ServerCredentials(principals, "\n".join(f"{u}:{p}" for u, p in entries.items()))
