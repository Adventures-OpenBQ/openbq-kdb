# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0
"""Malformed secrets fail closed without their contents appearing in errors."""
import json
import os
import secrets
import subprocess
import sys

import pytest


def files(tmp):
    token = secrets.token_urlsafe(48)
    passwords = tmp / "passwords"
    passwords.write_text(f"reader:{token}\n")
    passwords.chmod(0o600)
    roles = tmp / "roles.json"
    roles.write_text(json.dumps({"schema_version": 1, "principals": {"reader": ["query"]}}))
    return token, passwords, roles


@pytest.mark.parametrize("policy", [
    '{"schema_version":1,"principals":{"reader":["admin"]}}',
    '{"schema_version":1,"principals":{"reader":[]}}',
    '{"schema_version":1,"principals":{"reader":["query"],"reader":["recorder"]}}',
    '{"schema_version":1,"schema_version":1,"principals":{"reader":["query"]}}',
    '{"schema_version":2,"principals":{"reader":["query"]}}',
    '{"schema_version":1,"principals":{"reader":["query"]},"extra":true}',
])
def test_invalid_or_duplicate_policy_is_rejected(tmp_path, policy):
    from openbq_kdb.credentials import CredentialConfigurationError, load_server_credentials
    token, passwords, roles = files(tmp_path)
    roles.write_text(policy)
    with pytest.raises(CredentialConfigurationError) as err:
        load_server_credentials(passwords, roles)
    assert token not in str(err.value)


@pytest.mark.parametrize("value", ["", "too-short", "a" * 40 + ":extra", "a" * 40 + "\n\n"])
def test_client_rejects_empty_short_or_multiline_token(tmp_path, value):
    from openbq_kdb.credentials import CredentialConfigurationError, load_client_credentials
    secret = tmp_path / "token"
    secret.write_text(value)
    secret.chmod(0o600)
    with pytest.raises(CredentialConfigurationError):
        load_client_credentials({"KDB_USERNAME": "reader", "KDB_PASSWORD_FILE": str(secret)})


def test_client_accepts_one_final_newline_without_exposing_secret(tmp_path):
    from openbq_kdb.credentials import load_client_credentials
    token, _, _ = files(tmp_path)
    secret = tmp_path / "token"
    secret.write_text(token + "\n")
    secret.chmod(0o600)
    creds = load_client_credentials({"KDB_USERNAME": "reader", "KDB_PASSWORD_FILE": str(secret)})
    assert creds.username == "reader"
    assert creds.password == token
    assert token not in repr(creds)


@pytest.mark.parametrize("line", ["reader:short", "reader:" + "a" * 40 + "\nreader:" + "b" * 40,
                                  "reader:" + "a" * 40 + "\n\n"])
def test_server_rejects_invalid_or_duplicate_password_entries(tmp_path, line):
    from openbq_kdb.credentials import CredentialConfigurationError, load_server_credentials
    _, passwords, roles = files(tmp_path)
    passwords.write_text(line)
    with pytest.raises(CredentialConfigurationError):
        load_server_credentials(passwords, roles)


def test_server_refuses_world_readable_password_file(tmp_path):
    from openbq_kdb.credentials import CredentialConfigurationError, load_server_credentials
    _, passwords, roles = files(tmp_path)
    passwords.chmod(0o644)
    with pytest.raises(CredentialConfigurationError):
        load_server_credentials(passwords, roles)


@pytest.mark.skipif(os.name != "posix", reason="POSIX special-file behavior")
def test_special_file_configuration_cannot_hang_startup(tmp_path):
    fifo = tmp_path / "token-fifo"
    os.mkfifo(fifo, 0o600)
    code = (
        "import sys;from openbq_kdb.credentials import "
        "load_client_credentials,CredentialConfigurationError\n"
        "try: load_client_credentials()\n"
        "except CredentialConfigurationError: sys.exit(0)\n"
        "sys.exit(1)\n"
    )
    result = subprocess.run([sys.executable, "-c", code], timeout=1,
                            env=dict(os.environ, KDB_USERNAME="reader", KDB_PASSWORD_FILE=str(fifo)),
                            capture_output=True)
    assert result.returncode == 0
