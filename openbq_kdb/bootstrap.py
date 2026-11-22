# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0
"""Opt-in qualification launcher. Existing v10 KdbSession is not switched over.

This stage advertises ping only. Do not use it as the tick plant until the
remaining named operations and persistence integration have been qualified.
"""
from __future__ import annotations

import argparse
from importlib.resources import as_file, files
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile

from openbq_kdb.credentials import CredentialConfigurationError, load_server_credentials


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--q", required=True, type=Path)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--init-script", type=Path, help="trusted local initialization only")
    opts = parser.parse_args()
    if not 1 <= opts.port <= 65535:
        parser.error("port must be in 1..65535")
    try:
        credentials = load_server_credentials(
            Path(os.environ.get("KDB_AUTH_PASSWORDS_FILE", "")),
            Path(os.environ.get("KDB_AUTH_ROLES_FILE", "")),
        )
    except CredentialConfigurationError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    with tempfile.TemporaryDirectory(prefix="openbq-auth-") as directory:
        # Freeze the already validated pair; operator edits cannot change one
        # half between validation and q's native password-file read.
        root = Path(directory)
        password_copy = root / "passwords"
        password_copy.write_text(credentials.password_file)
        password_copy.chmod(0o600)
        policy_copy = root / "policy.json"
        policy_copy.write_text(json.dumps({"principals": list(credentials.principals)}))
        env = dict(os.environ, QHOME=str(opts.q.absolute().parent.parent),
                   OPENBQ_BOOTSTRAP_POLICY=str(policy_copy),
                   OPENBQ_BOOTSTRAP_LISTEN=f"127.0.0.1:{opts.port}",
                   OPENBQ_BOOTSTRAP_INIT=str(opts.init_script.absolute()) if opts.init_script else "")
        asset = files("openbq_kdb").joinpath("q/bootstrap.q")
        with as_file(asset) as startup:
            command = [str(opts.q), str(startup), "-q", "-p", "0", "-U", str(password_copy)]
            proc = subprocess.Popen(command, stdin=subprocess.PIPE, env=env, cwd=root)
            previous = {}

            def forward(number, _frame):
                if proc.poll() is None:
                    proc.send_signal(number)

            try:
                for number in (signal.SIGTERM, signal.SIGINT):
                    previous[number] = signal.signal(number, forward)
                return proc.wait()
            finally:
                for number, handler in previous.items():
                    signal.signal(number, handler)
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                if proc.stdin:
                    proc.stdin.close()


if __name__ == "__main__":
    raise SystemExit(main())
