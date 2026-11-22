# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0
"""Authenticated q launcher: ping qualification or durable named-data tickplant.

Only trusted local scripts run before the restricted network listener opens.
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
from openbq_kdb.diagnostics import RuntimeOutputDrain


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--q", required=True, type=Path)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--mode", choices=("qualification", "tickplant"), default="qualification")
    parser.add_argument("--workspace-mb", type=int, default=0)
    parser.add_argument("--data-dir", type=Path, help="enable named data API with durable local logs")
    parser.add_argument("--init-script", type=Path, help="trusted local initialization only")
    opts = parser.parse_args()
    if opts.workspace_mb < 0:
        parser.error("workspace-mb must be nonnegative")
    if opts.mode == "tickplant" and opts.data_dir is None:
        parser.error("tickplant mode requires --data-dir")
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
        manifest = json.loads(files("openbq_kdb").joinpath("resources/protocol-v1.json").read_text())
        grants = {user: sorted({op for role in roles for op in manifest["roles"][role]})
                  for user, roles in credentials.principals.items()}
        policy_copy.write_text(json.dumps({"principals": list(credentials.principals),
                                           "grants": grants}))
        if opts.data_dir:
            (opts.data_dir / "logs").mkdir(parents=True, exist_ok=True)
        env = dict(os.environ, QHOME=str(opts.q.absolute().parent.parent),
                   OPENBQ_BOOTSTRAP_POLICY=str(policy_copy),
                   OPENBQ_BOOTSTRAP_LISTEN=f"{opts.bind}:{opts.port}",
                   OPENBQ_BOOTSTRAP_INIT=str(opts.init_script.absolute()) if opts.init_script else "",
                   OPENBQ_DATA_DIR=str(opts.data_dir.absolute()) if opts.data_dir else "",
                   OPENBQ_Q_ASSETS=str(files("openbq_kdb").joinpath("q")))
        asset = files("openbq_kdb").joinpath("q/bootstrap.q")
        with as_file(asset) as startup:
            command = [str(opts.q), str(startup), "-q", "-p", "0", "-U", str(password_copy)]
            if opts.workspace_mb:
                command.extend(["-w", str(opts.workspace_mb)])
            proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, env=env, cwd=root)
            output_drain = RuntimeOutputDrain(proc.stdout).start()
            previous = {}

            def forward(number, _frame):
                if proc.poll() is None:
                    proc.send_signal(number)

            try:
                for number in (signal.SIGTERM, signal.SIGINT):
                    previous[number] = signal.signal(number, forward)
                code = proc.wait()
                if code != 0:
                    print("KDB_CHILD_EXITED", file=sys.stderr)
                return code
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
                output_drain.close()
                if proc.stdin:
                    proc.stdin.close()


if __name__ == "__main__":
    raise SystemExit(main())
