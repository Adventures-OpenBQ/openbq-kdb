#!/usr/bin/env bash
# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0

# One-shot check of the websocket tick stream against a real KDB-X.
# Spins a throwaway container on 127.0.0.1:5998 (the deployed service uses
# 5999, so this never collides with it), runs smoke.mjs, tears down.
set -euo pipefail
cd "$(dirname "$0")"
docker rm -f openbq-kdb-test >/dev/null 2>&1 || true
docker run -d --name openbq-kdb-test --platform linux/amd64 \
  -p 127.0.0.1:5998:5000 -e KX_PORT=0.0.0.0:5000 -e QLIC=/opt/kx-license \
  -v "$PWD:/data" -v "$PWD/../kdb-license:/opt/kx-license:ro" \
  ghcr.io/artcashin/kdb-x:amd64 /data/startup.q >/dev/null
trap 'docker rm -f openbq-kdb-test >/dev/null' EXIT
sleep 4
node smoke.mjs
