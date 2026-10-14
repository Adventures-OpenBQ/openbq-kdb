<!-- Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# openbq-kdb

The kdb+ tier of the *Adventures in OpenBQ* stack, which openbq-docker v10 vendors under
`v10/kdb/source` for its opt-in `live` compose profile. It has two parts:

- `openbq_kdb/` (distribution `openbq-kdb`): the shared kdb+ plumbing, meaning the `q`
  child process, its IPC connection, and the store built on top. live-grid, the tick
  recorder, uses it.
- `q/`: the q tick plant the stack's `openbq-kdb` service runs (`startup.q`, `persist.q`,
  the websocket demo chart), with its Python model `q/tests/tickplant_sim.py`. See
  [q/README.md](q/README.md).

It is bdobb-dev/openbb-docker's `kdb-store` and `kdb-ws` (v10.1.7, d606169), ported and
renamed; NOTICE records the provenance. The kdb-x runtime image is not part of it.

**Why this is its own package.** PyKX aborts the process when touched from more
than one thread — not merely under concurrency, but on strictly sequential
calls from different threads (`free(): invalid size`). `KdbSession` solves that
by marshalling every PyKX call onto one owner thread. Two consumers need that
guarantee, and there must be exactly one implementation of it.

## Test

    pip install -e .[dev] && pytest    # no kdb licence needed
