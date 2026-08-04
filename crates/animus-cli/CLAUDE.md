# CLAUDE.md — animus-cli

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The operator/client CLI (`animus`): a thin client over a node's APIs.
Subcommands: `status`, `put`, `get` (plain-TCP request/reply), and the `admin`
subcommand group (ADR 0020), which speaks the node's HTTP-JSON **admin** endpoint
on its admin address and prints the JSON response.

## What's non-obvious

- It depends on `animusd` only for the shared protocol types and frame
  helpers (`ClientRequest`/`ClientResponse`, `read_frame`/`write_frame`) — it
  does **not** speak the internal `Network`; it just opens one TCP connection to
  a node's client address, sends one framed request, and reads one framed reply.
- `<node-addr>` is any node's client address (as printed by `animusd --cluster`).
  Server-side coordination means any node can serve any key.
- The `admin` subcommand group (`animus admin <sub> <admin-addr> [args]`) is a
  separate transport: a minimal hand-rolled HTTP/1.0 client (`http_call`) against
  the node's **admin** address, not the plain-TCP `ClientRequest` API. Subcommands:
  `config|status|raft|raftkv|metrics|health`, `lsm|wal [tablet]`,
  `wal-segment <seg> [tablet]`, `key <key> [tablet]`, and the actions
  `split|flush|compact|reconfigure|drain`. It prints the server's JSON verbatim
  (the server pretty-prints) and exits non-zero on a non-2xx status. Metrics/storage
  views are **per node** (a follower's leader-only counters are 0) — point them at
  the relevant node.

## Tests

No tests of its own; the client path is covered end-to-end by
`animusd`'s `tests/cluster.rs`.
