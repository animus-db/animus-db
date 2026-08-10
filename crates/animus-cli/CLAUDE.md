# CLAUDE.md — animus-cli

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The operator/client CLI (`animus`): a thin client over a node's APIs, speaking
two transports —

- a **plain-TCP client API** (`status`/`put`/`get`): one framed
  `ClientRequest`, one framed `ClientResponse`, against a node's **client**
  address;
- an **HTTP/JSON admin** subcommand group (ADR 0020): a minimal hand-rolled
  HTTP/1.0 client (`http_call`) against a node's **admin** address, printing
  the server's JSON verbatim (the server pretty-prints) and exiting non-zero on
  a non-2xx status.

The whole crate is one file, `src/main.rs`.

## Client subcommands

```
animus status <node-addr>
animus put    <node-addr> <table> <key> <value>
animus get    <node-addr> <table> <key>
```

- **Every key names a table** (ADR 0023) — `put`/`get` take a `<table>`
  argument; the table is the routing argument, not part of the key bytes.
- `<node-addr>` is any node's client address (as printed by `animusd`).
  Server-side routing/forwarding means any node can serve any key.
- It depends on `animusd` only for the shared protocol types and frame helpers
  (`ClientRequest`/`ClientResponse`, `read_frame`/`write_frame`) — it does
  **not** speak the internal `Network`.
- `print_response` also handles `ClientResponse::JoinInfo` (ADR 0032) and
  `ClientResponse::NodeIdAllocated` (ADR 0036): the join-discovery and
  cluster-allocated-id replies `animusd join`/`data --seed` consume at
  startup — not CLI subcommands of their own. **`ClientResponse` gaining a
  variant means `print_response`'s `match` stops compiling until it's
  handled here too** — this is a separate exhaustiveness site from
  `animusd`'s own `is_relayable_command`/`ClientRequest` dispatch; both need
  checking whenever either enum grows.

## Admin subcommand group

`animus admin <sub> <admin-addr> [args]` — see `ADMIN_USAGE` in `main.rs` for
the canonical list. Read-only views:

```
config|status|raft|raftkv|metrics|health <admin-addr>
lsm|wal <admin-addr> [tablet]
wal-segment <admin-addr> <seg> [tablet]
key <admin-addr> <key> [tablet]
```

Mutating actions:

```
split <admin-addr> <tablet> <split-key>        # ADR 0028 single-command split
merge <admin-addr> <left> <right>              # ADR 0033 tablet merge
flush|compact <admin-addr> <tablet>
reconfigure <admin-addr> <tablet> <voter,voter,...>
drain <admin-addr> <node-id>                   # ADR 0029/0032 release replicas
drain-status <admin-addr> <node-id>
remove <admin-addr> <node-id>
decommission <admin-addr> <node-id>            # composite, see below
```

- Metrics/storage views are **per node** (a follower's leader-only counters
  are legitimately 0) — point them at the relevant node.
- **`decommission` is real orchestration, not a one-shot passthrough**
  (`run_decommission`, ADR 0032 PR3): it drives drain → polls `drain-status`
  until the node holds no replicas → `remove`. It must target the
  **control-plane leader's** admin address (the underlying admin actions are
  local-leader-only and not relayed); on a follower it fails with a
  "not the control-plane leader" routing error — retry against the leader.

## Tests

No tests of its own; the client path is covered end-to-end by `animusd`'s
`tests/cluster.rs`, and the admin surface by `animusd`'s admin/decommission
tests (`tests/decommission.rs` among others).
