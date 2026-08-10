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
decommission <admin-addr> <node-id> [--force-control-remove]  # composite, see below (ADR 0037 PR4)
control-add <leader-admin-addr> <node-id> <new-node-admin-addr>      # ADR 0037 PR3
control-remove <leader-admin-addr> <node-id> [--force]               # ADR 0037 PR3; --force: hardening PR2
control-grow <leader-admin-addr> <node-id> <admin-addr> [<node-id> <admin-addr>...]
```

- Metrics/storage views are **per node** (a follower's leader-only counters
  are legitimately 0) — point them at the relevant node.
- **`decommission` is real orchestration, not a one-shot passthrough**
  (`run_decommission`, ADR 0032 PR3): it drives drain → polls `drain-status`
  until the node holds no replicas → `remove`. It must target the
  **control-plane leader's** admin address (the underlying admin actions are
  local-leader-only and not relayed); on a follower it fails with a
  "not the control-plane leader" routing error — retry against the leader.
  **A combined node that is also a *live* control-plane voter (ADR 0037
  PR4)**: `run_decommission` asks `GET /admin/control/members` up front
  (best-effort — an old binary/route-miss just skips this pre-check and
  falls through to the ordinary flow, whose own final `remove` step still
  carries the authoritative refusal); if the target's paired **control** id
  (`node - RAFTKV_ID_BASE`, the same combined-mode convention `control-add`/
  `control-remove` already use) is a live voter, it refuses immediately
  (before ever draining) unless `--force-control-remove` is passed, in which
  case it runs `control-remove` + polls to convergence *first*, then falls
  through to the unchanged drain → drain-status → remove flow. The
  authoritative refusal always lives server-side in `admin_remove_member`
  (`animusd`) — this is a friendlier, fail-fast CLI-side mirror of it, not a
  replacement.
- **`control-add`/`control-grow` are also orchestration, not passthroughs**
  (ADR 0037 PR3): they take the new control voter's own **admin** address
  (this CLI's convention everywhere else), not the internal control-Raft
  address `POST /admin/control/member/add`'s wire payload actually wants —
  `run_control_add` bridges the two itself via a `GET /admin/config` against
  the new node (which doubles as the "confirm it's up" liveness check), then
  polls the **new node's own** `/admin/control/members` until it reports
  itself a voter. `control-grow` is a sequential loop of `control-add` calls
  (one server at a time — `RaftCore::change_membership` never accepts a
  multi-server delta), each waiting for its own catch-up before the next.
  `control-add`/`control-remove`/`control-grow` all target the **leader's**
  admin address, same "not relayed" discipline as `decommission`.
- **`control-remove ... [--force]` (ADR 0037 hardening PR2, PR #136, the quorum-guard
  liveness fix)**: the server now refuses a removal that would leave fewer
  than a majority of the *resulting* voters reachable (per
  `RaftNode::control_peer_believed_alive`, a control-id-native liveness
  signal — see `animus-control/CLAUDE.md`'s `node.rs` entry), naming the
  apparently-dead voter(s) and pointing at `--force`. `--force` bypasses that
  guard — the same explicit escape hatch `remove`/`decommission` already use
  for their own hard cases. **`decommission --force-control-remove` does
  NOT imply `--force`** — these are deliberately separate, independently-
  explicit flags: the former only says "run `control-remove` as part of
  decommission," never "and skip `control-remove`'s own safety check."
  `run_decommission`'s internal `control-remove` call always passes
  `force: false`; if the liveness guard refuses it, the operator must retry
  with `animus admin control-remove <leader> <id> --force` by hand.

## Tests

No tests of its own; the client path is covered end-to-end by `animusd`'s
`tests/cluster.rs`, and the admin surface by `animusd`'s admin/decommission
tests (`tests/decommission.rs` among others).
