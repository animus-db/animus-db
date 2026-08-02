# CLAUDE.md — animus-cli

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The operator/client CLI (`animus`): a thin plain-TCP client over a node's
request/reply API. Subcommands: `status`, `put`, `get`.

## What's non-obvious

- It depends on `animusd` only for the shared protocol types and frame
  helpers (`ClientRequest`/`ClientResponse`, `read_frame`/`write_frame`) — it
  does **not** speak the internal `Network`; it just opens one TCP connection to
  a node's client address, sends one framed request, and reads one framed reply.
- `<node-addr>` is any node's client address (as printed by `animusd --cluster`).
  Server-side coordination means any node can serve any key.
- Operator commands beyond status/put/get (membership changes, explicit
  split/merge) are future work; the node would expose them as new
  `ClientRequest` variants.

## Tests

No tests of its own; the client path is covered end-to-end by
`animusd`'s `tests/cluster.rs`.
