# CLAUDE.md — animus-node

This file provides guidance to Claude Code (claude.ai/code) when working in this
crate.

## Purpose

The `E: Env`-generic node core ADR 0061 Decision 1 carves out of `animusd`:
the growing home of every piece of node logic — routing, forwarding, the
wire surface, background loops, transaction coordination — that needs no
real clock, no real socket, and no `tokio`. `animusd` keeps the binary:
`main.rs`, config, listener binding, process lifecycle, signal handling,
and the **one** `ProdEnv` construction site. See ADR 0061 (the whole
document, but especially Decision 1 and its 2026-08-28 amendment) for the
rationale and the full delivery plan (Phase C's rung table).

## Why this is a crate, not a module

Genericizing the node logic in place (inside `animusd`, behind `E: Env`
generic parameters but no crate boundary) was considered and rejected —
see ADR 0061 Decision 1. The failure mode this carve-out exists to close is
nondeterminism creeping back into logic that ought to be pure; in-place
genericization leaves nothing to stop that but review, which is exactly the
enforcement class that already let `animusd` accumulate ~600 real
`Instant::now`/`tokio::spawn`/`tokio::time::{sleep,timeout}` call sites
(ADR 0061 Decision 4's as-built note, `animusd/CLAUDE.md`). A crate boundary
makes the constraint **compiler-enforced**:

- **`Cargo.toml` depends on `animus-env` with `default-features = false`.**
  `animus-env`'s `prod` feature (ADR 0061 rung C0) gates `ProdEnv`/
  `FsSegmentStore`/`PreBindRng` — its only real-time/real-IO/real-RNG
  surface — behind a default-off feature. With `default-features = false`
  here, `ProdEnv` genuinely does not exist in this crate's build: writing
  `animus_env::ProdEnv::new(..)` is a compile error, not a lint or a review
  catch. The dependency is declared as a direct `path = "../animus-env"`
  entry rather than `animus-env.workspace = true` — Cargo refuses to let a
  workspace-inherited dependency override `default-features` to `false`
  unless the root `[workspace.dependencies]` entry already says so, and
  changing that root entry would flip every *other* consumer's default
  too. A direct path dependency sidesteps workspace-dependency inheritance
  for this one line instead.
- **No `tokio` dependency at all** — not even as a transitive default
  through another crate. `TcpStream::connect`, `tokio::spawn`,
  `tokio::time::sleep` are all compile errors here.
- **Verify after every dependency change**: `cargo tree -e features -p
  animus-node` must show no `prod` feature anywhere in the tree. A
  dependency that enables `animus-env`'s `prod` feature for its own reasons
  (a `[dev-dependencies]` entry doesn't count — dev-dependency features
  never apply to a library build of that crate) would make the whole
  boundary decorative; treat that as a real finding, not something to
  route around quietly. As of rung C1, `animus-control`/`animus-cp-data`
  only enable `prod` in their own `[dev-dependencies]` (their real-thread
  `ProdEnv` liveness tests), which does not propagate to a downstream
  library build — confirmed clean by the rung C1 `cargo tree` check.

## What's here (rung C1)

- **`wire`** (re-exported at the crate root) — `ClientRequest`/
  `ClientResponse`: the length-prefixed-JSON wire protocol between a client
  and a node, and between nodes for forwarding (ADR 0017 #3b). `Surface`/
  `surface_of`: which listener(s) may receive a given `ClientRequest`
  variant bare (ADR 0047) — an exhaustive `match`, no wildcard arm, so a new
  variant is a compile error here until classified. `is_relayable_command`:
  whether a `MetaCommand` may ride the `ProposeSchema` relay envelope —
  **also** an exhaustive `match` as of this rung (previously a `matches!`,
  which has no exhaustiveness requirement; see "hardening" below). Plus the
  plain-data types these embed: `KindWriteOp`, `PendingKindWrite`,
  `TxnTableWrite`, `TxnPrecondition`, `TxnWriteCondition`. All ordinary
  serde data — none embeds `ProdEnv`, `CpGroup`, `RaftNode`, `RaftKvNode`,
  `LsmEngine`, or a tokio type.
- **`topology`** — pure CP-route resolution: `tablet_for_key`,
  `decide_cp_route`/`RouteDecision`, `format_not_leader_refusal`/
  `parse_not_leader_refusal`. The pure decision behind `animusd`'s
  `ClientCtx::resolve_cp_route`, which gathers the (real, `ProdEnv`-backed)
  inputs and executes the decision.
- **`decide`** — the pure predicates ADR 0061 rung A6 lifted out of
  `animusd`'s `impl ClientCtx`: `frozen_refusal`, `confirm_wait_is_futile`,
  `read_should_retry`, `ok_or_err`, `align_split_key`,
  `byte_weighted_median`, `other_tablet_replica_addr`/
  `decide_forward_retry`/`ForwardRetryStep`. Every function takes plain
  values (no `&self`, no `&CpGroup`, no `ProdEnv`) and returns a plain
  value.

`animusd::lib` re-exports everything in this crate's public surface at its
own crate root (`pub use animus_node::{ClientRequest, ClientResponse, ...,
decide, topology};`), so the ~500 pre-existing call sites across that
crate — `KindWriteOp` in `dynamo.rs`, `topology::decide_cp_route` in
`lib.rs`, `decide::frozen_refusal` throughout, etc. — keep compiling
unchanged. A handful of items that were `pub(crate)` inside `animusd`
(everything in `topology`/`decide`, `PendingKindWrite`/`TxnTableWrite`'s
fields) had to widen to `pub` here, purely mechanically: `pub(crate)` scopes
to *this* crate now, and `animusd`'s own re-export/struct-literal
construction sites are a different crate. This is not a design change —
nothing outside the `animus-node`/`animusd` pair is meant to consume these
types; the visibility widening only reflects where the crate boundary now
sits.

## What has NOT moved yet

`ClientCtx` (the struct and its `impl` blocks), `handle_request`, and
`cp_serve_forwarded` are all still in `animusd`, unchanged — they move in
rung C5 (the heaviest rung: split into `read_path`/`write_path`/
`txn_coordinator`/`forwarding`/`schema`, genericized over `E: Env`). Rungs
C2–C4 land the leaf background loops (`ttl_reaper`, `backup_janitor`,
etc.), `ControlHandle<E>` + relay/forwarding onto the multiplexed
`Network`, and the HTTP wire edges, in that order — see ADR 0061's Phase C
rung table for the full sequence.

**One cross-crate discipline gap this creates until C5**:
`cp_serve_forwarded`'s gating match (which internal-only `ClientRequest`
variant gets real handling when unwrapped from `Forwarded`) still lives in
`animusd`, while its input type (`ClientRequest`) lives here. The root
`CLAUDE.md`'s "grep every gating match site when a replicated/forwarded
command enum gains a variant" lesson therefore spans a crate boundary for
as long as this split lasts — a reviewer adding a `ClientRequest` variant
here must still go find `cp_serve_forwarded` in `animusd::lib` by hand;
there is no compiler link between the two crates for this one axis (unlike
`surface_of`, which *is* exhaustive and lives with the type it classifies).

## The `is_relayable_command` hardening (rung C1)

Previously `matches!(command, A{..} | B{..} | ...)`. `matches!` compiles to
a `match` with an implicit `_ => false` arm, so it has **no exhaustiveness
requirement** — a new `MetaCommand` variant silently defaults to "not
relayable" with zero compiler signal, exactly the bimodal per-process flake
the root `CLAUDE.md` warns about (a `ProposeSchema` relay that only ever
fails on a follower-connected node, working fine everywhere else). Rewritten
here as an exhaustive `match` with **no `_ =>` arm**: adding a `MetaCommand`
variant in `animus-control` is now a compile error in this function until
someone deliberately classifies it true or false. `wire::tests::
classification_is_pinned` pins every variant's classification as of this
rung (constructed directly, not through helper builders, so the test reads
as the same allowlist the function's doc describes) — a future edit that
changes a variant's classification shows up as a diff in that test, not a
silent behavior change.

## Working here

- Same determinism discipline as the rest of the workspace (root
  `CLAUDE.md`'s "load-bearing constraint" section) — this crate is squarely
  inside the `Env` seam's target zone, not one of the sanctioned exceptions.
  `clippy.toml`'s `disallowed_types`/`disallowed_methods` apply at the
  workspace default here (no package-level override, unlike `animusd`).
- Everything added here must stay `E`-generic or fully pure — no `&self`
  methods on a concrete `ProdEnv`-backed handle, no direct socket/clock/RNG
  access. If something genuinely needs `ProdEnv`, it belongs in `animusd`
  until (or unless) it can be expressed over `E: Env` instead.
- `cargo test -p animus-node` runs this crate's own unit tests directly
  (fast — no real sockets, no `SimEnv` cluster harness yet; that's ADR 0061
  Phase D's `SimCluster`). `cargo test -p animusd --lib` and a representative
  slice of `animusd`'s real-socket integration suite are still the
  regression net for the wiring — see `animusd/CLAUDE.md`.
