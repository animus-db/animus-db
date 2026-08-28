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

## What's here (rung C2)

ADR 0061 Phase C's original plan called the six background loops
(`ttl_reaper`, `backup_janitor`, `pitr_janitor`, `segment_janitor`,
`backup_completion`, `index_backfill`) the easy first movers of Phase C.
Scoping this rung found that premise wrong (see the ADR's second
2026-08-28 amendment): every one of them takes `animusd`'s `ClientCtx` —
the 5,569-line brain rung C5 moves last — so on the original ordering
nothing in C2 could move at all. The fix is dependency inversion, not
reordering: a small set of **host-capability traits**, below, each naming
just the slice of `ClientCtx`'s surface one or more loops actually use.

- **`host`** — three traits, chosen by capability rather than by loop, so a
  loop's own generic bound is the intersection of only what it needs:
  - **`ControlLeaderHost<E>`** — one method, `control_leader(&self) ->
    Option<RaftNode<E>>`. Every moved loop needs this (each self-gates its
    own tick on "am I the control-plane leader right now," `animusd`'s
    "run everywhere, self-gate" spawn pattern). It hands back a whole
    `RaftNode<E>` rather than exposing `metadata()`/`propose()` as two
    separate trait methods, because `animus_control::RaftNode<E>` is
    **already `E`-generic** — no `ClientCtx` involved at all — so wrapping
    it a second time would add a layer with no benefit.
  - **`BackupObjectStore`** — four methods (`backup_put`/
    `backup_list_local`/`backup_delete_local`/`backup_delete_at`) over the
    backup object store, each returning `None` on a control-only leader
    (mirroring `ClientCtx::data_opt()` returning `None` there). Not
    `E`-generic — nothing in its signatures is `Env`-typed. Used by
    `backup_completion` (`put` only), `backup_janitor` (`list_local`/
    `delete_local` only), and `pitr_janitor` (`delete_at` only) — each
    loop's own generic bound names only the methods it calls, but they
    share one trait because they are one cohesive capability (durable
    backup-object I/O), and `ClientCtx` implements all four anyway (it is
    the one host), so splitting further would buy nothing.
  - **`TtlScanHost`** — four methods (`ttl_metadata`/`led_tablets`/
    `scan_base_capped`/`ttl_delete_if_attribute_equals`) scoped to exactly
    what `ttl_reaper` needs: a `Metadata` read, which tablets this node
    leads, a pure local non-waking scan, and one conditional-delete write.
    The write method is the widest one in this rung — it delegates to
    `animusd`'s full ADR 0049 kind-write machinery (OCC seatbelt, GSI/LSI/
    change-log/stream side effects) behind one call, rather than exposing
    any of that as separate capabilities, because none of it is a decision
    the *loop* makes; the loop only decides *which* item is expired and
    *when* to look.

  Every trait method `animusd`'s `ClientCtx` implements
  (`client_ctx_host.rs`) is a **thin, logic-free delegation** to an
  already-existing method — nothing new was decided when adding these
  impls, only translated. See each trait's own doc for the full per-method
  contract.

- **`index_backfill`**, **`ttl_reaper`**, **`backup_completion`**,
  **`backup_janitor`**, **`pitr_janitor`** — five of the six loops, moved
  verbatim (same pacing, same ordering, same decisions — `tokio::time::
  sleep`/`Instant::now` became `env.sleep()`/`env.now()`, `tokio::spawn`
  callers are unaffected since `animusd` still owns every spawn site).
  Each module's own doc carries the full per-loop design (unchanged from
  `animusd/CLAUDE.md`'s prior entries, now the canonical copy — that
  crate's own entries point here). `animusd`'s own modules of the same
  name are now thin wrappers: clone `ctx.env`, call into here with `ctx`
  itself as the trait-implementing host, thread through any interval/
  duration parameters unchanged. Every pre-existing `animusd` call site
  (`ttl_reaper::ttl_reaper_loop(ctx.clone(), interval)`, etc.) compiles and
  behaves identically.

- **`segment_janitor` did NOT move** — the one loop this rung leaves in
  `animusd`, with reasons specific enough to act on rather than a vague
  "too hard": its replica-repair phase reads live bytes from whichever of
  a row's recorded replicas are still `Active` cluster members and pushes
  them to freshly-chosen targets via `SegmentStoreHandle::repair`
  (`animus_cp_data::cluster_segment_store::ClusterSegmentStore`'s own
  placement choice) — real orchestration logic over cluster membership and
  replica selection, not a value this rung's I/O-delegation pattern can
  capture as one narrow method the way `BackupObjectStore::backup_put`
  captures "durably store these bytes somewhere." Its orphan-reap phase
  layers an age-gate on top of that same repair machinery. Forcing this
  loop to move under the established pattern would mean either dragging
  real placement-decision logic into `animus-node` (the C5-shaped work
  this rung is explicitly not) or building a capability surface wide
  enough to expose that logic anyway — a trait that exists only to make
  the move happen, the exact "contorted trait" ADR 0061's C2 amendment
  warns against. It stays in `animusd`, `#[allow(dead_code)]`-free and
  otherwise untouched, a candidate for a later rung once C5 has landed
  `ClientCtx`'s own split (at which point the repair/placement logic may
  belong in `animus-placement`-adjacent code rather than behind a
  `TtlScanHost`-shaped seam here at all).

## What has NOT moved yet

`ClientCtx` (the struct and its `impl` blocks), `handle_request`, and
`cp_serve_forwarded` are all still in `animusd`, unchanged — they move in
rung C5 (the heaviest rung: split into `read_path`/`write_path`/
`txn_coordinator`/`forwarding`/`schema`, genericized over `E: Env`). Rung
C2 (above) landed five of the six leaf background loops behind narrow host
traits; `segment_janitor` stays in `animusd` (see rung C2's own entry for
why). Rung C3 (above) landed the client-frame codec's pure half, the
`RelayClient` capability trait, and `ControlHandle`/`RemoteControlClient`
themselves — but **not** `ClientCtx::relay` or any call site reaching relay
*through* `ClientCtx` (`propose_schema`, `cp_serve_forwarded`'s
forwarding), which stay `ClientCtx`-entangled and move with it in C5. C4
lands the HTTP wire edges — see ADR 0061's Phase C rung table for the full
sequence.

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

## What's here (rung C3a/C3b/C3c)

ADR 0061 Phase C's original one-line plan for C3 ("`ControlHandle<E, R>` and
a `RelayClient` capability trait") turned out to be four sub-rungs once
scoped (the third 2026-08-28 amendment) — a literal "move relay onto
`Network`" was rejected outright (it would collapse ADR 0047's `intra`
port into `internal`, a production wire-topology change this ADR
disclaims). This crate implements C3a–C3c; **C3d (a `Network`-backed,
`req_id`-correlated, sim-only `RelayClient`) is deliberately deferred to
Phase D** — nothing here builds request/reply correlation.

- **`codec`** (C3a) — the **pure** half of the length-prefixed client-frame
  wire codec: `MAX_FRAME_LEN` (64 MiB), `encode_client_frame` (serde_json
  encode + length-prefix + bound check, returning the full framed byte
  vector), `frame_payload_len` (validates an already-read `u32` length
  prefix against the bound before any allocation), and
  `decode_client_frame` (serde_json decode). No socket type anywhere in
  this module — `animusd`'s `write_frame`/`read_frame` keep the actual
  `TcpStream` reads/writes and call straight into these for everything
  that doesn't touch a socket. Unit-tested directly here (round trip,
  `MAX_FRAME_LEN` rejection on both the encode and the declared-length
  side, a malformed-JSON decode error) — `animusd`'s own wire-edge tests
  are still the real-socket regression net for the wrapper functions
  themselves.
- **`host::RelayClient`** (C3b) — a synchronous call/await RPC capability,
  beside C2's three host traits: `async fn relay(&self, addr: String,
  request: &ClientRequest, timeout: Duration) -> ClientResponse`. Returns
  `ClientResponse` directly, never a `Result` — mirrors `animusd`'s
  pre-existing `relay_request`, which already folds every transport
  failure into `ClientResponse::Error(..)`, so no caller needs a new
  failure shape. **No default method, and the trait itself enforces no
  timeout** — `tokio::time::timeout` cannot live in this crate at all (no
  `tokio` dependency), so every implementor supplies its own enforcement;
  `animusd`'s does, wrapping its own unchanged `relay_request_with_timeout`.
  See the trait's own doc for why `Network` (ADR 0026) doesn't fit relay
  (no request/response correlation, and a different port) — the full
  argument lives in ADR 0061's third 2026-08-28 amendment.
- **`control_handle`** (C3c) — `ControlHandle<E: Env, R: RelayClient>`
  (`Local(RaftNode<E>)` / `Remote(RemoteControlClient<R>)`) and
  `RemoteControlClient<R>`, moved here **whole** from `animusd`. This was a
  clean move, not just a generic-ification-in-place: every field on both
  types was already plain data (`Vec<String>`, `Arc<Mutex<..>>`,
  `MetadataWatch`, `MetricsHandle`, plus `Metadata`/`RaftNode`/`Role` from
  `animus-control`, all already `E`-generic or `ProdEnv`-free) — the *only*
  method doing real I/O, `RemoteControlClient::metadata_fresh`, used to
  call `animusd`'s free `relay_request`; it now calls `R::relay` on a
  generic field instead, so nothing here needed a socket type after all.
  `RemoteControlClient` additionally carries `relay: R` and `timeout:
  Duration` fields — the timeout is a constructor parameter, not a
  constant duplicated in this crate, since only the host crate
  (`animusd`'s `CLIENT_TIMEOUT`) knows the value it wants.
  `animusd`'s own `control_handle.rs` is now a thin wrapper: two type
  aliases binding `E = ProdEnv`, `R = AnimusdRelayClient` (a zero-sized
  `RelayClient` implementor wrapping the crate's unchanged
  `relay_request_with_timeout`), plus that implementor itself — see that
  crate's own `CLAUDE.md` entry.

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
- `cargo test -p animus-node` runs this crate's own unit tests plus, since
  rung C2, a handful of real `SimEnv` integration tests
  (`tests/index_backfill_sim.rs`, `tests/ttl_reaper_sim.rs`, and the
  in-module `backup_janitor::tests`) driving a moved loop against either a
  tiny single-voter `RaftNode<SimEnv>` (no sockets, no multi-node cluster)
  or a fully synthetic fake host (no `RaftNode` at all) — fast and
  deterministic, but **not** the multi-node `SimCluster` harness ADR 0061
  Phase D still has to build; a `[dev-dependencies]`-only `animus-sim`/
  `animus-storage` pair backs these (verified not to reintroduce the
  `prod` feature into this crate's *library* build — `cargo tree -e
  features -p animus-node` with no `--tests`/`--all-targets` stays clean).
  `cargo test -p animusd --lib` and a representative slice of `animusd`'s
  real-socket integration suite are still the regression net for the
  wiring — see `animusd/CLAUDE.md`.
