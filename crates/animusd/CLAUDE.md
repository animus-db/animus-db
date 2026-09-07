# CLAUDE.md — animusd

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The runnable AnimusDB node server — a **lib + bin**. `lib.rs` assembles a node
over `ProdEnv` (the first real use of the production seam): a control-plane Raft
(`animus-control`) for cluster metadata plus the CP data plane (`animus-cp-data`,
one leaderful Raft group per tablet) for linearizable reads/writes, fronted by
three wire edges (DynamoDB JSON/HTTP, a plain length-prefixed TCP client
protocol, and an admin/debug HTTP-JSON port with a web console). `main.rs` is a
thin CLI wrapper. `animus-cli` depends on this crate for the client protocol
types. v1 (ADR 0019) is **CP-only**; the leaderless AP `data`/`coord` roles are
gone. v1 is also **DynamoDB-only** (ADR 0053) — a CQL wire edge shipped for a
time and was dropped; retrievable from git history if ever revived.
Streams implementation notes: [`docs/streams-notes.md`](../../docs/streams-notes.md).

**This crate is exempt from the `disallowed_methods` half of the
determinism lint (ADR 0061 rung B5)** — `Cargo.toml`'s own `[lints.clippy]`
turns it off package-wide (lib, bin, every `tests/*.rs`, the bench), with
the reasoning in that file's comment: real time/`tokio::spawn`/real sockets
are this crate's entire job pre-Phase-C, and ~600 real call sites across 84
files made per-site `#[allow]`s a wall nobody would read rather than a
review aid. `disallowed_types` (HashMap/HashSet) is untouched and still
enforced here. This is documented, ADR-tracked debt, not a license to add
more real-time/spawn logic carelessly, and new code here should still
prefer `Env` methods where an `Env`-generic home is plausible.

**The exemption is no longer crate-wide.** ADR 0061 Phase C's closing rung
(the seventh 2026-08-28 amendment) put an explicit
`#[deny(clippy::disallowed_methods)]` on the `mod` declarations of the five
`E: Env`-generic client-path modules in `lib.rs` — `schema`, `read_path`,
`write_path`, `txn_coordinator`, `forwarding`. A lint attribute on a `mod`
declaration applies to that module's whole body, so the package-level allow
is overridden for all five: a reintroduced `Instant::now`, `tokio::spawn`
or `tokio::time::{sleep,timeout}` in any of them is a hard **build
failure**, not a review miss. (Verified as a negative control when the rung
landed, not assumed: a temporary `Instant::now()` added to `read_path.rs`
produced `error: use of a disallowed method` without even needing
`-D warnings`.)

This deny is what makes the determinism constraint compiler-enforced for
the node's brain. ADR 0061 Decision 1 originally expected that enforcement
from a crate boundary — moving `ClientCtx` into `animus-node` — and the
orphan rule blocked it (the sixth amendment); lint scope turned out to be a
real boundary too, and a cheaper one. The package-level allow now covers
only what genuinely is the process boundary: `lib.rs` itself, `dynamo.rs`,
the wire edges, the remaining background loops, and the test/bench targets.

**Narrow it further as more of this crate becomes seam-clean; never widen
it back to make a change compile** — that is precisely the hole this rung
closes. A module that has no live `tokio`/real-clock sites left earns its
own `#[deny(...)]` line. Five more leaf background-loop wrappers earned
theirs this way: `backup_completion`, `backup_janitor`, `index_backfill`,
`pitr_janitor`, `ttl_reaper` — each had its loop body moved to
`animus_node` in rung C2 (see each module's own map entry below) and is now
a thin, logic-free wrapper with zero `Instant::now`/`SystemTime::now`/
`tokio::time::*`/`tokio::spawn` sites of its own, verified by direct
inspection before the `#[deny(...)]` line was added. `segment_janitor`
deliberately did **not** get one — its replica-repair phase is real
placement/membership orchestration (a live `tokio::time::sleep`), not a
thin delegation — and stays under the package-level allow along with
`admin`, `backup_capture`, `backup_restore`, `client_ctx_host`, `console`,
`control_handle`, `dashboard`, `dynamo`, `dynamo_streams`, `http`, and
`import` (ADR 0068 §6, S-05 PR 2 — a real per-tablet `tokio::time::sleep`
loop, `backup_restore`'s own twin, not a thin delegation either). Ten
modules now carry the narrower `#[deny(...)]`: the original five (`schema`,
`read_path`, `write_path`, `txn_coordinator`, `forwarding`) plus these five.

**`lib.rs` is ~11,800 lines** (down from ~17,300 before ADR 0061 rung C5
step 2 split `impl<E: Env> ClientCtx<E>` into `schema.rs`/`read_path.rs`/
`write_path.rs`/`txn_coordinator.rs`/`forwarding.rs`, below) — grep for the
symbol, don't scroll. It also holds
in-crate `#[cfg(test)] mod`s that need private handles the `tests/` tree
can't reach — e.g. `confirm_futility_tests`
(issue #268 — the confirm-loop fast-fail regression, needing a raw
`CpGroup` + the `pub(crate)` `ClientCtx::cp_kind_eval_local` (retargeted
from the now-deleted `cp_kind_local`, ADR 0054 step 4b); `split_fence_tests`
and `hot_read_latch_tests` were deleted with their fence/latch subjects in
the ADR 0050 Train B rung-7 sweep). `kind_batch_signal_tests` is the newest
of these, but for a different reason than the others — it needs no bring-up
at all, only the private `classify_kind_batch_outcome`/`KindBatchSignal`
symbols (the pure predicate `poll_probe` calls; see that method's doc and
`docs/engineering-lessons.md`'s PR #334 entry for why an index-keyed
apply-time outcome needed a term check added). `index_drain.rs` has
another, `gsi_drain_cursor_tests`, and `dynamo.rs` another,
`stream_write_path_tests` (ADR 0042), for the same reason (see each file's
own entry below).

**Every in-crate bring-up retries the port-TOCTOU race (issue #278 item
3).** Since these mods can't reach `tests/support`, each hand-rolls its own
`free_addrs`/`single_node`-shaped fixtures — and each must independently
carry the bounded fresh-config retry documented in
`docs/engineering-lessons.md` (the same idiom every surviving `tests/
*_split*.rs`'s own `bring_up`/`bring_up_inplace` uses), or it panics
`AddrInUse` under `cargo test --workspace` contention. A
same-address restart (`gsi_drain_cursor_tests::
crash_mid_reconcile_recovers_without_skipping_or_corrupting_the_gsi`) instead
retries the rebind itself on a bounded deadline, mirroring
`tests/support/mod.rs::restart_same_addrs` — it can't reallocate ports since
reusing the captured config is the point of the test.

## Module map (`src/`)

- **`lib.rs`** (~11,800 lines) — the node assembly: `Node`/`BoundNode`/
  `BoundControlNode`/`BoundDataNode`, `ClientCtx`'s struct definition and
  its `DataRole`/`CpGroup`/`CpRoute`/`ClusterEdgeState`/`SharedEngine`
  neighbors, the tablet-host reconciler and auto-split loops, the admin/
  metrics slice of `impl ClientCtx` (below), and the `ClientRequest`/
  `ClientResponse` wire types (re-exported from `animus-node`, above).
  `ClientCtx`'s other five method clusters live in their own files, listed
  next — see the sections below for the parts worth a contract.
- **`schema.rs`**/**`read_path.rs`**/**`write_path.rs`**/
  **`txn_coordinator.rs`**/**`forwarding.rs`** (ADR 0061 rung C5 step 2) —
  `impl<E: Env> ClientCtx<E>` split by concern, each file its own inherent
  `impl` block for the same type: schema-catalog DDL + tablet provisioning
  + split trigger + force-seal (`schema.rs`), linearizable + ADR 0055
  eventually-consistent reads (`read_path.rs`), kind-scoped writes +
  `poll_probe`'s confirm loop (`write_path.rs`), the 2PC coordinator
  (`txn_coordinator.rs`), and leader routing + one-hop forwarding +
  `cp_serve_forwarded`'s top-level dispatch (`forwarding.rs`, moved last
  since it calls into every other cluster by name). See this file's own
  ADR 0061 rung C5 step 2 entry, below, for the full method-by-method
  breakdown, the visibility-widening lesson, and what deliberately stayed
  in `lib.rs` instead.
- **`main.rs`** — thin CLI wrapper; dispatches the invocation modes (below) and
  wires `otel::init_tracing` + the Ctrl-C graceful-shutdown path.
- **`config.rs`** — `ClusterConfig`/`RoleAddrs` (per-process deployment
  config; every entry names its own **`id: NodeId`** rather than deriving
  it from position — `from_json` hard-errors on a duplicate) and the
  **six-port stride** (ADR 0047 + ADR 0052 + ADR 0053: `base_port + 6*i +
  {internal,client,dynamo,admin,intra,console}` — `intra` at offset 4
  (the client/intra-cluster RPC port split), `console` at offset 5 (animusd
  console, ADR 0052's "AnimusDB Data Console" — a DynamoDB-shaped data app on its own
  port, deliberately separate from the operator dashboard the admin port
  serves; bound on combined/data-only nodes, never control-only, which
  hosts no CP-data tablet). `generate`/`generate_split` mint `"n{i}"`,
  **zero-padded** once the cluster has ≥ 10 nodes so lexicographic id order
  stays == numeric index order (`"n10" < "n2"` otherwise) — below that
  threshold ids stay the plain unpadded `"n{i}"` every existing test
  already assumes. **A future 8th port**: add the field to `RoleAddrs`
  first (no default), then let `cargo build -p animusd --all-targets`'s
  `error[E0063]: missing field` output enumerate every one of the ~60
  literal construction sites across `src/`+`tests/` — don't trust a grep
  pass to have found them all (see `docs/engineering-lessons.md`'s
  2026-08-19 "Code patterns" entry on this exact port addition). **Removing
  a field is the mirror**, and a regex-driven fixup script across the
  construction sites carries its own silent-corruption risk — see the
  2026-08-22 "Code patterns" entry (ADR 0053's `cql` port removal) on a
  field-name-keyed regex mangling unrelated `admin::Type`/`console::Type`
  module-path expressions into invalid syntax; always full-build immediately
  after, not just visually diff. **`dynamo_auth: Option<DynamoAuthConfig>`
  (ADR 0057)** is the client DynamoDB port's SigV4 credential store —
  `#[serde(default)]` so an absent section (every existing config)
  deserializes as `None`, but `ClusterConfig` itself derives no `Default`,
  so adding this field hit the exact same `error[E0063]` fan-out the port
  additions above describe: every `ClusterConfig { .. }` **literal**
  (`generate`/`generate_split`, and every `src/`+`tests/` fixture that
  builds one by hand rather than through those two constructors) needed
  `dynamo_auth: None,` added by hand, compiler-enumerated. `DynamoAuthConfig
  { credentials: BTreeMap<String, String> }` (`access_key_id →
  secret_access_key`) — `ClusterConfig::from_json` calls
  `DynamoAuthConfig::validate()` on a present section (empty credentials is
  a load-time error, the same `serde_json::Error::custom` idiom the
  duplicate-node-id check uses). Threaded from there down to `ClientCtx::
  dynamo_auth` (an `Option<Arc<BTreeMap<String, String>>>`, cheap to clone
  onto each connection) via `spawn_common_tail`'s own new trailing
  parameter — every `start_with_growth`/`start_data_with_growth` /
  `start_cluster_inner` layer that assembles `ClientCtx` gained the same
  trailing `dynamo_auth` knob, mirroring the `quiesce_after`/
  `ttl_sweep_interval` layered-wrapper convention: outer wrapper methods
  default it to `None`, and a caller that needs it set
  (`run_node_with_streams_quiesce_and_ttl_sweep_interval` reading
  `ClusterConfig::dynamo_auth`, `run_node_data`/`run_node_data_join`'s data-
  only duals, `main.rs`'s `--dynamo-auth`-fed config-less paths) calls the
  innermost layer directly rather than growing every wrapper's arity.
  `BoundControlNode::start_control_with` hardcodes `None` at its own
  `spawn_common_tail` call — a control-only node never binds the dynamo
  listener, so nothing there would ever read the field.
- **`cluster_settings: Option<ClusterSettings>` (S-06)** — cluster-wide
  operational knobs (auto-split, quiesce, orphan-sweep, stream-seal)
  reachable from a config file on every real deployment shape
  (`--config`/`--node`, `animusd control`, `animusd data --config`), not
  just `--cluster N`'s dev-only in-process CLI flags — the gap the root
  `CLAUDE.md`'s auto-split entry and ADR 0034/0040/0048's amendment notes
  describe. `ClusterSettings`'s seven fields mirror their CLI flags
  field-for-field (a `_secs` field is the flag's raw seconds value, never a
  `Duration`), each independently `#[serde(default)]`, and its own doc
  comment has the full per-field applicability table (a data-only node
  ignores `orphan_sweep_after_secs`/`stream_retention_secs`; a control-only
  node acts on `orphan_sweep_after_secs` alone). `main.rs`'s
  `resolve_cluster_settings` merges this section against whatever CLI flags
  the invocation actually gave, field by field — the same "specify it one
  way, not both" hard-error contract `apply_dynamo_auth_flag` uses for
  `dynamo_auth`, not a silent precedence rule; the merge is per-field, so an
  operator may still set *different* knobs on each side. This is
  `--config`/`--node`'s and `animusd data --config`'s only route to
  auto-split at all (previously reachable solely via `--cluster N`), and
  the config-file route that finally lets `animusd data --config` reach
  quiescence too (see the Quiescence section below, and `start_data_with_
  growth`'s own new `quiesce_after` parameter). Adding this field hit the
  identical `error[E0063]` fan-out `dynamo_auth` describes just above —
  ~55 `ClusterConfig { .. }` literals across `src/`+`tests/` needed a
  `cluster_settings: None,` line, again compiler-enumerated rather than
  grepped.
- **`control_handle.rs`** — the `ControlHandle` seam (ADR 0035 PR1):
  `Local(RaftNode<ProdEnv>)` for a node with real control Raft, vs.
  `Remote(RemoteControlClient)` for a data-only node reaching a separate control
  deployment over the network. `metadata_cached()` vs. `metadata_fresh()`
  freshness contract lives here. **`ControlHandle`/`RemoteControlClient`
  themselves moved to `animus_node::control_handle` whole (ADR 0061 rung
  C3c)**, genericized over `E: Env`/`R: RelayClient` — this file is now
  just two crate-local type aliases (`ControlHandle = animus_node::
  control_handle::ControlHandle<ProdEnv, AnimusdRelayClient>`, and
  `RemoteControlClient`'s dual) plus `AnimusdRelayClient`, the zero-sized
  `animus_node::host::RelayClient` implementor `RemoteControlClient::
  metadata_fresh` relays its `Status` fetch through — a thin wrapper over
  this crate's own **unchanged** `relay_request_with_timeout` (still a
  fresh `TcpStream` dial on the `intra`/`client` ports, still
  `tokio::time::timeout`-bounded, which stays here since `animus-node`
  cannot name it at all — no `tokio` dependency, and its `disallowed_
  methods` lint would refuse the call even if there were). Every
  pre-existing `ControlHandle`/`RemoteControlClient` call site in
  `lib.rs`/`admin.rs` compiles unchanged against the aliases; the two real
  `RemoteControlClient::new`/`with_mirror` construction sites in `lib.rs`
  gained two new arguments (`AnimusdRelayClient`, `CLIENT_TIMEOUT`), since
  the constructor is now generic over the relay implementor and takes its
  own transport timeout explicitly (only this crate knows that value —
  `animus-node` doesn't duplicate the constant). See `animus-node/
  CLAUDE.md`'s own C3a/C3b/C3c entry for the full design, including why
  the move was clean (every field on both types was already plain data or
  `E`-generic) rather than a generic-ification-in-place.
- **`write_frame`/`read_frame`** keep their `TcpStream` signatures here
  (ADR 0061 rung C3a) but now call straight into `animus_node::codec`'s
  pure `encode_client_frame`/`frame_payload_len`/`decode_client_frame` for
  the length-prefix arithmetic, the `MAX_FRAME_LEN` bound check, and the
  `serde_json` encode/decode — only the actual socket reads/writes stay
  here. `MAX_FRAME_LEN` itself is now `pub use animus_node::MAX_FRAME_LEN`
  (re-exported at this crate's root, so every existing
  `crate::MAX_FRAME_LEN`/`animusd::MAX_FRAME_LEN` reference kept compiling
  unchanged).
- **`topology`/`decide` moved to `animus-node`** (ADR 0061 rung C1) — pure,
  side-effect-free routing decisions (`decide_cp_route`, `tablet_for_key`,
  `format_not_leader_refusal`/`parse_not_leader_refusal`) and decision
  predicates (`frozen_refusal`, `confirm_wait_is_futile`,
  `read_should_retry`, `align_split_key`, `byte_weighted_median`,
  `other_tablet_replica_addr`/`decide_forward_retry`), respectively — moved
  verbatim into the `E: Env`-generic `animus-node` crate, visibility widened
  `pub(crate)` → `pub` since a crate boundary now sits where an in-crate
  module boundary used to. `lib.rs` re-exports both at this crate's own
  root (`pub use animus_node::{decide, topology};`), so every existing
  `topology::decide_cp_route`/`decide::frozen_refusal` call site kept
  compiling unchanged. `decide`'s predicates (originally lifted out of
  `impl ClientCtx` by ADR 0061 Phase A rung A6) take primitive facts
  (`is_frozen: bool`, `engine_applied_index: u64`, `is_leader: bool`)
  rather than `&CpGroup` — the caller in `lib.rs` still reads those fields
  off the real `ProdEnv`-backed handle immediately before calling in, since
  `CpGroup` can't be constructed without bring-up. `confirm_futility_tests`
  (in-crate here, real-socket, `#[tokio::test(flavor = "multi_thread")]`)
  deliberately stays in `lib.rs` rather than moving alongside
  `confirm_wait_is_futile`: it proves the wired end-to-end fast-fail
  behavior through a real `CpGroup` propose/apply/poll round trip with
  timing assertions, not the predicate in isolation — moving it would have
  broken `animus-node`'s "no bring-up" invariant for no benefit, since
  `decide::confirm_wait_is_futile` already has its own direct truth-table
  unit tests there. See `animus-node/CLAUDE.md` for the full module docs,
  now maintained there instead of here.
- **`ClientRequest`/`ClientResponse`/`Surface`/`surface_of`/
  `is_relayable_command`, plus the plain-data types they embed
  (`KindWriteOp`/`PendingKindWrite`/`TxnTableWrite`/`TxnPrecondition`/
  `TxnWriteCondition`), moved to `animus-node` too** (ADR 0061 rung C1,
  same PR as the `topology`/`decide` move) — re-exported at this crate's
  root the same way, so `dynamo.rs`'s `use crate::{ClientCtx, CpGroup,
  KindWriteOp, ...}` and every other bare/`crate::`-qualified reference
  kept compiling unchanged. `is_relayable_command` was also rewritten from
  a non-exhaustive `matches!` to an exhaustive `match` in the same move —
  see `animus-node/CLAUDE.md`'s own entry on that hardening.
  `ListenerKind` (the listener-*identity* type, distinct from `Surface`'s
  reachability *classification*) stayed here — it is `ProdEnv`-adjacent
  (which real socket a connection came in on), not pure. `ClientCtx` and
  `handle_request` have **not** moved (rung C5 step 3, not yet done) —
  `cp_serve_forwarded` (now in `forwarding.rs`, see below)'s gating match
  takes a type (`ClientRequest`) defined in a different crate; see
  `animus-node/CLAUDE.md`'s note on why "grep every gating site" now spans
  that boundary until step 3. **Hardened (a small follow-on to C1, independent of
  C5)**: the match is now exhaustive — every `ClientRequest` variant that
  reaches no real handling above is named explicitly in one final arm
  (grouped by why each is never a legitimate forwarded payload), replacing
  the `_ => ClientResponse::Error("unexpected forwarded request")`
  wildcard that used to catch a missed variant with zero compiler signal,
  the exact hazard the root `CLAUDE.md`'s "grep every gating match site"
  warning describes. A future 29th `ClientRequest` variant is now a
  compile error here until someone deliberately gives it a real arm or
  adds it to that final list — the cross-crate *grep* is still required
  (nothing links the two crates' files together), only the *silent-miss*
  failure mode is gone. Regression:
  `tests/intra_port_split.rs::cp_serve_forwarded_refuses_every_never_forwarded_variant`
  (a live single-node cluster, since building a bare `ClientCtx` outside a
  real bring-up isn't practical here — see this crate's own Tests section).
- **`CpGroup`/`SharedEngine`/`ClusterEdgeState`/`CpRoute`/`ClientCtx` are now
  generic over `E: Env` (ADR 0061 rung C5 step 1)** — still entirely
  in-crate, nothing moved. Each is `<E: Env = ProdEnv>`: a **default type
  parameter**, not a rename-plus-alias, so every pre-existing bare
  reference across this crate (`spawn_common_tail`'s params, `admin.rs`,
  `dynamo.rs`, the background loops, `tests/`) keeps compiling unchanged —
  the definition-site default is this rung's analogue of the type-alias
  containment C3c used for `ControlHandle`. `ClientCtx`'s own `control:
  ControlHandle` field deliberately stays the crate's fixed `ProdEnv`-bound
  alias (not `ControlHandle<E>`) — nothing in `ClientCtx`'s two `impl`
  blocks reads it through anything `CpGroup`/`SharedEngine`-shaped, so
  genericizing it would add a second, unused generic parameter for no
  benefit. **Gotcha a reviewer will hit immediately**: a default type
  parameter resolves to its **default**, never to an enclosing generic
  scope's own parameter, in any position it is *elided* — inside `impl<E:
  Env> ClientCtx<E>`, a bare `&CpGroup` in a method signature means
  `&CpGroup<ProdEnv>`, not `&CpGroup<E>`, and produces a plain `E0308`
  mismatch against a `CpGroup<E>` value (verified empirically before
  relying on it — see `docs/engineering-lessons.md`). Every signature
  inside `ClientCtx`'s two `impl` blocks that names `CpGroup`/`CpRoute`
  explicitly is therefore spelled `<E>`; **match/pattern positions and
  value construction did not need this** (they infer from the
  already-typed scrutinee/call arguments), which is why the ~60
  `CpRoute::Local(leader)`-shaped match arms across those two `impl`
  blocks needed zero changes. Three call chains cross into sibling
  functions that also had to gain `<E: Env>` for the whole crate to
  compile: `index_drain::{seal_now, pitr_seal_now, hot_read,
  clear_backfill_cursor}`, `dynamo::{kind_write_item_at_leader,
  eval_kind_txn_write, collection_bytes_at_leader}`, and this crate's own
  `median_split_key`. Every one of these seven is a signature-only change;
  no call-site logic moved. `DataRole` (holding `rmw_lock`,
  `segment_store`/`backup_store`, etc.) needed **no** change at all — none
  of its fields are `E`-typed, so it stays fully concrete and
  `ClientCtx<E>.data: Option<DataRole>` is untouched; `kind_write_item_
  at_leader`'s `rmw_lock` acquire/release span (scoped to read+evaluate
  only, issue #285) is unchanged byte-for-byte. `handle_request` and moving
  `ClientCtx` itself out of this crate are still rung C5's remaining work
  (step 3, below the next entry).
- **`impl<E: Env> ClientCtx<E>` split into five submodules (ADR 0061 rung
  C5 step 2)** — `lib.rs`'s two `impl` blocks (6,287 lines, 97 methods per
  the ADR's fifth 2026-08-28 amendment) held every `ClientCtx` method in
  one place; each of the five clusters that amendment identified is now
  its own file, each with its own `impl<E: Env> ClientCtx<E> { .. }`
  block — Rust allows a type's inherent `impl` to be split across modules
  in the same crate, so this was a mechanical, behavior-preserving
  relocation (doc comments, attributes, and bodies moved verbatim; no
  logic changes, no merged/split methods). Moved in the ADR's suggested
  order, one commit per cluster: **`schema.rs`** (18 methods — schema-
  catalog DDL proposals, tablet provisioning/serveability wait, node
  registration, table/tablet drop, split trigger, force-seal, stream
  growth, backfill-cursor clearing: `propose_schema`, `provision_tablet`,
  `await_table_serveable`, `watch_metadata`, `register_node`,
  `drop_table*`, `trigger_split`, `force_seal_tablet`,
  `force_pitr_seal_tablet`, `grow_stream*`, `clear_backfill_cursor*`,
  `read_stream_hot_records`, plus their private helpers); **`read_path.rs`**
  (21 methods — `cp_read`, `cp_read_snapshot`, `cp_scan`/`cp_scan_kind*`,
  and the ADR 0055 eventually-consistent-read fast path:
  `cp_read_eventual*`, `cp_scan_*_eventual`, `cp_stale_local`,
  `cp_stale_forward_target`, `relay_stale_read`, `record_eventual_read`,
  `cp_get_local_resolving`, `confirm_or_push`, `cp_get_local_snapshot`,
  `cp_scan_local`, `cp_scan_kind_local`, `cp_get`); **`write_path.rs`**
  (13 methods as moved, now 11 — `seed_rows_local`/`seed_child_rows` were
  deleted with the copy-based split-build driver in the copy-split-deletion
  endgame's Layer B1: `cp_kind_write_item`, `cp_kind_write_raw*`,
  `cp_kind_local`, `poll_probe`, `cp_batch_local`/`cp_batch_propose`,
  `cp_put_local`/`cp_delete_local`);
  **`txn_coordinator.rs`** (14 methods — the 2PC coordinator:
  `txn_stage_local`, `txn_prepare*`, `txn_decide_anchor`,
  `txn_resolve_participant`, `txn_status`, `txn_record_view`,
  `txn_verify`, `recovery_resolve`, `record_recovery_metric`,
  `split_group`, `check_preconditions`, `txn_recover`, `cp_txn`); and
  **`forwarding.rs`**, moved last per the ADR's own ordering rationale
  since `cp_serve_forwarded` calls into every other cluster by name so its
  callees needed stable homes first (17 methods — `cp_route`,
  `resolve_cp_route`, `tablet_for`, `cp_leader_hint`, `cp_forward_target`,
  `not_leader_refusal`, `other_tablet_replica_addr`, `cp_forward`,
  `forward_to_tablet_leader`, `relay`, `cp_serve_forwarded`, and the
  route/intra-addr accessors `route_addr`/`route_snapshot`/
  `control_leader_hint`/`intra_addr`/`intra_route_snapshot`/
  `intra_control_leader_hint`). Per the ADR's minimal cut, the **admin/
  metrics slice stayed in `lib.rs`** (9 methods: `metrics_text`,
  `metrics_json`, `stream_change_rates`, `metrics_history`,
  `admin_drain`, `admin_add_member`, `admin_remove_member`,
  `admin_add_control_member`, `admin_remove_control_member`) — nothing in
  the DynamoDB wire path or a `SimCluster` reaches them, and they have
  their own real-socket coverage; so did a handful of small,
  genuinely-crate-wide accessors that don't belong to any one cluster
  (`effective_metadata`, `metadata_fresh`, `data` — `data_opt` also lived
  here at the time, since deleted as dead code, W-10 — and
  `not_leader_error`) — moving them into one cluster would only have
  forced the identical `pub(crate)` widening onto them with no locality
  benefit, since `admin.rs`/`dynamo.rs`/`backup_capture.rs`/
  `backup_restore.rs`/`client_ctx_host.rs`/`dynamo_streams.rs`/
  `index_drain.rs` and every one of the five new clusters all call them.
  **The visibility lesson (mechanical, not a design call)**: Rust's
  privacy rule is "visible in the defining module and its descendants,"
  never ancestors or siblings — so a method that used to be a bare `fn`
  (visible everywhere in the crate, since every module here is a
  descendant of the `lib.rs` root) had to widen to `pub(crate)` the
  moment it moved into a child module (`schema`/`read_path`/etc.) *and*
  gets called from a sibling (another one of the five, or `dynamo.rs`/
  `admin.rs`) or from code that stays in the parent (`lib.rs` itself:
  `handle_request`, the admin methods, an in-crate `#[cfg(test)] mod`,
  background-loop free functions). Conversely, a method that stays
  private and lives in `lib.rs` (`CpGroup`'s own methods, free functions,
  constants) needed **no** widening to stay callable from the five new
  child modules — a parent's private items remain visible to every
  descendant, so this direction was already free. 30 methods widened
  from private to `pub(crate)` across the five clusters for this reason
  (see each cluster's own commit message for the exact list and why);
  `docs/engineering-lessons.md` has the general-purpose version of this
  lesson. Each module's `use` list is explicit (traced via `cannot find`
  compiler errors from a temporary `use crate::*;`, then narrowed) rather
  than a blanket glob-import, per this rung's own "keep each module's use
  list tight" instruction. Two extraction-tooling gotchas worth recording
  for anyone repeating this kind of split: a multi-line attribute (e.g.
  `#[tracing::instrument(\n    name = "...",\n)]`) and a single-line
  attribute followed by a trailing `// comment` (`#[allow(clippy::
  too_many_arguments)] // mirrors ...`) both need bracket-aware scanning
  to find their true start when walking upward from a `fn` signature — a
  naive "stop at the first line not starting with `///`/`#[`" heuristic
  leaves the attribute orphaned above the *next* function once the one it
  belonged to is extracted, which compiles as a **different**,
  non-obviously-related error (`error: expected item after attributes`,
  or a phantom `too_many_arguments` clippy failure on the wrong function)
  rather than something that points at the actual mistake.
- **`ClientCtx<E, R>` gains `R: RelayClient` (ADR 0061 rung C5 step 3a, the
  sixth 2026-08-28 amendment)** — `control` was the crate's fixed
  `ProdEnv`/`AnimusdRelayClient`-bound `control_handle::ControlHandle`
  alias; it is now the *generic* `animus_node::control_handle::
  ControlHandle<E, R>`, since `schema.rs`'s `watch_metadata` and
  `forwarding.rs`'s leader routing both read `self.control` from inside a
  `ClientCtx<E, R>`-generic `impl` block. `ClientCtx<E: Env = ProdEnv, R:
  RelayClient = AnimusdRelayClient>` — the same default-type-parameter
  technique step 1 used for `E` alone, so every pre-existing bare
  `ClientCtx` reference keeps compiling unchanged. All **six** `impl`
  blocks (`lib.rs` and the five split modules) became `impl<E: Env, R:
  RelayClient> ClientCtx<E, R>`, not just the two that read `self.control`
  directly — `forwarding.rs` calls into all four siblings by name, and
  `lib.rs`'s own admin/metrics slice is called from several of them, so the
  bound has to be uniform for the call graph to typecheck generically.
  **Three things this rung had to get right that a mechanical
  find-and-replace would have missed**:
  - **The elision gotcha bites the pattern match, not just signatures.**
    `schema.rs`'s `let ControlHandle::Local(raft) = &self.control else {
    .. }` used to match against `crate::ControlHandle` (this crate's own
    concrete alias); inside a `ClientCtx<E, R>`-generic body that alias
    resolves to its own default (`ControlHandle<ProdEnv,
    AnimusdRelayClient>`) and fails to match a `ControlHandle<E, R>`
    scrutinee for generic `E`/`R` — a plain `E0308` mismatch, confirmed
    with a two-line scratch program before touching the real file (see
    `docs/engineering-lessons.md`). The fix imports `animus_node::
    control_handle::ControlHandle` directly (the *generic* enum) under the
    same name, shadowing the crate alias import in that one file — every
    other file keeps using the concrete `crate::ControlHandle` for its own
    (still-concrete) uses.
  - **`RelayClient` needed `Clone + Send + Sync + 'static` added as
    supertrait bounds** (`animus-node::host`), mirroring `Env`'s own
    supertrait shape. Without it, `ClientCtx<E, R>`'s `#[derive(Clone)]`
    and `txn_coordinator.rs`'s one `env.spawn_task` capturing a cloned
    `ClientCtx` (see step 3b below) don't typecheck for a *generic* `R` —
    only the one concrete implementor that exists today
    (`AnimusdRelayClient`, a zero-sized type that already trivially
    satisfied all four). The alternative — bounding just the one `impl`
    block that needs it — was rejected: `txn_status` (needing the bound)
    and `cp_read` (not needing it) live in different files but are called
    from each other's callers' generic scope, so the bound would have had
    to cascade through most of the five-module call graph anyway.
  - **A `Self`-free associated function breaks type inference for `R`.**
    `ClientCtx::cp_kind_local(leader, ..)` (no `&self`, called from three
    sites in `dynamo.rs`/`lib.rs`'s in-crate tests) has nothing in its
    arguments that pins down which `R` to use once `R` is a real generic
    parameter, not a `_`-inferred default — `error[E0283]: type
    annotations needed`. Fixed with an explicit turbofish
    (`ClientCtx::<E, R>::cp_kind_local(..)` inside a generic caller,
    `ClientCtx::<ProdEnv, AnimusdRelayClient>::cp_kind_local(..)` inside a
    concrete-`ProdEnv` test) at each of the three call sites — the compiler's
    own suggested fix. Any other `Self`-free `ClientCtx` associated
    function reached the same way would need the same treatment.
  Four free functions taking `ctx: &ClientCtx<E>` explicitly (mirroring
  step 1's own "three call chains cross into sibling functions" note) also
  gained the `R` parameter: `dynamo::{kind_write_item_at_leader,
  eval_kind_txn_write}`, `index_drain::{pitr_seal_now, seal_now}` — none of
  the four reads `ctx.control`, they just need the signature to match their
  now-`ClientCtx<E, R>`-generic callers in `write_path.rs`/
  `txn_coordinator.rs`/`schema.rs`/`forwarding.rs`.
- **The 91 raw `tokio` sites the sixth amendment counted are converted
  (ADR 0061 rung C5 step 3b)** — every `tokio::time::{Instant::now,sleep}`
  in `schema.rs`/`read_path.rs`/`write_path.rs`/`txn_coordinator.rs`/
  `forwarding.rs` becomes `self.env.now()`/`self.env.sleep(..)` (a bare
  `deadline = tokio::time::Instant::now() + X` becomes `self.env.now().
  saturating_add(X)` — `Nanos` has no `Add<Duration>` impl, only the
  `saturating_add`/`duration_since` shape `animus-cp-data::
  cluster_segment_store`'s own deadline loops already use). Verify with
  `grep -nE "tokio::(time|spawn|select)"` over the five files — it returns
  nothing (comments referencing the pre-conversion shape by name are the
  only remaining hits). **Four sites needed more than the mechanical
  substitution**:
  - **`schema.rs`'s `WatchMetadata` long-poll's bare `tokio::select!`**
    (racing the metadata watch against the server-side timeout) has no
    `Env` equivalent — replaced with `futures::future::select(watch.
    changed(last_seen), self.env.sleep(WATCH_METADATA_SERVER_TIMEOUT))`,
    the same shape `animus-cp-data::cluster_segment_store`'s own
    relay-correlation race already uses. Both arms are `Unpin` without
    `pin_mut!` (`MetadataChanged` is a plain, non-self-referential struct;
    `env.sleep` is `async_trait`-boxed) — whichever resolves first is
    discarded either way, preserving the exact "change or timeout,
    whichever first" semantics.
  - **`txn_coordinator.rs`'s awaited-branch `tokio::time::timeout`**
    (bounding `resolve_all_parallel` by `TXN_RESOLVE_ALL_AWAIT_BUDGET`)
    became the same `futures::future::select` shape, `Box::pin`ning the
    resolve future first since an `async move` block capturing locals
    across `.await` is not `Unpin` in general (unlike the plain-struct
    `MetadataChanged` above) and `select` requires both arms to be.
  - **`txn_coordinator.rs`'s fire-and-forget `tokio::spawn`** became
    `self.env.spawn_task(..)` (needs `use animus_env::EnvExt;` — the trait
    providing `spawn_task` must be in scope, unlike the supertrait methods
    `E: Env` already brings in). Under `ProdEnv` this is `tokio::spawn`
    underneath, so the detached, fire-and-forget lifetime is unchanged: the
    call still returns immediately and the resolve either completes or is
    dropped on process exit, with no handle kept either side of the
    conversion.
  - **Two `tokio::time::Instant::now().elapsed()` reads** (a forwarded
    2PC-recovery caller's clock-skew fallback, `txn_coordinator.rs`) had no
    literal translation — `Nanos` has no `elapsed()`. The original measured
    the near-zero gap between minting an `Instant` and immediately reading
    it back (not any real wait — `Instant::now()` then `.elapsed()` on the
    same expression), so the faithful equivalent is two back-to-back
    `self.env.now()` reads and `Nanos::duration_since`'s own saturating
    subtraction, reproducing the identical near-zero result rather than
    "fixing" what reads like a pre-existing latent bug (this comparison's
    `now_ms` ends up far below the `wall_ms`-scale threshold it's checked
    against either way) — an incidental bug gets its own PR, never a
    drive-by fix bundled into a testability rung.
  **A subtler bug this rung's own mechanical pass introduced and had to
  catch by building, not by inspection**: seven `write_path.rs` functions
  (`poll_probe`, `cp_batch_local`, `cp_batch_propose`, `cp_put_local`,
  `cp_delete_local`, `cp_kind_local`, `seed_rows_local`) take `leader:
  &CpGroup<E>` with **no `&self`** at all (per step 1's own doc, above) —
  a blind `tokio::time::Instant::now()` → `self.env.now()` regex on the
  whole file compiles only in files where *every* site happens to be
  inside a `&self` method, and silently produces `error[E0425]: cannot
  find value \`self\`` everywhere it isn't. The fix reads `leader.env()`
  instead (a private `CpGroup<E>` accessor already visible to this
  descendant module, unchanged from step 1) — every regex-driven or
  find-and-replace conversion pass over this crate needs a `cargo build`
  immediately after, function-signature-aware, not a visual diff; see
  `docs/engineering-lessons.md`.
- **`client_ctx_host.rs`** (new, ADR 0061 rung C2) — `ClientCtx`'s `impl`
  blocks for `animus-node`'s three host-capability traits
  (`ControlLeaderHost<ProdEnv>`/`BackupObjectStore`/`TtlScanHost` — see
  that crate's own `host` module doc for the shape and why three, not
  one). Every method here is a **thin, logic-free delegation** —
  `self.edge.leader_handle()`, `self.backup_store.put(..)`, one call into
  `dynamo::kind_write_item_at_leader` — nothing is decided here that
  wasn't already decided by an existing `ClientCtx`/`CpGroup`/
  `BackupStoreHandle` method. **`BackupObjectStore`'s four methods always
  answer `Some(..)` now (W-10)** — `self.backup_store` is provisioned on
  every node shape (see `ClientCtx::backup_store`'s own doc), so the
  `self.data_opt()?` early-return this impl used to open with is gone;
  the trait itself stays `Option`-returning for a genuinely store-less
  host (`animus_node::backup_janitor`'s own `ControlOnlyStore` test
  double). This is the seam that let five of
  the six leaf background loops (`ttl_reaper.rs`, `index_backfill.rs`,
  `backup_completion.rs`, `backup_janitor.rs`, `pitr_janitor.rs` — each
  now a thin wrapper into `animus-node`, see their own entries below) move
  without `ClientCtx` itself moving, which doesn't happen until rung C5.
- **`dynamo.rs`** (~59 KB) — the DynamoDB JSON-over-HTTP edge; the `GET /metrics`
  route (ADR 0015) shares this listener. `dispatch` also forwards a
  `DynamoDBStreams_20120810.*` target to `dynamo_streams::execute_as`
  (below) — the two services share one listener/port. **SigV4 enforcement
  (ADR 0057, hardened by ADR 0066 S-02 step 3) lives in `handle_conn`,
  ahead of `dispatch`** — after the `GET /metrics` special case, before
  every other request reaches `dispatch`/`execute_routed_as`: whenever
  `ctx.dynamo_auth` is `Some` **or** `ctx.has_catalog_credentials()`
  answers `true` (the replicated credential catalog's own lock-free fast
  path — see `authz.rs`'s own entry below), `ctx.env.wall_now()` supplies
  "now" (never `SystemTime::now()`, ADR 0051 discipline) and is handed
  straight into `animus_node::sigv4_gate::merged_sigv4_gate(&request,
  &catalog, ctx.dynamo_auth.as_deref(), now_epoch_ms)` — the
  build-`SigV4Request`-then-verify sequence, now merged against **both**
  credential sources (the replicated catalog first, the static bootstrap
  map only when the catalog has no row at all for that access key id,
  ADR 0066 §3/§4), still lives in `animus-node` and is still pure; this
  connection handler does only the clock read, a `Metadata::default()`
  vs. a real `ctx.effective_metadata()` clone (gated on the same fast-path
  flag, so an empty catalog costs nothing new), and the response-shaping
  on failure/metrics-bumping on success (`Metric::AuthRotatedSecretUsed`
  when the caller's previous secret matched during a rotation grace
  window; `Metric::AuthUnknownKey` on `SigV4Error::UnrecognizedClient`). A
  verification failure short-circuits straight to a `400` with the
  AWS-faithful `com.amazon.coral.service#...` body (`sigv4_error_body`,
  rendered via `serde_json` rather than `WireError::to_json`'s
  DynamoDB-namespace prefix — a different `__type` namespace entirely). A
  successful verification resolves an `authz::Principal` (`Unrestricted`
  for the static bootstrap credential or auth-disabled; `Scoped{
  access_key_id, region, policy}` for a catalog match) threaded down
  through `dispatch`/`execute_routed_as`/`execute_as`/`run_operation` to
  the allow-list check (`authz.rs`, below). This gates the item API **and**
  Streams (both flow through `execute_routed_as`), and is deliberately
  **not** inside `execute_routed_as` itself: that function is also the
  admin dashboard's `POST /admin/data/dynamo` proxy's single dispatch
  point (ADR 0021), which must stay unauthenticated (ADR 0020's
  trusted-operator-network posture) — gating inside it would silently
  re-gate that surface too, so every such internal caller instead calls
  the unrestricted-`Principal`-defaulted `execute_routed`/`dynamo_streams::
  execute` wrappers, unchanged in signature from before this ADR. Neither
  `ctx.dynamo_auth` nor the catalog fast path being set skips the whole
  block — zero-cost and behavior-identical to pre-ADR-0057. `http.rs`'s
  `HttpRequest::headers` (every header, lowercased, repeats comma-joined —
  added by ADR 0057) is what makes the `SigV4Request` buildable at all; a
  `SignedHeaders` list can name any header, not just the three this crate
  used to retain.
- **`authz.rs`** (ADR 0066 §5/§6, S-02 step 3) — the per-key allow-list
  enforcement `run_operation`/`dynamo_streams::run_operation` check after
  the SigV4 gate resolves a `Principal`, before dispatching: `classify(op:
  &Operation) -> (&'static str, OpClass)` is an **exhaustive** match (no
  wildcard) over every wire operation, pinned by `authz::tests::
  every_operation_classifies_per_adr_0066_decision_1`; `authorize_op`
  resolves each operation's table(s) the same way (also exhaustive) and
  calls `authorize`/`authorize_each_table`/`authorize_unscoped` against
  the `Principal`'s own `Policy`. `BatchGetItem`/`BatchWriteItem`/
  `TransactGetItems`/`TransactWriteItems` are deliberate no-ops in
  `authorize_op` — each checks every one of its own tables **inside its
  own handler**, before any of that request's work runs, so a request
  spanning an allowed and a denied table is rejected whole (`dynamo.rs`'s
  `BatchWriteItem` arm and `run_transact`/`run_transact_get` each call
  `authz::authorize_each_table` up front). `RestoreTableFromBackup`/
  `RestoreTableToPointInTime` check the **target** table (reusing
  `Operation::table()`'s own pre-existing convention); `DescribeBackup`/
  `DeleteBackup` (ARN-keyed) resolve the checked table from
  `BackupRow::table`; `ListBackups`/DynamoDB Streams' `ListStreams` with no
  `TableName` filter are cross-table reads, allowed only to a policy
  scoped to `TableMatch::All` (`authorize_unscoped`) — a table-restricted
  key gets `AccessDeniedException` rather than a silently-narrowed result.
  A denial is `WireError::access_denied` (`AccessDeniedException`, the
  ordinary DynamoDB-namespace `__type`, never the SigV4 auth-layer one)
  naming the operation and a synthesized `arn:aws:dynamodb:<region>:
  000000000000:table/<table>` — never the policy's own contents — and
  bumps `Metric::AuthDenied`. `Principal::Unrestricted` (the static
  bootstrap credential, or every call with no SigV4 gate at all) never
  reaches `Policy::allows` — every check in this module is a structural
  no-op for it. See ADR 0066's as-built amendment for the full design.
- **`dynamo_streams.rs`** (ADR 0042 §3/§5/§6/§7/§9/§10/§11) — the
  DynamoDB Streams read API: `ListStreams`/`DescribeStream`/
  `GetShardIterator`/`GetRecords`. Full design (label resolution, the
  sealed-vs-open serve split, `StreamHotRead`) is in
  `docs/streams-notes.md` — this entry is just the module pointer.
- **`pitr_janitor.rs`'s two loop bodies moved to `animus_node::
  pitr_janitor`** (ADR 0061 rung C2) — this module is now a thin wrapper
  for both `pitr_snapshot_loop` and `pitr_janitor_loop`, same shape as
  `backup_completion.rs`'s own move above (`DEFAULT_PITR_RETENTION`/
  `DEFAULT_PITR_SNAPSHOT_CADENCE` re-exported from there too, so every
  existing `pitr_janitor::DEFAULT_*` call site elsewhere in this crate kept
  compiling unchanged).
- **`pitr_janitor.rs`** (ADR 0059 §9, Train 3) — PITR's two control-plane-
  leader-only background loops, mirroring `segment_janitor.rs`/
  `backup_janitor.rs`'s own shape: `pitr_snapshot_loop` (periodic
  internally-triggered `BeginBackup { pitr_base: true, .. }` for a
  PITR-enabled table, reusing Train 1's capture driver/aggregator
  completely unmodified — the `pitr_base` flag tags the row **atomically
  with the mint**, in the same apply, since issue #593's fix; there is no
  longer a separate tagging proposal or a self-healing sweep, both deleted
  along with `MetaCommand::MarkBackupPitrBase`) and `pitr_janitor_loop`
  (two-phase mark/reclaim over `Metadata::pitr_segments`, subject to the
  identical epoch-derivation guard `segment_janitor.rs` established for
  streams, plus a base-snapshot keep-anchor mark step that leaves the
  actual reclaim to the *existing* `backup_janitor_loop`, which already
  reclaims any `Expired`/`Failed` `BackupRow` regardless of a PITR tag).
  `DEFAULT_PITR_RETENTION` (35 days)/`DEFAULT_PITR_SNAPSHOT_CADENCE` (6h)
  are hardcoded production defaults — no CLI knob yet, the identical
  documented gap `ttl_reaper.rs`'s own sweep interval has. The module's own
  doc has the full design, including the incident the atomic-tag fix
  closed. **The retention loop's own
  control-only-leader scope gap is closed (W-10)**: `pitr_janitor_loop`'s
  segment-object reclaim now has a real `BackupStoreHandle`
  (`ClientCtx::backup_store`) on every node shape. `pitr_snapshot_loop`'s
  own `BeginBackup` **capture** step stays structurally inert on a
  control-only leader for an unrelated, unfixable reason — capture is
  per-tablet leader-side (`backup_capture.rs`), and a control-only node
  never hosts (so never leads) a CP-data tablet; it still correctly
  proposes `BeginBackup`/tags rows, it just never has a tablet to capture
  from. The fifth **consumer arm** itself (`pitr_tick`/`pitr_seal_now`)
  lives in `index_drain.rs`, alongside the stream seal arm it mirrors — see
  that module's doc.
- **`segment_janitor.rs` did NOT move in rung C2** (ADR 0061) — the one of
  the six leaf background loops that rung's scoping left behind, on
  purpose. Its replica-repair phase reads live bytes from whichever
  recorded replicas are still `Active` cluster members and pushes them to
  freshly-chosen targets via `SegmentStoreHandle::repair` — real placement/
  membership orchestration, not a value nameable as one narrow I/O
  delegation the way `BackupObjectStore::backup_put` captures "durably
  store these bytes somewhere." See `animus-node/CLAUDE.md`'s rung C2
  entry for the fuller reasoning; this file's design (below) is unchanged.
- **`segment_janitor.rs`** (ADR 0043 §A9) — the **segment janitor**: a
  control-plane-**leader**-only background loop (`segment_janitor_loop`)
  doing two-phase retention reclaim + replica repair over the whole
  `stream_shards` catalog. The module's own `//!` doc has the full
  design (including the load-bearing epoch-derivation guard and the
  convergent drop-table cascade); see also `docs/streams-notes.md`.
  **Retired-tablet rule (ADR 0050 rung 6)**: a cutover-removed split
  parent's shards expire by ORDINARY retention — the drop-table
  retention-zero rule keys on the table's *schema* (still live via the
  children), never tablet presence, and the max-epoch pin applies to live
  tablets only (a retired chain can never seal again) — both halves
  red-proven in `tests/stream_janitor.rs::retired_parents_*`.
  **The control-only-leader scope gap is closed (W-10, 2026-09-04, ADR
  0043 §A9)**: phases 2/3 (object deletion, replica repair) used to skip
  unconditionally on a control-only leader (`ClientCtx::data_opt() ==
  None` there — that accessor no longer exists at all, see below); every
  node shape now provisions a real `SegmentStoreHandle`
  (`ClientCtx::segment_store`), never gated on running the data role, so a
  **pure** split deployment (control-only nodes are the only control
  voters) reclaims stream segments correctly with no data-role node ever
  needing to take the lead. `segment_store`/`backup_store` moved off
  `DataRole` onto `ClientCtx` itself for this — `DataRole` now holds only
  genuinely data-role-specific fields (`raftkv_metrics`, `base_id`,
  `stream_seal_knobs`, `change_rates` — `rmw_lock` itself was later
  deleted outright, ADR 0054 step 4b); `ClientCtx::data()`
  (panicking) survives for those, but `data_opt()` (the non-panicking
  accessor this module used to be the sole caller of) was deleted as dead
  code. Regression: `tests/stream_janitor.rs::
  segment_janitor_reclaims_objects_from_a_genuinely_control_only_leader`
  — a genuine split deployment (3 control-only + 2 data-only nodes, no
  combined-mode node anywhere) whose control leader (necessarily one of
  the control-only trio) reclaims a sealed stream's segment objects,
  including the physical on-disk delete at every recorded (data-only)
  replica.
  **Publishes its own progress (roadmap U-07)**: `segment_janitor_loop`/
  `segment_janitor_tick` publish a small `SegmentJanitorProgress` (phase —
  `idle`/`listing`/`waiting_retention`/`deleting`/`sweeping`, mirroring
  this loop's own phase-1a-scan/phase-1b-delete/phase-3-orphan-reap
  structure (phase 2's replica repair publishes no phase of its own — it
  already has `/admin/metrics`' own `stream_repairs_total`/`stream_
  repair_backlog`) — plus `last_tick_at_ms`, cumulative
  `orphans_seen_total`/`orphans_deleted_total` (both cleanup paths
  combined: phase 1b's expired-row object deletes, including a dropped
  table's own immediately-due rows, and phase 3's proven-orphan deletes —
  one "objects reclaimed because nothing references them any more" total),
  `deleted_last_tick`, `pending_orphans` (live rows still awaiting
  retention), `retention_ms`, and `last_error`) at each phase transition,
  directly into `ClientCtx::segment_janitor_progress: Arc<std::sync::
  Mutex<SegmentJanitorProgress>>`. **No capability trait sits between the
  loop and this field** — unlike the backup janitor/TTL reaper (below),
  `segment_janitor.rs` never moved to `animus-node` (see that crate's own
  `CLAUDE.md`, rung C2's "segment_janitor did NOT move" entry), so the
  loop already holds a genuine `&ClientCtx` to mutate directly.
  `reap_orphans` (phase 3) now returns `(seen, deleted, last_error)`
  instead of nothing, feeding the counters above; its own in-crate
  regression tests (`orphan_reap_tests`) were extended to assert on these
  counts. `GET /admin/gc` (`admin.rs::gc_view`) reports
  `{janitor: SegmentJanitorProgress, leader: bool}` — `dropped_tables_
  pending` was deliberately NOT added: `DropTableTablets` removes a
  table's tablet rows from `Metadata` synchronously at apply time (ADR
  0024), so there is no durable "dropped table tombstone" anywhere in the
  replicated catalog for a route to count; what stays pending is each
  node's own local on-disk reclaim, a per-node diff the tablet-host
  reconciler recomputes fresh every tick — not something a single node can
  answer cheaply without a filesystem scan across the whole cluster. A
  non-leader's own progress simply stays `idle` forever, since the loop
  only ever advances it while `ctx.edge.leader_handle()` answers `Some` —
  an honest answer, not a gap. Renders on the Storage tab beside the TTL
  reaper card (`dashboard_storage.js`'s "GC (stream segment janitor)"
  card, `#gc-card`/`#gc-body`, fed from `STATE.gc` — a single SEED-only
  fetch like `/admin/backup-store`'s own card, since this janitor is
  control-plane-leader-only too, never `/admin/ttl`'s per-node fan-out).
  **A sibling route, `GET /admin/segment-store` (ADR 0043 §A7b, roadmap
  U-07's fourth and last route), reports the STORE's own state rather than
  this janitor's** — this node's configured segment store (redacted), the
  shard→replica placement every sealed `stream_shards` row was given
  (`StreamShardRow::replicas`, populated once, at seal time, by
  `ClusterSegmentStore::put_replicated`'s own placement selection — `null`
  for the single-shared-directory `fs` opt-in, which has no per-node
  replica concept), and a bounded live local object-count/byte scan
  (`SegmentStoreHandle::list_local`/`get_local`, mirroring `/admin/
  backup-store`'s own scan). No new progress type or capability trait —
  `admin.rs::segment_store_view` (`animus_node::host::AdminHost::
  segment_store_view`) reads durable replicated state plus a local scan,
  never a loop's own phase. Renders on the Storage tab beside the TTL
  reaper and GC cards (`dashboard_storage.js`'s "Segment store" card,
  `#seg-store-card`/`#seg-store-body`), fed from `dashboard_core.js`'s
  existing PER-NODE `loadAll()` fan-out (`STATE.nodes[*].segmentStore`) —
  like `/admin/ttl`, since this route's own `local_objects`/`local` fields
  are genuinely per-node facts. **This closes docs/roadmap.md's whole
  U-07 section.** Regression: `tests/admin_endpoint.rs::
  admin_segment_store_reports_shard_placement_and_local_objects` (a real
  3-node cluster with the default `cluster` store — create a streamed
  table, write, wait for the write to seal, poll converged-or-timeout
  until some node's own route shows `local_objects.count >= 1`, then until
  every node reports the identical, non-empty shard→replica placement) and
  `admin_segment_store_reports_null_shards_for_the_fs_kind`, plus
  `tests/dashboard_endpoint.rs::dashboard_u07_segment_store_card`.
  Regression: `tests/admin_endpoint.rs::
  admin_gc_reports_segment_janitor_progress_and_leader_state` (a real
  3-node streamed cluster with a generous 600s retention — proving the
  test's own reclaim is driven by the janitor's drop-table cascade, not
  retention elapsing — create a streamed table, write, seal, drop the
  table, poll converged-or-timeout until the leader's own route shows
  `orphans_deleted_total >= 1`; a follower stays `leader: false`/`idle`
  throughout) and `tests/dashboard_endpoint.rs::dashboard_u07_gc_card`.
- **`index_backfill.rs`'s loop body moved to `animus_node::
  index_backfill`** (ADR 0061 rung C2 — the first loop moved, and the only
  one needing no capability beyond `ControlLeaderHost<E>`: `metadata()`/
  `propose()` live directly on `animus_control::RaftNode<E>`, already
  `E`-generic, no `ClientCtx` in sight). This module is now a thin
  wrapper; `tests/index_backfill.rs` (below) is unchanged and still the
  real-cluster regression net, alongside a new `SimEnv`-driven corpus in
  `animus-node`'s own `tests/index_backfill_sim.rs` (a real single-voter
  `RaftNode<SimEnv>`, no sockets).
- **`index_backfill.rs`** (ADR 0045 §4) — the secondary-index
  **backfill-completion aggregator**: another control-plane-**leader**-only
  background loop (`index_backfill_loop`), same self-gating idiom as
  `segment_janitor_loop` just above, but its own distinct loop rather than a
  fourth arm of that one — one convergent concern per loop. Each tick, for
  every table with an index currently `Creating`, flips it to `Active` once
  every tablet **currently** in that table's live tablet map (a fresh read
  every tick, never cached) has a matching row in `Metadata::index_backfill`
  — the per-tablet catalog the backfill seeder (`index_drain.rs`, below)
  populates. Touches only replicated `Metadata` (no `SegmentStoreHandle`
  needed at all), so it never had a control-only-leader scope gap to begin
  with — unlike `segment_janitor.rs`'s own (since W-10, closed): a pure
  control-only leader drives the flip too. See the
  module's own doc for the full design; `tests/index_backfill.rs` proves
  convergence, the no-premature-flip property against a hand-driven
  `MarkIndexBackfilled` sequence (this file's own suite predates the
  seeder and stays hand-driven, by design — it proves the aggregator in
  isolation), a tablet that appears mid-backfill (a real `SplitTablet`)
  blocking the flip until it too reports, and the control-only-leader
  regression.
- **`index_drain.rs`** (ADR 0041 §4, ADR 0042/0043 cursor/seal/
  hot-trim rework, ADR 0045 §2 backfill seeder, ADR 0059 §9 PITR seal arm) —
  the per-node **change-consumer loop** (`change_consumer_loop`, renamed
  from `index_drain_loop` since it is no longer GSI-specific), **five**
  arms per tick per led tablet: the GSI drain, the stream seal arm, the
  **PITR seal arm** (`pitr_tick`/`pitr_seal_now`, ADR 0059 §9 Train 3 —
  the stream seal arm's twin: same trigger knobs
  (`ctx.data().stream_seal_knobs`), same ledger-named-object recovery
  argument, same `!splitting` exclusion guard mirrored into
  `inplace_split_driver_tick`'s own frozen-endgame final seal (the
  copy-based `split_driver_tick`'s identical guard was deleted with it in
  Layer B1), but writing to `crate::BackupStoreHandle`/`Metadata::
  pitr_segments` under `animus_cp_data::backup::pitr_segment_object_id`'s
  namespace rather than the streams `SegmentStoreHandle`/`stream_shards` —
  a table can have a stream, PITR, both, or neither, independently, and the
  marker-only idle fast path's own gate widens to `!pitr_enabled &&
  !ever_pitr_sealed` so a PITR-only table still gets real sealing rather
  than falling into the trim-everything marker branch), the **backfill
  seeder**, and the hot-trim arm (`trim_janitor` gained a `pitr_enabled`
  parameter alongside `stream_enabled`, folding in `Metadata::
  pitr_segment_watermark` as a third possible trim-blocking term). The
  disable-triggered final seal (`ClientRequest::ForcePitrSeal`/
  `ClientCtx::force_pitr_seal_tablet`, called from `dynamo.rs::
  update_continuous_backups`'s disable path) mirrors `ForceSeal`/
  `force_seal_tablet` exactly. The backfill seeder runs once per index
  currently `Creating` on a led tablet's table: it sweeps that tablet's own
  `KIND_BASE` scope forward from a per-index backfill cursor (a
  `KIND_CURSOR` row, tag `backfill:{index_name}`, storing a raw last-seeded
  base-key prefix rather than a packed HLC — see `animus_cp_data::cursor`'s
  module doc for the two value conventions side by side), seeding a
  synthetic change-log record per newly-discovered partition so the
  ordinary GSI drain materializes it with **zero changes to
  `drain_tablet`/`reconcile_partition`** — a seeded record is, by
  construction, indistinguishable from one a live write would have
  produced. Proposes `MetaCommand::MarkIndexBackfilled` once a tick's sweep
  reaches the tablet's *current* range end, re-derived (and re-proposed)
  every tick rather than as a one-shot side effect. Deliberately **no**
  split-lineage cursor inheritance (ADR 0045 §5 Fork A): a post-split
  right child simply restarts its own narrower sweep from scratch,
  unconditionally correct by the drain's own idempotence. See the module's
  own doc for the full per-arm design (including a documented, deliberate
  low-fidelity interaction with a table streamed while backfilling) and
  `tests/backfill_seeder.rs` for the end-to-end suite — five scenarios:
  materialization + `Active` flip, live writes racing the sweep, two
  indexes backfilling independently, a crash/restart mid-backfill, and a
  split during backfill converging to the correct final GSI; see also
  `docs/streams-notes.md`. The module's own 95-line `//!` doc predates the
  seeder section — read the doc comment in the source, not this summary,
  for the authoritative design. **The hot-trim arm's merge-residue
  cursor-row cleanup was removed** (tablets are split-only, ADR 0044) —
  `trim_janitor` only ever touches
  `KIND_CHANGE` rows now, never `KIND_CURSOR`. **`clear_backfill_cursor`**
  (ADR 0045 §5 step 3) is a fifth, on-demand (not per-tick) function in this
  module: an idempotent tombstone of one index's own backfill cursor row on
  one tablet, reached via the internal-only `ClientRequest::
  ClearBackfillCursor` RPC (refused bare, mirroring `ForceSeal`/
  `StreamHotRead`'s shape) and `ClientCtx::clear_backfill_cursor_for_table`
  — called (twice) by `dynamo.rs::drop_index`'s drop-index cascade so a
  later same-named `CreateTableIndex` never silently resumes the deleted
  index's own stale scan position (see the function's own doc and
  `docs/engineering-lessons.md`'s "convergent per-name cursor... can
  silently poison a same-named recreation" entry).
  **Issue #355 (2026-08-23), fixed**: the `"gsi"` cursor write in
  `drain_tablet` (`cursor::cursor_key(&group.scope_range().start,
  GSI_TAG)`) used to be token-truncated below a split right child's own
  `range.start` whenever the split key was non-token-aligned (the normal
  case — `byte_weighted_median` picks a real row's key, almost never
  `TOKEN_BYTES` long); since the write goes through `ClientCtx::
  cp_kind_write_raw`, which **routes by the write's own key**, it landed —
  successfully, not rejected — on the LEFT sibling's tablet instead of the
  right child's own. Fixed in `animus_cp_data::cursor::cursor_key`: the key
  now embeds the tablet's own `range.start` **verbatim** (never truncated),
  with a trailing 2-byte length so [`parse_cursor_key`] can still recover
  the tag unambiguously — see that module's own doc for the full scheme.
  Regression: `gsi_drain_cursor_tests::split_right_childs_gsi_cursor_
  after_a_non_token_aligned_split_issue_355` splits at a real row key and
  asserts the fixed end-state precisely: the watermark advances, the
  change log trims to zero, and the physical cursor row lives on the right
  child's own engine and nowhere else. The existing `split_right_childs_
  cold_start_re_reconciles_from_zero_without_corrupting_the_gsi` regression
  stays green throughout (its own split boundary, `BOUNDARY`, is already
  token-aligned, so it never exercised the bug either way — see
  `docs/engineering-lessons.md`'s Testing entry on this exact
  fixture-vs-production-shape gap).
- **`ttl_reaper.rs`'s loop body moved to `animus_node::ttl_reaper`** (ADR
  0061 rung C2) — the widest of this rung's five moves: the scan/cursor/
  expiry-decision control flow below is now `E: Env`-generic over a new
  `TtlScanHost` trait, but the actual delete still delegates one call
  (`TtlScanHost::ttl_delete_if_attribute_equals`) straight into this
  crate's own `dynamo::kind_write_item_at_leader` — the OCC seatbelt, GSI/
  LSI/change-log/stream side effects, and `rmw_lock` scoping described
  below are **not** duplicated or reimplemented, only reached through one
  more layer of indirection. `DEFAULT_TTL_SWEEP_INTERVAL` re-exports from
  there, so every existing `ttl_reaper::DEFAULT_TTL_SWEEP_INTERVAL` call
  site kept compiling unchanged. A new `SimEnv`-driven test
  (`animus-node`'s `tests/ttl_reaper_sim.rs`) drives the moved loop against
  a **fully synthetic** host — no `CpGroup`, no `ClientCtx`, not even a
  `RaftNode` — the first deterministic coverage this loop has ever had;
  `tests/dynamo_ttl.rs` (below) is unchanged and stays the real-cluster
  regression net for the write-path side this loop delegates to.
- **`ttl_reaper.rs`** (ADR 0051) — the DynamoDB-style **TTL reaper**: a
  per-node background loop, spawned everywhere `index_drain::
  change_consumer_loop` is (combined + data-only; see the module map
  bullets above for the two exact spawn sites), that deletes items whose
  declared TTL attribute has passed on every tablet this node **leads** of
  a TTL-enabled table. Deletes through `dynamo::kind_write_item_at_leader`
  (`KindWriteOp::Delete`) — the identical primitive `DeleteItem` uses — so
  GSI/LSI rows, the change-log record, and the stream image all fall out of
  the ADR 0049 universal kind-write path for free; this module owns only
  the scan. **Quiescence (ADR 0048)**: the scan itself
  (`CpGroup::local_scan_kind_capped`, a new thin forwarder alongside
  `local_scan_kind_bounded`) is a pure local engine read — verified against
  `animus-cp-data`'s source to never touch `RaftKvNode::wake`/
  `WakeSignal` — so a quiesced, nothing-expired tablet costs one idle LSM
  scan per sweep and stays quiesced; `group.wake()` (idempotent, cheap on
  every state) is called only once an expired item is actually found, right
  before the delete proposes. **Every delete is conditional** on the exact
  `AttributeValue` the sweep observed for the TTL attribute
  (`ConditionExpression::Equals`), so a client's concurrent TTL
  refresh/removal makes the delete no-op (`KindWriteOutcome::
  ConditionFailed`, a routine outcome, not an error) instead of racing it.
  Bounded per tick by `TTL_SCAN_BATCH` rows per led tablet via a
  **driver-local** resume cursor (`BTreeMap<TabletId, Vec<u8>>`, the same
  ownership discipline `change_consumer_loop`'s own `first_hot_seen`/
  `marker_bytes_seen` memos use) — no durable `KIND_CURSOR` row, since an
  interrupted sweep simply resumes (or, on a crash/leader change, safely
  restarts from scratch — every decision here is idempotent). Sweep
  cadence: `DEFAULT_TTL_SWEEP_INTERVAL` (a minute — see its own doc for
  why, mirroring `index_drain.rs`'s `INDEX_DRAIN_INTERVAL` doc style) is
  threaded through the same layered-wrapper convention as `quiesce_after`/
  `stream_retention`: `BoundNode::start_with_growth`'s own trailing
  parameter, defaulted by every wrapper above it, with
  `run_node_with_ttl_sweep_interval`/`run_node_with_streams_quiesce_and_
  ttl_sweep_interval` as the test-facing entry points (a real minute would
  make any e2e test glacial) — **no `--ttl-sweep-interval` CLI flag exists
  yet** (a documented gap, the same shape as `quiesce_after`'s own
  not-yet-wired split-deployment paths) and the data-only spawn site has no
  override at all, always the production default. A TTL deletion's change
  record carries `ChangeRecord::ttl_expired: true` (ADR 0051 §7), threaded
  through `kind_write_item_at_leader`/`kind_writes_for_item`'s own trailing
  `ttl_expired: bool` parameter (every other caller passes `false`) —
  `streams_wire::stream_record_json` renders it as a record-level
  `userIdentity: {"PrincipalId": "dynamodb.amazonaws.com", "Type":
  "Service"}`, absent entirely for an ordinary client write. E2e:
  `tests/dynamo_ttl.rs` (enable/disable + `DescribeTimeToLive`, the
  AWS-faithful immediate-visibility-then-eventual-reap contract, future/
  wrong-type/5-year-window never-expire cases, the conditional-delete
  outcome, and the stream `userIdentity`); the follower-relay regression
  for `UpdateTimeToLive` (`MetaCommand::SetTableTtl` on the
  `is_relayable_command` allowlist) lives in `tests/schema_ddl_relay.rs`,
  alongside its sibling DDL-relay tests. **Publishes its own progress
  (roadmap U-07)**: the loop's own `animus_node::ttl_reaper::
  TtlReaperProgress` (phase — `Idle`/`Scanning`/`Deleting` — plus
  `last_tick_at_ms`, a JSON-safe hex-truncated projection of the loop's own
  driver-local resume cursor, `deleted_last_tick`/`deleted_total`,
  `expired_seen_total`, `tables_with_ttl`, and `last_error`) is published
  at each phase transition through a new capability trait,
  `animus_node::host::TtlReaperProgressHost`, into
  `ClientCtx::ttl_reaper_progress: Arc<std::sync::Mutex<
  TtlReaperProgress>>` (`client_ctx_host.rs`'s impl is the usual thin,
  logic-free delegation — a short lock/mutate/drop, never held across an
  `.await`). `GET /admin/ttl` (`admin.rs::ttl_view`) reads it back out
  alongside every TTL-enabled table in the replicated catalog and how many
  of this node's own hosted tablets it currently leads of one — see ADR
  0020's and ADR 0051's matching 2026-09-06 as-built notes for the full
  route design. **Unlike the backup janitor (control-plane-leader-only)**,
  this loop runs on every node, so every node's own progress is a
  genuinely live answer, not an honest-idle placeholder. Regression:
  `tests/admin_endpoint.rs::admin_ttl_reports_reaper_progress_and_ttl_
  tables`, `tests/dashboard_endpoint.rs::dashboard_u07_ttl_reaper_card`,
  and `animus-node`'s `tests/ttl_reaper_sim.rs` (extended with progress
  assertions).
- **`admin.rs`** (~58 KB) — the admin/debug HTTP-JSON endpoint (ADR 0020):
  read-only `GET` views + gated `POST` actions + the dashboard's data-write
  surface; also serves the SPA static assets. **The `(method, path)`
  dispatch table moved to `animus_node::admin` (ADR 0061 rung C4d)**,
  generic over a new `animus_node::host::AdminHost` trait: this file's own
  `dispatch` is now a one-line wrapper (`animus_node::admin::dispatch(ctx,
  &request.method, &request.path, &request.query, &request.body).await`),
  and `impl AdminHost for ClientCtx` (right below it) is a thin, logic-free
  delegation to every handler function this file already had — `config_view`,
  `raft_view`, `raftkv_view`, `storage_lsm`/`storage_control`/`storage_wal`/
  `storage_wal_segment`/`storage_key`/`storage_scan`, `system_table`,
  `backups_view`/`restores_view`, `metrics_view`/`metrics_history_view`,
  `member_drain_status`, `health`, every `action_*` function, and
  `control_members_view`, none of which moved or changed. **Scoping this
  rung found the trait needs a materially wider surface than the ADR's own
  starting estimate** ("a 15-method cluster-shape slice"): this file
  actually touches 19 raw `ClientCtx` members, and three of them
  (`edge`/`control`/`control_storage`) are handles hardcoded to `ProdEnv`
  whose *own* further methods (`hosted_groups`, `local_cp`, `lsm_sstables`,
  raw engine/WAL scans, …) are what most handlers actually call — so
  `AdminHost` is drawn at "one method per admin route" instead, each
  returning the exact `Value`/`(u16, Value)` the route already produced;
  see that trait's own doc in `animus-node` for the full reasoning.
  `action_data_dynamo`/`action_data_seed` still reach `dynamo::
  execute_routed`/the kind-write path exactly as before — unmoved,
  unmodified, matching this rung's exclusion of `dynamo.rs`'s own
  handlers. The `OPTIONS`/CORS preflight, the dashboard's static JS/CSS
  assets, and the dashboard shell HTML (`handle_conn`, `static_asset`,
  `is_ui_path`) stay here, checked before `dispatch` is ever reached — they
  read `crate::dashboard`-owned `include_str!` constants no `AdminHost`
  method has a reason to carry. See `animus-node/CLAUDE.md`'s own rung C4d
  entry for the full design and the finding behind the wider-than-expected
  surface.
  **`config_view` (ADR 0020, roadmap U-06)** carries, alongside the
  identity/address/`auto_split_bytes_threshold` fields `AdminInfo` already
  had: `backup_store`/`segment_store` (this node's own configured store,
  redacted to `{kind, path}` via the `StoreView` type in `lib.rs` — never a
  credential; `null` on a control-only node, which provisions neither),
  `quiesce_after_ms` (the ADR 0048 threshold this node's reconciler was
  actually started with, `null` when quiescence is off **or**
  structurally inapplicable — control-only always; a data-only node
  reports it since S-06 wired `cluster_settings.quiesce_after_secs`
  through `start_data_with_growth`, see the Quiescence section below),
  `auth_enabled`/`auth_access_key_ids` (ADR
  0057's SigV4 gate — the access key **ids** only, never the secret; both
  `null` on a control-only node, which never binds the dynamo listener,
  `Some(false)`/`null` when the role has the listener but no `dynamo_auth`
  section), and `otlp_endpoint` (`otel::resolved_endpoint()`, ADR 0027 —
  `null` when `OTEL_EXPORTER_OTLP_ENDPOINT` is unset/empty). Every field is
  computed once at each `AdminInfo` construction site (`lib.rs`'s
  `start_with_growth`/`start_control_with`/`start_data_with_growth`, plus
  the in-crate `SimEnv` test harness), the same precedent
  `auto_split_bytes_threshold` set — adding one means a compiler-driven
  fan-out across all four `AdminInfo { .. }` literals, not a config_view-only
  change. The dashboard's Node view (`dashboard_node.js::
  renderNodeIdentity`) renders all six as extra `list-row`s below the
  existing address list, skipping (not blanking) whichever ones are `null`
  for this role — the same idiom `addrRows` already uses.
  `tests/admin_endpoint.rs::admin_config_reports_auth_state_and_never_
  serves_the_secret` is the secret-never-served regression: it asserts
  against the raw serialized response body, not just the parsed
  `auth_access_key_ids` field, so a secret leaking through some other key
  would still be caught. `tests/dashboard_endpoint.rs::
  dashboard_role_gating_split_deployment` covers the control-vs-data
  null/present split.

  **The credential admin CRUD (ADR 0066 §1/§2/§6, S-02 step 2)**: `GET
  /admin/credentials` (`credentials_view`) and `POST /admin/credentials`/
  `/rotate`/`/revoke` (`action_put_credential`/`action_rotate_credential`/
  `action_revoke_credential`) — the replicated credential catalog's own
  admin surface, mirroring `dynamo.rs::update_time_to_live`'s propose-then-
  commit-wait shape exactly (`ctx.propose_schema` + a bounded
  `ctx.metadata_fresh()` poll loop, `SCHEMA_COMMIT_TIMEOUT`/
  `SCHEMA_POLL_INTERVAL` — widened to `pub(crate)` in `dynamo.rs` for this
  reuse). **Never a secret in any response**: `credential_row_redacted`
  (shared by `credentials_view` and `system_table_value_display`'s own new
  `EntityKind::Credential` arm — the generic system-keyspace browse would
  otherwise leak one via its usual JSON-passthrough convention) renders
  only `enabled`/`policy`/`created_at`/`updated_at`/`rotation` — never
  `secret`/`previous.secret`. `policy_json`/`parse_policy` are this
  surface's own small, human-readable wire shape for
  `animus_control::Policy` (`{"tables": {"kind": "all"|"names"|"prefixes",
  ...}, "ops": [...]}`) — deliberately not `Policy`'s own `#[derive(
  Serialize)]` output (externally-tagged enum JSON is a worse shape for an
  operator or the CLI to read/build by hand). `config_view` gained one more
  field, `credentials_count` — computed live from `effective_metadata()`,
  never stored on `AdminInfo` (unlike every other field there, this one
  changes at runtime through this very admin surface, not only at node
  assembly); `auth_enabled` (ADR 0057) already answered "is the static
  bootstrap map present," so this catalog needed no second boolean.
  **A real defect found by this feature's own tests, not by inspection**:
  the first cut's convergence check compared `row.updated_at == now`
  (`now` being this catalog's own epoch-**seconds** convention — see
  `animus-control/CLAUDE.md`'s matching entry for why it's seconds, not
  the millisecond `wall_ms` most of this crate's commit-waits key on) —
  coarse enough that a `Put` immediately followed by a `Rotate` in the same
  wall-clock second computed an identical `now` for both, so `Rotate`'s
  very first poll iteration could read back the **pre-rotate** row and
  return it as already-converged. Fixed to compare content instead
  (`row.secret == command_secret && row.policy == ... && row.enabled ==
  ...` for `Put`; `row.secret == command_new_secret` for `Rotate`) — see
  `docs/engineering-lessons.md`'s matching entry for the general lesson.
  `Metric::AuthRotatedSecretUsed`/`AuthDenied`/`AuthUnknownKey`
  (`animus-env`) were appended to the enum in this step so S-02 step 3
  (allow-list enforcement at dispatch) only had to wire them, not widen
  it — **now wired**, by `dynamo.rs::handle_conn` (the first two) and
  `authz.rs::record_denied` (the third); see this file's own `authz.rs`
  and `dynamo.rs` entries above. Tests: `tests/admin_endpoint.rs`'s
  `admin_credentials_view_never_serves_a_secret` (the load-bearing
  never-a-secret assertion, mirroring `admin_config_reports_auth_state_
  and_never_serves_the_secret`'s own raw-body-string-search idiom),
  `admin_credentials_put_rotate_revoke_round_trip` (the full life cycle,
  including the rotate-idempotence-bug regression above), and
  `admin_credentials_put_on_a_follower_is_relayed_to_the_leader` (the
  `is_relayable_command` allowlist regression, mirroring
  `schema_ddl_relay.rs`'s precedent for `SetTableTtl`/`TagResource`/etc.).

  **`GET /admin/health` — the Kubernetes readiness probe (ADR 0060) — reads
  a HYSTERESIS-gated leader belief, not the raw one (issue #595, ADR 0020's
  2026-09-04 amendment).** `ctx.control.leader().is_some()` (the raw
  consensus belief, `RaftCore::leader_id`) is cleared the instant a
  follower's own election timer lapses (ADR 0009 pre-vote) — correct for
  consensus, but it gave this probe a false-negative window on every
  transient one-sided delay of one election timeout or more, even with a
  fully healthy cluster leader. `health()` now gates `200`/`503` on
  `ctx.control.leader_within(HEALTH_LEADER_GRACE_ELECTION_TIMEOUTS ×
  ctx.control.election_timeout())` (3 election timeouts, ~450ms at the
  default 150ms base) instead — see `animus_node::control_handle::
  ControlHandle::leader_within`/`animus_control::RaftCore::leader_within`'s
  own docs for the mechanism (an observational `last_leader_contact`
  timestamp, never read by any consensus decision). The JSON body's
  `control_leader_known` still reports the raw flag unchanged;
  `control_leader_recent` is the new hysteresis-gated one, and `ok`/the
  HTTP status now track the latter. Regression: `animus-control/tests/
  leader_within_hysteresis.rs` (the mechanism, at the `RaftCore` level) —
  `admin_endpoint.rs`'s own `/admin/health` assertions are unchanged
  (`ok == true`/`200` still holds on a healthy converged cluster). See
  `docs/engineering-lessons.md`'s matching entry for the general lesson.

- **`http.rs`** — thin `TcpStream` wrapper over `animus_node::http` (ADR
  0061 rung C4a): `read_http_request` does only `stream.read()`, handing
  every parsing decision (header-block framing, `Content-Length`
  validation, header lowercasing/comma-joining) to `animus_node::http::
  parse_request_head`; `write_response`/`write_response_with` do only
  `stream.write_all()`, formatting via `animus_node::http::
  format_response`. `HttpRequest` itself, `query_param`, `CORS_HEADERS`,
  and `eof` are re-exported from there (`percent_decode` isn't any more —
  its one caller moved to `animus-node` whole alongside `console.rs`, rung
  C4c, so nothing in this crate calls it directly today) — every existing
  `http::*` call site across `dynamo.rs`/`admin.rs` kept compiling
  unchanged. **`HttpRequest::headers` (ADR 0057)** retains every header
  (lowercased name → value, a repeated header's values comma-joined in
  receipt order — the SigV4 canonical form) instead of the three fields
  (`target`/`content-length` handling/`connection`) the parser used to keep
  and discard the rest of; those three keep their own derived fields (every
  existing caller untouched), `headers` is purely additive. Unit-tested
  directly in `animus-node` now (malformed request line, missing/oversized/
  non-numeric `Content-Length`, duplicate-header comma-joining,
  percent-decoding edge cases, response-formatting round trips) — this
  crate's own real-socket edge tests (`tests/dynamo_wire.rs` and friends)
  are still the regression net for the two thin wrappers themselves.
- **`otel.rs`** — OTLP/HTTP distributed-tracing seam (ADR 0027); opt-in, no-op
  unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Scoped to this crate only.
  **`resolved_endpoint()`** (roadmap U-06) factors `init_tracing`'s own env
  lookup out into a standalone function so `AdminInfo::otlp_endpoint`
  (`config_view`, above) can report the same resolved value without
  duplicating the read.
- **`dashboard.rs`** + **`dashboard.{html,css}`** + **`dashboard_*.js`** —
  animusd admin (ADR 0021's "AnimusDB Console") SPA: `include_str!`'d and served as
  distinct static assets, vanilla JS, no bundler/CDN/build step — edit,
  `cargo build`, reload. Tabs are role-gated client-side (ADR 0035 PR7). The
  Streams tab — shown on **every** role now, including control-only; only
  its live-tail poller degrades there, a real backend gap
  (`ClientCtx::data()` panics / a routing timeout) documented rather than
  fixed — design (label resolution, live-tail poller, the
  `/admin/data/dynamo` proxy it rides, and the control-only role-gating
  details) is in `docs/streams-notes.md`. **Not the same thing as
  `console.rs`** (below) despite the naming overlap — this is the
  **operator** surface (cluster health/placement/Raft/storage) on the
  admin port; see `console.rs`'s own entry and ADR 0052's "Naming,
  deliberately addressed" for the full disambiguation. **docs/roadmap.md
  U-01 (render-only, no backend change)** added a Transactions tab
  (`dashboard_txns.js`, read-only `/admin/txns`, gated like Tablets in
  `ROLE_TABS`), full per-replica Raft detail (commit/durable/snapshot
  index/log length, plus the group's live voters/learners) in the Tablets
  tab's `renderTabletDetail`, a `believes_alive` badge (the control leader's
  own real-time failure-detector verdict, ADR 0012) next to each data member
  row in Overview, and a dependency-free inline-SVG `sparkline()` shared
  component in `dashboard_core.js`, charting the six CP read-path counters
  (`cp_read_barriers_served`/`_timed_out`,
  `cp_eventual_reads_local`/`_forwarded`/`_fell_back`,
  `cp_uncertainty_restarts`) from `/admin/metrics/history` on a new Overview
  card; and `dashboard_storage.js`'s `SYSTEM_TABLE_KINDS` extended to all 16
  `EntityKind` variants (`animus-control::syskv`), dropping a stray
  `"keyspace"` entry that never matched any real `EntityKind` segment.
  **docs/roadmap.md U-02** added a ninth tab, Backups (`dashboard_backups.js`,
  gated to control + combined exactly like Placement — no per-node fan-out,
  just replicated `Metadata` a data-only node can't read locally): a render
  of `/admin/backups`/`/admin/restores` (`admin.rs::backups_view`/
  `restores_view`) plus a per-table PITR status row derived from
  `/admin/status`'s own `schemas[*].pitr` (`TableSchema::pitr`,
  `animus-control::schema`, already fetched — no third route), and four
  gated actions, each behind `window.confirm` and posted through the
  existing `/admin/data/dynamo` proxy (ADR 0021) with the real DynamoDB op
  names/payload shapes: `CreateBackup{TableName,BackupName}`,
  `DeleteBackup{BackupArn}`, `RestoreTableFromBackup{TargetTableName,
  BackupArn}`, and `UpdateContinuousBackups{TableName,
  PointInTimeRecoverySpecification:{PointInTimeRecoveryEnabled}}`. The
  Create-backup table picker and the PITR table list both reuse
  `dashboard_browser.js`'s `dynamoTables()` rather than a second table
  fetch — `dashboard_backups.js` loads after `dashboard_browser.js` for
  this reason. **No proxy allowlist change was needed**:
  `admin.rs::action_data_dynamo` has no op allowlist beyond the bare-name
  Streams-vs-item disambiguation (`STREAMS_OPS`), and none of these four
  ops are Streams ops, so each resolves to the ordinary
  `DynamoDB_20120810.<op>` item-API target and reaches
  `animus_dynamo::wire::decode_request` unchanged — the same path every
  other Data Browser mutation already takes. `admin.rs`'s own `backup_id`
  field **is** the backup's DynamoDB ARN, not a bare id (`wire::backup_arn`
  mints the whole ARN as the catalog's opaque `BackupId` key, `animus-
  dynamo::wire`'s own doc), so the row's `backup_id` is posted directly as
  `BackupArn` with no client-side ARN construction. `RestoreTableFromBackup`
  is the only action needing input beyond confirmation (the new table's
  name) — taken via `window.prompt`, this tab's one departure from the
  Data Browser's static-form convention, since a per-row target-table
  prompt has no natural home in a persistent form the way Create-backup's
  table+name pair does.

  **docs/roadmap.md U-07** added a read-only "Backup store" card
  (`#bk-store-card`/`#bk-store-body`, `renderBackupStore`) to this same
  tab, above the backup list — store kind/location, a bounded object
  count/byte total, and the backup janitor's own live phase/counters,
  straight off `GET /admin/backup-store`'s response shape
  (`{store, objects, janitor, leader}`; see `admin.rs`'s own
  `backup_store_view` entry above and ADR 0020's matching as-built note
  for the route itself). Fetched into `STATE.backupStore` alongside
  `backups`/`restores` in `dashboard_core.js`'s existing `loadAll()`
  — no new poll timer, no per-node fan-out. `last_tick_at_ms` renders via
  `dashboard_streams.js`'s `monoDuration` (an `env.now()`-derived value,
  never wall-clock — same rendering caveat as a stream shard's own
  `seal_wall_ms`), so this file gained that one cross-file dependency,
  documented in its own header comment. Read-only — no action to gate,
  since this card mutates nothing.

  **docs/roadmap.md U-07's second route** (`dashboard_storage.js`) added a
  read-only "TTL reaper" card (`#ttl-card`/`#ttl-body`, `renderTtlReaper`,
  called from `renderStorageSelectors`) to the **Storage** tab, not
  Backups — a deliberate departure from `/admin/backup-store`'s own
  placement, since this route's semantics don't fit that tab's "one shared
  answer" shape: the TTL reaper runs on **every** node (self-gated per
  tablet), so `GET /admin/ttl` is genuinely per-node, unlike the backup
  janitor's control-leader-only progress. Fed from `dashboard_core.js`'s
  existing **per-node** `loadAll()` fan-out (`STATE.nodes[*].ttl`, a new
  entry in the same `Promise.all` that already fetches
  `config`/`raft`/`raftkv`/`txns`/`health`/`metrics` per node) rather than
  a second single-fetch-against-SEED call — no new poll timer either way.
  The card renders every TTL-enabled table (from whichever node answered
  first — the catalog is identical everywhere) plus one row per reachable
  node showing its own reaper phase, resume cursor, deleted/expired
  counters, and last error, straight off `GET /admin/ttl`'s response shape
  (`{reaper, tables, leader_tablets}`; see `admin.rs`'s own `ttl_view`
  entry below and ADR 0020's/ADR 0051's matching 2026-09-06 as-built notes
  for the route itself). `last_tick_at_ms` renders via
  `dashboard_streams.js`'s `monoDuration`, the identical cross-file
  dependency the Backup store card above already established. Read-only —
  no action to gate.

  **docs/roadmap.md U-04** (`dashboard_browser.js`) added a `#br-dy-ttl`
  row beside `#br-dy-stream` (`renderTtlRow`, called from
  `renderDynamoFields` alongside `renderStreamRow`) — same shape as the
  Stream row: current state read straight off the already-fetched
  `schema.ttl` (`/admin/status`, the identical replicated-catalog fact
  `dynamo::describe_time_to_live`'s own `meta.table_ttl(table)` read
  answers, so no extra `DescribeTimeToLive` round trip), enable/disable
  posted through the existing `/admin/data/dynamo` proxy behind
  `window.confirm` with the real `UpdateTimeToLive{TableName,
  TimeToLiveSpecification:{Enabled,AttributeName}}` shape — `AttributeName`
  is sent on **both** calls (AWS requires it even to disable, to name the
  attribute being disabled), so `disableTtl` reads it back off `schema.ttl`
  rather than asking the user to retype it. **`#br-dy-table-form`
  (`submitTableForm`) now declares GSIs, LSIs, a stream, and TTL in one
  place**, mirroring `console::ConsoleBackend::create_table`'s own request
  sequence (`lib.rs`, above): a real `CreateTable` call whose
  `AttributeDefinitions` covers every base **and** index key attribute —
  the base key's own type picker, every index-only key attribute defaulted
  to `"S"` (`declareDefault`, the same default `schema::column_type_for
  (None)` applies bridge-side — this form collects no type for an
  index-only key attribute, a deliberate scope cut inherited from the
  console's own precedent, not a mechanism gap) — followed by a
  `UpdateTimeToLive` call once the table exists, since `CreateTable`'s own
  wire shape carries no TTL field. `addCtLsiRow`/`addCtGsiRow` are dynamic
  attribute-row editors (`+ LSI`/`+ GSI`, a remove button per row), the same
  shape `addItemAttrRow` above already uses; unlike the "Add index (GSI)"
  form's own LSI-less scope (ADR 0045 §7 — an LSI can't be added to a
  populated table), this form's LSI rows **do** get a real
  `ALL`/`KEYS_ONLY`/`INCLUDE` projection control, since `wire::
  decode_index_entry` parses `Projection` identically for a `CreateTable`-
  declared GSI or LSI (`animus-dynamo/CLAUDE.md`'s own module doc) — a
  wire-supported field the console's own create-table form deliberately
  omits for LSIs (its own `CreateLsiRequest` doc), but nothing stops this
  form from offering it. Client-side validation mirrors
  `ConsoleBackend::create_table`'s own checks verbatim (every GSI/LSI needs
  a name and hash/sort attribute, an LSI needs the table's own sort key,
  an `INCLUDE` projection needs at least one non-key attribute, TTL needs
  an attribute name to enable) so a mistake is caught here rather than
  bouncing off the wire as a decode error. Tests:
  `tests/dashboard_endpoint.rs::dashboard_u04_ttl_row`/
  `dashboard_u04_create_table_form` (the same render-markers-plus-live-
  round-trip structure as U-01/U-02's own tests above) — the actual
  `UpdateTimeToLive`/`DescribeTimeToLive` wire mechanics keep their full
  end-to-end coverage in `tests/dynamo_ttl.rs`, unchanged by this item.
  **docs/roadmap.md U-05's own first slice** (`dashboard_node.js`) added a
  control-plane members panel to the Node tab, next to `#nd-mirror`
  (`#nd-control-members`, `renderNodeControlMembers`, called from
  `renderNode()`): a render of `GET /admin/control/members`
  (`admin.rs::control_members_view`, ADR 0037 PR3 — the live voter set plus
  the replicated address book), fetched alongside everything else `SELF`
  already carries (`dashboard_core.js::loadSelf`'s own `Promise.all`, so no
  extra poll timer — same `loadAll()` cadence as every other Node-tab
  panel). Per member: id, its most operator-useful address (`admin`,
  falling back to `internal`), a role pill, a voter-vs-learner pill (a
  third, neutral "unknown" state when `voters` is `null` — a `Remote`
  handle that hasn't synced yet, per `ControlHandle::config`'s own
  documented "unknown vs. genuinely empty" distinction — never conflated
  with "learner"), and a leader marker (compared against this node's own
  `/admin/raft`'s `leader` field, already fetched into `SELF.raft`). **Read-
  only in this slice, deliberately** — no add/remove/transfer buttons yet;
  those are the roadmap's own next U-05 PR, gated the same `window.confirm`
  way every other admin action already is. No new `include_str!`/`<script>`
  wiring needed — both `dashboard_node.js` and `dashboard_core.js` already
  load on every role that shows the Node tab. Test:
  `tests/dashboard_endpoint.rs::dashboard_u05_control_members_panel`.
  **docs/roadmap.md U-05's second slice** (`dashboard_tablets.js`) added a
  split-lineage/directed-placing panel to the Tablets tab, `#tb-lineage`, a
  sibling card next to the existing per-tablet detail card (`#tb-detail`),
  keyed by the same `tbSelectedId`: `renderTabletLineage` renders this
  tablet's upward ancestor chain from `GET /admin/system-table?kind=
  split_lineage` (ADR 0050 fork F9 — parent, grandparent, … as far as the
  chain goes, each hop's own cutover time and the parent's final stream
  epoch) and its downward children (the same kind's rows, walked in
  REVERSE — every row whose own `parent` field names this tablet, however
  many generations deep) plus its directed-Placing target/`done` state from
  `?kind=split_placing` (ADR 0062 §2). Explicit "no lineage (never split)"/
  "no pending placing" empty states rather than a blank card. **The
  system-table route has no per-tablet filter** (only `kind`/`after`/
  `limit`, per its own doc in `admin.rs`), and an ancestor lookup (point
  read by id) and a children lookup (the reverse — which row names this id
  as `parent`) need to see different things no single query can both
  answer — so `fetchSystemTableAll`/`loadTabletLineage` fetch the WHOLE
  kind (paginating `next_after`, `LINEAGE_FETCH_PAGE_CAP` = 20 pages of
  1000 rows each — a real cluster's total split count is normally far
  below that bound) and build both directions client-side rather than
  asking the route for a filter it doesn't have. Fetched from `SEED` (the
  node this console is attached to) — `split_lineage`/`split_placing` are
  ordinary replicated `Metadata` collections mirrored identically on every
  control-role node's own system keyspace (ADR 0038), the same "any
  control-role node answers alike" reasoning `controlMembers`
  (`dashboard_core.js`, above) already relies on, and the Tablets tab is
  itself only ever shown on a control-role node (`ROLE_TABS`). **Refreshed
  on selection change AND on this tab's existing `loadAll()` poll cadence**
  (`renderTablets()` triggers a re-fetch unconditionally on every tick a
  tablet is selected, since it already runs every tick regardless of which
  card is open) — no dedicated timer of its own, matching the task's own
  "reuse the existing poll idiom" instruction (unlike the Raft/storage
  detail card beside it, `loadTabletDetailStorage`, which fetches once per
  selection only — this panel's data can change out from under an open
  selection via a background split, so it needed the extra per-tick
  refresh the storage card didn't). No action buttons (read-only, later PRs
  per the roadmap), no new admin route. Tests:
  `tests/dashboard_endpoint.rs::dashboard_u05_lineage_panel` (shell/script
  markers plus a live round trip against both kinds on a cluster that has
  never split) and `tests/admin_endpoint.rs::
  admin_system_table_split_lineage_after_a_real_split` (a real 3-node
  cluster split through to cutover, asserting the `split_lineage` kind
  actually carries the `{id, value: {parent, ...}}` shape this panel
  parses for both children).
  **docs/roadmap.md U-05's third slice, the TABLET action family**
  (`dashboard_tablets.js`) added four gated buttons to `#tb-detail`'s own
  "Actions" section — Split, Flush, Compact, Reconfigure — over the four
  PRE-EXISTING routes (`POST /admin/tablet/split`, `POST /admin/storage/
  {flush,compact}`, `POST /admin/raftkv/reconfigure`); no new admin route.
  Each button is a `window.confirm` naming the tablet id and the action,
  posted through the same `postJSON` helper the Data Browser/Backups tabs'
  own gated mutations use (this crate's one mutation idiom — see
  `docs/roadmap.md`'s §4 Conventions note), with the route's response (or
  error) rendered in a small status line inside the card
  (`tbSetActionMsg`/`#tb-action-msg`) — never `alert()` — then the tab's
  existing `loadAll()` refresh, no new timer. **Targeting mirrors each
  route's own gating, not a uniform choice**: Split posts to `SEED` since
  `ClientCtx::trigger_split` resolves/forwards to the tablet's leader
  internally; Flush/Compact/Reconfigure post to `tbLeaderBase(tablet)` —
  the identical `lead.node.base` the pre-existing storage-detail card and
  "Open in Storage" button already resolve, re-derived nowhere else — since
  Flush/Compact need a node that locally hosts the tablet and Reconfigure
  is leader-only server-side (a `409` "retry on the leader" otherwise); a
  refusal is shown verbatim, never retried automatically. Reconfigure's
  voter-list input pre-fills from the tablet's current `replicas` and, like
  the Split-key input, survives this tab's own ~5s poll re-render via a
  module-level string kept in sync by an `input` listener (`tbSplitKeyInput`/
  `tbReconfigureVoters`) rather than being recomputed from scratch every
  tick — the same "don't clobber an in-flight edit" concern `dyTable`'s
  render-gate in `dashboard_core.js::render` already documents for the Data
  Browser. **The only gate on these four buttons is `window.confirm` plus
  this tab (and card) only ever rendering on a control-role console** — see
  ADR 0020's matching 2026-09-06 as-built note and ADR 0021's "Actions"
  amendment for why that is a deliberate, plainly-stated non-gate rather
  than an oversight: the admin port itself has no auth (ADR 0020), so
  anyone who can reach it can already call any of these four routes
  directly, button or not. `POST /admin/storage/compact` had **no**
  integration coverage anywhere in this crate before this slice (`/admin/
  storage/flush`'s own coverage predates it, `admin_endpoint.rs::
  admin_interface_surfaces_state_and_actions`) — added
  `tests/admin_endpoint.rs::admin_storage_compact_action`. Dashboard-wiring
  test: `tests/dashboard_endpoint.rs::dashboard_u05_tablet_actions`.
  **docs/roadmap.md U-05's fourth slice, the NODE action family**
  (`dashboard_node.js`) added a new card, `#nd-actions`, beside the
  control-plane members panel on the Node tab — three gated buttons over
  three PRE-EXISTING routes: Drain (`POST /admin/drain {node}`, ADR 0032
  PR3 decommission step 1), Remove (`POST /admin/member/remove {node}`,
  decommission step 2 — a refusal of a still-undrained node is shown
  verbatim, never retried), and Add member (`POST /admin/member/add
  {node}`, ADR 0030 online growth); no new admin route. **There is no
  separate "remove member" route to wire beyond Remove above** — `/admin/
  member/remove` already IS both "finish decommissioning a drained node"
  and "remove a member," the same route either way — so this family is
  three buttons, not four, over the three data-plane-membership routes
  `crates/animus-node/src/admin.rs`'s dispatch table actually has
  (`/admin/control/member/{add,remove}`, the **control-plane** counterpart,
  is the separate members-panel PR the roadmap already calls out — not this
  slice). Same house style as the tablet family: `window.confirm` naming
  the node id and the action → `postJSON` → the response/error in
  `#nd-action-msg` → the tab's existing `loadAll()` refresh on success
  only. **Targeting is NOT uniform, mirroring each route's own server-side
  gating** (the tablet family's own precedent): Drain/Remove are
  local-control-leader-only and deliberately not relayed
  (`ClientCtx::admin_drain`/`admin_remove_member`'s own doc), so both post
  to `ndControlLeaderBase()` — the control leader's admin `base`, resolved
  from the identical cross-node fan-out (`STATE.nodes`, each node's own
  `/admin/raft.is_leader`) `computeHealth()`'s own `controlLeader` already
  reads, no extra probe; Add member IS relayed (`ClientCtx::
  admin_add_member`'s own doc — "works from any reachable admin port"), so
  it posts to `SEED`, this console's own node, needing no leader lookup at
  all. **The node-id input defaults to THIS node's own id** (`SELF.config.
  node_id` — how the Node tab already identifies "this node" everywhere
  else on this view) but stays a plain editable text field, since `animus
  admin drain <admin-addr> <node-id>`'s own `<node-id>` argument is
  arbitrary — typically the node actually being decommissioned, reached
  through a DIFFERENT (healthy) node's admin port, not necessarily the
  console's own node — and persists across this tab's own poll cadence via
  a module-level string kept in sync by an `input` listener
  (`ndActionNode`), the identical "don't clobber an in-flight edit"
  discipline `tbSplitKeyInput`/`tbReconfigureVoters` already use.
  **`/admin/member/add` has no dedicated `animus-cli` one-shot subcommand**
  (only `/admin/control/member/add` does) — in production this route is
  called by a joining node's own startup code, never by an operator
  directly, but it is a real, always-live, ungated POST route like every
  other one this dashboard already wires a button to, so it gets one here
  too. All three routes already had real-cluster integration coverage
  before this slice (`tests/decommission.rs`, `tests/cluster_growth.rs`,
  `tests/seed_join*.rs`, `tests/control_membership_admin.rs`) — no new
  `admin_endpoint.rs` test was needed, only the dashboard-wiring one:
  `tests/dashboard_endpoint.rs::dashboard_u05_node_actions`.
  **docs/roadmap.md U-05's fifth and LAST slice, the CONTROL-MEMBERS action
  family (2026-09-06)** — gated add/remove/transfer buttons on the
  control-plane members panel itself: `renderNodeControlMembers`'s per-row
  template gained a **Transfer leadership here** button (hidden on a
  member's own row while it already leads) and a **Remove** button, over
  the pre-existing `POST /admin/control/transfer {to}` (fa41fcb) and
  `POST /admin/control/member/remove {node}` (ADR 0037 PR3) routes; a new
  sibling card, `#nd-control-actions`, gained an **Add** control over the
  third pre-existing route, `POST /admin/control/member/add {node?, addr}`
  — no new admin route anywhere in this slice. Same house style as every
  earlier U-05 family: `window.confirm` naming the node id and the action →
  `postJSON` → the response/error in `#nd-control-msg` → `loadAll()`
  refresh on success only; Remove's response surfaces the server's own
  `warning` field verbatim (ADR 0037 §2's quorum-loss cases), never
  swallowed and never auto-retried with `force`. **All three target
  `ndControlLeaderBase()`** (ae0ad02's own resolver, reused unchanged) —
  every one of the three routes is local-control-leader-only and
  deliberately not relayed (`ClientCtx::admin_transfer_control_leadership`/
  `admin_remove_control_member`/`admin_add_control_member`'s own docs), the
  same reasoning that already put Drain/Remove on this resolver on the
  `#nd-actions` card beside it — unlike that card's own relayed Add member,
  every control-member action here needs the leader specifically.
  **Add's body needs an address, not just an id** — unlike the data-plane
  `/admin/member/add` (a joining node registers its own address
  separately), `/admin/control/member/add`'s wire body wants the new
  voter's own **internal control-Raft** listen address directly. `animus
  admin control-add`'s CLI form (`run_control_add`, `animus-cli`) resolves
  this by fetching the new node's own `/admin/config` first and reading its
  internal address — **fixed 2026-09-06**: ADR 0040 PR1 had merged the old
  `control`/`raftkv` address pair into one `addrs.internal` field, and the
  CLI's own runtime JSON lookup was left reading the removed top-level
  `control` field, so the 3-argument (operator-supplied-id) form of
  `control-add` had been silently broken since that merge (found while
  grounding this slice against the CLI, reported at the time rather than
  fixed since it was out of this slice's own scope). The read now goes
  through a small pure helper (`internal_addr_from_admin_config`,
  `animus-cli`), unit-tested against both the current and the removed
  legacy shape, plus a real-cluster regression pinning the wire shape
  (`tests/control_membership_admin.rs::
  admin_config_reports_the_internal_addr_the_cli_resolves_control_add_through`).
  This dashboard control still sidesteps the whole problem rather than
  reproducing the CLI's own resolution step: it asks the
  operator for the new voter's internal address directly (two inputs, node
  id optional/blank-self-mints and address required, both persisted across
  this tab's poll cadence via `ndCtlAddNode`/`ndCtlAddAddr` — the same
  "don't clobber an in-flight edit" module-level-variable discipline
  `ndActionNode`/`tbReconfigureVoters` already use) rather than attempting
  a cross-origin fetch of another node's admin port, which this admin
  surface advertises no CORS support for anyway. **There is no separate
  `grow` route to wire** — `animus admin control-grow` is a purely
  client-side loop of the same `control/member/add` call, one pair at a
  time (`run_control_grow`, `animus-cli`), never a distinct server
  endpoint, so a "Grow" button would just be "Add" invoked repeatedly and
  adds no real capability this one control doesn't already offer — per the
  task's own instruction to skip and say so when a would-be second route
  turns out not to exist, this family is three actions (Transfer, Remove,
  Add), not four. **This closes docs/roadmap.md's whole U-05 section** —
  every bullet across all five PRs in the series has now landed; see ADR
  0020's and ADR 0021's own matching 2026-09-06 closing amendments.
  Regression: `tests/dashboard_endpoint.rs::
  dashboard_u05_control_member_actions`.
- **`console.rs`** + **`console.html`** + **`console.css`** + **`console.js`**
  — animusd console (ADR 0052's "AnimusDB Data Console"): a DynamoDB-shaped data app for
  application developers, on its own dedicated port (`RoleAddrs.console`) —
  never the admin port (documented no-auth, trusted-interface-only, ADR
  0020) and never a route on the DynamoDB wire listener. **The pure routing
  moved to `animus_node::console` (ADR 0061 rung C4c)** — every request/
  response type, the `ConsoleBackend` trait, and `route` itself, moved
  nearly verbatim (that module was already at this rung's target shape).
  This file is now a thin wrapper: `serve`/`handle_conn` (the real
  `TcpListener`/`TcpStream` accept loop, never under `SimEnv`), the three
  `include_str!`'d shell assets (kept here, alongside the shared
  `fonts.css`/`tokens.css` `dashboard.rs`'s operator console also
  `include_str!`s — moving them would mean duplicating or reaching across
  the crate boundary, which this rung's "no behaviour change" charter isn't
  here to buy), and re-exports of the types `lib.rs`'s `impl
  console::ConsoleBackend for ClientCtx` names by path. See
  `animus-node/CLAUDE.md`'s own rung C4c entry for the moved module's
  design. Bound on combined
  and data-only nodes only; a control-only node hosts no CP-data tablet, so
  it binds none (`BoundControlNode::start_control_with` passes `None` into
  `spawn_common_tail`'s `console_listener` parameter). **This module still
  takes no `ClientCtx` (PR2's tables-list screen, PR3's Config tab, PR4's
  Items tab, and PR5's Stream data tab)** —
  a structural enforcement, not just a documented rule, of the console's one
  defining constraint: it must never surface cluster-shaped state (nodes,
  replicas, tablets, Raft, quorum, leaders, placement, health). PR2 added
  this listener's first JSON endpoint, `GET /console/api/tables`, without
  widening that boundary: `console::serve` takes a `console::
  TableSnapshotFn` (`Arc<dyn Fn() -> Vec<console::TableSummary>>`) instead
  of a `ClientCtx`/`Metadata` reference — a closure `lib.rs::
  spawn_common_tail` builds from `ctx.effective_metadata()` and
  `lib.rs::console_table_summaries` (the **one** function in the crate that
  reads the schema catalog on the console's behalf; see ADR 0052's
  2026-08-20 amendment for why that projection exists instead of reusing
  `/admin/status`). **PR3 (the table page's Config tab) needs more than
  reads — it mutates a table's GSIs/stream/TTL and can delete the table —
  so the seam widens from one closure to a small `async_trait`
  `console::ConsoleBackend` trait** (`table_detail`/`add_gsi`/`drop_gsi`/
  `set_stream`/`set_ttl`/`delete_table`), `serve`'s second parameter
  alongside `TableSnapshotFn` (which stays exactly as PR2 left it — a
  parameterless, infallible read has no reason to move onto the new trait).
  The widening is in *shape* only, never in *kind*: every method still
  takes and returns nothing but plain owned console types (`TableDetail`,
  `GsiDetail`, `LsiDetail`, `AddGsiRequest`, `SetStreamRequest`,
  `SetTtlRequest`, `ConsoleError`) — `console.rs` imports no `Metadata`/
  `TableSchema`/`IndexKind`/`IndexDef`/any schema-catalog type before or
  after PR3; `lib.rs`'s `impl console::ConsoleBackend for ClientCtx` (built
  into an `Arc<dyn ConsoleBackend>` alongside the `TableSnapshotFn` closure
  in `spawn_common_tail`) is the trait's one implementor and the only place
  a schema-catalog type is ever in scope on the console's behalf — see ADR
  0052's second 2026-08-20 amendment for the full design, including why
  `add_gsi`/`drop_gsi`/`set_stream`/`set_ttl` build the same JSON body a
  real DynamoDB client would and call `crate::dynamo::execute_routed` (the
  identical function the real edge and `POST /admin/data/dynamo` already
  call) rather than re-deriving `MetaCommand` proposals directly — **since
  issue #319/W-05, `add_gsi` also builds an `AttributeDefinitions` entry
  for each of `AddGsiRequest`'s new optional `hash_attribute_type`/
  `sort_attribute_type` fields** (validated case-insensitively against
  `S`/`N`/`B` by `console_validate_attribute_type`, a `400` on anything
  else) so a type the console form's picker declares survives into the
  replicated catalog exactly like a real DynamoDB `UpdateTable` call's own
  `AttributeDefinitions` would — `console.js`'s Add-GSI form (`saveGsi`)
  is the only console screen with this control; the create-table form's
  `CreateGsiRequest`/`CreateLsiRequest` remain deliberately type-less (a
  console-form scope cut, not a mechanism gap — see `console.rs`'s own
  `CreateTableRequest` doc), while
  `delete_table` — not a DynamoDB wire operation at all — calls the same
  `ClientCtx::drop_table` `admin.rs::action_drop_table` does. A GSI and an
  LSI render from two distinct types/templates (`GsiDetail`/`gsiRowHtml` vs.
  `LsiDetail`/`lsiRowHtml`), never a shared shape with optional fields — an
  LSI is a scope inside the table's own storage, not a separate
  materialized table, has no lifecycle status, and can't be dropped.
  `console.rs` imports no `Metadata`/`TableSchema`/`IndexKind`/any
  schema-catalog type — only the plain owned console types both seams hand
  it. Item count/size are still deliberately absent from the tables-list
  projection (PR2's ADR amendment) pending a server-side rollup — do not
  fan out to `/admin/*` from here to backfill them. `console.js` is the
  client-side app: a `location.pathname`-based router (mirroring
  `dashboard_core.js::activateTab`'s idiom, but via real `<a href>`
  navigation rather than push-state) rendering the tables list, a table's
  own page, or the create-table form (PR6) at `/console/ui/tables/new` —
  the server serves the identical static shell for every `/console/ui/*`
  path (`is_shell_path`, unchanged since PR1) regardless of which of those
  the client then renders. **PR4 (the table
  page's Items tab) widens `ConsoleBackend` a second time — five more
  methods (`scan_items`/`query_items`/`get_item`/`put_item`/
  `delete_item`), same shape/kind discipline as PR3's own widening** —
  every one still takes/returns only plain owned console types. The one
  new type worth its own note is `console::WireItem` (`serde_json::Map`,
  DynamoDB's own `{"attr": {"S": "value"}}` shape): unlike every other type
  in this module, an item is deliberately **not** projected into a
  console-only shape — there is no fixed "console item shape" to project
  onto (a DynamoDB row is schemaless beyond its declared key attributes), so
  `WireItem` passes straight through every one of the five new methods;
  `console.rs` never interprets an attribute name or value, only moves the
  map between the wire and the HTTP body. See ADR 0052's third 2026-08-20
  amendment for the full reasoning, including why the table page's two tabs
  (Config, default; Items) are two real routes
  (`/console/ui/tables/{name}` vs. `/console/ui/tables/{name}/items`) rather
  than one shared-page pushState toggle — `console.js`'s own Settings/
  Indexes/Danger-zone jump nav (`#settings`/`#indexes`/`#danger`, now
  rendered by `renderConfigTab`, called from `renderTablePage`'s tab
  dispatch) stays a plain same-page anchor, unchanged from PR3 — and why
  `Query` (unlike `Scan`) had no "Load more" as of this PR: `animus_dynamo::
  wire::decode_query` never parsed a `Limit`/`ExclusiveStartKey` at all, a
  pre-existing gap in the underlying wire layer this PR did not attempt to
  paper over client-side. **That wire-layer gap is since closed** (`Query`
  now paginates exactly like `Scan` — see `crates/animus-dynamo/CLAUDE.md`'s
  "Still deferred" entry for the mechanism), but `console.rs`'s
  `QueryItemsRequest` still doesn't expose `limit`/`exclusive_start_key`, so
  the Items tab's "Load more" gap for `Query` remains a separate,
  not-yet-done console-side follow-up rather than a wire-layer one.
  Scanning/querying a named GSI/LSI (`index_name`,
  a real closed set from this same table's own `TableDetail.gsis`/`lsis` —
  rendered with a `<select>`, never free text) fell out cleanly: `lib.rs`'s
  `query_items` resolves the partition/sort attribute names to query by
  from the replicated catalog server-side, the same way `add_gsi`/
  `table_detail` already read it, rather than asking the client to know or
  type them. **PR5 (the table page's Stream data tab, its third and final
  tab) widens `ConsoleBackend` a third time — three more methods
  (`stream_shards`/`get_shard_iterator`/`get_stream_records`), same
  discipline again**: every one built on the real `ListStreams`/
  `DescribeStream`/`GetShardIterator`/`GetRecords` wire operations
  (`crate::dynamo::execute_routed(self, "DynamoDBStreams_20120810.<Op>",
  ..)`, the streams sibling of the `DynamoDB_20120810.*` target every
  earlier PR's mutation already routes through). **This is the PR where
  the "never show cluster state" rule gets genuinely sharp**: a DynamoDB
  Streams shard is *implemented* as a seal epoch of one tablet's own
  change log (ADR 0042/0043), so `console::ShardSummary::shard_id`
  literally embeds a tablet id and a seal epoch
  (`shardId-<tablet>-<epoch>`) as digits. It is surfaced anyway — the id is
  DynamoDB's own public wire identifier, not this console's invention; a
  real client already receives exactly this string from `DescribeStream`
  and passes it back to `GetShardIterator`, so hiding it would make the
  tab useless for the "why did my row vanish" debugging it exists for. What
  never crosses `ConsoleBackend`'s new methods, structurally (no
  `TabletId`/`NodeId`/replica-set type in any of their signatures): which
  node/replica currently serves a shard, and a seal's own storage-internal
  `object_id`/`replicas` (ADR 0042 §10). A table with no stream gets the
  same "honest empty answer, not an error" treatment PR4's `get_item`
  established for a missing key: `stream_shards` returns `enabled: false`
  with a `200`, and `console.js` renders a plain "no stream enabled"
  message pointing at the Config tab's Settings section rather than a
  grid that looks broken. The shard list paginates over `DescribeStream`'s
  own real `ExclusiveStartShardId`/`LastEvaluatedShardId` contract (a `GET`
  with a query param, since a shard id — unlike `Scan`'s `ExclusiveStartKey`
  — is a flat string); a shard's records page over `GetShardIterator`/
  `GetRecords`'s own `NextShardIterator` walk, the honest paging equivalent
  of PR4's `ExclusiveStartKey` walk. `console::StreamRecordsPage::records`
  passes DynamoDB's own `Record` wire shape straight through, unprojected —
  the same "no fixed console shape to project onto" call PR4's `WireItem`
  already made, now including a TTL-reaper delete's `userIdentity` (ADR
  0051 §7) when present, which `console.js` renders as a small "TTL expiry"
  badge next to the event pill. See ADR 0052's fourth 2026-08-20 amendment
  for the full reasoning, including the "closed set gets a real control"
  call for the iterator-type picker (`TRIM_HORIZON`/`LATEST`/
  `AT_SEQUENCE_NUMBER`/`AFTER_SEQUENCE_NUMBER`, DynamoDB's own closed set)
  and why the Stream tab scopes to a table's *current* stream only,
  deliberately not the disable-grace-window pair ADR 0042 §4/§11 lets
  coexist on the raw wire. **PR6 (the create-table form) ships the
  console's last screen and widens `ConsoleBackend` a fourth and final
  time — one more method, `create_table`** — completing the console's
  three-screen set (tables list, a table's own page with its three tabs,
  and the create-table form). `POST /console/api/tables`
  (`console::CreateTableRequest` in, `console::TableDetail` out, same
  `execute_routed`-reuse discipline as every mutation before it: a real
  `CreateTable` call, plus a follow-up `UpdateTimeToLive` call for TTL,
  since `CreateTable`'s own wire operation carries no TTL field) covers
  table name, partition key (a real `S`/`N`/`B` control — `CreateTable`
  genuinely records a **base table** key's declared type), an optional
  sort key, any LSIs, any GSIs (with a projection), a stream, and TTL.
  **LSIs are declarable *only* on this form** — `ConsoleBackend` has no
  `add_lsi`/`drop_lsi` and never will, since a DynamoDB LSI is
  create-time-only by DynamoDB's own contract, not a policy this console
  chose (`console::CreateLsiRequest`'s own doc states this). Tracing
  `CreateTable`'s own decoder for this PR found that an index's key
  attribute gets **no** recorded type even when the index is declared at
  `CreateTable` time — `schema::to_control` only ever builds a `ColumnDef`
  for the base table's own partition/sort key, and `schema::
  index_to_control` (used identically for every `CreateTable`-declared
  index) never receives `key_types` at all — correcting PR3's own ADR text,
  which had asserted the opposite without tracing it; `CreateGsiRequest`/
  `CreateLsiRequest` accordingly ask for index key attribute *names* only,
  same as the Add-GSI form. A projection genuinely *does* survive
  (`decode_index_entry` parses `Projection` for every declared index
  regardless of kind), so this PR adds a real `ALL`/`KEYS_ONLY`/`INCLUDE`
  control plus a new `console::ProjectionSummary` field on `GsiDetail`
  (rendered for every GSI, not just create-time ones). Two maintainer
  corrections from earlier drafts, both now load-bearing: the sort-key
  toggle that gates the LSI section defaults **on** (a blocked LSI section
  with no visible way to unblock it was the exact defect flagged); stream-
  enabled/TTL-enabled are `console.js`'s existing `toggleSwitch`, never a
  segmented `ENABLED`/`DISABLED` pair (segmented stays reserved for the
  form's genuinely closed sets — stream view type, GSI projection type).
  See ADR 0052's fifth 2026-08-20 amendment for the full design, and the
  fourth amendment (referenced above) plus that ADR generally for why the
  console does *not* join the replicated `NodeAddrs` book — no other node
  ever needs to resolve it.
- **`TableDetail` gains `pitr`/`backups` (roadmap U-03, ADR 0059 §3/§4/§9)**
  — no `ConsoleBackend` widening this time (unlike every PR above): both
  new fields are read-only projections `console_table_detail`
  (`lib.rs`) builds alongside the GSI/LSI/stream/TTL fields it already
  computes, so `table_detail`'s existing one method covers them for free.
  `pitr: Option<console::PitrStatus>` is `None` when continuous backups are
  disabled — an `Option` on the *outer* field replacing the `enabled: bool`
  companion-field shape `StreamSummary`/`TtlSummary` use, since there is
  nothing else to carry when disabled; `Some` reuses `dynamo::
  pitr_description` **verbatim** (widened to `pub(crate)`) rather than
  re-deriving `DescribeContinuousBackups`'s own restore-window computation
  a second way. `backups: Vec<console::BackupSummary>` (`backup_id`/
  `status`/`created_wall_ms` only — never a node/tablet/replica/object-path
  detail, ADR 0052's own rule) is `meta.backups` filtered to this table's
  own rows, excluding an `Expired`/`Failed` row (the identical filter
  `visible_backup`/`list_backups` already apply) and a PITR base snapshot
  (`meta.pitr_base_backups` — internal machinery, never a user's own
  `CreateBackup`, mirroring `ListBackups`' own default `USER`-only filter);
  `dynamo::backup_wire_status` (also widened to `pub(crate)`) supplies the
  same `CREATING`/`AVAILABLE`/`DELETED` label `DescribeBackup`/`ListBackups`
  use. **Table-scoped, not backup-scoped**: a backup outliving its dropped
  source table (ADR 0059 §3's own "scar") never appears here — this list is
  for a live table's own detail page only, reached instead via
  `DescribeBackup`/`ListBackups` directly. `console.js`'s Config tab gained
  a fourth jump-nav section, "Backups" (`renderBackupsSection`, between
  Indexes and Danger zone) — a read-only fact strip for the PITR status
  plus a plain list for on-demand backups, no new endpoint and no edit
  affordance (this item adds no way to enable PITR or create a backup from
  the console — only DynamoDB's own `UpdateContinuousBackups`/
  `CreateBackup` calls do that today). Tests:
  `tests/console_table_config.rs`'s
  `table_detail_with_no_pitr_or_backups_is_null_and_empty`/
  `table_detail_shows_pitr_status_and_backups`.

## CLI reference

`main.rs --help` (or the `gen-config`/`join`/`control`/`data` subcommand
help) prints the full invocation reference (durable LSM backend by
default; `--ephemeral` selects the volatile memory engine). Notes not
obvious from `--help` alone:

**`--dynamo-auth PATH` (ADR 0057)** — a JSON file of the same shape as a
`ClusterConfig`'s own `dynamo_auth` section (`{"credentials": {"AKID...":
"secret...", ...}}`), naming the client DynamoDB port's SigV4 credential
store. Accepted by `run`'s shared flag parser (so it applies to `--config
FILE --node I` and `--cluster N`) and by `data`'s own parser (`data --config
FILE --node I` and `data --seed ...`) — the config-less shapes (`--cluster
N`, `--cluster-control`/`--cluster-data`, `data --seed`) have no other way
to supply credentials; `--config`/`data --config` can instead put the
`dynamo_auth` section directly in the config file. Supplying credentials
**both** ways (a config file whose own section is present, **and** the
flag) is a hard startup error (`apply_dynamo_auth_flag`) — never a silent
precedence rule. Not accepted by `join`/`control` (a control-only node never
binds the dynamo listener). Omitted (the default), auth stays disabled —
byte-identical to pre-ADR-0057 behavior.

**`--segment-store`/`--backup-store s3://bucket[/prefix]?endpoint=...&region=...
[&path_style=true][&insecure_http=true]` (S-04 PR 2)** — an S3-compatible
bucket in place of the default `cluster`/`fs:PATH` opt-ins; see this file's
own Gotchas entry (grep "S-04 PR 2") for the full design. Needs
`--s3-credentials PATH` (a standalone JSON credentials file — never a
`ClusterConfig` field) or the `ANIMUS_S3_ACCESS_KEY_ID`/
`ANIMUS_S3_SECRET_ACCESS_KEY` environment variables; a plaintext
`insecure_http=true` endpoint needs `--allow-insecure-s3` too unless it's
loopback. Reaches `run` (`--config`/`--node` and `--cluster N`),
`run_control`, and — since issue #676 — `join` and `data --seed` too (the
two real growth paths, threaded through `run_node_join_with_settings`/
`run_node_data_join_with_settings`). **`--cluster-control`/`--cluster-data`
and `data --config` remain a documented gap**: the former's
`start_split_cluster_with_growth` hardcodes the default `Cluster` store for
every data-role node it stands up (no CLI route at all on that dev-only
path), and the latter has no `cluster_settings`-shaped route to either
store the way it does for `quiesce_after_secs`/`heartbeat_batch`/
`shared_wal`.

**`--tls-cert PATH --tls-key PATH --tls-ca PATH` (ADR 0064, S-01 commit
2)** — this **one process's own** TLS material: all three or none (the
internal wire is always mutual TLS the moment TLS is configured at all, so
there is never a legitimate case with a cert/key but no CA to verify peers
against). Accepted by `run`'s shared flag parser for **`--config
FILE --node I` only** (applied onto `config.nodes[index]` via
`apply_tls_flag` — the same per-node-entry shape `--advertise-host` uses,
**not** `--dynamo-auth`'s cluster-wide one, since TLS material is
inherently per-node) and by `data`'s parser for **both** `data --config
FILE --node I` (same per-node merge) and `data --seed ...` (no config file
at all on that path — the flag sets `RoleAddrs::tls` directly, no conflict
to check). Supplying a `tls` section both in the config file's own
`nodes[index]` entry and via the flag is a hard startup error, the
identical "specify it one way, not both" contract `--dynamo-auth`/
`--advertise-host` already use. **Not accepted by `--cluster N`/
`--cluster-control`/`--cluster-data`/`join`/`control`** — the two
in-process dev-cluster modes have no per-node config entries to apply the
flag to and **hard-error** rather than silently ignore it (a deliberate
departure from those paths' usual silent-gap precedent for other flags —
see `main.rs`'s own doc for why silently downgrading a requested-TLS
cluster to plaintext is a worse failure mode than an explicit rejection);
`join`/`control` simply mirror `--dynamo-auth`'s own non-acceptance there.
A real multi-node deployment wanting TLS should bake every node's `tls`
section into one shared config file up front (the shape a Kubernetes
ConfigMap naturally wants, commit 3's target) rather than relying on each
process's own `--tls-*` flags to agree — see `ClusterConfig::validate_tls`'s
own doc for exactly why the flag route can't be cross-checked at any one
process's own startup. Every port a node binds gets **mutual** TLS on
`internal`/`intra`, **server-only** TLS on `client`/`dynamo`/`admin`/
`console` (`Node::bind`/`bind_control`/`bind_data`, `TlsMaterial::
acceptor` vs `server_acceptor`) — see this file's "TLS" section below for
the full design. Omitted (the default), every listener/dialer stays plain
TCP, byte-identical to before this ADR.

**`--encryption-key PATH` (ADR 0069, S-03 PR 1 and 2)** — this node's own data
directory encryption key file (`animus_env::EncryptionKey::
load_from_file`'s format: 64 hex characters, optionally a trailing
newline; generate one with `openssl rand -hex 32 > key.hex`). Threads to
`RoleAddrs::encryption_key_path: Option<String>` — **per-node**, mirroring
`tls`'s own shape (not `dynamo_auth`'s cluster-wide one), since each
node's disk is independent. `--config FILE --node I`: merged onto that
one node's own config entry (`apply_encryption_key_flag`, the identical
"flag and config both set it is a hard error" contract `apply_tls_flag`
uses). `--cluster N`: the same path applied to every generated node
(`bind_cluster_with_advertise_host_and_key`) — each still writes to its
own distinct data directory, so one shared key just means every node's
disk is sealed under it. **Rejected outright** (a loud `Err`, matching
`--tls-*`'s own posture) by `--cluster-control`/`--cluster-data` — that
in-process dev path has no per-node config entries to apply the flag to,
the same posture `--tls-*` already has there (unchanged by issue #676:
"silently downgrading a requested-encryption cluster" is the identical
worse-failure-mode argument that path's own TLS gate already makes).
**Now also accepted by `animusd control`, `animusd join`, and `animusd
data --seed` (issue #676)** — `control` merges it onto
`config.nodes[index]` via `apply_encryption_key_flag`, identically to
`--config`/`--node`'s own combined-mode route above; `join`/`data --seed`
set `RoleAddrs::encryption_key_path` directly (no config file on either
path to conflict-check against, the same shape `--tls-*` already has
there). **`animusd data --config` remains a documented gap** — no CLI flag
of its own for this knob yet, unlike `--quiesce-after`/`--heartbeat-batch`/
`--shared-wal`'s S-06 `cluster_settings` route (a config file's own
`nodes[index].encryption_key_path` field still works there directly, since
`Node::bind_data` reads it the identical way `Node::bind`/`bind_control`
do). `Node::bind`/`bind_control`/`bind_data` each load
the key and call the new `ProdEnv::bind_with_tls_and_key` (`bind_with_
tls`'s general form, `animus-env`) instead of `bind_with_tls` — the loud
refusal (a key/directory mismatch in either direction) happens inside
that call, before any listener binds. Omitted (the default), every
node's disk stays plaintext, byte-for-byte pre-ADR-0069 behavior. See
`docs/adr/0069-encryption-at-rest.md` for the full design and
`crates/animus-env/CLAUDE.md`'s `encrypted.rs` entry for the wrapper
itself; `crates/animusd/tests/encryption_at_rest_e2e.rs` is the real
`ProdEnv`/disk/DynamoDB-wire regression.

**PR 2 also seals a `SegmentStore`, not just the `Disk` seam — and, since
the 2026-09-07 "As-built: cluster store" amendment (closing issue #680),
this now covers the DEFAULT `cluster` store too, not only the `fs:`/
`s3://` opt-ins**: the same `--encryption-key` (loaded once per
`Node::bind*`, cloned onto `BoundNode`/`BoundControlNode`/
`BoundDataNode::encryption_key`) is handed to `build_segment_store`/
`build_backup_store` (both `async`), which seal **every** variant —
`Cluster` (the default) included — under `animus_env::
EncryptedSegmentStore` when a key is configured. `Fs`/`S3` are unchanged
from PR 2 (`SegmentStoreHandle`/`BackupStoreHandle` each carry an
`EncryptedFs` variant; `S3` needed no new arm, since it already boxes
`Arc<dyn SegmentStore>`). **`Cluster`'s own local building block —
`SegmentStoreHandle::Cluster`/`BackupStoreHandle::Cluster`'s field type,
`ClusterSegmentStore<ProdEnv, LocalSegmentStore>` — is a single new
`pub(crate)` two-arm enum, `LocalSegmentStore { Plain(FsSegmentStore),
Encrypted(EncryptedSegmentStore<FsSegmentStore, ProdEnv>) }`**, occupying
`ClusterSegmentStore<E, S: SegmentStore>`'s own pre-existing generic
parameter `S` (never concretely `FsSegmentStore` inside that type to
begin with — the "widening" PR 2's own scope-cut paragraph worried about
turned out to be entirely local to `animusd`, zero changes to
`animus-cp-data`) — one variant each on `SegmentStoreHandle`/
`BackupStoreHandle`, never a fourth `EncryptedCluster` arm, so every
existing `match` on those two enums is untouched. `local_cluster_store
(env, dir, encryption_key)` is the one new helper both `build_segment_
store`/`build_backup_store`'s `Cluster` arms call — the identical
`Fs`-arm control flow (wrap in `EncryptedSegmentStore::open` when `Some`;
run `verify_or_init_segment_store_marker` directly and stay `Plain` when
`None`, the "off by default still checks" rule), factored out once rather
than duplicated a third time. **Key scope is cluster-wide for every
variant, `Cluster` included, not per-node** — unlike a `Disk` file, a
backup/PITR/stream-segment object is routinely read by a different node
than the one that wrote it, so every node sharing a store must be
configured with the identical key file; for `Cluster` this is the
existing deployment-wide `--encryption-key` convention, now load-bearing
for this store too (no second decision needed — see the ADR's
"As-built: cluster store" amendment). See `animus_env::
EncryptedSegmentStore`'s own module doc and the ADR's PR 2/"As-built:
cluster store" amendments for the full decision (including what a
mismatched-key node does: refuses loudly at its own startup, before it
ever binds a listener — never a silent half-encrypted cluster).
`crates/animusd/tests/encryption_at_rest_segment_store_e2e.rs` is the
real `ProdEnv` regression for the `fs:` opt-in store (unchanged): a
2-node cluster's `CreateBackup` on node 0 → `RestoreTableFromBackup` on
node 1 with the same key succeeds and the shared backup-store directory
never holds the plaintext value; a differently-keyed node against that
same directory, and a keyed node against an existing plaintext directory,
are both refused at startup. `crates/animusd/tests/
encryption_at_rest_default_cluster_store_e2e.rs` is its sibling for the
**default** store: the identical restore-across-nodes/no-plaintext-anywhere
proof but with every store left at its default (no `--segment-store`/
`--backup-store` at all) and covering both `<node dir>/segments` (the
streamed table's sealed shard) and `<node dir>/backups` (the on-demand
backup) at once, plus both loud-refusal directions — isolated from PR 1's
own `Disk`-seam marker (which lives at the sibling `<node dir>/internal`
directory, never scanned by the same `Disk::list()` call that guards
`<node dir>/segments`/`<node dir>/backups`) by constructing a target
directory carrying only the cluster store's own local content, never a
node's top-level marker/WAL/engine files — see that test file's own doc
and the ADR amendment's "Marker semantics" section for why that isolation
is needed at all.

**`--advertise-host NAME` (ADR 0060's advertise/dial split)** — this
node's own stable dial name, when its bind address isn't itself something a
peer can dial reliably (a Kubernetes pod's wildcard/pod-IP bind, whose IP
changes on every reschedule but whose own DNS name doesn't). Threads
`RoleAddrs::advertise_host: Option<String>` (`#[serde(default)]`, so an
absent/`None` config entry is byte-identical to pre-ADR-0060 behavior —
every self-registered address is the bind address itself, stringified via
`advertised_addr`) down to every place a bind `SocketAddr` becomes the
`host:port` string a peer actually dials: each `Bound{Node,ControlNode,
DataNode}`'s own `NodeAddrs` self-registration, its own peer-book entry
(`peer_entries`/`peer_entry`), and the **static** `ClusterConfig`-derived
seed (`ClusterConfig::peer_book`, and the `client_route`/`intra_route`
builders in `run_node_with`/`run_node_control`/`run_node_data`/
`run_node_growth`) — not just each node's own self-registered `NodeAddrs`,
since a fresh cluster's very first Raft dial happens before anything has
replicated once. One shared host for every port a `RoleAddrs` entry binds
(a real deployment advertises one pod identity, not six); bind addresses
themselves stay numeric and untouched everywhere. Accepted by `run`
(`--config`/`--node` — applied as an override onto that one node's own
config entry via `apply_advertise_host_flag`, the same both-ways-is-an-
error shape `--dynamo-auth` uses; `--cluster N` — applied uniformly to
every generated node, each still binding its own distinct ephemeral port so
`{host}:{port}` stays unique per node via
`bind_cluster_with_advertise_host`), `join`, and `data --seed`
(`run_data_join`). **Not accepted by `--cluster-control`/`--cluster-data`**
(`run_in_process_split_cluster` has no per-node-advertise-host wrapper to
call) or the standalone `control`/`data --config` subcommand paths beyond
what's listed — documented gaps, the same shape several other flags already
have on those entry points. `--seed`/`--advertise-host` together is what
lets a hostname (not just a numeric address) name a join target — see
`animus-env/CLAUDE.md`'s `ProdEnv` peer-book entry for the matching
production-seam half (`set_peers`/`merge_peer` are string-keyed end to end;
resolution happens only on the connect path). Tests: `tests/
advertise_host.rs` — a mixed-address two-node bootstrap proving a plain
node dials an advertising peer purely by its advertised name, a same-
identity restart on a different bind IP (`127.0.0.2`) that keeps
`Metadata.node_addrs` byte-identical because the advertised string never
changed (using a real, test-owned `/etc/hosts` entry to simulate the DNS
re-point a rescheduled pod would get — see `docs/engineering-lessons.md`
for why a static alias like `localhost` can't stand in for that), and a
3-node cluster whose every entry shares one advertised host, proving the
static config-derived peer book (not just self-registration) prefers it.

**The copy-based split-build driver (ADR 0050 Train B rungs 4-8) and the
`SplitMode` selector that chose between it and the in-place workflow below
were deleted whole in the copy-split-deletion endgame's Layer B1**
(`docs/adr/0058-*.md` rung 4) — `SplitBuild`/`split_driver_tick`/`ship`/
`ship_all`/`tail_pass`/`seed_row_bytes`/`packed_hlc`/`prefix_upper`/
`max_change_hlc`/`SEED_KINDS`/`SEED_CHUNK_BYTES`/`SPLIT_MAX_TAIL_PASSES`
(`index_drain.rs`), `ClientCtx::seed_rows_local`/`seed_child_rows`
(`write_path.rs`), the `SeedRows` wire RPC (`animus-node::wire`), and
`animusd::config::SplitMode`/`ClientCtx.split_mode`/`--split-mode
{copy,inplace}` are all gone; `tests/split_build.rs` was deleted earlier,
in Layer A. `ClientCtx::trigger_split` now unconditionally proposes
`MetaCommand::BeginSplitInPlace` — see its own doc, `schema.rs`.
`MetaCommand::BeginSplit` itself (and `split_child_placement`'s fork F5
placement-at-mint logic) was deleted in Layer B2. `animus_control::
select_replicas_balanced` (the placement primitive `split_child_placement`
used to call) stays in `animus-placement` — production-dead now, kept
since nothing in this layer's scope
justified touching that crate. What follows below is the **in-place**
workflow, which was always independent of the copy-based one apart from
sharing `trigger_split`'s choke point and (until Layer B1) the GSI-drain/
backfill-veto constants documented next.

**The in-place cutover driver** (`index_drain.rs::
inplace_split_driver_tick`) is `change_consumer_loop`'s in-place arm for a
`Splitting` parent this node leads — the only arm now that the copy-based
driver above it is gone, selected (still per-tablet, not per-node, though
there is no longer a second workflow to distinguish it from) by
`Tablet::inplace_split.is_some()` (a durable fact of the tablet's own
`Metadata` row, set only by `BeginSplitInPlace`). Everything upstream of
this — proposing the single-entry `KvCommand::SplitTablet` fork itself
(**since ADR 0062, immediately, with no learner-add-and-wait phase to run
first — every replica the fork touches already hosts the parent as an
ordinary voter**) and materializing both children's engines on every fork
participant — is entirely `animus_cp_data::host`'s own reconciler (ADR
0058 Train 2 rung 3; fork-first per ADR 0062 rung 5, unmodified by this
driver); this function has nothing to do until `CpGroup::pending_split()`
answers `Some` on this replica. Once forked, there is no build, no freeze,
no tail, no convergence bound — the atomic mint already fully formed both
children — so what is left is exactly the two pre-cutover vetoes the
deleted copy-based endgame used to share the constants for (GSI-drain,
accelerated via `gsi_caught_up`/`FROZEN_ENDGAME_GSI_DRAIN_MAX_PASSES`,
both of which stayed — the in-place driver is now their only caller;
backfill-seeder) run against the parent's own (now-frozen, static —
`SplitTablet` reuses `Freeze`'s exact whole-range seal discipline) change
log, plus the **streams final seal** anchored at the fork position
(`seal_now`, looped to exhaustion), before proposing
`MetaCommand::CutoverSplit` with the identical confirm-by-observation loop
(re-issued every tick until the parent vanishes from the map). Fully
idempotent across crash/re-lead with **no driver-local state at all**:
every check is a fresh read off durable/replicated state.

**Gotcha this rung found, in real `ProdEnv`, not `SimEnv`**: proposing
`CutoverSplit` the instant `pending_split()` is `Some` races
`animus_cp_data::host`'s own reconciler, which is a *different*,
independently-scheduled per-node loop (`tablet_host_reconciler_loop`).
The fork itself commits nothing on the control plane, so that reconciler's
`metadata_watch` wakes once (at `BeginSplitInPlace`'s own commit) and not
again until `CutoverSplit`'s — leaving its `RECONCILE_FALLBACK_INTERVAL`
(500ms) fallback as the only thing that can make it discover a completed
fork and run `HostAction::MaterializeSplitChild`. This driver's own
200ms-paced tick can — and, with no GSI/stream veto to wait on, routinely
does — get `CutoverSplit` committed before some replica's reconciler has
ticked even once since the fork, which then hosts the freshly-`Active`
child via the *wrong* (non-split) path once the parent's row — and with it
`Tablet::inplace_split`, the only signal that branch keys on — is gone:
permanent, silent data loss, not a transient blip. Closed by two additions,
both local to `animusd` (`lib.rs`): `tablet_host_reconciler_loop` shortens
its own fallback to `INPLACE_SPLIT_RECONCILE_INTERVAL` (50ms) for as long
as *any* tablet cluster-wide carries an in-place split intent — every fork
participant observes the identical `BeginSplitInPlace` commit and flips
into this cadence together, well before Stage 3 ever applies — and
`inplace_split_driver_tick` additionally requires
`INPLACE_SPLIT_MATERIALIZE_SETTLE_MS` (250ms, a small multiple of the new
50ms cadence) to have elapsed since the fork applied (`PendingSplit::ts`,
the same `env.now()`-derived clock `cutover_wall_ms` already uses — no
driver-local timer) **and** this replica's own `ctx.edge.hosted_groups()`
to already contain both children, before it may ever propose cutover. See
`docs/engineering-lessons.md`'s entry for the general lesson. E2e:
`tests/inplace_split_e2e.rs` — a real 3-node cluster, a paced continuous
writer riding kickoff through cutover
(asserting every acked write survives with its exact value, and observing
zero write refusals across every run once the fix landed), and a
streams-enabled variant walking a `GetRecords` iterator from the parent's
own shard 0 across the fork to both children's own shard 0s with no loss
or duplication.

`--auto-split-bytes B` (byte size), `--auto-split-change-rate RATE`
(streamed tables only, ADR 0042 §14 Fork F — bytes/sec of a tablet's own
`KIND_CHANGE` growth, `/admin/metrics`'s `stream_change_rates`), and
`--auto-split-ops-rate RATE` (W-09, ADR 0034 amendment — any table, ops/sec
of a tablet's own leader-side write rate, `/admin/metrics`'s
`request_rates`) are independent OR-gated triggers — any subset of them,
including none. (**The former key-count trigger, `--auto-split K`, was
removed** — see the root `CLAUDE.md`'s auto-split entry.)
`--auto-split-change-rate` closes the gap bytes structurally can't:
`CpGroup::approx_bytes` is base-scoped (ADR 0034), so a high-churn,
small-footprint streamed table never crosses a byte threshold regardless of
write rate — but only for a *streamed* table. `--auto-split-ops-rate`
closes the identical gap for **any** table (streamed or not): a tablet's
`RequestRateTracker` estimate is fed from every successful leader-side
write, not from the change log, so a plain table under heavy write load is
visible to it too. Neither
change-rate nor ops-rate has a production-tuned default — omitting a flag
disables that trigger entirely (zero behavior change); an operator must
pick its own `RATE` per workload. **All three flags reach every real
deployment shape, not just `--cluster N`/`--cluster-control`+
`--cluster-data`'s dev-cluster mode** — `--config`/`--node` and `animusd
data --config` reach them via a config file's `cluster_settings.
auto_split_{bytes,change_rate,ops_rate}` section instead of a CLI flag
(S-06, `config.rs`'s own module-map entry has the full mechanism and the
"CLI flag and config field both set is a hard error" contract);
`--cluster-control`/`--cluster-data` and the standalone `control`/`join`
subcommands have no route to that config-file section, a documented gap
S-06 itself names. **`--tablet-max-read-units`/`--tablet-max-write-units`
(ADR 0067, W-08b) are a fourth trigger's knobs, but not opt-in like the
three above** — they default to 3000/1000 (DynamoDB's own per-partition
ceilings) rather than to "disabled"; `0` disables one dimension. Plumbed
identically to `--throttle-read-units`/`--throttle-write-units` (CLI flag,
`cluster_settings.tablet_max_{read,write}_units` config field, same "one
way, not both" conflict check) — see this file's own "Throughput-derived
minimum tablet count" entry above for the trigger itself. **`--node I` is gone from
`join`/`data --seed` entirely** — there is no index to derive a
default port range from, so `--base-port` is **required** on both. `--id
NAME` proposes a durable identity (`NodeId::propose` validates it at the
CLI boundary); omitted, the node **self-mints** one (`NodeId::mint`) and
claims it via `MetaCommand::RegisterNode`'s registration CAS — closing ADR
0032's documented residual race (two simultaneous joiners choosing the same
identity) structurally, not just by convention. A self-minted join is
**ephemeral-identity**: a restart with a fresh dir mints a *new* id, and
the old id's `Member` entry lingers `Down`/address-less forever (never
reused, prunable via the existing `RemoveMember`/decommission path). `--id
NAME`'s durable, restart-stable identity is unaffected.

**`--seed`'s entries accept a hostname, not just a literal socket address**
— motivated directly by the Kubernetes operator deployment target (root
`CLAUDE.md`'s architecture map): a seed Service's DNS name, not a pod IP,
is the honest address to hand a joining pod. Seed entries flow through the
join chain as `host:port` strings (`main.rs::parse_seed_arg` →
`run_node_join`/`run_node_data_join`) and resolve at dial time via
`TcpStream::connect`'s own `ToSocketAddrs` handling — there is no
pre-resolution step to go stale, and a not-yet-propagating DNS record
behaves like an unreachable seed under the existing
`JOIN_RETRY_INTERVAL`/`JOIN_DISCOVERY_BUDGET` retry cadence. Regression:
`tests/seed_join_hostname.rs` (a real `localhost:<port>` seed, proven
through the same `run_node_join` entry point every other join test uses).

**`main.rs` now handles SIGTERM, not just Ctrl-C/SIGINT** (`unix`-only
`wait_for_ctrl_c`, `tokio::signal::unix::signal(SignalKind::terminate())`
raced via `select!` against `tokio::signal::ctrl_c()`) — a Kubernetes pod's
`preStop`/termination path sends SIGTERM, and without this every call
site's `shutdown_graceful()` was unreachable on pod eviction. A failure to
install the SIGTERM handler only logs a warning (Ctrl-C alone still works);
the non-`unix` build keeps the old Ctrl-C-only fallback, since this
workspace is linux-first (root `CLAUDE.md`'s env note) and SIGTERM has no
portable non-unix equivalent.

## Deployment shapes (ADR 0035)

Three shapes (combined/control-only/data-only), all built from the same
role assemblies — see ADR 0035 for the full design. **There is no
engine-less control-plane deployment shape**: `BoundControlNode::
start_control_with` **unconditionally** provisions one small dedicated
system-keyspace engine, since `Metadata` is `StateMachine::DRIVER_APPLIED`
and this engine is the durable home of the control plane's async apply
task's published cache (see `animus-control/CLAUDE.md`'s `node.rs`/
`mirror.rs` entries).

**Console binding (ADR 0052) follows the same split as `dynamo`**:
combined and data-only bind `RoleAddrs.console` (real CP-data tablets to
show); control-only does not (`Node::bind_control` never reads
`addrs.console` at all, and `BoundControlNode::start_control_with` passes
`None` for `spawn_common_tail`'s `console_listener` — `Node::console_addr()`
panics there, mirroring `dynamo_addr()`'s existing
control-only-panics contract).

## Request routing (CP)

The `ClientCtx` primitives resolve the tablet's group leader the same way via
`cp_route` (pure core: `topology::decide_cp_route`): `cp_read` (linearizable
ReadIndex), `cp_scan` (linearizable range read), and the kind-write family
(`cp_kind_write_raw`/`cp_kind_write_item`/`cp_kind_write`, all Raft-committed
and waited to durable+applied). **The plain routed write primitives
(`cp_write`/`cp_delete`/`cp_put`/`cp_batch_write`) were deleted in Train A
rung 5 (ADR 0049)** — every write surface now rides the kind path, so no
production sender of a bare/`Forwarded` plain write remains; the plain
`KvCommand::Put`/`Batch`/`Delete` variants and `cp_serve_forwarded`'s serve
arms for them stay (internal machinery + wire compat; the local halves
`cp_put_local`/`cp_delete_local`/`cp_batch_local` back those arms). The
plain client protocol's `Put`/`PutBatch`/`Delete` arms commit through
`dynamo::marker_batch_write_raw` (one `KindBatch` per tablet, one image-less
marker per mutation, full-raw-key-as-prefix; `Put`/`PutBatch` auto-provision
like the old `cp_put` did, `Delete` deliberately never does).

**`poll_probe` is the shared durable-before-ack confirm wait behind
`cp_batch_local`** (its other caller, the now-deleted `cp_kind_local`,
went with ADR 0054 step 4b; `cp_kind_eval_local`'s own confirm loop
consults `classify_kind_batch_outcome` directly rather than going through
`poll_probe` at all — see that method's own doc) — it prefers
`animus-cp-data`'s
per-`KindBatch` apply-time outcome (`RaftKvNode::kind_batch_outcome`) over a
raw value re-read, since value equality alone can't distinguish "my entry
no-op'd" from "my entry applied and a concurrent write then overwrote it"
(the second is a success). **A recorded `Applied` outcome is trusted as a
confirm only when its own term matches the term `ProposeResult::Accepted`
handed the proposer** (`classify_kind_batch_outcome`, a small pure predicate
factored
out specifically so this identity check is unit-testable in isolation —
`kind_batch_signal_tests`, above) — `ProposeResult::Accepted{index}` means
"appended to my own log," never "committed," so an *accepted but not yet
committed* entry's index can be reoccupied by a completely different
command if this node loses leadership first (Raft log-matching), and that
reoccupying entry's own `Applied` outcome would otherwise be read as a
confirm of the *original* proposer's write — the false-ack found in review
of PR #334, closed by pairing the outcome with the entry's own Raft term
(index **and** term together identify one entry, cluster-wide).
`ConditionFailed`/`Sealed` need no such check (a no-op is a no-op
regardless of whose entry occupies the index). See
`docs/engineering-lessons.md` for the full incident and
`animus-cp-data/tests/kind_batch_outcome_identity.rs` for the seed-
reproducible truncation regression that proves it end to end.

**`poll_probe`'s value-equality fallback is idempotency-gated (issue #469).**
When `classify_kind_batch_outcome` is `Inconclusive` (not yet applied, aged
out of the bounded outcome map, or applied-but-not-yet-readable), `poll_probe`
used to fall back to plain value equality — "does the key already hold the
bytes I proposed?" — at both sites in its loop, unconditionally. That fallback
proves the bytes are *visible*, never that *this proposer's entry* put them
there, and for a **non-idempotent** write (a numeric `ADD`) that distinction
was load-bearing, not academic — this bug was found on the pre-ADR-0054
leader-evaluated design, where `kind_write_item_at_leader` read `old` under
`ctx.data().rmw_lock` (released *before* proposing, issue #285), so two
concurrent evaluators of the same key could read the identical stale `old`
and compute byte-identical `new` (a pure function of `(cur, delta)`) —
nothing downstream disambiguated them. Each proposed an entry carrying those
same bytes guarded by an own-key OCC seatbelt keyed to that same stale `old`;
the first to apply won the seatbelt and wrote the bytes, every later one
legitimately `ConditionFailed`ed. In the window after the winner's bytes were
visible and before a *loser* entry's own outcome was recorded, the old
ungated fallback matched on value alone and returned `Confirmed` for the
loser — acking an increment that never applied. Fixed by threading
`ProbeIdentity` (`ValueProves`/`RequiresOwnEntry`) down from whichever caller
already knows `dynamo::kind_write_is_idempotent`'s answer; `cp_batch_local`
hardcodes `ValueProves` since the raw `Batch` command only ever carries Put
semantics, never `ADD` — `poll_probe` never recomputes idempotency itself.
**`ProbeIdentity` outlived the mechanism it was built for**: ADR 0054
deleted `cp_kind_local`/`rmw_lock` and the leader-side read entirely, but
the same enum now gates `cp_kind_eval_local`'s own, analogous lost-payload
recovery (`kind_eval_confirmed`, `write_path.rs`) — whether a best-effort
re-read can stand in for a leader-local `old`/`new` slot that aged out from
under an unusually slow confirm; `kind_write_item_at_leader` still passes
the identical `kind_write_is_idempotent`-derived identity into that call. A
non-idempotent write's `Inconclusive` branch now never consults `local_get`
at all, at either site; it keeps polling `classify_kind_batch_outcome` for
its own (index, term) until that resolves or the deadline ends the wait in
`TimedOut`. Idempotent writes (Put/Delete/SET/REMOVE, a set union or
difference) keep both fallbacks exactly as they were — any entry landing
those exact bytes is a legitimate success regardless of whose entry it was.
Regression: `write_path.rs`'s in-crate `poll_probe_identity_tests` (a
`SimEnv`-driven single-voter `RaftKvNode` harness that proposes a second
`KindBatch` with the same bytes as an already-applied first entry, engineered
to legitimately `ConditionFailed`, and drives `poll_probe` directly for the
second entry's own accepted-but-unapplied window).

**`cp_scan_kind` (ADR 0041)** is `cp_scan`'s single-tablet, kind-scoped
sibling — the LSI `Query` read primitive: unlike `cp_scan`'s per-table
fan-out, `start`/`end` must resolve to the *same* tablet (an LSI query is
scoped to one base partition, hence one tablet, checked rather than assumed),
served locally via `RaftKvNode::linearizable_scan_kind` or forwarded via the
internal-only `ClientRequest::KindScan` (refused bare, exactly like
`KindWrite`; handled only inside `cp_serve_forwarded`). `cp_scan_kind_table`
is its table-wide fan-out sibling — the LSI `Scan` read primitive — issuing a
kind-scoped `KindScan` per overlapping tablet instead of a base one; `end:
None` (unbounded above) is legal on `KindScan` too, resolved inside
`RaftKvNode::linearizable_scan_kind` itself for the one tablet whose own
range is open-ended, never computed by the caller (no finite byte string
could do that job — see the DynamoDB wire-edge entry above).

**Eventually-consistent reads take a second route entirely (ADR 0055).**
`cp_read`/`cp_scan`/`cp_scan_kind`/`cp_scan_kind_table` each take a
`ReadConsistency` (`Strong`/`Eventual`, built from DynamoDB's `ConsistentRead`
via `ReadConsistency::from_consistent_read`). `Eventual` tries a cheap attempt
FIRST and falls through to the untouched linearizable loop on `None`, so the
strong path's behavior is bit-for-bit what it always was and the weak one can
never fail where the strong one would have succeeded:

- `cp_stale_local(tablet)` — serve from a **local replica**, leader or not, if
  this node is a voter in the group's own durable Raft config (the same check
  `resolve_cp_route` makes), the key/range is inside the handle's live
  `scope_range()`, and `CpGroup::stale_read_ready()` passes. **Deliberately no
  `wake()`** — unlike `resolve_cp_route`'s wake-on-demand edge; an eventual
  read needs no Raft activity and a quiesced group is fully applied by
  construction (ADR 0048 fork F's "reading never wakes anything," now extended
  from diagnostics to a real client read path).
- otherwise `cp_stale_forward_target(tablet)` — **any** replica's intra
  address, and `relay_stale_read` sends ONE `Forwarded` frame with a short
  `STALE_READ_FORWARD_TIMEOUT` (2s). Deliberately **not**
  `forward_to_tablet_leader`: there is no leader to chase, a refusal
  (`STALE_READ_REFUSAL`) means "not cheaply, then", and waiting out an
  election to serve a stale read is incoherent.

**Testing gotcha this created (ADR 0055).** Two shapes in this crate's
`tests/` tree stopped being implicitly safe. (1) **A read that verifies a
write** must ask for `ConsistentRead: true` — the wire default no longer
guarantees read-your-writes, and the failure is a *race*, so one green run of
a binary proves nothing. (2) **A read loop that rotates across nodes** — the
deliberate round-robin several pagination suites do to exercise the forwarded
path — is only stable if every node it touches agrees: consecutive pages now
sample different, independently-lagging replicas. Fix by asking for the strong
read, or (a GSI rejects it) converging on *every* address first
(`dynamo_query_pagination.rs::await_gsi_query_everywhere`). Both are in
`docs/engineering-lessons.md`'s Testing section.

**Observability**: `Metric::CpEventualReadsLocal`/`CpEventualReadsForwarded`/
`CpEventualReadsFellBack` (`/metrics`'s `cp_eventual_reads_*`), recorded by
`ClientCtx::record_eventual_read` — a no-op on a control-only node, which has
no data-role sink. The fallback counter is the one that matters: a high rate
means the cheap path is silently not being taken, and **nothing a client sees
would reveal it** (the reads are still correct, just expensive). None of the
three measures *staleness* — see ADR 0055's Consequences for that named gap.

The wire carries it as `#[serde(default)] stale: bool` on `ClientRequest::
Get`/`Scan`/`KindScan` rather than three new variants — a field is caught at
every construction site by `error[E0063]`, where a new variant would need ADR
0047's exhaustive `surface_of` table and every gating allowlist updated by
hand. `cp_serve_forwarded` splits each of those three arms on `stale`
(`true` → `cp_stale_local`-or-refuse, never a re-forward or a wait; `false` →
the unchanged leader arm). `animus get-eventual` is the plain-protocol client
form. **What stays `Strong` regardless of what a client asked** is ADR 0055
§6's list — transaction preconditions/`ConditionCheck` reads,
`TransactGetItems` (`cp_read_snapshot`, which has no eventual path at all),
and `await_table_serveable`'s readiness probe; add to that list in the ADR
when adding to it here.

**`cp_kind_write_raw` does NOT auto-provision a table's first tablet** —
unlike `cp_kind_write`/`cp_kind_write_item`/`cp_txn`/`marker_batch_write_raw`
(with its provision flag set), which all do. (`cp_batch_write_patient` was
deleted in Train A rung 4 with its one caller, the admin seeder — its
poll-not-repropose retry lore lives in the seeder's own comment; the plain
`cp_write`/`cp_delete`/`cp_put`/`cp_batch_write` primitives this paragraph
used to contrast went in rung 5.) A caller
targeting a table nothing upstream has provisioned must call
`provision_tablet` itself first, or `cp_route` waits out `CLIENT_TIMEOUT` on
a tablet that will never exist and fails — every tick, forever, if the
caller is a retrying loop (the ADR 0041 GSI drain hit exactly this; see
`docs/engineering-lessons.md`).

`cp_route` serves **locally** if this node hosts the leader; **forwards** one hop
(`ClientRequest::Forwarded { request, traceparent }`) to the leader's node if a
local replica gives a hint + a `client_route` exists; otherwise **waits** for the
local group to elect (never forwards to a non-leader, including itself, during
election). **One-hop invariant**: the receiver (`cp_serve_forwarded`) never
re-forwards.

**Hinted-retry forwarding** (`ClientCtx::forward_to_tablet_leader`, the single
choke point for every forward — `cp_forward` is its (table, key)-resolving
wrapper, and every **tablet-id-addressed** internal RPC (`ForceSeal`,
`TriggerAutoSplit`, `ClearBackfillCursor`, `StreamHotRead` — `SeedRows`, the
copy-based split-build driver's own tablet-addressed RPC, was deleted in
Layer B1) calls it directly): a "not the leader here" refusal carries the
refusing node's own leader hint (`topology::format_not_leader_refusal`, a
plain string suffix so old and new binaries interoperate); the chase retries
at the hint if untried, else at another of the tablet's known replicas,
bounded to one pass over {hint} ∪ replicas within the overall
`CLIENT_TIMEOUT`. The tablet-addressed RPCs used to relay once and
re-resolve from scratch instead — which never converges when the calling
node hosts **no replica** of the target tablet (the fallback
deterministically re-picks the first metadata replica; the deleted
copy-based split driver seeding an off-node fork-F5 child spun on exactly
this forever, parking the split — see `docs/engineering-lessons.md`). A new
tablet-addressed RPC must forward through this choke point, and its test
suite needs a caller hosting no replica of the target, which only a cluster
larger than RF can produce.

**A dead candidate is chased too, not just a wrong-but-reachable one
(issue #316, fixed).** The hinted-retry fix above only helps when the
guessed candidate is alive and answers with a real refusal — a plain
**transport** failure (the candidate crashed/was killed) used to be
terminal: `relay_request_with_timeout` folds every connect/write/read
failure into one sentinel string (`RELAY_TRANSPORT_FAILURE`), which
doesn't parse as a "not the leader here" refusal, so the pre-fix chase
gave up on the very first unreachable hop instead of trying another known
replica. Since the guess itself is deterministic (the no-local-replica
fallback and a refusal's own embedded hint are both plain reads, never
liveness-checked), a caller that keeps re-resolving and re-forwarding (the
deleted copy-based split-build driver's own per-tick retry, formerly
`seed_child_rows`) kept reproducing the identical dead end forever once its
first guess or hint chase happened to land on a node that had since died —
the confirmed root cause of the deleted `tests/split_build.rs::
split_survives_losing_one_childs_leader_mid_build`'s reported hang. Fixed
by giving a transport failure the identical "no hint" treatment a
live-but-mid-election refusal already gets (try another known replica),
rather than a terminal return. Regression:
`forward_transport_failure_tests::
forward_to_tablet_leader_survives_a_dead_first_guess` (`lib.rs` — **not**
beside `forward_to_tablet_leader` in `forwarding.rs`, whose module carries
a hard `#[deny(clippy::disallowed_methods)]`, ADR 0061 Phase C's closing
rung, that a real-socket test's `tokio::time` calls would trip; probed via
`ClientCtx::clear_backfill_cursor_for_table` since Layer B1 deleted
`seed_child_rows`, the original probe — see that test's own updated doc).
See `docs/engineering-lessons.md`'s matching entry for the full incident,
including why this sandbox could never reproduce the original hang live
(fast localhost + a small dataset's bulk pass usually outracing the
test's own 30s victim-detection poll) yet the fix was still provable
red-before/green-after via a fully deterministic isolation of the exact
mechanism.

**A reachable-but-slow candidate can starve the chase too, not just a dead
one (issue #585, fixed).** Issue #316's fix above only helps once a
transport failure has actually happened; each hop's own transport timeout
used to be `remaining = deadline.duration_since(self.env.now())` — the
*entire* time left before the chase's own overall deadline — handed whole
to a single `relay_request_with_timeout` call. A candidate that accepts the
TCP connection but is merely slow (a loaded sandbox, a starved disk) or
simply never answers isn't a transport failure at all until its own
timeout fires, so it could consume the *whole* remaining `CLIENT_TIMEOUT`
budget on that one hop — the loop would then find `now >= deadline`
immediately after and give up having tried exactly **one** replica, even
with two other live replicas (one the tablet's real leader) reachable in
well under a second the whole time. Observed as a bimodal `admin_endpoint`
failure in a loaded sandbox — a different test each time, always
`RELAY_TRANSPORT_FAILURE` — never reproduced on CI. Fixed by capping every
hop's own transport timeout to `FORWARD_HOP_TIMEOUT` (2s,
`remaining.min(FORWARD_HOP_TIMEOUT)`, `lib.rs`) instead of the whole
remaining budget — see that constant's own doc for the sizing rationale
(comfortably above a healthy hop's real round trip, while still leaving
room for several more hops inside `CLIENT_TIMEOUT`). **The identical
per-candidate-gets-the-whole-timeout shape also existed in
`ClientCtx::propose_schema`'s own broadcast-to-every-known-address chase**
(the ADR 0030 growth-node fallback, `schema.rs`) — fixed the same way,
calling `relay_request_with_timeout` directly with `FORWARD_HOP_TIMEOUT`
instead of `self.relay`'s flat `CLIENT_TIMEOUT`. `remote_metadata_watch_
loop`'s long-poll (`WATCH_METADATA_CLIENT_TIMEOUT`, deliberately *longer*
than `CLIENT_TIMEOUT` since it must outlive the serving node's own
`WATCH_METADATA_SERVER_TIMEOUT` park) is the one call site that
legitimately needs its own long single timeout and was left unchanged —
see that constant's own doc. Regression:
`forward_hop_timeout_tests::forward_to_tablet_leader_bounds_each_hop_so_a_
slow_candidate_cannot_starve_the_chase` (`lib.rs`, beside
`forward_transport_failure_tests` above and for the identical reason — a
real-socket `ProdEnv` test, since `forwarding.rs` carries the same hard
`#[deny(clippy::disallowed_methods)]`): a real raw `TcpListener` stub
rebinds the deterministic first-guess replica's own former intra address
and accepts every connection but never writes a reply, proving the fixed
chase recovers via a live replica in a few seconds rather than dead-ending
near the full `CLIENT_TIMEOUT`. See `docs/engineering-lessons.md`'s
matching Testing entry for the general lesson (a hint-chasing forward's
per-candidate timeout must be a bounded slice of the overall deadline,
never the whole remaining budget). **This specific test still can't move
to `SimEnv`** even though `forward_to_tablet_leader` itself is now relay-
generic (ADR 0061 rung C3d) — what it actually proves is
`AnimusdRelayClient`'s own real-transport timeout behavior
(`relay_request_with_timeout`'s `RELAY_HOP_TIMEOUT`/
`RELAY_TRANSPORT_FAILURE` split against a real, deliberately-stalling
`TcpListener` stub), which has no `SimEnv` analogue to test against —
`SimRelayClient`'s own timeout races `env.sleep()` against a `Pending`
poll, a different mechanism with its own coverage (`animus_node::
sim_relay::tests`), not a second implementation of this one.

**The hop cap above regressed the property it was supposed to leave
intact — a genuinely slow-but-LIVE leader must still be waited out (issue
#585, continued, fixed).** Capping every hop closes the starvation hole,
but the fix's own `RELAY_TRANSPORT_FAILURE` sentinel folded two different
causes together: a candidate confirmed dead *within* budget (a fast
connect/write/read failure) and a candidate whose hop merely *ran out* of
its own `FORWARD_HOP_TIMEOUT` before any answer arrived — reachable, just
slow. `tried` treats both identically: permanently excluded from the rest
of the pass, including from ever being the target of another replica's own
hint. Under a real membership-change storm (five voters, some being
added/removed), the group's actual leader can legitimately take **several
seconds** to commit — comfortably inside `CLIENT_TIMEOUT`, past one
`FORWARD_HOP_TIMEOUT`. The first hop to that leader times out and marks it
`tried`; every other replica's own "not the leader here" refusal then
names that *same* address as its hint — filtered out every time by
`!tried.contains(a)` — so the chase falls through to the tablet's other
(non-leader) replicas over and over, burning a full round's worth of
`FORWARD_HOP_TIMEOUT`s before `ForwardRetryStep::WaitElection`'s
`tried.clear()` ever lets it circle back, routinely enough to blow the
whole `CLIENT_TIMEOUT` budget on a leader that would have answered in a
few seconds. Observed live in CI as a transient ~3s leaderless window
during a real 5-voter placing reconfiguration
(`tests/split_placing_two_replica_diff_e2e.rs`), surfacing as a
`RELAY_TRANSPORT_FAILURE` put error — the write simply ran out of
retries, having spent the whole time on real (if temporarily
non-leading) candidates.

Fixed by telling "confirmed dead" and "timed out" apart **at the
source**: `relay_request_with_timeout` (`lib.rs`) now returns one of two
distinct sentinels depending on which half of `tokio::time::timeout`'s own
result fires — `RELAY_TRANSPORT_FAILURE` when the inner connect/write/read
genuinely fails *within* budget (`Ok(None)`, fast, confirmed dead), or the
new `RELAY_HOP_TIMEOUT` when the whole attempt merely outlives `timeout`
(`Err(_)`, `tokio::time::error::Elapsed` — nothing failed, there just
wasn't an answer *yet*). `forward_to_tablet_leader` tracks
`RELAY_HOP_TIMEOUT` addresses in a second set, `timed_out`, alongside
`tried`. The actual candidate-resolution decision moved to a new pure,
unit-tested function, `decide::resolve_forward_candidate` (ADR 0061 A6,
`animus-node`): a hint naming an address in `timed_out` is retried
immediately, even though that address is also in `tried` — the strongest
possible signal that node is the live leader — ahead of exploring any
fresh replica; once every known replica is genuinely exhausted with no
hint left, the chase falls back to retrying a `timed_out` candidate
directly (its own doc has the full priority order and rationale for why
`timed_out` must be a separate set rather than folded into `tried`). This
keeps BOTH properties at once: a reachable-but-silent candidate still
cannot consume the whole budget (every other known replica is always tried
first, the original #585 property), and a genuinely slow-but-live leader
is still waited out within `CLIENT_TIMEOUT` rather than permanently
written off after one capped hop (the pre-#585 property this continuation
restores).

Regression: `forward_hop_timeout_tests::
forward_to_tablet_leader_waits_out_a_slow_but_live_leader_instead_of_giving_up`
(`lib.rs`, beside the sibling test above, for the identical reason —
`forwarding.rs` carries the hard `#[deny(clippy::disallowed_methods)]`).
**All three** of the tablet's replicas are replaced with stubs (not just
the first guess): the first-guess stub stalls its first connection past
`FORWARD_HOP_TIMEOUT` (a `RELAY_HOP_TIMEOUT`) but answers a later
connection immediately; the other two each answer — after a real ~1.8s
delay of their own, modeling a busy cluster where *every* hop costs real
time — with a refusal hinting at the first guess. A version of this test
with the other two replicas answering *instantly* still passes against
the reverted (pre-this-fix) candidate logic too, since PR #106's
pre-existing `WaitElection` backoff already self-heals a fully-exhausted
pass and an instant self-heal round costs only `FORWARD_ELECTION_BACKOFF`
(100ms) — the real ~1.8s-per-hop delay is load-bearing for making a full
extra round genuinely expensive, matching the real production failure
shape. Confirmed red-before/green-after by temporarily reverting
`forward_to_tablet_leader`'s candidate resolution to the pre-fix
`hint.filter(|(_, a)| !tried.contains(a)).map(..).or(other)` shape: the
test then fails the elapsed-time assertion deterministically, every run
(a plain unit test of the same decision — timeout vs. refusal vs. a hint
naming a timed-out node — lives in `animus-node`'s own
`decide::tests::resolve_candidate_*`, no sockets involved). See
`docs/engineering-lessons.md`'s matching entry for the sandbox-flakiness
angle this investigation surfaced (most reproduction attempts under a
heavily loaded, resource-constrained sandbox fail on unrelated causes —
FD exhaustion, connection reset/refused — not the mechanism under test).

**The second fix let the chase RETURN to a slow-but-live leader — but every
hop it's retried at, hinted or not, was still individually capped at
`FORWARD_HOP_TIMEOUT` (issue #585, a third continuation, fixed).** Fixing
*which* candidate the chase picks is not the same as fixing *how long* that
candidate's own attempt is allowed to run. Each retry is a **fresh**
proposal on the receiving leader, not a resumed wait on the original one, so
if the leader genuinely needs longer than `FORWARD_HOP_TIMEOUT` to commit on
*every* attempt (a membership-change storm serializing its proposes ahead of
this one, not just the first attempt), a capped retry can never outlast the
leader's own real commit latency no matter how many times the chase
correctly circles back to it — every attempt restarts the clock and dies at
the identical wall. Confirmed live in CI:
`tests/split_placing_two_replica_diff_e2e.rs`'s paced writer failed `put
failed: Error("no CP group leader reachable")` twice out of two runs with
the second fix already in place, during the 5-voter → 3-voter trajectory
where the leader moves n2 → m0 — the chase was demonstrably resolving to
the *right* candidate (the second fix's own property held) and still never
succeeding, because every return trip to it was capped exactly like an
ungrounded first guess.

Fixed by giving the forward-candidate decision a classification, not just
an address: `decide::ForwardCandidate::Hinted`/`Guessed` (`animus-node`).
`Hinted` means some live replica named this address as the leader — this
node's own local replica's `cp_leader_hint` for the very first hop
(`CpRoute::Forward`'s new second field), or a refusal's own embedded hint
thereafter, including the `timed_out` last-resort retry once every known
replica is exhausted (nothing else is left this pass to protect a cap
from). `Guessed` means a deterministic pick with zero liveness signal — the
no-local-replica fallback, or an untried replica the chase reaches for on
its own. `forward_to_tablet_leader` now caps a hop at `FORWARD_HOP_TIMEOUT`
only when its candidate is `Guessed`; a `Hinted` one gets the full remaining
`CLIENT_TIMEOUT` budget, exactly like every hop did before the first #585
fix. This keeps the starvation fix intact — a `Guessed` candidate is always
tried, capped, before the chase ever falls back to retrying one a second
time — while finally letting a genuinely slow-but-live leader actually
finish once the chase is pointed at it with a real vouch behind it, rather
than being retried at it forever under the same wall every time. See
`decide::ForwardCandidate`'s own doc (`animus-node`) and
`forward_to_tablet_leader`'s own doc (`forwarding.rs`) for the full
per-case reasoning.

Regression: `forward_hop_timeout_tests::
forward_to_tablet_leader_does_not_cap_a_hinted_retry_of_a_genuinely_slow_leader`
(`lib.rs`, beside the two sibling tests above, for the identical reason).
**Why the second fix's own test above didn't already catch this**: its
stub (`spawn_slow_then_ok_stub`) special-cases only the *first* connection
to stall — every later connection, retry included, answers instantly — so
whether that retry's own hop happened to be capped or not never mattered to
that fixture; it proves candidate *selection* (the chase returns to the
right address) but nothing about *how long the return trip is allowed to
run*. This test's own stub (`spawn_always_slow_ok_stub`) instead answers
**every** connection — first attempt and retry alike — only after a delay
past `FORWARD_HOP_TIMEOUT`, matching the real production shape (a standing
commit-latency cost on every attempt, not a one-time slow start), so only a
genuinely uncapped hinted retry can succeed inside `CLIENT_TIMEOUT`. See
`docs/engineering-lessons.md`'s matching entry for the general lesson (a
fixture built to prove one fix in a mechanism can stay green through a
regression in a different dimension of that same mechanism unless it
faithfully models that dimension too) and for this investigation's own
repro-loop findings (two sandbox-only `"Too many open files"` failures out
of ten post-fix runs of the flaky e2e test above, zero occurrences of the
actual pre-fix failure signature).

**Election-wait backoff (PR #106)**: when *every* candidate refuses with
`leader_hint=none` (the group is mid-election — a split-child/first-provision
formation window, or a crashed leader), one exhausted pass is not a failure.
`cp_forward` backs off `FORWARD_ELECTION_BACKOFF` (100ms, ≈ one election timeout,
lib.rs:470) and re-runs the pass, still hard-bounded by `CLIENT_TIMEOUT` — the
forwarded dual of the local path's `RouteDecision::Wait`. Gated on the tablet
being resolvable so an unmappable op still fails fast. Regression:
`tests/cluster_split.rs::single_shot_first_write_through_control_node_succeeds`.

**Write fences are GONE (ADR 0050 Train B rung 7).** A tablet's declared
range is immutable from birth, so the per-entry `fence` fields, the
pre-propose `scope_range()` checks, and the `*_fenced` proposers were all
deleted; the plain `put`/`put_batch`/`put_kind_batch_conditioned`/`delete`/
`cas` proposers are the only shapes left. What replaces the fence's job:
route-time filtering (`Building` tablets unroutable, `Active`-only serving)
plus the frozen split parent's retryable refusal (`frozen_refusal` +
`KvCommand::Freeze`'s apply-time whole-range seal backstop). One cheap
pre-propose key∈declared-range guard survives in the kind-write path purely
as a routing-bug tripwire (immutable range, no lock).

**A routed operation's error policy must be identical on `CpRoute::Local`
and `CpRoute::Forward` (issue #572).** `force_seal_tablet`/
`force_pitr_seal_tablet` (`schema.rs`) resolve the tablet leader, then split
on whether that leader is this node (`Local`) or another one (`Forward`);
the `Forward` arm has always retried a transient `ClientResponse::Error`
inside its own `loop { .. deadline .. }` (`SCHEMA_COMMIT_TIMEOUT`/
`SCHEMA_POLL_INTERVAL`). The `Local` arm used to call
`index_drain::seal_now`/`pitr_seal_now` once and return the first `Err`
verbatim — but that call has a documented dueling-seal race against the
periodic `seal_tick`/`pitr_tick` arm (same tick, `INDEX_DRAIN_INTERVAL`):
when the periodic arm commits the same `(tablet, next_epoch)` slot first
with a shorter range, the call returns a transient `"; retry"`-suffixed
error (`index_drain::is_retryable_elsewhere`, made `pub(crate)` for exactly
this reuse), not a permanent one. Both `Local` arms now retry that same
error class inside their own `loop`, using the identical classifier the
periodic-arm-retry code elsewhere already relies on, so a request that used
to succeed or 500 depending on nothing but whether it happened to land on
the tablet leader's own node now behaves the same either way. When adding
or touching *any* `ClientCtx` method with a `Local`/`Forward` split, check
that a transient failure gets the same retry treatment on both arms —
"passes only when forwarded" is the same bimodal-per-process-flake family
as the missed-forwarding-allowlist lesson (see CLAUDE.md's "When adding a
variant to a replicated/forwarded command enum" note and
`docs/engineering-lessons.md`).

**The same error family shows up a third time, in a route that is neither
`Local` nor `Forward` (issue #580).** `inplace_split_driver_tick`'s own two
final-seal exhaustion loops (`while seal_now(..)?.is_some() {}` and the
`pitr_seal_now` twin, immediately inside `change_consumer_loop`'s
`Splitting` arm) call the identical `seal_now`/`pitr_seal_now` primitives
above and are exposed to the identical dueling-seal race against the
periodic `seal_tick`/`pitr_tick` arm — but used `?` to propagate *any*
`Err`, transient included, aborting the whole tick (every downstream veto
and the `CutoverSplit` propose) rather than just retrying the loop. Fixed
the same way as #572: `continue` on `index_drain::is_retryable_elsewhere`,
propagate everything else. The lesson generalizes past "Local vs. Forward"
— any two call sites that can independently invoke the same racy primitive
must classify its errors identically, however different their own control
flow looks; grep for other `seal_now`/`pitr_seal_now` (or any primitive
documented as having a dueling-write race) call sites before adding a new
one, not just the two `ClientCtx::force_*` methods this was first found in.

## Multi-participant transactions (ADR 0018 §2)

`ClientCtx::cp_txn(writes, preconditions, write_conditions) ->
Result<HlcTimestamp, String>` is the coordinator for a cross-tablet atomic
transaction, reachable via `ClientRequest::Txn`. See ADR 0018 §2 (and its
follow-up amendments) for the full 2PC-over-Raft protocol, anchor/
participant roles, and recovery semantics (prepare/decide/resolve,
`txn_resolver_loop`, in-doubt recovery) — this section states only the two
animusd-specific rules that aren't in the ADR.

**Internal-only `ClientRequest` variants — `TxnPrepare`/`TxnDecide`/
`TxnResolve`/`TxnStatus`/`TxnRecordView`/`TxnVerify` — are never sent
bare**, only wrapped in `Forwarded`; their real handling lives in
`cp_serve_forwarded`'s match only. **Since ADR 0047 all six ride the intra
port** (`Surface::Intra`) alongside `Forwarded` itself — a bare send, or a
`Forwarded`-wrapped send, on the client port is refused by the port guard.
**Routed by the actual data key** being
staged/resolved/verified (`table` + `writes[0]`/`keys[0]`/`span.start`),
**never `record_key`** for `TxnPrepare`/`TxnResolve` — a non-anchor
participant's `record_key` names the anchor's record, which lives in a
*different* tablet's (possibly a different table's) keyspace entirely.
`TxnDecide`/`TxnStatus`/`TxnRecordView` always target the anchor's own
tablet, so routing by `record_key` there is correct. These are data-plane
RPCs, not `MetaCommand`s — `is_relayable_command` does not apply to them.

**`ClientCtx::recovery_resolve` groups a decided transaction's
`intent_spans` by `(table, tablet)`, re-resolving each key's own current
tablet immediately before grouping** (ADR 0018 §2 write-loss amendment,
Bug 3) — never by table name alone, which used to bundle a split table's
two different tablets' keys into one `txn_resolve_participant` call
routed by the bundle's first key alone, silently misrouting the rest onto
the wrong tablet's shared physical key (ADR 0028). `cp_txn`'s own
`resolve_all` was never affected (it builds its own `(table, tablet)`-keyed
map directly from the per-participant stage calls it just issued, never
regrouping through `intent_spans`); only the `txn_recover`/`txn_resolver_
loop` recovery path went through the buggy grouping. `KvCommand::
TxnResolve`'s own `fence` (`animus-cp-data/CLAUDE.md`'s Key invariants
entry) is the structural seatbelt against a repeat of this specific
mistake, in this function or any future caller.

**Fixed (issue #298, confirmed 2026-08-26, closed the same day)**:
`txn_recover`'s `all_staged` loop used to fold a `txn_verify` `Err` (most
commonly a transient "no CP group leader reachable" while a participant's
tablet is mid-fork/cutover) into the same bucket as a genuine `Ok(false)`
("never staged"). Under a high split cadence this could push recovery to
Abort a transaction whose own coordinator (`cp_txn`) was concurrently
deciding, or had already decided, Commit — a live instance of the
"duelling decider" hazard ADR 0018 §2/PR5 accepts as legal only because
both deciders are assumed to reach an objectively correct decision from
independently verified state; an unconfirmed `Err` breaks that assumption.
Caught live (a captured `all_staged=false`/`Aborted` decision immediately
preceding an "acked write lost" panic) during a `SplitMode::InPlace`-
unpinned soak. **Fix**: any `Err` now makes the whole recovery push
*inconclusive* — `txn_recover` declines (`Pending`, proposes nothing)
rather than ever letting an unconfirmed span feed a decision; a
`txn_resolver_loop`-local grace tracker logs+meters a transaction stuck
inconclusive well past `RECOVERY_GRACE` (`Metric::
CpTxnRecoveryStuckInconclusive`), a pure liveness signal. A **sibling**
conflation was found and fixed in the same pass: `RaftKvNode::
txn_record_view` (the primitive the orphan-record branch reads) had the
identical shape one level up — see `animus-cp-data/CLAUDE.md`'s matching
entry and `docs/engineering-lessons.md`'s amendment for the full account,
including why an `Err`/`None` audit must cover every query a decision is
built on, not just the first one found. Regression:
`animus-cp-data/tests/txn_record_view_served.rs` (the fixed primitive's own
"served" contract) and the mirrored fix in `animus-test/tests/
txn_serializable.rs`'s own `push`/`resolver_tick`.

**`ClientRequestToken` idempotency for `TransactWriteItems` (ADR 0018's
2026-08-24 amendment; the 2026-08-27 amendment closing issue #298's "deep
shape A" residual)**: `dynamo.rs::run_transact` preflights a token against a
durable `token → (fingerprint, outcome)` record on a reserved internal
table (`animus_dynamo::internal_tables::TXN_IDEMPOTENCY_TABLE`) — a
conditional claim `Put` guarantees the transaction itself executes **at
most once** per token, independent of anything the outcome bookkeeping
records. The 2026-08-27 amendment fixed the bookkeeping's own remaining bug:
`run_transact`'s `cp_txn` call site used to record `TXN_IDEMPOTENCY_
CANCELLED` for **every** `cp_txn` failure, including a genuinely **ambiguous**
one (`TxnAbortReason::is_ambiguous` — a `"; retry"`-suffixed `Other`, e.g. a
leader move mid stage or a `StageOutcome::Fenced` naming a concurrent
in-doubt-recovery decision) where the transaction may in fact have committed
via a path this exact call never observed — the false-negative half of the
"an unconfirmed outcome is UNKNOWN" defect class this section's issue #298
entry above already fixed twice in `txn_recover`'s own queries, now fixed a
third time in the `ClientRequestToken` outcome cache. Fixed: `run_transact`
now retries `cp_txn` internally (bounded by `CLIENT_TIMEOUT`, a fresh `TxnId`
each attempt) ONLY for the narrow, **allowlisted** subset of ambiguous
reasons proven to occur before any propose for this transaction could have
applied (`TxnAbortReason::is_safe_to_retry_fresh` — a frozen-tablet
refusal, no route reachable, a leader-side read failure, `Fenced`'s
stage-time structural causes); every other ambiguous reason (including
every DECIDE-phase confirmation loss — a leader move during anchor commit/
abort/resolve) is never retried, since a confirmed decide, unlike a
confirmed stage, fully materializes the write. Either way, if the outcome
stays ambiguous, the idempotency record is left `PENDING` (self-healing via
the ADR 0051 TTL reaper) rather than ever recording a possibly-wrong
`CANCELLED`; the client gets a genuine, SDK-tolerated
`TransactionInProgressException`, never a false `TransactionCanceledException`.
**The allowlist shape is load-bearing, not a style choice**: an earlier
denylist-shaped version of this fix (excluding only the two known
stage-time messages) missed the decide-phase messages entirely and
reproduced the exact duplicate-delivery bug live in this amendment's own
proof-soak — see ADR 0018's 2026-08-27 amendment for the full account
(including why this didn't need the alternative "derive `TxnId` from the
token" design) and `animus-dynamo/CLAUDE.md`'s own entry for the
wire-level mechanism.

**A wire-reachable panic found (and fixed) while testing this**:
`RaftKvNode::txn_stage`'s anchor-key-length assert (ADR 0022, `TOKEN_BYTES`)
was a sound "caller invariant" before `ClientRequest::Txn` existed — no
untrusted caller could reach it with an arbitrary key. `cp_txn` now
validates every write's key length up front and returns a client-facing
error instead of ever reaching that assert. See `docs/engineering-
lessons.md` for the general lesson.

**A write against an indexed/streamed table participates too (2026-08-16,
ADR 0046 A1/U3, `TxnStage` kind-writes stack)** — `dynamo.rs::run_transact`
no longer rejects it. `TxnTableWrite` carries either an already-known
`value` (a plain table's write) or a `pending: PendingKindWrite` (a
kind-write-path table's write: the item identity + op + condition, no
coordinator-computed diff). `ClientCtx::txn_stage_local` — the ONE place a
stage actually executes on the leader's own node, shared by `txn_prepare`'s
own local branch and `cp_serve_forwarded`'s `TxnPrepare` arm — turns every
`pending_kind_writes` entry into a self-contained `TxnWrite::pending`
payload there, with no read/evaluation at all (ADR 0054 step 4a,
2026-09-05): `KvCommand::TxnStage`'s own apply arm evaluates every
pending write itself, in commit order, reusing the identical evaluator
`KvCommand::KindEval` uses. `dynamo::eval_kind_txn_write` — the original
leader-side evaluator this producer called under `ctx.data().rmw_lock`
before that step, plus the mandatory own-key OCC condition (ADR 0046
Fork C1) it used to build alongside — is deleted outright; see ADR 0054's
step 4a as-built amendment for the full design. For a
transaction touching any kind-write-path table, `cp_txn`'s post-commit
resolve is **awaited under a short bounded budget**
(`TXN_RESOLVE_ALL_AWAIT_BUDGET`) and parallelized across participants
(`resolve_all_parallel`) instead of the plain transaction's unchanged
fire-and-forget spawn (Fork D1) — LSI rows and the GSI/stream change
record only exist from resolve onward (materialize-at-resolve, ADR 0046
A1), so an unconditional async-ack window would leave a committed write
transiently absent from its own index/stream. **Two bugs found and fixed
delivering this** (see `docs/adr/0018-cross-tablet-transactions.md`'s
2026-08-16 amendment for the full incidents): a genuine self-deadlock
(`run_transact` used to hold `rmw_lock` across its own `cp_txn` call,
which now recurses into the same node-local lock the instant a write
targets a locally-led kind-write-path table); and parallelizing
`resolve_all` *universally* (not just for the new bounded-await path)
destabilized a pre-existing timing-sensitive regression
(`dynamo_txn.rs`'s torn-pair test) — fixed by keeping `resolve_all`
sequential and adding `resolve_all_parallel` as a scoped sibling.

Tests: `tests/cp_txn.rs` (real 3-process cluster). The 2PC mechanics
themselves are proven deterministically at the primitive level in
`animus-cp-data`'s `tests/txn_multi.rs`/`tests/txn_recovery.rs`, and (ADR
0046) `tests/txn_kind_writes.rs`. The kind-write-path extension's own wire-
level coverage is `tests/dynamo_index_writes.rs`/`tests/dynamo_streams.rs`
(replacing the wholesale-rejection tests they used to carry) and
`crates/animus-test/tests/txn_serializable.rs`'s corpus (a
`kind_consistency` invariant) / `tests/stream_lineage_corpus.rs`'s
`transactional_writes_exactly_once_and_ordered` cell.

## Control-plane access

`ClientCtx.control` is a `ControlHandle`, not a bare `RaftNode`. Reads split by
freshness contract:

- `metadata_cached()` — staleness-tolerant. `effective_metadata()` layers the ADR
  0030 growth-node / data-only mirror on top.
- `metadata_fresh()` — read-your-writes, never mirror-substituted; **`async`** (a
  real round trip for `Remote`). Used by schema commit-wait polls, the DynamoDB
  conditional-write existence gate, and `provision_tablet`'s initial replica-set
  read.

For `Local` the two are identical (`raft.metadata()`); `Remote` genuinely differs
(mirror vs. network fetch). **Proposing is inherently local-Raft-log-only** —
`ClusterEdgeState::leader_handle()` stays a concrete `RaftNode` registry and never
goes through `ControlHandle`; `Remote` returns inert honest values for
`is_leader()`/`term()`/etc.

**`config()` returns `Option<BTreeSet<NodeId>>`, not a bare set (ADR 0037).**
`Local` is always `Some(raft.config())`. `Remote` has no local `RaftCore`,
so it answers the last control-voter set it has *observed on the wire*
(`RemoteControlClient::control_voters`) — `None` until the first
`Status`/`WatchMetadata` reply lands. Deliberately an `Option`, not an
always-populated `BTreeSet::new()` default: "never fetched yet" and "the
control group genuinely has zero voters" must stay distinguishable to any
caller that cares (see the engineering-lessons "handle has no local
authority" entry) — most callers just `.unwrap_or_default()` it.
`ClientResponse::Status` carries `control_voters` — the wire echo of the
*live* Raft config that actually governs quorum, distinct from
`Metadata.node_addrs`' `role: "control"` bookkeeping (a node can be
registered with the control role and not currently be a live voter). It
rides the same round trip `metadata_fresh()` already makes, so `Remote`
picks it up for free — the intended reader is a caller that needs "who can
I even try talking to."

**Discipline**: a read feeding a *non-retried, permanent* decision must use
`metadata_fresh()`, not `metadata_cached()`/`effective_metadata()` — a
data-only node's routinely-stale mirror makes that window wide. The type
system can't catch this (`Remote` and `Local` both compile). Grep every
`metadata_cached()` call site when adding a `ControlHandle` consumer.
`provision_tablet` was fixed for exactly this (RF silently pinned at 1);
see the root `CLAUDE.md` engineering-lessons log. **That fix only closed
the READ side — a deeper hazard recurred later under heavy concurrent
load**: `provision_tablet`'s `SetTabletPolicy` no longer derives a
tablet's RF from `t.replicas.len()` (the observed size of its *initial*
replica set) at all — it always records the fixed target
`MAX_REPLICATION_FACTOR`, so a best-effort under-sized initial set
self-heals via `reconcile_placement` rather than the observed size
becoming a silently-permanent policy. See `tests/tablet_rf_self_heals.rs`.

**`Remote` internals** (`RemoteControlClient`): `seeds` (the control deployment's
client-API addresses), a polled `mirror`, and a `leader_hint`. `metadata_fresh()`
tries the hint first, else scans every seed. `ClientResponse::Status` carries
`leader_hint` and a `watermark: u64`; the long-poll `ClientRequest::WatchMetadata
{ last_seen }` gives a `Remote` node a real wake-on-commit signal via
`remote_metadata_watch_loop` (a genuine `Local` replica serves it, parking on
`metadata_watch().changed(last_seen)` up to an 8s server bound; a `Remote` node
rejects it outright). `RemoteControlClient` owns its own driven `MetadataWatch`.
The ADR 0030 growth-node branch of `remote_metadata_sync_loop` uses this same
long-poll mechanism rather than a fixed poll — a growth node's
`ClientCtx.control` stays `ControlHandle::Local` (a real, permanently
non-voting control-group member, not `Remote`), so it constructs a standalone
`RemoteControlClient::with_mirror` sharing `ClientCtx.remote_metadata`'s
`Arc<Mutex<Option<Metadata>>>` directly, then drives it through the same loop.

**Gotcha**: a `WatchMetadata` request already in flight to a node at the
instant it's killed via `Node::shutdown()` doesn't fail over quickly —
`shutdown()` can't abort an already-spawned `serve_clients` per-connection
handler task (fire-and-forget, no tracked `JoinHandle`), so the zombie
handler's `select! { changed(..), sleep(8s) }` always falls through to the
timeout arm and replies with stale-but-plausible cached data up to 8s late.
A fixed-sleep assertion right after a test's node-kill can be outrun by
this; poll to convergence instead (see the engineering-lessons log).
**This specific scenario is unchanged by the issue #596 cancellation**
(see the "`handle_connection` cancels an in-flight request" entry in the
module map above): #596 detects the *caller's* socket closing, and here
nothing closes it — the server process being killed doesn't touch the
already-open TCP connection or the untracked handler task sitting on it,
so the zombie still runs its full `WATCH_METADATA_SERVER_TIMEOUT`. What
#596 *does* fix is the more common case where the caller itself gives up
on the connection (a client-side timeout tearing down the socket, or a
plain disconnect) — that zombie now exits as soon as the close is
observed instead of running the full server-side budget regardless.

**`WatchMetadata`'s reply is incremental (ADR 0038).** After the long-poll
resolves, `ClientCtx::watch_metadata` tries the serving node's own
`RaftNode::watch_delta_since(last_seen)` first: if its bounded delta ring
(`animus_control::DeltaRing`) contiguously covers `(last_seen, watermark]`,
the reply is a cheap `ClientResponse::MetadataDelta { writes, watermark,
leader_hint, control_voters }` instead of a full `Status` clone. Falls back
to a full `ClientResponse::Status` whenever the ring doesn't cover the
range **or** while this node's own ADR 0030 growth-node mirror overlay is
active (that overlay serves `effective_metadata()` from a different source
than this node's own local ring). `RemoteControlClient::observe_delta` is
the **single shared consumer** for both a genuine data-only `Remote` node
and the growth-node branch above, installing each `KeyWrite` onto the
cached `Metadata` via `animus_control::mirror::apply_key_write`. **Race
guard**: since `RemoteControlClient` is `Arc`-shared between the background
watch loop and any concurrent `metadata_fresh()` caller, a delta is only
applied if the mirror's *current* watermark exactly equals the delta's own
`last_seen` basis — a concurrent full `observe()` moving the mirror in the
meantime makes sequential delta application unsafe; a stale delta is
dropped, not mis-applied, and self-heals on the loop's next iteration.
Regression: `tests/watch_metadata.rs` and `tests/cluster_growth.rs::
growth_node_observes_metadata_promptly_via_watch`.

## Tablet lifecycle

**The per-node tablet-host reconciler (ADR 0031) is the single owner of
this node's tablet lifecycle.** The pure `plan` decision + `Reconciler`
executor live in `animus_cp_data::host` (read that crate's `CLAUDE.md` for
the mechanism, including the fixed action order — tablets are split-only,
ADR 0044; merge's dual `WidenScope`/`Absorb` actions were removed). What
stays in `animusd` (`tablet_host_reconciler_loop`):

- **Trigger**: one task per node racing `ctx.control.metadata_watch().changed(..)`
  (event-driven — observes a change on the commit that made it) against a
  `RECONCILE_FALLBACK_INTERVAL` (500ms) sleep. The fallback is **load-bearing for
  growth / data-only nodes** whose local control Raft never advances (their watch
  never fires; the mirror is read via `effective_metadata()`). Coalesce to
  `watch.latest()` after a wake so a commit burst collapses to one tick.
- **Pre-recovery guard**: skip while `raft.last_applied() == 0` **and** the remote
  mirror is empty (default-empty `Metadata` would read as "everything dropped").
  A data-only node needs the third signal `has_synced_metadata()`.
- **Edge mirror**: `ClusterEdgeState`'s `raftkv` registry is a read-only mirror
  with exactly one writer — the reconciler's `on_host`/`on_teardown` hooks.
- **Formation**: `Epoch::INITIAL` (or `StorageScope::has_data` on restart) ⇒ full
  voter config; a bumped epoch ⇒ quiet non-voter until the leader adds it. Dedup
  is `LocalState::hosted`.

**Auto-split (byte-based, ADR 0034)**: `auto_split_loop` gates per-tick on
`CpGroup::approx_bytes` (either backend). The split point is always
`decide::byte_weighted_median` (`animus-node`, unit-tested in that crate's own
test module — see `animus-node/CLAUDE.md`) — which scans every achievable
key-boundary cut for the one closest to half the bytes, not a single
accumulate-and-threshold pass (subtly wrong when one key dominates; see the
root log). **The former key-count trigger (`--auto-split K`) and its plain
positional-median split point were removed** — bytes (plus, for streamed
tables, change-rate, and for any table, ops-rate — both below) cover every
use case key count did, with no key-count-specific failure mode left to
justify a fourth independent knob;
`CpGroup::approx_key_count` itself is unchanged and still backs `/admin/
raftkv`'s informational `key_count` display. **Tablets are split-only
(ADR 0044)** — there is no merge, automatic or operator-driven, to trigger;
a tablet's count only ever grows, and reversing an over-eager split is no
longer possible (see that ADR's "shrink-in-place" note).

**Change-append-rate trigger (opt-in, ADR 0042 §14 Fork F, growth PR3)**:
`--auto-split-change-rate RATE` joins the same either-fires gate above,
streamed tables only. `CpGroup::approx_bytes` is deliberately base-scoped
(ADR 0034's own fix), so it structurally cannot see change-log churn — a
high-churn, small-footprint streamed table would otherwise never gain a
second shard regardless of write rate. `ChangeRateTracker` (`lib.rs`)
closes the gap for free: `index_drain::seal_tick` already computes
`approx_bytes_kind(KIND_CHANGE)` every tick for `Metric::StreamHotBytes`,
so the tracker just EWMA-smooths each tick's own delta/elapsed into a
bytes/sec estimate — no new scan. Read via `ClientCtx::stream_change_rates`
(`/admin/metrics`'s `stream_change_rates` array) and
`ChangeRateTracker::get` (the trigger check itself). When hot, splits via
the identical `byte_weighted_median`/`trigger_split` path every other
trigger uses, so F11/Fork E apply automatically. No production-tuned
default exists — omitting the flag is a true no-op.

**Request-rate trigger (opt-in, W-09, ADR 0034's own deferred bullet,
closed)**: `--auto-split-ops-rate RATE` joins the same any-fires gate
above, applicable to **any** table — unlike change-rate, this one needs no
change log, so it isn't streamed-tables-only. `RequestRateTracker`
(`lib.rs`, sharing `ChangeRateTracker`'s own `RateSample` EWMA shape) is
observed at **two** choke points together covering every leader-side
non-transactional write: `dynamo::kind_write_item_at_leader` (the ADR 0046
U3 evaluate-at-leader funnel — a condition, an old-image echo, or an
images-carrying table) and `dynamo::fast_marker_write` (the ADR 0049 fast
arm — an unconditioned `Put`/`Delete` on a plain, unindexed/unstreamed
table, which never reaches the funnel at all). Missing either one leaves
the signal blind to a real write shape; the fast arm in particular is the
*common* case for an ordinary plain-table write, so it is not optional.
Each successful write ticks the tablet's own tracker once, converting
inter-write timing into a smoothed ops/sec estimate — no separate counter
plumbing. Read via `ClientCtx::request_rates` (`/admin/metrics`'s
`request_rates` array) and `RequestRateTracker::get` (the trigger check
itself). **Writes only, never reads** — an eventually-consistent read (ADR
0055's wire default) is served from any replica and never reaches the
leader at all, so folding reads in would silently undercount a tablet whose
hot path is reads spread across replicas; a hot-but-small tablet under
heavy write load — the exact gap ADR 0034 deferred — is fully visible
through writes alone. When hot, splits via the identical
`byte_weighted_median`/`trigger_split` path every other trigger uses. No
production-tuned default exists — omitting the flag is a true no-op.

**Throughput-derived minimum tablet count (ADR 0067, W-08b, a direct ADR
0065 follow-up)**: a fourth trigger, `min_tablets.rs`'s pure
`min_tablets_for(throughput, ceilings) = max(1, ceil(RCU/max_rcu +
WCU/max_wcu))` — DynamoDB's own up-front partition-sizing formula.
**Unlike every trigger above, its two ceilings are on by default, not
opt-in**: `--tablet-max-read-units`/`--tablet-max-write-units` (3000/1000,
DynamoDB's own defaults; `0` disables a dimension), threaded through
`ClusterSettings`/`AdminInfo` the identical way
`throttle_read_units`/`throttle_write_units` are. `auto_split_loop` is
therefore now spawned **unconditionally** on every node shape (previously
gated on at least one of the three opt-in triggers) — a cheap lock-free
check (no opt-in trigger configured **and** `ClientCtx::
any_table_throughput` false) is the first statement of every tick, so an
unprovisioned cluster's added cost is one atomic load per
`AUTO_SPLIT_INTERVAL`, not a metadata read. Runs as a **separate, per-table**
pass after the per-tablet byte/change-rate/ops-rate pass: for every table
with its own `ProvisionedThroughput` (`Metadata::table_throughput`) whose
current `Active` tablet count sits below its derived minimum, forks the
**widest** (by approximate token range, `min_tablets::token_range_width`)
`Active` tablet of that table this node leads — at most one split per table
per tick, per node — converging in `min - 1` ticks on a single-leader
deployment (linear, since only one split happens per table per tick
regardless of tablet count), faster when a table's tablets are spread
across several leaders. A table with `>= 2`
materialized pairs splits via the same `byte_weighted_median` every other
trigger uses; a table with fewer (the common case immediately after
`CreateTable`, before any write — the byte/rate triggers above never even
reach a tablet this sparse) instead splits at `min_tablets::
midpoint_split_key`'s synthetic **token midpoint** of the tablet's own
range — a plain 8-byte token, inherently satisfying `KeyRange::split_at`'s
strict-interior requirement whenever the range spans more than one token
(a single-token range returns `None` and this tick's candidate is skipped,
never a doomed propose). Tables without their own `throughput` are never
touched. Deliberately does **not** skip a quiesced candidate (unlike the
three triggers above) — this trigger fires on a *configured* value, not
observed activity, so quiescence's "nothing could have changed" premise
doesn't apply; reading a quiesced group's local pairs is itself a safe,
wake-free scan (ADR 0048 fork F). **No cap on the derived minimum** — a
huge provisioned value legitimately means many tablets; quiescence keeps
an over-provisioned table's idle extras cheap, not a shrink mechanism
(tablets are still split-only, ADR 0044 — raising throughput mints more,
lowering it never merges back down). New metric:
`Metric::AutoSplitMinTablets`, incremented on every successful split this
arm triggers. See `docs/adr/0067-*.md` for the full design.

**Per-table throttling (ADR 0065, W-08 — all four steps landed)**: `ThrottleTracker`
(`lib.rs`, beside `ChangeRateTracker`/`RequestRateTracker`) is the
admission-control sibling of the rate trackers above — a per-tablet token
bucket in DynamoDB capacity units (`ThrottleBucket`: `tokens`/`rate`/
`capacity = 300 × rate`/`last: Nanos`, clocked exclusively on `env.now()`),
not a reporting-only estimate. **Unlike `ChangeRateTracker`/
`RequestRateTracker`, `ThrottleTracker` lives directly on `ClientCtx`, not
behind `DataRole`'s `Option`** — every `SimEnv` `ClientCtx` fixture in this
crate (`simenv_client_ctx_tests`, `two_node_relay_tests`, `sim_cluster.rs`)
constructs `data: None`, and this ADR's own testing section needs a real
`SimCluster`-driven virtual-clock corpus to exercise admission decisions, so
a `DataRole`-gated tracker could never be reached by any of them.
`ClientCtx::throttle_limits_for(meta, table) -> ThrottleLimits` resolves the
effective `read_units`/`write_units` limits: `meta.table_throughput(table)`
(the table's own replicated `TableSchema.throughput`, ADR 0065 §5(b)) when
set — **no per-field merge**, a table with its own spec ignores the cluster
default entirely, even in whichever direction (read/write) its own spec
leaves `None`-equivalent — else the cluster-wide default
(`ClientCtx::throttle_defaults`, a lock-free `AtomicU64`-backed pair —
`None`/`None` means `PAY_PER_REQUEST`, the default, byte-for-byte unchanged
from before this ADR). A tablet's own per-tablet **share** is the table's limit divided
by its current tablet count (`meta.tablets_for_table(table).count()`,
re-derived fresh on every check, never cached — a split re-divides the
budget the instant the new tablet map commits) — the identical
"re-derive from live `Metadata`, never a snapshot" discipline
`RequestRateTracker::retain_existing` already follows. Enforced strictly
*before* propose (never inside Raft apply, ADR 0054's determinism
requirement) at the same two write choke points `RequestRateTracker`
observes plus the transaction stage point, and read-side at whichever node
serves the read (leader for `ConsistentRead: true`, any replica for
`false`) — see this ADR's own Decision 2 for the full enforcement-point
list and `dynamo.rs`/`write_path.rs`/`read_path.rs`/`txn_coordinator.rs`'s
own entries below for exactly where each check lives. `ThrottledWrites`/
`ThrottledReads` (`animus-env::Metric`) count every refusal; `/admin/
metrics`'s `throttle` array (`ClientCtx::throttle_snapshot`) mirrors
`request_rates`'s own shape, one entry per currently-tracked tablet (tokens,
rate, throttled counts, read and write separately). When no limit is
configured anywhere (the overwhelming common case — no cluster-wide
default and no table anywhere with its own `throughput` set), every
enforcement point costs **at most one atomic load plus one `Option`
check: no lock, no `BTreeMap` lookup, no `Metadata` clone** (step 5, ADR
0065 §5(b), restoring the lock-free fast path step 4 had to drop). Step 4
correctly removed the step-3 fast path — a lock-free peek at
`ClientCtx::throttle_defaults` alone, before ever touching `Metadata` —
because it could only ever see the cluster-wide half of ADR 0065 §5's
two-layer configuration: a per-table override with no cluster default set
would otherwise silently never throttle. But the replacement (fetch
`Metadata` unconditionally) was more expensive than its own "the identical
cost the surrounding write/read path already pays for routing" framing let
on: `effective_metadata()` locks a `Mutex` and deep-clones the **entire**
`Metadata` — the whole tablet map, schema catalog, and backup catalog —
not a cheap `Arc` bump, so step 4 put that clone on the hot path of every
raw write and every read even on a cluster that throttles nothing at all.
Step 5 closes that gap with `ClientCtx::any_table_throughput: Arc<
AtomicBool>` — `true` iff at least one table in this node's last-observed
`Metadata` has `throughput.is_some()` — checked together with
`throttle_defaults` via `ClientCtx::throttle_maybe_configured()` **before**
`throttle_check_write_raw`/`throttle_precharge_read` ever call
`effective_metadata()`. The flag is recomputed (never guessed) at
`ClientCtx` construction (from the initial `metadata_cached()`), on every
tick of the metadata-watch loop (`index_drain::change_consumer_loop`,
alongside `self.throttle.retain_existing(&meta)`), and — so the node that
itself served the DDL doesn't wait a whole watch tick — immediately after
this node's own `CreateTable`/`UpdateTable(ProvisionedThroughput)` commit
(`dynamo::create_table`/`update_table_throughput`). `Relaxed` ordering
throughout: this is a pure hint, so a reader observing a stale value for
one more request either falls through to `effective_metadata()` one
request later than ideal (never earlier) or pays one avoidable
`effective_metadata()` call before the next recompute clears it — neither
is a correctness hazard. `kind_write_item_at_leader`/`txn_stage_local`
needed no such flag either at step 4 or since — both already had
`Metadata` in hand for other reasons (routing, schema resolution) before
ever checking throttle limits, so neither ever called
`effective_metadata()` a second time just for throttling. See
`docs/engineering-lessons.md`'s matching entry for the general lesson (a
per-request deep clone hidden behind an innocuous accessor name is a cost
to design around with a cheap invalidated flag, not to accept as
"already paid elsewhere").

**Enforcement points, as built**: `write_path.rs`'s
`cp_kind_write_raw` (the ADR 0049 fast marker arm, `throttle_check_write_raw`
— `throttle_maybe_configured()` first, then `Metadata` and
`throttle_limits_for` only if that says something might throttle);
`dynamo.rs`'s `kind_write_item_at_leader` (the evaluate-at-leader funnel —
pre-charge before propose, a post-charge correction once the real
`ConsumedCapacity` is known from the applied outcome); `read_path.rs`'s
`cp_get_local_resolving`/`cp_read_eventual*`/`cp_scan_one*`/
`cp_scan_kind_one*` (both the linearizable and ADR 0055 eventual arms — a
throttled eventual read returns the refusal directly, **never** silently
falls through to the linearizable retry path, which would defeat the whole
point of a cheap admission check); and `txn_coordinator.rs`'s
`txn_stage_local` (2× cost per pending kind-write, mirroring
`ConsumedCapacity::scaled(2.0)`'s own transactional-write accounting —
refusal surfaces as `TxnAbortReason::Throttled { table, key }`, mapped to a
`CancellationReasons[].Code == "ThrottlingError"` entry in `run_transact`'s
existing cancellation-reason match). A transactional write's pre-charge has
**no post-charge correction** (unlike the single-item funnel) — the
coordinator has no cheap point to reconcile actual `ConsumedCapacity` back
into the bucket once a transaction commits, so a transactional write's
charge is always its estimated cost, never trued up; this is a deliberate,
minor over/under-charge tolerance, not a bug (see `docs/engineering-
lessons.md` if this ever needs revisiting).

**`BatchWriteItem`'s throttle granularity depends on the table shape it
hits, and this is a real, user-visible difference, not a test artifact.**
A **plain** (unindexed, unstreamed) table's batch commits as ONE
`KindBatch` Raft entry per tablet (the ADR 0049 fast-arm batching
`marker_batch_write_raw` already does for throughput) — so a throttle
refusal there is necessarily **all-or-nothing per tablet-group**: either
the whole per-tablet group of requests fits the tablet's current token
balance, or every request in that group is shed to `UnprocessedItems`
together, even if the balance could have covered some of them individually.
An **indexed or streamed** table's batch instead routes each request
through the per-item evaluate-at-leader funnel, one throttle check per
item, so a partially-exhausted budget sheds only the specific items that
don't fit. `Operation::BatchWriteItem`'s handler (`dynamo.rs`) reflects this
directly: the marker arm maps a whole shed tablet-group's base keys back to
their original `WriteRequest`s via a `BTreeMap`, while the per-item arm
catches a `ProvisionedThroughputExceededException` one request at a time.
`BatchGetItem` has no such split — every key is always checked
individually regardless of table shape, since a read never batches into one
entry the way a marker write does.

**The config surface, in two layers (step 4, landed)**:

- **(a) Cluster-wide default** (ADR 0065 §5(a)): `ClusterSettings`
  (`config.rs`) gained `throttle_read_units`/`throttle_write_units`
  (`Option<u64>`, `#[serde(default)]`, the same shape/per-role
  applicability every other data-hosting knob there has). `main.rs`'s
  `--throttle-read-units N`/`--throttle-write-units N` CLI flags thread
  through `resolve_cluster_settings`'s identical "one way, not both"
  conflict check and, from there, `run_node_with_cluster_settings`/
  `run_node_data_with_cluster_settings`/`start_cluster_with_growth_and_
  quiesce_after` (the exact `--auto-split-ops-rate`/W-09 layered-wrapper
  precedent: widen the one real caller and the innermost layer that
  actually builds `AdminInfo`, pass `None`/`None` through every other
  wrapper). `AdminInfo` carries `throttle_read_units`/`throttle_write_units`
  too, surfaced on `/admin/config` and `/admin/metrics` beside
  `auto_split_ops_rate_threshold` — `spawn_common_tail` reads them back off
  the already-built `admin_info` to seed `ThrottleDefaults::new(..)`, rather
  than threading a second, parallel pair of parameters through that
  function. `animus-operator`'s `desired::cluster_config::ClusterSettings`
  mirrors the two fields for JSON shape parity (same as
  `auto_split_ops_rate`'s own precedent) — no `AnimusClusterSpec` field
  exposes either yet, so the operator never populates them.
  `ClientCtx::set_throttle_defaults`/`POST /admin/throttle/defaults` remain:
  a genuinely useful **live override** on top of the durable config, not
  replaced by it.
- **(b) Per-table** (ADR 0065 §5(b)): `animus_control::schema::
  ProvisionedThroughput { read_units, write_units }` (modeled on `TtlSpec`,
  no identity label) sits on a new `TableSchema.throughput: Option<..>`
  field; `MetaCommand::SetTableThroughput { table, spec }` (modeled on
  `SetTableTtl`'s idempotent-on-identical-value apply semantics) mutates it,
  and is on `is_relayable_command`'s allowlist (`animus-node/src/wire.rs`)
  and `mirror.rs`'s schema-catalog-class mirror bucket beside `SetTableTtl`.
  `animus_dynamo::wire`: `CreateTable` decodes `BillingMode`
  (`PROVISIONED`/`PAY_PER_REQUEST`, default `PAY_PER_REQUEST` — deliberately
  **not** real DynamoDB's own legacy `PROVISIONED` default, matching this
  adapter's pre-existing "never inspects `BillingMode`" behavior for the
  unthrottled case) + `ProvisionedThroughput` (required, both units `>= 1`,
  for `PROVISIONED`; rejected alongside `PAY_PER_REQUEST`); `UpdateTable`
  accepts the identical pair as a *third* mutually-exclusive change
  alongside a stream or index change (`reject_billing_mode_combined_with_
  other_change` — a bare `BillingMode: PAY_PER_REQUEST` restatement
  alongside a real stream/index change is still tolerated, the pre-existing
  precedent; `PROVISIONED` or a bare `ProvisionedThroughput` combined with
  either is rejected as "more than one change"). `CreateTable`/
  `DescribeTable`/`UpdateTable`/`DeleteTable`'s shared
  `table_description_object` renders `BillingModeSummary`/
  `ProvisionedThroughputDescription` (`PAY_PER_REQUEST` reports 0/0 units +
  `NumberOfDecreasesToday: 0`, matching real DynamoDB). `dynamo.rs`:
  `create_table` bakes `throughput` into the schema it proposes (no separate
  `SetTableThroughput` needed at create time); `update_table_throughput`
  (mirroring `update_time_to_live`'s commit-wait shape) proposes
  `SetTableThroughput` for an `UpdateTable` throughput change.
  `console_table_detail`/`console.js`'s Settings tab render a read-only
  "Capacity" fact strip (billing mode + units) — no console mutation route,
  matching the PITR section's own read-only precedent.

**Test coverage**: `sim_cluster_throttle.rs` (`#[cfg(test)] mod`, `cargo
test -p animusd --lib sim_cluster_throttle`) is the `SimEnv`-driven,
virtual-time-only corpus proving the bucket lifecycle (admit a burst,
refuse, recover after a full 300s window) through `SimCluster`'s real
route/propose/confirm and route/local-resolve loops, for both writes and
eventual/linearizable reads — see that file's own module doc for why a
per-op virtual-clock refill (`SimCluster`'s `OP_BUDGET`) means a throttle
test's per-op cost must clear that refill by a wide margin, not just the
nominal configured rate; `SimCluster::set_table_throughput` (proposing
`MetaCommand::SetTableThroughput` on the control leader and
converged-or-timeout polling every node, the identical shape
`create_table_with_replication`'s own tail uses, then a
`SimClusterHandle::recompute_any_table_throughput_all()` call so every
node's own `ClientCtx::any_table_throughput` reflects the freshly
converged catalog — this fixture never spawns
`index_drain::change_consumer_loop`, the real cluster's own recompute
site, so nothing else here would ever flip the flag) backs one dedicated
per-table-override-wins-over-a-restrictive-cluster-default cell, plus
`any_table_throughput_flag_tracks_the_catalog_through_set_and_revert`
(step 5's own flag proof: `false` on a fresh table, `true` once a
per-table `throughput` is set, `false` again once it reverts to
`PAY_PER_REQUEST`). `lib.rs`'s own `simenv_client_ctx_tests` module has a
companion unit test,
`unconfigured_throttle_checks_return_ok_from_the_default_state` — with
nothing configured on a fresh `ClientCtx`, both `throttle_check_write_raw`/
`throttle_precharge_read` return `Ok` from the guard clause alone (there is
no clean way to assert "no clone happened" directly, so this test instead
pins the default flag/defaults state the guard's correctness rests on;
the guard's placement as each function's first statement is a
code-review invariant, documented on both). `sim_cluster_dynamo_update_
table.rs` (`#[cfg(test)] mod`, `cargo test -p animusd --lib sim_cluster_
dynamo_update_table`, ADR 0061 rung D3 PR 2b) is `sim_cluster_throttle.rs`'s
own sibling for the DynamoDB-wire throughput-*config-surface* shapes
(`CreateTable`/`UpdateTable`/`DescribeTable`'s `BillingMode`/
`ProvisionedThroughput` handling), driven through `dynamo::dispatch_table_
op`'s `UpdateTable` arm rather than `sim_cluster_throttle.rs`'s own direct
`set_table_throughput`/`put`/`get` fixture bypasses: `CreateTable`'s own
declared `ProvisionedThroughput` throttling with **no** admin call at all,
`UpdateTable` to `PAY_PER_REQUEST` lifting a limit, `UpdateTable` raising
units eventually admitting more (a bounded loop of further `SimCluster::
dynamo` calls, not a one-shot assert — `ThrottleBucket::set_rate` refills
at the OLD rate through the moment of the change and only then raises the
ceiling, so the very next check after a raise still pays that reassignment
at the old rate; each further call already advances the cluster's own
virtual clock by `OP_BUDGET`, so no explicit `run_for`/sleep is needed
between attempts), `DescribeTable`'s `BillingModeSummary`/
`ProvisionedThroughputDescription` for both billing modes, and
`MetaCommand::SetTableThroughput`'s own follower-relay regression. `tests/
dynamo_throttling.rs` is the real-thread, real-socket regression for
everything that stays — `ADR 0061 rung D3` moved its own single-item
write/read throttling-and-recovery pair to `sim_cluster_throttle.rs`'s
`write_admits_a_burst_then_refuses_then_recovers_after_a_full_refill`/
`read_admits_a_burst_then_refuses_then_recovers_after_a_full_refill`, and
PR 2b moved the five config-surface tests just described to
`sim_cluster_dynamo_update_table.rs` above (see that module's own doc for
the exact list) — leaving this file the shapes with no sim analog:
`ProvisionedThroughputExceededException`'s wire shape (still exercised via
the forwarded-write and admin-metrics tests below),
`BatchGetItem`'s `UnprocessedKeys`, `BatchWriteItem`'s
`UnprocessedItems` (via a **streamed** table specifically, to get true
per-item granularity rather than the marker fast-arm's per-tablet-group
shape described above), `TransactWriteItems`' `ThrottlingError` cancellation
reason, a forwarded write throttled on the actual leader, the
`ThrottledWrites`/`ThrottledReads` metric counters (never incrementing
under `SimCluster` — see `sim_cluster_throttle.rs`'s own module doc for
why), an unthrottled table (the default) staying byte-for-byte unaffected,
and a cluster started with the `cluster_settings` config surface
(`bring_up_with_throttle_defaults`, calling `run_node_with_cluster_
settings` directly rather than `POST /admin/throttle/defaults`) throttling
a table with no per-table setting while a table with its own higher
override is not — the one step-4 test with no sim analog, since it proves
a CLI/config-file surface no `SimCluster` fixture reaches.

**Manual growth trigger (`POST /admin/stream/grow {table}`, ADR 0042 §14,
growth PR3)**: splits *every* tablet of a streamed table at its own
byte-weighted median in one action (`ClientCtx::grow_stream` →
`grow_stream_tablet` per tablet, reusing the identical
`local_pairs`/`byte_weighted_median`/`trigger_split` primitives). A tablet
led by a different node than the one serving the admin request is reached
via the internal, relayable `ClientRequest::TriggerAutoSplit` RPC (mirrors
`ForceSeal`'s shape — addressed by tablet id, refused bare, handled only in
`cp_serve_forwarded`). A per-tablet skip (Fork E's single-token limit, an
empty/singleton tablet, or — since ADR 0050 rung 6 — a mid-split tablet:
a `Splitting` parent or `Building` child classifies up front as
`STREAM_GROW_MID_SPLIT`, never routed to and never miscounted as a split
this call performed) is reported in that tablet's own response entry,
never escalated into a whole-call failure. `animus admin stream-grow
<admin-addr> <table>` is the CLI form.

`grow_stream`'s per-tablet loop walks a `Metadata` *snapshot* taken once up
front but awaits real Raft/network activity per iteration, so a tablet
captured `Active` in that snapshot can be retired by a **cascade** split
(one tablet's cutover racing another's still-pending turn in the same
walk) before this loop reaches it — issue #454. `grow_stream_tablet`'s own
"no such tablet" lookup miss for exactly that tablet is not a real error:
it means the split this call would have triggered already happened, one
beat early. `schema::classify_grow_response` folds that exact message into
the identical `STREAM_GROW_MID_SPLIT` skip, deliberately narrow (only that
literal message, only on `grow_stream`'s own call path) so a genuinely
unknown tablet id elsewhere (e.g. `POST /admin/tablet/split`) still errors.

**Split is the ADR 0058 in-place workflow's METADATA half (Train 2 rung
3, directed by ADR 0062) — the sole split mechanism since the copy-based
workflow was deleted whole 2026-09-01** (the copy-split-deletion stack,
Layers A/B1/B2; see `docs/adr/0058-*.md`'s 2026-09-01 as-built note):
`trigger_split` — still the one choke point every surface calls — proposes
`MetaCommand::BeginSplitInPlace` (parent → `Splitting`, still fully
serving; no tablet-map rows minted for the children at all — **since ADR
0062**, both children's replicas are fixed as the parent's own CURRENT
replicas, verbatim, identical for both, never placement-chosen at this
step; a child's eventual final home is a separate directed-Placing
decision made later, at `CutoverSplit`'s own apply) and confirms by
observing the parent's own **state** become `Splitting` — never an
epoch-advance (a rebalance CAS also bumps the epoch; a stray bump re-arms
the CAS instead). Kickoff is **asynchronous and idempotent**: success
means the workflow *started*; a `Splitting` parent returns success
immediately. From there the whole workflow completes with no further
`animusd`-level proposal needed to *start* it: the CP data plane's own
host reconciler (`animus_cp_data::host`) adds learners, catches them up,
and forks the parent's group entirely on its own (ADR 0058 rung 3), and
`index_drain.rs::inplace_split_driver_tick` watches for that fork, runs
the pre-cutover GSI-drain/backfill-seeder vetoes, and proposes
`CutoverSplit` once they pass — see `animus-cp-data/CLAUDE.md`'s "In-place
split" entry for the data-plane half this bullet doesn't cover.
Per-tablet `state` rides `/admin/status`'s serialized `Metadata` (the
split-status surface — no new endpoint); placement (reconcile + rebalance)
is frozen for the whole mid-split set. Both the copy-based workflow's own
metadata command (`MetaCommand::BeginSplit`) and the older zero-copy
`MetaCommand::SplitTablet` it itself superseded (ADR 0028, deleted at
Train B rung 7) are gone — `MetaCommand::BeginSplitInPlace`/`CutoverSplit`
is the only pair a caller ever proposes. E2e: `tests/inplace_split_e2e.rs`
(a real 3-node cluster, a paced continuous writer riding the whole
fork→cutover window, every acked write readable afterward on whichever
child now owns it) and `admin_endpoint.rs::
admin_split_in_place_children_inherit_the_parents_own_replicas` (asserts
the ADR 0062 fork-first replica inheritance directly off the parent's
recorded intent, without waiting for the fork/cutover to complete).
(Merge — `MetaCommand::MergeTablets` and the reconciler's `WidenScope`/
`Absorb` reaction — was removed entirely by ADR 0044, superseding ADR
0033.)

**`ClientCtx::trigger_split` is the ONE choke point every split proposer
calls** (`auto_split_loop`, `admin::action_split`, and
`ClientRequest::SplitTablet`'s handler — nothing else ever builds a
`MetaCommand::BeginSplitInPlace`), which is where F11 (ADR 0042 §14) rounds a
streamed table's split key down to its own 8-byte token boundary
(`align_split_key`, private to `lib.rs`, unit-tested in
`align_split_key_tests`) — a manual split can no longer separate one
partition's records across sibling tablets the way it could before growth
PR2 moved the rounding out of `auto_split_loop` alone.
`MetaCommand::BeginSplitInPlace`'s own apply arm independently re-checks token
alignment on a streamed table as the ADR 0028 fence-idiom seatbelt (never
the primary enforcement). A token-rounded key that collapses onto the
target tablet's own `range.start` (a single very hot partition token owning
the whole tablet) is the accepted single-token hot-partition limit (ADR
0042 §14 Fork E): `trigger_split` returns immediately (no propose attempt)
and increments `Metric::StreamSplitSingleTokenSkipped`; `auto_split_loop`
matches that specific error to skip its own "split did not commit" warning,
which would otherwise fire every cooldown, forever. Regression:
`tests/f11_split_alignment.rs` (a follower-connected admin split with a
deliberately unaligned key, red on the pre-PR2 code).

**Drop-table GC** (ADR 0024) is the reconciler's `Reclaim` action;
**removed-replica GC** (ADR 0029) is its `Release` dual — see
`animus-cp-data`'s `host.rs`/`CLAUDE.md` for the mechanics
(`erase_scope`/`erase_bound`). Drop + GC are convergent (a restart replays
through historical map states) — test post-restart state with a poll,
never a fixed sleep. A new `MetaCommand` that must commit from a
follower-connected node must be added to `is_relayable_command` (missing
there is a bimodal per-process flake).

**`ClientCtx::drop_table` cascades to every GSI's hidden table (ADR 0041).**
A GSI's rows live in a *separate* table (`animus_dynamo::index_table_name`)
with its own tablets, so dropping only the base table's schema + tablets
would orphan it forever. The three steps run in a load-bearing order: (1)
read `metadata_fresh` and drop each **global** index's hidden table's
tablets via the same `MetaCommand::DropTableTablets` the base table itself
uses; (2) drop the base schema; (3) drop the base table's own tablets (base
+ colocated **LSI** rows + change log + footprints — every kind lives in
the tablet's own private engine, so the reconciler's `Reclaim` deleting
that engine's files reclaims every kind at once (ADR 0050 rung 1); an LSI
needs no separate cascade step). A crash between any two steps leaves a state a re-run of
`drop_table` completes, since every step is independently idempotent.
**Belt-and-suspenders second sweep**: the GSI drain (`index_drain.rs`)
provisions a hidden table's first tablet lazily and can race a drop, so
after step 3 `drop_table` re-scans the tablet map itself (not the now-gone
`IndexDef`s) for any tablet named `<table>$<index>` and drops those too —
which also mops up any orphan a pre-fix drop left behind. Regression:
`tests/drop_table_index_cascade.rs`.

**`dynamo.rs::drop_index` (ADR 0045 §5) is `drop_table`'s single-index
sibling** — `UpdateTable`'s `GlobalSecondaryIndexUpdates` `Delete` path,
not `drop_table`'s own DROP-TABLE-wide cascade. Same idempotent-steps/
belt-and-suspenders shape, one index instead of every one, plus a fourth
concern `drop_table` doesn't need: `SetIndexStatus{Deleting}` first (so the
drain/seeder stop touching the index before anything is torn down) and
`ClientCtx::clear_backfill_cursor_for_table` (run twice) to keep a stale
backfill cursor from poisoning a later same-named recreate — see
`index_drain.rs`'s own entry above and `docs/engineering-lessons.md`.
Regression: `tests/update_table_drop_index.rs` (a populated `Active`
index, an in-flight-cancellation of a still-`Creating` one, a
create-drop-recreate of the same name, and a crash/retry mid-cascade).

**`dynamo.rs::create_index` (ADR 0045 §2/§6) is `drop_index`'s add-half
sibling** — `UpdateTable`'s `GlobalSecondaryIndexUpdates` `Create` path.
Validates client-side (duplicate name; a name colliding with the reserved
namespace or containing `$`, since it becomes half of the hidden index
table's own name; `Local` kind rejected, defense-in-depth since the wire
decoder never actually produces one), then bridges via
`schema_bridge::index_to_control` **overriding `status` to `Creating`**
and proposes `CreateTableIndex` with a **presence-by-name** commit-wait
(not "status == Creating" — the completion aggregator can flip a small
table's index to `Active` before the caller's own next poll; see
`docs/engineering-lessons.md`'s entry on why a commit-wait must never pin a
transient status value). No `provision_tablet` call: the drain lazily
provisions the hidden table. `describe_table` threads each index's real
status through a side channel (`wire::describe_table_response`'s new
`index_statuses` param — kept off `SecondaryIndex` itself, mirroring
`StreamDescription`'s own separate-bridge precedent) so `DescribeTable`
reports real `CREATING`/`ACTIVE`/`DELETING` plus a per-index
`Backfilling: true` while `Creating` (AWS places it inside each
`GlobalSecondaryIndexes[]` entry, not table-level). `run_index_query`/
`run_index_scan` reject a non-`Active` index with `ValidationException`,
beside their existing `ConsistentRead`-against-a-GSI check. Regression:
`tests/update_table_create_index.rs` (populated-table backfill with a
concurrent write racing it, client-side validation, and a non-leader-node
relay convergence check).

## Quiescence (ADR 0044 phase 1 / ADR 0048)

Data-plane-only (the control plane never quiesces, fork G); the mechanism
itself (`RaftCore`'s state machine, `RaftKvNode::wake`/`enable_quiescence`/
`is_quiesced`/`set_quiesce_veto`) lives in `animus-cp-data` — see that
crate's `CLAUDE.md`. This crate's own contribution:

- **Wake-on-demand**: `resolve_cp_route` calls `wake()` on a local handle
  before deciding anything — cheap, unconditional, a no-op on every state
  except a locally-woken quiesced follower's "are you still there?" check.
  `host::Reconciler::tick`'s own proactive wake (fork H, on a `Down`
  replica) lives in `animus-cp-data`.
- **The `hot_read` scope-transition latch is GONE (ADR 0050 Train B rung
  7)** — together with the residual it narrowed: a tablet's range is
  immutable and a split retires its parent whole (its group refuses via the
  freeze, then tears down), so no scope-transition window exists for an
  open-tail read to race. `hot_read` takes only the group handle now.
- **Quiesce veto**: `change_consumer_loop` (`index_drain.rs`) computes
  `!group.pending_changes().await.is_empty()` once per led tablet per tick
  and calls `CpGroup::set_quiesce_veto(held, fresh_through)` with it — held
  while the change log is non-empty, released the instant a sweep finds it
  empty. `fresh_through` is the tablet's `engine_applied_index()`, read
  **once per tick before any of that tick's engine scans** and reused by
  every `set_quiesce_veto` call in the tick, so the observations stay
  mutually consistent and each is a valid lower bound (issue #302 — see
  `animus-cp-data/CLAUDE.md`'s fork-D bullet for why reading it afterward,
  or stamping wall-clock time instead, would both be unsound). The floor
  `MIN_QUIESCE_AFTER` (= `INDEX_DRAIN_INTERVAL`) is validated on
  `--quiesce-after` so a nonzero setting can never sit below the sweep
  period that feeds the veto.
- **Sweeper skip** (the fleet-scale CPU win — PR5's veto alone only stops
  pointless Raft timer/heartbeat/apply-poll activity, not these loops' own
  per-tablet LSM scans): `change_consumer_loop`, `txn_resolver_loop`, and
  `auto_split_loop` all skip a led tablet outright once `CpGroup::
  is_quiesced()` is true, rather than merely finding nothing to do. Sound
  by construction: the first two follow directly from the veto invariant
  above — note that soundness now rests on the veto's **freshness** clause,
  not on the sweeper's own cadence, because before issue #302's fix the
  argument was circular (a group that quiesced on a stale veto was then
  skipped by the very loop that would have refreshed it, so the bad state
  was sticky rather than self-correcting); `auto_split_loop`'s skip is sound because a quiesced group's
  bytes/key-count are provably static (no activity for `quiesce_after`
  means no write since it last quiesced) — whatever its last
  pre-quiescence tick already checked still holds. The skip is a strict,
  reversible short-circuit: any write un-quiesces the group via the
  pre-existing propose-wake plumbing, so the very next tick resumes normal
  sweeping.
- **Observability**: `Metric::CpQuiesces`/`CpUnquiesces` (counters,
  incremented by `animus-cp-data`'s own consensus loop on every genuine
  transition) and `Metric::CpGroupsQuiesced` (a level, sampled once per
  `metrics_sample_loop` tick across `ctx.edge.hosted_groups()` — the
  identical "counter slot re-purposed as a last-write-wins level"
  convention `StreamHotBytes`/`StreamSegmentsLive` already use).
  `CpRaftView.quiesced` (`/admin/raftkv`) and the Console Tablets view's
  neutral "quiesced" pill (`dashboard_tablets.js`, reusing the `.forming`
  style — informational, never a health/data-risk signal, ADR 0021 §7's
  own rule) surface it. **Fork F**: reading it never wakes anything —
  `CpGroup::is_quiesced()`/`RaftKvNode::is_quiesced()` are pure frozen
  accessors, so an open dashboard tab cannot un-quiesce a fleet.
- **Production wiring**: `--quiesce-after SECS` (`main.rs`) threads through
  `--config`/`--node` (`run_single` → `run_node_with_cluster_settings` →
  `run_node_with_streams_quiesce_and_ttl_sweep_interval` →
  `BoundNode::start_with_growth`) and `--cluster N`
  (`start_cluster_with_growth_and_quiesce_after`) — **defaults ON at 5s**
  (`main::DEFAULT_QUIESCE_AFTER_SECS`; `0` disables). See that constant's
  own doc and ADR 0048's Consequences section for the evidence behind this
  default and what was *not* separately validated (a large fleet under
  sustained mixed load with real inter-process latency) — a
  maintainer-reviewable call, not a settled fact. **Since issue #676, also
  wired for `--cluster-control`/`--cluster-data`** (`run_in_process_split_
  cluster` → `start_split_cluster_with_growth`, resolved to the identical
  on-by-default value the two real deployment paths already used) **and for
  `join`/`data --seed`** (`run_node_join_with_settings`/`run_node_data_
  join_with_settings`, the widened siblings of `run_node_join`/
  `run_node_data_join` — those two narrower functions keep their own
  original arity and now default to the same on-by-default value
  internally, so every existing caller, this crate's own test suite
  included, picks up the fix with no signature change). The standalone
  `control` subcommand still has no route to it (never a documented gap —
  a control-only node has no data plane to quiesce at all, see
  `animusd::config::ClusterSettings`'s own applicability table).
  **`animusd data --config` also reaches it (S-06)** —
  `BoundDataNode::start_data_with_growth` gained its own `quiesce_after`
  parameter and `enable_quiescence` call (mirroring the combined-mode
  reconciler's), closing what used to be a hardcoded `Duration::ZERO` on
  every data-only node; not via a CLI flag of its own, but through the same
  `ClusterConfig::cluster_settings.quiesce_after_secs` config-file section
  `--config`/`--node` also reads (`ClusterSettings`'s own doc in
  `config.rs` has the field-by-field applicability breakdown, and the root
  `CLAUDE.md`'s auto-split entry / ADR 0034/0040/0048's amendment notes
  cover the section as a whole). A CLI flag and the config section setting
  the *same* field is a hard startup error (`resolve_cluster_settings` in
  `main.rs`), the identical "one way, not both" contract `--dynamo-auth`
  already uses.
- **`/admin/config`'s `quiesce_after_ms`** (roadmap U-06) reports the actual
  threshold this node's own reconciler was started with — `null` when it's
  `0`/disabled (on a data-only node that means no
  `cluster_settings.quiesce_after_secs` in its config, since S-06 is the
  only route to the knob there — there is still no `data --config` CLI
  flag) and on a control-only node (structurally inapplicable — no CP-data
  tablet to quiesce). See `admin.rs::config_view`, above.

Tests: `index_drain.rs`'s own `stream_sealer_tests` module (in-crate, needs
private `CpGroup` access) covers the veto end to end
(`hot_backlog_holds_the_quiesce_veto_until_the_hot_tail_trims`) and the
sweeper-skip regression
(`a_rewoken_tablet_is_picked_back_up_by_every_sweeper_within_one_interval`);
 `tests/cp_quiescence.rs` is the critical
`ProdEnv` leader-kill liveness regression
(`write_after_leader_kill_of_a_quiesced_group_converges`) — the one
property `SimEnv` structurally cannot prove.

## Heartbeat batching (ADR 0044 phase 2 — C-02 PR 2 shipped it off by
default; PR 3, the cutover, flips the default ON — C-02 is now complete)

Data-plane-only, **on by default since the PR 3 cutover**, the mechanism
itself (`animus_cp_data::heartbeat_batch::HeartbeatBatcher`,
`host::Reconciler::enable_heartbeat_batching`) lives in `animus-cp-data` —
see that crate's `CLAUDE.md`, unchanged by the cutover. This crate's own
contribution is purely the CLI/config-flag plumbing, threaded through the
**identical** wrapper chain `--quiesce-after` already uses, at each
function's own trailing `heartbeat_batch: bool` parameter placed right
after `quiesce_after: Duration`:

- **`--heartbeat-batch` / `--no-heartbeat-batch`** (`main.rs`, both bare
  boolean flags — no value, since the batcher's own flush cadence is fixed
  at `RaftCore::heartbeat_interval` with no independent tunable) thread
  through `--config`/`--node` (`run_single` →
  `run_node_with_cluster_settings` →
  `run_node_with_streams_quiesce_and_ttl_sweep_interval` →
  `BoundNode::start_with_growth`) and `--cluster N`
  (`start_cluster_with_growth_and_quiesce_after` →
  `start_cluster_inner`) — **on by default** (`main::
  DEFAULT_HEARTBEAT_BATCH = true`), the identical default-ON posture
  `--quiesce-after` already has (that flag's own `DEFAULT_QUIESCE_AFTER_
  SECS`), now that C-02 PR 2's own corpus/liveness proof gives this
  mechanism the same soak `--quiesce-after` had when ADR 0048 defaulted it
  on. `--heartbeat-batch` is a no-op restating the default (kept for
  explicit/scripted invocations); `--no-heartbeat-batch` is the real opt-
  out, resolving `Option<bool>` to `Some(false)` instead of `None`
  defaulting to `DEFAULT_HEARTBEAT_BATCH`. `animusd data --config` reaches
  the same knob too, via the `cluster_settings.heartbeat_batch`
  config-file field `run_node_data_with_cluster_settings` reads, mirroring
  S-06's own `quiesce_after_secs` route exactly (`ClusterSettings::
  heartbeat_batch`'s own doc in `config.rs` has the field's applicability
  — `None`/absent resolves to `DEFAULT_HEARTBEAT_BATCH`, an explicit
  `false` is the opt-out). A CLI flag and the config section setting the
  same field is the identical "one way, not both" hard-error contract
  `resolve_cluster_settings` already enforces for every other knob there.
- **Reaches every real deployment shape now (issue #676), same as
  `--quiesce-after`** — `--cluster-control`/`--cluster-data`
  (`run_in_process_split_cluster` → `start_split_cluster_with_growth`,
  resolved to `DEFAULT_HEARTBEAT_BATCH` when the flag is omitted) and
  `join`/`data --seed` (`run_node_join_with_settings`/`run_node_data_
  join_with_settings`, with `run_node_join`/`run_node_data_join` keeping
  their own arity and defaulting to `DEFAULT_HEARTBEAT_BATCH` internally —
  the identical shape `--quiesce-after`'s own reach-gap closure uses).
  **Every narrower test/convenience wrapper that doesn't expose every knob
  its own widest sibling does still hardcodes `false`**, unaffected by this
  closure (`run_node_with_streams_and_quiesce_after`, `run_node_with_
  streams_and_pitr_snapshot_cadence`, `run_node_with_streams_quiesce_and_
  backup_store`, `start_cluster_with_quiesce_after`,
  `start_cluster_with_growth`, and their own ancestors — each hardcodes
  `false` at its own call into a batching-aware layer, with a comment
  pointing at the wider sibling that does expose it). A test using one of
  these narrower wrappers gets batching OFF regardless of this cutover —
  read the wrapper's own doc, not this section, before assuming a test
  exercises the new default.
- **No `/admin/config` field yet** (unlike `--quiesce-after`'s own
  `quiesce_after_ms`) — a deliberate scope cut, unchanged by the cutover,
  named here so it isn't mistaken for an oversight; a follow-up can add
  one the same way roadmap U-06 added `quiesce_after_ms`.
- **`crates/animusd/tests/heartbeat_batch_liveness.rs` (C-02 PR 3)** is the
  real-thread `ProdEnv` liveness proof this cutover added — a 3-node
  cluster hosting three tablet groups with batching on by default (no flag
  passed), proving a stable leader/term under continuous traffic for a
  fixed wall interval, then re-election within a bounded budget after
  killing the physical node leading the most groups (the busiest batcher
  source), with reads/writes continuing to work throughout via the
  survivors. Mirrors `tests/cp_quiescence.rs`'s own role for quiescence
  (root `CLAUDE.md`'s "`SimEnv` proves logic and ordering, not real-thread
  liveness" lesson) — the `prod-liveness-animusd` CI shard now runs this
  test alongside every other real-socket integration binary. Run 5x
  locally to confirm no flake before landing any future change to this
  path.

See ADR 0044's 2026-09-06 phase-2-cutover amendment for the full design
record — the reserved stream id, response-batching decision, and
receiver-lookup-ownership decisions from PR 2 are unchanged; this
amendment only records the default flip and its measured proof.

## Shared WAL (ADR 0028's amendment — C-05 PR 2 shipped it off by default;
PR 3 flips the default ON — C-05 is now complete)

Data-plane-only, **on by default since the PR 3 cutover** (2026-09-06, same
day as PR 2). The mechanism itself (`animus_control::SharedWal`, the
per-node coordinator; the two persist-path drainer branches;
`host::Reconciler::enable_shared_wal`) lives in `animus-control`/
`animus-cp-data` — see `animus-control/CLAUDE.md`'s `shared_wal.rs` entry
and `animus-cp-data/CLAUDE.md`'s own "`SharedWal` is wired into this exact
persist path" entry for the coordinator and the persist-path wiring,
respectively, unchanged by the cutover. This crate's own contribution is
the CLI/config-flag plumbing, threaded through the **identical** wrapper
chain `--heartbeat-batch`/`--quiesce-after` already use, at each function's
own trailing `shared_wal: bool` parameter placed right after that chain's
existing trailing knobs:

- **`--shared-wal` / `--no-shared-wal`** (`main.rs`, both bare boolean
  flags — no value) thread through `--config`/`--node` (`run_single` →
  `run_node_with_cluster_settings` →
  `run_node_with_streams_quiesce_and_ttl_sweep_interval` →
  `BoundNode::start_with_growth`) and `--cluster N`
  (`start_cluster_with_growth_and_quiesce_after` → `start_cluster_inner`)
  — **on by default** (`main::DEFAULT_SHARED_WAL = true`), the identical
  default-ON posture `--heartbeat-batch`/`--quiesce-after` already have.
  `--shared-wal` is a no-op restating the default (kept for
  explicit/scripted invocations); `--no-shared-wal` is the real opt-out,
  resolving `Option<bool>` to `Some(false)` instead of `None` defaulting to
  `DEFAULT_SHARED_WAL`. `animusd data --config` reaches the same knob too,
  via the `cluster_settings.shared_wal: Option<bool>` config-file field
  (`config.rs`) that `run_node_data_with_cluster_settings` now reads — the
  identical S-06 route `--quiesce-after`/`--heartbeat-batch` already use.
  **Widened onto this path by PR 3** — PR 2 left `cluster_settings.
  shared_wal` silently ignored for a data-only node even though the
  identical config-file field already reached `heartbeat_batch` there; a
  real gap, not a documented scope cut, closed as part of this cutover
  (`BoundDataNode::start_data_with_growth` gained the matching trailing
  `shared_wal: bool` parameter and its own `check_wal_layout`/
  `SharedWal::open`/`enable_shared_wal` call sequence, byte-identical in
  shape to `BoundNode::start_with_growth`'s own below). A CLI flag and the
  config section setting the same field is the identical "one way, not
  both" hard-error contract `resolve_cluster_settings` already enforces
  for every other knob there.
- **`BoundNode::start_with_growth`'s (and, as of PR 3,
  `BoundDataNode::start_data_with_growth`'s) own `shared_wal: bool`
  parameter** is where the flag actually does something. **Before anything
  else**, it calls `animus_cp_data::host::check_wal_layout(&env,
  shared_wal)` — a directory listing checked against `shared_wal`, refusing
  to start (a named `io::Error`, propagated with `?`) if this node's data
  directory already holds the OTHER WAL layout (see `animus-cp-data/
  CLAUDE.md`'s own `check_wal_layout` entry and ADR 0028's corrected
  layout-mismatch paragraph for why this has to be a loud startup failure,
  not a silent reset: flipping the flag against an existing data dir would
  otherwise silently discard every hosted tablet's persisted Raft state).
  **Since the default is now ON, this is the check every pre-cutover data
  directory hits on a plain upgrade with no flag at all** — its error
  message says so explicitly, naming `--no-shared-wal` as the fix (see
  `check_wal_layout`'s own doc and this crate's `main.rs` module doc for
  the exact text); the reverse direction's message symmetrically says to
  *omit* `--no-shared-wal`, never to pass `--shared-wal` (a no-op restating
  the now-default value). Only once the check passes does it call
  `SharedWal::<KvCommand, KvState>::open(&env, SHARED_WAL)` **once, before
  the reconciler hosts anything**, then `reconciler.enable_shared_wal
  (shared)` — every group this node ever hosts (initial bring-up, a later
  split child, a later GC) then threads that one `Arc<SharedWal<..>>`
  through automatically (`host::Reconciler`'s own doc has the call-site
  list). A failure to open the shared WAL file itself (distinct from the
  layout-mismatch refusal above) is also a hard startup error
  (`std::io::Error::other`), not a silent fallback to the per-group path —
  the flag means "use the shared file," and a node that can't open it has
  nothing safe to fall back to mid-recovery.
- **Reaches every real deployment shape now (issue #676), same as
  `--heartbeat-batch`/`--quiesce-after`**: `--cluster-control`/
  `--cluster-data` (the in-process split-cluster dev path,
  `start_split_cluster_with_growth`) and `join`/`data --seed`
  (`run_node_join_with_settings`/`run_node_data_join_with_settings`, with
  `run_node_join`/`run_node_data_join` keeping their own arity and
  defaulting to `DEFAULT_SHARED_WAL` internally). **Every narrower test/
  convenience wrapper that doesn't expose every knob its own widest sibling
  does still hardcodes `false`**, unaffected by this closure (the narrower
  `start_cluster_with*` wrappers, `start_data_with_streams`, the narrower
  `run_node_with_streams_*` wrappers, `index_drain.rs`'s own in-crate
  bring-up helper, `run_node_data`/`run_node_data_with_streams`) all
  hardcode `false` directly at their own call into a shared-WAL-aware
  layer, mirroring `--heartbeat-batch`'s own hardcoded-`false` wrapper list
  exactly (see that section's own bullet for the full enumeration and
  reasoning). A test using one of these narrower wrappers gets the shared
  WAL OFF regardless of what this section says — read the wrapper's own
  doc, not this section, before assuming a test exercises the new default.
- **No `/admin/config` field yet** (the identical, deliberate scope cut
  `--heartbeat-batch` itself still has, unchanged by the cutover) — a
  follow-up can add one the same way roadmap U-06 added `quiesce_after_ms`.
- **`crates/animusd/tests/shared_wal_e2e.rs`** is the real-`ProdEnv`/
  real-disk liveness-and-correctness proof PR 2 added, unchanged by the
  cutover except for the layout-mismatch error text it asserts (see below)
  — a single combined-mode node hosting two tables (two distinct CP-data
  tablets sharing the one node's `SharedWal` over a real `LsmEngine`),
  writes to both, a genuine process restart (same data dir/addresses,
  `run_node_with_cluster_settings` called fresh with the flag still on),
  and both tables' data — plus a fresh post-restart write — converging
  (polled, never a fixed-deadline single-shot assert — the tablet-host
  reconciler re-hosts each table's tablet asynchronously after a fresh
  process start) back to their pre-restart values. The deterministic/
  fault-injection side (cross-tablet ordering, crash mid-round, GC, a quiet
  tablet surviving a noisy sibling's compaction) is `animus-cp-data`'s own
  `sharedwal_fault_corpus.rs` (`ANIMUS_SHAREDWAL_SEEDS`) — this file is
  deliberately the smaller, real-disk complement, not a second corpus. A
  second test in this same file, `a_restart_with_shared_wal_flipped_
  refuses_to_start`, is the layout-mismatch loud-failure proof (ADR 0028's
  corrected amendment) — both directions (written shared, restarted
  per-group; written per-group, restarted shared) through the real
  `run_node_with_cluster_settings` startup surface, asserting the returned
  error names `--shared-wal`, the layout it found, "Refusing to start", and
  "persisted Raft state" — substrings that still hold under the new,
  default-aware phrasing (`check_wal_layout`'s own doc has the exact
  before/after text).
- **`crates/animusd/tests/shared_wal_liveness.rs` (C-05 PR 3)** is the
  real-thread `ProdEnv` liveness proof this cutover added — a multi-node
  cluster hosting several tables (several CP-data tablets sharing each
  node's own `SharedWal`) with the shared WAL on by default (no flag
  passed) under continuous concurrent client writes, proving: every acked
  write is `ConsistentRead: true`-readable; a leader kill/restart mid-load
  converges (converged-or-timeout poll, never a fixed-deadline one-shot
  assert); and the shared WAL's segment GC actually runs under sustained
  load without stalling writes. **The load phase itself is
  converge-or-timeout, not fixed-duration** (issue #699, fixed after a
  wall-clock-window write count flaked on a loaded runner — same shape as
  the #690 lesson: a non-vacuity count taken over a fixed wall-clock window
  is the "eventual property, one-shot assert" bug wearing a throughput
  costume): each writer task keeps going until its own table has crossed
  `TARGET_WRITES_PER_TABLE` (a margin past `COMPACT_THRESHOLD`, 64) **and**
  at least `LOAD_DURATION` (5s) has elapsed, so a fast run still gets a
  genuine sustained-load window and a slow run still gets every write it
  needs; the whole load phase is additionally bounded by
  `LOAD_PHASE_BUDGET` (120s), which fires only on a genuine stall, never on
  ordinary runner slowness. The write-count assertion that follows is now
  stated as the loop's own contract (guaranteed by its exit condition), not
  a timing bet. Mirrors `heartbeat_batch_liveness.rs`'s own role for that
  mechanism's cutover (root `CLAUDE.md`'s "`SimEnv` proves logic and
  ordering, not real-thread liveness" lesson) — the `prod-liveness-animusd`
  CI shard now runs this test alongside every other real-socket
  integration binary. Run 5x locally (and once under a concurrently
  running heavy test binary, to simulate contention) to confirm no flake
  before landing any future change to this path.

See `docs/adr/0028-shared-storage-single-command-split.md`'s C-05 PR 2
amendment for the wiring design record, and its C-05 PR 3 amendment for
the cutover itself (the default flip, the `--no-shared-wal` opt-out, the
`animusd data --config` gap closed, and the liveness proof).

## Wire edges

All edges are production-only I/O (real tokio sockets, hand-rolled framing) and
route below the edge through the same `ClientCtx` CP primitives.

- **DynamoDB** (`dynamo.rs`, `RoleAddrs.dynamo`) — decodes `X-Amz-Target` +
  AttributeValue-JSON via `animus_dynamo::wire`. `CreateTable` proposes its
  key schema **and** GSI/LSI *definitions* into the replicated catalog (ADR
  0013) and waits for commit — and, before acking, for the provisioned
  tablet's group to actually **serve** (`ClientCtx::await_table_serveable`,
  a linearizable probe read; ADR 0023's 2026-08-17 amendment — the 200 must
  not hand the client the group's formation/election window; regression:
  `tests/create_table_ready.rs`, whose readiness assertion is one-shot at
  ack time on purpose); a node reconciles its local registry from
  `Metadata::table_indexes` — the registry holds only *definition*
  bookkeeping, never index entries (there is no in-memory index at all). An
  indexed/streamed table's `PutItem`/`DeleteItem`/`UpdateItem` commits the
  base row, its **LSI rows** and a **change-log record** as one
  `KvCommand::KindBatch` Raft entry (`kind_writes_for_item`) — with the
  *diff* evaluated **inside `KvCommand::KindEval`'s own apply arm, in
  commit order** (ADR 0054, Accepted): `ClientCtx::cp_kind_write_item`
  routes a `ClientRequest::KindWriteItem { table, pk, sk, op: KindWriteOp,
  condition }` to the item's tablet leader (in-process if local, one
  forwarded hop via `cp_serve_forwarded` if not), and
  `dynamo::kind_write_item_at_leader` builds the `WriteSchema` slice
  (`write_schema_for`) and a `KindEvalOp` mirror of the client's operation,
  proposes a self-contained `KvCommand::KindEval`
  (`ClientCtx::cp_kind_eval_local`, `write_path.rs`), and reads back the
  **apply-side** confirmed decision. Apply itself — not the leader — reads
  the item's current committed value, evaluates `condition`, computes `new`
  from `op` (`Put`/`Delete`/`Update{key_item, actions}`, the last folding
  `UpdateItem`'s base-value RMW into the same mechanism), and derives the
  LSI diff and change record (`kind_writes_for_item`'s own logic, now
  called from apply rather than the leader), all against **the same fresh
  read that decides the write**, so no before-image can ever go stale
  between being read and being applied.

  **This closes the propose→apply staleness window two earlier designs each
  left open, one after the other** — see ADR 0054 for the full account of
  both and why each was a real, measured defect, not a theoretical one:
  the original edge-evaluated design (`index_aware_write`, deleted by ADR
  0046) diffed under a node-local lock two different edge nodes never
  shared, so two nodes writing the same item could both diff against the
  same stale `old`; the leader-evaluated design that replaced it (ADR 0046
  U3) closed that by serializing every write of one item on its own
  tablet's leader, but still computed the finished bytes **before** the
  log, guarded only by a byte-level OCC seatbelt
  (`KvCommand::KindBatch.conditions`) that made a losing write a hard
  refusal rather than a race the leader could resolve — measured at 2 of
  10 concurrent `ADD`s refused under a starved apply task (ADR 0054's own
  Context section). **Neither `rmw_lock`, the OCC seatbelt, nor a
  leader-side read of any kind survive in this path today** (ADR 0054 step
  4b, 2026-09-05) — `KvCommand::KindEval` carries no byte-level seatbelt at
  all, since apply's own fresh read already is the authoritative current
  state, and `DataRole` no longer has an `rmw_lock` field for anything to
  take. See ADR 0054's Decision/Sequencing sections and its closing
  amendment for the full design, every deleted mechanism (`predict_kind_
  eval_decision`, `Metric::KindEvalSeatbeltMismatch`, `cp_kind_local`, and
  more), and the `concurrent_increments_all_land_exactly_once`/
  `concurrent_conditional_add_all_land_exactly_once` regressions (ADR 0061
  rung D3 moved both to `crates/animusd/src/
  sim_cluster_dynamo_update_add_delete.rs`, `SimCluster::dynamo_concurrent`-
  driven; `dynamo_update_add_delete.rs` keeps only the one remaining test
  that needs a GSI) that prove zero refusals under contention.

  **`TransactWriteItems` evaluates the identical way (ADR 0054 step 4a)** —
  `ClientCtx::txn_stage_local` builds a self-contained `TxnWrite::pending`
  payload (the schema slice, `pk`/`sk`/`op`/`condition`, no `ts` — the
  enclosing stage entry supplies one) and appends it unevaluated, with no
  read or lock of its own; `KvCommand::TxnStage`'s own apply arm evaluates
  every pending write in commit order, reusing the identical
  `evaluate_kind_eval` core `KvCommand::KindEval` uses — no second
  evaluator, and no mandatory own-key OCC `conditions` entry for these
  writes either, for the identical reason the single-item path needs none.
  See ADR 0054's step-4a amendment for the full design, including the
  same-txn WAL-replay discipline a non-idempotent `ADD` needs when a
  transaction's own already-staged intent can be re-observed as "current
  state" on an ordinary crash restart.

  **The plain-table half of the old named gap is closed (ADR 0049)**: a
  plain table's conditioned `PutItem`/`DeleteItem` and `UpdateItem` now
  route through this same leader funnel (constant-true gate, below), so
  their conditions/RMW evaluate at the leader too.
  An **unevaluated** plain-table write (no condition, no
  old-image echo) takes the ADR 0049 **fast arm** instead
  (`dynamo::fast_marker_write`): the edge builds base row + marker record
  and proposes routed, no leader read, no `rmw_lock` — see that function's
  doc for why the funnel must NOT carry these (lock-across-commit
  serializes a batch into N sequential fsync round trips, the documented
  disk-starvation shape). `BatchWriteItem` groups a marker
  table's requests **per tablet** and commits each group as ONE
  `KindBatch` entry carrying every base row + every marker record
  (`KindBatch.change_log` is a `Vec` since codec v17) — the same
  entry-granularity the old `cp_batch_write` path had; a first cut
  proposed one entry per item (concurrently), which is ~N× the
  entries/WAL/apply work and blew `backfill_seeder`'s populate-then-
  backfill budget under load (regression + guard:
  `stream_write_path_tests::batch_write_on_a_marker_table_commits_one_
  entry_per_tablet`, which pins "one distinct apply HLC per tablet per
  batch"). Images-carrying tables' requests go through the per-item
  funnel, atomic per-item only (the old `cp_batch_write` fast path was
  deleted in rung 5 along with the primitive itself). **`TransactWriteItems` now participates
  too (2026-08-16, ADR 0046 A1/U3, `TxnStage` kind-writes stack)** — the
  wholesale per-table rejection this paragraph used to document (a write
  action against an indexed *or* streamed table cancelling the whole
  transaction, since `TxnStage` could only ever stage the base row) is
  gone. `TxnStage`'s own `writes` element now carries an optional derived
  `kind_writes`/`change_log` payload alongside its base `key`/`value`,
  evaluated **at the item's own tablet leader at stage time**
  (`dynamo::eval_kind_txn_write`, the identical U3 shape as this
  paragraph's own non-transactional write path) and materialized by
  `TxnResolve`'s commit branch — see the "Multi-participant transactions"
  section below and `docs/adr/0018-cross-tablet-transactions.md`'s
  2026-08-16 amendment for the full mechanism. Every evaluated
  transactional write also carries an ADR 0049 §3 **stage marker**
  (`TxnWrite::stage_marker`, built by `dynamo::stage_marker_change_log` via
  the shared marker core) that `TxnStage`'s apply arm materializes at the
  stage entry's own HLC — consumer-hidden (`ChangeRecord::staged`), so the
  existing exactly-one-record-per-transactional-write streams e2e
  (`tests/dynamo_streams.rs`) doubles as its leak regression.

  **DynamoDB Streams (ADR 0042/0043).** `TableSchema.stream:
  Option<StreamSpec>` rides the same `CreateTable`/`UpdateTable` surface as
  the key schema/indexes (mint a fresh label on enable, reject a
  same-command relabel — the caller must disable first); `DescribeTable` is
  a pure read of the replicated catalog; the read path
  (`dynamo_streams.rs`, `ListStreams`/`DescribeStream`/`GetShardIterator`/
  `GetRecords`) shares the DynamoDB listener via a target-prefix dispatch
  fork in `dynamo.rs::dispatch`. Full wire-edge contracts — label minting,
  the sealed-vs-open serve split, and the iterator token shape — are in
  `docs/streams-notes.md`. The record-shape predicate
  (`table_change_records_carry_images`) stays here, next paragraph.

  **The write-path gate is structural since ADR 0049 (the universal
  kind-write path — rung 1 made the old `table_takes_kind_write_path`
  predicate constant-true; rung 5 deleted it with the plain branches it
  guarded)**: every
  Dynamo table's every mutation commits through `KindBatch`, so every
  tablet has a change log unconditionally. What *varies* per table is the
  record's shape, decided by `table_change_records_carry_images` (the old
  predicate, `!indexes.is_empty() || stream.is_some()`, renamed to what it
  now actually gates): with a stream or index the record carries both
  images exactly as before; with neither, it is an **image-less marker**
  (`ChangeRecord::marker` — the ADR 0049 §1 dirty-key signal, filtered off
  both Streams serve paths by `ChangeRecord::consumer_hidden`, exactly like
  the backfill's `seeded` records; the GSI drain additionally **skips**
  marker records outright — a marker predates every index by construction,
  so pre-index history stays the backfill seeder's job, and a marker-only
  backlog must never lazily provision a hidden table mid-`drop_index` —
  see `drain_tablet`'s ADR 0049 comment). The plain single-key fallbacks in
  the handlers, `kind_writes_for_item`'s `None` arm (it returns
  `IndexedWrite` directly now), `run_update_item`, `quorum_write`, and
  `run_transact`'s coordinator-valued write path (with its own-key
  `write_conditions` feeding — a write action's condition rides
  `PendingKindWrite::condition` + the C1 OCC instead) were all deleted in
  rung 5. Two consequences worth knowing:
  ADR 0046 §2's "a plain table's condition only has node-local `rmw_lock`
  protection" gap is **closed for the Dynamo edge** (every write now
  evaluates at the tablet leader), and a plain table's markers are
  **transient** (Train A rung 4):
  `change_consumer_loop` now visits every led tablet — a marker table gets
  a mandatory cheap idle gate (`approx_bytes_kind(KIND_CHANGE) == 0` ⇒
  nothing at all this tick), holds the quiesce veto while markers are
  pending, and runs only the trim arm, whose existing zero-expected-terms
  trim-everything rule deletes them (`Metric::ChangeLogTrimmedTotal`
  counts deletions — also the trim-safe half of the marker-emission
  tests' accounting, since a racing trim tick may erase the live
  evidence). The admin seeder and raw `ClientRequest::Txn` plain writes
  emit markers/stage-markers too (rung 4's entry-point completeness —
  the seeder routes through `dynamo::marker_batch_write`/the per-item
  funnel like `BatchWriteItem` itself). **A
  streamed-but-unindexed table**: `indexes` is empty, so the LSI loop is
  simply a no-op, and the entry commits exactly base row + change record —
  this same change record *is* the hot shard the sealer reads directly, no
  separate copier involved.
  **A real, independent correctness gap this surfaced**: `PutItem`/
  `DeleteItem` only fetched the prior item (`needs_old`) when a
  `ConditionExpression` or `ALL_OLD` was requested — an unconditional
  replace/delete on an indexed *or* streamed table therefore silently
  skipped the read `kind_writes_for_item`'s LSI diff (and now a stream's
  `OLD_IMAGE`/`NEW_AND_OLD_IMAGES` fidelity) actually needs. The fix made
  both handlers' `needs_old` check the shared kind-path gate (since rung 5
  the whole question is moot — every evaluated write reads its old image at
  the leader, and the `needs_old` sites went with the deleted plain
  fallbacks). See `docs/engineering-lessons.md` for the
  general lesson (a fast-path gate and a "do I need the old value" gate
  must be the *same* predicate, not two that happen to agree today).

  `ClientRequest::KindWrite` is the forwarding payload — **internal-only,
  refused bare** (a client could otherwise write arbitrary bytes into a table's
  LSI/change scopes and desynchronise its indexes), handled only inside
  `cp_serve_forwarded`; it is a data-plane RPC, not a `MetaCommand`, so
  `is_relayable_command` does not apply. `cp_kind_write` **verifies every key
  maps to one tablet** rather than assuming it: a batch straddling two tablets
  cannot be atomic, and committing only the first tablet's share is exactly the
  torn base-row-without-its-index-row state the mechanism exists to prevent.

  **A `Query`/`Scan` — base or index — is always a native CP range scan,
  never an in-memory lookup.** A base `Query`/`Scan` uses `cp_scan`; a GSI
  `Query`/`Scan` scans the index's own hidden table (`index_table_name`)
  directly, fanned across its tablets by ordinary `cp_scan` (its own
  GSI-shaped pagination cursor, since the hidden table's engine key isn't
  the base table's key). An LSI `Query` is a scan of the *base table's own
  tablet* over its `KIND_LSI` scope (scoped to one base partition/tablet);
  an LSI `Scan` is table-wide, via `ClientCtx::
  cp_scan_kind_table` (`cp_scan`'s kind-scoped sibling, fanning a
  `KindScan` per overlapping tablet — its tail tablet needs a genuinely
  unbounded-above scan, since no finite byte string can bound an LSI row's
  keyspace, so the primitive derives the bound from the kind scope's own
  physical prefix). `ClientRequest::KindScan` is the LSI path's forwarding
  payload — **internal-only, refused bare**, the read-side dual of
  `KindWrite`. A hidden table with no tablet yet reads as **empty**, the
  same gate `ClientCtx::cp_get` uses. A **GSI** query/scan is always
  eventually consistent (DynamoDB's own contract — the drain materializes
  asynchronously); an **LSI** one is strong iff `ConsistentRead: true` —
  eventual by default, same as a base read (ADR 0055; see below).
  `ConsistentRead: true` is accepted everywhere except a GSI `Query`/`Scan`,
  which rejects it (`ValidationException` — only `animusd`, with `Metadata`
  in hand, knows an index's kind). **Since ADR 0055 the flag selects a real
  read path** rather than only halving the reported capacity: `true` is the
  linearizable ReadIndex read; `false` — the wire default — is served from any
  replica's applied state (see "Request routing (CP)" above). Two consequences
  worth knowing when writing tests here: **a write followed by an unqualified
  read may not see it** (add `"ConsistentRead":true` to any read that is
  asserting a write landed — that is what a DynamoDB client must do, and it
  makes the test say what it depends on), and a **GSI** read is now always
  eventual, which falls out of the rejection above rather than being a special
  case (`consistent_read` is always `false` there, so the ordinary derivation
  produces `Eventual`).

  Regression: `animus-dynamo`'s `wire` unit tests plus `tests/
  dynamo_index_scan.rs`/`kind_scan.rs` end to end.

  Surface also covers `UpdateItem`/`BatchWriteItem` (condition-gated,
  per-request/per-tablet atomicity only) and **atomic** `TransactWriteItems`/
  `TransactGetItems` (via `ClientCtx::cp_txn`) — see ADR 0018 §2 for the
  condition-evaluation layering, including the follow-up amendment that
  gave a write action's own `ConditionExpression` full **cross-node** OCC
  (apply-time `write_conditions`, not just same-node `rmw_lock`
  protection). `DeleteItem` writes a tombstone *value*.

  **`TransactGetItems` (`dynamo::quiescent_multi_get`) reads every key via
  `ClientCtx::cp_read_snapshot`, never plain `cp_read`** (ADR 0018 §2's
  newest amendment, torn-pair-fix stack PR2): a quiescent round's own
  correctness argument needs every key sampled at *the same instant*,
  which `cp_read`'s deliberately asymmetric intent resolution (a bounded
  blocking chase for a local intent, an immediate give-up for a foreign
  one — correct for plain `GetItem`, which this leaves untouched) breaks
  under a tight concurrent writer. `cp_read_snapshot` makes exactly one
  non-blocking attempt per key regardless of locality; any key that
  doesn't resolve reports `SnapshotRead::Unresolved` and the **whole
  round** is discarded, never partially compared. See the ADR amendment
  for the full incident and `docs/engineering-lessons.md` for a residual,
  unrelated write-side bug this investigation surfaced but did not fix.

  **DynamoDB-style TTL (ADR 0051).** `UpdateTimeToLive`/`DescribeTimeToLive`
  ride the same replicated-catalog shape as streams/indexes:
  `dynamo::update_time_to_live` proposes `MetaCommand::SetTableTtl` (`Some`
  to enable/change, `None` to disable) and commit-waits exactly like
  `enable_stream`/`disable_stream`; `dynamo::describe_time_to_live` is a
  pure `meta.table_ttl(table)` read, mirroring `describe_table`. Unlike a
  stream's minted `label`, `TtlSpec` has no identity — re-enabling with the
  same attribute is a catalog no-op and changing it in place needs no
  disable first (see `MetaCommand::SetTableTtl`'s own doc, `animus-control`)
  — so `update_time_to_live` only validates client-side that a **disable**
  call's `AttributeName` matches the *currently-enabled* one, and only when
  something is currently enabled (nothing to mismatch against otherwise).
  The actual deletion is a background loop, not this wire path — see
  `ttl_reaper.rs`'s own module-map entry above (quiescence contract,
  conditional delete, the reaper's `userIdentity` threading through
  `kind_write_item_at_leader`/`kind_writes_for_item`'s trailing
  `ttl_expired: bool`). `MetaCommand::SetTableTtl` is on the
  `is_relayable_command` allowlist beside `SetTableStream` — regression:
  `tests/schema_ddl_relay.rs`.

  **Resource tagging (roadmap W-06).** `TagResource`/`UntagResource`/
  `ListTagsOfResource` ride the same replicated-catalog shape (`dynamo::
  tag_resource`/`untag_resource` commit-wait `MetaCommand::TagResource`/
  `UntagResource` exactly like `update_time_to_live`; `list_tags_of_resource`
  is a pure `meta.table_tags(table)` read). **Gotcha, not obvious from the
  TTL precedent above**: the commit-wait convergence check is **per-key
  membership**, not whole-map equality — `TagResource`/`UntagResource`
  *merge* into `TableSchema::tags` rather than replacing it wholesale (unlike
  `TtlSpec`, which `update_time_to_live` can safely compare for exact
  equality since the whole `Option<Spec>` is what the command sets), so a
  whole-map equality check would spin past `SCHEMA_COMMIT_TIMEOUT` the
  moment a concurrent, unrelated tag mutation on the same table landed in
  between polls. See `docs/engineering-lessons.md`'s entry on this for the
  general form. `MetaCommand::TagResource`/`UntagResource` are on the
  `is_relayable_command` allowlist beside `SetTableTtl` — regression:
  `tests/dynamo_tags.rs`. `DescribeLimits`/`DescribeEndpoints` need no
  catalog at all: `dynamo::describe_limits` is a static read (four named
  constants); `dynamo::describe_endpoints` reads `ctx.admin.dynamo_addr` —
  the same field `admin.rs::config_view`'s `addrs.dynamo` already reports —
  for this node's own bound DynamoDB listen address.
  **`ExecuteStatement` (ADR 0071, W-07 PR 2 `SELECT`; PR 3 `INSERT`/
  `UPDATE`/`DELETE`)** — `dynamo::execute_statement` is a thin edge glue
  function, not a new execution path: it parses the PartiQL `Statement`
  (`animus_dynamo::partiql::parse_statement`, which has no catalog access
  and dispatches to one of the four per-kind parsers by leading keyword),
  resolves the target's key attribute **names** from `Metadata` (a
  `SELECT`'s: the base table's `TableSchema`, or the named index's
  `IndexDef.hash_attribute`/`sort_attribute` when `FROM "t"."index"` names
  one; an `INSERT`/`UPDATE`/`DELETE`'s: always the base table's own
  `TableSchema` — none of the three grammars carry an index `FROM`), and
  lowers onto the matching `Operation`. A `SELECT` lowers onto
  `Operation::Query`/`Operation::Scan` (`partiql::lower_select`) and
  dispatches to `run_query`/`run_scan` directly, unchanged from PR 2 — so
  it inherits every existing `Query`/`Scan` behavior (GSI/LSI dispatch,
  `ConsistentRead`'s ADR 0055 path selection, the leader-forwarding a
  non-hosting node needs) with zero new data-plane code. An `INSERT`/
  `UPDATE`/`DELETE` (PR 3) lowers onto a **real** `Operation::PutItem`/
  `UpdateItem`/`DeleteItem` (`partiql::lower_insert`/`lower_update`/
  `lower_delete`) and runs through `run_operation` itself — the exact
  dispatcher a client-built `PutItem`/`UpdateItem`/`DeleteItem` request
  already goes through, so conditions, LSI/GSI index maintenance, DynamoDB
  Streams change records, and per-table throttling (ADR 0065) are all
  inherited with zero new write-path code, and `run_operation`'s own
  `authz::authorize_op` call — keyed on the now-concrete, now-parsed
  operation, whose `classify` is `OpClass::Write` — is the real
  enforcement point for a mutation (`authz::classify`'s own
  `Operation::ExecuteStatement` row stays `OpClass::Read` unconditionally,
  since it can't see inside unparsed statement text; see that module's own
  doc comment on the row). `execute_statement` calls `run_operation` from
  inside one of `run_operation`'s own match arms — a genuine mutual async
  recursion, resolved with `Box::pin(run_operation(..)).await` at that one
  call site (the standard technique for a directly/mutually recursive
  `async fn`'s otherwise-infinite generated `Future` type; only one edge
  of the cycle needs boxing). Its own job beyond dispatch is small:
  `Operation::table()` returns `None` for `ExecuteStatement` (the table is
  only known once `Statement` is parsed, same as `BatchGetItem`/
  `BatchWriteItem`), so `reject_internal_table` runs once inside
  `execute_statement` itself instead of at `run_operation`'s shared
  pre-dispatch gate (a `SELECT`'s `authz::authorize` call is likewise
  explicit here, unchanged from PR 2; an `INSERT`/`UPDATE`/`DELETE`'s is
  automatic via `run_operation`, above); and reshaping each op's own
  response into `ExecuteStatement`'s own shape
  (`reshape_query_scan_response_to_execute_statement` for `Query`/`Scan` —
  `{Items, Count, ScannedCount, LastEvaluatedKey?}` → `{Items, NextToken?}`,
  `LastEvaluatedKey` becoming an opaque `partiql::encode_next_token`-minted
  `NextToken`, `Count`/`ScannedCount` dropped — no `ExecuteStatement`
  equivalent; `reshape_write_response_to_execute_statement` for `PutItem`/
  `UpdateItem`/`DeleteItem` — `{Attributes?, ConsumedCapacity?,
  ItemCollectionMetrics?}` → `{Items}`, `Attributes` becoming a
  single-element `Items` array when present, an **empty** one when absent,
  never omitted). **`INSERT`'s implicit `attribute_not_exists(pk)`
  condition failure maps to a new `WireError::duplicate_item`
  (`DuplicateItemException`)** — or is silently swallowed (empty `Items`,
  no error) when the statement carried `ON CONFLICT DO NOTHING`; every
  other `ConditionalCheckFailedException` (an `UPDATE`'s implicit
  `attribute_exists(pk)`, or a non-key `WHERE` term on `UPDATE`/`DELETE`
  evaluating false) surfaces unchanged. `ConsumedCapacity` is decoded/
  accepted but never populated for any statement kind, mirroring `Query`/
  `Scan`/`PutItem`/`UpdateItem`/`DeleteItem`'s own pre-existing gap (not a
  PartiQL-specific omission — see ADR 0071 §11).
  **`BatchExecuteStatement` (ADR 0071, W-07 PR 4)** — `dynamo::
  run_batch_execute_statement`/`execute_one_batch_statement` run 1..=25
  statements independently (no cross-statement atomicity, mirroring
  `BatchWriteItem`/`BatchGetItem`'s own per-request contract), **reusing
  `parse_statement`/the four `lower_*` functions with no new grammar**: an
  `INSERT`/`UPDATE`/`DELETE` statement calls `execute_statement` wholesale
  (the exact same lowering/`DuplicateItemException` mapping/`RETURNING`
  handling PR 3 built, dispatched through `run_operation`); a `SELECT` runs
  a **restricted** copy of `execute_statement`'s own `SELECT` arm that
  additionally requires `partiql::select_is_exact_key` — AWS limits a batch
  statement to a single-item operation, so a range/filter-bearing `SELECT`
  is a per-statement `ValidationError` entry, checked on the parsed AST
  *before* anything runs (never executes a forbidden `Scan` just to reject
  it after the fact). Never propagates an `Err` up to `run_operation`:
  every per-statement failure — parse error, denied/unknown table, a
  non-exact-key `SELECT`, a condition failure — becomes that statement's
  own `wire::BatchStatementResult` entry (`{TableName, Item}` on success,
  `{TableName?, Error: {Code, Message}}` on failure, `wire::
  WireError::batch_statement_error_code` mapping this crate's `__type`
  codes onto AWS's bare `BatchStatementErrorCodeEnum`, e.g.
  `ConditionalCheckFailedException` → `ConditionalCheckFailed`,
  `ValidationException` → `ValidationError`), never a whole-request
  failure — mirrored by `authz`: `classify`'s `BatchExecuteStatement` row
  stays `OpClass::Read` unconditionally (same "opaque statement text, no
  catalog here" reasoning as `ExecuteStatement`'s own row) and
  `authorize_op` is a no-op for it, joining `ExecuteStatement`'s
  table-unknown-until-parsed group; the real per-statement check — an
  explicit `authz::authorize` call for a `SELECT`, `run_operation`'s own
  `authorize_op` call for a lowered mutation — turns a denial into that
  statement's own `AccessDenied` entry rather than rejecting the whole
  batch, a **deliberate departure** from `BatchGetItem`/`BatchWriteItem`'s
  `authorize_each_table` whole-request rejection (justified by AWS's own
  real per-statement IAM authorization; see ADR 0071's "As-built: PR 4"
  amendment for the full account and the regression test naming). See that
  amendment for the complete response-shape table and error-code mapping.
- **`ExecuteTransaction` (ADR 0071, W-07 PR 5, closes the W-07 PartiQL
  train)** — `dynamo::execute_transaction` is, like `execute_statement`,
  edge glue rather than a new commit protocol: it parses every one of
  `TransactStatements` with the identical `partiql::parse_statement`
  `ExecuteStatement` uses (no new grammar), requires them to be **all**
  `SELECT` or **all** `INSERT`/`UPDATE`/`DELETE` (a mixed set is a
  `ValidationException` — AWS: a transaction is all-reads or all-writes),
  lowers each statement to one `TransactGet`
  (`partiql::lower_select_to_transact_get`) or one `TransactAction`
  (`partiql::lower_insert_to_transact_action`/`lower_update_to_transact_
  action`/`lower_delete_to_transact_action`), and hands the whole batch to
  the **exact same functions** a client-built `TransactGetItems`/
  `TransactWriteItems` request already goes through —
  `run_transact_get`/`run_transact`, unmodified. This is deliberate, not
  merely convenient: `run_transact`/`run_transact_get` already carry every
  guarantee this PR's own scope needed (whole-set `authz::
  authorize_each_table` before anything runs, `ClientRequestToken`
  idempotency including its ambiguous-outcome handling, per-action
  `CancellationReasons` correlated by index, duplicate-key-across-
  statements rejection), so `execute_transaction` needed to write none of
  it itself. Unlike a `SELECT`-shaped `ExecuteStatement`'s exact
  partition-key-or-scan flexibility, a `SELECT` **inside a transaction**
  must be an exact-key read — no named index, no `ORDER BY`, no non-key
  `WHERE` term — since `TransactGetItems` has no filter/index/order
  concept to lower onto at all; `lower_select_to_transact_get` reuses PR 3's
  `lower_exact_key_where` (the same exact-match rule `UPDATE`/`DELETE`
  already use), not `lower_select`'s partial-key-or-scan rule. A `RETURNING`
  clause or `ON CONFLICT DO NOTHING` on a transaction statement is rejected
  at lowering time, not honored or silently dropped — `TransactWriteItems`
  itself reports no item image on a successful action (only
  `ReturnValuesOnConditionCheckFailure`, and only on that action's own
  *cancellation*), and a per-statement conflict-swallow has no meaning once
  any statement's condition failure cancels the whole transaction anyway;
  see ADR 0071's "As-built: PR 5" amendment for the full reasoning on both.
  The wire-level `ReturnValuesOnConditionCheckFailure` field (real AWS
  `ParameterizedStatement` shape, distinct from the PartiQL `RETURNING`
  clause just rejected) **is** decoded and threaded through to each lowered
  `TransactAction`'s own `rvocf` field, so a transaction statement's
  condition failure can still echo an old image in `CancellationReasons`.
  `Operation::ExecuteTransaction::table()` is `None` (multi-table, joining
  `ExecuteStatement`'s "resolved inside its own handler" group) —
  `execute_transaction` resolves and `reject_internal_table`/`table_known`-
  checks every statement's table itself, before lowering anything, mirroring
  `execute_statement`'s own order. Response shape: an all-`SELECT`
  transaction returns `run_transact_get`'s own `{"Responses": [{"Item": ..}
  | {}, ..]}` unmodified (it already matches AWS's documented shape); a
  write transaction's `run_transact` call returns a bare `{}`
  (`TransactWriteItems`'s own success shape), reshaped into
  `wire::execute_transaction_write_response`'s `{"Responses": [{}, ..]}` —
  one empty entry per statement. `ConsumedCapacity` is decoded/accepted but
  never populated, the identical pre-existing gap `ExecuteStatement`/
  `Query`/`Scan` already have.
- **Admin / debug** (`admin.rs`, `RoleAddrs.admin`, ADR 0020) — read-only
  `GET` views + gated `POST` actions + data writes; grep `admin.rs`'s route
  table for the full endpoint inventory. Below the edge it only reads node
  state (aggregated live per request) or drives a gated action. **No
  auth — bind to a trusted interface.** The `animus admin` CLI consumes it.

  `POST /admin/data/dynamo` (`action_data_dynamo`) reaches **both**
  services on the DynamoDB listener — the item API and the Streams read
  API — by resolving `op` to a target and calling `dynamo::execute_routed`,
  the same prefix-fork function `dynamo::dispatch` itself uses; **never**
  call `dynamo::execute` from here directly, which skips that fork
  entirely (see `docs/engineering-lessons.md`'s "same-listener dispatch
  fork" entry for the bug this shortcut caused before the fix).

  `GET /admin/system-table?kind=&after=&limit=` browses the control
  plane's reserved system keyspace. **Load-bearing**: scans
  `animus_control::syskv::reserved_scan_bounds()`'s `[start, end)` via one
  `StorageEngine::scan` — **never** `StorageEngine::entries()`, which
  would scan the *whole* engine (every user table's data too, on a
  combined node sharing it with the CP data plane, ADR 0028); see the
  engineering-lessons entry before ever "simplifying" this to `entries()`.
- **Web console** (`dashboard.rs` + assets, ADR 0021) — a self-contained
  vanilla-JS SPA, a pure client of `/admin/*` JSON; tabs are role-gated
  client-side (a data-only node shows a dedicated **Node** view instead of
  the cluster-wide tabs). **Cluster health means "is the data at risk," not
  "is anything in transition"** (ADR 0021 §7): `tabletStatus`'s ladder
  (`quorum-lost` → `under-replicated` → `healthy` → `forming`) only
  degrades on an actual redundancy/quorum loss; a split-child or
  freshly-provisioned tablet forming its Raft group with every assigned
  replica's node alive renders as a neutral `forming` pill, escalating to
  degraded only if stuck past 60s. **A GSI's hidden `<base>$<index>` table
  has NO entry of its own in `status.schemas.tables`** — verified against a
  live cluster; it exists only as ordinary rows in `status.tablets[*].table`
  (and only once the drain lazily provisions its first tablet), so any
  dashboard code deriving "which tables exist" from the schema catalog
  naturally already excludes it, and code that needs to know about it must
  scan the tablet map instead (`splitHiddenTable`, `dashboard_core.js`,
  groups it under its base table in the Tablets/Overview views). The
  Streams tab's design (including its control-only role-gating, ADR 0021
  #10) is in `docs/streams-notes.md`.
- **OTel** (`otel.rs`, ADR 0027) — `init_tracing(instance_id)` from `main.rs`;
  `current_traceparent`/`set_parent_traceparent` carry W3C trace context across a
  forwarded hop (`cp_forward` injects, the receiver's `handle_client`
  re-parents), so a forwarded write is one joined trace when export is enabled.
- **`GET /metrics`** (ADR 0015) shares the DynamoDB listener; `ClientCtx::
  metrics_text` aggregates both role sinks (control + raftkv) live at request time.

## TLS (ADR 0064, S-01 commit 2)

Config-gated, default off — a node with no `tls` section anywhere behaves
byte-for-byte as before this ADR. See `crates/animus-env/CLAUDE.md`'s own
TLS entry for commit 1's `TlsConfig`/`TlsMaterial`/`MaybeTlsStream`
primitives (this section only covers what this crate adds on top) and the
ADR itself for the full design/rationale.

- **Per-port mode**: `internal` (raw Raft wire, commit 1) and `intra`
  (`ClientRequest` relay) are **mutual** TLS — every peer on those ports
  presents a cert the cluster's own CA signed, closing the membership-
  authentication gap ADR 0064 opens with. `client`/`dynamo`/`admin`/
  `console` are **server-only** — a caller verifies the node, the node
  neither requires nor verifies a caller cert (SigV4, ADR 0057, remains
  the caller-identity story on `dynamo`).
- **Config**: `RoleAddrs::tls: Option<config::TlsSection>` — **per-node**
  (mirrors `advertise_host`'s shape, not `dynamo_auth`'s cluster-wide one):
  each node presents its own cert, so there's no single cluster-wide field
  to hold it. `TlsSection{cert_path, key_path, ca_path: Option<PathBuf>}`
  mirrors `animus_env::TlsConfig` field-for-field and converts to it via
  `to_tls_config()` — kept as an independent type (not a re-export) purely
  so this crate's own config type doesn't have to assume `animus-env`'s
  `prod` feature just to round-trip through `serde_json` (`animusd` itself
  always enables that feature; see the type's own doc). `ClusterConfig::
  validate_tls` (called from `from_json`) enforces ADR 0064 Decision 3's
  all-or-none rule — every node's `tls` presence must agree across one
  config **file**; it cannot (and doesn't try to) check across separate
  processes' own `--tls-*` CLI flags — see the flag's own doc in "CLI
  reference" above for that documented gap and why the config-file route
  sidesteps it.
- **Bind time**: `Node::bind`/`bind_control`/`bind_data` each call
  `RoleAddrs::tls.map(TlsSection::to_tls_config)` once, hand it to
  `ProdEnv::bind_with_tls` for the internal port (mutual, commit 1's own
  mechanism), and separately call `TlsConfig::load()` for this node's own
  `TlsMaterial` covering every other listener — two loads of the same PEM
  files, a deliberate simplicity trade-off over threading a pre-loaded
  `TlsMaterial` into `ProdEnv::bind_with_tls` (which takes a `TlsConfig`,
  not a `TlsMaterial`, and changing that signature would touch commit 1's
  already-shipped API for a startup-time cost that doesn't matter). The
  resulting `TlsMaterial` is stored on `Bound{Node,ControlNode,DataNode}`
  and threaded into `spawn_common_tail`'s trailing `tls` parameter, which
  both stores it on the shared `ClientCtx` (`ClientCtx::tls`, cheap to
  clone onto every connection — its own doc has the full per-port
  breakdown of what each acceptor/the connector are for) and hands the
  right acceptor to each listener it spawns
  (`m.acceptor`/`m.server_acceptor` per the table above). The `dynamo`
  listener (spawned outside `spawn_common_tail`, since a control-only node
  has none) reads `ctx.tls.as_ref().map(|m| m.server_acceptor.clone())` at
  its own call site instead of taking a separate parameter.
- **One generic stream, not a fork**: `http.rs`'s helpers and every
  listener's `handle_conn`/`handle_connection` are generic over `S:
  AsyncRead + AsyncWrite + Unpin` (or, for `write_frame`/`read_frame`
  specifically, an anonymous `impl AsyncRead`/`impl AsyncWrite` **argument**
  rather than a named type parameter — seeing why matters: those two
  functions are called via explicit turbofish all over the pre-existing
  test suite (`read_frame::<ClientResponse>(..)`), and Rust does not infer
  an *unspecified trailing* explicit type parameter regardless of its
  position, so adding a second named parameter would have turned every one
  of those call sites into an arity error; an `impl Trait` argument sidesteps
  the whole problem since it never participates in turbofish at all).
  Each accept loop wraps a plain `TcpStream` in `MaybeTlsStream::Plain`
  when TLS is off, or runs it through the port's own acceptor when on; a
  failed handshake is logged at `warn` with the peer's address and the
  connection dropped — the loop keeps serving, mirroring `animus_env::
  prod::spawn_accept`'s contract at every one of this crate's own
  listeners.
- **Dialers**: `ClientCtx::tls`/`AnimusdRelayClient::tls` (the latter no
  longer zero-sized) carry `Option<TlsMaterial>`; `relay_request`/
  `relay_request_with_timeout` (the free functions every cross-node relay
  in this crate is built on — `ClientCtx::relay`, `forward_to_tablet_
  leader`, `propose_schema`'s broadcast, `AnimusdRelayClient::relay`) take
  it as an explicit parameter and dial through `TlsMaterial::connector`
  (always mutual — every relay this crate makes targets the `intra` port,
  never `client`), deriving the `ServerName` from the dialed address via
  `animus_env::tls::server_name_for` (now `pub`, widened by this commit
  specifically for this reuse). `remote_metadata_watch_loop` (a data-only
  node's `WatchMetadata`/`Status` long-poll, which drives its own round
  trips outside `RemoteControlClient::metadata_fresh`) reaches the
  identical relay path via a new `RemoteControlClient::relay()` accessor
  (`animus-node`) instead of re-dialing by hand — the one place this
  commit widened `animus-node`'s own surface.
- **What stays plain**: `cluster_bench` — a deliberate scope cut (see the
  ADR's own testing-expectation note), not an oversight.
- **Fixture + regression**: `tests/support/mod.rs::tls_pki`/
  `bring_up_deadline_tls` (a small independent copy of `animus-env`'s own
  `#[cfg(test)]`-private PKI helper — see that function's own doc for why
  it isn't reused directly across the crate boundary) and
  `tests/tls_e2e.rs` (a real 3-node combined cluster with TLS on every
  port: `CreateTable`/`PutItem`/`GetItem` across different nodes over
  server-only TLS on the dynamo port — `GetItem` deliberately uses
  `ConsistentRead: true` so the assertion isn't racing ADR 0055's
  eventually-consistent default against cross-replica propagation lag,
  independent of anything TLS-related — admin/console `GET` over TLS, a
  plain-TCP dial refused while the port keeps serving genuine TLS clients
  afterward, a different-CA client refused on the `intra` port (asserted
  by attempting a write/read after the handshake, not by asserting
  `connect()` itself errors — TLS 1.3's client-side handshake future can
  resolve `Ok` having only *sent* its own `Finished` flight, before ever
  reading back the server's verdict on the client cert it just presented;
  the rejection only surfaces on the next real I/O, the same reasoning
  `animus_env::prod::tests::tls_peer_from_different_ca_is_refused` uses
  one layer down), and the mixed-config `validate_tls` error.

## Gotchas

- **The DynamoDB Streams segment store + sealer knobs are wired via the
  `_with_orphan_sweep_after`-style layered-wrapper convention** (ADR
  0042/0043): `main.rs`'s `--stream-seal-bytes B`/
  `--stream-seal-age SECS`/`--segment-store dir:PATH`/`--stream-retention
  SECS` flags (`--config/--node` and `--cluster N` only, so far — the
  split-deployment and data-only CLI paths are a named follow-up) select
  the `_streams`-suffixed entry-point variants; every non-`_streams` call
  site defaults internally to `StreamSealKnobs::default()` (4 MiB / 4h) /
  `SegmentStoreConfig::default()` (`Cluster`) / `DEFAULT_STREAM_RETENTION`
  (24h). Full per-parameter/per-call-site detail: `docs/streams-notes.md`.
- **The on-demand backup subsystem's own store handle is wired the
  identical way, as a deliberately parallel (not shared) second knob** (ADR
  0059 §1, Train 1 PR②): `main.rs`'s `--backup-store cluster|fs:PATH` flag
  (same `--config/--node`-and-`--cluster N`-only scope as `--segment-store`,
  parsed by `parse_backup_store`) selects `BackupStoreConfig` — a distinct
  enum from `SegmentStoreConfig`, not a second value of the same type, kept
  separate because the ADR documents the two knobs' durability tradeoffs
  independently even though the shapes are identical today (`Cluster` |
  `Fs(PathBuf)`). `build_backup_store` mirrors `build_segment_store` exactly
  (same `ClusterSegmentStore<ProdEnv, FsSegmentStore>`/`FsSegmentStore`
  backends — this crate has no `SimEnv` dependency at all, so unlike
  `animus-cp-data`'s own sim corpus neither store handle ever constructs a
  `SimSegmentStore` here) but roots the cluster variant's local building
  block at `dir.join("backups")` instead of `dir.join("segments")` — kept
  physically separate from the streams store's own local directory even
  though the two stores' object namespaces
  (`animus_cp_data::backup::backup_manifest_object_id`/
  `backup_data_object_id` vs. `animus_cp_data::segment::segment_id`) are
  already disjoint, the same belt-and-suspenders posture the ADR itself
  takes for the namespace split. `BackupStoreHandle` (`ClientCtx::
  backup_store`, alongside `ClientCtx::segment_store` — **W-10 moved both
  off `DataRole` onto `ClientCtx` itself**, provisioned on every node
  shape) is threaded through combined (`BoundNode::start_with_growth`),
  data-only (`BoundDataNode::start_data_with_growth`), **and now
  control-only** (`BoundControlNode::start_control_with`, W-10, ADR 0043
  §A9's control-only-leader gap — closed) node assembly. **Consumed since Train 1
  PR③** (`#[allow(dead_code)]` removed from `BackupStoreHandle`/its impl/
  `DataRole::backup_store`, PR② → PR③'s own promised follow-up): the
  capture driver (`backup_capture.rs`) `put`s chunked data objects and the
  completion aggregator (`backup_completion.rs`) `put`s the manifest
  object — see both modules' own entries below. **`list_local`/`delete_local`
  consumed since Train 1 PR④** by the backup janitor's own local-only
  reclaim sweep (`backup_janitor.rs`'s own entry below has the full
  design/residual). **`get_any` (Train 2's own addition, not `get`) is the
  restore driver's own read primitive** — `get`'s explicit-`replicas`
  contract needs a recorded replica list no backup object carries (the same
  residual `backup_janitor.rs`'s doc names for `delete`), so `get_any` goes
  through the trait's own best-effort `SegmentStore::get(id)` instead
  (local copy first, then every one of the store's *current* placement
  candidates) — sound because a backup's own data objects are immutable and
  `Available`-gated, so "ask any node" always finds an already-committed
  answer. `get`/`delete` (the explicit-`replicas` pair) stay individually
  `#[allow(dead_code)]`-marked — neither gained a caller in Train 2 either
  (`delete`, unlike the janitor's own `delete_local`, still has no recorded
  `replicas` list to call it with; restore never deletes anything).
  `data --config`/`--cluster-control`+`--cluster-data` still default to
  `BackupStoreConfig::Cluster` internally — no CLI flag reaches either, the
  identical documented gap `--segment-store` has on those same entry points
  (`data --config` has no `cluster_settings`-shaped route to either store;
  `--cluster-control`/`--cluster-data`'s `start_split_cluster_with_growth`
  hardcodes the default for every data-role node it stands up). **`animusd
  control` (W-10) and, since issue #676, `join`/`data --seed` are the
  exceptions**: `--segment-store`/`--backup-store` now thread through all
  three exactly as they do through `--config`/`--node` and `--cluster N`
  (`main.rs`'s `run_control` → `run_node_control_with_stores`; `run_join`/
  `run_data_join` → `run_node_join_with_settings`/`run_node_data_join_
  with_settings`).
- **Both stores gained a real S3 backend (S-04 PR 2, ADR 0059's 2026-09-06
  amendment)** — `SegmentStoreConfig`/`BackupStoreConfig` each gained an
  `S3(S3StoreConfig)` variant (`lib.rs`), selected by `--segment-store`/
  `--backup-store s3://bucket[/prefix]?endpoint=scheme://host[:port]
  &region=region[&path_style=true][&insecure_http=true]`
  (`main.rs`'s `parse_s3_uri`, shared verbatim by `parse_segment_store`/
  `parse_backup_store`). `SegmentStoreHandle`/`BackupStoreHandle`'s own new
  `S3` variant holds `Arc<dyn animus_env::SegmentStore>` — a trait object,
  not the concrete `animus_env::S3SegmentStore<animus_s3::prod::
  HyperRustlsTransport>` production actually constructs (`s3_segment_store`,
  `lib.rs`) — specifically so an in-crate test (`s3_store_handle_tests`,
  below) can build the identical variant over `animus_s3::fake::FakeS3`
  with no change to either enum's shape; every other variant (`Cluster`/
  `Fs`) is handled identically for `S3` in every method (no per-node
  replica concept — "ask any node," the same signal `Fs` already sends).
  `build_segment_store`/`build_backup_store` became fallible
  (`std::io::Result<..>`, `?`-propagated at their 3 call sites in `lib.rs`)
  purely for the `S3` arm's own `HyperRustlsTransport::new()`/
  `new_allow_insecure_http()` construction, which can fail only if a
  *different* rustls crypto provider is already installed process-wide
  (never actually possible in this process — nothing else in `animusd`
  installs one — but propagated rather than `.expect()`ed, per this repo's
  no-panic-on-a-remote-possibility discipline). **Both functions also
  became `async` in ADR 0069 S-03 PR 2** — the `Fs`/`S3` arms' loud-refusal
  marker check (`animus_env::verify_or_init_segment_store_marker`/
  `EncryptedSegmentStore::open`) does real I/O, so this is now the
  *expected*, not just the theoretical, way either function returns
  `Err`: a key/store mismatch on `Fs`/`S3` refuses at node startup, before
  any listener binds. `Cluster` is still infallible (and, since PR 2,
  still the one variant `--encryption-key` never touches at all — see
  that ADR's PR 2 amendment for why).

  **Credentials are deliberately NOT a `ClusterConfig` field** — see this
  file's own `config.rs` entry above for why a new field there means a
  compiler-enumerated ~55-60-call-site `error[E0063]` fan-out across every
  `ClusterConfig { .. }` literal in `src/`+`tests/`; a feature whose own ADR
  already specifies "static, file/env-sourced, no cluster-wide semantics"
  credentials had no reason to pay that cost. Instead: `--s3-credentials
  PATH` (a standalone JSON file, `main.rs::S3CredentialsFile` —
  `{"access_key_id": "...", "secret_access_key_file": "..."}` or
  `{"access_key_id": "...", "secret_access_key_env": "VAR"}`, exactly one
  of the two secret sources, mirroring ADR 0064's `tls` section's own
  cert/key-**path** precedent rather than `dynamo_auth`'s in-`ClusterConfig`
  static-map one), falling back to the `ANIMUS_S3_ACCESS_KEY_ID`/
  `ANIMUS_S3_SECRET_ACCESS_KEY` environment variables
  (`main.rs::resolve_s3_credentials`) when the flag is omitted. Resolved
  **once** per process (`run`/`run_control`, before either
  `parse_segment_store`/`parse_backup_store` call) and passed to both —
  an `s3://` store with no credential resolvable anywhere is a startup
  error naming both sourcing options, never a panic; a process with
  neither store set to `s3://` never even attempts resolution's own
  fs/env reads to fail on. `--s3-credentials`/`--allow-insecure-s3` (next
  paragraph) reach `run` (`--config`/`--node` and `--cluster N`),
  `run_control`, and — since issue #676 — `run_join`/`data --seed`'s own
  `run_data` dispatch too, the identical set `--segment-store`/
  `--backup-store` themselves now reach; not `data --config`/
  `--cluster-control`+`--cluster-data`, the identical documented gap those
  two flags still have on those entry points.

  **The insecure-HTTP gate is enforced entirely inside `parse_s3_uri`,
  at parse time**: `endpoint`'s own `http://`/`https://` prefix must agree
  with the URI's `insecure_http` query key (an `http://` endpoint always
  needs `insecure_http=true` and vice versa — this never infers a TLS
  decision from the scheme string alone, so a copy-pasted `http://` can't
  silently downgrade a production config), and `insecure_http=true` against
  a non-loopback host (`is_loopback_host` — a conservative literal
  `localhost`/`127.0.0.0/8`/`::1` match, never a DNS resolution) is refused
  unless `--allow-insecure-s3` is also given. `path_style=false`
  (virtual-hosted addressing) is rejected as unimplemented — this client
  only ever addresses path-style — rather than silently ignored.

  **Admin surface**: `GET /admin/segment-store`/`GET /admin/backup-store`
  render `"kind": "s3"` with a `location` of `s3://bucket[/prefix]@host`
  (host only — no query string, no credentials, ever) through the
  pre-existing `StoreView`/`redact_store_location` machinery — the `S3` arm
  of `StoreView`'s two `From` impls (`s3_store_location`/`s3_endpoint_host`,
  `lib.rs`) is the only admin-side code this needed, since both routes
  already project every store kind through that one type.

  **Testing**: `main.rs`'s own `tests` module gained the URI-shape/
  credential/insecure-http-gate matrix (accepted-well-formed, missing
  endpoint/bucket/credentials, http-without-insecure_http, insecure_http
  against loopback vs. non-loopback with/without `--allow-insecure-s3`,
  `path_style=false`, an unknown query key, and `S3CredentialsFile`'s own
  file/env resolution) directly on `parse_segment_store`/`parse_backup_store`/
  `resolve_s3_credentials`. `lib.rs`'s own `s3_store_view_tests` (in-crate,
  needs no private access — could have lived in `tests/` but sits beside
  `redact_store_location_tests` for locality) asserts the admin-surface
  rendering never leaks the credential, string-searching the rendered
  location the same way `admin_config_reports_auth_state_and_never_
  serves_the_secret` does. **`lib.rs`'s own `s3_store_handle_tests`
  is this PR's end-to-end proof, deliberately scoped smaller than a live-
  node e2e** — it needs `BackupStoreHandle`/`SegmentStoreHandle`'s
  `pub(crate)` visibility (no external `tests/*.rs` file can reach them,
  the identical reason `simenv_client_ctx_tests` lives here too), and
  builds a real `BackupStoreHandle::S3`/`SegmentStoreHandle::S3` directly
  over `animus_s3::fake::FakeS3` (no real sockets, `animus-s3`'s `fake`
  feature — this crate's own `[dev-dependencies]` entry), then drives the
  exact `put`/`put_sealed`/`list_local`/`get_local`/`get_any`/`delete_local`
  methods `backup_capture.rs`/`backup_janitor.rs`/`admin.rs` call in
  production. **What this deliberately does NOT do**: stand up a full
  running `Node` with an injected transport and drive `CreateBackup`/
  `DeleteBackup` over the real DynamoDB wire — that would need either
  widening `BackupStoreHandle`'s visibility to `pub` (a public-API change
  this PR didn't need) or a second, parallel node-construction entry point
  accepting a pre-built handle instead of a `BackupStoreConfig` (a
  materially larger change to `spawn_common_tail`'s own call chain); a
  live-node fake-transport e2e is a reasonable follow-up, not required for
  this PR's own correctness claim, which rests on the handle-level proof
  above being the identical code path the wire-level operations call
  through. See `crates/animus-env/CLAUDE.md`'s own `S3SegmentStore` entry
  for the store's own object-layout/write-once/retry design and
  `docs/adr/0059-backup-restore.md`'s "As-built: PR 2" amendment for the
  full account.
- **`backup_capture.rs`** (ADR 0059 §4/§5/§6, Train 1 PR③) — the on-demand
  backup **capture driver**: a per-tablet, leader-side, event-driven loop
  (`backup_capture_loop`, the same "run everywhere, self-gate per tablet on
  `group.is_leader()`" shape as the GSI drain/TTL reaper) that sweeps a
  `Creating` backup's `KIND_BASE`/`KIND_LSI`/`KIND_FOOTPRINT` rows into
  chunked backup-store objects (`animus_cp_data::backup::
  backup_data_object_id`/`encode_data_chunk`) via `animus_cp_data::
  RaftKvNode::local_scan_kind_snapshot`, then reports completion through
  `MetaCommand::RecordBackupTabletComplete` (on the `is_relayable_command`
  allowlist — a tablet's own leader need not be, and on a split deployment
  may not even be control-connected to, the control-plane leader).
  **Targeting is real production code, not reimplemented here**:
  `animus_control::Metadata::backup_capture_target` (directly pinned, or a
  live `split_lineage` descendant of a retired pinned tablet, ADR 0059 §6)
  is the one predicate this driver, the completion aggregator, and the
  `ANIMUS_BACKUP_SEEDS` corpus all share. The module's own doc has the full
  object-identity write-once argument (a durable `CaptureCursor`
  — `KIND_CURSOR` row, tag `format!("backup:{backup_id}")`, registered in
  `animus_cp_data::cursor::classify_tag` as
  `SplitPolicy::RestartFromScratch` — pins `cut_version` **once**, at a
  tablet's first tick for a given backup, and never re-derives it, which is
  what keeps a retried chunk's content byte-identical across a leader
  change) and the deliberately minimal quiescence posture (wakes only
  immediately before a tick that actually proposes something, mirroring
  `ttl_reaper.rs`'s "read for free, wake only to write" discipline — never
  a standing veto like the split-build driver's). Spawned on combined and
  data-only nodes only (a control-only node hosts no CP-data tablet).
- **`backup_completion.rs`'s loop body moved to `animus_node::
  backup_completion`** (ADR 0061 rung C2) — this module is now a thin
  wrapper (`ctx.env.clone()` + a call into the moved, `E: Env`-generic
  loop; `ClientCtx` implements the two capability traits it needs,
  `client_ctx_host.rs`). Design unchanged from the paragraph below, which
  now describes the moved code — see `animus-node/CLAUDE.md`'s own rung C2
  entry for the trait shapes.
- **`backup_completion.rs`** (ADR 0059 §3/§4, Train 1 PR③) — the on-demand
  backup **completion aggregator**: a control-plane-**leader**-only
  background loop (`backup_completion_loop`), the identical self-gating
  shape as `index_backfill.rs`/`segment_janitor.rs`. For every `Creating`
  backup: once `Metadata::backup_ready_to_complete` (§6-aware — every
  pinned tablet's own current live capture frontier has reported) answers
  true, assembles the manifest object from `Metadata::
  backup_manifest_tablet_progress` (never a blanket scan of every
  `backup_tablet_progress` row — that accessor's own doc, and
  `docs/engineering-lessons.md`'s entry on it, explain why a
  split-superseded stale report must never double-count into the
  manifest), `put`s it **before** proposing `MetaCommand::CompleteBackup`
  (durable-before-visible, ADR 0059 §4) — or, past a driver-local
  `STUCK_CREATING_TIMEOUT` (10 minutes, no CLI knob yet) with zero observed
  report-count growth, proposes `MetaCommand::FailBackup`. **The
  control-only-leader scope gap `segment_janitor.rs` used to document is
  closed (W-10)**: failing always needed only `Metadata` (a control-only
  leader could always do it); completing needs a `BackupStoreHandle` to
  durably `put` the manifest, which every node shape now provisions
  (`ClientCtx::backup_store`, never gated on data role) — spawned on
  combined and control-only nodes (never data-only, which never becomes
  control leader at all).
- **`backup_janitor.rs`'s loop body moved to `animus_node::backup_janitor`**
  (ADR 0061 rung C2) — this module is now a thin wrapper, same shape as
  `backup_completion.rs`'s own move above. The regression tests that used
  to live in this file's own `#[cfg(test)] mod tests` (a hand-rolled
  `FsSegmentStore` + `reclaim_one` helper) moved with the logic, now
  `animus_node::backup_janitor::tests` against a synthetic
  `BackupObjectStore` and a real single-voter `RaftNode<SimEnv>` — see
  that crate's own doc.
- **`backup_janitor.rs`** (ADR 0059 §3, Train 1 PR④) — the on-demand
  backup **janitor**: a control-plane-**leader**-only background loop
  (`backup_janitor_loop`), the identical self-gating shape as
  `segment_janitor.rs`/`backup_completion.rs`. For every `Expired` (a wire
  `DeleteBackup` call's own mark, `dynamo::delete_backup` → `MetaCommand::
  MarkBackupDeleted`) or `Failed` (the completion aggregator's own stuck-
  timeout) row: reclaims objects, then finalizes with the pre-existing,
  unmodified `MetaCommand::DeleteBackup` (row-plus-progress removal, PR①).
  **Reclaim is local-only** — a deliberate Train 1 simplification, unlike
  the segment janitor's own cataloged-`replicas` reclaim: no backup object
  carries a recorded replica list (`backup_capture.rs`/`backup_
  completion.rs` both discard `BackupStoreHandle::put`'s own returned
  replica set) and a tablet's completion record carries no chunk count
  either, so there is no way to enumerate a backup's own object ids without
  asking the store — `SegmentStore::list()`, scoped to `backup/{backup_id}/`
  on this node's own local directory (`BackupStoreHandle::list_local`/
  `delete_local`, no longer `#[allow(dead_code)]` as of this PR), exactly
  the tool ADR 0059 §3 licenses for this. **Named residual**: on a cluster
  larger than `ClusterSegmentStore::DEFAULT_K` (3) a leader that never holds
  a copy of a given backup's objects finalizes (removes the row) on its very
  first tick, before a node that does hold a copy ever sweeps its own —
  those copies become permanent, uncataloged orphans. See the module's own
  doc, the ADR's 2026-08-27 as-built amendment, and `docs/engineering-
  lessons.md` for the full note. **The control-only-leader scope gap
  `segment_janitor.rs`/`backup_completion.rs` used to document is closed
  (W-10)**: object reclaim needs a `BackupStoreHandle`, which every node
  shape now provisions (`ClientCtx::backup_store`) — spawned on combined
  and control-only nodes, never data-only. **Publishes its own progress
  (roadmap U-07)**: the loop's own `animus_node::backup_janitor::
  JanitorProgress` (phase — `Idle`/`Reclaiming`/`RemovingRow` — plus
  `last_tick_at_ms`, cumulative `backups_seen`/`objects_reclaimed`,
  `last_error`, and the backup id currently being worked) is published at
  each phase transition through a new capability trait,
  `animus_node::host::BackupJanitorProgressHost`, into
  `ClientCtx::backup_janitor_progress: Arc<std::sync::Mutex<
  JanitorProgress>>` (`client_ctx_host.rs`'s impl is the usual thin,
  logic-free delegation — a short lock/mutate/drop, never held across an
  `.await`, mirroring `metrics_history`'s own precedent). `GET
  /admin/backup-store` (`admin.rs::backup_store_view`) reads it back out
  alongside this node's own store config (redacted via the new
  `redact_store_location` helper, `lib.rs`) and a bounded live
  `list_local` scan for object count/bytes (`BackupStoreHandle::
  get_local`, a new local-only sibling of `list_local`/`delete_local`,
  capped at 200 objects with `"truncated"` past the cap) — see ADR 0020's
  and ADR 0059's matching 2026-09-06 as-built notes for the full route
  design. A non-leader's own progress simply stays `Idle` forever, since
  the loop only ever advances it while `control_leader()` answers `Some`
  — an honest answer, not a gap. Regression:
  `tests/admin_endpoint.rs::
  admin_backup_store_reports_reclaim_progress_and_leader_state`,
  `tests/dashboard_endpoint.rs::dashboard_u07_backup_store_card`, and
  `animus_node::backup_janitor::tests`' own progress assertions.
- **`backup_restore.rs`** (ADR 0059 §7, Train 2; §10, Train 3 PR②) — the **restore driver**:
  a per-tablet, leader-side, event-driven loop (`backup_restore_loop`, the
  identical "run everywhere, self-gate per tablet on `group.is_leader()`"
  shape as `backup_capture.rs`/the GSI drain/TTL reaper) that seeds a
  `Seeding` restore's single destination tablet from its backup's data
  objects, then activates it. **Deliberately no durable per-tablet cursor**
  (unlike `backup_capture.rs`'s own `CaptureCursor`) — each tick's own
  `restore_tick` call sweeps the WHOLE manifest from its first chunk, safe
  because `KvCommand::SeedBatch`'s merge-at-carried-version is idempotent
  regardless of how many times a chunk repeats; a crash/leader-change
  simply restarts the full sweep from scratch on whatever engine state a
  new leader already has. **Every captured value is re-wrapped via
  `animus_cp_data::backup::encode_restored_value`** before merging — a real
  bug found by this train's own first end-to-end test (a `SeedBatch` merge
  of a capture's already-resolved, envelope-less value corrupts the engine
  from a later read's point of view; see that function's own doc and
  `docs/engineering-lessons.md` for the full incident). Bounded liveness:
  a driver-local (non-durable) per-restore progress clock
  (`RESTORE_STUCK_TIMEOUT`, 10 minutes) proposes `MetaCommand::FailRestore`
  once a `Seeding` restore goes that long with no observed forward
  progress — embedded directly in this per-tablet driver rather than a
  separate control-plane-leader aggregator (unlike `backup_completion.rs`),
  since a restore has exactly one destination tablet, so its own leader is
  already the sole authority on its progress. Once every chunk is seeded,
  `complete_restore` proposes `MetaCommand::CompleteRestore` and then — in
  the same step, not before — declares every one of `RestoreRow::gsi_defs`
  via `MetaCommand::CreateTableIndex` (ADR 0059 §8): see
  `animus-control/CLAUDE.md`'s restore-catalog entry for why this ordering
  is load-bearing, not incidental. Spawned on combined and data-only nodes
  only (mirrors `backup_capture.rs`'s own scope — a control-only node hosts
  no CP-data tablet).

  **PITR replay (Train 3 PR②) is one more phase of this same
  `restore_tick`, not a parallel driver** — a `Seeding` restore whose
  `RestoreRow.pitr` carries a `PitrRestorePlan` runs `replay_pitr_segments`
  after the ordinary base-manifest chunk sweep above: for each of the
  plan's own (already-resolved, epoch-ordered) `PitrReplaySegmentRef`s, it
  fetches + decodes the segment (`animus_cp_data::segment::
  decode_and_slice`), decodes every `ChangeRecord`, skips
  `consumer_hidden()` ones (markers/seeded/staged records — never real
  content), and re-derives `KIND_BASE`/`KIND_LSI` writes via
  `dynamo::kind_writes_for_item` — the same pure function a live write's
  own leader-side evaluation already uses, so PITR replay needs no second
  implementation of LSI derivation. `KIND_CHANGE`/`KIND_FOOTPRINT` are
  never replayed (a footprint is rebuilt fresh by the post-activation GSI
  backfill regardless of how base content arrived). Every derived value
  goes through the identical `encode_restored_value` re-wrap the base-chunk
  sweep already uses — confirmed load-bearing here too by this PR's own
  first end-to-end run, exactly as Train 2's own as-built note had
  predicted. Everything downstream (`complete_restore`'s activation +
  GSI-declare sequence) is unmodified: a PITR restore looks identical to an
  on-demand one from that point on.
- **`dynamo.rs`'s `restore_table_from_backup` (ADR 0059 §7, Train 2)** —
  `RestoreTableFromBackup`'s wire handler: validates the backup
  (`BackupNotFoundException`/`BackupInUseException`, `visible_backup`'s own
  "`Expired`/`Failed` read as not-found" convention) and the target name
  (reserved-namespace check; `ResourceInUseException` — the identical code
  `create_table`'s own duplicate-name check uses — if it already exists),
  proposes `CreateTableSchema` (the manifest's schema stripped of
  `indexes`/`stream`/`ttl` — TTL/streams are deliberately never
  re-enabled) then `CreateTableIndex` for every LSI the manifest recorded
  (LSIs are always `Active`, colocated physical data this restore is about
  to seed — no backfill involved), resolves the restore's own GSI plan
  (`GlobalSecondaryIndexOverride` or the manifest's own GSIs, forced
  `Creating`) without yet declaring it, then proposes `MetaCommand::
  BeginRestore` and returns **immediately** — unlike `create_table`'s own
  blocking `await_table_serveable` wait, this is asynchronous by design
  (the restore driver above does the seeding/activation/GSI-declare in the
  background). `dynamo::table_status` derives `DescribeTable`'s
  `TableStatus` purely from whether every one of a table's *current*
  tablets is `Active` (`CREATING` while any is `Building`) — no new
  persisted state, the identical "derive from live state" discipline an
  `IndexStatus`/`BackupStatus` already follow. **A deliberate, named AWS
  deviation**: the restore response (and any `DescribeTable` before
  completion) shows no GSIs at all, not even `CREATING` — they only appear
  once the base table activates and `CreateTableIndex` actually declares
  them (see the ADR's Train 2 amendment for the full reasoning and the
  wire-layer-only follow-up that would close this gap).
- **`dynamo.rs`'s `restore_table_to_point_in_time` (ADR 0059 §10, Train 3
  PR②)** — `RestoreTableToPointInTime`'s wire handler, sharing
  `provision_restore_target`/`finish_restore_kickoff` (factored out of
  `restore_table_from_backup` for this purpose) for the schema/LSI/GSI-plan
  resolution and `BeginRestore`-kickoff steps: only the *source selection
  and validation* half is new. Resolves `T` (`RestoreDateTime`, truncated
  to the second, or `UseLatestRestorableTime`) against
  `Metadata::pitr_restore_window(source_table_name)` — `None` there means
  either the source table has no PITR history at all
  (`TableNotFoundException`) or PITR was never enabled
  (`PointInTimeRecoveryUnavailableException`, distinguished by whether
  `Metadata::pitr_generation` has ever seen this table name); `T` outside
  `[earliest_floor_ms, latest_ms]` (the whole-second floor fix — see the
  ADR's own as-built amendment) is `InvalidRestoreTimeException`, the same
  code AWS uses for this case. Once validated, picks the newest `Available`
  row from `Metadata::pitr_base_backups_for_table` at or before the cutoff
  (`t_ms + 999` for a literal `T`, or `latest_ms` for
  `UseLatestRestorableTime`), fetches and decodes that backup's own
  manifest to build `base_tablet_progress`, and calls `Metadata::
  pitr_replay_segments` (never re-derived here) to build the
  `PitrRestorePlan` handed to `finish_restore_kickoff`. Same asynchronous-
  kickoff contract as `restore_table_from_backup`: returns as soon as
  `BeginRestore` commits, with `backup_restore.rs`'s driver doing the
  actual seed/replay/activate/GSI-declare sequence in the background.
- **`split_placing_completion.rs`** (ADR 0062 §3) — the in-place split
  **directed-Placing completion loop**: a per-tablet, leader-side loop, the
  same "run everywhere, self-gate per tablet on `group.is_leader()`,
  propose a relayable completion command once local convergence is
  observed" shape as `backup_capture.rs`/`index_backfill.rs` (`RaftKvNode::
  spawn_reconfigure_loop` has zero production callers — see the ADR's own
  §3 correction to its brief's original anchor). Each tick, for every led
  tablet with an un-`done` `Metadata::split_placing` entry: if the live
  Raft group's own voter config matches `Metadata`'s current `replicas`
  with no dangling learners (`CpGroup::config()`/`CpGroup::learners()`,
  the second newly unwrapped by this rung, the first pre-existing since ADR
  0029's own release-GC use) — the identical convergence predicate
  `RaftKvNode::reconfigure_step`'s own early return already checks —
  **and that observation has held continuously for
  `SPLIT_PLACING_DONE_SETTLE`** (a small multiple of `animus-control::
  node`'s own `RECONCILE_INTERVAL`, tracked in a driver-local `BTreeMap<
  TabletId, Nanos>`), proposes `MetaCommand::MarkSplitPlacingDone` via
  `ClientCtx::propose_schema` (on the `is_relayable_command` allowlist).
  **The settle window is load-bearing, not defensive padding** — a naive
  one-shot compare marks `done` on the very first tick after cutover,
  before the control-plane's own reconcile loop has had a single chance to
  move the tablet off its fork-inherited (trivially "converged" against
  itself) replicas; see `docs/engineering-lessons.md`'s entry on this rung
  for the full incident (found via this loop's own real end-to-end test,
  not by inspection) and a separate, pre-existing `reconfigure_step`
  oscillation the same investigation surfaced (a 2-of-3-replica-swap target
  failing to converge under a real cluster) — unrelated to and unmodified
  by this loop, not fixed here. Spawned on combined and data-only nodes
  only (`start_with_growth`'s two call sites — mirrors `backup_capture.rs`/
  `backup_restore.rs`'s own scope: no control-plane-leader dependency of
  its own, so a control-only node, which hosts no CP-data tablet, gets
  nothing from spawning it). **Status surface**: no new admin/dashboard
  code — `admin.rs`'s `status_json` already serializes `effective_metadata()`
  directly, so a live `split_placing` entry (and its `done` flag) is
  visible for free, the same "derive from already-replicated state, don't
  build a bespoke view" discipline every other diagnostic here follows.
  **Issue #513 (a suspected `reconfigure_step` oscillation for a
  two-(or-more)-replica-difference target) was investigated and closed as
  not reproducible** — see `crates/animusd/tests/
  split_placing_two_replica_diff_e2e.rs`, `crates/animus-cp-data/tests/
  reconfigure_multi_replica_diff.rs`, and `docs/engineering-lessons.md`.
  This loop's own convergence predicate reaches `done` for a
  multi-replica-difference target exactly like a one-replica one; no
  known gap remains. **Issue #596**: that e2e test's own assertion that the
  swap passes through the transient 5-voter intermediate used to sample
  `/admin/raftkv` externally every 200ms and assert on the observed max —
  flaky under load (a fast-enough pair of consecutive reconciler ticks can
  remove both extras between two samples), since the intermediate's own
  *duration* was never a guarantee, only that it is logically reached. Fixed
  by reading `RaftKvNode::voter_history()` (`animus-cp-data`) — a durable
  in-process record of every distinct voter configuration adopted, not an
  external poll — instead; see that crate's `CLAUDE.md` for the mechanism
  and `docs/engineering-lessons.md`'s matching entry for the general lesson.

  **Issue #528 (this loop's own predicate never firing under sustained
  load) was root-caused and fixed entirely in `animus-control` — this
  file's own logic, settle tracker, and leader gate are unchanged.** The
  actual defect was one layer below: the §2 reconcile-loop phase this
  loop's own predicate depends on (`Metadata::split_placing_reconcile`)
  used to recompute `select_replicas` fresh every tick, which under real
  failure-detector flap moved the target itself faster than
  `reconfigure_step` could ever converge — so `group.config() ==
  t.replicas` simply never held long enough to settle, regardless of how
  correct this loop's own settle-window logic was. Fixed by making
  `split_placing[..].target` authoritative (driven toward verbatim while
  healthy, dwell-gated before a genuinely-dead member triggers a
  REPLICATED retarget) — see `crates/animus-control/CLAUDE.md`'s ADR 0062
  entry and ADR 0062's 2026-09-01 amendment for the full mechanism.
  `tests/split_placing_completion.rs` stays green, unmodified, and is now
  a stronger proof than before: it demonstrates this loop actually firing
  under the corrected upstream mechanism.
- **`dynamo.rs`'s `ExportTableToPointInTime`/`DescribeExport`/`ListExports`
  (ADR 0068, S-05 PR 1)** — a single leader-driven **export job**, run once
  on whichever node received the wire request (`create_export` proposes
  `MetaCommand::BeginExport`, mints a `CREATE_EXPORT_ID_ATTEMPTS`-bounded
  fresh `ExportId`/ARN via `wire::export_arn`, then `tokio::spawn`s
  `run_export_job` — permitted here since `dynamo.rs` is **not** one of the
  ten `#[deny(clippy::disallowed_methods)]` modules the root `CLAUDE.md`
  lists), deliberately **not** the backup catalog's per-tablet
  leader-side-capture-plus-completion-aggregator shape (ADR 0059 §4/§5) —
  see ADR 0068 §1 for the full reasoning: an export's payload is
  base-rows-only DynamoDB JSON with no restore-back-into-cluster need, and
  `ctx.cp_scan` (the same primitive `Scan` itself uses) already fans out
  across a table's tablets and tolerates a concurrent split transparently,
  so one job needs no per-tablet progress catalog at all. **Known
  residual, stated plainly rather than silently shipped**: this buys no
  crash-resumability — a node crash mid-export leaves the row `InProgress`
  forever (no janitor reclaims it in this PR; PR 2/3 territory).
  `create_export`'s `ClientRequestToken` idempotency (`Metadata::
  export_by_client_token`) returns the existing export's description on a
  retried token rather than minting a second job. `validate_export_time`
  checks an `ExportTime` request against `Metadata::pitr_restore_window`
  (reused verbatim from the PITR restore validation, `InvalidExportTimeException`
  outside the window or with no PITR history at all) — **but the export's
  own content is always current-state, never actually replayed to that
  point in time** (a true point-in-time replay would need adapting
  `backup_restore.rs`'s PITR segment-replay machinery, out of scope for
  this PR and named as ADR 0068 §9's "Known residual #2" rather than
  silently shipped as if fully correct).

  **`run_export_job_inner`'s object layout** (constants `EXPORT_MANIFEST_ROOT
  = "AWSDynamoDB"`, `EXPORT_CHUNK_ROWS = 1000`, mirroring AWS's own real
  export layout closely enough for the e2e test's own round-trip
  assertions, not byte-for-byte AWS-identical): under
  `AWSDynamoDB/<export-id-suffix>/`, a `_started` marker object (written
  first, before any data — a crash-detection breadcrumb, unused by
  anything in this PR itself), one gzip'd `data/NNNN.json.gz` object per
  `EXPORT_CHUNK_ROWS`-row chunk (`{"Item": <DynamoDB JSON>}` lines,
  `wire::encode_item`/`decode_stored_item` — a tombstone value decodes to
  `None` and is skipped, never exported as a row), `manifest-files.json`
  (one line per data file: `itemCount`/`md5Checksum`/`etag`/
  `dataFileS3Key` — `md5Checksum`/`etag` are **CRC32 stand-ins**
  (`crc32_hex`, reusing the already-present `crc32fast` workspace
  dependency rather than adding a real MD5 crate for two fields nothing in
  this PR itself re-verifies), a documented simplification, not a
  correctness claim), and `manifest-summary.json` written **last** (after
  every data file — durable-before-visible discipline: a reader should
  never see a summary pointing at data files that might not exist yet).
  `gzip_bytes` uses `flate2`'s pure-Rust `rust_backend`/miniz_oxide feature
  (new workspace dependency, `Cargo.toml`'s own comment explains the
  no-C-FFI choice mirrors the `lz4_flex` precedent for this workspace's
  `unsafe_code = "forbid"` posture).

  **Customer-bucket store configuration and injection**: `ExportS3Config`
  (`endpoint`/`region`/`insecure_http`/`credentials`) is this node's own
  resolved ability to *reach* S3 at all (from `--export-s3-endpoint`/
  `--export-s3-region`, reusing the existing `--s3-credentials`/
  `ANIMUS_S3_ACCESS_KEY_ID`/`ANIMUS_S3_SECRET_ACCESS_KEY` credential
  resolution `--segment-store`/`--backup-store s3://...` already
  established, S-04) — **not** which bucket a given export targets, which
  is per-request (`S3Bucket`/`S3Prefix` on the wire call itself, the
  "distinct wire model" ADR 0059 §1 deferred this whole feature over).
  `ExportStoreFactory = Arc<dyn Fn(&str, Option<&str>) -> io::Result<
  Arc<dyn animus_env::SegmentStore>> + Send + Sync>` is the seam: given a
  bucket + optional prefix, build a store handle for it.
  `default_export_store_factory(export_s3: Option<ExportS3Config>)` is the
  production factory — `None` (no `--export-s3-endpoint`/`--export-s3-region`
  configured on this node) makes every export attempt fail immediately
  with a plain, named startup-shaped error ("S3 export is not configured on
  this node"), never a panic. Stored on `ClientCtx::export_store_factory:
  Arc<Mutex<ExportStoreFactory>>` (swappable in place, so every
  already-cloned per-connection `ClientCtx` sharing the `Arc` sees a
  replacement) and mirrored on `Node`, with a genuinely **`pub`** (not
  `#[cfg(test)]`-gated) `Node::set_export_store_factory` — needed because
  `ClientCtx` is `pub(crate)` and a `#[cfg(test)]`-gated field/method is
  invisible to an external `tests/*.rs` integration binary (which links
  against the plain, non-test-cfg library); this is the one production
  method whose sole purpose is a test injection point, mirrored on `Node`
  rather than reusing the existing `#[cfg(test)] test_ctx` pattern for
  exactly that reason. **CLI reach is deliberately narrow, the same
  documented-gap shape `--dynamo-auth`/`--segment-store` already have**:
  `--export-s3-endpoint`/`--export-s3-region` thread through
  `main.rs::resolve_export_s3` → `run_single` → `run_node_with_cluster_
  settings` → `run_node_with_streams_quiesce_and_ttl_sweep_interval` →
  `BoundNode::start_with_growth` → `spawn_common_tail`'s trailing
  `export_s3` parameter — reaching **only** `--config FILE --node I`;
  every other entry point (`--cluster N`, `--cluster-control`/
  `--cluster-data`, `animusd control`, `animusd data`, `animusd join`)
  passes `None` and never provisions export capability via CLI at all
  (`crates/animusd/tests/dynamo_export.rs` never needs the CLI path either
  way — it injects a fake store factory directly via
  `Node::set_export_store_factory`, bypassing CLI/S3 credentials
  entirely).

  Regression: `tests/dynamo_export.rs` — a shared `FakeS3` (wrapped in a
  test-local `SharedFakeS3(Arc<FakeS3>)` `Transport` newtype, since
  `animus_s3::fake::FakeS3` itself is not internally `Arc`-shared/`Clone`,
  so every node's own factory call and the test's own verification reads
  must share one externally-held `Arc` to see the same bucket) installed
  on every node of a real 3-node cluster: a full export issued against a
  **follower** node, with a real forced split mid-scan, converges to
  `COMPLETED` and every one of 30 written items round-trips through the
  gzip'd data files back to `animus_dynamo::wire::decode_item`; `ListExports`
  with/without a `TableArn` filter; `ClientRequestToken` idempotency;
  unknown-table/unknown-export errors; `ION`/`INCREMENTAL_EXPORT`
  rejection; and an `ExportTime` request against a table with no PITR
  history rejecting `InvalidExportTimeException`.
- **`dynamo.rs`'s `ImportTable`/`DescribeImport`/`ListImports` +
  `import.rs`'s driver (ADR 0068 §6, S-05 PR 2)** — the mirror-image data
  flow of the export trio just above: reads a customer S3 bucket's
  DynamoDB JSON export layout (this adapter's own, or real AWS's) back
  into a **brand-new** table. Modeled on **both** the export catalog (an
  ARN-shaped `ImportId`, no delete/reclaim command, `is_relayable_command`
  for all three `MetaCommand`s since the job may run on any node) and the
  restore driver (`crate::backup_restore`, a per-tablet leader-side
  event-driven loop, `KvCommand::SeedBatch`, a single `Building`
  destination tablet that keeps the target unroutable to an ordinary
  client write until the import completes — the same mechanism a restore
  target uses, reused rather than inventing a new tablet state, GSIs
  resolved at `IndexStatus::Creating` but declared only at completion).
  **Deliberately not folded into `backup_restore.rs` itself** — see
  `import.rs`'s own module doc for why it's a sibling module instead (a
  different source shape needing real item→row *derivation*
  (`kind_writes_for_item`, the identical primitive PITR replay uses)
  rather than restore's own "re-wrap already-physical captured bytes"
  merge, and a fixed constant seed version rather than a captured
  real one).

  `dynamo::create_import` (ADR 0068 §6): `ClientToken` idempotency first
  (`Metadata::import_by_client_token` — **not** scoped by table, unlike
  export's `(table, token)` pair, since a retried `ImportTable` names the
  same target table by construction), then `provision_import_target`
  commits the target schema through the **identical** `CreateTable`
  decode/validation helpers (`decode_key_schema`/`decode_attribute_types`/
  `decode_indexes`/`decode_create_table_throughput`/
  `check_attribute_definitions`, via the new
  `wire::TableCreationParameters` type) so schema validation, GSIs, and
  throughput behave identically to an ordinary `CreateTable` — a name
  already registered (a real pre-existing table, **or** another import
  already claiming it, since `BeginImport` proposes this same
  `CreateTableSchema` before minting its own row) is
  `ImportConflictException`, real DynamoDB's own code for both cases, and
  this is the entire "one import per target name at a time" enforcement
  (no separate concurrency check). `finish_import_kickoff` then mints the
  destination tablet + `BeginImport` row and returns immediately —
  asynchronous, like restore.

  `import.rs`'s `import_tick` (once per `IMPORT_TICK_INTERVAL` per led
  tablet, no durable cursor — safe to re-sweep on retry because every
  seeded row carries the fixed `IMPORT_SEED_VERSION`, and
  `SeedBatch`'s merge-at-carried-version only applies a *strictly newer*
  version, `animus-storage`'s own `merge` contract): resolves
  `manifest-summary.json` via `ctx.export_store_factory` (the identical
  seam the export job uses — kept as one shared name rather than renamed,
  a deliberate no-op decision), supporting **both** of DynamoDB's own
  `S3KeyPrefix` shapes (the level above the export's own `AWSDynamoDB/<id>/`
  folder, listed; or the export folder itself, direct — `rebase_recorded_key`
  rewrites the manifest's own recorded keys in the second case, since this
  adapter's own export writes them relative to whatever store *it* was
  built over, never a bucket-absolute key), streams each data file
  (gunzip when `GZIP`), decodes each `{"Item": ...}` line via
  `animus_dynamo::wire::decode_item`, validates key attributes against the
  target's declared `AttributeDefinitions` (presence + `S`/`N`/`B` type
  match — a mismatch increments `ErrorCount` and is skipped, up to
  `MAX_MALFORMED_ITEMS` (10,000), past which the import fails immediately
  — malformed content can't be fixed by retrying, unlike every I/O fault
  here, which is retried until `IMPORT_STUCK_TIMEOUT`, 10 minutes), derives
  `KIND_BASE`/`KIND_LSI` writes via `crate::dynamo::kind_writes_for_item`
  (never trusts captured physical bytes the way restore's base-chunk sweep
  does — an import's source is customer text, not this cluster's own
  previously-captured rows), and batches them into bounded `SeedBatch`
  proposes (`IMPORT_SEED_BATCH_ROWS`, 500) on this node's own leader
  handle. On full success: `CompleteImport` (freezing
  `ProcessedItemCount`/`ImportedItemCount`/`ErrorCount`/
  `ProcessedSizeBytes`) then declares every resolved GSI, the identical
  restore-driver ordering. **On failure — unlike a failed restore, which
  leaves its target table in place for manual cleanup — the driver also
  drops the half-created target table** (`ClientCtx::drop_table`,
  best-effort: a drop failure here is logged, not retried, since the
  target stays a clean, state-agnostic `DeleteTable` away from full
  cleanup either way), matching real DynamoDB's own "a failed
  `ImportTable` rolls back the table it was creating" contract —
  `ImportRow`/`ImportStatus::Failed`'s own doc has the full reasoning.

  Spawned on combined and data-only nodes only, both existing
  `backup_restore::backup_restore_loop` spawn sites (mirrors that driver's
  own scope exactly — no control-plane-leader dependency, and a
  control-only node hosts no CP-data tablet to seed).

  Regression: `tests/dynamo_import.rs` — reuses the S-04 `FakeS3`/
  `SharedFakeS3` harness `dynamo_export.rs` established (duplicated, not
  shared — this repo's own per-`tests/dynamo_*.rs`-file convention): a
  full export→import round trip (source table force-split, import issued
  against a follower-connected node) converging to `COMPLETED` with exact
  `ProcessedItemCount`/`ImportedItemCount`/`ErrorCount` and every item
  reading back through `GetItem`/`Scan`; a `NONE`-compressed export
  written by hand (an input shape this adapter's own export job never
  produces); two deliberately malformed items counted in `ErrorCount`
  with the rest still imported; `ImportConflictException` for both an
  existing table name and a name-collision with an in-flight import;
  `ImportNotFoundException`; `ListImports` pagination/`TableArn`
  filtering; `ION`/`ZSTD` rejection; and `ClientRequestToken` idempotency.
  **Testing gotcha found building this suite, recorded in
  `docs/engineering-lessons.md`**: a test helper's own default request
  shape (compression) silently diverging from what its paired fixture
  helper actually wrote produced no error at all — just an import stuck
  `IN_PROGRESS` forever, since every fault in this driver is deliberately
  retried rather than surfaced — until the outer test's own
  converged-or-timeout poll finally gave up.
  **A second, real production bug found the same way (CI flake,
  2026-09-06, fixed)**: `dynamo::finish_import_kickoff`'s destination-
  tablet replica pick — the identical "first `min(N, MAX_REPLICATION_
  FACTOR)` `Active` members" snapshot `ClientCtx::provision_tablet` also
  takes — had no guard against that snapshot coming back **empty** (every
  member transiently `Down`, a real-thread failure-detector false positive
  under CPU-starved contention, ADR 0012 — not a `SimEnv`-provable race).
  `provision_tablet`'s own `CreateTablet` tolerates this because its
  tablet mints `Active`, so `reconcile_placement`'s ordinary policy-driven
  self-heal can still grow an under-shot set later; this tablet mints
  `Building` and stays placement-frozen for its whole seeding lifetime
  (`reconcile_placement`'s own `TabletState::Active` gate,
  `animus-control`'s `meta.rs`) — a `Building` tablet with `replicas: []`
  can never become `Active` (hosting requires a nonempty replica set in
  the first place), so it never self-heals: `import_loop` polls "not
  hosted here yet" forever and the wire caller's own terminal-state poll
  times out. Fixed with the same guard `provision_tablet` already
  established — wait (bounded by the existing `SCHEMA_COMMIT_TIMEOUT` per
  attempt) for at least one `Active` member before computing `replicas`,
  and skip proposing `BeginImport` (retry with a fresh id) if the wait
  still ends empty. Reproduced with a foreground loop of the standalone
  test binary run under contention from three sibling `animusd`
  integration-test binaries (50-150 iterations, ~5-9% failure rate before
  the fix, 0/250+ after); see `docs/engineering-lessons.md` for the full
  diagnosis. **`dynamo::finish_restore_kickoff` (issue #657, the kickoff
  half): fixed with the identical guard.** The twin gap this entry
  originally flagged as not-yet-fixed is closed — `finish_restore_kickoff`
  (shared by both `RestoreTableFromBackup`'s and `RestoreTableToPointInTime`'s
  own wire handlers, since both route through it) now calls the same shared
  `await_active_metadata_for_new_tablet` wait/retry helper `finish_import_kickoff`
  was factored to use, so both `Building`-minting kickoffs wait (bounded by
  their own per-attempt `SCHEMA_COMMIT_TIMEOUT`) for at least one `Active`
  member before computing `replicas` via the same `active_replicas_for_new_tablet`,
  and both skip their propose (retrying with a fresh id) if the wait still
  ends empty. Regression: `active_replicas_tests` gained a restore-specific
  pin (`restore_kickoff_shares_the_import_kickoffs_selection`,
  `restore_kickoff_sees_no_replicas_when_every_member_is_down`) — the async
  wait/retry shape itself stays untested the same way `finish_import_kickoff`'s
  own is, for the identical reason (real-thread liveness only, `dynamo.rs`
  is not generic over `E: Env`, out of scope for this fix). Issue #657's
  second half — `backup_restore.rs`'s own missing propose-side patience and
  confirm-timeout logging, the `import.rs`-inherited issue #268 amplification
  shape this same investigation found but did not fix — **remains open, its
  own separate PR.**
- **`ClientRequest::ForceSeal { tablet }`** and **`ClientRequest::
  StreamHotRead { tablet, from_position, limit }`** are the two
  internal-only streams RPCs (F12-b's disable-triggered final seal, and
  the open-shard `GetRecords`/`GetShardIterator` forwarding path) — both
  addressed by tablet id directly, refused bare, handled only inside
  `cp_serve_forwarded`. **Since ADR 0047 both now ride the intra port**
  (`surface_of` classifies them `Surface::Intra`) — a bare send on the
  client port is refused by `handle_request`'s port guard before ever
  reaching their own "must be sent wrapped in `Forwarded`" match-arm
  refusal (that wording is still reachable, just only via the intra port
  now). **Every send of an internal-only variant across
  the wire must wrap it in `ClientRequest::Forwarded`, even when the
  caller already knows it isn't the leader** — a first attempt called
  `ClientCtx::relay` directly with a bare `ForceSeal`, which compiled and
  passed every single-node test (the local branch never goes through
  `relay` at all) but failed loudly the moment a real multi-node test
  exercised the forwarding branch, exactly because the receiving side's
  bare-request refusal is designed to catch precisely that mistake. See
  `docs/engineering-lessons.md`'s Testing section for the general rule
  this is now an instance of (a forwarded-command test suite needs at
  least one non-leader-issued call). Full design/call-site detail:
  `docs/streams-notes.md`.
- **A node runs one internal `ProdEnv`, on one id (ADR 0040)** — the control
  Raft rides `PRIMARY_STREAM` (stream 0, ADR 0026's default); every per-tablet
  Raft group this node hosts rides its own stream (`stream = tablet_id`, which
  floors at 1), so the two never collide on the one shared inbox (a combined
  node used to bind *two* `ProdEnv`s on two distinct ids purely because one
  inbox was single-consumer, before ADR 0026 let one id host several
  protocol instances). The client API is a plain TCP server, *not* on the
  `Network` — a non-leader forwards over a fresh client connection.
- **Two client-protocol listeners, one dispatch (ADR 0047)**: `RoleAddrs.client`
  (external, DynamoDB-adjacent callers) and `RoleAddrs.intra` (every
  node-to-node `ClientRequest` — `Forwarded`, `ProposeSchema`,
  `WatchMetadata`, `JoinInfo`, and every internal-only forwarding payload)
  are the **same** length-prefixed JSON `ClientRequest`/`ClientResponse`
  framing on two ports, not two protocols. `serve_requests`/
  `handle_connection` (`lib.rs`) are one function parameterized by
  `ListenerKind::{Client, Intra}`, never forked; `handle_request` has
  exactly one guard clause before its ~160-line match, refusing a
  `Client`-listener connection asking for a `Surface::Intra`-classified
  variant (`surface_of`, the one exhaustive table, no wildcard arm — a new
  `ClientRequest` variant is a compile error there until classified).
  `Intra` is deliberately a **superset** of `Client`, not a disjoint
  partition — neither port has auth yet, and intra is the more-trusted
  network segment (the operator's Kubernetes topology keeps it off any
  externally-reachable Service), so it transparently also serving ordinary
  client-shaped ops is intentional, not a gap. `--seed`/`animusd join`
  target the **intra** address (joining is a cluster-membership action, not
  an external-client one). Machine-relay address resolution
  (`cp_leader_hint`, `propose_schema`'s relay, `remote_metadata_watch_loop`)
  uses a parallel `intra_route`/`intra_addr`/`intra_leader_hint` — never
  `client_route`/`route_addr`/`leader_hint`, which stay reserved for
  human-facing consumers (`not_leader_error`'s admin message, the
  dashboard's leader display) — see ADR 0047 for the full design and the
  hint-field-conflation finding that shaped this split, and the standing
  rule in `docs/engineering-lessons.md` (machine relay →
  `intra_leader_hint`; anything a human reads → `leader_hint`).
- **`handle_connection` cancels an in-flight request when the peer closes the
  connection (issue #596), on both listeners.** `serve_requests` still
  spawns one untracked, fire-and-forget task per accepted connection (see
  the `WatchMetadata` gotcha below for what that still doesn't fix), but
  the per-connection loop itself no longer runs a request to completion
  with nobody listening: `handle_connection` splits the socket
  (`TcpStream::into_split`) once, then races `handle_request(..)` against a
  `peer_closed(&mut read_half)` future in a `tokio::select! { biased; .. }`
  — `biased` so a response that finishes at the same poll as the
  peer-close observation still gets written; the peer-closed arm only wins
  when the handler has genuinely not finished. `peer_closed` **peeks**
  (`OwnedReadHalf::peek`, which never consumes what it sees) rather than
  reading, and resolves on a clean EOF (`Ok(0)`) or a transport error —
  either means the peer is definitely gone. An `Ok(n)` with `n > 0` means
  the peer is alive and has simply written ahead of reading this request's
  reply — a pipelined next frame, which this protocol doesn't forbid even
  though no client in this repo happens to do it today — and that is not
  abandonment: an earlier version of this function used a plain `read`
  here, which consumed that byte and silently ate the start of the next
  frame while dropping the *current* response on the floor even though the
  peer was still waiting for it. On `Ok(n > 0)` the future instead parks
  forever via `std::future::pending` (registers no waker, cheap to hold),
  so `select!` falls through to the handler's own completion and the next
  loop iteration's `read_frame` consumes the pipelined frame the normal
  way — never loop-and-`peek` to wait out the close either, that would
  busy-spin the task. On the peer-closed branch the in-flight response
  future is simply dropped and the connection loop returns, incrementing
  the `client_requests_abandoned` metric (ADR 0015, `/admin/metrics`) as
  the observable signal.
  `read_frame`/`write_frame` are generic over `AsyncRead`/`AsyncWrite` +
  `Unpin` (not hardcoded to `&mut TcpStream`) precisely so the split
  `OwnedReadHalf`/`OwnedWriteHalf` halves work here with no change to any
  other caller's framing.
  **Why dropping the handler future here is safe, not just convenient**:
  every CP mutation on the client path (`schema`/`read_path`/`write_path`/
  `txn_coordinator`/`forwarding`) already has to tolerate a mid-flight
  **process crash** — `ProposeResult::Accepted` means "appended to the
  local Raft log", never "committed", and every proposer already
  distinguishes never-accepted from accepted-unconfirmed (see the root
  `CLAUDE.md`'s durable-before-visible entry) — so an entry this node
  already proposed keeps living in the Raft log regardless of whether
  anything is still awaiting `poll_probe`'s confirm loop; dropping that
  await is equivalent to the process crashing right after the propose
  call, a case the whole design already has to handle. The one lock these
  modules hold across an await, `ctx.data().rmw_lock`
  (`kind_write_item_at_leader`'s/`txn_stage_local`'s read+evaluate scope,
  narrowed off the propose+confirm path since issue #285), is a
  `tokio::sync::Mutex` — its guard releases on drop exactly like an
  ordinary panic unwind, never poisoning, so a cancelled holder leaves the
  lock exactly as available as a crashed one would. `watch_metadata`'s own
  long-poll is a pure read with no lock at all, so cancelling it is
  strictly safe too — and is, incidentally, a genuine improvement for that
  path's own stale-reply gotcha below, whenever the *client's* own
  connection (not the whole server process) is what goes away. No admin/
  dashboard/DynamoDB-wire handler is reachable through this code path —
  those are separate HTTP listeners, out of scope for this mechanism.
  Regression: `client_cancellation_tests` (in-crate, grep
  `client_requests_abandoned`) — one test drives a request that blocks
  server-side for the write path's full confirm budget, closes the client
  socket shortly after sending, and asserts the metric increments well
  inside that budget; a second sends two cheap requests back to back on
  one connection with no read in between (a pipelined client) and asserts
  both responses come back in order and the metric stays at zero — the
  regression for `peer_closed`'s own peek-not-read distinction above.
- **`ClusterEdgeState` is scoped to one NODE** (ADR 0031 PR2), created fresh per
  node — even in `--cluster N`, which previously shared one instance across the
  cluster and masked cross-process bugs. Holds this node's own control handle, its
  hosted CP group handles (keyed by tablet), and the DynamoDB `SchemaRegistry`.
  No process-global (`OnceLock`) mutable state.
- **`ClientCtx.data: Option<DataRole>`** groups the data-role-only fields
  (`raftkv_metrics`, `base_id` — `rmw_lock` was deleted, ADR 0054 step
  4b). `ClientCtx::data()` **panics** if
  absent — safe only from paths that structurally can't run on a control-only node
  (the dynamo edge, `auto_split_loop`). `resolve_cp_route` must never panic — it
  matches `self.data.as_ref()` directly (control-only node ⇒ zero local replicas).
- **`--cluster N` without `--dir` reuses ONE fixed path** (`$TMPDIR/animusd`), and
  `--ephemeral` does NOT make the control/raftkv WALs ephemeral (it only selects
  the CP-data `StorageBackend`). Two concurrent `--cluster N` runs contend on the
  same on-disk WALs — always pass a fresh explicit `--dir` for a throwaway run.
- **The cluster's members are node ids** (ADR 0040 unified the control and
  raftkv id spaces into one) — `bootstrap` (leader-only, idempotent)
  registers each data-role node's own id as `Active`. Failure detection
  runs over `ProdEnv`: each node's `heartbeat_loop_live` heartbeats the
  control group *as its own member id*, so the control leader's
  `detect_loop` marks a crashed node `Down`. **`heartbeat_loop_live`'s
  destination list is live** — it re-derives the control-group target list
  from `ctx.control.config()` every tick rather than a bring-up-time
  snapshot (a `ControlHandle::Remote` data-only node falls back to a static
  list until its first live reply lands); `peer_sync_loop` (`lib.rs`) must
  independently keep merging `Metadata.node_addrs[*].internal` into the
  node's own peer book, since a live destination list alone is still inert
  if `ProdEnv::send` has no address to send to — see the engineering-lessons
  "two staleness axes" entry (a live-destination-list audit must also check
  the transport address book).
- **Online growth (ADR 0030) is data-plane only** — the control group stays
  static; a grown node's control role is a permanent non-voter and mirrors
  `Metadata` via `remote_metadata_sync_loop` into `effective_metadata()` —
  long-polling `ClientRequest::WatchMetadata` (see the `ControlHandle`
  section above), not a fixed-poll. A replicated node address book
  (`Metadata.node_addrs` + `route_sync_loop`) keeps `client_route`/
  `/admin/peers` live so forwarding reaches nodes grown in later.
- **A node's deployment role rides that same replicated address book**
  (`NodeAddrs.role: String`, `#[serde(default = "combined")]` for WAL
  back-compat) — each of `BoundNode::start_with`/`BoundControlNode::
  start_control_with`/`BoundDataNode::start_data_with` stamps its own
  literal role (`"combined"`/`"control"`/`"data"`) at its `NodeAddrs`
  construction site, so `/admin/peers` can report every OTHER node's role
  straight from `Metadata.node_addrs` instead of the dashboard fanning out
  to each node's own `/admin/config`.
- **Decommission (ADR 0032)** = `drain` + `MetaCommand::RemoveMember`; check
  leadership *before* any metadata-dependent refusal (a follower's replica
  lags). Not a fence — a restarted process at the same id rejoins like a
  fresh join. `admin_remove_member`'s control-voter refusal reads
  `self.control.config()` (the live Raft config, not a static snapshot) —
  a node that is still a *live* control voter is refused, pointing the
  operator at `animus admin decommission --force-control-remove`, which
  checks `GET /admin/control/members` up front and, if the target is a
  live voter, runs `control-remove` + polls to convergence *before* the
  ordinary drain → drain-status → remove flow even starts. Regression:
  `tests/decommission.rs::
  decommission_refuses_live_control_voter_then_succeeds_after_control_remove`.
- **Self-minted member ids (ADR 0040) replace ADR 0036's monotonic
  allocator entirely.** A joining node self-mints (`NodeId::mint`, off
  `animus_env::prod::PreBindRng` at the pre-bind CLI boundary) or proposes
  an explicit `--id`, then claims it via `MetaCommand::RegisterNode`'s
  registration CAS **before ever binding a listener**: a minted collision
  re-mints and retries; a proposed-id collision fails loudly
  (`AlreadyExists`). `is_relayable_command` must allow `RegisterNode` — a
  joining process has no local control role yet, so relaying it is its
  *only* way to reach the real leader. It **never claims a `members` row
  for a control-only registration** (`NodeAddrs.role == "control"`) — a
  control-only node can never host a tablet, so appearing in `members`
  would make it a placement candidate and silently corrupt tablet
  placement the moment it's picked (caught by `tests/control_only.rs`
  going bimodal — see `docs/engineering-lessons.md`).
- **Orphan-member auto-reclaim sweep (ADR 0040)**: the mechanism itself
  lives entirely in `animus-control` — see that crate's `CLAUDE.md`. This
  crate's whole contribution is plumbing the `orphan_sweep_after: Duration`
  knob from a config/CLI flag (`--orphan-sweep-after SECS`) down to
  `RaftNode::start_with_orphan_sweep_after` — `Duration::ZERO` disables the
  sweep outright; every existing entry point keeps its exact signature,
  defaulting internally to `animus_control::node::DEFAULT_ORPHAN_SWEEP_
  AFTER` (10 minutes). Only meaningful on a mode that runs a local control
  `RaftNode` (every mode except `data`). `/admin/raft`'s per-member view
  carries a `has_activated` field alongside `believes_alive`; the Overview
  dashboard appends "(never activated)" for a `Down` member with
  `has_activated: false`.
- **Control-plane membership change (ADR 0037)**: `ClientCtx::
  admin_add_control_member`/`admin_remove_control_member` (`lib.rs`) grow/
  shrink the control group's *live* `RaftCore` config at runtime —
  local-control-leader-only, **not** relayed, **not** in
  `is_relayable_command` (the underlying primitive is `RaftNode::
  change_membership`, not a `MetaCommand` proposal, so only a genuine
  control-group voter's own in-process handle can call it). `POST
  /admin/control/member/{add,remove}` + `GET /admin/control/members`;
  `animus admin control-{add,remove,grow}`. This crate's own contribution
  on top of the `animus-control` primitive: **Remove** has a genuine
  survivor-liveness guard living here, not in the core — `admin_remove_
  control_member` refuses if the *resulting* live voter count would fall
  below a majority (via `RaftNode::control_peer_believed_alive`), pointing
  at a `force: bool` parameter (`--force`), deliberately independent of
  `decommission --force-control-remove` (which only means "run
  `control-remove` as part of decommission," never "skip its safety
  checks"). See ADR 0037 (and ADR 0040's amendment on it) for the full
  design, and `docs/engineering-lessons.md` for the id-space-mismatch and
  self-registration/admin-action-clobber war stories. **`admin_add_control_
  member`'s "already registered?" gate must check `Metadata::node_addrs`,
  never `members` alone, and must bound-wait for this leader's own
  `engine_applied_index() >= commit_index()` before reading either
  (issues #406/#450)**: a control-only registration never claims `members`
  by design (the bullet above), so gating on `members` alone made the
  "genuinely unclaimed" branch run on *every* call for this node shape,
  re-deriving a `NodeAddrs` from a `metadata_cached()` snapshot that can be
  lagging this leader's own already-committed Raft log (ADR 0038's async
  apply task) — a permanent CAS collision, or worse, a durably blank
  address book if the malformed guess won the race. See
  `docs/engineering-lessons.md`'s matching entry for the full account,
  including why the `node_addrs` gate fix alone (without the wait) still
  measurably collides. **Self-removal's
  leadership-transfer arm is one-shot, not auto-retried (issue #405)**:
  `admin_remove_control_member`'s `node == my_id` branch calls
  `RaftCore::transfer_leadership` exactly once — if the target's
  `peer_match` hasn't caught up to `commit_index()` at that precise
  instant (plausible right after a runtime `control/member/add`, since
  ordinary background churn — a liveness `UpsertMember`, a placement
  reconcile — can keep advancing `commit_index`, worse under load), the
  call returns a `409` indistinguishable, by status code alone, from
  "armed, but the target took the full internal poll to step up." Every
  refusal this action can return says "retry" for exactly this reason —
  a caller (a human operator, or a test standing in for one) must retry
  the **whole call**, not just wait on the side effect; see
  `crates/animus-control/CLAUDE.md`'s "Leadership transfer" entry and
  `docs/engineering-lessons.md`'s issue #405 entry for the full mechanism
  and `tests/heartbeat_live_destinations.rs`'s fix.
- **`POST /admin/control/transfer {"to": <node id>}` (ADR 0020/0037,
  roadmap U-05, 2026-09-05; contract fixed 2026-09-07, issue #688)** — a
  standalone leadership-transfer route, beside
  `admin_remove_control_member`'s own internal self-removal transfer arm
  above: `ClientCtx::admin_transfer_control_leadership` lets an operator
  move control-plane leadership without also removing a voter. Same
  local-control-leader-only, not-relayed discipline as every other
  `control/member/*` action; idempotent if `to` already leads, refused if
  `to` isn't a current voter, otherwise arms `RaftCore::transfer_leadership`
  and polls (bounded by the same `CONTROL_TRANSFER_POLL_TIMEOUT` the
  self-removal arm uses). **`200` means `to` is genuinely the observed
  leader, not merely "this node stepped down" (issue #688, a second
  failure mode #671 left standing)**: while a transfer is armed the old
  leader keeps heartbeating every peer and steps down on **any**
  higher-term vote, not only the target's — under real scheduling jitter a
  *third* voter's own election timer can lapse on the same late heartbeats
  and win the election before or instead of the named target, so
  "stepped down" alone is not proof the transfer completed. The poll now
  reads this node's own live `RaftCore::leader()` belief (which keeps
  updating after step-down) until it names `to` specifically: `200` only
  once it does; a **new, distinct `409`** naming the actual stable leader
  if this node stepped down but a *different* voter is now leading (the
  caller must retry the whole `POST` against that node's own admin port —
  this node's `RaftCore` can arm nothing once it isn't the leader); the
  original arm/timeout `409` ("did not complete within Ns; retry") if the
  poll runs out with no leader observed at all. `animus admin
  control-transfer <admin-addr> <node-id>` is the CLI form — it prints the
  server's JSON verbatim, so the new refusal text surfaces there
  unchanged, no CLI-side change needed. Regression: `tests/
  admin_endpoint.rs::admin_control_transfer_moves_leadership_to_the_named_node`/
  `admin_control_transfer_on_a_follower_is_refused` — both now retry the
  whole `POST` (re-resolving the current leader) on the new 409, per the
  fixed contract, rather than asserting on a single accepted attempt. See
  `docs/engineering-lessons.md`'s issue #688 entry for the general lesson
  (a "stepped down" signal is not "target elected"; a handoff route's
  success criterion must be the positive end state).
- **The CP group is durable by default** — and since ADR 0050 Train B rung
  1, **each hosted tablet gets its OWN private `LsmEngine`** (filename
  prefix `tablet_lsm_prefix(t)` = `db-t{t}-`; the trailing `-` keeps
  `db-t5-*` from prefix-matching `db-t51-*`), opened/probed/destroyed by
  the reconciler through `host::EngineFactory` (`LsmTabletFactory` here).
  The node's own `LSM_PREFIX = "db-"` engine now backs **only** the
  control plane's system keyspace (ADR 0038). Files use flat filename
  prefixes, not subdirectories (`ProdEnv`'s disk doesn't create
  intermediate dirs). Idle per-tablet engines cost ~1 KB RSS each and
  spawn nothing (`animus-storage/tests/idle_engine_cost.rs` — the ADR
  0050 gating measurement). Node-start entry points are async+fallible
  (`io::Result`).
- **`Node::shutdown()` is a graceful teardown** — aborts the listener tasks and
  `ProdEnv::shutdown()`s the node's one internal env, freeing all six ports
  on a combined/data-only node (ADR 0040 PR1's `internal`/`client`/`dynamo`/
  `admin` stride, plus ADR 0047's `intra` and ADR 0052's `console` — the
  pre-ADR-0040 stride was six too, but split across two role envs instead of
  one node/one port-block; a control-only node frees four, since it never
  binds `dynamo`/`console`) so a replacement can rebind
  the same addresses/dir. Dropping a `Node` without it leaves tasks running.
  **It's fire-and-forget (`abort()` then return), not a guarantee those ports are
  free the instant it returns** — see `animus-env/CLAUDE.md`'s `ProdEnv::shutdown()`
  entry. A same-address restart needs **`Node::shutdown_and_wait()`** (aborts, then
  waits for every task to actually finish) or, more commonly, just
  `shutdown_graceful()` — which now ends in `shutdown_and_wait` rather than the
  plain `shutdown` — so every existing restart test got this fix for free without
  a test-file change. This was the actual root cause of the
  `full_split_cluster_restart_recovers_metadata_and_data` flake under `cargo test
  --workspace`; see `docs/engineering-lessons.md`'s "abort() is a request, not a
  guarantee" entry.
- **Every path that abruptly stops a node's driver tasks — bare
  `shutdown()`/`shutdown_and_wait()`, and dropping a `Node` that was never
  explicitly shut down — first latches every hosted CP group's `halted`
  flag via `ClusterEdgeState::halt_hosted_cp_groups`** (issues #282/#279):
  `animus-cp-data`'s WAL/apply I/O tolerance (`persist_wal`/`flush_pending`)
  hard-panics on a live I/O error and only tolerates one while a group's
  `halted: AtomicBool` is set, and that flag used to latch **only** on the
  graceful path (`shutdown_graceful` → `shutdown_all_cp_groups`) — a bare
  kill (the doc-blessed fault-injection idiom above) or a panicking test's
  `Vec<Node>` unwind (`Node` had no `Drop` impl at all) could abort a
  driver mid-I/O with `halted` still `false`, turning a routine kill/panic
  race into an unconditional panic indistinguishable from a genuine live
  durability fault. `halt_hosted_cp_groups` is cheap and safe to call from
  anywhere, including `Drop` (it bottoms out in `RaftKvNode::shutdown`, a
  plain `AtomicBool` store plus two `Notify` wakes — no I/O, no `.await`,
  no runtime dependency). **`Drop for Node` latches and nothing else** —
  it deliberately does not abort tasks or tear down envs, so the "dropping
  a `Node` without `shutdown()` leaves tasks running" behavior two
  paragraphs up is unchanged; only the durability assert those still-live
  tasks can now safely race against an eventual abrupt stop is fixed.
  Regression: `halted_shutdown_tests` (in-crate, `cargo test -p animusd
  --lib`) and `animus-cp-data`'s
  `tests/shutdown.rs::a_halted_followers_incoming_write_tolerates_a_wal_fault_with_no_panic`.
- **A merged-across-nodes admin view must carry each item's own identity** —
  `/admin/raftkv`'s `CpRaftView::node` carries the real hosting node id because the
  dashboard merges every node's response; the answering server isn't a reliable
  attribution once combined.
- **`/admin/raftkv` is POLLED, so its default must not materialize
  (2026-08-19).** The Console fetches it from every node on its auto-refresh
  interval (5s default), so `key_count`/`byte_size` are the cheap
  `CpGroup::approx_key_count`/`approx_bytes` estimates — the very counters
  `auto_split_loop` gates on, so the Tablets view's over-threshold pills
  agree with the trigger that will fire. `?exact=1` selects the old
  materializing `local_pairs` path for one deliberate look. It used to
  materialize unconditionally: on a 20,000-row table mid-split, polling the
  route every 3s inflated the split's own build ~9x (41.8s vs 4.5s) — an
  observer that perturbs what it observes. `key_count` is `None` on the
  memory backend (no cheap counter) and `approx_bytes` is base-scoped (ADR
  0034) where the exact sum spans every kind — both documented on
  `CpRaftView`'s fields. Regression:
  `tests/admin_endpoint.rs::admin_raftkv_default_does_not_materialize_the_dataset`,
  metering `storage_sstable_block_reads` rather than wall clock. Any new
  O(dataset) admin read needs the same question asked of it (ADR 0020's
  2026-08-19 amendment).
- **`CpRaftView.voter_history` (issue #596)** — every distinct voter
  configuration this replica's group has adopted, in adoption order, each
  entry a sorted `Vec<String>` of node ids (`CpGroup::voter_history()`
  forwarding to `RaftKvNode::voter_history()`, see `animus-cp-data/
  CLAUDE.md`'s matching entry for the recording mechanism). A pure
  diagnostic like `quiesced`/`learners` above — building this view never
  wakes a quiesced group, and it costs nothing extra to poll since the
  underlying ring is already maintained per consensus-loop iteration
  regardless of who reads it. Exists so a caller doesn't have to reconstruct
  a transient intermediate configuration from an external poll that can
  race a fast reconciler shut — see `split_placing_two_replica_diff_e2e.rs`'s
  own entry below for the incident this closes.
- **CP writes need no client-assigned version — but the MVCC version is a
  packed HLC commit timestamp, NOT the Raft log index (stale text corrected
  2026-08-19).** ADR 0018 §2/PR2 (2026-08-11) retired the interim
  `version_floor`-scaled Raft-index scheme; every mutating `KvCommand`
  carries a leader-minted `ts: HlcTimestamp`, and the engine version at apply
  is `hlc::pack(ts) = (wall_ms << 20) | logical` (`animus-cp-data`'s
  `hlc.rs`, `KvCommand`'s own doc comment). Per-key LWW still reproduces the
  agreed order — commit order and HLC order coincide within one group
  (`assert_ts_monotonic`) — but **a group's `engine_applied_index()` (a Raft
  log index: single/low-thousands under any real workload) is not
  comparable to a row's packed-HLC version (wall-clock milliseconds shifted
  left 20 bits: astronomically larger) — never substitute one for the
  other as a version floor/ceiling.** Caught investigating the split
  driver's `bulk_version_floor` pre-pass (`index_drain.rs`): the tempting
  "skip the version-floor scan, read `engine_applied_index()` instead"
  optimization is unsound both ways — using a log index as the floor
  systematically under-filters (every real row's HLC version dwarfs any
  plausible index, so the final image would degenerate back into the
  unfiltered whole-table re-ship rung 8 fixed) — a known regression bought
  for no saved scan. The two are simply different value spaces and neither
  bounds the other, which is the rule to remember; note this driver still
  runs only over `ProdEnv` in production (nothing in the split-build path
  is reached by the `SimEnv` `ClientCtx` harness below — see that
  section), so simulated-clock reasoning does not apply to it either way.
  No code changed; the pre-pass scan stays.
- Several gotchas here are instances of cross-cutting lessons — port-TOCTOU
  bring-up retries (`support::restart_same_addrs`), "a flaky `ProdEnv` test is a
  real bug", restart-test discipline (poll for catch-up, not leadership),
  converged-or-timeout polls for eventual properties, retry loops distinguishing
  never-accepted from accepted-unconfirmed. See the **engineering-lessons log
  (root `CLAUDE.md`)** for the general form of each.

## SimEnv `ClientCtx` harness (ADR 0061 Phase C's closing rung)

`lib.rs`'s `simenv_client_ctx_tests` (an in-crate `#[cfg(test)] mod`, run via
`cargo test -p animusd --lib`) is this crate's **first** `SimEnv`-driven
test: it constructs a real `ClientCtx<SimEnv, _>` — the production struct
`ClientCtx<E: Env = ProdEnv, R: RelayClient = AnimusdRelayClient>`
instantiated at `E = SimEnv` — and drives a genuine write + read through its
own `cp_kind_write_raw`/`cp_get` methods, deterministically and
seed-reproducibly, with no sockets and no `ProdEnv` anywhere in the run.
This is what makes the seventh 2026-08-28 ADR 0061 amendment's claim ("a
`ClientCtx<SimEnv>` can be constructed and driven in `animusd`'s own tests")
true rather than an unverified assertion — see that amendment, and its
eighth-amendment follow-up, for the full account of what this rung found.

**What it constructs.** One `Simulator`, a one-voter control `RaftNode<
SimEnv>` (node 0), a one-voter CP data-plane `RaftKvNode<SimEnv,
MemoryEngine>` (node 1, tablet 1, a whole-ring `StorageScope`) registered
into a real `ClusterEdgeState<SimEnv>`, and a real `ClientCtx<SimEnv,
NeverRelay>` struct-literal built from those handles plus placeholder
`AdminInfo`/routing-table/metrics fields. `NeverRelay` is a zero-sized
`RelayClient` implementor whose `relay` always returns
`ClientResponse::Error(..)` — sound because this fixture is single-node and
its one tablet is always led locally, so nothing in the paths this harness
drives ever needs to relay. `data: None` (no `DataRole`) is deliberate, not
a shortcut — see "What it cannot drive" below.

**Why an in-crate `#[cfg(test)] mod`, not `tests/*.rs`.** `ClientCtx`'s own
fields, `ClusterEdgeState::register_raftkv`, `CpGroup`, and `AdminInfo` are
all private to this crate. Rust's privacy rule ("visible in the defining
module and its descendants") lets a child module of `lib.rs` construct every
one of them exactly as they already are; an external `tests/` file could
only reach them by widening several types' visibility for no reason beyond
"an external file wants to construct them once." **This rung widened no
visibility at all** — the same precedent `confirm_futility_tests`/
`kind_batch_signal_tests` already set in this file.

**What it can drive.** `cp_kind_write_raw` and `cp_get` — the *exact*
methods `handle_request`'s `ClientRequest::Put`/`Get` arms call in
production. The harness calls them directly rather than through
`handle_request` itself, because `handle_request` (and
`dynamo::marker_batch_write_raw`, the thin wrapper the real `Put` handler
goes through) are both hardcoded to `&ClientCtx` = `ClientCtx<ProdEnv,
AnimusdRelayClient>` and so cannot be called with a `SimEnv` context at all
— they simply never needed a second type parameter before this rung, and
genericizing them is not needed to prove this rung's claim (see "Not yet
generic" below). Driving `cp_kind_write_raw` exercises the real
route → propose → confirm loop, including the exponential confirm-poll
backoff (`CP_CONFIRM_POLL_INIT`/`_MAX`); `cp_get` exercises the real
route → local-resolve loop. Both are spawned onto `ctx.env` and driven with
`Simulator::run_for` (never `block_on` — see the gotcha below), following
the corpus's converged-or-timeout idiom rather than a fixed-deadline assert.

**What it cannot drive, and why (read before extending it) — two separate,
precisely located blockers, neither introduced by this rung:**

- **Schema DDL (`ClientCtx::propose_schema`, and therefore
  `provision_tablet`, `trigger_split`, `drop_table*` — every DDL path).**
  `propose_schema`'s local-propose fast path reads `ClusterEdgeState::
  control: Arc<Mutex<Vec<RaftNode<ProdEnv>>>>` — a field that is
  concretely, permanently `ProdEnv`-typed regardless of the enclosing
  `ClientCtx<E, R>`'s own `E` (see that field's own doc: `ControlHandle`
  in `animus-node::control_handle` deliberately carries no `propose`
  method at all, "because proposing is inherently a local-Raft-log
  operation," and every proposal instead goes through this concrete
  handle). This is a **pre-existing, deliberate design choice** the ADR's
  own C3c rung already documented, not a gap this rung introduced or could
  route around without inventing a capability trait purely to make DDL
  sim-drivable — exactly the "contorted trait" failure mode the second and
  fourth 2026-08-28 amendments warn against. The harness's fixture
  (`seed_schema`) works around it the honest way: it proposes
  `CreateTableSchema`/`CreateTablet` directly on the control `RaftNode`,
  bypassing `ClientCtx` entirely for setup — the identical thing
  `animus-node/tests/index_backfill_sim.rs` already does for the same
  reason.
- **`ClientCtx::segment_store`/`backup_store`'s `SegmentStoreHandle`/
  `BackupStoreHandle`.** Both hardcode `FsSegmentStore`/
  `ClusterSegmentStore<ProdEnv, FsSegmentStore>` regardless of the
  enclosing `ClientCtx<E, R>`'s `E` (`animus-env`'s `prod` feature is
  unconditionally on for this crate, so `FsSegmentStore` genuinely exists
  here — the blocker isn't C0's feature gate, it's that neither handle
  type takes an `E` parameter at all). **W-10 moved these two fields off
  `DataRole` onto `ClientCtx` itself** (provisioned on every node shape,
  including control-only — see `ClientCtx::segment_store`'s own doc), so
  unlike every other `DataRole` field this harness's fixture can no longer
  dodge them via `data: None` — every `ClientCtx` construction, generic
  `E` included, now needs a real value for both. The fixture (below) works
  around this the same way it already dodges the schema-DDL blocker above:
  `SegmentStoreHandle::Fs`/`BackupStoreHandle::Fs` are the one variant of
  each enum that carries no `E`-typed (or any `ProdEnv`-typed) state at
  all — `FsSegmentStore::new` doesn't touch the filesystem or any `Env`
  until `put`/`get`/`delete` is actually called — so a placeholder `Fs`
  value satisfies the field without needing `ProdEnv` or touching disk;
  `single_node_ctx`'s own doc has the detail. Only the **`Cluster`**
  variant is the genuine blocker: neither `cp_kind_write_raw` nor `cp_get`
  ever reads `ctx.segment_store`/`ctx.backup_store` at all (verified by
  reading both call chains, not assumed), so this rung never needed a real
  `Cluster`-backed handle to prove its claim. Any future extension that
  needs a genuinely working (not placeholder) store handle under `SimEnv`
  (the segment janitor, the backup/PITR capture-and-reclaim loops) hits
  this blocker first — it would need a `SimSegmentStore`-shaped
  `SegmentStoreHandle`/`BackupStoreHandle` variant, which doesn't exist in
  this crate (ADR 0043 §A7b's `SimSegmentStore` is `animus-cp-data`'s own
  sim-corpus concern, never reached from here).

**Not yet generic, also worth knowing before extending this harness**:
`handle_request` and `dynamo::marker_batch_write_raw`/
`kind_write_item_at_leader`'s *callers* in `dynamo.rs` stay hardcoded to
`&ClientCtx` (`E = ProdEnv`) — only the five split modules
(`schema`/`read_path`/`write_path`/`txn_coordinator`/`forwarding`) and a
handful of `lib.rs`-resident crate-wide accessors
(`effective_metadata`/`data`/…) are `E`-generic today (`data_opt` no
longer exists, W-10 — see this file's own entry above). Driving
the DynamoDB wire-shaped write path (`cp_kind_write_item`, which *is*
already `E`-generic in `write_path.rs`) under `SimEnv` is reachable in a
follow-on rung once a `SimEnv`-safe `DataRole`/store-handle pair exists; driving
`handle_request`/`dynamo.rs`'s handlers themselves is a larger, separate
genericization this rung did not attempt.

**Gotchas hit standing this up:**

- **`futures::executor::block_on` does not work over a `SimEnv`-driven
  future — it hangs.** `cp_kind_write_raw`/`cp_get` both potentially
  `.await` an `env.sleep()` (the confirm-poll backoff, the route-wait
  retry loop); nothing advances `SimEnv`'s virtual clock or fires its
  timers except `Simulator::run_for`/`run_until` stepping the simulator's
  own cooperative executor. The correct shape — the same one `animus-
  cp-data/CLAUDE.md`'s own "Linearizable reads are async... drive them as
  spawned tasks + `run_for`" rule already names — is `env.spawn_task(fut)`
  capturing the result into a shared `Arc<Mutex<Option<T>>>` slot, then
  `sim.run_for(bound)`, then read the slot back out
  (`spawn_and_capture`, this module's own shared helper).
- **A method call's receiver borrow and a later argument's move of the
  same struct conflict.** `ctx.env.spawn_task(async move { ...
  ctx.cp_kind_write_raw(..) })` does not compile: the receiver expression
  `ctx.env` borrows `ctx` for the call, and the `async move` block later
  in the same expression tries to move the whole `ctx` — clone `ctx.env`
  into its own local binding *before* constructing the future that moves
  `ctx`, mirroring `animus-node/tests/index_backfill_sim.rs`'s own
  `let loop_env = node.env().clone(); loop_env.spawn_task(..)` idiom.
- **`clippy::unusual_byte_groupings` fires on a hand-picked hex seed
  literal** (`0x51_4E_0001`, meant to spell "SimEnv" loosely in hex,
  grouped in 2-digit chunks) — clippy wants hex digits grouped in **fours**
  from the right (`0x514E_0001`). Caught by the full `-D warnings` gate,
  not by `cargo test` (which doesn't run clippy) — a reminder that a green
  `cargo test` does not imply a green `cargo clippy` for a freshly written
  sim test.
- **`cargo clippy -p animusd --all-targets --all-features` does not pay
  this crate's disk cost the way `cargo build`/`cargo test` with the same
  flags does.** Clippy checks every target (including the ~100 files in
  `tests/`) without linking full binaries, so it stayed under a few GB of
  target-directory growth for this whole crate — safe to run as the actual
  gate 2 command from the disk-discipline section above, unlike `cargo
  build -p animusd --all-targets`, which is exactly what fills the disk.

### C3d landed: `two_node_relay_tests`, a real two-node relay smoke (ADR 0061)

The blocker this section's own "What it cannot drive" list did **not**
name — because it wasn't a gap in this harness, it was simply the next
rung — is closed: `ClientCtx<E, R>` now carries a `relay: R` field, and
`forward_to_tablet_leader`/`ClientCtx::relay`/`read_path.rs`'s
`relay_stale_read`/`schema.rs`'s `propose_schema` broadcast fallback all
call `self.relay.relay(..)` instead of the free `relay_request`/
`relay_request_with_timeout` functions directly. `animus_node::
sim_relay::SimRelayClient<E: Env>` is the sim-only, `Network`-backed
implementor this buys — see that module's own doc (`animus-node/
CLAUDE.md`'s own entry, and ADR 0061's 2026-09-04 amendment) for the
stream allocation, the `addr == NodeId::to_string()` convention, and the
`req_id`-correlated wire shape.

`lib.rs`'s `two_node_relay_tests` (a sibling `#[cfg(test)] mod` to
`simenv_client_ctx_tests`, same in-crate-for-private-handles reason) is
the proof this actually works end to end: two `ClientCtx<SimEnv,
SimRelayClient<SimEnv>>`s on two distinct `SimEnv` node ids, sharing one
control `RaftNode<SimEnv>` (`Arc`-backed clones, so both see the identical
replicated `Metadata` with no propagation delay) but each with its own
`ClusterEdgeState` — node A hosts the sole tablet replica, node B hosts
none. Node B's `cp_kind_write_raw`/`cp_get` calls resolve `CpRoute::
Forward` and carry the request to node A over a real `SimRelayClient`
wire; node A's own relay server (`relay_a.serve(..)`, closed over a clone
of `ctx_a`) answers through `forwarding::handle_relayed_request` — the
same `E`/`R`-generic dispatcher production's own `handle_request`
delegates its `Status`/`Forwarded`/`ProposeSchema` arms to. A third,
direct `cp_get` on `ctx_a` confirms the write landed on node A's own
engine, not merely echoed back through the relay's own bookkeeping.

**What this still does not drive, unchanged from the blocker above**:
`ClientCtx::propose_schema`'s *local-propose fast path* is still
`ProdEnv`-locked (`ClusterEdgeState::control`'s own concrete typing) —
`two_node_relay_tests` seeds schema the same way `simenv_client_ctx_tests`
does, proposing directly on the shared control `RaftNode`. What changed is
that `propose_schema`'s *relay* branches are now genuinely reachable
(under `SimEnv` they're the only branches that can ever fire, since the
fast path's own field structurally can't hold a `SimEnv` handle) — this
rung's own smoke doesn't happen to call `propose_schema` at all, so it
doesn't exercise that specific path, but nothing about the relay seam
itself blocks a follow-on test that does.

### `SimCluster`: the multi-node generalization (ADR 0061 rung D1)

`sim_cluster.rs`'s `SimCluster` (declared `#[cfg(test)] mod sim_cluster;`
from `lib.rs`, kept in its own file — unlike `simenv_client_ctx_tests`/
`two_node_relay_tests` above — purely so `lib.rs` doesn't keep growing;
same descendant-of-the-crate-root privacy property, so `ClientCtx`'s
private fields are still reachable with no visibility widened) is what
those two harnesses generalize to: an N-node cluster with a real
**multi-voter** control `RaftNode<SimEnv>` quorum (every node id is a
voter, `animus-control/tests/control_raft.rs::cluster`'s own shape — not
`two_node_relay_tests`' single-voter/shared-`Arc` stand-in), a
`SimRelayClient<SimEnv>` per node with rung C3d's generic relayed-request
dispatcher installed, and a `ClientCtx<SimEnv, SimRelayClient<SimEnv>>`
per node whose `client_route`/`intra_route` name every node id up front
(the whole node set is known at construction, so no
`route_sync_loop`/`intra_route_sync_loop` equivalent is needed).

**Public surface**: `SimCluster::new(seed, nodes, replication)`;
`create_table(table)` (this cluster's own default replication factor) /
`create_table_with_replication(table, replication)`; `tablet_of(table)`;
`put`/`get`/`delete`/`scan` (each takes a plain `u64` node index — see
below — and issues the op from that node's own `ClientCtx`, returning
`Result` rather than panicking, since several scenarios expect a failure);
the fault surface `crash`/`restart`/`partition`/`heal_all`/`run_for`
(mirroring `raftkv_linearizable.rs`'s own `Nemesis::apply` shape); and
`leader_of(tablet) -> Option<NodeId>`/`leader_index_of(tablet) ->
Option<u64>`/`metadata(node)`/`node_count()`/`seed()` for assertions.

**A node is addressed by a plain `u64` index (`0..nodes`), never a
`NodeId` directly** — `nid(n)` encodes as the literal string `"n{n}"`
(`animus-env::nid`'s own implementation), so round-tripping a `NodeId`
back through `.to_string().parse::<u64>()` to recover a usable index
fails outright (found live building this rung's own tests — every
`leader_of(..).to_string().parse()` call site had to become
`leader_index_of(..)` instead, a plain accessor that never goes through
`NodeId`'s own `Display` at all). `leader_of` still hands back a real
`NodeId` (matching the ADR's own suggested signature, and useful for a
caller comparing against `Metadata`), but every other method on this
fixture takes and expects the plain index.

**Design decisions** (see the ADR's own 2026-09-05 amendment for the full
account, including what a follow-on corpus rung still needs):

- **Tablets are hosted by a real per-node `animus_cp_data::host::
  Reconciler` (ADR 0061 rung D4 PR 1, closing issue #715).** `SimCluster::
  new` builds one `Reconciler<SimEnv, MemoryEngine>` per node (its own
  `MemoryTabletEngines` engine registry, `on_host`/`on_teardown` hooks
  mirroring hosting changes into that node's `ClusterEdgeState` — the
  identical read-only-mirror discipline `animusd`'s own production node
  assembly uses) and spawns a driving task (`spawn_reconciler_loop`) that
  ticks it on `metadata_watch()` wakes plus a fixed fallback, mirroring
  `animusd::tablet_host_reconciler_loop`'s own event-driven-with-fallback
  shape. **This replaced a D1-era hand-hosting design** — `create_table`
  used to build a `RaftKvNode<SimEnv, MemoryEngine>` directly on each
  chosen replica node and register it by hand, and a D3 PR 2a-era stand-in
  watcher (`spawn_policy_tablet_host_loop`) hosted a wire-provisioned
  tablet but never tore one down (the "reconciler hazard," below) — both
  are gone; the real reconciler is now this fixture's ONLY hosting path,
  for both hand-hosted and wire-provisioned tables alike.
- **DDL is still a control-plane-Raft bypass, for hand-hosted tables** —
  `create_table_with_replication` never calls `propose_schema`, proposing
  `CreateTableSchema`/`CreateTablet`/`MetaCommand::SetTabletPolicy`
  directly on the control leader's own `RaftNode` handle instead (the
  policy attachment is new since D4 PR 1 — the signal every node's real
  reconciler hosts a tablet off of; its own recorded RF is the caller's
  `replication` argument, not the wire path's fixed
  `MAX_REPLICATION_FACTOR`, preserving the "exactly N replicas on nodes
  `0..N`" contract). **`ClientCtx::propose_schema`'s local-propose fast
  path was `ProdEnv`-locked through rung C3d (which only made its *relay*
  branches reachable) — this changed in ADR 0061 rung D3 PR 2a**:
  `ClusterEdgeState<E>::control` widened from a fixed
  `Vec<RaftNode<ProdEnv>>` to `Vec<RaftNode<E>>`, and `SimCluster::new`
  now registers every node's own control handle onto its own edge, so
  `propose_schema`'s real leader-local fast path (and its non-leader relay
  branch, now genuinely exercised rather than a leader relaying to itself)
  both work under `SimEnv` — reached by `dynamo::create_table`/
  `delete_table` via the new `dynamo::dispatch_table_op`, not by this
  method directly. See that rung's own `CLAUDE.md`/ADR entries, below and
  in `docs/adr/0061-*.md`, for the full account.
- **`restart` is a true process restart on `MemoryEngine`, no longer a
  wipe** — `Simulator::stop` then a fresh `RaftNode::start` and a fresh
  `Reconciler` on the same node id. **Since ADR 0061 rung D4 PR 1 the
  fresh reconciler reuses the SAME `MemoryTabletEngines` handle** this node
  was built with — mirroring `reconciler_corpus.rs::Cluster::
  crash_restart`'s own "a durable engine survives a process crash"
  modeling — so a restarted node's own tablet data is no longer wiped
  (a deliberate behavior change from the pre-D4 restart, which always
  built a brand-new `MemoryEngine::new()`). Recovery either way is via
  ordinary peer catch-up/chunked `InstallSnapshot`, never a local WAL
  replay (this tier has no durable engine to replay). `crash` (mute, tasks
  stay alive) is the separate, cheaper fault this restart is not a
  substitute for.
- **Always `start_hosted` with `stream = tablet.0`, never `start_scoped`**
  — a real `host::Reconciler` (see `animus-cp-data/CLAUDE.md`'s own host
  module entry) already follows this discipline in production, and this
  fixture's `create_table_with_replication` can host more than one table's
  tablet on overlapping node sets (scenario 5 does exactly this) —
  `start_scoped` pins every group to `PRIMARY_STREAM`, which would
  cross-talk two tablets sharing node ids (the exact bug `animus-test/
  CLAUDE.md`'s stream-corpus entry documents finding in its own harness).
- **Every op takes a `u64` node index and issues from that node's own
  `ClientCtx`**, so a non-hosting/non-leader node's op genuinely exercises
  `forward_to_tablet_leader`/`cp_serve_forwarded` over the real
  `SimRelayClient` wire, not merely a local call.

**Five scenarios ship with it** (`sim_cluster::tests`, seed-parameterized):
a leader write reading back consistent from every node including a
forwarding non-leader (plus a `scan`/`delete` pass); a write from a node
hosting no replica at all (RF < node count); crash-the-leader → write
through a survivor → restart → converge (a converged-or-timeout retry
loop, `poll_until_get_eq`, never a one-shot assert); a 1-node minority
partitioned off cannot ack, the majority side still succeeds, and the
minority catches up after `heal_all`; and a second `create_table` after
the first, with both tables independently writable/readable.

**Still `ProdEnv`-only**: `SegmentStoreHandle`/`BackupStoreHandle`'s
`Cluster` variant (this fixture only ever uses the `Fs` placeholder —
nothing it drives reads either field). **No longer `ProdEnv`-only, since
ADR 0061 rung D3 PR 2a**: `ClientCtx::propose_schema`'s local-propose fast
path (see the bullet above) and a real `DataRole` (`sim_cluster_dynamo.rs`,
D2 PR 1, gave every node a real one — `data: None` was this rung's own
original shape, not the current one).

**A GSI's own hidden table is materialized on demand via `SimCluster::
drain_gsi` (ADR 0061 rung D3 PR 3b) — closing the PR 3a gap this bullet
used to describe.** `create_table` proposes a declared index's
`CreateTableIndex` schema-catalog entry (since PR 3a, base table or not),
but the hidden `<base>$<index>` table's own tablet is still minted
lazily, only once something drains a row into it — in production, the
first tick of `index_drain::change_consumer_loop`'s GSI-drain arm;
`SimCluster` still never spawns that loop at all (this section's own
"hand-hosted, not reconciler-hosted" bullet). PR 3b's `drain_gsi(node,
table)` (`sim_cluster.rs`) is the fixture-side stand-in: for every tablet
`node` both hosts and leads whose `Metadata` row names `table`, it
recomputes `gsis` the identical way the production loop does and calls
`index_drain::drain_tablet` directly (widened to `<E: Env, R:
RelayClient>` for exactly this reuse — a pure signature change, no
behavior change, since every callee it and its private helper
`reconcile_partition` use was already generic), then drives the simulator
until the resulting `cp_kind_write_raw` calls commit. Two of the
production loop's own guards are replicated by hand (leader check,
hidden-table-name skip); `is_quiesced()`/`Building`-child skips are
**not** replicated — they are structurally unreachable here
(`SimCluster` never calls `enable_quiescence`, and never splits a table),
not merely untested, and a future rung that adds either capability to
this fixture would need to add the matching guard. Before a `drain_gsi`
call, `run_gsi_query`/`run_gsi_scan`'s own `!meta.has_table_tablet(&
index_table)` gate is still unconditionally true (not "the rows are
stale," "the table doesn't exist yet") — pinned by `sim_cluster_dynamo_
table_ops.rs::gsi_query_materializes_rows_after_a_drain` (renamed from
PR 3a's own `gsi_query_reads_empty_under_the_fixture_until_the_drain_
generalizes`, now a positive assertion: empty before the drain, populated
after). An LSI never needed any of this — its rows are written
synchronously in the same Raft entry as the base row (ADR 0041 §2), so
`SimCluster`'s hand-hosted/wire-provisioned tablets serve it immediately.
`SimClusterHandle::leader_index_of` also had to widen from scanning only
`create_table_with_replication`'s own hand-hosted-table bookkeeping to
scanning every node id — every `drain_gsi` caller's table is created
through the real wire, which never populates that bookkeeping at all; see
`docs/engineering-lessons.md`'s matching entry.

### `sim_cluster_corpus`: the SimCluster cycles/durability corpus (ADR 0061 rung D1 step 3)

`crates/animusd/src/sim_cluster_corpus.rs` (`#[cfg(test)] mod
sim_cluster_corpus;` from `lib.rs`, a sibling of `sim_cluster` for the
identical reason — same descendant-of-the-crate-root privacy property, no
visibility widened) is the first cycles/durability corpus over the
fixture above: a list-append `Recorder`/`History` model over
`SimClusterHandle::put`/`get` (see that type's own doc in `sim_cluster.rs`
for why it, not `SimCluster` itself, is what a concurrently spawned client
task calls), checked with `animus_test::check::{check_cycles,
check_durability, check_convergence}` — the identical oracle
`raftkv_linearizable.rs` (`animus-test`) uses. Run via `cargo test -p
animusd --lib sim_cluster_corpus`; depth knob `ANIMUS_SIMCLUSTER_SEEDS`
(default 1 = 8 frozen cells, ~14s; held at 25 in the nightly
`corpus-deep.yml` tier).

**`SimClusterHandle` (ADR 0061 rung D1 step 3) is what made a concurrent
workload possible at all** — `SimCluster` itself is `&mut self`
everywhere (every fault-injection method, and even its own `put`/`get`/
`delete`/`scan`, drives the simulator internally), which is fine for one
op at a time from a test's own thread but not for several overlapping
client tasks. The handle is `Clone`-able and holds only the two fields a
client-issued op ever reads/writes (`ctxs`, `tablets`) behind their own
`Mutex`es — never the whole fixture behind one coarse lock, which would
have serialized every concurrent op behind whichever one is currently
mid-`.await`. Its `put`/`get`/`delete`/`scan` are plain `&self` async
methods with **no internal simulator driving** of their own: `ClientCtx::
cp_kind_write_raw`/`cp_get`/`cp_scan` are already `CLIENT_TIMEOUT`-bounded
internally, so a client task spawned via `env.spawn_task` can `.await` one
directly while this corpus's own runner drives `Simulator::run_for`
exactly once per scenario — the identical `client_loop`/`Group::apply`
split `raftkv_linearizable.rs` uses, generalized to a whole cluster.
`SimCluster`'s own driver methods (`crash`/`restart`/`partition`/
`heal_all`/`run_for`, plus DDL) stay `&mut self`, since `Simulator` itself
isn't safely shareable that way and nothing but the driver ever needs it.

**Cells**: `baseline` (no faults), `leader_crash`, `follower_crash`,
`stop_restart` (a true process restart, not a mute), `leader_partition`
(isolate the tablet leader from the whole cluster), `split_brain`
(partition the whole cluster into two non-empty halves, not keyed to
wherever the leader lands), `forward_heavy` (RF 2 of 4 nodes — most ops
route through a node hosting no local replica), and `two_tables` (two
independently-provisioned tables, ops interleaved by the same client
tasks — see the module doc's own `Key`-space note for how two tables'
key ranges stay disjoint without a `Mop`/`History` model change). Every
op's issuing node is drawn fresh from the scenario's own seed each round
(`env.gen_below(node_count)`), so `forward_heavy` routinely forwards.

**`delete` is exercised but deliberately never fed into `check_cycles`**
— the shared `Mop`/`History` model is list-append-only (every observed
read must be a prefix of its key's one recovered order), an invariant a
tombstoning delete cannot satisfy without manufacturing a false-positive
divergence. `run_delete_probe` instead does a direct put→get(present)→
delete→get(absent) round trip from every node in the cluster in turn,
post-heal — proving a forwarded delete too, not just a forwarded put/get.

**A hang found and fixed while building this**: `run_delete_probe`'s
first draft called `SimClusterHandle::put`/`get`/`delete` via a bare
`futures::executor::block_on`, mirroring `raftkv_linearizable.rs`'s own
`final_state`'s `block_on(node.local_get(..))` — sound there because
`local_get` is a pure local read with no internal wait. `SimClusterHandle`'s
ops, unlike `local_get`, internally `.await` real `env.sleep()`-paced
poll loops; `block_on`ing one directly with nothing advancing `SimEnv`'s
virtual clock hangs forever. Fixed by driving the probe through
`SimCluster`'s own synchronous `put`/`get`/`delete` (which spawn +
`run_for` internally) instead. See `docs/engineering-lessons.md` and ADR
0061's 2026-09-05 (2) amendment for the general rule.

**Non-vacuity teeth**: `ok_writes > 0` every cell; `forward_heavy`
additionally asserts a write from a non-hosting node succeeded (tracked
per-issuing-node); `delete_probe.is_ok()` every cell. Shrink wiring
(`ANIMUS_SHRINK=1`, `sim_cluster_shrink_replay`) mirrors
`raftkv_linearizable.rs`'s own exactly. No product bug found.

### `sim_cluster_dynamo`: the DynamoDB wire edge, driven against SimCluster (ADR 0061 rung D2 PR 1)

`crates/animusd/src/sim_cluster_dynamo.rs` (`#[cfg(test)] mod
sim_cluster_dynamo;` from `lib.rs`, a sibling of `sim_cluster`/`sim_cluster_
corpus`/`sim_cluster_throttle` for the identical privacy reason) is the
first proof that a DynamoDB JSON request decoded by `animus_dynamo::
wire::decode_request` executes through the **same** generic core
`dynamo::run_operation`'s own production item-op arms call, reachable
against a `SimEnv`-backed `ClientCtx` for the first time. Run via `cargo
test -p animusd --lib sim_cluster_dynamo`. **Not yet the full nemesis
corpus** — five hand-picked, seed-parameterized scenarios today; PR 2's
plan (below) is the actual `Recorder`/`History`/`check_cycles` corpus.

**What's generic now, and how it's reached.** `dynamo::run_operation`/
`execute_as` themselves stay concrete (`ClientCtx<ProdEnv,
AnimusdRelayClient>`) — the DDL/backup/export/import/PartiQL/transact
handlers they also dispatch to are either genuinely `ProdEnv`-only or would
have pushed the signature count well past a reviewable PR (see the ADR's
own 2026-09-07 amendment for the full sizing). Instead a new function,
`dynamo::dispatch_item_op<E: Env, R: RelayClient>`, holds **six**
operations moved verbatim from `run_operation`'s own match arms — `PutItem`,
`DeleteItem`, `GetItem`, `BatchGetItem`, `UpdateItem`, `BatchWriteItem` —
calling the same `ClientCtx::cp_kind_write_item`/`cp_get`/`cp_scan` methods
those arms always called (already `E`/`R`-generic since rung C5, so no
change there at all). `run_operation`'s own arms for those six now delegate
to it (`dispatch_item_op(ctx, principal, meta, op).await`), monomorphized
at `E = ProdEnv` for production — a pure move, not a rewrite, so production
behavior is byte-identical. A new sibling entry point, `dynamo::
execute_item_op_as<E, R>`, runs `execute_as`'s own decode/prelude and is
the one `SimClusterHandle::dynamo` calls (mirroring `admin.rs::
action_data_dynamo`'s own unauthenticated `execute_routed` proxy — an
unrestricted `Principal`, since this fixture has no SigV4 listener to
resolve a scoped one from).

**`Query`/`Scan` are covered by `dispatch_item_op` for the *base-table*
case only** (`index: None` — an `index` name returns a clean
`InternalServerError` naming the gap). Genericizing the real, full
`run_query`/`run_scan` needs their own `run_index_query`/`run_gsi_query`/
`run_lsi_query`/`run_index_scan`/`run_gsi_scan`/`run_lsi_scan`/
`paginated_kind_examine`/`paginated_kind_examine_one` made generic too,
deferred to PR 2. **`run_operation`'s own `Query`/`Scan` arms are NOT in
the delegated set** — they still call the full, unmodified, concrete
`run_query`/`run_scan` with the complete GSI/LSI dispatch tree, exactly as
before this rung. This mattered live: the first cut of this PR *did* route
`run_operation`'s own `Query`/`Scan` through the narrowed
`dispatch_item_op`, and `cargo test -p animusd --lib` caught it as four
real `index_drain::gsi_drain_cursor_tests` failures (production GSI
queries returning "not yet supported") before it ever reached a committed
state. See the ADR amendment's own restatement of the general lesson: a
narrowed generic core must not become the *sole* path an unrelated,
still-full-featured production caller uses for the cases the narrowing
dropped, and the way to catch that is running the dispatcher's full
existing test suite, not just the new corpus.

**`SimCluster::new` now gives every node a real `DataRole`**, not `data:
None` — `write_path::kind_write_item_at_leader` (already generic) and
`dynamo::fast_marker_write`/`authz::record_denied` all call `ctx.data()`
(a panic on `None`, ADR 0035 PR3's guard) on their hot paths. Every
`DataRole` field is a plain, `Env`-free, `Default`-able handle
(`MetricsHandle::noop()`, `StreamSealKnobs::default()`,
`ChangeRateTracker`/`RequestRateTracker`, both `#[derive(Default)]`), so
building a real one costs nothing and needs no `ProdEnv` — this was the
"construct a SimEnv-safe `DataRole`" branch, not "gate the reads."

**`SimClusterHandle::dynamo`/`SimCluster::dynamo`** mirror `put`/`get`/
`scan`'s own async-handle/sync-wrapper split, running a decoded request
against one node's own `ClientCtx`.

**Five scenarios**: PutItem → GetItem(`ConsistentRead: true`) through the
wire on a 3-node RF3 cluster, both from a non-leader node (proves the
generic path forwards over the real `SimRelayClient` wire); `UpdateItem`
with a satisfied and a failing `ConditionExpression` (proves the
evaluate-at-leader `cp_kind_write_item` path, not the fast arm); `Query`
over a composite `(pk, sk)` table (proves `dispatch_item_op`'s base-table
arm returns exactly one partition); `BatchWriteItem`; and one leader crash
+ write-through-a-survivor + restart with a converged-or-timeout wire read
from every node afterward.

**Harness gotcha found building the crash/restart scenario**: a crashed
(muted) node's own `is_leader_local` stays frozen at its last local
belief — calling `leader_index_of` again right after `crash` + an election
window can return the **crashed** node's own id, not the new leader's
(nothing tells a muted node its peers re-elected). Route the follow-up
write through any survivor node index instead, without consulting
`leader_index_of` — `cp_kind_write_item`'s own hint-chasing forward finds
the real leader internally, exactly the shape `sim_cluster.rs`'s own
scenario 3 already uses.

**PR 2's plan** (landed — see `sim_cluster_dynamo_corpus` below): generic
GSI/LSI `Query`/`Scan`; `TransactWriteItems`/`TransactGetItems` (still
blocked on a new proof — `ClientCtx::propose_schema`'s relayed path
successfully auto-provisioning the internal idempotency table against a
genuine multi-voter `SimEnv` control quorum, never yet exercised by any
fixture in this crate); `ExecuteStatement`/`BatchExecuteStatement`/
`ExecuteTransaction` (PartiQL, no new logic once the above are generic) —
all three remain unbuilt, named as PR 2's own residuals rather than
attempted; the actual corpus itself is what PR 2 delivered.

### `sim_cluster_dynamo_corpus`: the actual end-to-end DynamoDB-wire corpus (ADR 0061 rung D2 PR 2)

`crates/animusd/src/sim_cluster_dynamo_corpus.rs` (`#[cfg(test)] mod
sim_cluster_dynamo_corpus;` from `lib.rs`, a sibling of `sim_cluster_
corpus`/`sim_cluster_dynamo` for the identical privacy reason those two
already document) is `sim_cluster_dynamo.rs`'s own "PR 2's plan" delivered:
a list-append `Recorder`/`History` model over `SimClusterHandle::dynamo`
— PR 1's generic entry point — checked with the identical `animus_test::
check::{check_cycles, check_durability, check_convergence}` oracle
`sim_cluster_corpus` already uses, but with every op crossing the real
DynamoDB JSON wire codec (`animus_dynamo::wire::decode_request` →
`dynamo::dispatch_item_op`) first. Reuses `sim_cluster_corpus`'s own 8
named cells (`baseline`/`leader_crash`/`follower_crash`/`stop_restart`/
`leader_partition`/`split_brain`/`forward_heavy`/`two_tables`) and fault
matrix verbatim — the fault dimension is unchanged from D1, only the
workload riding on top moved onto the wire. Run via `cargo test -p
animusd --lib sim_cluster_dynamo_corpus`; depth knob
`ANIMUS_DYNAMO_WIRE_SEEDS` (default 1 = the 8 frozen cells, held green at
`=25` in ~10m2s wall (601s test-binary time, 200 scenarios) — see this module's own Gates entry in the ADR's matching
2026-09-07 amendment for the exact commands).

**The list-append mapping: a server-evaluated `UpdateItem`, not a
client-tracked list.** `sim_cluster_corpus`'s own write is a plain `put`
of a client-maintained, locally-extended list — sound only because a raw
`put` is an idempotent whole-value overwrite. This corpus's one write
mechanism is instead a real `UpdateItem` request whose
`UpdateExpression` is `SET items = list_append(if_not_exists(items,
:empty), :v)` — a **server-evaluated**, genuinely non-idempotent
operation (a duplicated apply of the same entry would append the same
value twice, unlike a plain `Put`), satisfying the "SET and list_append,
non-idempotent" write shape with one expression; no separate `ADD` op was
needed. `if_not_exists(items, :empty)` means the very first write to a
key needs no separate provisioning step — the checker's `info`-not-`fail`
discipline for an indeterminate `UpdateItem` response is what makes this
safe under a fault.

**The read-consistency modeling decision (ADR 0055).** `GetItem`/`Query`/
`Scan` all decode `ConsistentRead`, and this corpus issues both values —
but only `ConsistentRead: true` observations feed the shared history
`check_cycles` runs against (the same "a read that verifies a write must
ask for `ConsistentRead: true`" discipline this file's own ADR 0055
testing-gotcha entry states). `ConsistentRead: false` reads are excluded
from `check_cycles`'s history entirely (the shared, cross-crate checker
has no weaker-read flag, and feeding a replica-local un-barriered
observation into the same `wr`/`rw` graph a linearizable read builds would
manufacture false-positive "divergence" violations the instant a
stale-but-legal read landed during a fault window) — each is instead
recorded separately and checked as a **prefix of the scenario's own
converged final state** once the fault schedule has healed and drained,
sound under this corpus's single-writer-per-key discipline (a
lagging/un-barriered replica can only have observed an earlier state of
the one writer's own strictly-ordered commit sequence). The same
exclusion `sim_cluster_corpus` already made for `delete` (a workload shape
the shared checker's model doesn't fit, not a defect either side), applied
to a second dimension this corpus introduces.

**`DeleteItem`/`BatchWriteItem` are exercised but kept out of
`check_cycles`**, for the identical reason `delete` already is in
`sim_cluster_corpus`: a tombstoning delete cannot satisfy the list-append
prefix invariant, and a `BatchWriteItem` `PutRequest` is a whole-item
overwrite, not an append. Both get their own direct correctness probes
(`run_delete_probe`/`run_batch_write_probe`), run from every node in the
cluster in turn post-heal/drain, on key namespaces (`delete-probe-*`/
`batch-probe-*`) disjoint from the list-append model's own `part-*`/
`item-*` keys.

**Base-table `Query`/`Scan` feed the same history, as multiple
`Mop::Read`s per transaction.** Every table's items live under exactly two
fixed partition keys (`part-0`/`part-1`), so a `Query` (`pk = :p`) returns
a real, strict subset of what a `Scan` returns — genuinely exercising
`dispatch_item_op`'s base-table `Query` arm rather than a `Scan` synonym —
and both decode every returned item into one `Mop::Read`, all recorded as
one history entry (`Recorder::ok` already takes a `Vec<Mop>` for exactly
this shape).

**Residuals, unchanged from PR 1's own list, pushed one rung further
out**: GSI/LSI `Query`/`Scan`, `TransactWriteItems`/`TransactGetItems`,
and PartiQL are all still unreachable through the generic
`dispatch_item_op` core this corpus drives — deferred to D3/D4, not
attempted here.

**No product bug found.** Every cell held green at both the frozen and
`=25` depths on the first complete run; the `ConsistentRead: false`
prefix check and both direct probes never found a violation.

## Tests

`cargo test -p animusd` — every test in `tests/` is a real-socket `ProdEnv`
integration test that polls with timeouts, not a deterministic assertion;
`simenv_client_ctx_tests` (above) is this crate's one `SimEnv`-driven
exception, and lives in `lib.rs` rather than `tests/` for exactly the
private-handle reason its own section gives. **ADR 0061 rung D3 (C-04)
began converting the "B class" of `tests/` binaries — base-table
DynamoDB logic reachable through `dynamo::dispatch_item_op`, needing no
real-thread liveness or real-disk durability — to `SimCluster`-driven
`#[cfg(test)] mod`s in `src/` instead**, shrinking the real-thread tier's
size rather than any CI retry count (the roadmap's own success criterion
was stale — see `docs/roadmap.md`'s C-04 entry and this crate's own
`sim_cluster_dynamo`/`sim_cluster_dynamo_corpus` doc for why). PR 1 landed
eleven sibling modules — `sim_cluster_dynamo_batch_get`,
`sim_cluster_dynamo_boolean_composition`, `sim_cluster_dynamo_eventual_
read`, `sim_cluster_dynamo_expression_surface`, `sim_cluster_dynamo_
extended`, `sim_cluster_dynamo_item_size_cap`, `sim_cluster_dynamo_
parallel_scan`, `sim_cluster_dynamo_predicate_bugs`, `sim_cluster_dynamo_
update_add_delete`, `sim_cluster_dynamo_updated_return_values`, and
`sim_cluster_kind_batch_outcome` — each replacing some or all of one
`tests/dynamo_*.rs`/`tests/kind_batch_outcome.rs` binary; see each
module's own doc for exactly which tests moved and which stayed on
`ProdEnv` (a GSI/LSI query, a wire-level `CreateTable`, or
`TransactWriteItems` — none reachable through `dispatch_item_op` yet).
`SimCluster::dynamo_concurrent` (`sim_cluster.rs`) is this rung's own new
fixture primitive, letting several wire requests race the same key/tablet
before one shared `Simulator::run_for`, replacing the `tokio::spawn`-
raced-writers idiom several of the converted tests used. A parallel
redundancy audit also found two `tests/dynamo_*.rs` tests that duplicated
existing `sim_cluster_throttle.rs`/`sim_cluster_dynamo.rs` coverage outright
(no rewrite needed) — see `dynamo_wire.rs`'s and `dynamo_throttling.rs`'s
own doc comments for the removed tests and their sim citations.

**PR 2a (ADR 0061 rung D3 PR 2a) landed 2026-09-07**: base-table DDL —
`CreateTable`/`DeleteTable`/`ListTables`/`DescribeTable`, deliberately
**without** `UpdateTable` (PR 2b) — is now drivable through `SimCluster`
over the real wire, via a new `dynamo::dispatch_table_op<E, R>` (the DDL
sibling of `dispatch_item_op`; see that function's own doc for what it
covers and why `CreateTable` rejects a declared GSI/LSI or a stream).
`sim_cluster_dynamo_table_ops.rs` (new sibling module) replaces `tests/
dynamo_table_ops.rs` whole, `dynamo_schema.rs::
create_table_rejects_reserved_namespace`, and `dynamo_extended.rs::
create_table_query_and_conditional_writes` (emptying that file, since its
own sibling test had already moved in PR 1 — deleted rather than left as
an empty shell). This PR needed two `SimCluster` fixes to make wire DDL
work **at all**, both real findings from this rung's own investigation, not
part of the original design:

- **`SimCluster::seed_members`** (new): populates `Metadata::members` for
  every node (`RegisterNode{role: "combined"}` then
  `UpsertMember{status: Active}`) — closing the gap PR 1's own amendment
  left open. Confirmed harmless to every hand-hosted scenario:
  `reconcile_placement`/`rebalance_placement` iterate `Metadata::policies`,
  never `tablets` directly, and `create_table_with_replication`'s
  hand-hosted tablets never attach one.
- **`ClusterEdgeState<E>::control` widened** `Arc<Mutex<Vec<RaftNode<
  ProdEnv>>>>` → `Arc<Mutex<Vec<RaftNode<E>>>>` (`register_control`/
  `leader_handle` widened to match), with `SimCluster::new` now calling
  `register_control` for every node. Before this, `ClientCtx::
  propose_schema`'s local-propose fast path was structurally `ProdEnv`-only
  regardless of the enclosing `E`, so under `SimEnv` **every** schema
  proposal — even a leader-issued one — took the relay branch, which meant
  relaying to **itself** and recursing until timeout (D2 called this
  branch "never yet exercised"). One existing production call site
  (`ClientCtx::admin_add_control_member`'s `leader.env().merge_peer(..)`)
  needed a new `animus_env::Env::merge_peer` default no-op method (`ProdEnv`
  overrides it to delegate to the pre-existing inherent method) to keep
  compiling generically — see `animus-env/CLAUDE.md`'s own entry.

Two more real, previously-unreachable `SimCluster` bugs were found and
fixed getting these two changes to actually work end to end: (1) a member
seeded `Active` flipped back to `Down` within `DETECT_TIMEOUT` (500ms) —
`RaftNode::start`'s own `detect_loop` gives a freshly-`Active`-but-
untracked member exactly one **synthetic** liveness observation
(ADR 0030's "phantom-member hardening"), and with no *real* heartbeat ever
following (this fixture ran none), that timestamp ages out like any
other; fixed by spawning `animus_control::node::heartbeat_loop` on every
node (`SimCluster::new` and `SimCluster::restart` both); (2)
`create_table_with_replication`'s own fixture-local tablet-id counter
(`next_tablet_id`, now deleted) could collide with `Metadata::
next_free_tablet_id()` — the wire path's own live allocator — the moment
a test used both paths in one cluster (this PR's own `list_tables_*` test
was the first to); fixed by deriving `create_table_with_replication`'s id
from the same live allocator instead. See `docs/engineering-lessons.md`'s
matching entries for both.

**`spawn_policy_tablet_host_loop`** (new, private to `sim_cluster.rs` at the
time): a minimal per-node watcher, spawned in `SimCluster::new`/`restart`,
that hosted a `RaftKvNode` for any tablet carrying a placement **policy**
(`Metadata::policies`) whose replica set names this node — the signal a
wire-provisioned tablet always carries (via `SetTabletPolicy`) and a
hand-hosted one never did, keeping the two hosting paths' tablet sets
disjoint by construction. Without it, a wire `CreateTable`'s own
`await_table_serveable` probe would time out waiting for a group nobody
ever forms — this fixture ran no real `animus_cp_data::host::Reconciler`
at all at the time.

**This PR's own reconciler-hazard investigation found a real, deterministic
gap, deliberately left unfixed at the time**: this watcher only ever
*added* a replica newly named in a tablet's `Metadata.tablets[t].replicas`
— it never tore down one a `MetaCommand::CasTabletReplicas` just dropped.
On a cluster with `node_count > MAX_REPLICATION_FACTOR` (3), the control
leader's own live (and entirely correct) `rebalance_placement` pass would
rebalance a wire-provisioned tablet's replica set to spread load across
the otherwise-idle extra node(s), leaving a stale `RaftKvNode` running on
the node the CAS removed — genuinely split-brain-shaped for that one
tablet id (two different-membership groups, no fault injection needed),
reachable with a plain `CreateTable` whenever `node_count > 3`.

**Closed by ADR 0061 rung D4 PR 1 (2026-09-07, see that rung's own entry
below, in `docs/adr/0061-*.md`, and this file's own SimCluster design-
decisions section above)**: `spawn_policy_tablet_host_loop` is deleted
outright, replaced by a real per-node `animus_cp_data::host::Reconciler` —
the same mechanism that closes this exact hazard in production. The
`node_count <= 3` restriction this gap forced on every wire-`CreateTable`
test in this crate no longer applies; `sim_cluster_dynamo_table_ops.rs::
reconciler_hazard_fires_deterministically_when_node_count_exceeds_
replication` (this investigation's own characterization) is now `every_
node_hosts_exactly_its_replica_set_after_rebalance` — a convergence proof
that every node's own hosted-tablet set converges to exactly what
`Metadata` says it should host, at the identical 4-node/3-wire-table shape
plus ten more seeds.

**PR 2b (ADR 0061 rung D3 PR 2b) landed 2026-09-07**: `UpdateTable`'s own
**throughput-only** change (`BillingMode`/`ProvisionedThroughput`, ADR
0065) is now drivable through `SimCluster` too — the half PR 2a's own
`dispatch_table_op` doc named as deferred. `dynamo::update_table_
throughput` widened to `<E: Env, R: RelayClient>` (the identical
`enable_stream`/`create_table` shape PR 2a already used — its three
`tokio::time::Instant::now()`/`tokio::time::sleep` sites became
`ctx.env.now().saturating_add(..)`/`ctx.env.sleep(..)`), and
`dispatch_table_op` gained a fifth arm: `Operation::UpdateTable` proceeds
only when `stream` and `index_update` are both `None` and
`throughput_update` is `Some(spec)` (mirroring `update_table`'s own
`(None, None, Some(spec))` match arm exactly), calling `update_table_
throughput` then re-describing the table; anything else (a stream/index
change, or no change at all) falls through to the same
`unsupported_by_generic_dispatch` shape every other excluded operation
uses. `update_table` itself — the full three-way dispatch, GSI/LSI/stream
machinery included — stays completely untouched, still `ProdEnv`-only,
still the one `run_operation` calls; `dispatch_table_op` is reached only
from `execute_item_op_as` (its `matches!` gained `Operation::UpdateTable {
.. }` alongside PR 2a's four operations), the `SimCluster`-facing entry
point, mirroring PR 2a's own "never becomes the production dispatcher's
ONLY path" discipline.

`sim_cluster_dynamo_update_table.rs` (new sibling module of
`sim_cluster_dynamo_table_ops.rs`) replaces five of `tests/dynamo_
throttling.rs`'s eleven tests — `create_table_with_provisioned_throughput_
throttles_without_any_admin_call`, `update_table_to_pay_per_request_lifts_
the_limit`, `update_table_raising_units_admits_more`, `describe_table_
reports_billing_mode_and_throughput`, `update_table_throughput_on_a_
follower_is_relayed_to_the_leader` — every assertion carried over unchanged
in *kind* (an admission/refusal outcome, a rendered `DescribeTable` shape,
a converged per-table throughput spec on every node), never a metric
counter (see `sim_cluster_throttle.rs`'s own doc for why: `ThrottledWrites`/
`ThrottledReads` never increment under this fixture, since every
metric-recording site gates on `self.data.as_ref()` and this fixture's
`DataRole`, real since D2 PR 1, never populates those two specific
counters). The `ProdEnv` original's own real-wall-clock converged-or-
timeout retry for a raised-throughput admission becomes a bounded loop of
further `SimCluster::dynamo` calls here — no explicit `run_for`/sleep
needed, since each call already advances the cluster's own virtual clock
by `OP_BUDGET` (12s). The remaining six `dynamo_throttling.rs` tests (batch
shedding, `TransactWriteItems`, a forwarded-write throttle check, the
`/admin/metrics` counter regression, and the cluster-wide `cluster_
settings` config-surface test) stay on `ProdEnv` — none reachable through
`dispatch_item_op`/`dispatch_table_op` yet. **No new `SimCluster` fixture
bugs found this PR** — PR 2a's two fixes (the member-liveness heartbeat
gap, the tablet-id-allocator collision) were sufficient; `UpdateTable`'s
commit-wait shape is byte-identical to `CreateTable`'s.

`crates/animusd/tests/auto_split_min_tablets.rs` was checked and
deliberately left on `ProdEnv`: its own `UpdateTable` call raises a
table's declared throughput to grow ADR 0067's derived minimum tablet
count, but the test's real subject is that background trigger's
real-thread behavior (a live per-tick auto-split loop forking a real
CP-data tablet group) — `SimCluster` hand-hosts tablets (no real
`animus_cp_data::host::Reconciler`, no live auto-split loop), so this test
has no sim analog regardless of how far `dispatch_table_op` widens; a
D4-shaped gap, not a D3 one.

**PR 3a (ADR 0061 rung D3 PR 3a) landed 2026-09-07**: GSI/LSI `Query`/
`Scan` dispatch through `SimCluster`, plus `CreateTable` with a declared
GSI/LSI — the D2 PR 1 residual `dispatch_item_op`'s own doc named
(`run_index_query`/`run_gsi_query`/`run_lsi_query`/`run_index_scan`/`run_
gsi_scan`/`run_lsi_scan`/`paginated_kind_examine`/`paginated_kind_examine_
one`, all eight now `<E, R>`-generic) and the D3 PR 2a residual
(`dispatch_table_op`'s `CreateTable` arm no longer rejects a declared
index — only a stream declaration is rejected now). `dispatch_item_op`'s
`Query`/`Scan` arms route a named index through `run_index_query`/`run_
index_scan` instead of `unsupported_by_generic_dispatch`, mirroring `run_
query`/`run_scan`'s own dispatch exactly; `run_operation`'s own arms stay
untouched, still calling the full concrete `run_query`/`run_scan` (the D2
lesson repeated a third time). 42 new tests across nine sibling modules —
`sim_cluster_dynamo_query_filter`/`_query_pagination`/`_query_range`/
`_scan_index_forward`/`_consistent_read`/`_select`/`_consumed_capacity`/
`_item_collection_metrics`/`_indexes` — converted from `dynamo_query_
filter.rs`/`dynamo_query_pagination.rs`/`dynamo_query_range.rs`/`dynamo_
scan_index_forward.rs`/`dynamo_consistent_read.rs` (deleted whole)/`dynamo_
select.rs`/`dynamo_consumed_capacity.rs` (deleted whole)/`dynamo_item_
collection_metrics.rs` (deleted whole)/`dynamo_indexes.rs`. **A real
`SimCluster` capability gap this PR's own investigation surfaced (see this
crate's own `SimCluster` section, above, for the full account)**: a GSI's
own hidden table is never materialized at all under this fixture — no
`index_drain::change_consumer_loop` ever runs, so `run_gsi_query`/`run_gsi_
scan`'s own `!meta.has_table_tablet` gate is unconditionally true here —
pinned by `sim_cluster_dynamo_table_ops.rs::gsi_query_reads_empty_under_
the_fixture_until_the_drain_generalizes`; every GSI-*data* test therefore
stays on `ProdEnv`, and `cross_index_cursor_mismatch_is_rejected`'s
`SimCluster` version drops one of its four sub-cases for the identical
underlying reason one level earlier (the empty-page gate fires before the
`ExclusiveStartKey`'s own shape is ever checked). **No other new fixture
bugs found**. `cargo test -p animusd --lib`: 294 passed (252 before, +42,
zero regressions, ~110s wall, 3 ignored throughout).

**PR 3b (ADR 0061 rung D3 PR 3b) landed 2026-09-07, closing the GSI-drain
gap PR 3a's own investigation surfaced**: `index_drain::drain_tablet`
(now `pub(crate)`) and its private helper `reconcile_partition` widened
to `<E: Env, R: RelayClient>` — a pure signature change (every callee
each uses was already generic; the full pre-existing real-socket
regression net for both, run against the widened code before any
`ProdEnv` file was trimmed, passed unchanged) — plus a new `SimCluster::
drain_gsi(node, table)` fixture helper (`sim_cluster.rs`, see this file's
own `SimCluster` section above for the full design) that materializes a
table's hidden GSI table(s) on demand. Flips `sim_cluster_dynamo_table_
ops.rs`'s own boundary regression to a positive assertion (renamed
`gsi_query_materializes_rows_after_a_drain`) and converts every GSI-data
test PR 3a left on `ProdEnv`: `sim_cluster_dynamo_query_filter.rs`
(`filter_applies_to_a_gsi_query`), `sim_cluster_dynamo_query_
pagination.rs` (`gsi_query_paginates_with_the_scan_cursor_shape`, plus
restoring `cross_index_cursor_mismatch_is_rejected`'s dropped fourth
sub-case now that a real hidden-table tablet lets the cursor-shape check
run), `sim_cluster_dynamo_query_range.rs` (`gsi_range_queries_over_
mixed_digit_count_n_sort_keys`), `sim_cluster_dynamo_scan_index_
forward.rs` (`descending_applies_to_a_gsi_query`, `gsi_scan_index_
forward_orders_n_sort_keys_numerically`), `sim_cluster_dynamo_select.rs`
(`count_select_applies_to_a_gsi_query`) — 6 tests across 5 existing
sibling modules, each now fully converted (their own `tests/dynamo_*.rs`
source deleted whole). Three conversions needed new homes: `dynamo_
documents.rs`'s all three tests move to a new `sim_cluster_dynamo_
documents.rs` (only the middle one, `multiple_gsis_composite_gsi_and_
lsi`, actually touches a GSI); `dynamo_update_add_delete.rs`'s last test
(`an_add_that_changes_an_indexed_attribute_reindexes`) moves into the
existing `sim_cluster_dynamo_update_add_delete.rs`, draining twice (once
for the pre-update baseline, once after the reindex); `dynamo_schema.rs`'s
`create_table_index_replicates_to_second_node` moves to a new `sim_
cluster_dynamo_schema.rs` (that file's restart proof and `extended_
surface` stay — real WAL durability and `TransactWriteItems` respectively,
neither reachable here). Per this rung's explicit instruction, `dynamo_
indexes.rs::gsi_write_then_query` is **not** deleted (still D2 PR 1's
real-socket proof of `run_operation`'s independent path); `sim_cluster_
dynamo_indexes.rs` gains a sim twin, `gsi_write_then_query_sim`, proving
the identical sequence through the generic core instead. **A second, small
fixture fix was needed along the way**: `SimClusterHandle::leader_index_
of` used to scan only `create_table_with_replication`'s own hand-hosted-
table bookkeeping, which every wire-created table (every table this PR's
own tests use) never populates — widened to scan every node id instead
(strictly more general, no less correct for a hand-hosted table either).
Seven `tests/dynamo_*.rs` files deleted whole (`dynamo_query_filter.rs`/
`dynamo_query_pagination.rs`/`dynamo_query_range.rs`/`dynamo_scan_index_
forward.rs`/`dynamo_select.rs`/`dynamo_documents.rs`/`dynamo_update_add_
delete.rs`), one trimmed (`dynamo_schema.rs`). `cargo test -p animusd
--lib`: 306 passed (294 before, +12, zero regressions, ~115s wall, 3
ignored throughout; `gsi_drain_cursor_tests` stays green). See ADR 0061's
2026-09-07 "D3 PR 3b" amendment for the full account, including the
`ANIMUS_SEED` determinism replay and the before/after real-socket gate
runs.

**D4 PR 1 (ADR 0061 rung D4 PR 1) landed 2026-09-07, closing issue #715**:
`SimCluster` is now hosted by a real `animus_cp_data::host::Reconciler`,
one per node — see this file's own SimCluster "Design decisions" section
above for the full design and this section's own reconciler-hazard entry
for what it replaced. Summary of what changed, all in `sim_cluster.rs`:
`spawn_policy_tablet_host_loop` (private, hosted only *newly-named*
replicas, never tore one down) is deleted outright, replaced by
`build_reconciler`/`spawn_reconciler_loop` — one `Reconciler<SimEnv,
MemoryEngine>` per node, its own `MemoryTabletEngines` registry, `on_host`/
`on_teardown` hooks mirroring hosting into `ClusterEdgeState` exactly like
`animusd`'s own production node assembly, driven by a task racing
`metadata_watch().changed(..)` against a fixed fallback sleep (mirroring
`tablet_host_reconciler_loop`'s own shape, minus its `fork_wake()` arm and
pre-recovery guard — neither is reachable here, see that function's own
doc). `create_table_with_replication` no longer constructs a `RaftKvNode`
by hand: it proposes `CreateTableSchema`/`CreateTablet`/`SetTabletPolicy`
(the policy's RF is the caller's own `replication` argument, not
`MAX_REPLICATION_FACTOR`) and waits for the reconciler to host it, the
identical mechanism a wire `CreateTable` already used — both hosting paths
are now literally the same code. `restart` purges every one of a node's
own stale edge registrations (`ctx.edge.hosted_groups()`, not just this
fixture's own hand-hosted-table bookkeeping) and builds a fresh
`Reconciler` reusing the SAME `MemoryTabletEngines` handle — a deliberate
behavior change from the pre-D4 restart, which always built a fresh, empty
`MemoryEngine`: a restarted node's own tablet data is no longer wiped (see
this file's own SimCluster restart bullet). Production-side: zero
signature changes were needed — `ClusterEdgeState::unregister_raftkv`
already existed next to `register_raftkv` (this rung's own read-only pass
had flagged adding one as a possible need; the existing method was
sufficient as-is).

**No existing scenario changed behavior.** Every `SimCluster`/corpus cell
that provisions a table keeps its own tablet-hosting count within ADR
0029's max−min ≤ 1 balanced band (a single table's initial placement, or
`two_tables`' identical-replica-set pair) — the real reconciler's own
`reconcile_loop`/`rebalance_placement` genuinely never has anything to move
for any of them, so no fixture bookkeeping (`SimClusterHandle::
replicas_of`'s static creation-time snapshot, used by `sim_cluster_corpus.
rs`/`sim_cluster_dynamo_corpus.rs`) went stale. `every_node_hosts_exactly_
its_replica_set_after_rebalance` (renamed from `reconciler_hazard_fires_
deterministically_when_node_count_exceeds_replication`, `sim_cluster_
dynamo_table_ops.rs`) is the one scenario that *does* rebalance (by
design — it's the former hazard's own 4-node/3-table shape), now a
convergence proof instead of a documented gap, run at the pinned seed plus
ten more (`every_node_hosts_exactly_its_replica_set_after_rebalance_over_
seeds`). `create_table_issued_on_a_control_follower_relays_and_converges`
was bumped from 3 to 4 nodes as this rung's own proof the `node_count <= 3`
restriction is lifted for an ordinary (non-rebalancing) scenario too. A
new `SimClusterHandle::hosted_tablets(node)`/`SimCluster::hosted_tablets`
accessor (the tablet-id set of `ClusterEdgeState::hosted_groups()`) backs
both the renamed test's own check and a new general "no zombie groups"
invariant added to `sim_cluster_corpus.rs`'s end-of-scenario checks
(`check_no_zombie_groups`, `ScenarioResult::no_zombie_groups`) — checked
unconditionally on every cell, non-vacuous only against a future cell that
introduces a genuine rebalance (none does today). `cargo test -p animusd
--lib`: 306 passed / 118.4s wall before this rung, 307 passed / 113.4s wall
after (net +1: +2 new tests, −1 renamed away — zero regressions, and wall
time is within ordinary run-to-run noise of the baseline). **This needed
one tuning pass**: [`RECONCILER_FALLBACK`] (`sim_cluster.rs`) started at
50ms (matching `poll_until`'s own convergence-check step) and measured
~3s/scenario in `sim_cluster_corpus.rs` at `ANIMUS_SIMCLUSTER_SEEDS=3`
(vs. ~1.75s/scenario pre-D4-PR-1) — every long-running scenario (`SETTLE`/
fault-window/`DRAIN`, several seconds of virtual time) pays for a
reconciler tick every 50ms whether or not anything changed, since real
convergence is driven by `metadata_watch()`'s own near-instant wake
regardless of the fallback's own length. Widened to 200ms (still 2.5x more
responsive than production's own 500ms `RECONCILE_FALLBACK_INTERVAL`) —
~2.1s/scenario, and the full-suite wall time above reflects it. See ADR
0061's matching 2026-09-07 "D4 PR 1" amendment for the full account,
including what remains open for D4 PRs 2-5 (auto-split's own `tokio::time`
conversion, GC's `drop_table` driver, join/growth, the backup janitor's
`client_ctx_host.rs` widening).

**D3 closed 2026-09-07 (rung D3 PR 4, CI re-baseline + docs, no source
change)**: the real-thread `tests/*.rs` tier and the deterministic sim
tier now run in different CI jobs. `.github/workflows/ci.yml`'s `gates`
job runs `cargo test -p animusd --lib` — the deterministic
`sim_cluster*`/`SimCluster` corpus, plus a handful of pre-existing
real-thread `#[cfg(test)] mod`s that still live inside `--lib` and ride
along since `cargo test --lib` has no way to split one target further
(`confirm_futility_tests`/`forward_transport_failure_tests`/
`halted_shutdown_tests`/`issue_412_tests`/`issue_298_conflict_tests` in
`lib.rs`, `stream_write_path_tests` in `dynamo.rs`, `gsi_drain_cursor_
tests`/`stream_sealer_tests` in `index_drain.rs`, `orphan_reap_tests` in
`segment_janitor.rs`, `system_table_tests` in `admin.rs` — each a small
single-node bring-up, judged an acceptable blast radius for `gates`'s
2-vCPU runner) — while `prod-liveness-animusd`'s 4 nextest shards now run
`--tests` only: the 100 real-socket `tests/*.rs` integration binaries
(down from 120 pre-D3; 418 tests, down from 521). `sim_cluster*.rs` itself
grew from 5 to 29 modules and 35 to 140 tests across D3's five PRs. Net
`cargo test -p animusd --lib`: 240 → 306 (307 after D4 PR 1). The CI
shard partition count stays at 4: each shard rebuilds `-p animusd` from a
cold cache, so a compile floor sits under every shard regardless of test
count — fewer partitions raises the max shard wall time, it does not
lower it (measured: 598s → 564s at the slowest shard). See ADR 0061's
2026-09-07 "D3 closed" amendment for the full before/after numbers.

**Residual `tests/*.rs` inventory (100 files / 418 tests) by class, as of
the D3 close**: (A) real-thread liveness/timing, 10 files/13 tests —
genuine OS-thread timing (election, group commit, lock races) `SimEnv`
cannot prove, stays `ProdEnv` permanently; (B) real-disk durability/
restart, 9 files/24 tests — real fsync/crash recovery, stays `ProdEnv`
permanently; (C) real crypto/DNS/TLS/OTLP/sockets, 9 files/32 tests — TLS
handshake, DNS resolution, SigV4, OTLP export, raw framing, stays
`ProdEnv` permanently; (D) waiting on a `SimCluster` capability this
fixture doesn't have yet, 65 files/317 tests, split by what's missing —
admin/console/dashboard HTTP (10/66), PartiQL (2/37), join/growth/
decommission (9/34), Transact (6/32), index DDL beyond plain `CreateTable`
(9/30), backup/PITR/export/import (6/29), Streams (3/28), control/data
role split (5/21), reconciler-driven split/rebalance/GC (7/13), TTL
(1/9), node assembly/raw `ClientRequest` (2/8), throttle metric counters
(1/6), auto-split loops (2/2), `--config` bring-up (2/2) —
reconciler-driven split/rebalance/GC, auto-split, join/growth, and the
backup janitor are D4's own scope (D4 PR 1 already supplied the real
reconciler these need next); Transact and PartiQL are D2's own named
residuals; admin/console/dashboard HTTP, Streams, TTL, the control/data
role split, `--config` bring-up, index DDL beyond `CreateTable`, node
assembly, and the throttle-metric counters are unowned by any planned
rung as of this close; (E) frozen behind an open flake issue, 7 files/32
tests (#298, #418, #592, #601, #610, #619/#622, #627) — out of scope for
C-04, tracked by their own issues.

**Standing rule for new `animusd` logic tests**: default to a
`sim_cluster_*` sibling module (`SimCluster::dynamo`/`dynamo_concurrent`/
`create_table_with_replication`, or the generic `dispatch_item_op`/
`dispatch_table_op`/index-query cores) — deterministic, seed-replayable,
no real socket/thread/disk. Reach for a `tests/*.rs` `ProdEnv` binary only
when the behavior under test genuinely needs class (A), (B), or (C) above:
real-thread liveness/timing, real-disk durability across a process
restart, or real crypto/DNS/TLS/socket framing.

The restart tests run both incarnations in the same runtime,
calling `Node::shutdown()` between them. In-crate `#[cfg(test)] mod`s
(`confirm_futility_tests`) live in `lib.rs` itself
because they need private handles (a raw `CpGroup`/the `pub(crate)`
`ClientCtx::cp_kind_eval_local`, retargeted from the now-deleted
`cp_kind_local`, ADR 0054 step 4b) that no external `tests/` file can reach;
`index_drain.rs`'s own `gsi_drain_cursor_tests` is a third (run via `cargo
test -p animusd --lib`, not the `tests/` tree) — the ADR 0042 §7/§8
cursor-based drain + trim janitor regressions, needing `CpGroup`'s private
`pending_changes`/`cursor_min_watermark` and the plain-client-protocol
`ClientRequest::SplitTablet` (an arbitrary binary `split_key`, unlike the
admin HTTP surface's UTF8-string one); `dynamo.rs`'s own
`stream_write_path_tests` is a fourth (ADR 0042
§1), needing `CpGroup`'s private `pending_changes`/`local_scan_kind_bounded`
(a new, non-linearizable bounded kind-scan wrapper, mirroring
`local_get_kind`'s existing shape) to prove a streamed-unindexed table's
write commits exactly base + change, no LSI/footprint row;
`index_drain.rs`'s own `stream_sealer_tests` is a fifth (round-3 sealer PR,
extended by the ADR 0042 fork G age-trigger-derivation rewrite) — the seal
arm's triggers/sequence (size, age — both the never-sealed driver-local
fallback and the catalog-derived basis a later backlog uses once a tablet
has sealed at least once — empty-hot no-seal, a real-but-below-threshold
backlog also never seals, and the exactly-at-watermark boundary), the
F10/F12-b hot-trim rework (the GSI+stream min-rule, and — reviewed hard —
the disabled-draining-does-not-block-trim rule), disable-as-final-seal with
epoch continuity across a disable/re-enable cycle, and F11's split-key
token alignment, needing `CpGroup`'s private `pending_changes`/
`approx_bytes_kind`/`cursor_min_watermark` and, to confirm a segment
genuinely landed, a second `FsSegmentStore` handle at the exact
`<node dir>/segments` path the default store roots its own local building
block at. `lib.rs`'s own `halted_shutdown_tests` is a sixth — the
issues #282/#279 regression above, needing the same private `CpGroup`
(specifically its `#[cfg(test)]`-only `is_halted`) to prove bare
`Node::shutdown()` and `Node`'s `Drop` impl both latch every hosted
group's `halted` flag. `lib.rs`'s own `simenv_client_ctx_tests` is a
seventh, and differently motivated than the other six: it needs no private
handle any of them is missing, but a real `ClientCtx<SimEnv, _>` — see the
"SimEnv `ClientCtx` harness" section above for the full design.

One binary per behavior; the file names describe them (`ls
crates/animusd/tests/`) — covering combined/control-only/data-only/split
deployment shapes and growth/decommission, control-plane and CP-data-plane
membership change, the DynamoDB/admin/dashboard wire edges (including
the ADR 0041 secondary-index and ADR 0018 transaction suites), the ADR
0042/0043 streams surface end to end (`docs/streams-notes.md` has the
streams-specific test notes), the ADR 0051 TTL surface end to end
(`dynamo_ttl.rs` — enable/disable/describe, the AWS-faithful
immediate-visibility-then-eventual-reap contract, the future/wrong-type/
5-year-safety-window never-expire cases, the conditional-delete outcome,
and the stream `userIdentity`; its own follower-relay regression for
`UpdateTimeToLive` lives beside the rest of `schema_ddl_relay.rs`'s DDL
suite, not in `dynamo_ttl.rs` itself), the ADR 0059 Train 1 PR④ on-demand
backup wire surface end to end (`dynamo_backup.rs` — `CreateBackup`/
`DescribeBackup`/`ListBackups`/`DeleteBackup`, the wire round trip through
`AVAILABLE`, `DescribeBackup`/`ListBackups(TableName)` still working after
the source table is dropped with the frozen `BackupSizeBytes` unchanged, the
janitor's own row-removal convergence, `ListBackups` pagination, and the
`TableNotFoundException`/`BackupNotFoundException`/`BackupInUseException`
error shapes; its own follower-relay regression for `DeleteBackup`
(`MetaCommand::MarkBackupDeleted` on the `is_relayable_command` allowlist)
lives beside the rest of `schema_ddl_relay.rs`'s DDL suite, mirroring
`UpdateTimeToLive`'s own precedent exactly), the ADR 0059 Train 2 restore
surface end to end (`dynamo_restore.rs` — the full `CreateBackup` →
`AVAILABLE` → write-more-data → `RestoreTableFromBackup` → converged round
trip proving the restored table serves exactly the backup-time rows with a
queryable, converged GSI; restore-after-source-drop; and the
`BackupNotFoundException`/`BackupInUseException`/`ResourceInUseException`
error shapes; its own follower-relay regression for `BeginRestore`/
`CompleteRestore` lives beside the rest of `schema_ddl_relay.rs`'s DDL
suite, the same precedent again), the ADR 0059 Train 3 PR② PITR restore
surface end to end (`dynamo_pitr_restore.rs` — enable PITR → timed writes,
each confirmed sealed via the sealed segment's own `seal_wall_ms` read off
`node.metadata()` rather than a wall-clock race → `RestoreTableToPointInTime`
to a mid-point second → exactly the rows as of `T`, both via a literal
`RestoreDateTime` and `UseLatestRestorableTime`; a deleted-table PITR
restore within the window; and the `TableNotFoundException`/
`PointInTimeRecoveryUnavailableException`/`InvalidRestoreTimeException`
error shapes — using `run_node_with_streams_and_pitr_snapshot_cadence` to
shrink the periodic base-snapshot cadence from its 6-hour production
default to a test-sized interval), restart/durability across every
deployment shape, and the `WatchMetadata`/system-table/OTel/metrics support
surfaces.
`support/mod.rs` holds the shared bring-up helpers (port-TOCTOU retries,
split-cluster bring-up), **and `support::PanicSafeTempDir`/
`support::panic_safe_tempdir()` (issue #511)** — the drop-in replacement
for `tempfile::tempdir().unwrap()`/`TempDir::new().unwrap()`: it leaks
its directory (`std::mem::forget`) instead of removing it on a
*panicking* drop, so a mid-test assertion failure can no longer cascade
into the control-plane WAL's unconditional `.expect("wal append"/"wal
sync")` panic (`animus-control::node::persist_wal` has no `halted`-gate at
all — see that crate's own CLAUDE.md entry — and `Drop for Node`
deliberately only latches CP-group `halted` flags, never aborting the
still-live background driver tasks a panic's own unwind can race). A
normal, non-panicking drop is byte-identical to `TempDir`. **Every one of
the 101 `tests/*.rs` files now `mod support;` and uses it** — the
migration originally scoped to the 59 files already pulling in `mod
support` (PR #533) was completed for the remaining ~42 hand-rolled-fixture
files in the issue #511 residual sweep (including the original #273 site,
`dynamo_index_scan.rs::setup()`), a purely mechanical `mod support;` +
`support::panic_safe_tempdir()` swap with no fixture restructuring. **Not
covered, and structurally can't be**: eight in-crate `#[cfg(test)] mod`s
inside `src/` construct a bare `TempDir` directly and remain exposed to
the same cascade, since `tests/support` is a separate crate (each `tests/
*.rs` file) these modules can't reach —
`dynamo.rs::stream_write_path_tests`, `index_drain.rs::{
gsi_drain_cursor_tests, stream_sealer_tests}`, `lib.rs::{
confirm_futility_tests, forward_transport_failure_tests,
halted_shutdown_tests, issue_412_tests, issue_298_conflict_tests}`,
`segment_janitor.rs::orphan_reap_tests`, and
`admin.rs::system_table_tests`. Closing this residual would need a
production-side (not test-only) panic-safe temp-dir helper shared across
`src/`, which is a separate, deliberately-not-attempted design decision —
see `docs/engineering-lessons.md`'s matching entry.
`docs/engineering-lessons.md` names the mechanism and this file's own doc
comment on `PanicSafeTempDir` has the full account. Regression
(deterministic, no ProdEnv, no timing dependency):
`tests/panic_safe_teardown.rs`.

## Benchmark

`benches/cluster_bench.rs` (`cargo bench -p animusd`) is a hand-rolled
(no criterion, zero new dependencies), `harness = false` bench of the
DynamoDB JSON/HTTP wire over a real in-process cluster — `ProdEnv`, real
sockets/disk/clock, following `animus-storage/benches/engine_bench.rs`'s
style. It measures, per operation class, p50/p99/p99.9/mean latency and
throughput: `PutItem`, `GetItem` with `ConsistentRead: true` and `false`
**reported separately, never blended** (ADR 0055's two read paths),
`Query` within a partition, a paged `Scan`, a concurrent-`PutItem`
throughput sweep at `ANIMUS_BENCH_CLIENTS` client counts (each its own
persistent TCP connection), and a **degraded phase**: after the
healthy-cluster classes it finds and kills the bench tablet's own leader
node (`/admin/raftkv`'s `is_leader`) and re-measures `PutItem`/
`GetItem(ConsistentRead:true)` through the resulting election, via a
bounded-retry wire helper that counts (and reports) retries rather than
failing on a transient "not the leader here". Cluster bring-up follows
`tests/inplace_split_bench.rs`'s bounded-retry port-TOCTOU idiom. Workload
knobs: `ANIMUS_BENCH_NODES` (3),
`ANIMUS_BENCH_ITEMS` (2_000, preload size), `ANIMUS_BENCH_OPS` (1_000,
measured ops per class), `ANIMUS_BENCH_VALUE_BYTES` (256),
`ANIMUS_BENCH_CLIENTS` ("1,8,32"), and `ANIMUS_BENCH_JSON=<path>` to also
write a machine-readable results document. Its methodology deliberately
tracks `website/performance.html`'s stated commitments (tail percentiles
not averages, both read modes reported apart, a failure phase in every
run, no DynamoDB comparison) — see that file and this bench's own module
doc for the full mapping.

**Manual/local only — this bench does not run in CI.** Real sockets, real
disk, and real elapsed wall clock make it unsuitable for a shared runner's
noise floor (the same reason `tests/inplace_split_bench.rs`'s own bench is
`#[ignore]`d rather than part of the default `cargo test` run). Run it
locally when you need a number, not as
a gate. **Numbers are comparable only to another run on the same host, in
the same session** — never across machines or sessions, per
`docs/engineering-lessons.md`'s "a historical bench figure from a
different host is not a baseline" entry: if you need to compare against
an earlier figure, rerun the earlier configuration alongside the new one
on this same host rather than trusting a number quoted from elsewhere.

**`tests/cluster_gt_rf_split_bench.rs`** (ADR 0062's cluster>RF follow-up
amendment, 2026-09-01) is the `#[ignore]`d bench that finally answers the
"reproducing that win would need a bench cluster wider than RF" gap
rung 7 named and could not fill: a 3-node RF=3 cluster grown by one
lower-sorting-id node immediately before kickoff (mirroring `tests/
split_placing_completion.rs`'s own recipe), so the split's placement
target is provably NOT the parent's current replicas. Measures (a)
kickoff→children-Active, (b) kickoff→directed-Placing fully converged, and
(c) max write blip across the whole window, with a paced continuous
writer running throughout. **The workload's write-continuity matters more
than the node count**: an idle version of the identical scenario (no
interleaved writer) converges the pre-ADR-0062 F5-fused baseline's
learner catch-up in ~10s; add the writer, and it did not complete within a
5-minute budget in 3/3 runs on this host — see
`docs/engineering-lessons.md`'s matching entry and the ADR's own amendment
for the full numbers, including a second, separately-flagged (not fixed)
finding: fork-first's own post-cutover directed-Placing completion loop
also failed to reach `done` within a 240s budget in 2/3 runs under the
same load, despite one of those two already having live replicas matching
its target.

## Appendix — SimCluster drop-table GC coverage (ADR 0061 rung D4 PR 3, 2026-09-07)

`sim_cluster_dynamo_drop_table.rs` is the deterministic `SimCluster`
coverage for dropped-table GC (ADR 0024), driven through the real
`DeleteTable` wire operation against every node's own real
`animus_cp_data::host::Reconciler` (D4 PR 1) — a driver-plus-assertions
PR, no `host.rs` changes. `SimCluster::storage(node, tablet) ->
MemoryEngine` (new this rung, mirroring `animus-cp-data/tests/
reconciler_corpus.rs::Cluster::storage`'s own "reads back empty"
convention) is the physical-reclaim observable: a reclaimed tablet's
engine, read back through the same `MemoryTabletEngines` registry the
node's reconciler opens from, is a fresh, empty one. Four of five
scenarios converge (base table, GSI cascade via `SimCluster::drain_gsi`,
create-then-immediately-drop, drop off a rebalanced replica set); the
fifth found a real gap — a node crashed while hosting a table, restarted
only after the drop has already converged elsewhere, never reclaims its
own leftover tablet engine, because `host::Reconciler::gather_facts`
derives every fact solely from tablets `Metadata` *currently* names, and
the dropped tablet is synchronously absent from it by the time the fresh
post-restart `LocalState` ever gets a first look — see ADR 0061's
matching 2026-09-07 "D4 PR 3" amendment for the full mechanism and the
seeds it was confirmed at. Kept as one `#[ignore]`d regression
(`scenario_4_a_node_crashed_during_the_drop_and_restarted_leaks_its_
engine`) rather than fixed here — a real fix would need a new
probe-existing-local-engines-not-in-Metadata capability on `EngineFactory`
and both its implementors, genuine new mechanism out of this PR's own
scope. `crates/animusd/tests/drop_table_gc.rs`/`drop_table_index_
cascade.rs` were left whole, unconverted — every test in both interleaves
real-disk `LsmEngine` WAL-file assertions with metadata/hosting
convergence in one body, a shape this fixture's `MemoryEngine` tier
cannot stand in for.

## Appendix — issue #722 fixed: the crashed-during-the-drop reclaim gap D4 PR 3 found (2026-09-07)

The gap the previous appendix names — `scenario_4_a_node_crashed_during_
the_drop_and_restarted_leaks_its_engine`, kept `#[ignore]`d as "not fixed
by this PR (driver+assertions scope only)" — is now closed, in
`animus-cp-data::host` (not here): `EngineFactory` gained `local_tablets(&
self) -> BTreeSet<TabletId>`, the reconciler's second fact source
alongside replicated `Metadata`, consulted exactly once (this node's very
first tick after construction, never a later one). See `crates/animus-cp-
data/CLAUDE.md`'s host-module entry for the mechanism, the `known`-set
safety argument (an engine only ever exists locally for a tablet id this
node has itself observed as real, so a locally-present id absent from both
the current tablet map and every live in-place-split intent's own children
is always a genuine leftover, never a tablet that merely hasn't appeared in
`Metadata` yet), and `docs/adr/0024-drop-table-data-gc.md`'s 2026-09-07
amendment for the full incident and its restored restart-time guarantee.

**This crate's own contribution**: the production `EngineFactory` impl,
`LsmTabletFactory` (`lib.rs`) — `local_tablets` lists this node's data
directory once (the identical `env.list()` call `probe`/`destroy` already
make) and parses each filename's own `db-t{tablet}-` prefix via the new
`parse_tablet_id_from_lsm_filename` (the inverse of `tablet_lsm_prefix`,
unit-tested in `lsm_tablet_filename_tests` — including the `db-t5-*`
vs. `db-t51-*` disambiguation `tablet_lsm_prefix`'s own doc names, and that
the bare control/syskv engine's own `db-MANIFEST`/`db-wal-*`/`db-sst-*`
files are never misread as tablet engine files). No `animus_cp_data::
host::plan`/`Reconciler` change was needed on this side — only the trait
implementation.

**The formerly-`#[ignore]`d scenario is now a positive, un-ignored
assertion**, renamed `scenario_4_a_node_crashed_during_the_drop_and_
restarted_reclaims_its_engine` (`sim_cluster_dynamo_drop_table.rs`),
replayed at the original six investigation seeds
(`0xE4AF_0004`, `0xE4AF_4000..=0xE4AF_4004`) plus ten more
(`_over_seeds`). **Two real test-harness bugs, unrelated to the fix
itself, were found and fixed delivering this positive assertion — both
worth recording as general lessons, not just this scenario's own
footnotes**:

- **The victim node must be neither the tablet's own data-plane leader NOR
  the control-plane's own leader.** The scenario's original victim
  selection only excluded the data-plane leader; crashing a victim that
  also happened to be the control-plane leader forced a control-plane
  election before `DeleteTable`'s own 10s commit-wait could ever succeed —
  an entirely different (and, empirically, not always fast enough under
  this fixture's own polling) scenario the test was never trying to
  exercise. A 200-seed scan at an unrelated fresh seed range found this
  hit roughly half the time; fixed by excluding
  `cluster.control_leader_index()` too — a 3-node cluster always has a
  node that is neither.
- **`assert_reclaimed`'s own convergence check was split across two
  passes — a metadata/hosted-set poll, then a separate, unwaited engine
  read — which is unsound for a just-restarted node specifically.** A
  freshly restarted control `RaftNode` in this fixture starts from a
  genuinely blank `Metadata` (see `sim_cluster.rs`'s own `restart` doc),
  indistinguishable at that instant from "already caught up to the table
  being dropped" — so the metadata/hosted-set half of the check could
  (and, for the pinned seed, did) converge on the very first poll, well
  before the reconciler had ticked even once, and the separate post-loop
  engine read then observed stale, pre-crash content and failed on
  otherwise-correct behavior. Fixed by folding all three observables
  (metadata absence, hosted-set absence, engine-empty) into the SAME poll
  loop — this module's own doc now states the rule directly: never split
  one converged-or-timeout property across two differently-timed checks,
  the same root `CLAUDE.md` lesson wearing a new shape (a two-stage check
  where each stage looks correct in isolation).

**A matching real-disk regression** landed in `tests/drop_table_gc.rs`:
`a_node_stopped_before_the_drop_and_restarted_after_reclaims_its_leftover_
engine` — a real 3-node, one-process-per-node cluster, node 2's whole
process stopped (`shutdown_graceful()`) before `DROP TABLE`, the drop
issued and fully converged on the two live nodes, only then node 2
restarted on the same directory/addresses
(`support::restart_same_addrs`) — its own leftover `db-t{tablet}-*` files
(`tablet_engine_present`, a new helper alongside the file's existing
`tablet_wal_present`) are gone within a bounded converge-or-timeout poll.
Run 3x locally with no flake before landing.
