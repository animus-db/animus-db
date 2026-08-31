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
`control_handle`, `dashboard`, `dynamo`, `dynamo_streams`, and `http`. Ten
modules now carry the narrower `#[deny(...)]`: the original five (`schema`,
`read_path`, `write_path`, `txn_coordinator`, `forwarding`) plus these five.

**`lib.rs` is ~11,800 lines** (down from ~17,300 before ADR 0061 rung C5
step 2 split `impl<E: Env> ClientCtx<E>` into `schema.rs`/`read_path.rs`/
`write_path.rs`/`txn_coordinator.rs`/`forwarding.rs`, below) — grep for the
symbol, don't scroll. It also holds
in-crate `#[cfg(test)] mod`s that need private handles the `tests/` tree
can't reach — e.g. `auto_split_median_tests` and `confirm_futility_tests`
(issue #268 — the confirm-loop fast-fail regression, needing a raw
`CpGroup` + the `pub(crate)` `ClientCtx::cp_kind_local`; `split_fence_tests`
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
`docs/engineering-lessons.md` (the same idiom `tests/split_build.rs::bring_up`
uses), or it panics `AddrInUse` under `cargo test --workspace` contention. A
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
  (13 methods — `cp_kind_write_item`, `cp_kind_write_raw*`,
  `cp_kind_local`, `poll_probe`, `cp_batch_local`/`cp_batch_propose`,
  `cp_put_local`/`cp_delete_local`, `seed_rows_local`/`seed_child_rows`);
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
  (`effective_metadata`, `metadata_fresh`, `data`/`data_opt`,
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
  `self.edge.leader_handle()`, `self.data_opt().map(|d| d.backup_store
  ...)`, one call into `dynamo::kind_write_item_at_leader` — nothing is
  decided here that wasn't already decided by an existing `ClientCtx`/
  `CpGroup`/`BackupStoreHandle` method. This is the seam that let five of
  the six leaf background loops (`ttl_reaper.rs`, `index_backfill.rs`,
  `backup_completion.rs`, `backup_janitor.rs`, `pitr_janitor.rs` — each
  now a thin wrapper into `animus-node`, see their own entries below) move
  without `ClientCtx` itself moving, which doesn't happen until rung C5.
- **`dynamo.rs`** (~59 KB) — the DynamoDB JSON-over-HTTP edge; the `GET /metrics`
  route (ADR 0015) shares this listener. `dispatch` also forwards a
  `DynamoDBStreams_20120810.*` target to `dynamo_streams::execute`
  (below) — the two services share one listener/port. **SigV4 enforcement
  (ADR 0057) lives in `handle_conn`, ahead of `dispatch`** — after the `GET
  /metrics` special case, before every other request reaches `dispatch`/
  `execute_routed`: when `ctx.dynamo_auth` is `Some` (a `dynamo_auth`
  cluster-config section, or `--dynamo-auth PATH` on a config-less startup
  mode), `ctx.env.wall_now()` supplies "now" (never `SystemTime::now()`,
  ADR 0051 discipline) and is handed straight into `animus_node::
  sigv4_gate(&request, credentials, now_epoch_ms)` (ADR 0061 rung C4b) —
  the build-`SigV4Request`-then-`verify` sequence itself is pure and lives
  in `animus-node` now; this connection handler does only the clock read
  and the response-shaping on failure. A verification failure short-
  circuits straight to a `400` with the AWS-faithful
  `com.amazon.coral.service#...` body (`sigv4_error_body`, rendered via
  `serde_json` rather than `WireError::to_json`'s DynamoDB-namespace
  prefix — a different `__type` namespace entirely). This gates the item
  API **and** Streams (both flow through `execute_routed`), and is
  deliberately **not** inside `execute_routed` itself: that function is
  also the admin dashboard's `POST /admin/data/dynamo` proxy's single
  dispatch point (ADR 0021), which must stay unauthenticated (ADR 0020's
  trusted-operator-network posture) — gating inside it would silently
  re-gate that surface too. `ctx.dynamo_auth: Option<Arc<BTreeMap<String,
  String>>>` — `None` (every existing config/test/deployment) skips the
  whole block, zero-cost and behavior-identical to pre-ADR-0057. `http.rs`'s
  `HttpRequest::headers` (every header, lowercased, repeats comma-joined —
  added by this same ADR) is what makes the `SigV4Request` buildable at
  all; a `SignedHeaders` list can name any header, not just the three this
  crate used to retain.
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
  internally-triggered `BeginBackup` for a PITR-enabled table, reusing
  Train 1's capture driver/aggregator completely unmodified, then tagging
  the row via `MetaCommand::MarkBackupPitrBase` with a self-healing sweep
  for a dropped tag ack) and `pitr_janitor_loop` (two-phase mark/reclaim
  over `Metadata::pitr_segments`, subject to the identical epoch-derivation
  guard `segment_janitor.rs` established for streams, plus a base-snapshot
  keep-anchor mark step that leaves the actual reclaim to the *existing*
  `backup_janitor_loop`, which already reclaims any `Expired`/`Failed`
  `BackupRow` regardless of a PITR tag). `DEFAULT_PITR_RETENTION`
  (35 days)/`DEFAULT_PITR_SNAPSHOT_CADENCE` (6h) are hardcoded production
  defaults — no CLI knob yet, the identical documented gap `ttl_reaper.rs`'s
  own sweep interval has. The module's own doc has the full design
  including the self-healing-tag residual and the control-only-leader
  scope gap. The fifth **consumer arm** itself (`pitr_tick`/`pitr_seal_now`)
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
  `stream_shards` catalog. The module's own 80-line `//!` doc has the full
  design (including the load-bearing epoch-derivation guard and the
  convergent drop-table cascade); see also `docs/streams-notes.md`.
  **Retired-tablet rule (ADR 0050 rung 6)**: a cutover-removed split
  parent's shards expire by ORDINARY retention — the drop-table
  retention-zero rule keys on the table's *schema* (still live via the
  children), never tablet presence, and the max-epoch pin applies to live
  tablets only (a retired chain can never seal again) — both halves
  red-proven in `tests/stream_janitor.rs::retired_parents_*`.
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
  populates. Touches only replicated `Metadata` (no `SegmentStoreHandle`/
  data role), so unlike the segment janitor it has **no** control-only-leader
  scope gap: a pure control-only leader drives the flip too. See the
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
  argument, same `!splitting` exclusion guard mirrored into both
  `split_driver_tick`/`inplace_split_driver_tick`'s own frozen-endgame
  final seal, but writing to `crate::BackupStoreHandle`/`Metadata::
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
  alongside its sibling DDL-relay tests.
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
  deliberately addressed" for the full disambiguation.
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
  call) rather than re-deriving `MetaCommand` proposals directly, while
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

**The split-build driver** (ADR 0050 Train B rung 4) is a
`change_consumer_loop` arm: for a `Splitting` parent this node leads, it
wakes the group, holds the quiesce veto, **holds trim** (metadata-derived —
the `!splitting` gates on the marker branch and `trim_janitor`; driver
liveness never gates it), bulk-copies BASE+LSI+FOOTPRINT (never
CHANGE/CURSOR) into the two `Building` children via
`ClientCtx::seed_child_rows` (local-or-`Forwarded{SeedRows}`, one confirm
implementation `seed_rows_local`, confirm-by-applied-index) — **one ship
per child, concurrently** (`ship_all`/`try_join_all`; `ship` returns its
row count rather than taking `&mut build.rows_shipped`, which is what used
to serialize them at the borrow checker). A failure cancels the sibling's
in-flight future, which is safe for the same reason a crashed driver is:
`SeedBatch` merges at carried versions, so the next tick re-ships as a
no-op (`tests/split_build.rs::split_survives_losing_one_childs_leader_mid_
build` kills a child's leader mid-build to prove it). Worth ~0.6s of a ~6s
build, not the ~2x the shape suggests — the ships are a minority of the
cost; **three full engine scans** (version-floor pre-pass, bulk scan, final
image) dominate a quiet build and are the named next win, then tails the
parent's change log by **packed-HLC watermark** (never a key-position
cursor — see the engineering-lessons entry) at token granularity (or full
prefix for sub-token raw keys). **The tail costs the DELTA, not the table
(2026-08-19 amendment):** its watermark starts at the parent's highest
change HLC as of the pre-bulk pass (captured beside `bulk_version_floor`,
under the same monotonicity argument) rather than at 0 — a zero start made
the first pass re-ship every row the bulk image already held — and it
batches rows per child across dirty units to the same `SEED_CHUNK_BYTES`
budget the bulk pass uses, instead of one `SeedBatch` (hence one consensus
round, plus a forwarded hop for an off-node child) per partition key.
Before the fix, a 20,000-row split spent ~6,000 no-op Raft entries per
child and ~85% of its wall clock re-copying; a child's `commit_index`
growth per row received is the batch-size meter that shows it, and is what
`tests/split_build.rs::split_build_tail_does_not_re_ship_the_bulk_image_
row_by_row` asserts on. Progress mirrors to
`ctx.data().split_builds` → `/admin/raftkv`'s
`split_rows_shipped`/`split_converged`/`split_phase`. **Rung 5 completes the
workflow**: at convergence — caught up, OR `SPLIT_MAX_TAIL_PASSES` (25)
post-bulk chasing passes elapsed (the rung-8 liveness bound: a
continuously-written parent must still freeze; see the engineering-lessons
entry) — the driver proposes `KvCommand::Freeze` on the parent (terminal
whole-range seal; USER data only — consumer bookkeeping writes stay
allowed so the vetoes below can converge, see the engineering-lessons
entry), then in ONE tick (rung 8: each phase-per-tick boundary was pure
write-blip): final tail drain to zero → the **final image** (a re-scan
ship of the frozen parent *filtered by the pre-bulk version floor* — txn
decisions/resolves are signal-less writes an O(delta) tail misses, and
apply order == HLC order makes every bulk-missed rewrite out-version the
floor; deliberately not `latest_version()`, which the read-ceiling marker
future-shifts; gated on the apply task reaching the freeze-window commit
floor) → streams final seal — **the pre-bulk floor costs its own full
engine scan and is deliberately NOT `group.engine_applied_index()`, a
Raft log index that is not the same value space as a row's packed-HLC
MVCC version** (ADR 0018 §2/PR2; see the "CP writes need no
client-assigned version" gotcha below and ADR 0050's 2026-08-19
"investigated and rejected" amendment — that substitution was considered
for this exact scan and found unsound both directions, not merely
unoptimized)
(`seal_now`, no size/age gate) → GSI-drain veto (`"gsi"` cursor ≥ max
pending record) → backfill veto (`MarkIndexBackfilled` for every
`Creating` index) → proposes `CutoverSplit` until the parent leaves the
map (the reconciler then `Reclaim`s it everywhere). **The GSI-drain veto is
a correctness gate, not a liveness heuristic** (cutover retires the parent
and the reconciler reclaims its engine outright — no drain-before-halt, see
`animus-cp-data/CLAUDE.md`'s "Superseded by ADR 0044" entry — so firing
cutover past an un-drained cursor would silently lose GSI updates forever):
its fix for slow convergence under a write flood (issue #288) is therefore
to accelerate the drain, never to bound or bypass the veto the way
`SPLIT_MAX_TAIL_PASSES` bounds the *build* phase's own chase (that bound is
safe only because the build's correctness never depended on the lag being
zero; this veto's does). Once the parent freezes the backlog this veto
watches is fixed, not growing, so `split_driver_tick`'s frozen endgame
drives the GSI drain to exhaustion in a tight loop right there
(`FROZEN_ENDGAME_GSI_DRAIN_MAX_PASSES`) instead of waiting on
`change_consumer_loop`'s own once-per-`INDEX_DRAIN_INTERVAL` call to make
progress — zero fairness cost against a static parent, and it survives a
transient propose failure under load without costing a full extra tick to
retry. See `docs/engineering-lessons.md`'s issue #288 entry for the general
"accelerate a correctness gate, never bound it" rule, and
`tests/split_build.rs::
indexed_put_item_unthrottled_flood_racing_the_split_converges_with_no_lost_gsi_updates`
for the regression (an unpaced flood racing a split, asserting both
convergence and that the post-cutover GSI reflects every acked write). A
stale-routed write
to a frozen parent gets the retryable `FROZEN_REFUSAL` from every local
write/txn helper (`frozen_refusal`; bookkeeping-only kind batches exempt).
**Both `ClientCtx::cp_kind_write_item` (the evaluated arm) and
`cp_kind_write_raw` (the fast/marker arm) retry this internally** (issue
#288: pre-fix, neither had a retry loop at all — every Dynamo/raw-
protocol write funnels through one of these two since ADR 0049's write-path
unification, so a write racing the freeze window got a terminal 500
instead of the write landing on the child a moment later), mirroring
`cp_read`'s deadline-bounded loop and re-resolving `cp_route` each attempt.
A `Building` child runs **no consumer arms at all**: it is structurally
unroutable (`topology::tablet_for_key` excludes it — its range overlaps
the still-serving parent's), so any cursor write keyed inside its own
range would land in the parent's scope and poison the parent's
min-over-rows watermark regardless of the cursor key's own encoding —
issue #355's fix (`cursor::cursor_key` embedding `range.start` verbatim)
closes a *different* misrouting (a fresh right child's own writes landing
on its left sibling post-cutover) and doesn't touch this one, since an
unroutable tablet has no routing candidacy to fix into. E2e:
`tests/split_build.rs` (full workflow + racing txns + post-freeze leader
kill).

**`SplitMode` (ADR 0058 Train 2 rung 3's `animusd`-level driver residue)
selects between the copy-based workflow above and the in-place one**:
`animusd::config::SplitMode` (`Copy`/`InPlace`; **`InPlace` is the default
since ADR 0058 rung 4 layer 2** — measurement showed it ~1.8× faster to
converge with no correctness gap, see that ADR's rung-4-layer-2 as-built
note — `Copy` was the default and every config/test's implicit behavior
before this layer, and stays fully selectable via `--split-mode copy`
pending its own deletion, not yet done), stored as a plain
`ClientCtx.split_mode` field — deliberately **not** a `ClusterConfig` field
(unlike `DynamoAuthConfig`, which has no other way to reach a `--config
FILE` process): this is threaded exactly the way `--auto-split`/
`--quiesce-after` are, as a CLI-parsed runtime parameter down the
`spawn_common_tail`/`start_with_growth`/`start_control_with`/
`start_data_with_growth` call chain, so adding it never touched
`ClusterConfig`'s struct-literal shape (which dozens of existing tests
construct directly). `--split-mode {copy,inplace}` threads through
`--config FILE --node I` and `--cluster N` only — the identical scope
`--quiesce-after` has, including the same documented gap for
`--cluster-control`/`--cluster-data` and the standalone
`control`/`data`/`join` subcommands (each always runs `SplitMode::
default()` = `InPlace`, no flag of its own to select `Copy`). A test that
specifically exercises the copy workflow's own mechanics (its
`Splitting`/`Building` intermediate metadata shape, its build/freeze/tail
driver, its own bench) must pin `SplitMode::Copy` explicitly rather than
relying on `SplitMode::default()`/`run_node` — see
`docs/engineering-lessons.md`'s rung-4-layer-2 entry for the audit and the
two test files this caught. `ClientCtx::trigger_split` is still the ONE
choke point both workflows share (see its own doc, `schema.rs`): `self.
split_mode` is the sole branch point between proposing `MetaCommand::
BeginSplit` or `BeginSplitInPlace`, the identical idempotent
already-`Splitting` handling, the identical confirm loop, and identical
F11 alignment — `auto_split_loop`/`admin::action_split`/
`ClientRequest::SplitTablet` all fall in behind whichever mode is
configured automatically, with no fork of their own. **Since ADR 0062
rung 4, the two branches' `children` computation itself is where they
diverge**: `Copy` still mints its two children at placement-chosen final
homes (`split_child_placement`/fork F5, unchanged); `InPlace` no longer
does — both children carry the parent's own current `replicas`, verbatim
and identical to each other, read from the same already-fetched `meta`
the confirm loop already holds. The wire *shape* of `children:
[(TabletId, Vec<NodeId>); 2]` is still identical between the two
`MetaCommand`s — only the values a proposer computes for it differ.

**The in-place cutover driver** (`index_drain.rs::
inplace_split_driver_tick`) is `change_consumer_loop`'s in-place sibling to
`split_driver_tick` above, selected per-tablet — not per-node — by
`Tablet::inplace_split.is_some()` (a durable fact of the tablet's own
`Metadata` row, set only by `BeginSplitInPlace`): a node can be configured
`InPlace` while driving a tablet someone else split with `BeginSplit`
before the flag flipped, and vice versa. Everything upstream of this —
proposing the single-entry `KvCommand::SplitTablet` fork itself (**since
ADR 0062, immediately, with no learner-add-and-wait phase to run first —
every replica the fork touches already hosts the parent as an ordinary
voter**) and materializing both children's engines on every fork
participant — is entirely `animus_cp_data::host`'s own reconciler (ADR
0058 Train 2 rung 3; fork-first per ADR 0062 rung 5, unmodified by this
driver); this function has nothing to do until `CpGroup::pending_split()`
answers `Some` on this replica. Once forked, there is no build, no freeze,
no tail, no convergence bound — the atomic mint already fully formed both
children — so what is left is exactly the copy-based endgame's own two
pre-cutover vetoes (GSI-drain, accelerated identically via
`gsi_caught_up`/`FROZEN_ENDGAME_GSI_DRAIN_MAX_PASSES`; backfill-seeder) run
against the parent's own (now-frozen, static — `SplitTablet` reuses
`Freeze`'s exact whole-range seal discipline) change log, plus the
**streams final seal** anchored at the fork position (`seal_now`, looped
to exhaustion — identical call the copy-based endgame makes, closing the
shard so an in-flight streams iterator drains the parent shard and walks
on to the children via `split_lineage`), before proposing
`MetaCommand::CutoverSplit` with the identical confirm-by-observation loop
(re-issued every tick until the parent vanishes from the map). Fully
idempotent across crash/re-lead with **no driver-local state at all**
(stricter than `SplitBuild`, which memoizes real progress): every check is
a fresh read off durable/replicated state.

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
`tests/inplace_split_e2e.rs` — a real 3-node `--split-mode inplace`
cluster, a paced continuous writer riding kickoff through cutover
(asserting every acked write survives with its exact value, and observing
zero write refusals across every run once the fix landed), and a
streams-enabled variant walking a `GetRecords` iterator from the parent's
own shard 0 across the fork to both children's own shard 0s with no loss
or duplication.

`--auto-split K` (key count), `--auto-split-bytes B` (byte size), and
`--auto-split-change-rate RATE` (streamed tables only, ADR 0042 §14 Fork F —
bytes/sec of a tablet's own `KIND_CHANGE` growth, `/admin/metrics`'s
`stream_change_rates`) are independent OR-gated triggers — any combination,
or none. `--auto-split-change-rate` closes the gap the other two
structurally can't: `CpGroup::approx_bytes` is base-scoped (ADR 0034), so a
high-churn, small-footprint streamed table never crosses a byte/key
threshold regardless of write rate. No production-tuned default exists yet
— omitting the flag disables the trigger entirely (zero behavior change);
an operator must pick `RATE` for their own workload. All three flags are
`--cluster N`/`--cluster-control`+`--cluster-data` dev-cluster-only (not
reachable from `--config/--node`'s real per-process deployment, matching
the two older flags' own existing scope). **`--node I` is gone from
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
`cp_kind_local`/`cp_batch_local`** — it prefers `animus-cp-data`'s
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
is load-bearing, not academic: `kind_write_item_at_leader` reads `old` under
`ctx.data().rmw_lock`, which is released *before* proposing (issue #285), so
two concurrent evaluators of the same key can read the identical stale `old`
and compute byte-identical `new` (a pure function of `(cur, delta)`) —
nothing downstream disambiguates them. Each proposes an entry carrying those
same bytes guarded by an own-key OCC seatbelt keyed to that same stale `old`;
the first to apply wins the seatbelt and writes the bytes, every later one
legitimately `ConditionFailed`s. In the window after the winner's bytes are
visible and before a *loser* entry's own outcome is recorded, the old
ungated fallback matched on value alone and returned `Confirmed` for the
loser — acking an increment that never applied. Fixed by threading
`ProbeIdentity` (`ValueProves`/`RequiresOwnEntry`) down from whichever caller
already knows `dynamo::kind_write_is_idempotent`'s answer
(`kind_write_item_at_leader` for `cp_kind_local`; `cp_batch_local` hardcodes
`ValueProves` since the raw `Batch` command only ever carries Put semantics,
never `ADD`) — `poll_probe` never recomputes idempotency itself. A
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
wrapper, and every **tablet-id-addressed** internal RPC (`SeedRows`,
`ForceSeal`, `TriggerAutoSplit`, `ClearBackfillCursor`, `StreamHotRead`) calls
it directly): a "not the leader here" refusal carries the refusing node's own
leader hint (`topology::format_not_leader_refusal`, a plain string suffix so old
and new binaries interoperate); the chase retries at the hint if
untried, else at another of the tablet's known replicas, bounded to one pass over
{hint} ∪ replicas within the overall `CLIENT_TIMEOUT`. The tablet-addressed
RPCs used to relay once and re-resolve from scratch instead — which never
converges when the calling node hosts **no replica** of the target tablet
(the fallback deterministically re-picks the first metadata replica; the
split driver seeding an off-node fork-F5 child spun on exactly this forever,
parking the split — see `docs/engineering-lessons.md`). A new tablet-addressed
RPC must forward through this choke point, and its test suite needs a caller
hosting no replica of the target, which only a cluster larger than RF can
produce (`tests/split_build.rs::
split_completes_when_a_child_lives_off_the_parent_leader_node`).

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
liveness-checked), a caller that keeps re-resolving and re-forwarding
(the split-build driver's own per-tick retry, `seed_child_rows`) kept
reproducing the identical dead end forever once its first guess or hint
chase happened to land on a node that had since died — the confirmed root
cause of `tests/split_build.rs::
split_survives_losing_one_childs_leader_mid_build`'s reported hang. Fixed
by giving a transport failure the identical "no hint" treatment a
live-but-mid-election refusal already gets (try another known replica),
rather than a terminal return. Regression:
`forward_transport_failure_tests::
forward_to_tablet_leader_survives_a_dead_first_guess` (`lib.rs` — **not**
beside `forward_to_tablet_leader` in `forwarding.rs`, whose module carries
a hard `#[deny(clippy::disallowed_methods)]`, ADR 0061 Phase C's closing
rung, that a real-socket test's `tokio::time` calls would trip). See
`docs/engineering-lessons.md`'s matching entry for the full incident,
including why this sandbox could never reproduce the original hang live
(fast localhost + a small dataset's bulk pass usually outracing the
test's own 30s victim-detection poll) yet the fix was still provable
red-before/green-after via a fully deterministic isolation of the exact
mechanism.

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
own local branch and `cp_serve_forwarded`'s `TxnPrepare` arm — evaluates
every `pending_kind_writes` entry there (`dynamo::eval_kind_txn_write`,
mirroring `kind_write_item_at_leader`'s own U3 shape) under the identical
`ctx.data().rmw_lock`, merging the result into `writes` immediately before
staging; a mandatory own-key OCC condition rides alongside (Fork C1). For a
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
`CpGroup::approx_key_count` (LSM-only) **and** `CpGroup::approx_bytes` (either
backend). The split point matches the metric: a byte-configured cluster splits at
`byte_weighted_median` (private to `lib.rs`, unit-tested in
`auto_split_median_tests`) — which scans every achievable key-boundary cut for the
one closest to half the bytes, not a single accumulate-and-threshold pass (subtly
wrong when one key dominates; see the root log). Key-count clusters keep the plain
positional median. **Tablets are split-only (ADR 0044)** — there is no
merge, automatic or operator-driven, to trigger; a tablet's count only ever
grows, and reversing an over-eager split is no longer possible (see that
ADR's "shrink-in-place" note).

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

**Split is the ADR 0050 copy-based workflow's METADATA half (Train B rung
3)**: `trigger_split` — still the one choke point every surface calls —
now proposes `MetaCommand::BeginSplit` (parent → `Splitting`, still fully
serving; two `Building` children minted at **placement-chosen final
homes**, fork F5 via `split_child_placement` →
`animus_control::select_replicas_balanced`, falling back to inheriting
the parent's replicas when the recorded RF exceeds the live member count —
the same self-heal-later stance `provision_tablet` takes) and confirms by
observing the parent's own **state** become `Splitting` — never the old
epoch-advance (a rebalance CAS also bumps the epoch; a stray bump re-arms
the CAS instead). Kickoff is **asynchronous and idempotent**: success
means the workflow *started*; a `Splitting` parent returns success
immediately; a `Building` child refuses ("not splittable"). Routing
serves only `is_routable()` tablets (`Building` children overlap their
un-narrowed parent, so the `tablet_for_key`/scan-fan-out filters are
load-bearing); auto-split skips non-`Active` tablets; placement
(reconcile + rebalance) is frozen for the whole mid-split set. **The
workflow STOPS at this rung** — no driver/copy/freeze/cutover callers yet
(B4/B5), so a started split parks at parent-`Splitting` +
children-`Building` indefinitely; per-tablet `state` rides
`/admin/status`'s serialized `Metadata` (the split-status surface —
no new endpoint). The old zero-copy `MetaCommand::SplitTablet` is deleted
(Train B rung 7) — test topology fixtures propose a real
`BeginSplit`+`CutoverSplit` round instead (sound on an EMPTY table:
children activate over nothing, the parent retires). E2e:
`tests/split_lifecycle.rs` (3-node, follower-connected kickoff = the
`BeginSplit` relay regression) and `admin_endpoint.rs::
admin_split_kicks_off_the_copy_based_workflow`.
(Merge — `MetaCommand::MergeTablets` and the reconciler's `WidenScope`/
`Absorb` reaction — was removed entirely by ADR 0044, superseding ADR
0033.)

**`ClientCtx::trigger_split` is the ONE choke point every split proposer
calls** (`auto_split_loop`, `admin::action_split`, and
`ClientRequest::SplitTablet`'s handler — nothing else ever builds a
`MetaCommand::BeginSplit`), which is where F11 (ADR 0042 §14) rounds a
streamed table's split key down to its own 8-byte token boundary
(`align_split_key`, private to `lib.rs`, unit-tested in
`align_split_key_tests`) — a manual split can no longer separate one
partition's records across sibling tablets the way it could before growth
PR2 moved the rounding out of `auto_split_loop` alone.
`MetaCommand::BeginSplit`'s own apply arm independently re-checks token
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
  `--config`/`--node` (`run_node_with_streams_and_quiesce_after` →
  `BoundNode::start_with_growth`) and `--cluster N`
  (`start_cluster_with_growth_and_quiesce_after`) — **defaults ON at 5s**
  (`main::DEFAULT_QUIESCE_AFTER_SECS`; `0` disables). See that constant's
  own doc and ADR 0048's Consequences section for the evidence behind this
  default and what was *not* separately validated (a large fleet under
  sustained mixed load with real inter-process latency) — a
  maintainer-reviewable call, not a settled fact. Not yet wired for the
  `--cluster-control`/`--cluster-data` split-deployment dev path or the
  standalone `control`/`data`/`join` subcommands (documented gaps in
  `main.rs`'s own module doc).

Tests: `index_drain.rs`'s own `stream_sealer_tests` module (in-crate, needs
private `CpGroup` access) covers the veto end to end
(`hot_backlog_holds_the_quiesce_veto_until_the_hot_tail_trims`) and the
sweeper-skip regression
(`a_rewoken_tablet_is_picked_back_up_by_every_sweeper_within_one_interval`);
 `tests/cp_quiescence.rs` is the critical
`ProdEnv` leader-kill liveness regression
(`write_after_leader_kill_of_a_quiesced_group_converges`) — the one
property `SimEnv` structurally cannot prove.

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
  `KvCommand::KindBatch` Raft entry (`kind_writes_for_item`) — but the *diff*
  is now evaluated **at the tablet's own leader**, not at the receiving edge
  node: `ClientCtx::cp_kind_write_item` routes a `ClientRequest::
  KindWriteItem { table, pk, sk, op: KindWriteOp, condition }` to the leader
  (in-process if local, one forwarded hop via `cp_serve_forwarded` if not),
  and `dynamo::kind_write_item_at_leader` — the only caller of
  `kind_writes_for_item` — reads its own `old` image, evaluates `condition`,
  computes `new` from `op` (`Put`/`Delete`/`Update{key_item, actions}`, the
  last folding `UpdateItem`'s base-value RMW into the same mechanism), then
  proposes. **This is the ADR 0046 ("the tablet log model", draft PR #222)
  U3 fix**: `index_aware_write`'s prior edge-evaluated design (now deleted)
  read/diffed under a **node-local** `ctx.data().rmw_lock`, so two edge
  nodes writing the same item never contended on the same lock and could
  both diff against the same stale `old` — the loser's stale LSI row
  orphaned forever (nothing reconciles it; only the GSI drain self-heals).
  Locking `rmw_lock` **at the leader** instead serializes every write of one
  item regardless of which edge node received it, since every write now
  funnels through the same function on the same node. A `KindBatch.
  conditions` OCC seatbelt (PR1, `animus-cp-data`) closes the one residual
  the lock alone can't: a `txn_resolver_loop` recovery push never takes
  `rmw_lock` — real now that `TransactWriteItems` participates on these
  tables too (see below). **`rmw_lock` is scoped to read+evaluate only
  (issue #285)** — `kind_write_item_at_leader` drops it before proposing,
  mirroring `txn_stage_local`'s identical scoping just below; it used to
  span the whole `cp_kind_local` propose+confirm-poll too, so one item's
  slow confirm (apply backlog) stalled every *other* evaluated write on the
  node behind it, not just racing writers of the same item — the seatbelt
  above is what actually keeps concurrent writers of one item safe, and it
  already has to work lock-free for the `txn_resolver_loop` case, so the
  lock never needed the wider span for correctness. The regression test for
  this (`confirm_futility_tests::an_unrelated_evaluated_write_is_not_
  stalled_behind_another_writes_confirm_wait`) needs write A's propose+
  confirm phase to reliably still be running when an unrelated write
  returns — racing a real apply backlog against real time to manufacture
  that was itself load-sensitive (a starved flood can fail to build any
  backlog, so the "slow" write finishes first; see
  `docs/engineering-lessons.md`'s Testing section, 2026-08-22 entry), so it
  now uses `dynamo::rmw285_confirm_gate`, a `#[cfg(test)]`-only hook that
  holds that phase open for a fixed delay under the test's own control.
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
  takes for the namespace split. `BackupStoreHandle` (`DataRole::
  backup_store`, alongside `DataRole::segment_store`) is threaded through
  combined (`BoundNode::start_with_growth`) and data-only
  (`BoundDataNode::start_data_with_growth`) node assembly — **never**
  control-only (`BoundControlNode::start_control_with` takes no such
  parameter at all), the identical "no data role, no `SegmentStoreHandle`-
  shaped handle" gap the streams segment janitor already documents for ADR
  0043 §A9, inherited rather than fixed here. **Consumed since Train 1
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
  `data --config`/`data --seed`/`control`/`join`/`--cluster-control`+
  `--cluster-data` all default to `BackupStoreConfig::Cluster` internally —
  no CLI flag reaches any of them, the identical documented gap
  `--segment-store` already has on those same entry points.
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
  report-count growth, proposes `MetaCommand::FailBackup`. **Inherits the
  identical control-only-leader scope gap `segment_janitor.rs` already
  documents**: failing needs only `Metadata` (a control-only leader can do
  it); completing needs a `BackupStoreHandle`, which no control-only node
  provisions (`ClientCtx::data_opt() == None` there) — spawned on combined
  and control-only nodes (never data-only, which never becomes control
  leader at all).
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
  lessons.md` for the full note. **Inherits the identical control-only-
  leader scope gap** `segment_janitor.rs`/`backup_completion.rs` already
  document (object reclaim needs a `BackupStoreHandle`, which no
  control-only node provisions) — spawned on combined and control-only
  nodes, never data-only.
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
  **Known limitation carried forward, not this loop's to fix** (issue
  #513): the loop's own convergence predicate is correct, but the
  `reconfigure_step` convergence it observes can itself oscillate
  indefinitely for a two-(or-more)-replica-difference target — so a child
  whose fresh `select_replicas` target differs from its fork-inherited
  replicas by more than one replica may never reach `done` at all, not
  just slowly.
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
- **`ClusterEdgeState` is scoped to one NODE** (ADR 0031 PR2), created fresh per
  node — even in `--cluster N`, which previously shared one instance across the
  cluster and masked cross-process bugs. Holds this node's own control handle, its
  hosted CP group handles (keyed by tablet), and the DynamoDB `SchemaRegistry`.
  No process-global (`OnceLock`) mutable state.
- **`ClientCtx.data: Option<DataRole>`** groups the data-role-only fields
  (`rmw_lock`, `raftkv_metrics`, `base_id`). `ClientCtx::data()` **panics** if
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
- **`DataRole`'s `SegmentStoreHandle`/`BackupStoreHandle`.** Both hardcode
  `FsSegmentStore`/`ClusterSegmentStore<ProdEnv, FsSegmentStore>`
  regardless of the enclosing `ClientCtx<E, R>`'s `E` (`animus-env`'s
  `prod` feature is unconditionally on for this crate, so `FsSegmentStore`
  genuinely exists here — the blocker isn't C0's feature gate, it's that
  neither handle type takes an `E` parameter at all). A real `DataRole`
  therefore cannot be built generically today. Not exercised by this rung:
  neither `cp_kind_write_raw` nor `cp_get` ever calls
  `self.data()`/`self.data_opt()` (verified by reading both call chains,
  not assumed), so `data: None` is sufficient to prove this rung's claim.
  Any future extension that needs a real `DataRole` under `SimEnv` (the
  DynamoDB-shaped `cp_kind_write_item` write path, the TTL/backup/stream
  loops) hits this blocker first.

**Not yet generic, also worth knowing before extending this harness**:
`handle_request` and `dynamo::marker_batch_write_raw`/
`kind_write_item_at_leader`'s *callers* in `dynamo.rs` stay hardcoded to
`&ClientCtx` (`E = ProdEnv`) — only the five split modules
(`schema`/`read_path`/`write_path`/`txn_coordinator`/`forwarding`) and a
handful of `lib.rs`-resident crate-wide accessors
(`effective_metadata`/`data`/`data_opt`/…) are `E`-generic today. Driving
the DynamoDB wire-shaped write path (`cp_kind_write_item`, which *is*
already `E`-generic in `write_path.rs`) under `SimEnv` is reachable in a
follow-on rung once a `SimEnv`-safe `DataRole` exists; driving
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

## Tests

`cargo test -p animusd` — every test in `tests/` is a real-socket `ProdEnv`
integration test that polls with timeouts, not a deterministic assertion;
`simenv_client_ctx_tests` (above) is this crate's one `SimEnv`-driven
exception, and lives in `lib.rs` rather than `tests/` for exactly the
private-handle reason its own section gives. The restart tests run both
incarnations in the same runtime,
calling `Node::shutdown()` between them. In-crate `#[cfg(test)] mod`s
(`auto_split_median_tests`, `confirm_futility_tests`) live in `lib.rs` itself
because they need private handles (a raw `CpGroup`/the private
`byte_weighted_median` helper/the `pub(crate)` `ClientCtx::cp_kind_local`)
that no external `tests/` file can reach;
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
split-cluster bring-up).

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
`tests/inplace_split_bench.rs`/`tests/split_build.rs`'s bounded-retry
port-TOCTOU idiom. Workload knobs: `ANIMUS_BENCH_NODES` (3),
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
noise floor (the same reason `tests/inplace_split_bench.rs`/
`split_build.rs`'s own benches are `#[ignore]`d rather than part of the
default `cargo test` run). Run it locally when you need a number, not as
a gate. **Numbers are comparable only to another run on the same host, in
the same session** — never across machines or sessions, per
`docs/engineering-lessons.md`'s "a historical bench figure from a
different host is not a baseline" entry: if you need to compare against
an earlier figure, rerun the earlier configuration alongside the new one
on this same host rather than trusting a number quoted from elsewhere.
