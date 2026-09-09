//! The DynamoDB-wire cycles/durability corpus (ADR 0061 rung D2 PR 2, C-04
//! D2 step 3): the actual end-to-end wire corpus D2 PR 1
//! (`sim_cluster_dynamo.rs`'s own module doc) named as its own remaining
//! work — every op issued as a real DynamoDB JSON request through
//! [`SimClusterHandle::dynamo`], decoded back into the shared
//! [`animus_test::history`] `Mop`/`History` model so
//! `check_cycles`/`check_durability`/`check_convergence` run **unchanged**,
//! mirroring `sim_cluster_corpus.rs`'s own architecture exactly (see that
//! file's module doc for the shared design this one reuses verbatim: the
//! `SimClusterHandle`-over-`&self` split that makes a concurrent workload
//! possible at all, the single-writer-per-key discipline, the
//! `Key`-embeds-table-index convention for `two_tables`, and the
//! converged-or-timeout durability/convergence poll).
//!
//! # The list-append mapping: `UpdateItem`'s `list_append`, not a
//! client-tracked list
//!
//! Unlike `sim_cluster_corpus.rs` (which tracks each client's own list
//! locally and `put`s the whole encoded list on every write — sound only
//! because a raw `put` is a plain, idempotent overwrite), this corpus's one
//! write mechanism is a **server-evaluated** `UpdateItem`:
//!
//! ```text
//! SET items = list_append(if_not_exists(items, :empty), :v)
//! ```
//!
//! with `:v` a one-element list holding this write's own globally-unique
//! value (`Shared::fresh_value`) and `:empty` an empty list, so the very
//! first write to a key needs no separate provisioning step
//! (`if_not_exists` supplies the seed). This single expression is both the
//! `SET` clause and the `list_append` function call the task brief asks
//! for, **and** it is the corpus's genuinely non-idempotent operation: the
//! server reads the item's *current* list at apply time and appends to it
//! — a retried/duplicated apply of the same entry would append the same
//! value twice, unlike a plain `Put`/`SET x = :v`, which a duplicate apply
//! leaves byte-identical. No separate `ADD` op is needed to get this
//! property; `list_append` already has it by construction, which is why
//! the checker's `info`-not-`fail` discipline for an indeterminate outcome
//! genuinely matters here (see `run_write`'s own doc).
//!
//! # The read-consistency modeling decision (ADR 0055)
//!
//! `GetItem`/`Query`/`Scan` all decode `ConsistentRead`, and this corpus
//! issues both values. **`ConsistentRead: true` reads feed the shared
//! `Recorder`/`History` `check_cycles` runs against** — a linearizable
//! ReadIndex read is a real, ordered observation, exactly the discipline
//! `sim_cluster_corpus.rs`'s own read helper documents ("a read that
//! verifies a write must ask for `ConsistentRead: true`",
//! `crates/animusd/CLAUDE.md`'s ADR 0055 testing-gotcha entry).
//! **`ConsistentRead: false` reads are deliberately EXCLUDED from
//! `check_cycles`'s history** — feeding a replica-local, un-barriered
//! observation into the same `wr`/`rw` graph a linearizable read builds
//! would manufacture false-positive "divergence" violations the instant a
//! stale-but-legal read landed during a fault window, exactly the trap
//! `check_cycles`'s own `recover` doc names for a workload-modeling
//! mismatch (a legitimately weaker read is not the same defect class as a
//! forked/stale *strong* read). Instead, every `ConsistentRead: false`
//! observation is recorded separately
//! (`Shared::eventual_reads`) and checked directly against the scenario's
//! own **converged final state** once the fault schedule has healed and
//! drained: `check_eventual_reads_are_prefixes` asserts each observed list
//! is a prefix of the final state — sound under this corpus's
//! single-writer-per-key discipline, since a lagging/un-barriered replica
//! can only ever have observed an *earlier* state of the one writer's own
//! strictly-ordered commit sequence, never a value out of order or a value
//! that never committed. This is the "record them with the checker's own
//! weaker-read flag" fork the task brief poses, resolved as "no such flag
//! exists on the shared, cross-crate `check_cycles` — exclude and check
//! against convergence instead," the identical choice
//! `sim_cluster_corpus.rs` already made for `delete` (see that file's own
//! "delete is exercised, but deliberately kept OUT of the Elle model"
//! section) — a workload-shape exclusion from a checker whose model
//! doesn't fit, not a defect in either the workload or the checker.
//!
//! # `DeleteItem`/`BatchWriteItem`: exercised, kept out of `check_cycles`
//!
//! Both share `sim_cluster_corpus.rs`'s own `delete` reasoning: a
//! tombstoning `DeleteItem` cannot satisfy the list-append prefix
//! invariant (a deleted-then-reappended key's later reads are legitimately
//! not a superset of an earlier one), and a `BatchWriteItem` `PutRequest`
//! is a **whole-item overwrite**, not an append — feeding either into the
//! shared list-append history would manufacture the same false-positive
//! divergence the module doc above already explains for eventual reads.
//! Both get their own **direct** correctness probes instead
//! ([`run_delete_probe`]/[`run_batch_write_probe`]), run from every node in
//! the cluster in turn after the scenario's fault schedule has healed and
//! drained — mirroring `sim_cluster_corpus.rs`'s own `run_delete_probe`
//! exactly, on dedicated key namespaces (`delete-probe-*`/
//! `batch-probe-*`) disjoint from the list-append model's own
//! `part-{0,1}`/`item-{k}` keys, so neither probe's writes can ever be
//! mistaken for (or corrupt) a modeled key.
//!
//! # Multi-key reads: `Query`/`Scan` feed the SAME history, as multiple
//! `Mop::Read`s per transaction
//!
//! Every table's items live under exactly [`PARTITIONS`] fixed partition
//! keys (`part-0`/`part-1`), each holding every item whose logical key
//! hashes to it — so a base-table `Query` (`KeyConditionExpression: pk =
//! :p`) returns a real, strict subset of the table (proving
//! `dispatch_item_op`'s base-table `Query` arm, not merely a synonym for
//! `Scan`), and a `Scan` returns the whole table. Both are always issued
//! `ConsistentRead: true` (Query/Scan's own consistency-mode coverage is
//! not this corpus's concern — `GetItem`'s dedicated true/false split
//! above already covers that dimension) and decode every returned item's
//! own `items` list into one [`Mop::Read`] per item, all recorded as one
//! history entry (`Recorder::ok` already takes a `Vec<Mop>` for exactly
//! this shape) — `check_cycles`'s `wr`/`rw` edges fall out unchanged for a
//! multi-key read exactly as they do for `raftkv_linearizable.rs`'s own
//! transactional workloads.
//!
//! # `TransactWriteItems`/`TransactGetItems` (ADR 0061 rung F, C-06 PR 4)
//!
//! `execute_item_op_as`'s `Operation::TransactWriteItems`/
//! `Operation::TransactGetItems` arms (wired in by C-06 PR 3,
//! `sim_cluster_dynamo_transact.rs`'s own module doc) mean this corpus's
//! `client_loop` can now issue both through the real wire, folding them into
//! the SAME fault-injecting cell matrix every other op already rides —
//! `sim_cluster_dynamo_transact.rs`'s own 7 scenarios prove Transact
//! correctness on a clean or single-crash cluster; this corpus proves it
//! survives the identical `leader_crash`/`follower_crash`/`stop_restart`/
//! `leader_partition`/`split_brain`/`forward_heavy`/`two_tables` matrix every
//! other operation in this file already does, including a cross-table
//! transaction racing a fault that only touches ONE of its two participant
//! tablets (`two_tables`'s own `Nemesis::apply` still only ever targets
//! `primary_tablet` — table 0's — so a transact op spanning t0+t1 during that
//! cell's fault window is a genuine partial-participant-crash exercise
//! `sim_cluster_dynamo_transact.rs` never constructs).
//!
//! **`TransactWriteItems`: two owned keys, `Update`+`list_append`, plus an
//! always-passing `ConditionCheck`.** [`run_transact_write`] draws its two
//! write keys from the SAME client's own `owned` set (never a second
//! client's key — the single-writer-per-key discipline this file's own
//! module doc already establishes for [`run_write`], preserved here across
//! BOTH write mechanisms) via [`distinct_pair`], so a transact write's two
//! `Update` actions are exactly two more `SET items = list_append(..)`
//! calls — the identical genuinely-non-idempotent expression [`run_write`]
//! uses — each contributing one [`Mop::Append`], recorded as ONE
//! `invoke`/`ok` pair (`Recorder::ok` already takes a `Vec<Mop>` for exactly
//! this shape, the same technique [`run_query`]/[`run_scan`] already use for
//! a multi-key read). **Atomicity is therefore checked by construction**: a
//! transaction's two appends only ever enter the shared history TOGETHER, as
//! one atomic entry — there is no way for `check_cycles` to observe one half
//! landing without the other. A third action, `ConditionCheck` against a
//! table-scoped `__guard__` marker item (seeded once per table, before the
//! workload starts, and never modified again —
//! `attribute_exists(pk)`, always true), exercises `ConditionCheck` in every
//! random transact write without EVER cancelling one — a deliberate choice
//! (see Residuals below for why the FAILING case needs its own dedicated,
//! deterministic proof instead). Roughly a third of writes from a client
//! with at least 2 owned keys go through this path rather than plain
//! `UpdateItem`; about half of those also carry a fresh,
//! call-unique `ClientRequestToken` (never reused within the randomized
//! workload — a genuine reuse is a distinct workload shape, covered by
//! [`run_transact_probe`] below), exercising the token-preflight code path
//! under this cell's own fault schedule without changing the write's
//! observable semantics.
//!
//! **`TransactGetItems`: two keys from the FULL keyspace (any table, any
//! client's key), always feeding the shared history.** Unlike `GetItem`,
//! real DynamoDB gives `TransactGetItems` no `ConsistentRead` parameter — it
//! is unconditionally the strong, quiescence-confirmed snapshot
//! (`dynamo.rs::run_transact_get`'s own doc) — so, unlike [`run_get`]'s
//! true/false split, EVERY `TransactGetItems` observation feeds
//! `check_cycles`, decoded into two [`Mop::Read`]s recorded as one atomic
//! entry via [`run_transact_get`]/[`distinct_pair`]. **Isolation with
//! respect to every other op in this file is checked for free**: a
//! `TransactGetItems` entry lands in the exact same `Recorder`/`History`
//! every `UpdateItem`/plain `GetItem(ConsistentRead: true)`/`Query`/`Scan`
//! op already feeds, so `check_cycles`'s wr/ww/rw graph reasons about all of
//! them together — a transact read observing a torn or stale pair relative
//! to a concurrent single-item write would surface as an ordinary cycle,
//! exactly like a plain multi-key `Query`/`Scan` already would (this file's
//! own "Multi-key reads" section above). A non-200 response (including the
//! retryable `TransactionCanceledException` a snapshot that never quiesces
//! under contention can return) is recorded `info`, mirroring [`run_get`]'s
//! own "an op that returns no observation carries no information either
//! way" discipline — never `fail`.
//!
//! **[`run_transact_probe`]: the deterministic, dedicated proof for what the
//! randomized workload above cannot safely produce.** Run from every node in
//! the cluster in turn, after each scenario's fault schedule has healed and
//! drained (mirroring [`run_delete_probe`]/[`run_batch_write_probe`]'s own
//! placement and per-node loop exactly), on a `transact-probe-{node}-*` key
//! namespace disjoint from every other key this file's probes/model use:
//! (1) a transaction whose `ConditionCheck` genuinely FAILS (a per-node
//! guard item seeded to exist, checked with `attribute_not_exists`) —
//! asserts `TransactionCanceledException` with per-action
//! `CancellationReasons` and, critically, that NEITHER of the transaction's
//! `Put`/`Update` actions landed, proving the all-or-nothing half of
//! atomicity the randomized workload's always-passing guard can never
//! exercise; (2) a transaction mixing `Put`+`Delete`+`Update` (spanning
//! BOTH tables when the cell has two) with a passing `ConditionCheck` and a
//! `ClientRequestToken` — asserts all three actions landed together; (3) an
//! identical retry of (2)'s exact request under the SAME token — asserts
//! the cached outcome (no double `ADD`, the `Delete`d key stays deleted, not
//! resurrected) — the idempotency property the randomized workload's
//! always-fresh tokens never test; (4) the SAME token with a genuinely
//! different payload — asserts `IdempotentParameterMismatchException`.
//!
//! **Residuals specific to Transact (why these are dedicated-probe-only,
//! not part of the randomized Elle-model workload)**: a `Delete` action
//! inside a random transact write would tombstone one of the SAME two owned
//! keys the corpus's list-append model tracks, breaking the prefix
//! invariant [`check_eventual_reads_are_prefixes`] and every plain
//! `DeleteItem`/`BatchWriteItem` already stays out of `check_cycles` for
//! (this file's own "`DeleteItem`/`BatchWriteItem`" section above) — the
//! identical exclusion, extended to a third write shape; and a genuinely
//! REUSED `ClientRequestToken` (as opposed to the randomized workload's
//! always-fresh ones) is a fundamentally different, non-repeatable workload
//! shape no per-round random draw can safely produce without either
//! silently skipping the transaction's own effects on a cache hit
//! (undercounting `ok_writes`) or manufacturing a value-reuse hazard
//! `animus-test/CLAUDE.md`'s own "every appended element must be globally
//! unique" rule forbids. Both are instead proven deterministically by
//! [`run_transact_probe`] above, under the identical per-scenario fault
//! schedule and post-heal/drain placement as every other probe in this
//! file — not a weaker proof, a differently-shaped one, exactly like
//! `DeleteItem`/`BatchWriteItem`'s own existing probes.
//!
//! # Corpus-fixture findings from this rung (all fixed; none is a product bug)
//!
//! Extending the workload with Transact ops surfaced three real,
//! deterministic `SimCluster`/`ClientCtx` fixture defects — every one
//! classified (a)/(b) per the task's own found-bug protocol as **(b)**: a
//! corpus/fixture staleness or model gap, never a defect in the transact
//! protocol itself (each was root-caused to a specific fixture mechanism,
//! not left as an unexplained flake).
//!
//! **Finding A — a stale `SimClusterHandle::replicas_of` snapshot
//! (fixed via [`live_replicas`]).** A tokened `TransactWriteItems`
//! auto-provisions the internal `__animus_txn_idempotency` table, which
//! shifts every node's total hosted-tablet count enough to trigger a real
//! `rebalance_placement` move of this corpus's own modeled table on the
//! `dynamowire_forward_heavy` cell (4 nodes, RF 2) — a move
//! `SimClusterHandle::replicas_of`'s creation-time-frozen snapshot never
//! reflects. `run_scenario`'s durability check was reading an empty,
//! no-longer-a-replica engine and reporting every acknowledged write
//! against it as lost. Fixed by replacing every `replicas_of` read this
//! file's own checks depend on with [`live_replicas`], a live
//! `SimCluster::hosted_tablets` query. See that function's own doc for the
//! full incident.
//!
//! **Finding B — a same-tablet ("self-transaction") abandoned
//! `TransactWriteItems` permanently masked under `local_get`'s raw-peek
//! semantics (fixed via [`force_resolve_all_keys`]).** `SimCluster` never
//! spawns `animusd::txn_resolver_loop`, and the plain-`GetItem` local-read
//! path never calls `confirm_or_push` for a LOCAL (same-tablet) `Pending`
//! intent — only a foreign one — so a transaction abandoned mid-flight by
//! a fault landing on its own single tablet had no path to resolution in
//! this fixture at all, and this file's own raw-`local_get`-based
//! durability oracle read the masked, pre-intent value as "lost." Fixed by
//! issuing one covering `TransactGetItems` after drain
//! ([`force_resolve_all_keys`]) — `TransactGetItems`'s own read primitive
//! resolves a local intent exactly like a foreign one, so it pushes any
//! outstanding transaction to its real decision as a side effect. See that
//! function's own doc for the full incident (seed `13022590114329469744`,
//! `dynamowire_leader_crash_s22`).
//!
//! **Finding C — `SimCluster::restart` never told a restarted node's own
//! `ClusterEdgeState` about its fresh control handle (fixed via
//! `ClusterEdgeState::replace_control`, `lib.rs`).** `ClientCtx::
//! propose_schema`'s local-propose fast path reads `ctx.edge`'s own
//! `control` registry, not `ctx.control` directly — two different fields
//! with different lifecycles (`animusd/CLAUDE.md`'s `ControlHandle`
//! entry). `restart` rebuilt `ctx.control` but never updated the edge, so
//! a restarted node's own `ClusterEdgeState::control` entry pointed at the
//! OLD, `Simulator::stop`ped (dead) `RaftNode` for the rest of the
//! scenario — any NEW schema proposal issued through that node's fast path
//! (e.g. `ensure_txn_idempotency_table`'s `CreateTableSchema`, which a
//! tokened `TransactWriteItems` needs) spun until `SCHEMA_COMMIT_TIMEOUT`
//! even with a real, healthy, reachable leader elsewhere — reproduced
//! deterministically via `dynamowire_stop_restart_s02`, and shown to be
//! general control-plane-restart infrastructure, not anything
//! transact-specific (an ordinary, non-transactional `CreateTable` from
//! the restarted node reproduced the identical failure). A first fix
//! (calling the existing, append-only `register_control` from `restart`)
//! left the exact same scenario failing: the edge's registry then held
//! BOTH the stale and the fresh handle, and `leader_handle()`'s `find`
//! could return the stale one first. `ClusterEdgeState::replace_control`
//! (clears before pushing) is the real fix — see its own doc in `lib.rs`.
//!
//! # A resource-scale finding filed, not fixed: peak memory grows with
//! `ANIMUS_DYNAMO_WIRE_SEEDS` depth
//!
//! Measured on this rung's own development sandbox (4 vCPU / 15 GiB RAM,
//! no swap): `sim_cluster_dynamo_corpus_is_consistent` at the default
//! depth (8 scenarios) completes in ~114s with modest RSS; at
//! `ANIMUS_DYNAMO_WIRE_SEEDS=4` (32 scenarios) it completed cleanly in
//! ~460s; at `=12` (96 scenarios) process RSS was observed climbing
//! through ~6.3 GiB at the ~10-minute mark and on to a ~13.8 GiB plateau
//! by ~30-37 minutes, matching (not exceeding) the plateau independently
//! observed at `=25` (200 scenarios) before the OS OOM-killed that run on
//! this same sandbox. The plateau, not a strictly-monotonic per-scenario
//! climb, and its rough independence from total scenario count once
//! depth is large enough, are both consistent with ordinary glibc
//! allocator high-water-mark behavior (freed memory not returned to the
//! OS) rather than a true per-scenario leak — but this was not run to a
//! confirmed root cause, only measured and reported per this task's own
//! explicit instruction not to keep investigating it here. Filed as a
//! resource-scale characteristic of the fixture worth a maintainer look
//! (e.g. under a memory profiler, or with `MALLOC_ARENA_MAX=1`, which did
//! not visibly change the trajectory in this sandbox), not a correctness
//! defect and not fixed in this PR — CI's own already-established green
//! `=25` figure (`~10m2s wall`, see the D2 PR 2 entry this file's own doc
//! history references) implies CI's runners simply have materially more
//! RAM than this 15 GiB sandbox.
//!
//! # The cells
//!
//! Identical to `sim_cluster_corpus.rs`'s own 8 (`baseline`,
//! `leader_crash`, `follower_crash`, `stop_restart`, `leader_partition`,
//! `split_brain`, `forward_heavy`, `two_tables`) — same `Nemesis`
//! semantics, same cluster shapes, reused rather than reinvented since the
//! fault dimension this corpus exercises is identical; only the workload
//! riding on top changed from raw KV ops to DynamoDB wire ops.
//!
//! # Depth knob
//!
//! `ANIMUS_DYNAMO_WIRE_SEEDS` (default 1 = the 8 cells above,
//! byte-identical to the committed set) — `corpus::seed_expand` over
//! [`corpus_cells`], the same house convention every other corpus in this
//! workspace uses. Run via `cargo test -p animusd --lib
//! sim_cluster_dynamo_corpus`.
//!
//! # Shrink wiring (ADR 0061 rung B4)
//!
//! Mirrors `sim_cluster_corpus.rs`'s own wiring exactly: [`Scenario`]/
//! [`Nemesis`] derive `Serialize`/`Deserialize`, [`scenario_candidates`]
//! reduces `faults`/`window`/`rounds`/`keyspace`/`clients` (never `name`/
//! `seed`/`nodes`/`replication`/`tables`), [`shrink_and_report`] runs under
//! `ANIMUS_SHRINK=1` only after a scenario is already known to have
//! failed, and `sim_cluster_dynamo_shrink_replay` (`#[ignore]`d) reads
//! `ANIMUS_SHRINK_REPLAY` to re-run a printed minimized case.
//!
//! # Residuals (out of scope for this rung — see `dynamo.rs`'s own
//! `dispatch_item_op` doc for the full "what's ProdEnv-only, why" account)
//!
//! GSI/LSI `Query`/`Scan` and PartiQL (`ExecuteStatement`/
//! `BatchExecuteStatement`/`ExecuteTransaction`) are still unreachable
//! through the generic `dispatch_item_op` core this corpus drives —
//! `execute_item_op_as` returns a clean `InternalServerError` for any of
//! them, so this corpus never issues one. Deferred to C-06 PRs 5/6 (PartiQL
//! siblings + sim tests), not attempted here. **`TransactWriteItems`/
//! `TransactGetItems` are no longer a residual as of this PR** — see the
//! dedicated module-doc section above for exactly what's in the randomized
//! workload versus [`run_transact_probe`]'s own dedicated coverage, and
//! exactly why (never a silent gap: a `Delete` action inside a transact
//! write, and a genuinely reused `ClientRequestToken`, are both
//! deliberately probe-only, not randomized-workload material).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use animus_env::{Clock, EnvExt, Rng};
use animus_item::AttributeValue;
use animus_sim::SimEnv;
use animus_test::corpus::{self, SeedVariant};
use animus_test::history::{Key, Mop, Process};
use animus_test::shrink::{self, ShrinkReport};
use animus_test::{CheckReport, Recorder, check_convergence, check_cycles, check_durability};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::sim_cluster::{SimCluster, SimClusterHandle};
use super::*;

/// Settle time before the workload starts — mirrors `sim_cluster_corpus.
/// rs`'s identical `SETTLE`.
const SETTLE: Duration = Duration::from_millis(300);
/// Inter-round gap so client tasks interleave rather than lock-stepping.
const POLL: Duration = Duration::from_millis(80);
/// How long the runner holds a scheduled fault's own outage window open
/// before healing.
const FAULT_WINDOW: Duration = Duration::from_millis(1200);
/// Post-heal drain: run the workload tail to completion before taking the
/// history snapshot the `cycles` verdict is checked against.
const DRAIN: Duration = Duration::from_secs(6);
/// Converged-or-timeout poll step + budget for the durability/convergence
/// checks.
const CONVERGENCE_POLL_STEP: Duration = Duration::from_secs(1);
const CONVERGENCE_BUDGET: Duration = Duration::from_secs(15);

/// Deterministic per-scenario executor-cost ceiling (ADR 0061 rung I C-09
/// PR 3's follow-on, 2026-09-09) — `run_scenario`'s own regression against
/// the corpus-deep `dynamo_wire` CI timeout this fix closes: at
/// `ANIMUS_DYNAMO_WIRE_SEEDS=25` on nightly run 34327875460 (main @
/// `57073da0`), the 30-minute `.github/workflows/corpus-deep.yml` timeout
/// fired at scenario 181/200, ~9-11s/scenario, no hang, root-caused to six
/// always-on per-node `SimCluster` background loops each ticking every
/// 200ms — see `sim_cluster.rs`'s own `SIM_FALLBACK_TICK` doc for the full
/// root-cause account and `docs/engineering-lessons.md`'s matching entry
/// for the general lesson (a background loop's own cheap-when-idle
/// self-assessment is per-loop, not per-fixture).
///
/// `animus_sim::Simulator::stats()`'s `timer_fires` — the unified
/// `(time, seq)` timeline's own fired-entry count, covering every
/// `env.sleep()` tick **and** every message delivery (ADR 0003's one
/// shared queue) — is a pure, seed-reproducible proxy for wall-clock
/// executor cost, so `run_scenario` bounds it directly rather than a real
/// wall-clock ceiling, which a shared, contended CI runner's own noise
/// floor would make an unreliable gate (root `CLAUDE.md`'s "a flaky test
/// is a real bug" rule — a wall-clock assert in a sim test is exactly the
/// flake source that rule warns against). Read once right after
/// `SimCluster::new` returns and once right before `run_scenario` returns;
/// the delta is this one scenario's own executor cost, independent of
/// whatever bring-up cost the cluster's own control-plane election paid
/// during construction.
///
/// **Value, and how it was picked**: measured post-fix
/// (`SIM_FALLBACK_TICK` = 1s) at `ANIMUS_DYNAMO_WIRE_SEEDS=5` (40
/// scenarios across all 8 cells) — the heaviest cell, `forward_heavy`
/// (RF2 over 4 nodes, so most ops route through a node hosting no local
/// replica), peaked at ~1.60M timer fires per scenario across its 5
/// seeds; every other cell stayed under ~1.12M. **A diagnostic finding
/// worth recording here, not just in the report**: an A/B measurement at
/// `SIM_FALLBACK_TICK` = 200ms (the pre-fix value) vs. 1s vs. 60s on the
/// cheapest cell (`baseline`, no faults) showed these five loops'
/// **own** contribution to total executor cost is real but small — task
/// polls moved 948,525 → 907,806 → 897,804 across that range, a ~5.6%
/// spread — so most of a scenario's ~900K-1.6M timer fires come from
/// something this fix does not touch (real per-tablet/control Raft
/// consensus machinery ticking across the large cumulative virtual time
/// this corpus's own `OP_BUDGET`-per-call probe design burns, `sim_
/// cluster.rs`'s own `spawn_and_capture` doc). This budget carries ~2x
/// headroom over the observed `forward_heavy` maximum specifically so it
/// still catches a genuine regression in what this fix DOES control
/// (reintroducing a fast per-loop tick, or a sixth always-on loop) without
/// being sensitive to the larger, out-of-this-fix's-scope cost the A/B
/// measurement above attributes elsewhere.
const SCENARIO_TIMER_FIRES_BUDGET: u64 = 3_300_000;

/// A `Key`'s high-order digits name which table it belongs to — see
/// `sim_cluster_corpus.rs`'s own identical constant/note for why (multiple
/// tables sharing one `Key` space without conflating their histories).
const TABLE_KEY_STRIDE: Key = 1_000_000;

/// Every table's items live under exactly this many fixed partition keys
/// (`part-0`, `part-1`, …) — small and fixed so a base-table `Query`
/// (scoped to one partition) returns a real, strict subset of a `Scan`
/// (the whole table), giving the two operations genuinely different
/// coverage rather than one being a synonym for the other.
const PARTITIONS: u64 = 2;

/// Split `key` back into `(table name, partition key, sort key)` — the
/// wire-request identity for this fixture's own `(pk, sk)` composite
/// schema. `logical % PARTITIONS` picks a fixed partition; `item-{logical}`
/// is the sort key, parsed back by [`key_from_table_and_sk`].
fn table_pk_sk(key: Key) -> (String, String, String) {
    let table = key / TABLE_KEY_STRIDE;
    let logical = key % TABLE_KEY_STRIDE;
    (
        format!("t{table}"),
        format!("part-{}", logical % PARTITIONS),
        format!("item-{logical}"),
    )
}

/// The inverse of [`table_pk_sk`]'s `sk` half, given the table index a
/// `Query`/`Scan` caller already knows (it built the request for that
/// table) — recovers a full `Key` from one returned item's own `sk`
/// attribute. `None` for a row this corpus didn't write itself (should
/// never happen against a table only this corpus's own workload touches,
/// but a probe's own disjoint `delete-probe-*`/`batch-probe-*` keys must
/// never be mistaken for a modeled one if a caller ever queried the same
/// table — this corpus keeps every probe on its own partition-free key
/// shape specifically so this parse fails cleanly on them instead of
/// silently aliasing onto a modeled `Key`).
fn key_from_table_and_sk(table: u64, sk: &str) -> Option<Key> {
    let logical: u64 = sk.strip_prefix("item-")?.parse().ok()?;
    Some(table * TABLE_KEY_STRIDE + logical)
}

/// Build the DynamoDB JSON value for a one-element list holding `value`
/// (`{"L": [{"N": "value"}]}`) — the `:v` operand of every append's
/// `UpdateExpression`.
fn one_element_list(value: u64) -> Value {
    json!({"L": [{"N": value.to_string()}]})
}

/// Decode an already-parsed item's `items` attribute (DynamoDB wire JSON:
/// `{"items": {"L": [{"N": "1"}, {"N": "2"}, ...]}}`) into the plain
/// `Vec<u64>` the shared checker model wants — absent/malformed reads as
/// an empty list, the same "absent key ⇒ `Some(vec![])`" convention
/// `sim_cluster_corpus.rs`'s own `run_read` uses for a `None` value.
fn decode_items_attr(item: &Value) -> Vec<u64> {
    item.get("items")
        .and_then(|v| v.get("L"))
        .and_then(Value::as_array)
        .map(|elems| {
            elems
                .iter()
                .filter_map(|e| e.get("N")?.as_str()?.parse::<u64>().ok())
                .collect()
        })
        .unwrap_or_default()
}

/// Decode the raw stored bytes at one engine key (`SimClusterHandle::
/// local_value`'s own return shape — this corpus's *internal* item codec,
/// `animus_item::decode_stored_item`, not the DynamoDB wire JSON
/// `decode_items_attr` above parses) into the same `Vec<u64>` shape —
/// used only by [`final_state`], the direct local-engine read every other
/// corpus in this crate uses for its own durability/convergence snapshot.
fn decode_engine_items(bytes: &[u8]) -> Vec<u64> {
    let Ok(Some(item)) = animus_item::decode_stored_item(bytes) else {
        return Vec::new();
    };
    match item.get("items") {
        Some(AttributeValue::L(list)) => list
            .iter()
            .filter_map(|v| match v {
                AttributeValue::N(s) => s.parse::<u64>().ok(),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Declarative scenario model — identical shape to `sim_cluster_corpus.rs`.
// ---------------------------------------------------------------------------

/// A fault the runner injects once, at a scheduled offset from the start of
/// the workload — identical semantics to `sim_cluster_corpus.rs`'s own
/// `Nemesis` (kept as its own copy, per this workspace's "each corpus's own
/// workload-adjacent details are a private copy" convention — see that
/// file's module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Nemesis {
    /// Crash the tablet's current leader.
    LeaderCrash,
    /// Crash a non-leader replica of the tablet.
    FollowerCrash,
    /// A true process restart of the tablet's current leader.
    StopRestart,
    /// Partition the tablet's current leader away from every other node.
    LeaderPartition,
    /// Partition the WHOLE cluster into two non-empty halves.
    SplitBrain,
}

impl Nemesis {
    fn apply(self, cluster: &mut SimCluster, tablet: TabletId) {
        match self {
            Nemesis::LeaderCrash => {
                if let Some(leader) = cluster.leader_index_of(tablet) {
                    cluster.crash(leader);
                }
            }
            Nemesis::FollowerCrash => {
                if let Some(leader) = cluster.leader_index_of(tablet) {
                    // A live lookup, not `SimClusterHandle::replicas_of`'s
                    // stale creation-time snapshot (see `live_replicas`'s
                    // own doc) — a follower this nemesis picked off a stale
                    // replica set could crash a node that no longer hosts
                    // the tablet at all, silently downgrading this cell to
                    // a no-op fault.
                    let node_count = cluster.node_count() as u64;
                    let follower = (0..node_count)
                        .find(|&n| n != leader && cluster.hosted_tablets(n).contains(&tablet));
                    if let Some(follower) = follower {
                        cluster.crash(follower);
                    }
                }
            }
            Nemesis::StopRestart => {
                let victim = cluster.leader_index_of(tablet).unwrap_or(0);
                cluster.restart(victim);
            }
            Nemesis::LeaderPartition => {
                if let Some(leader) = cluster.leader_index_of(tablet) {
                    for n in 0..cluster.node_count() as u64 {
                        if n != leader {
                            cluster.partition(leader, n);
                        }
                    }
                }
            }
            Nemesis::SplitBrain => {
                let n = cluster.node_count() as u64;
                let half = n.div_ceil(2);
                for a in 0..half {
                    for b in half..n {
                        cluster.partition(a, b);
                    }
                }
            }
        }
    }
}

/// A seed-reproducible scenario — the DynamoDB-wire twin of
/// `sim_cluster_corpus.rs`'s own `Scenario`, same fields, same meaning.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Scenario {
    name: String,
    seed: u64,
    nodes: usize,
    replication: usize,
    tables: usize,
    clients: usize,
    rounds: u64,
    keyspace: u64,
    read_pct: u64,
    faults: Vec<(Duration, Nemesis)>,
    window: Duration,
}

impl SeedVariant for Scenario {
    fn scenario_name(&self) -> &str {
        &self.name
    }
    fn reseeded(&self, name: String, seed: u64) -> Self {
        Scenario {
            name,
            seed,
            ..self.clone()
        }
    }
}

fn base_workload(
    name: &str,
    nodes: usize,
    replication: usize,
    tables: usize,
    faults: Vec<(Duration, Nemesis)>,
    window: Duration,
) -> Scenario {
    Scenario {
        seed: corpus::name_seed(name),
        name: name.to_owned(),
        nodes,
        replication,
        tables,
        clients: 3,
        rounds: 6,
        // 6, not 3 (as of C-06 PR 4): a `TransactWriteItems` draws its two
        // write keys from the SAME client's own `owned` set (never a second
        // client's — the single-writer-per-key discipline this file's
        // module doc establishes), so each client needs at least 2 owned
        // keys per table. With `clients: 3` a keyspace of 3 gave exactly 1
        // owned key per client per table; 6 gives exactly 2 — see this
        // file's own "TransactWriteItems/TransactGetItems" module-doc
        // section.
        keyspace: 6,
        read_pct: 40,
        faults,
        window,
    }
}

/// The 8 named cells this corpus ships with — identical fault matrix to
/// `sim_cluster_corpus.rs`'s own [`corpus_cells`] (see this file's module
/// doc's "The cells" section).
fn corpus_cells() -> Vec<Scenario> {
    const FAULT_AT: Duration = Duration::from_millis(900);
    vec![
        base_workload("dynamowire_baseline", 3, 3, 1, vec![], Duration::ZERO),
        base_workload(
            "dynamowire_leader_crash",
            3,
            3,
            1,
            vec![(FAULT_AT, Nemesis::LeaderCrash)],
            FAULT_WINDOW,
        ),
        base_workload(
            "dynamowire_follower_crash",
            3,
            3,
            1,
            vec![(FAULT_AT, Nemesis::FollowerCrash)],
            FAULT_WINDOW,
        ),
        base_workload(
            "dynamowire_stop_restart",
            3,
            3,
            1,
            vec![(FAULT_AT, Nemesis::StopRestart)],
            FAULT_WINDOW,
        ),
        base_workload(
            "dynamowire_leader_partition",
            3,
            3,
            1,
            vec![(FAULT_AT, Nemesis::LeaderPartition)],
            FAULT_WINDOW,
        ),
        base_workload(
            "dynamowire_split_brain",
            3,
            3,
            1,
            vec![(FAULT_AT, Nemesis::SplitBrain)],
            FAULT_WINDOW,
        ),
        base_workload("dynamowire_forward_heavy", 4, 2, 1, vec![], Duration::ZERO),
        base_workload("dynamowire_two_tables", 3, 3, 2, vec![], Duration::ZERO),
    ]
}

fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_DYNAMO_WIRE_SEEDS")
}

fn corpus() -> Vec<Scenario> {
    corpus::seed_expand(corpus_cells(), seeds_per_cell())
}

// ---------------------------------------------------------------------------
// The workload.
// ---------------------------------------------------------------------------

struct Shared {
    rec: Mutex<Recorder>,
    next_value: Mutex<u64>,
    /// Acked-write count per issuing node — the `forward_heavy` non-vacuity
    /// signal, identical to `sim_cluster_corpus.rs`'s own field.
    ok_writes_by_node: Mutex<BTreeMap<u64, usize>>,
    /// Every `ConsistentRead: false` observation this scenario made:
    /// `(key, observed list)` — checked after the scenario converges (see
    /// the module doc's "read-consistency modeling decision"), never fed
    /// into the shared `Recorder`/`check_cycles` history.
    eventual_reads: Mutex<Vec<(Key, Vec<u64>)>>,
    /// Successful `TransactWriteItems`/`TransactGetItems` counts — the
    /// non-vacuity signal for the new op mix (ADR 0061 rung F, C-06 PR 4),
    /// mirroring `eventual_reads`'s own role for the pre-existing
    /// `ConsistentRead: false` dimension.
    transact_write_oks: Mutex<usize>,
    transact_get_oks: Mutex<usize>,
}

impl Shared {
    fn fresh_value(&self) -> u64 {
        let mut v = self.next_value.lock().expect("next_value poisoned");
        *v += 1;
        *v
    }

    fn record_ok_write(&self, node: u64) {
        *self
            .ok_writes_by_node
            .lock()
            .expect("ok_writes_by_node poisoned")
            .entry(node)
            .or_default() += 1;
    }

    fn record_eventual_read(&self, key: Key, observed: Vec<u64>) {
        self.eventual_reads
            .lock()
            .expect("eventual_reads poisoned")
            .push((key, observed));
    }

    fn record_transact_write_ok(&self) {
        *self
            .transact_write_oks
            .lock()
            .expect("transact_write_oks poisoned") += 1;
    }

    fn record_transact_get_ok(&self) {
        *self
            .transact_get_oks
            .lock()
            .expect("transact_get_oks poisoned") += 1;
    }
}

/// Which flavor of read a round picked — see the module doc's "read-
/// consistency modeling decision" and "multi-key reads" sections for why
/// each is handled differently by [`run_get`]/[`run_query`]/[`run_scan`].
#[derive(Clone, Copy, Debug)]
enum ReadKind {
    ConsistentGet,
    EventualGet,
    Query,
    Scan,
    /// `TransactGetItems` over two keys — always strongly consistent (real
    /// DynamoDB gives it no `ConsistentRead` parameter at all), so unlike
    /// [`ReadKind::ConsistentGet`]/[`ReadKind::EventualGet`] there is no
    /// weaker variant to split out.
    TransactGet,
}

/// Pick two DISTINCT indices in `0..n`, or `None` if `n < 2` — the shared
/// technique [`run_transact_write`] (over a client's own `owned` keys) and
/// [`ReadKind::TransactGet`]'s selection (over the whole keyspace) both use.
/// `i1 = (i0 + 1 + gen_below(n - 1)) % n` ranges over every value except
/// `i0` exactly once as the inner draw varies, guaranteeing `i1 != i0`
/// without a retry loop.
fn distinct_pair(env: &SimEnv, n: u64) -> Option<(u64, u64)> {
    if n < 2 {
        return None;
    }
    let i0 = env.gen_below(n);
    let i1 = (i0 + 1 + env.gen_below(n - 1)) % n;
    Some((i0, i1))
}

/// Append `value` to `key`'s list via a real `UpdateItem` wire request —
/// see the module doc's own "The list-append mapping" section for the
/// exact `UpdateExpression` and why it is the corpus's one genuinely
/// non-idempotent write. `Ok`/`info`, never `fail` — a non-200 response is
/// indeterminate (the write may have applied and only the *reply* was
/// lost/timed out), exactly `sim_cluster_corpus.rs`'s own `run_write`
/// discipline.
async fn run_write(
    env: &SimEnv,
    handle: &SimClusterHandle,
    shared: &Arc<Shared>,
    proc: Process,
    key: Key,
    node: u64,
) {
    let value = shared.fresh_value();
    let (table, pk, sk) = table_pk_sk(key);
    let mops = vec![Mop::Append { key, value }];
    shared
        .rec
        .lock()
        .expect("recorder poisoned")
        .invoke(proc, env.now().0, mops.clone());

    let body = json!({
        "TableName": table,
        "Key": {"pk": {"S": pk}, "sk": {"S": sk}},
        "UpdateExpression": "SET items = list_append(if_not_exists(items, :empty), :v)",
        "ExpressionAttributeValues": {
            ":empty": {"L": []},
            ":v": one_element_list(value),
        },
    })
    .to_string();
    let (status, _body) = handle
        .dynamo(node, "DynamoDB_20120810.UpdateItem", body.as_bytes())
        .await;

    let mut rec = shared.rec.lock().expect("recorder poisoned");
    if status == 200 {
        rec.ok(proc, env.now().0, mops);
        drop(rec);
        shared.record_ok_write(node);
    } else {
        rec.info(proc, env.now().0, mops);
    }
}

/// A single-key `GetItem`, at either consistency level — see the module
/// doc's "read-consistency modeling decision" for why only the
/// `ConsistentRead: true` path feeds the shared history.
async fn run_get(
    env: &SimEnv,
    handle: &SimClusterHandle,
    shared: &Arc<Shared>,
    proc: Process,
    key: Key,
    kind: ReadKind,
    node: u64,
) {
    let (table, pk, sk) = table_pk_sk(key);
    let consistent = matches!(kind, ReadKind::ConsistentGet);
    if consistent {
        shared.rec.lock().expect("recorder poisoned").invoke(
            proc,
            env.now().0,
            vec![Mop::Read {
                key,
                observed: None,
            }],
        );
    }

    let body = json!({
        "ConsistentRead": consistent,
        "TableName": table,
        "Key": {"pk": {"S": pk}, "sk": {"S": sk}},
    })
    .to_string();
    let (status, body) = handle
        .dynamo(node, "DynamoDB_20120810.GetItem", body.as_bytes())
        .await;

    if status != 200 {
        if consistent {
            shared.rec.lock().expect("recorder poisoned").info(
                proc,
                env.now().0,
                vec![Mop::Read {
                    key,
                    observed: None,
                }],
            );
        }
        // An eventual read's own failure is dropped, never recorded — it
        // carries no observation to check against anything.
        return;
    }

    let list = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|v| v.get("Item").map(decode_items_attr))
        .unwrap_or_default();
    if consistent {
        shared.rec.lock().expect("recorder poisoned").ok(
            proc,
            env.now().0,
            vec![Mop::Read {
                key,
                observed: Some(list),
            }],
        );
    } else {
        shared.record_eventual_read(key, list);
    }
}

/// Decode a `Query`/`Scan` response's `Items` array into one [`Mop::Read`]
/// per returned item, resolving each item's own `Key` from its `sk`
/// attribute via [`key_from_table_and_sk`] — see the module doc's
/// "Multi-key reads" section.
fn read_mops_from_items_response(table: u64, body: &str) -> Vec<Mop> {
    let Some(items) = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v.get("Items").and_then(Value::as_array).cloned())
    else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            let sk = item.get("sk")?.get("S")?.as_str()?;
            let key = key_from_table_and_sk(table, sk)?;
            Some(Mop::Read {
                key,
                observed: Some(decode_items_attr(item)),
            })
        })
        .collect()
}

/// A base-table `Query` scoped to one of [`PARTITIONS`] fixed partitions,
/// always `ConsistentRead: true` — proves `dispatch_item_op`'s base-table
/// `Query` arm returns exactly that partition's rows, feeding a
/// [`Mop::Read`] per returned item into the shared history as one
/// transaction.
async fn run_query(
    env: &SimEnv,
    handle: &SimClusterHandle,
    shared: &Arc<Shared>,
    proc: Process,
    table_idx: u64,
    table: &str,
    node: u64,
) {
    let part = env.gen_below(PARTITIONS);
    let pk = format!("part-{part}");
    shared
        .rec
        .lock()
        .expect("recorder poisoned")
        .invoke(proc, env.now().0, Vec::new());

    let body = json!({
        "TableName": table,
        "ConsistentRead": true,
        "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": {":p": {"S": pk}},
    })
    .to_string();
    let (status, body) = handle
        .dynamo(node, "DynamoDB_20120810.Query", body.as_bytes())
        .await;

    let mut rec = shared.rec.lock().expect("recorder poisoned");
    if status == 200 {
        rec.ok(
            proc,
            env.now().0,
            read_mops_from_items_response(table_idx, &body),
        );
    } else {
        rec.info(proc, env.now().0, Vec::new());
    }
}

/// A whole-table `Scan`, always `ConsistentRead: true` — [`run_query`]'s
/// unscoped sibling.
async fn run_scan(
    env: &SimEnv,
    handle: &SimClusterHandle,
    shared: &Arc<Shared>,
    proc: Process,
    table_idx: u64,
    table: &str,
    node: u64,
) {
    shared
        .rec
        .lock()
        .expect("recorder poisoned")
        .invoke(proc, env.now().0, Vec::new());

    let body = json!({"TableName": table, "ConsistentRead": true}).to_string();
    let (status, body) = handle
        .dynamo(node, "DynamoDB_20120810.Scan", body.as_bytes())
        .await;

    let mut rec = shared.rec.lock().expect("recorder poisoned");
    if status == 200 {
        rec.ok(
            proc,
            env.now().0,
            read_mops_from_items_response(table_idx, &body),
        );
    } else {
        rec.info(proc, env.now().0, Vec::new());
    }
}

/// A two-item `TransactWriteItems` — `Update`+`list_append` on TWO of this
/// client's own owned keys plus an always-passing `ConditionCheck` against
/// the target table's `__guard__` marker (seeded once, before the workload
/// starts, in [`run_scenario`]) — see the module doc's own
/// "`TransactWriteItems`/`TransactGetItems`" section for why the guard is
/// always-passing and why atomicity is checked by construction here (both
/// appends are recorded as ONE `invoke`/`ok` entry, never separately).
/// `with_token` optionally attaches a fresh, call-unique `ClientRequestToken`
/// — exercising the token-preflight path without ever actually retrying one
/// (a genuine retry is [`run_transact_probe`]'s own job).
#[allow(clippy::too_many_arguments)]
async fn run_transact_write(
    env: &SimEnv,
    handle: &SimClusterHandle,
    shared: &Arc<Shared>,
    proc: Process,
    keys: [Key; 2],
    with_token: bool,
    node: u64,
) {
    let v0 = shared.fresh_value();
    let v1 = shared.fresh_value();
    let (table0, pk0, sk0) = table_pk_sk(keys[0]);
    let (table1, pk1, sk1) = table_pk_sk(keys[1]);
    let mops = vec![
        Mop::Append {
            key: keys[0],
            value: v0,
        },
        Mop::Append {
            key: keys[1],
            value: v1,
        },
    ];
    shared
        .rec
        .lock()
        .expect("recorder poisoned")
        .invoke(proc, env.now().0, mops.clone());

    let mut body = json!({
        "TransactItems": [
            {"Update": {
                "TableName": table0,
                "Key": {"pk": {"S": pk0}, "sk": {"S": sk0}},
                "UpdateExpression": "SET items = list_append(if_not_exists(items, :empty), :v)",
                "ExpressionAttributeValues": {":empty": {"L": []}, ":v": one_element_list(v0)},
            }},
            {"Update": {
                "TableName": table1,
                "Key": {"pk": {"S": pk1}, "sk": {"S": sk1}},
                "UpdateExpression": "SET items = list_append(if_not_exists(items, :empty), :v)",
                "ExpressionAttributeValues": {":empty": {"L": []}, ":v": one_element_list(v1)},
            }},
            {"ConditionCheck": {
                "TableName": table0,
                "Key": {"pk": {"S": "__guard__"}, "sk": {"S": "__guard__"}},
                "ConditionExpression": "attribute_exists(pk)",
            }},
        ],
    });
    if with_token {
        body["ClientRequestToken"] = json!(format!("corpus-tw-{proc}-{v0}-{v1}"));
    }
    let (status, _body) = handle
        .dynamo(
            node,
            "DynamoDB_20120810.TransactWriteItems",
            body.to_string().as_bytes(),
        )
        .await;

    let mut rec = shared.rec.lock().expect("recorder poisoned");
    if status == 200 {
        rec.ok(proc, env.now().0, mops);
        drop(rec);
        shared.record_ok_write(node);
        shared.record_transact_write_ok();
    } else {
        rec.info(proc, env.now().0, mops);
    }
}

/// Decode a `TransactGetItems` response's `Responses` array (`{}` for an
/// absent item, `{"Item": {..}}` for a present one — `wire::
/// transact_get_response`'s own shape) into one [`Mop::Read`] per requested
/// key, in request order — the `TransactGetItems` sibling of
/// [`read_mops_from_items_response`].
fn read_mops_from_transact_get(keys: [Key; 2], body: &str) -> Option<Vec<Mop>> {
    let responses = serde_json::from_str::<Value>(body)
        .ok()?
        .get("Responses")?
        .as_array()?
        .clone();
    if responses.len() != 2 {
        return None;
    }
    Some(
        keys.into_iter()
            .zip(responses.iter())
            .map(|(key, resp)| Mop::Read {
                key,
                observed: Some(resp.get("Item").map(decode_items_attr).unwrap_or_default()),
            })
            .collect(),
    )
}

/// A two-key `TransactGetItems`, always strongly consistent (real DynamoDB
/// gives this operation no `ConsistentRead` parameter — see the module
/// doc's own section) — feeds the shared history as ONE atomic entry of two
/// [`Mop::Read`]s, exactly like [`run_query`]/[`run_scan`]'s own multi-key
/// reads. A non-200 response (including the retryable
/// `TransactionCanceledException` an un-quiesced snapshot can return) is
/// `info`, never `fail` — mirrors [`run_get`]'s own discipline for a read
/// that carries no observation.
async fn run_transact_get(
    env: &SimEnv,
    handle: &SimClusterHandle,
    shared: &Arc<Shared>,
    proc: Process,
    keys: [Key; 2],
    node: u64,
) {
    shared.rec.lock().expect("recorder poisoned").invoke(
        proc,
        env.now().0,
        keys.into_iter()
            .map(|key| Mop::Read {
                key,
                observed: None,
            })
            .collect(),
    );

    let (table0, pk0, sk0) = table_pk_sk(keys[0]);
    let (table1, pk1, sk1) = table_pk_sk(keys[1]);
    let body = json!({
        "TransactItems": [
            {"Get": {"TableName": table0, "Key": {"pk": {"S": pk0}, "sk": {"S": sk0}}}},
            {"Get": {"TableName": table1, "Key": {"pk": {"S": pk1}, "sk": {"S": sk1}}}},
        ],
    })
    .to_string();
    let (status, body) = handle
        .dynamo(node, "DynamoDB_20120810.TransactGetItems", body.as_bytes())
        .await;

    let mut rec = shared.rec.lock().expect("recorder poisoned");
    match (status, read_mops_from_transact_get(keys, &body)) {
        (200, Some(mops)) => {
            rec.ok(proc, env.now().0, mops);
            drop(rec);
            shared.record_transact_get_ok();
        }
        _ => {
            rec.info(
                proc,
                env.now().0,
                keys.into_iter()
                    .map(|key| Mop::Read {
                        key,
                        observed: None,
                    })
                    .collect(),
            );
        }
    }
}

/// One client's loop: each round, draw a fresh issuing node from the
/// scenario's own seed, then run one op — a write (single-writer, its own
/// owned keys only, sometimes a multi-key `TransactWriteItems`) or one of
/// five read shapes across the whole keyspace.
#[allow(clippy::too_many_arguments)]
async fn client_loop(
    env: SimEnv,
    handle: SimClusterHandle,
    shared: Arc<Shared>,
    proc: Process,
    clients: usize,
    rounds: u64,
    tables: usize,
    keyspace: u64,
    read_pct: u64,
    node_count: usize,
) {
    let owned: Vec<Key> = (0..tables as u64)
        .flat_map(|t| (0..keyspace).map(move |k| t * TABLE_KEY_STRIDE + k))
        .filter(|&k| k % clients as u64 == proc)
        .collect();
    let total_keys = tables as u64 * keyspace;
    for _round in 0..rounds {
        let node = env.gen_below(node_count as u64);
        let is_read = env.gen_below(100) < read_pct;
        if is_read {
            let t = env.gen_below(tables as u64);
            let table = format!("t{t}");
            let kind = match env.gen_below(12) {
                0..=4 => ReadKind::ConsistentGet,
                5..=7 => ReadKind::EventualGet,
                8 => ReadKind::Query,
                9 => ReadKind::Scan,
                _ if total_keys >= 2 => ReadKind::TransactGet,
                // `distinct_pair` needs 2+ keys — a shrunk `keyspace`
                // candidate (ADR 0061 rung B4) can legally drop below that;
                // fall back to a plain read rather than skip the round.
                _ => ReadKind::ConsistentGet,
            };
            match kind {
                ReadKind::ConsistentGet | ReadKind::EventualGet => {
                    let k = env.gen_below(keyspace);
                    let key = t * TABLE_KEY_STRIDE + k;
                    run_get(&env, &handle, &shared, proc, key, kind, node).await;
                }
                ReadKind::Query => run_query(&env, &handle, &shared, proc, t, &table, node).await,
                ReadKind::Scan => run_scan(&env, &handle, &shared, proc, t, &table, node).await,
                ReadKind::TransactGet => {
                    let (i0, i1) =
                        distinct_pair(&env, total_keys).expect("checked total_keys >= 2 above");
                    let key_of = |idx: u64| (idx / keyspace) * TABLE_KEY_STRIDE + (idx % keyspace);
                    run_transact_get(&env, &handle, &shared, proc, [key_of(i0), key_of(i1)], node)
                        .await;
                }
            }
        } else if owned.len() >= 2 && env.gen_below(3) == 0 {
            let (i0, i1) =
                distinct_pair(&env, owned.len() as u64).expect("checked owned.len() >= 2 above");
            let with_token = env.gen_below(2) == 0;
            run_transact_write(
                &env,
                &handle,
                &shared,
                proc,
                [owned[i0 as usize], owned[i1 as usize]],
                with_token,
                node,
            )
            .await;
        } else if !owned.is_empty() {
            let key = owned[env.gen_below(owned.len() as u64) as usize];
            run_write(&env, &handle, &shared, proc, key, node).await;
        }
        env.sleep(POLL).await;
    }
}

/// `PutItem` → consistent `GetItem`(present) → `DeleteItem` → consistent
/// `GetItem`(absent), issued **from every node in the cluster in turn**, on
/// a `delete-probe-{node}` key namespace disjoint from the list-append
/// model's own `part-*`/`item-*` keys — the direct correctness check the
/// module doc's own "DeleteItem/BatchWriteItem" section explains. Mirrors
/// `sim_cluster_corpus.rs`'s own `run_delete_probe` (including its "drive
/// `SimCluster`'s own synchronous driver, never a bare `block_on` of
/// `SimClusterHandle`'s ops with nothing advancing the simulator" gotcha —
/// `cluster.dynamo` is that same synchronous driver, mirroring `put`/`get`/
/// `delete` exactly).
fn run_delete_probe(
    cluster: &mut SimCluster,
    table: &str,
    node_count: usize,
) -> Result<usize, String> {
    for node in 0..node_count as u64 {
        let pk = format!("delete-probe-{node}");
        let sk = "v";
        let put_body = json!({
            "TableName": table,
            "Item": {"pk": {"S": pk}, "sk": {"S": sk}, "items": one_element_list(9)},
        })
        .to_string();
        let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.PutItem", put_body.as_bytes());
        if status != 200 {
            return Err(format!(
                "node {node}: put failed: status={status} body={resp}"
            ));
        }

        let get_body = json!({
            "ConsistentRead": true,
            "TableName": table,
            "Key": {"pk": {"S": pk}, "sk": {"S": sk}},
        })
        .to_string();
        let (status, body) = cluster.dynamo(node, "DynamoDB_20120810.GetItem", get_body.as_bytes());
        if status != 200 || !body.contains(r#""N":"9""#) {
            return Err(format!(
                "node {node}: put not visible via a consistent get (status={status} body={body})"
            ));
        }

        let del_body = json!({
            "TableName": table,
            "Key": {"pk": {"S": pk}, "sk": {"S": sk}},
        })
        .to_string();
        let (status, resp) =
            cluster.dynamo(node, "DynamoDB_20120810.DeleteItem", del_body.as_bytes());
        if status != 200 {
            return Err(format!(
                "node {node}: delete failed: status={status} body={resp}"
            ));
        }

        let (status, body) = cluster.dynamo(node, "DynamoDB_20120810.GetItem", get_body.as_bytes());
        if status != 200 {
            return Err(format!(
                "node {node}: get after delete failed: status={status} body={body}"
            ));
        }
        if body.contains("\"Item\"") {
            return Err(format!(
                "node {node}: item still present after delete ({body})"
            ));
        }
    }
    Ok(node_count)
}

/// A two-item `BatchWriteItem`, verified with a consistent `GetItem` per
/// item, from every node in the cluster in turn — the module doc's own
/// direct correctness probe for `BatchWriteItem` (a whole-item overwrite,
/// kept out of the list-append model for the same reason `DeleteItem` is).
/// Own `batch-probe-{node}` key namespace, disjoint from every other key
/// this file's own probes/model use.
fn run_batch_write_probe(
    cluster: &mut SimCluster,
    table: &str,
    node_count: usize,
) -> Result<usize, String> {
    for node in 0..node_count as u64 {
        let pk = format!("batch-probe-{node}");
        let body = json!({
            "RequestItems": {
                table: [
                    {"PutRequest": {"Item": {"pk": {"S": pk}, "sk": {"S": "a"}, "items": one_element_list(1)}}},
                    {"PutRequest": {"Item": {"pk": {"S": pk}, "sk": {"S": "b"}, "items": one_element_list(2)}}},
                ],
            },
        })
        .to_string();
        let (status, resp) =
            cluster.dynamo(node, "DynamoDB_20120810.BatchWriteItem", body.as_bytes());
        if status != 200 {
            return Err(format!(
                "node {node}: BatchWriteItem failed: status={status} body={resp}"
            ));
        }

        for (sk, expect) in [("a", 1u64), ("b", 2u64)] {
            let get_body = json!({
                "ConsistentRead": true,
                "TableName": table,
                "Key": {"pk": {"S": pk}, "sk": {"S": sk}},
            })
            .to_string();
            let (status, body) =
                cluster.dynamo(node, "DynamoDB_20120810.GetItem", get_body.as_bytes());
            let seen = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|v| v.get("Item").map(decode_items_attr))
                .unwrap_or_default();
            if status != 200 || seen != vec![expect] {
                return Err(format!(
                    "node {node}: batch-written item {sk} not visible via a consistent \
                     get (status={status} body={body})"
                ));
            }
        }
    }
    Ok(node_count)
}

/// A consistent `GetItem` for `hits`/`N` (an `ADD`-counter probe read) — the
/// idempotency proof [`run_transact_probe`] uses: a re-run would double it,
/// a cached retry leaves it unchanged. Mirrors `sim_cluster_dynamo_
/// transact.rs`'s own `read_counter` helper.
fn read_hits_counter(cluster: &mut SimCluster, table: &str, pk: &str) -> Result<i64, String> {
    let body = json!({
        "ConsistentRead": true,
        "TableName": table,
        "Key": {"pk": {"S": pk}, "sk": {"S": "v"}},
    })
    .to_string();
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.GetItem", body.as_bytes());
    if status != 200 {
        return Err(format!(
            "GetItem({table}/{pk}) failed: status={status} body={resp}"
        ));
    }
    let v: Value = serde_json::from_str(&resp).map_err(|e| format!("invalid JSON: {e}"))?;
    Ok(v["Item"]["hits"]["N"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0))
}

/// The deterministic, dedicated proof for what [`client_loop`]'s randomized
/// `TransactWriteItems` mix cannot safely produce itself — see this file's
/// own "`TransactWriteItems`/`TransactGetItems`" module-doc section for the
/// full account of why each of the four steps below needs to be here rather
/// than in the shared Elle-model workload. Run from every node in the
/// cluster in turn, on a `transact-probe-{node}-*` key namespace disjoint
/// from every other key this file's probes/model use, reusing the scenario's
/// own `__guard__` marker item (seeded once per table in [`run_scenario`],
/// before the workload starts, and never touched by anything else) for both
/// the failing and the passing `ConditionCheck` below.
fn run_transact_probe(
    cluster: &mut SimCluster,
    table_names: &[String],
    node_count: usize,
) -> Result<usize, String> {
    let table0 = table_names[0].as_str();
    let table1 = table_names.get(1).map_or(table0, String::as_str);

    for node in 0..node_count as u64 {
        // --- (1) A genuinely FAILING ConditionCheck (the pre-existing
        // `__guard__` item DOES exist, so `attribute_not_exists` is false)
        // cancels the whole transaction — neither `Put` nor `Update` lands.
        let a_pk = format!("transact-probe-{node}-a");
        let b_pk = format!("transact-probe-{node}-b");
        let cancel_body = json!({
            "TransactItems": [
                {"Put": {"TableName": table0,
                    "Item": {"pk": {"S": a_pk}, "sk": {"S": "v"}, "items": one_element_list(1)}}},
                {"Update": {"TableName": table0, "Key": {"pk": {"S": b_pk}, "sk": {"S": "v"}},
                    "UpdateExpression": "ADD hits :one",
                    "ExpressionAttributeValues": {":one": {"N": "1"}}}},
                {"ConditionCheck": {"TableName": table0,
                    "Key": {"pk": {"S": "__guard__"}, "sk": {"S": "__guard__"}},
                    "ConditionExpression": "attribute_not_exists(pk)"}},
            ],
        })
        .to_string();
        let (status, resp) = cluster.dynamo(
            node,
            "DynamoDB_20120810.TransactWriteItems",
            cancel_body.as_bytes(),
        );
        if status != 400 || !resp.contains("TransactionCanceledException") {
            return Err(format!(
                "node {node}: expected the guard check to cancel: status={status} body={resp}"
            ));
        }
        let reasons = serde_json::from_str::<Value>(&resp)
            .ok()
            .and_then(|v| v["CancellationReasons"].as_array().cloned())
            .unwrap_or_default();
        if reasons.len() != 3 || reasons[2]["Code"] != "ConditionalCheckFailed" {
            return Err(format!(
                "node {node}: unexpected CancellationReasons for the cancel case: {reasons:?}"
            ));
        }
        for pk in [&a_pk, &b_pk] {
            let get_body = json!({
                "ConsistentRead": true,
                "TableName": table0,
                "Key": {"pk": {"S": pk}, "sk": {"S": "v"}},
            })
            .to_string();
            let (status, body) =
                cluster.dynamo(node, "DynamoDB_20120810.GetItem", get_body.as_bytes());
            if status != 200 || body.contains("\"Item\"") {
                return Err(format!(
                    "node {node}: a cancelled transaction still wrote {pk}: \
                     status={status} body={body}"
                ));
            }
        }

        // --- (2)/(3)/(4): a committed transaction mixing `Put`+`Delete`+
        // `Update` (spanning BOTH tables when the cell has two) with a
        // passing `ConditionCheck` and a `ClientRequestToken` — the commit
        // case, then an identical retry (idempotency: cached, no re-run),
        // then a mismatched retry (rejected).
        let to_delete_pk = format!("transact-probe-{node}-todelete");
        let seed_delete = json!({
            "TableName": table0,
            "Item": {"pk": {"S": to_delete_pk}, "sk": {"S": "v"}, "items": one_element_list(0)},
        })
        .to_string();
        let (status, resp) =
            cluster.dynamo(node, "DynamoDB_20120810.PutItem", seed_delete.as_bytes());
        if status != 200 {
            return Err(format!(
                "node {node}: transact-probe delete-seed failed: status={status} body={resp}"
            ));
        }

        let put_pk = format!("transact-probe-{node}-put");
        let counter_pk = format!("transact-probe-{node}-counter");
        let token = format!("transact-probe-tok-{node}");
        let commit_body = |put_val: u64| {
            json!({
                "ClientRequestToken": token,
                "TransactItems": [
                    {"Put": {"TableName": table0,
                        "Item": {"pk": {"S": put_pk}, "sk": {"S": "v"},
                                 "items": one_element_list(put_val)}}},
                    {"Delete": {"TableName": table0,
                        "Key": {"pk": {"S": to_delete_pk}, "sk": {"S": "v"}}}},
                    {"Update": {"TableName": table1,
                        "Key": {"pk": {"S": counter_pk}, "sk": {"S": "v"}},
                        "UpdateExpression": "ADD hits :one",
                        "ExpressionAttributeValues": {":one": {"N": "1"}}}},
                    {"ConditionCheck": {"TableName": table1,
                        "Key": {"pk": {"S": "__guard__"}, "sk": {"S": "__guard__"}},
                        "ConditionExpression": "attribute_exists(pk)"}},
                ],
            })
            .to_string()
        };

        let (status, resp) = cluster.dynamo(
            node,
            "DynamoDB_20120810.TransactWriteItems",
            commit_body(11).as_bytes(),
        );
        if status != 200 {
            return Err(format!(
                "node {node}: the mixed Put+Delete+Update commit failed: status={status} body={resp}"
            ));
        }
        let put_get = json!({
            "ConsistentRead": true,
            "TableName": table0,
            "Key": {"pk": {"S": put_pk}, "sk": {"S": "v"}},
        })
        .to_string();
        let (status, body) = cluster.dynamo(node, "DynamoDB_20120810.GetItem", put_get.as_bytes());
        let seen = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|v| v.get("Item").map(decode_items_attr))
            .unwrap_or_default();
        if status != 200 || seen != vec![11] {
            return Err(format!(
                "node {node}: committed Put not visible: status={status} body={body}"
            ));
        }
        let del_get = json!({
            "ConsistentRead": true,
            "TableName": table0,
            "Key": {"pk": {"S": to_delete_pk}, "sk": {"S": "v"}},
        })
        .to_string();
        let (status, body) = cluster.dynamo(node, "DynamoDB_20120810.GetItem", del_get.as_bytes());
        if status != 200 || body.contains("\"Item\"") {
            return Err(format!(
                "node {node}: committed Delete left the item present: status={status} body={body}"
            ));
        }
        let hits = read_hits_counter(cluster, table1, &counter_pk)?;
        if hits != 1 {
            return Err(format!(
                "node {node}: committed Update counter is {hits}, expected 1"
            ));
        }

        // (3) an identical retry, same token: cached — 200, no re-run (the
        // counter must stay 1, not 2).
        let (status, resp) = cluster.dynamo(
            node,
            "DynamoDB_20120810.TransactWriteItems",
            commit_body(11).as_bytes(),
        );
        if status != 200 {
            return Err(format!(
                "node {node}: the same-token retry failed: status={status} body={resp}"
            ));
        }
        let hits = read_hits_counter(cluster, table1, &counter_pk)?;
        if hits != 1 {
            return Err(format!(
                "node {node}: a same-token retry re-ran the transaction — counter is {hits}, \
                 expected 1"
            ));
        }

        // (4) the same token, a genuinely DIFFERENT payload: rejected.
        let (status, resp) = cluster.dynamo(
            node,
            "DynamoDB_20120810.TransactWriteItems",
            commit_body(12).as_bytes(),
        );
        if status != 400 || !resp.contains("IdempotentParameterMismatchException") {
            return Err(format!(
                "node {node}: a mismatched same-token retry should be rejected: \
                 status={status} body={resp}"
            ));
        }
    }
    Ok(node_count)
}

// ---------------------------------------------------------------------------
// The scenario runner.
// ---------------------------------------------------------------------------

struct ScenarioResult {
    cycles: CheckReport,
    durability: CheckReport,
    convergence: CheckReport,
    /// Every recorded `ConsistentRead: false` observation is a prefix of
    /// the converged final state — see the module doc's own
    /// "read-consistency modeling decision" section.
    eventual_prefix: CheckReport,
    ok_writes: usize,
    nonempty_reads: usize,
    non_hosting_ok_writes: usize,
    eventual_reads: usize,
    delete_probe: Result<usize, String>,
    batch_probe: Result<usize, String>,
    transact_probe: Result<usize, String>,
    transact_write_oks: usize,
    transact_get_oks: usize,
}

fn combine_reports(seed: u64, reports: impl Iterator<Item = CheckReport>) -> CheckReport {
    let mut violations = Vec::new();
    for r in reports {
        violations.extend(r.violations);
    }
    CheckReport {
        ok: violations.is_empty(),
        violations,
        seed,
    }
}

/// Read every known key's raw stored value straight off `node`'s own local
/// engine (never routed) — the direct-decode sibling of
/// `sim_cluster_corpus.rs`'s own `final_state`, decoding this corpus's real
/// `animus_item`-encoded stored item instead of that file's own
/// hand-rolled `u64` list encoding.
fn final_state(
    handle: &SimClusterHandle,
    tablets: &BTreeMap<u64, TabletId>,
    node: u64,
    tables: usize,
    keyspace: u64,
) -> BTreeMap<Key, Vec<u64>> {
    let mut map = BTreeMap::new();
    for t in 0..tables as u64 {
        let Some(&tablet) = tablets.get(&t) else {
            continue;
        };
        for k in 0..keyspace {
            let key = t * TABLE_KEY_STRIDE + k;
            let (_, pk, sk) = table_pk_sk(key);
            let list = block_on(handle.local_value(node, tablet, &pk, &sk))
                .map(|b| decode_engine_items(&b))
                .unwrap_or_default();
            map.insert(key, list);
        }
    }
    map
}

/// `tablet`'s CURRENTLY, ACTUALLY hosting node set — a live query via
/// [`SimCluster::hosted_tablets`], never [`SimClusterHandle::replicas_of`]'s
/// own creation-time snapshot (`SimCluster::create_table_with_replication`'s
/// own bookkeeping, frozen the instant the table is created and never
/// refreshed afterward — see that method's own doc for why it's a
/// deliberately static convenience, not a live read). **Found necessary by
/// this file's own C-06 PR 4 investigation**: a tokened `TransactWriteItems`
/// auto-provisions the internal `__animus_txn_idempotency` table
/// (`dynamo.rs::ensure_txn_idempotency_table`) at its own default RF, which
/// shifts every node's own TOTAL hosted-tablet count — on `dynamowire_
/// forward_heavy` (4 nodes, RF 2) this was enough to push the fixture's
/// real per-node `Reconciler`/`rebalance_placement` (ADR 0029's max−min ≤ 1
/// policy, evaluated per-node across ALL of that node's hosted tablets, not
/// per-table) into moving the corpus's OWN modeled table off the node the
/// static snapshot still named, onto a different one — a real, correct
/// rebalance the D4 PR1 entry's own "no existing cell ever triggers one"
/// claim never anticipated, since no cell before this PR ever provisioned a
/// second table via anything but its own explicit `create_table_with_
/// replication` call. Checking `final_state` against the STALE node left
/// [`run_scenario`] reading an empty, no-longer-a-replica engine and
/// reporting every acknowledged write against it as "lost" — a corpus-side
/// staleness bug, not a real durability violation (see this module's own
/// "TransactWriteItems/TransactGetItems" section's closing note, and
/// `docs/engineering-lessons.md`'s matching entry, for the full incident).
fn live_replicas(cluster: &SimCluster, node_count: usize, tablet: TabletId) -> Vec<u64> {
    (0..node_count as u64)
        .filter(|&n| cluster.hosted_tablets(n).contains(&tablet))
        .collect()
}

fn check_eventual_reads_are_prefixes(
    seed: u64,
    observations: &[(Key, Vec<u64>)],
    final_state: &BTreeMap<Key, Vec<u64>>,
) -> CheckReport {
    let mut violations = Vec::new();
    for (key, observed) in observations {
        let converged = final_state.get(key).cloned().unwrap_or_default();
        if !converged.starts_with(observed) {
            violations.push(format!(
                "eventually-consistent read of key {key} observed {observed:?}, not a \
                 prefix of the converged final state {converged:?}"
            ));
        }
    }
    if violations.is_empty() {
        CheckReport {
            ok: true,
            violations,
            seed,
        }
    } else {
        CheckReport {
            ok: false,
            violations,
            seed,
        }
    }
}

/// Force any outstanding transaction intent, on any key this scenario's
/// workload could have touched, toward resolution — a **model-only**
/// diagnostic/repair pass (never fed into `shared`'s own history, exactly
/// like [`run_delete_probe`]/[`run_batch_write_probe`]/
/// [`run_transact_probe`]) that has to run before this file's own raw
/// `local_get`-based [`final_state`] can be trusted at all once
/// `TransactWriteItems` is part of the workload.
///
/// **Why this exists — a real finding from this file's own C-06 PR 4
/// investigation, resolved as a corpus-model gap, not a product bug.**
/// `RaftKvNode::local_get`'s OWN documented contract (`animus-cp-data::
/// lib.rs`) is that a key currently covered by a `Pending` transaction
/// intent reads as **absent**, not as whatever value was committed
/// *before* that intent was staged — a deliberate, correct design for its
/// stated job (observing genuinely-applied state), but exactly wrong for
/// `final_state`'s own "is the acknowledged value durably present"
/// question the instant a workload can leave an intent Pending for a long
/// time. And this corpus's own random `TransactWriteItems` mix (drawing
/// its two keys from a client's own already-written `owned` set) can do
/// exactly that: a `LeaderCrash`/`FollowerCrash`/`StopRestart`/
/// `LeaderPartition`/`SplitBrain` fault landing on the SAME tablet a
/// transact write's own coordinator call is mid-flight against can leave
/// that transaction genuinely in-doubt — and, for every single-table cell
/// in this corpus, BOTH of that transaction's own actions land on the
/// SAME tablet (a "self-transaction": the anchor and its one participant
/// coincide). That combination has no path to resolution in THIS fixture
/// specifically: `SimCluster` never spawns `animusd::txn_resolver_loop`
/// (production's own background sweep, which would eventually resolve
/// ANY abandoned transaction regardless of who reads what), and the
/// on-demand recovery-via-read path (`ClientCtx::confirm_or_push`,
/// `read_path.rs`) is reached from the **plain** single-key `GetItem` path
/// (`cp_get_local_resolving_inner`) only for a **foreign** intent (a
/// different tablet than the reader's own) — a local (same-tablet) Pending
/// intent instead takes `linearizable_get_served`'s bounded *blocking*
/// chase, which never calls `confirm_or_push`/`txn_recover` at all, so a
/// same-tablet self-transaction's own local intent never gets pushed by an
/// ordinary read no matter how many are issued or how long this corpus
/// waits (confirmed directly: extending this scenario's own convergence
/// window from 15s to 120s changed nothing). **`TransactGetItems`'s own
/// read primitive is the one path that resolves a LOCAL intent exactly
/// like a foreign one** (`ClientCtx::cp_get_local_snapshot`'s shared
/// `FastRead::Pending(info) | FastRead::Foreign(info)` arm, `read_path.rs`
/// — both call `confirm_or_push` identically), so one `TransactGetItems`
/// covering every key this scenario could have written pushes any
/// outstanding intent toward its real decision as a side effect,
/// regardless of whether the read itself ever reports a clean quiesced
/// answer. Issued once, after `DRAIN`, from node 0 — a genuine, resolving
/// side effect the underlying transaction protocol already guarantees is
/// safe to invoke any number of times on an already-decided key. See this
/// module's own "TransactWriteItems/TransactGetItems" doc section's
/// closing note and `docs/engineering-lessons.md`'s matching entry for the
/// full incident (seed `13022590114329469744`,
/// `dynamowire_leader_crash_s22`).
fn force_resolve_all_keys(cluster: &mut SimCluster, table_names: &[String], keyspace: u64) {
    let mut gets: Vec<Value> = Vec::new();
    for (t, table) in table_names.iter().enumerate() {
        for k in 0..keyspace {
            let (_, pk, sk) = table_pk_sk(t as u64 * TABLE_KEY_STRIDE + k);
            gets.push(json!({
                "Get": {"TableName": table, "Key": {"pk": {"S": pk}, "sk": {"S": sk}}},
            }));
            if gets.len() == 100 {
                let body = json!({"TransactItems": gets}).to_string();
                let _ = cluster.dynamo(0, "DynamoDB_20120810.TransactGetItems", body.as_bytes());
                gets = Vec::new();
            }
        }
    }
    if !gets.is_empty() {
        let body = json!({"TransactItems": gets}).to_string();
        let _ = cluster.dynamo(0, "DynamoDB_20120810.TransactGetItems", body.as_bytes());
    }
}

fn run_scenario(s: &Scenario) -> ScenarioResult {
    let mut cluster = SimCluster::new(s.seed, s.nodes, s.replication);
    // Deterministic per-scenario executor-cost regression — see
    // `SCENARIO_TIMER_FIRES_BUDGET`'s own doc for the full account. Read
    // right after construction (excluding the cluster's own bring-up cost)
    // and again right before returning; asserted at the bottom of this
    // function, once every table/probe cost this scenario paid is in.
    let cost_before = cluster.sim_stats();
    let table_names: Vec<String> = (0..s.tables).map(|t| format!("t{t}")).collect();
    let mut tablets: BTreeMap<u64, TabletId> = BTreeMap::new();
    for (i, name) in table_names.iter().enumerate() {
        let tablet = cluster.create_table_with_replication(name, s.replication);
        tablets.insert(i as u64, tablet);
    }
    let primary_tablet = tablets
        .get(&0)
        .cloned()
        .expect("table 0 must exist — every scenario creates at least one table");

    // A `__guard__` marker item on every table, seeded once, before the
    // workload starts, and never touched again — the always-present target
    // every `ConditionCheck` in this file's own randomized `TransactWriteItems`
    // mix and in `run_transact_probe` checks against (see the module doc's
    // own "TransactWriteItems/TransactGetItems" section). Its own `sk`
    // ("__guard__") never parses as `key_from_table_and_sk`'s `item-{n}`
    // shape, so it is invisible to `Query`/`Scan`'s own `Mop::Read`
    // derivation, exactly like every other probe's own disjoint namespace.
    for name in &table_names {
        let body = json!({
            "TableName": name,
            "Item": {"pk": {"S": "__guard__"}, "sk": {"S": "__guard__"}, "seeded": {"BOOL": true}},
        })
        .to_string();
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(
            status, 200,
            "seed={}: __guard__ seed on {name} failed: {resp}",
            s.seed
        );
    }

    let shared = Arc::new(Shared {
        rec: Mutex::new(Recorder::new(s.seed)),
        next_value: Mutex::new(0),
        ok_writes_by_node: Mutex::new(BTreeMap::new()),
        eventual_reads: Mutex::new(Vec::new()),
        transact_write_oks: Mutex::new(0),
        transact_get_oks: Mutex::new(0),
    });

    let handle = cluster.handle();
    for c in 0..s.clients {
        let env = cluster.client_env(c as u64);
        let handle = handle.clone();
        let shared = Arc::clone(&shared);
        let (tables, rounds, keyspace, read_pct, node_count, clients) = (
            s.tables, s.rounds, s.keyspace, s.read_pct, s.nodes, s.clients,
        );
        let proc = c as Process;
        env.clone().spawn_task(async move {
            client_loop(
                env, handle, shared, proc, clients, rounds, tables, keyspace, read_pct, node_count,
            )
            .await;
        });
    }

    cluster.run_for(SETTLE);

    let mut elapsed = Duration::ZERO;
    for (at, nem) in s.faults.clone() {
        if at > elapsed {
            cluster.run_for(at - elapsed);
            elapsed = at;
        }
        nem.apply(&mut cluster, primary_tablet);
    }
    if !s.window.is_zero() {
        cluster.run_for(s.window);
    }
    cluster.heal_all();
    cluster.run_for(DRAIN);
    force_resolve_all_keys(&mut cluster, &table_names, s.keyspace);

    let history = shared
        .rec
        .lock()
        .expect("recorder poisoned")
        .history()
        .clone();
    let cycles = check_cycles(&history);

    let all_states = |c: &SimCluster| -> Vec<BTreeMap<Key, Vec<u64>>> {
        let h = c.handle();
        live_replicas(c, s.nodes, primary_tablet)
            .iter()
            .map(|&n| final_state(&h, &tablets, n, s.tables, s.keyspace))
            .collect()
    };
    let mut states = all_states(&cluster);
    let mut durability = combine_reports(
        s.seed,
        states.iter().map(|st| check_durability(&history, st)),
    );
    let mut convergence = combine_reports(
        s.seed,
        states[1..]
            .iter()
            .map(|st| check_convergence(s.seed, &states[0], st)),
    );
    let poll_deadline_steps = CONVERGENCE_BUDGET.as_millis() / CONVERGENCE_POLL_STEP.as_millis();
    let mut polled: u128 = 0;
    while !(durability.ok && convergence.ok) && polled < poll_deadline_steps {
        cluster.run_for(CONVERGENCE_POLL_STEP);
        states = all_states(&cluster);
        durability = combine_reports(
            s.seed,
            states.iter().map(|st| check_durability(&history, st)),
        );
        convergence = combine_reports(
            s.seed,
            states[1..]
                .iter()
                .map(|st| check_convergence(s.seed, &states[0], st)),
        );
        polled += 1;
    }

    let eventual_observations = shared
        .eventual_reads
        .lock()
        .expect("eventual_reads poisoned")
        .clone();
    let eventual_prefix =
        check_eventual_reads_are_prefixes(s.seed, &eventual_observations, &states[0]);

    let ok_writes = history
        .ok_entries()
        .flat_map(|e| &e.mops)
        .filter(|m| matches!(m, Mop::Append { .. }))
        .count();
    let nonempty_reads = history
        .ok_entries()
        .filter(|e| {
            e.mops
                .iter()
                .any(|m| matches!(m, Mop::Read { observed: Some(l), .. } if !l.is_empty()))
        })
        .count();
    let ok_writes_by_node = shared
        .ok_writes_by_node
        .lock()
        .expect("ok_writes_by_node poisoned")
        .clone();
    let final_replicas = live_replicas(&cluster, s.nodes, primary_tablet);
    let non_hosting_ok_writes: usize = (0..s.nodes as u64)
        .filter(|n| !final_replicas.contains(n))
        .map(|n| ok_writes_by_node.get(&n).copied().unwrap_or(0))
        .sum();

    let delete_probe = run_delete_probe(&mut cluster, &table_names[0], s.nodes);
    let batch_probe = run_batch_write_probe(&mut cluster, &table_names[0], s.nodes);
    let transact_probe = run_transact_probe(&mut cluster, &table_names, s.nodes);

    let cost_after = cluster.sim_stats();
    let timer_fires = cost_after
        .timer_fires
        .saturating_sub(cost_before.timer_fires);
    assert!(
        timer_fires <= SCENARIO_TIMER_FIRES_BUDGET,
        "scenario={} seed={}: executor cost regression — this scenario fired \
         {timer_fires} sim timeline entries (timers + message deliveries), \
         budget is {SCENARIO_TIMER_FIRES_BUDGET} (task_polls delta: {}); see \
         `SCENARIO_TIMER_FIRES_BUDGET`'s own doc for what this catches and \
         how the budget was picked",
        s.name,
        s.seed,
        cost_after.task_polls.saturating_sub(cost_before.task_polls),
    );

    ScenarioResult {
        cycles,
        durability,
        convergence,
        eventual_prefix,
        ok_writes,
        nonempty_reads,
        non_hosting_ok_writes,
        eventual_reads: eventual_observations.len(),
        delete_probe,
        batch_probe,
        transact_probe,
        transact_write_oks: *shared
            .transact_write_oks
            .lock()
            .expect("transact_write_oks poisoned"),
        transact_get_oks: *shared
            .transact_get_oks
            .lock()
            .expect("transact_get_oks poisoned"),
    }
}

fn run_scenario_identified(s: &Scenario) -> ScenarioResult {
    eprintln!("scenario={} seed={}", s.name, s.seed);
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_scenario(s))) {
        Ok(r) => r,
        Err(payload) => {
            let msg = payload
                .downcast_ref::<&str>()
                .map(|m| m.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "<non-string panic payload>".to_string());
            panic!("scenario={} seed={}: {msg}", s.name, s.seed);
        }
    }
}

fn scenario_failed(r: &ScenarioResult) -> bool {
    !r.cycles.ok
        || !r.durability.ok
        || !r.convergence.ok
        || !r.eventual_prefix.ok
        || r.delete_probe.is_err()
        || r.batch_probe.is_err()
        || r.transact_probe.is_err()
}

fn assert_scenario_ok(s: &Scenario, r: &ScenarioResult) {
    assert!(
        r.cycles.ok,
        "scenario {} not serializable: {:?} (seed={})",
        s.name, r.cycles.violations, s.seed
    );
    assert!(
        r.durability.ok,
        "scenario {} lost an acked append: {:?} (seed={})",
        s.name, r.durability.violations, s.seed
    );
    assert!(
        r.convergence.ok,
        "scenario {} did not converge: {:?} (seed={})",
        s.name, r.convergence.violations, s.seed
    );
    assert!(
        r.eventual_prefix.ok,
        "scenario {} had a non-prefix eventually-consistent read: {:?} (seed={})",
        s.name, r.eventual_prefix.violations, s.seed
    );
    assert!(
        r.delete_probe.is_ok(),
        "scenario {} delete probe failed: {:?} (seed={})",
        s.name,
        r.delete_probe,
        s.seed
    );
    assert!(
        r.batch_probe.is_ok(),
        "scenario {} batch-write probe failed: {:?} (seed={})",
        s.name,
        r.batch_probe,
        s.seed
    );
    assert!(
        r.transact_probe.is_ok(),
        "scenario {} transact probe failed: {:?} (seed={})",
        s.name,
        r.transact_probe,
        s.seed
    );
}

// ---------------------------------------------------------------------------
// Failure minimization (ADR 0061 rung B4) — mirrors `sim_cluster_corpus.rs`'s
// own wiring exactly.
// ---------------------------------------------------------------------------

fn scenario_candidates(s: &Scenario) -> Vec<Scenario> {
    let mut out = Vec::new();
    if !s.faults.is_empty() {
        out.push(Scenario {
            faults: Vec::new(),
            ..s.clone()
        });
    }
    if !s.window.is_zero() {
        out.push(Scenario {
            window: Duration::ZERO,
            ..s.clone()
        });
    }
    if s.rounds > 1 {
        out.push(Scenario {
            rounds: (s.rounds / 2).max(1),
            ..s.clone()
        });
    }
    if s.keyspace > 1 {
        out.push(Scenario {
            keyspace: (s.keyspace / 2).max(1),
            ..s.clone()
        });
    }
    if s.clients > 1 {
        out.push(Scenario {
            clients: s.clients - 1,
            ..s.clone()
        });
    }
    out
}

fn shrink_and_report(s: &Scenario) -> ShrinkReport<Scenario> {
    let report = shrink::minimize(
        s.clone(),
        scenario_candidates,
        |cand| scenario_failed(&run_scenario(cand)),
        shrink::budget_from_env(),
    );
    eprintln!("{}", shrink::describe(&s.name, &report));
    match shrink::replay_json(&report) {
        Ok(json) => {
            eprintln!("  replay handle (JSON): {json}");
            eprintln!(
                "  Replay directly: ANIMUS_SHRINK_REPLAY='{json}' \\\n    \
                 cargo test -p animusd --lib sim_cluster_dynamo_shrink_replay \\\n    \
                 -- --ignored --nocapture"
            );
        }
        Err(e) => eprintln!("  (failed to serialize replay handle: {e})"),
    }
    report
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[test]
fn dynamo_wire_baseline_is_consistent() {
    let scenario = base_workload("dynamowire_baseline", 3, 3, 1, vec![], Duration::ZERO);
    let r = run_scenario(&scenario);
    assert_scenario_ok(&scenario, &r);
    assert!(r.ok_writes > 0, "no acked writes — vacuous run");
    assert!(
        r.nonempty_reads > 0,
        "no non-empty reads — checker had nothing to chew"
    );
}

#[test]
fn sim_cluster_dynamo_corpus_is_consistent() {
    let scenarios = corpus();
    let mut total_ok_writes = 0usize;
    let mut total_eventual_reads = 0usize;
    let mut total_transact_write_oks = 0usize;
    let mut total_transact_get_oks = 0usize;
    for s in &scenarios {
        let r = run_scenario_identified(s);
        if scenario_failed(&r) && shrink::shrink_enabled() {
            shrink_and_report(s);
        }
        assert_scenario_ok(s, &r);
        assert!(
            r.ok_writes > 0,
            "scenario {} did no acked writes — vacuous run (seed={})",
            s.name,
            s.seed
        );
        if s.nodes > s.replication {
            assert!(
                r.non_hosting_ok_writes > 0,
                "scenario {}: no write issued from a non-hosting node ever succeeded \
                 (seed={}) — forwarding may be broken, or the workload never actually \
                 exercised it",
                s.name,
                s.seed
            );
        }
        total_ok_writes += r.ok_writes;
        total_eventual_reads += r.eventual_reads;
        total_transact_write_oks += r.transact_write_oks;
        total_transact_get_oks += r.transact_get_oks;
    }
    assert!(
        total_ok_writes > scenarios.len(),
        "corpus too vacuous: only {total_ok_writes} acked writes across {} scenarios",
        scenarios.len()
    );
    assert!(
        total_transact_write_oks > 0,
        "corpus never issued a successful randomized TransactWriteItems — the atomicity/\
         isolation coverage this PR adds has nothing to prove (the dedicated \
         run_transact_probe still ran on every scenario regardless — this is about the \
         RANDOM op mix specifically)"
    );
    assert!(
        total_transact_get_oks > 0,
        "corpus never issued a successful randomized TransactGetItems — the isolation \
         coverage this PR adds has nothing to prove"
    );
    assert!(
        total_eventual_reads > 0,
        "corpus never exercised a ConsistentRead:false read — the eventual-read \
         prefix check has nothing to prove"
    );
}

/// Coverage guard: every named fault class plus the fault-free forwarding
/// and multi-table cells must still be present at the frozen depth —
/// mirrors `sim_cluster_corpus.rs`'s own `sim_cluster_corpus_covers_every_
/// cell_shape`.
#[test]
fn dynamo_wire_corpus_covers_the_fault_matrix() {
    let cells = corpus_cells();
    assert_eq!(cells.len(), 8, "expected exactly 8 named cells");
    assert!(
        cells
            .iter()
            .any(|s| s.faults.is_empty() && s.tables == 1 && s.nodes == s.replication)
    );
    for nem in [
        Nemesis::LeaderCrash,
        Nemesis::FollowerCrash,
        Nemesis::StopRestart,
        Nemesis::LeaderPartition,
        Nemesis::SplitBrain,
    ] {
        assert!(
            cells
                .iter()
                .any(|s| s.faults.iter().any(|(_, f)| *f == nem)),
            "no cell schedules {nem:?}"
        );
    }
    assert!(
        cells.iter().any(|s| s.nodes > s.replication),
        "no forward-heavy (nodes > replication) cell"
    );
    assert!(cells.iter().any(|s| s.tables > 1), "no multi-table cell");
}

/// The replay entry point named in every `ANIMUS_SHRINK` report
/// (`shrink_and_report`'s printed instructions) — mirrors
/// `sim_cluster_corpus.rs`'s own `sim_cluster_shrink_replay`.
#[test]
#[ignore = "opt-in replay entry point — set ANIMUS_SHRINK_REPLAY to a shrink report's printed JSON"]
fn sim_cluster_dynamo_shrink_replay() {
    let Ok(json) = std::env::var("ANIMUS_SHRINK_REPLAY") else {
        eprintln!(
            "sim_cluster_dynamo_shrink_replay: skipped — set ANIMUS_SHRINK_REPLAY to a \
             shrink report's printed JSON to replay it"
        );
        return;
    };
    let scenario: Scenario =
        serde_json::from_str(&json).expect("ANIMUS_SHRINK_REPLAY must be a Scenario JSON blob");
    let r = run_scenario(&scenario);
    eprintln!(
        "replayed '{}' (seed={}): cycles.ok={} durability.ok={} convergence.ok={} \
         eventual_prefix.ok={} delete_probe={:?} batch_probe={:?} transact_probe={:?} \
         ok_writes={} transact_write_oks={} transact_get_oks={}",
        scenario.name,
        scenario.seed,
        r.cycles.ok,
        r.durability.ok,
        r.convergence.ok,
        r.eventual_prefix.ok,
        r.delete_probe,
        r.batch_probe,
        r.transact_probe,
        r.ok_writes,
        r.transact_write_oks,
        r.transact_get_oks,
    );
    assert!(
        scenario_failed(&r),
        "replayed scenario '{}' (seed={}) did NOT reproduce the failure",
        scenario.name,
        scenario.seed
    );
}
