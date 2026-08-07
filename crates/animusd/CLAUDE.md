# CLAUDE.md — animusd

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The node server. A **lib + bin**: `lib.rs` assembles a runnable AnimusDB node
over `ProdEnv` (the first real use of the production seam); `main.rs` is a thin
CLI wrapper. `animus-cli` depends on this crate for the client protocol types.

## Entry points

- `Node::bind` → `BoundNode::start` — two-phase construction (bind listeners,
  then install the peer address book and start protocols), so a cluster can use
  ephemeral ports and exchange addresses afterward.
- `config::ClusterConfig` — the per-process deployment config (every node's six
  addresses). Node ids follow a fixed convention from the index (control `i`,
  raftkv `300+i`) so processes agree without listing ids. `run_node(config, index,
  dir)` binds *this* node and starts it.
- `bind_cluster` / `start_cluster` — spin up an in-process cluster (the binary's
  `--cluster N` mode and `tests/cluster.rs`).
- `ClientRequest` / `ClientResponse` + `read_frame` / `write_frame` — the
  length-prefixed JSON client protocol (reused by `animus-cli`).
- `dynamo` module — the **DynamoDB JSON-over-HTTP endpoint** (a fifth listener
  per node). A hand-rolled HTTP/1.1 server decodes `X-Amz-Target` +
  AttributeValue-JSON via `animus_dynamo::wire`, then routes through the **same
  `ClientCtx`** as the plain-TCP API. v1 (ADR 0019): reads/writes/scans go to the
  **CP plane** (`ClientCtx::cp_read`/`cp_write`/`cp_scan`), not the AP coordinator.
- `admin` module — the **admin / debug HTTP-JSON endpoint** (ADR 0020), a dedicated
  sixth listener (`RoleAddrs.admin`). Read-only introspection (config, status, both
  Raft layers, LSM/WAL debug, metrics, health) + gated operator actions
  (split/flush/compact/reconfigure/drain). `http` module — the shared hand-rolled
  HTTP/1.1 helpers (request parser + response writers) used by both `dynamo` and
  `admin`.
- `cql` module — the **CQL (Cassandra) v4 binary-protocol endpoint** (a
  listener per node). A hand-rolled framed server does the `STARTUP → READY` /
  `OPTIONS → SUPPORTED` handshake and runs `QUERY`/`PREPARE`/`EXECUTE` via the
  pure `animus_cql` crate (a typed `CREATE KEYSPACE`/`USE`/`CREATE TABLE` schema
  catalog incl. **clustering/compound primary keys**, typed
  `INSERT`/`SELECT`/`UPDATE`/`DELETE` + prepared statements), routing through the
  **same `ClientCtx`** as the other edges. v1 (ADR 0019): reads/writes go to the
  **CP plane** (`cp_read`/`cp_write`/`cp_delete`), which is linearizable — the
  requested **consistency level is accepted but moot** (CP is at least as strong as
  any level; it no longer sizes a quorum). A *partition* is one CP value, so
  `INSERT`/`UPDATE`/`DELETE` are read-modify-write of that value **under the coord
  lock** (which serializes a node's RMWs so the linearizable read + CP write are
  atomic per node; the Raft index is the MVCC version, so no client-assigned
  version), and a `DELETE` that empties the partition issues a CP tombstone
  (`cp_delete`). The keyspace set + prepared-statement store are **per-cluster edge
  state** (see below).
- `otel` module — OpenTelemetry-compatible distributed tracing (ADR 0027).
  `init_tracing(instance_id)` (called once, from `main.rs`) installs the process
  subscriber: the existing stdout `fmt` layer plus, when `OTEL_EXPORTER_OTLP_ENDPOINT`
  is set, an OTLP/HTTP span exporter — opt-in, no-op by default, same doctrine as
  the ADR 0015 metrics seam. `current_traceparent`/`set_parent_traceparent` are the
  inject/extract primitives `cp_forward`/`handle_client` use to carry trace context
  across a forwarded cross-process hop (see below). `init_tracing_with_endpoint` is
  the test-facing seam (explicit endpoint, no process-env mutation — `set_var` is
  `unsafe` and this workspace forbids `unsafe_code`); see
  `tests/otel_tracing.rs`. **Scoped to this crate only** — no other crate depends on
  `opentelemetry*`.

## What's non-obvious

- A node runs **two internal `ProdEnv` roles on distinct ids/ports** — control
  (Raft metadata, id `i`) and **raftkv** (the leaderful **CP** per-tablet Raft
  group, `300+i`, ADR 0017 #3a — the v1 data plane) — because one inbox is
  single-consumer. `ClusterConfig` assigns **six** consecutive ports per node (the two
  internal roles + client/dynamo/cql/**admin**, the admin port being ADR 0020). v1 (ADR 0019) is **CP-only**: the leaderless
  AP `data`/`coord` roles, `serve_replica`, anti-entropy, and hinted handoff are
  gone. The **client API is a plain request/reply TCP server**, *not* on the
  `Network`: a node that does not host the CP group leader **forwards** a data op to
  the leader's node over a fresh client connection (ADR 0017 #3b), so dynamic client
  addresses never touch the internal network.
- **CP routing (ADR 0017 #3a / v1 ADR 0019).** The data path is the **leaderful
  per-tablet Raft group** (`animus-cp-data`), reached through five `ClientCtx`
  primitives that all resolve the leader the same way (`cp_route`): `cp_read`
  (linearizable ReadIndex), `cp_write` / `cp_delete` (Raft-committed, waited to
  durable+applied — durable-before-ack), `cp_scan` (linearizable range read), and
  **`cp_batch_write`** (bulk-write batching, ADR 0017): it **groups keys by tablet**
  and commits each group as **one `KvCommand::Batch` Raft entry** on that tablet's
  group leader (one consensus round for the whole group; forwarded via
  `ClientRequest::PutBatch` if this node isn't the leader), waited to durable+applied.
  Atomic **within** a tablet (one entry), non-atomic **across** tablets — matching
  DynamoDB `BatchWriteItem` semantics. The DynamoDB `BatchWriteItem` edge (a
  `Delete` is a tombstone-*value* write, so puts + deletes ride the same batch) and
  the admin bulk seeder (`SEED_BATCH_SIZE` keys per entry) both route through it.
  `cp_route` serves **locally** if this node hosts the leader, **forwards** to the
  leader's node if a local replica gives a leader hint + a `client_route` exists
  (ADR 0017 #3b cross-process, wrapped in `ClientRequest::Forwarded { request,
  traceparent }`, one hop — the `traceparent` field is ADR 0027: `handle_client`
  wraps every accepted request in a `client_request` span, `cp_forward` injects
  that span's W3C trace context onto the wire via `otel::current_traceparent`, and
  the receiving node's `handle_client` re-parents its own `client_request` span
  from it via `otel::set_parent_traceparent` *before* dispatching to
  `cp_serve_forwarded` — so a forwarded write is one joined distributed trace
  across both nodes when OTLP export is enabled, `None`/no-op otherwise), and
  otherwise **waits** for the local group to elect (it never forwards a CP op to a
  non-leader — including itself — during election). **Every data op — the wire edges
  (DynamoDB, CQL) and the plain-client `Put`/`Get`/`Scan`/`Delete` — routes through
  these.** The optional `table` no longer selects a plane (there is only the CP
  plane); the single CP group covers the whole keyspace. The edges create their
  tables in `ReplicationMode::Cp` (the mode is recorded for truthfulness, but
  routing no longer depends on it). A just-proposed write is confirmed via a **local**
  read on the leader (not a quorum barrier — the leader applies only after a quorum
  commit + WAL fsync, so a local read reflecting the value means it's durable; a
  per-write barrier would not scale under concurrent load). The confirm loop polls
  at a **fine adaptive interval** (`CP_CONFIRM_POLL_INIT` ~200µs, doubling to a
  `CP_CONFIRM_POLL_MAX` 5ms ceiling), *not* the coarse 50ms `SCHEMA_POLL_INTERVAL` —
  paired with cp-data's wake-on-propose, a lone write returns in ~1ms instead of
  eating a fixed 50ms floor (`cp_plane.rs::single_write_latency_is_low`). At node start a node
  hosts the **bootstrap CP group** spanning the first `min(N, MAX_REPLICATION_FACTOR)`
  nodes' `raftkv` ids; a **tablet split** (Phase 2.2/2.4) then stands up additional
  co-resident groups, each backed by its own `LsmEngine` or `MemoryEngine` per
  [`StorageBackend`] (an enum-wrapped `CpGroup`), keyed by `TabletId` in the edge
  registry (Phase 2.1 tablet-keyed routing). `tests/cp_plane.rs` (in-process
  round-trip) + `tests/cp_cross_process.rs` (forwarding) + the dynamo/cql wire +
  schema tests all exercise the CP path. **Split tablets are re-hosted on restart**
  (#2): each node durably records the split tablets it hosts in a **`cp-hosted`
  marker** (`load_hosted_cp`/`save_hosted_cp` on the `raftkv` env disk — genuinely
  *local* state: which co-resident `db-t{id}-` engines physically exist here, not
  derivable from the tablet map, which records placement in stable base ids). At
  start `start_with` reads the marker, pre-populates the per-node `minted` set from
  it (so the bootstrap group re-applying a committed `Split` on WAL recovery does
  **not** mint the sibling twice — split crash-idempotency), and spawns `cp_rehost`
  for each recorded tablet: mint its sibling, recover its `db-t{id}-` engine +
  `raftkv.wal` via `start_seeded` with an **empty** seed (the data is already on
  disk — the seed only ever carries a *fresh* split's handoff), register it, and
  re-publish the sibling's new address. `tests/cp_rehost.rs` (split a tablet, restart
  the cluster, the upper-range key survives on the re-hosted group). **Address
  publish is cross-process** now: the split-seed + re-host paths publish via
  `ClientCtx::register_cp_addr`, which relays the registration to the control leader
  through `client_route` (a follower can't propose directly) — so the `ctx` is built
  before the CP hosting block and threaded into the hook + re-host. **Hosting is
  driven by the tablet map too** (D1): a per-node **join-host loop** stands up an
  *empty* co-resident group for any tablet newly *placed* on this node by the
  reconciler (`epoch > INITIAL`, so it never starts a fresh split child empty), which
  the leader then catches up via `InstallSnapshot` — closing the auto-replace cascade.
  All hosting state is bundled in a **`CpHostCtx`** so every group (bootstrap, split
  child, re-hosted, joined) carries a split hook, enabling **deep splits** (D3: a
  split tablet can be split again); member ids derive **flatly** from the base id
  (`cp_member_id`) at any depth. **Each node can host at most `CP_SIBLING_POOL + 1`
  co-resident tablets** (the bootstrap group + one pre-bound listener-pool slot per
  split child); the pool is **64**, sized for bulk-seed→auto-split sharding tests.
  Exceeding it panics the split-hook task (`Coresident::sibling`), leaving the
  over-cap tablet **leaderless** (writes to its range hang) — so keep the pool ahead
  of the tablet count. (Found via bulk-seed: a pool of 4 left a 6th tablet leaderless;
  the `ProdEnv` "send to unknown peer" log of the missing split member is the
  downstream symptom, now at `debug`.)
  **A split's two steps (control-plane `SplitTablet` metadata, then the data-plane
  `propose_split`) are not atomic, and `auto_split_loop` now accounts for that**: a
  step-2 failure (leader moved, no route — real under bulk-write-induced leader
  churn) used to be silently discarded, orphaning the tablet forever (visible in
  `Metadata.tablets` with a real range/replicas, but `leader: unknown` — no CP Raft
  group anywhere, so any read/write to its range hangs) and, since the source
  tablet's data never actually shrank, causing the loop to mint *more* orphans on
  later ticks. The loop now tracks step-1-committed/step-2-pending tablets in a
  `pending` map, retries step 2 with the same split key every tick until it
  succeeds (`propose_split` is idempotent per group, so this is always safe), and
  skips a tablet for a *fresh* split while it's already pending. `ClientCtx::trigger_split`
  (used by the one-shot admin/manual `SplitTablet` request) is unchanged — a human
  or script driving it can decide to retry on error — the factored
  `propose_split_metadata`/`propose_split_data` steps exist so `auto_split_loop` can
  drive the retry itself instead of going through the combined one-shot helper.
  **`propose_split_data` also confirms the split before reporting success** —
  `leader.propose_split(split_key)` returning `ProposeResult::Accepted` only means
  the entry was appended to the leader's local log, not that it committed (the same
  caveat as every other CP-data write; see `cp_put_local`'s doc), and under leader
  churn an accepted-but-uncommitted `Split` is silently truncated. Trusting
  `Accepted` alone was a second, independent bug behind the same "tablet exists in
  metadata but never gets a CP group" symptom the `pending` map above was built to
  fix — a step-2 "success" that wasn't real never got queued for retry either. Fixed
  by polling `RaftKvNode::applied_split_key()` (a confirm-by-key primitive, mirroring
  `engine_applied_index`) via the new `confirm_split` helper, comparing the *exact*
  key — not just "has this group split at all" — because under `--cluster N`'s
  shared edge state two nodes' auto-split loops can independently compute a
  *different* median for the same tablet in the same tick, and the group can only
  split once: a bare boolean would let the loser's confirm wrongly pass. `cp_split_here`
  (the forwarded-split handler) gets the same confirmation. Diagnosed with OpenTelemetry
  tracing added specifically for this (spans on `auto_split`/`split_metadata`/
  `split_data`/`cp_split_seed`/`cp_split_hook`) — the "accepted" log line looked
  identical whether the split was real or not; only correlating it against
  `/admin/raftkv` (no group ever appeared for several "accepted" splits) and the
  split-hook trace (it never fired for those) surfaced the gap. The `pending` map's
  retry also needed a matching update: a tablet whose own key lost the same-tick
  race must be dropped from `pending` (not retried forever — the group will never
  apply that key), detected via any local replica's `applied_split_key()` differing
  from the pending key.
  **The `pending` map's retry also had a retry-amplification bug, the split-path
  sibling of the bulk-seeder's (see the root `CLAUDE.md` engineering-practices
  entry):** it called `propose_split_data` (propose **and** confirm) fresh on
  every ~2s tick regardless of whether the previous attempt reached `Accepted`.
  `ProposeResult::Accepted` only means the `Split` entry reached the leader's
  log, not that it committed, so a bare confirm-timeout usually means "still
  committing" — proposing again appends a redundant `Split` entry, safe (splits
  are idempotent at apply time) but wasteful, doubling WAL/replication load
  under exactly the slow/contended conditions that caused the timeout in the
  first place. Fixed by `propose_and_confirm_split(leader, split_key,
  confirm_rounds)`: the pending-retry call (and `cp_split_here`, the
  cross-process forwarded-split handler, which can't tell whether its caller
  is about to retry) pass `confirm_rounds: 2`, polling the already-accepted
  entry a second time before the next tick would otherwise re-propose. The
  one-shot `trigger_split`/fresh-trigger call sites keep `confirm_rounds: 1`
  (`propose_split_data`'s default) — byte-identical behavior there.
- **`ClientCtx::propose_and_await` — the generic schema-proposal helper
  `propose_split_metadata` (step 1 of a split), `register_cp_addr`,
  `create_table_schema`, `replace_table_schema`, and `drop_table`/
  `drop_table_schema` all sit on top of — had the same retry-amplification
  shape as the step-2 `propose_split_data` bug above, just one layer further
  down.** It called `propose_schema` fresh on every 50ms poll tick regardless
  of whether the prior call had already reached a leader's log, for up to the
  full 10s `SCHEMA_COMMIT_TIMEOUT` — up to ~200 duplicate proposals per call
  (harmless to apply for an idempotent command like `SplitTablet`, but wasted
  WAL/replication work). Under `--auto-split`'s per-node trigger loop running
  concurrently on every node in `--cluster N` (see the topology-map entry on
  `auto_split_loop`), this is what turned a transient slow commit into a
  live-observed 10-minute-long stall of `SplitTablet` metadata never
  committing at all — three nodes' retry storms flooding the control-plane
  Raft log faster than one 10s window could drain. Fixed by having
  `propose_schema` return whether it believes the command reached a leader's
  log, and `propose_and_await` only resubmitting immediately when it knows the
  prior attempt went nowhere (otherwise backing off `SCHEMA_PROPOSE_PATIENCE`,
  1s). See the root `CLAUDE.md` engineering-practices entry for the general
  lesson (sweep the *shared primitive*, not just the two call sites a bug
  report named).
- **`auto_split_loop`'s "only the leader's host triggers" gate
  (`ctx.edge.cp_leader(tablet)`) is not node-scoped under `--cluster N`**:
  `ClusterEdgeState::cp_leader` returns the first registered handle for a
  tablet that `is_leader()` across the *whole* shared `raftkv` map, with no
  notion of which node is asking — so every node's `auto_split_loop` task sees
  `Some(leader)` simultaneously, and all of them can independently propose a
  fresh (possibly differently-keyed) `SplitTablet` for the same source tablet
  in the same tick. Since a source tablet's underlying CP group can only win
  its one-time data-plane split for a single key, every losing proposer's
  metadata-only tablet is permanently orphaned — live-observed as 3
  near-simultaneous step-1 commit failures for one tablet, and as a tablet's
  split id churning upward forever (an endless fresh-attempt/abandon cycle).
  Fixed with a **cluster-wide** (not per-node) claim,
  `ClusterEdgeState::claim_auto_split`/`release_auto_split`, that a loop must
  win before proposing a fresh split for a tablet — held through step 1, step
  2, and any pending retry, released only on a terminal outcome (success,
  step-1 failure, or abandonment). See the root `CLAUDE.md` engineering-
  practices entry for the general lesson (a shared-registry "does anyone
  satisfy this" query is not the same as a per-node gate, and the two only
  diverge under `--cluster N`).
- **The `pending` map's abandon path (see above) didn't refresh
  `last_triggered`, so an abandoned tablet was immediately re-eligible for a
  brand-new fresh split on the very next tick** instead of backing off for
  `AUTO_SPLIT_COOLDOWN` like a normal fresh trigger does. Under repeated
  contention on a tablet this let its split id climb every tick indefinitely
  (8→10→12…), each attempt abandoned in turn. Fixed by inserting into
  `last_triggered` on abandon too — see the root `CLAUDE.md`
  engineering-practices entry for the general lesson (a "give up" exit from a
  retry loop must leave the same rate-limit state a normal successful cycle
  would).
- **The "abandon" branch above stopped a losing proposer from retrying forever,
  but never cleaned up the metadata-only tablet id its own step 1 had already
  minted — that id was a *permanent* orphan, not just a transient one.** The
  root cause was one layer down, in `animus-control`: `MetaCommand::SplitTablet`
  applied unconditionally as long as `split_key` fell inside the source
  tablet's *current* range, with no CAS on its epoch (unlike its sibling
  `CasTabletReplicas`) — so two proposers racing to split the same tablet at
  the same epoch (two `auto_split_loop` instances despite `claim_auto_split`
  serializing *auto-triggered* attempts against each other, or any other racing
  caller) could both have their `SplitTablet` commit, each minting a `new_id`.
  Only one can ever get a real CP group (the tablet's own per-group Raft
  applies at most one `Split`, ever), so the loser's `new_id` was permanently
  `leader: unknown` — present in `Metadata.tablets`, invisible in
  `/admin/raftkv`, unreachable, forever (live-observed: two such tablets under
  `--cluster 3 --auto-split 2000` bulk-seed). Fixed at the source: `SplitTablet`
  now takes `expected_epoch` and is rejected on mismatch, exactly like
  `CasTabletReplicas` — so the loser's `propose_split_metadata` (step 1) itself
  now fails cleanly, hitting the *existing* "nothing was allocated, no orphan to
  track" path, and no second `new_id` is ever minted **for a same-epoch,
  concurrent race**. See the root `CLAUDE.md` engineering-practices entry (a new
  command mutating a resource another command already CASes needs the same
  guard) and `animus-control/CLAUDE.md`'s `SplitTablet` note.
  **This did not close the *sequential* case, and permanent orphans kept
  accumulating live under sustained `--auto-split` bulk-seed + leader churn.**
  The epoch CAS only rejects a second mint at the *same* epoch; nothing stops
  a *later* fresh trigger from minting a second child once the first mint's
  own epoch bump has already advanced the source's epoch — legitimate at
  propose time, but still fatal, since the underlying CP-data group can only
  ever apply one real `Split`. Whichever key doesn't win hits the abandon
  branch above, which correctly stops retrying but (as originally written)
  never removed the dead mint — so under real leader churn, `Metadata.tablets`
  accumulated permanent, unreachable orphans one abandonment at a time
  (live-observed: two orphans within ~10 minutes of one `--cluster 3
  --auto-split 2000` run, growing without bound). Fixed with
  `MetaCommand::DropOrphanTablet` (CAS-gated like its siblings; always safe,
  since a mint that never got `cp_split_seed`'d never held data) — both the
  abandon branch here and `trigger_split`'s own step-2 failure path (the
  one-shot manual/admin trigger, which can hit the identical "a different key
  already won" outcome) call `ClientCtx::drop_orphan_tablet` once they've
  confirmed via `applied_split_key()` that their own key lost. Deterministic
  regression (no timing race needed): `tests/cp_plane.rs::
  a_lost_split_race_does_not_leave_a_permanent_orphan` drives two *sequential*
  manual splits of the same tablet — the second's `split_key` is chosen
  strictly inside the source's already-narrowed post-first-split range, so its
  step 2 can never confirm — and asserts the resulting orphan is GC'd, not left
  dangling. The genuine live race (via `auto_split_loop`, needing real leader
  churn) is exercised only by the unit-tested pure CAS/apply logic in
  `animus-control`, per this file's "extract the invariant into a pure
  function" convention — reproducing the *exact* live timing on demand isn't
  tractable, but the sequential-mint precondition it depends on is.
  **Superseded by "a group can be split more than once" below**: a group no
  longer "can only ever apply one real `Split`," so two *sequential* manual
  splits of the same tablet (this entry's original repro) now both succeed
  instead of the second permanently failing — that specific reproduction
  stopped demonstrating a bug. The underlying hazard (a proposal's key no
  longer matching the group's current boundary, by the time step 2 is
  attempted) is still real and still needs the same GC, just via a different
  precondition: `applied_split_key()` was renamed `current_split_bound()` (see
  `animus-cp-data/CLAUDE.md`), and the test was rewritten to a mechanism that's
  still permanently unconfirmable under the new rules — a **data-plane-only**
  `CpSplit` at a *small* key first (bypassing metadata, so it can't be
  confirmed the normal way), then a **normal** manual split at a *larger* key:
  metadata mints a child fine (it never heard about the first split), but the
  data-plane boundary has already moved *below* the second key, which the
  monotonically-narrowing CAS can never re-widen past. `drop_orphan_tablet`
  also gained a hard safety gate on top of this (see its doc) once the
  confirm-by-key signal became ambiguous rather than certain by construction —
  worth reading alongside this entry.
- **`auto_split_loop`'s `pending` retry map has no way to notice its target
  tablet vanished out from under it — a `DROP TABLE` mid-retry left a `tablet=N
  kind="retry"` entry retrying forever against a tablet id that will never
  have a leader again.** `DropTableTablets` removes every tablet scoped to a
  table in one apply, including a source tablet whose split was still pending
  (and any child it had already minted, since a split child inherits the
  parent's table scope — so there's no orphan left to GC here, unlike the
  entry above). But nothing told the *retry loop* the entry it's holding is
  now pointless: the retry call resolves "no CP group leader reachable" (a
  routing failure over a tablet id absent from `Metadata`, not the "different
  key won" case the abandon check above is built to detect — `local_cp(tablet)`
  returns `None` for an unregistered id, so `abandoned` stays `false`), which
  the loop treats as "still committing, retry next tick" — forever, one
  routing-timeout round trip per tick, live-observed continuing for minutes
  after the table was dropped. Fixed with the obvious guard: before retrying,
  check whether `tablet` is still in `ctx.raft.metadata().tablets` at all; if
  not, give up (no GC needed, no cooldown bookkeeping needed — a dropped id
  never reappears as a fresh-split candidate, since the fresh-scan iterates
  `Metadata.tablets.keys()`). **No dedicated regression test**: unlike the
  entries above, there's no black-box-observable difference between "gave up"
  and "kept quietly retrying forever" other than wasted CPU/network and log
  noise — no counter is exposed to assert against, and the fix itself is a
  one-line existence check with no branching to get subtly wrong. Verified by
  code review + the full existing `cp_plane.rs`/`drop_table_gc.rs` suites
  staying green, not by a new test.
- **Once a group can split more than once, `drop_orphan_tablet`'s caller-side
  signal is a heuristic, not a certainty — so `drop_orphan_tablet` itself
  gained a hard, independent safety gate, and no longer trusts the caller
  alone.** Before successive splits, "the group's applied key no longer equals
  mine" meant "mine definitely lost" — a group could only ever apply one real
  `Split`, so there was nothing else it could mean. Once a group can split
  again, `current_split_bound` no longer equals a key `K` either because `K`
  genuinely lost, *or* because `K` won earlier and a **later** split has since
  narrowed past it (see `animus-cp-data/CLAUDE.md`'s note on why that
  ambiguity is an intentional, accepted tradeoff for keeping this O(1) state
  instead of an ever-growing history). Both `auto_split_loop`'s abandon branch
  and `trigger_split`'s step-2 failure path only ever produce that ambiguous
  signal now — calling `drop_orphan_tablet` on a tablet that's ambiguous-but-
  actually-alive would delete a live tablet's metadata while it's still
  serving real data, which is a categorically worse outcome than the orphan
  pileup this mechanism exists to fix. `drop_orphan_tablet` therefore checks,
  before touching anything: does *this node* still have a locally registered,
  live CP group for the tablet (`ctx.edge.local_cp(tablet).is_some()`)? The
  split hook mints a member on **every** original replica when a split
  genuinely applies (`cp_split_seed`), so if the tablet really was seeded, this
  node — one of the original replicas — has a handle for it, regardless of
  what the ambiguous signal said. If it does, skip the drop entirely (log and
  return) — the worst case of a wrong call *into* `drop_orphan_tablet` is now
  "skip a cleanup that would've been safe," the same tolerated "orphan lingers
  a bit longer" behavior from before any GC existed, never a deleted-while-
  alive tablet. No dedicated regression test for the gate itself (same
  reasoning as the "no way to notice its target tablet vanished" entry above —
  no clean black-box signal to assert a *skipped* drop against without
  engineering the exact live-hosting race, which needs real leader-churn
  timing); `a_lost_split_race_does_not_leave_a_permanent_orphan` exercises the
  gate's *other* branch (nothing hosted → the drop proceeds).
- **A `RaftKvNode` group used to apply at most one `Split`, *ever* —
  `auto_split_loop` had to permanently exclude an already-split tablet from
  fresh-split candidacy, which just moved the real problem (a heavily-loaded
  lineage eventually running out of room to shard) one step later instead of
  fixing it.** The original guard (`leader.applied_split_key().is_some()`
  before the `hot`/`local_pairs` work) was a necessary stopgap at the time — a
  bootstrap or once-split tablet that kept growing re-tripped the threshold
  every cooldown window, and since step 2 (`propose_split`) could never confirm
  a second split against an already-split group, every attempt burned a full
  `CLIENT_TIMEOUT` before abandoning, mint-ing a brand-new orphan tablet id
  each time (live-reproduced with `--cluster 3 --auto-split 2000` under a
  sustained bulk seed: 13 tablets in metadata, only 3 with a real CP group).
  Excluding an already-split tablet forever fixed the immediate symptom, but
  the underlying "at most once, ever" invariant was always the actual bug — as
  this file used to note as a residual limitation. It's since been lifted (see
  `animus-cp-data/CLAUDE.md`'s "a group can be split more than once" entry): a
  `Split` is now valid whenever its key is strictly less than the group's
  *current* boundary, which a growing, already-once-split tablet can always
  satisfy again as it keeps regrowing. **The permanent-exclusion guard is
  therefore gone** — `is_fresh_split_candidate`'s existing `pending`/cooldown
  exclusion is the only gate needed now (don't retrigger while a split for this
  tablet is already in flight or within `AUTO_SPLIT_COOLDOWN`); nothing gates
  on split history anymore. Regression:
  `tests/cp_plane.rs::already_split_tablet_splits_again_once_it_regrows`
  (split once at a low threshold, regrow the *original* tablet's still-open
  lower range specifically, assert it splits *again* rather than staying
  frozen, then assert every key across both rounds is still reachable).
- **The cluster's members are the CP `raftkv` nodes, not the control ids.** The
  control ids `0..N` are only the Raft *consensus group* for metadata; `bootstrap`
  (leader-only, idempotent) registers the **raftkv ids** (`300+i`) as `Active`
  `Metadata` members and records the single bootstrap **CP tablet** (whole keyspace)
  placed on the first `min(N, MAX_REPLICATION_FACTOR)` of them — the same set the CP
  group spans in `start_with`. This keeps `metadata().tablets`/`status` meaningful
  and gives dynamic CP reconfigure a hook (`tablets[t].replicas`). **Data-node
  failure detection is now wired over `ProdEnv`** (#3): every node spawns
  `heartbeat_loop` on its `raftkv` env, heartbeating the control group *as its
  `raftkv` member id*, so the control leader's `detect_loop` marks a crashed CP node
  `Down` (`tests/cp_reconfigure.rs::data_node_failure_is_detected`). And **each
  CP-hosting node runs `cp_reconfigure_loop`** (#3 / ADR 0017 Stage C): for every
  tablet whose group it leads, it pulls `tablets[t].replicas`, translates base ids to
  the group's member ids via `cp_members_for` (bootstrap tablet = base ids; a split
  tablet = `base + tablet * CP_SPLIT_ID_STRIDE`, matching `cp_split_seed` — so the
  replicated map can stay in base ids without reconciling to derived ids, #4), and
  takes one single-server `reconfigure_step` toward it
  (`tests/cp_reconfigure.rs::cp_group_follows_tablet_replica_set`: dropping a follower
  from the replica set reconfigures the group's voters down). **The full cascade is
  closed** (D1): `bootstrap` attaches a label-free RF `PlacementPolicy`, so on a `Down`
  replica the reconciler picks an Active spare, the spare's join-host loop stands up an
  empty group, and the leader adds + catches it up — auto-replacing the dead replica
  end to end (`tests/cp_reconfigure.rs::failure_auto_replaces_replica_onto_spare`). The
  v0 heartbeat/anti-entropy/hinted-handoff loops and the `serve_replica` data role are
  gone; the control-plane mechanisms (failure detection, placement) remain sim-proven
  in `animus-control`. **Small remainder:** new-group ids are derived, not
  control-plane-allocated (fine for realistic clusters).
- **Drop-table GC (ADR 0024) is the join-host loop's dual.** The real drop sink is
  `ClientCtx::drop_table` (CQL `DROP TABLE` + admin `/admin/data/drop-table`):
  `DropTableSchema` then `DropTableTablets`. **`drop_table_schema` stays
  schema-only** (the admin panel's schema-only drop). CQL `ALTER TABLE … ADD` no
  longer drops at all: it mutates the schema **in place, atomically** via
  `MetaCommand::ReplaceTableSchema` (`ClientCtx::replace_table_schema`) — the old
  drop-then-recreate could strand the table schema-less if a crash landed between
  the two commands — and an ALTER must never GC data. The per-node `cp_gc_loop` then reclaims any tablet in this
  node's `minted` set that is absent from `Metadata.tablets`: unregister *this
  node's* handle (`unregister_raftkv(tablet, member)` — the shared `--cluster N`
  edge holds every node's handles, so match by the handle env's member id),
  `CpGroup::shutdown()` + wait `is_stopped()` (never delete under a live driver;
  on timeout re-register and retry a later tick), delete `db-`/`db-t{id}-*` +
  `raftkv.wal*` via the group env's `Disk::list`/`remove`, `shutdown_tasks()` a
  sibling env (never `shutdown()` — that drains the shared pool), prune the
  `cp-hosted` marker, release `minted` last. Guards worth keeping: skip while
  `last_applied() == 0` (pre-recovery metadata is empty ⇒ reads as
  "everything dropped"), and skip a minted-but-unregistered tablet (stand-up in
  flight). **Drop + GC are convergent, not one-shot**: a restarted control
  replica re-applies its log through *historical* map states, so join-host may
  briefly re-host a dropped tablet's empty group — the GC reclaims it once
  replay passes the drop (test the post-restart state with a poll, never a
  fixed sleep). **A new `MetaCommand` that must commit from a follower-connected
  node has to be added to `is_relayable_command`** — missing there is a
  *bimodal* failure: works when the connected node happens to be the control
  leader, silently times out ("did not commit") when it must relay
  (`tests/drop_table_gc.rs` caught exactly this for `DropTableTablets`).
- **The CP group is durable by default**: each hosting node's `RaftKvNode` is
  backed by the on-disk `LsmEngine` opened over its **raftkv** `ProdEnv`
  (`StorageBackend::Lsm`), so a value acked to a client (Raft-committed + WAL-fsynced
  before the ack) survives a process restart (the LSM + Raft WAL recover on reopen).
  The engine's files use a **flat filename prefix** (`LSM_PREFIX = "db-"`), *not* a
  subdirectory — `ProdEnv`'s disk opens files directly under the role's data dir and
  does not create intermediate directories, so a slash-bearing prefix (e.g. `"db/"`)
  would fail to create the files. `--ephemeral` (or `StorageBackend::Memory`) selects
  the volatile `MemoryEngine` instead (the `CpGroup` enum wraps either), for dev runs
  that intentionally start empty. `start`/`start_cluster`/`run_node` default to the
  durable backend; `start_with`/`start_cluster_with`/`run_node_with` take an explicit
  `StorageBackend`. These are **async + fallible** (opening the LSM is async and can
  fail), so the node-start entry points return `io::Result`. (`tests/durable_restart.rs`
  proves a client write survives a restart on the LSM backend and is lost on the
  memory backend; `tests/self_heal.rs` is now just a concurrent-load smoke test.)
- Each node also serves a **fifth listener, the DynamoDB JSON/HTTP endpoint**
  (`RoleAddrs.dynamo`, `Node::dynamo_addr`). It is a *production-only I/O edge*
  (real tokio sockets + hand-rolled HTTP/1.1, like `ProdEnv`); below the edge it
  routes through the CP primitives (`ClientCtx::cp_read`/`cp_write`/`cp_scan`).
  DynamoDB `DeleteItem` writes a sentinel tombstone *value* that `GetItem` reads
  back as absent (distinct from the CQL whole-partition `cp_delete`). **`CreateTable` now
  proposes its key schema into the control plane's replicated catalog (ADR 0013)
  and waits for commit**, so a created table is durable + cluster-agreed (it
  survives a restart — `tests/dynamo_schema.rs`); the edge reaches the leader
  through the cluster's set of registered control handles (held in
  `ClusterEdgeState`, threaded via `ClientCtx::edge` — see below). A
  never-`CreateTable`d table falls back to the legacy `pk`/`sk` convention.
  `CreateTable` now decodes `AttributeDefinitions` into `key_types` (carried on
  `Operation::CreateTable`) and passes them to `schema_bridge::to_control`, so the
  replicated catalog records each key column's declared **type** (`S`/`N`/`B` →
  `String`/`Number`/`Binary`) — previously the edge passed `&[]`, defaulting every
  key to `String`. The dashboard's key prefill reads these types.
  **`CreateTable`'s GSI/LSI *definitions* are also replicated now** (ADR 0013):
  after the schema commits, `create_table` proposes one
  `MetaCommand::CreateTableIndex` per declared index (built via
  `animus_dynamo::schema::index_to_control`, passing the base partition key) and
  waits for each to replicate. The local registry is then reconciled to the
  replicated set via `mirror_catalog_schema` → `SchemaRegistry::sync_indexes`
  (called on the read/write paths too), so a freshly restarted node — or a follower
  that never saw the `CreateTable` — rebuilds its index machinery from
  `Metadata::table_indexes`, not process-local memory. Only the index *entry data*
  (the `escape(hash)||…||base_key` index) stays in-memory, maintained from observed
  `note_put`/`note_delete` writes (O(log n) per write via a base-key→entry reverse
  map) and **lazily backfilled on the first index query** against freshly-created
  index machinery (`dynamo.rs::backfill_index_if_needed`: one base-table scan
  replayed through `note_put`, then `mark_table_backfilled`) — so a GSI query
  returns pre-restart items without re-writing them (proven in
  `tests/dynamo_schema.rs`'s `create_table_index_replicates_to_second_node` /
  `…_survives_node_restart`).
  **Base-table `Query`/`Scan` use the CP plane's linearizable range scan**
  (`ClientCtx::cp_scan` → `RaftKvNode::linearizable_scan`) over a contiguous key
  range (a partition prefix for `Query`, the whole-table prefix for `Scan`),
  decoding each live pair and dropping DynamoDB tombstone values — **no in-memory
  written-key tracking** (proven across a restart in `tests/dynamo_schema.rs`). The edge keeps only the
  **GSI/LSI index declarations** in-memory (for an *index* `Query`), held
  **per-cluster** in `ClusterEdgeState` (not a process `OnceLock`). The surface now
  also covers `UpdateItem`/`BatchWriteItem`/`TransactWriteItems` (the last
  condition-gated but not yet atomic), per-index projections, and document-path
  projections.
- And a **sixth listener, the CQL binary-protocol endpoint** (`RoleAddrs.cql`,
  `Node::cql_addr`). Same shape: a production-only I/O edge (real tokio sockets +
  hand-rolled CQL v4 framing in `cql.rs`; the pure protocol/type/catalog/planning
  logic is in `animus-cql`), routed through the same `ClientCtx`. It runs
  `QUERY`/`PREPARE`/`EXECUTE`: `CREATE TABLE` proposes a typed schema into the
  control plane's **replicated catalog** (ADR 0013) and `INSERT`/`SELECT` resolve
  columns from it (a typed row is one data-plane value keyed by `escape(table) ||
  pk_key_bytes`; the partition key is not stored in the value). `CREATE KEYSPACE`
  records the keyspace in the per-cluster `CqlState` (keyspaces are not yet
  replicated).
  - **The keyspace set + prepared-statement store (`CqlState`) are per-cluster
    edge state**, held in the cluster's `ClusterEdgeState` (threaded through
    `ClientCtx::edge`), **not** a process `OnceLock` — like the DynamoDB
    `SchemaRegistry`. They are shared across the cluster's CQL listeners (so
    `--cluster N` dev mode sees one node's `CREATE KEYSPACE` from another) but
    **isolated between two clusters in one process** (so a test harness can run
    several independent clusters without their edge state leaking — the fix for
    the former process-global `OnceLock` state-leak). They are still **not durable
    and not control-plane replicated**: lost on restart, and a one-process-per-node
    deployment has a per-process catalog (re-create schemas per process). Note
    table *schemas* are no longer here at all — they live in the control plane's
    replicated catalog (ADR 0013). Per-connection state (the `USE`d keyspace)
    lives in `Session`.
  - The **prepared-statement id is content-addressed** — a stable hash of the
    statement text (FNV-1a, no RNG so the edge stays deterministic) — so `PREPARE`
    on one connection and `EXECUTE` on another resolve to the same statement.
- **A dedicated admin / debug HTTP-JSON endpoint** (`RoleAddrs.admin`,
  `Node::admin_addr`, ADR 0020) — a **sixth** per-node listener, isolated from the
  client/dynamo/cql data edges. A production-only I/O edge in `admin.rs` (real
  tokio sockets + the shared hand-rolled HTTP helpers extracted to `http.rs`, now
  shared with `dynamo.rs`). Read-only `GET` views — `/admin/{config,status,raft,
  raftkv,storage/lsm,storage/wal,storage/wal/segment,storage/key,storage/scan,metrics,health}`
  — plus gated `POST` actions — `/admin/{tablet/split,storage/flush,storage/compact,
  raftkv/reconfigure,drain}` and **data writes** — `/admin/data/{dynamo,cql,drop-table,seed}`
  (ADR 0021, the dashboard's write surface). Below the edge it only **reads** node state
  (control + CP Raft accessors, `LsmEngine` introspection: `sstable_views`/
  `wal_segment_*`/`memtable_*`, the `CpGroup` introspection passthroughs) aggregated
  live at request time, or drives an explicit action; node identity for `/admin/config`
  is captured into `ClientCtx.admin` (an `AdminInfo`). **No auth yet** — bind it to a
  trusted interface. The `animus admin <subcommand>` CLI consumes it.
  - **The web dashboard (ADR 0021) is the "AnimusDB Console"** — a from-scratch
    visual/IA redesign (2026-08-06, implemented from a Claude Design mockup the
    user provided) replacing the earlier flat-tab debug dashboard. Still served
    from the same port: `GET /` (and the `/admin`, `/admin/ui` aliases, plus any
    `/admin/ui/<tab>`) returns a self-contained vanilla-JS SPA embedded via
    `include_str!` (`dashboard.rs` → `dashboard.html`) — no bundler/npm, the
    build stays `cargo`-only, and **no external fonts/CDN either** (ADR 0021 §1
    is firm on this; the console approximates the source design's Inter/IBM
    Plex Mono with system font stacks instead of a Google Fonts fetch). It is a
    pure **client** of the `/admin/*` JSON, so every `/admin/*` response
    carries **CORS** (`http::CORS_HEADERS`; an `OPTIONS` preflight returns 204)
    because the page loaded from one node fans out in the browser to **every**
    node. The fan-out seed is **`GET /admin/peers`**.
    **Shell: a sidebar, not a top tab row** (`dashboard.html`) — five views,
    `overview`/`placement`/`tablets`/`browser`/`storage` (`TABS` in
    `dashboard_core.js`), each with its own JS module
    (`dashboard_overview.js`/`dashboard_placement.js`/`dashboard_tablets.js`/
    `dashboard_browser.js`/`dashboard_storage.js`, loaded after `dashboard_core.js`
    in that order — plain `<script src>` tags sharing one global scope, so later
    files call earlier ones' functions freely). **Each view keeps a real URL**
    (ADR 0021 follow-up 7): `/admin/ui/<tab>`, `admin.rs::is_ui_path` prefix-serving
    the SPA for any path under it (an unrecognized tab 200s and falls back to
    the default client-side, so a stale bookmark degrades gracefully); the page
    reads `location.pathname` on load (`tabFromPath`/`activateTab`) and uses
    `history.pushState`/`popstate`. The Storage tab's selected tablet/node ride
    along as `?tablet=&node=` (`gotoStorage`/`syncStorageUrl`/
    `applyPendingStorageParams` in `dashboard_core.js`) — the one piece of
    sub-tab URL state, reused by the Tablets view's "Open in Storage →" link and
    by Placement's per-node tablet rows.
    **Both a dark and a light theme** (`dashboard.css` CSS custom properties,
    the mockup's `oklch()` palette verbatim), toggled by a button in the top bar
    and persisted to `localStorage` (a UI preference, not data — no server
    round-trip). **Three things the design showed have zero backend support and
    are deliberately omitted, not faked**: per-node CPU/mem/disk % (nothing
    samples host resources anywhere in this workspace), an activity/event feed
    (no persisted/queryable event log exists — distinct from OTel tracing and
    the counter-snapshot `/admin/metrics/history` ring buffer), and a per-tablet
    election-history log (only current Raft state is tracked). Fabricating these
    would violate this admin tool's ground-truth-data ethos. The **Overview**
    view's "Tables" panel (a per-table tablet-count + status breakdown) is a
    real, honest substitute for the design's dropped "Recent activity" panel.
    **Tablets is one view with a `Lanes`/`Table`-shaped predecessor collapsed
    into a single filterable list + detail panel** (not the earlier
    lanes-vs-table toggle, which is superseded) — clicking a row opens a
    right-side panel with the raft group (from data already fetched) plus
    storage-engine stats fetched **on demand**, only for the selected tablet's
    leader, from `/admin/storage/lsm?tablet=` (`dashboard_tablets.js`'s
    `loadTabletDetailStorage`) — not for every row.
    **The Data Browser view replaces the old Write tab's Dynamo attribute-row
    form with a real item list + detail panel** (`dashboard_browser.js`):
    Scan/Query build real requests against `/admin/data/dynamo` (Query supports
    the exact sort-key grammar `animus_dynamo::wire` parses — `=`, `BETWEEN`,
    `begins_with` — see `buildQueryPayload`), decode the returned
    AttributeValue-map `Items` for display, and per-row Edit/Delete/Create use
    a dynamic attribute-row editor (key columns locked, arbitrary extra
    attributes addable/removable) because DynamoDB items are schemaless beyond
    their declared keys — a fixed-column form (as the source mockup's fake
    table had) can't represent that. **Each browser/write panel owns its own
    table selector** (`#br-dy-table` here, `#seed-table` on the folded-in Bulk
    seed tool — `dyTable`/`seedTable`), auto-picking the first valid table
    rather than requiring an explicit pick, and rather than one shared global
    header dropdown (an earlier revision had that; removed as redundant).
    `lastRenderedDyTable` gates when the Dynamo op panel's state is rebuilt
    (table actually changed) vs. left alone on a routine poll refresh,
    preserving in-progress edits.
    **The Storage view folds in the pre-redesign dashboard's debug tools**
    (`dashboard_storage.js`) — WAL segment/record inspection, LSM shape, a
    single-key inspector (`/admin/storage/key`), a **browse-keys** list
    (`/admin/storage/scan` → `CpGroup::local_scan`), and the Bulk seed tool —
    ported essentially unchanged, since the console design doesn't include this
    level of manual storage debugging at all and it would otherwise be lost.
    Its **node dropdown is filtered to nodes whose `/admin/raftkv` view lists the
    selected tablet** (the storage endpoints are node-local — `local_cp` — and
    404 on a non-hosting node); if no reachable node hosts the tablet yet (group
    still forming) the dropdown is empty with a hint (the Load/Browse/inspect
    handlers no-op on an empty node).
    `tests/dashboard_endpoint.rs` proves serve + CORS + preflight + peers; its
    "the shell contains X" assertions target the shell (`dashboard.html`) or the
    specific JS asset that actually carries the behavior being checked (e.g.
    the item form's key-lock indicator lives in `dashboard_browser.js`, not the
    shell) — a lesson from a **latent bug this redesign caught**: the pre-split
    single-file dashboard (before PR #48) had its whole JS inline, so asserting
    on `GET /`'s body for a JS-source string worked by accident; after the
    file split it silently stopped proving anything (the string had moved to a
    separately-served file `GET /` never returns), and nothing caught it until
    this rewrite touched the same test. When splitting a previously-inline asset
    into files, re-audit every test assertion that greps the *original*
    response body for content that may have moved.
    - **Displayed keys show the partition token as unpadded base64url**
      (`admin.rs::key_display`): a wire-edge/seeder key is `token || escape(pk) ||
      rk` (ADR 0022), and the leading `TOKEN_BYTES` are a **binary** Murmur3 token
      that lossy UTF-8 would mangle — so a key with a non-printable prefix renders
      as `<11-char-base64url-token>:<readable pk/rk>` (e.g. `CCX7PfaR_cM:seed:0000…`).
      The encoding is base64url with no padding (RFC 4648 §5) because displayed
      keys are pasted back into `?key=`/`?start=` query params, where the standard
      alphabet's `+` decodes as a space (and `=` padding percent-encodes noisily);
      the codec is `animus_dynamo::wire::{base64url_encode,base64url_decode}` (the
      standard padded pair stays on the DynamoDB `B` wire). A *plain-client* `Put`
      stores its key verbatim (no token), so a fully-printable key is shown as text
      unchanged. **Values** keep lossy UTF-8 (`key_str`). `parse_key_display` is
      the inverse (the exactly-`TOKEN_BYTES` decode is strict — URL-safe alphabet
      only, canonical trailing bits — which keeps a plain `:`-bearing key from
      being mistaken for a token), so a browsed key round-trips back through the
      inspector (`/admin/storage/key`) and the scan `start` (paging). The
      dashboard's JS helpers (`b64url`/`bytes`/`tokenBound`) mirror the same
      encoding, so tablet range boundaries and SSTable key ranges are
      eyeball-comparable with browsed keys. Unit tests live in `admin.rs`; the
      `admin_endpoint` plain-`Put` `admin-key` guards the
      not-every-key-is-token-prefixed case.
  - **The Data Browser view (ADR 0021) writes through the admin port.** `POST
    /admin/data/dynamo {op, payload}` reuses the DynamoDB edge in-process
    (`dynamo::execute` — the factored decode+`run_operation`), returning the op's
    JSON. `POST /admin/data/cql {query, keyspace?}` runs CQL by driving **this
    node's own CQL port as a loopback client** (`cql_client` — STARTUP→QUERY per
    `;`-split statement, decoding the binary RESULT frame to JSON via
    `animus_cql::types`), so the 1000-line CQL edge is reused untouched rather than
    refactored to emit JSON. The browser can't speak the CQL binary protocol, so a
    server-side proxy is mandatory; Dynamo is proxied too for one origin / one CORS
    / one future-auth boundary. **This makes the admin port a data-write *and* DDL
    surface (still no auth)** — sharpening the bind-to-trusted-interface /
    auth-before-exposure follow-up. `tests/admin_endpoint.rs::admin_data_write_dynamo_and_cql`
    proves a Dynamo Put→Get round-trip + a CREATE/INSERT/SELECT CQL script.
    - **Dynamo table management + Scan/Query/item CRUD.** The panel lists tables
      from the replicated catalog (`/admin/status` `schemas.tables`, filtered to
      plain-named = Dynamo, vs CQL `ks.table`), creates via `CreateTable`, and drops
      via `POST /admin/data/drop-table` (`ctx.drop_table_schema`; the Dynamo wire has
      no `DeleteTable`, so this reuses the control-plane drop, schema-only). The op
      **targets its own `#br-dy-table` selector** (see above) — disabled unless a
      Dynamo table exists, so you can't act on a non-existent or CQL-only table.
      Scan and Query build **real** requests (`dashboard_browser.js`'s
      `runDynamoOp`/`buildQueryPayload`) rather than the pre-redesign Write tab's
      Form/JSON editor over one fixed op; results decode the returned
      AttributeValue-map `Items` for a real item list, and per-row Edit/Delete
      plus "+ Create item" open a dynamic attribute-row editor (key columns
      locked, rows addable/removable) — not a fixed-column form, since items are
      schemaless beyond their declared keys. `tests/admin_endpoint.rs::admin_table_management_create_and_drop`
      (also asserts a numeric sort key's type reaches the catalog) still covers
      the underlying create/drop; the Scan/Query/CRUD paths reuse the same
      `/admin/data/dynamo` operations the old Write tab used, just orchestrated
      differently client-side, so no new server-side test was needed for them.
    - **Bulk seed for sharding tests.** `POST /admin/data/seed {table, count, start?,
      key_prefix?, value_bytes?}` writes synthetic rows whose partition key is
      `key_prefix` + zero-padded index, stored under the edges' token-prefixed
      layout (`partition_token(escape(pk)) || escape(pk)`, `admin.rs::seed_key` —
      ADR 0022: seeding must hash like a real write, so sequential indices spread
      across the ring instead of piling into one tablet's tail)
      into an **existing** `table` (ADR 0023: seeding writes into a table, it
      does not create one — a non-existent table is a `404`, looked up in the
      replicated tablet map), committed sequentially as `SEED_BATCH_SIZE`-key
      `cp_batch_write_patient` batches; capped at `SEED_MAX_PER_REQUEST` per
      call. Each batch is **retried** (`SEED_WRITE_ATTEMPTS`) so writes racing
      a tablet **split** — routed to the parent and truncated as the upper
      range moves to the new child — re-route to the elected child and land
      (idempotent per-key LWW), instead of surfacing "CP batch write did not
      commit in time". **The retry uses `ClientCtx::cp_batch_write_patient`,
      not a plain loop over `cp_batch_write`**: a bare confirm-timeout means
      the batch's `Batch` Raft entry was accepted onto the leader's log but
      not yet confirmed durable+applied — not that it's lost — so blindly
      resubmitting would append a second, fully duplicate entry for the same
      keys on top of one probably still committing, doubling replication/fsync
      load under exactly the slow/contended conditions that caused the
      timeout (root-caused via a live repro: `--auto-split 2000` under
      sustained bulk-seed looked like a leader-election storm but every Raft
      term, control plane and every CP group, stayed flat the whole time —
      `commit_index` kept climbing well past individual attempts already
      reported failed; the actual bottleneck was disk fsync latency, ~12-27ms
      measured on a WSL2 host vs. sub-ms on real NVMe). `cp_batch_write_patient`
      polls the *same* already-accepted entry for a second confirm window
      before falling back to a fresh propose, so only a genuine routing
      failure (leader moved, e.g. a split) triggers a real resubmission. The
      dashboard's **Bulk seed** card chunks a
      larger total into requests, showing progress + refreshing the Tablets view so
      splits appear live; it also **targets its own `#seed-table` selector**,
      disabled with a hint unless the selected table already has a tablet (from
      the tablet map in `/admin/status` — the exact set the endpoint's
      `has_table_tablet` check accepts, so Dynamo *and* CQL `ks.table` tables both
      qualify). Combined with the binary's **`--cluster N --auto-split K`**
      flag (a CP-hosting node splits a tablet it leads once it exceeds K keys, Phase
      2.4, via `start_cluster_with_auto_split`), seeding past K auto-shards the
      keyspace — verified end to end (seed 12k keys, `--auto-split 4000` → 5 tablets).
      `tests/admin_endpoint.rs::admin_seed_writes_synthetic_keys`. **Wrapped in an
      `admin_seed` span** (per-chunk `admin_seed_batch` children, ADR 0027): the
      seeder calls `cp_batch_write` directly rather than going through
      `handle_client`, so without its own span a batch forward's
      `otel::current_traceparent()` would have no active context to inject — the
      seed would write real data but be invisible in a trace backend no matter how
      much it wrote.
  - **Gotcha — `/admin/raftkv` is node-local, but in a single `--cluster N` process
    the shared `ClusterEdgeState` registers *every* node's CP group handle, so one
    node's view lists all replicas; a one-process-per-node deployment (separate edge
    each) shows just the local group.** A storage route resolves the tablet's *local*
    handle (`edge.local_cp`), so `--cluster` mode targets the first-registered
    replica's engine, not necessarily this node's — scrape per-process for true
    node-local storage debug (`tests/admin_endpoint.rs` uses `run_node` per node).
    **The dashboard's Tablets/Placement/Overview views were themselves a victim of
    this gotcha** — `dashboard_core.js::cpGroupsByTablet()` tagged every group in a
    node's `/admin/raftkv` response with *that fetching node's identity*, correct
    only in one-process-per-node mode. Under `--cluster N` every node's response
    lists the same full cluster-wide group set, so this produced duplicate
    `{node, group}` entries mis-tagged with whichever admin port happened to answer
    — and since every replica dot's role lookup (`gs.find(x => nodeRaftkvId(x.node)
    === rid)`) matched on that wrong tag, it deterministically resolved to the
    *same* (first) group's `is_leader` for every replica in a tablet's row: every
    dot showed identical status ("nodes are either all followers or all leaders"),
    and the Overview balance chart / per-node hosted-count and the Placement
    per-node tablet list were equally wrong for the same reason. `CpRaftView` had
    no field identifying which physical node a group belongs to at all — only the
    fetching admin port's identity, which is not the same thing under a shared
    edge. Fixed by adding `CpRaftView::node` (`lib.rs::raft_view`, the group's
    member id translated back to a **base** raftkv id via `topology::cp_base_id` —
    needed because a split tablet's member id is derived, `base + tablet *
    CP_SPLIT_ID_STRIDE`, not the base id itself) and having `cpGroupsByTablet()`
    resolve/dedupe by that real id (`nodeByRaftkv(g.node)`, keyed on `tablet:node`)
    instead of the fetching node. **General lesson: a debug/admin view whose
    response can legitimately be a cluster-wide aggregate (not just this node's own
    state) must carry each item's own identity in the payload — a client cannot
    infer "whose state is this" from which server answered.**
  - **Metrics are per-node sinks**: a follower's leader-only counters
    (`elections_won`, `append_entries_sent`) are legitimately 0, so `/admin/metrics`
    (and `/metrics`) is meaningful **per node** — scrape the control leader for the
    leader-only counters (the test asserts election counters only on the leader).
- **A `GET /metrics` admin route shares the DynamoDB HTTP listener** (ADR 0015) —
  the line-oriented metrics export stays on the dynamo port (the dedicated admin
  port above serves the richer JSON surface, incl. `/admin/metrics`). The DynamoDB edge's request parser now
  captures the request method + path; a `GET /metrics` is answered with the
  text-format snapshot as `text/plain` (everything else is the existing
  `POST /` + `X-Amz-Target` DynamoDB protocol). The body is **aggregated across the
  node's two role sinks** (control / raftkv) by `ClientCtx::metrics_text`: each role
  records into its **own** `ProdEnv` sink (`RaftNode::start` → `control_env.metrics()`;
  the CP group → the raftkv env's), so the handler snapshots both **at request time**
  (live, not cached), sums the counters, and takes the max leadership gauge. The
  raftkv sink is captured in `start_with` before its env is moved and threaded into
  `ClientCtx`. The endpoint is on `Node::dynamo_addr()` (`curl -s <dynamo addr>/metrics`).
- CP writes need **no client-assigned version**: the Raft log index *is* the MVCC
  version, so per-key LWW reproduces the agreed Raft order. (The v0 AP path derived
  a quorum version via `read_version`+1; that is gone with the AP plane.)
- A CQL/DynamoDB read-modify-write is serialized per node behind `rmw_lock` so the
  linearizable CP read + CP write are **atomic per node**. On the CQL edge that is
  every `INSERT`/`UPDATE`/`DELETE` (a partition RMW); on the DynamoDB edge it is
  the RMW ops only — conditional `PutItem`/`DeleteItem` (or `ReturnValues:
  ALL_OLD`), `UpdateItem`, and the whole of `TransactWriteItems` (one guard across
  all actions; the per-action helpers deliberately take no lock — the tokio Mutex
  is not reentrant). Unconditional puts/deletes and batch writes do no pre-read
  and take no lock. (The DynamoDB edge once took no lock at all — two concurrent
  `attribute_not_exists` puts on one node could both pass; regression in
  `tests/dynamo_extended.rs::concurrent_conditional_puts_one_wins`.) Cross-node
  atomicity (a CAS on the CP group) is later v1 work.
- **The wire edges snapshot the replicated `Metadata` once per request**
  (`dynamo.rs::run_operation` takes `let meta = &metadata(ctx)` and threads
  `&Metadata` through the helpers) — `RaftNode::metadata()` deep-clones under a
  lock, and a single request used to re-clone it 2+ times. Two rules keep the
  snapshot sound: (1) a path that must observe *fresh* state (the `CreateTable`
  commit-wait polls, the post-commit `mirror_catalog_schema`) reads live; and
  (2) **an existence gate that short-circuits a linearizable read must not
  conclude "absent" from the request-entry snapshot** — `quorum_read`'s
  "no tablet ⇒ no data" gate re-checks *live* on the snapshot-miss path, because
  a concurrent first write can provision the tablet after the request began, and
  under the `rmw_lock` a conditional writer's read must see it (two racing
  `attribute_not_exists` puts both succeeded when the gate trusted the snapshot —
  caught by `dynamo_extended.rs::concurrent_conditional_puts_one_wins`). Trust
  the snapshot on the hit path; re-verify on the miss path.
- Two run modes: `--cluster N` (whole cluster in one process, dev convenience)
  and `--config FILE --node I` (one node per process — real deployment). Both
  share `Node::bind`/`start`; only address/peer assembly differs.
- **`--cluster N` without an explicit `--dir` defaults to ONE fixed path,
  `$TMPDIR/animusd` (`main.rs`), reused across every invocation on the
  machine — and `--ephemeral` does NOT make a run ephemeral with respect to
  that default dir.** `--ephemeral` only selects the CP-data group's
  `StorageBackend` (`Memory` vs `LsmEngine`, consumed later in
  `start_cluster_with`); `Node::bind` unconditionally opens the **control**
  role's `ProdEnv` at `dir/node-{i}/control` and the **raftkv** role's at
  `dir/node-{i}/raftkv` *before* that backend choice is ever consulted — so
  the replicated `Metadata` (tablet map, membership, schema catalog) and the
  raftkv role's own Raft WAL persist to disk across `--ephemeral` runs, and a
  "fresh" cluster silently inherits a previous run's tablet/split state
  (live-observed: a brand-new `--cluster 3 --ephemeral` already had a
  multiply-split tablet with a real range from an unrelated earlier run).
  Worse, **two `--cluster N` processes running concurrently without distinct
  `--dir`s will contend on the same on-disk control/raftkv WAL files** — a
  real correctness hazard for local dev (two agents/terminals each running
  `animusd --cluster 3` for a quick manual check), not just stale-state
  confusion. Always pass an explicit, freshly-created `--dir` for a
  throwaway manual run; don't rely on `--ephemeral` alone for a clean slate.
- **The wire edges' mutable state is `ClusterEdgeState`, scoped to one cluster**
  (not the whole process). It holds the set of control `RaftNode` handles a schema
  DDL proposal fans out to (so a follower-connected `CreateTable`/`CREATE TABLE`
  still reaches the leader), the DynamoDB `SchemaRegistry` (GSI/LSI index
  declarations — the base written-key index is gone, replaced by the native range
  scan), and the CQL `CqlState` (keyspaces + prepared statements). It is created
  once per cluster — in `start_cluster_with` (shared by every node of that
  cluster, so `--cluster N` dev mode agrees) and freshly in `run_node_with` (one
  per process) — and threaded into `start_with` → `ClientCtx::edge`. In
  `--cluster N` mode one process is one cluster, so this is equivalent to the old
  process-global; the point is that a **test harness running several independent
  clusters in one process gets a distinct, isolated edge-state set per cluster**,
  so two clusters never share a registry or a handle set. (This replaced the
  former `OnceLock` process statics, which leaked across tests in one binary —
  a later test's `CreateTable` fanned its proposal across every still-running
  cluster's leaders and timed out.) Schema DDL routes through
  `ClusterEdgeState::{leader_handle, propose_on_leaders}`; reads/writes resolve
  the table schema from this node's own replicated `Metadata`.
- **`Node::shutdown()` is a graceful teardown**: it aborts the node's
  client-facing listener tasks (client/dynamo/cql/admin, on plain `tokio::spawn`) and
  calls `ProdEnv::shutdown()` on each of the two internal role envs (control +
  raftkv), which aborts every task they own (the two Raft drivers + internal accept
  loops). This frees all six listener ports so a replacement node can rebind the
  same addresses on the same data dir — the clean teardown a stopped OS process
  would provide. On-disk state is untouched (a value acked to a client was Raft-
  committed + WAL-fsynced before the ack, so it survives). Wired to the Ctrl-C path
  in `main`. Dropping a `Node` without `shutdown()` still leaves its detached tasks
  running (they hold the ports), so call `shutdown()` to restart in-place.

## Tests / running

`cargo test -p animusd` — `tests/cluster.rs` (in-process cluster),
`tests/per_process.rs` (nodes started independently from a shared config),
`tests/dynamo_wire.rs` (PutItem → GetItem → DeleteItem over the real DynamoDB
JSON/HTTP wire), `tests/cql_wire.rs` (STARTUP → CREATE KEYSPACE/USE/CREATE
TABLE → PREPARE INSERT → EXECUTE with typed bound values → typed SELECT, columns
round-tripping, over the real CQL binary wire), `tests/cql_clustering.rs`
(compound primary key: INSERT rows out of clustering order → clustering-ordered
SELECT → single-row SELECT → UPDATE → single-row + whole-partition DELETE, at
QUORUM consistency), `tests/durable_restart.rs` (a key written
through the client API survives a node stop + restart on the **same dir +
addresses** with the LSM backend, and is lost with the `--ephemeral` memory
backend), `tests/metrics_endpoint.rs` (the admin `GET /metrics` HTTP route, ADR 0015: a
3-node cluster elects a leader, the scrape returns the `text/plain` `name value`
export with `control_elections_won >= 1` and `control_is_leader 1` on the leader /
`0` on a follower), `tests/cp_plane.rs` (CP round-trip: write via one node, read via
another — the CP group is the single source of truth), `tests/cp_cross_process.rs`
(cross-process CP forwarding to the leader's node — including the **derived-member-id**
regression: a *second* provisioned table's group speaks `cp_member_id`-derived ids, so
`cp_forward_target` must translate its leader hint back to a base id via `cp_base_id`
before the `client_route` lookup; the first table rides the bootstrap group where
member == base and can't catch this), `tests/admin_endpoint.rs` (the
admin / debug interface, ADR 0020: a per-process 3-node cluster, then the read-only
views config/status/raft/raftkv/storage·wal/metrics/health over the dedicated admin
port + the `storage/flush` action observed via `storage/lsm`; metrics asserted on
the control leader since sinks are per-node; bring-up wrapped in the port-TOCTOU
retry), `tests/dashboard_endpoint.rs` (the web dashboard, ADR 0021: `GET /` serves
the embedded SPA as `text/html`, every `/admin/ui/<tab>` deep link (incl. an
unrecognized tab name) also serves it, `/admin/*` responses carry the CORS header, an
`OPTIONS` preflight returns 204, and `/admin/peers` lists all 3 nodes' admin
addresses — the fan-out seed), and `tests/self_heal.rs` (a
concurrent-client smoke test that the assembled node does not deadlock under load).
All use real TCP/time, so they poll with timeouts, not deterministic assertions. The restart test runs both incarnations in the **same** runtime,
calling `Node::shutdown()` between them to abort the node's detached tasks and
free its listener ports (dropping a `Node` does not stop them), then rebinds the
same addresses and recovers — a clean teardown → rebind → recover cycle standing
in for an OS process restart.

Per-process run:
```sh
animusd gen-config --nodes 3 > cluster.json
animusd --config cluster.json --node 0   # one process per node, distinct --node
animus status <node-0 client addr>
# the node also prints its DynamoDB HTTP endpoint; talk to it with any
# DynamoDB JSON client, e.g.:
curl -s <dynamo addr>/ \
  -H 'X-Amz-Target: DynamoDB_20120810.PutItem' \
  -d '{"TableName":"t","Item":{"pk":{"S":"a"},"v":{"N":"1"}}}'
# and an admin / debug endpoint (ADR 0020) — read-only introspection + actions:
curl -s <admin addr>/admin/status        # full cluster metadata
curl -s <admin addr>/admin/raftkv        # per-tablet CP group Raft state
curl -s '<admin addr>/admin/storage/wal/segment?tablet=1&seg=0'  # decoded WAL
animus admin status <admin addr>         # same, via the CLI
```
