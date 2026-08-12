# CLAUDE.md — animusd

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The runnable AnimusDB node server — a **lib + bin**. `lib.rs` assembles a node
over `ProdEnv` (the first real use of the production seam): a control-plane Raft
(`animus-control`) for cluster metadata plus the CP data plane (`animus-cp-data`,
one leaderful Raft group per tablet) for linearizable reads/writes, fronted by
four wire edges (DynamoDB JSON/HTTP, CQL v4, a plain length-prefixed TCP client
protocol, and an admin/debug HTTP-JSON port with a web console). `main.rs` is a
thin CLI wrapper. `animus-cli` depends on this crate for the client protocol
types. v1 (ADR 0019) is **CP-only**; the leaderless AP `data`/`coord` roles are
gone.

**`lib.rs` is ~6800 lines** — grep for the symbol, don't scroll. It also holds
two in-crate `#[cfg(test)] mod`s that need private handles the `tests/` tree
can't reach: `split_fence_tests` (lib.rs:6452) and `auto_split_median_tests`
(lib.rs:6725).

## Module map (`src/`)

- **`lib.rs`** (~6800 lines) — the node assembly and everything routing/hosting.
  `Node`/`BoundNode`/`BoundControlNode`/`BoundDataNode` (bind → start pairs),
  `ClientCtx` (per-request context), `ClusterEdgeState` (per-node mutable edge
  state), all `run_node*` entry points, CP routing (`cp_route`/`cp_forward` +
  `FORWARD_ELECTION_BACKOFF`, `CLIENT_TIMEOUT`), `tablet_host_reconciler_loop`,
  `auto_split_loop`, `byte_weighted_median`, the `ClientRequest`/`ClientResponse`
  protocol types + `read_frame`/`write_frame`, and the two in-crate test mods
  above.
- **`main.rs`** — thin CLI wrapper; dispatches the invocation modes (below) and
  wires `otel::init_tracing` + the Ctrl-C graceful-shutdown path.
- **`config.rs`** — `ClusterConfig` (per-process deployment config), `RoleAddrs`
  (a node's five addresses + `role: NodeRole` = `Control`/`Data`/`Both` +,
  since ADR 0040 PR3, an explicit **`id: NodeId`** field — every config entry
  now names its own identity rather than it being purely re-derived from
  position; `ClusterConfig::from_json` hard-errors on a duplicate `id`
  across entries), role-filtered accessors (`control_ids`/`data_ids`/
  `peer_book`, which read each entry's own `id` field, not an index
  re-derivation), `generate`/`generate_split`, and the **five-port stride**
  (`base_port + 5*i + {internal,client,dynamo,cql,admin}`). `generate`/
  `generate_split` mint `"n{i}"` (`config::node_id(i)`, still the
  free-function convention `nid(u64)` mirrors for tests), **zero-padded**
  once the cluster has ≥ 10 nodes (`minted_id`) so lexicographic id order
  stays == numeric index order (`"n10" < "n2"` otherwise) — below that
  threshold ids stay the plain unpadded `"n{i}"` every existing test already
  assumes. ADR 0040 PR1 merged the pre-existing `control i` / `raftkv 300+i`
  pair into one identity per node; PR3 made that identity a validated
  string, not a `u64`.
- **`control_handle.rs`** — the `ControlHandle` seam (ADR 0035 PR1):
  `Local(RaftNode<ProdEnv>)` for a node with real control Raft, vs.
  `Remote(RemoteControlClient)` for a data-only node reaching a separate control
  deployment over the network. `metadata_cached()` vs. `metadata_fresh()`
  freshness contract lives here.
- **`topology.rs`** — pure, side-effect-free routing decisions extracted from
  `lib.rs` for unit-testing: `decide_cp_route` (→ `RouteDecision`), `tablet_for_key`,
  and `format_not_leader_refusal`/`parse_not_leader_refusal` (the leader-hint
  string suffix `cp_forward` chases). All `pub(crate)`.
- **`dynamo.rs`** (~59 KB) — the DynamoDB JSON-over-HTTP edge; the `GET /metrics`
  route (ADR 0015) shares this listener.
- **`cql.rs`** (~42 KB) — the CQL (Cassandra) v4 binary-protocol edge.
- **`cql_client.rs`** — a minimal loopback CQL client the admin dashboard's CQL
  editor uses (`POST /admin/data/cql`) to drive this node's own CQL port.
- **`admin.rs`** (~58 KB) — the admin/debug HTTP-JSON endpoint (ADR 0020):
  read-only `GET` views + gated `POST` actions + the dashboard's data-write
  surface; also serves the SPA static assets.
- **`http.rs`** — shared hand-rolled HTTP/1.1 helpers (request parser + response
  writers) used by both `dynamo.rs` and `admin.rs`.
- **`otel.rs`** — OTLP/HTTP distributed-tracing seam (ADR 0027); opt-in, no-op
  unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Scoped to this crate only.
- **`dashboard.rs`** + **`dashboard.{html,css}`** + **`dashboard_{core,overview,
  placement,tablets,browser,storage,node}.js`** — the "AnimusDB Console" SPA
  (ADR 0021), `include_str!`'d and served as distinct static assets. Vanilla JS,
  no bundler/CDN. Tabs are role-gated client-side (ADR 0035 PR7).

## CLI reference

`main.rs` documents these invocation modes (durable LSM backend by default;
`--ephemeral` selects the volatile memory engine):

| Invocation | What it does |
|---|---|
| `gen-config --nodes N [--host H] [--base-port P]` | print a combined-mode cluster config (JSON) |
| `gen-config --control-nodes N --data-nodes M [--host H] [--base-port P]` | print a split-deployment config (ADR 0035) |
| `--config FILE --node I [--dir DIR] [--ephemeral]` | run node I of a config, combined mode (one process per node) |
| `--cluster N [--dir DIR] [--ip ADDR] [--ephemeral] [--auto-split K] [--auto-split-bytes B]` | run an N-node combined cluster in one process (dev) |
| `--cluster-control N --cluster-data M [--dir DIR] [--ip ADDR] [--ephemeral] [--auto-split K] [--auto-split-bytes B]` | run a whole split deployment in one process (dev, ADR 0035) |
| `join --seed ADDR[,ADDR...] [--id NAME] --base-port P [--ip A] [--dir D] [--ephemeral]` | combined-mode seed/join startup (ADR 0032 PR2; ADR 0040 PR4: `--id` proposes a durable identity, omitted self-mints one) |
| `control --config FILE --node I [--dir DIR] [--ephemeral]` | run node I as a control-only node (ADR 0035 PR3); `--ephemeral` (ADR 0038) selects a volatile in-memory system-keyspace engine instead of the durable on-disk default — `Metadata` does NOT survive a restart |
| `data --config FILE --node I [--dir DIR] [--ephemeral]` | run node I as a data-only node (ADR 0035 PR4) |
| `data --seed ADDR[,ADDR...] [--id NAME] --base-port P [--ip A] [--dir D] [--ephemeral]` | data-only seed/join (ADR 0035 PR5; ADR 0040 PR4: `--id` proposes a durable identity, omitted self-mints one) |

`--auto-split K` (key count) and `--auto-split-bytes B` (byte size) are
independent OR-gated triggers — either, both, or neither. **`--node I` is
gone from `join`/`data --seed` entirely — a clean break (ADR 0040 PR4)**:
there is no more index to derive a default port range from, so `--base-port`
is **required** on both. `--id NAME` proposes a durable identity
(`NodeId::propose` validates it at the CLI boundary); omitted, the node
**self-mints** one (`NodeId::mint`, ADR 0040 Decision B) and claims it via
`MetaCommand::RegisterNode`'s registration CAS (Decision C) — closing ADR
0032's own documented residual race (two simultaneous joiners choosing the
same identity) structurally, not just by convention. A self-minted join is
**ephemeral-identity**: a restart with a fresh dir mints a *new* id, and the
old id's `Member` entry lingers `Down`/address-less forever (never reused,
prunable later via the existing `RemoveMember`/decommission path). `--id
NAME`'s durable, restart-stable identity is unaffected. One unified entry
point per role now (`run_node_join`/`run_node_data_join` in `lib.rs`, both
taking `id: Option<NodeId>`) — the old `run_node_join_allocated`/
`run_node_data_join_allocated` split is gone, along with
`check_join_collision`/`generate_join_nonce` (superseded by the CAS and by
`animus_env::prod::PreBindRng`, respectively).

## Deployment shapes (ADR 0035)

Three shapes, all built from the same role assemblies:

- **Combined** — every node runs both roles. `--cluster N` (one process) or
  `--config FILE --node I` (one process per node), against a `Both`-role config.
  `Node::bind` → `BoundNode::start_with`.
- **Control-only** — a small static metadata quorum, no CP **data** storage
  engine, no data role. `animusd control --config FILE --node I`.
  `Node::bind_control` → `BoundControlNode::start_control_with(.., backend)` —
  **fallible** (`io::Result<Node>`) and takes a `StorageBackend` since ADR
  0038: it now **unconditionally** provisions one small **dedicated** system-
  keyspace engine (`StorageBackend::Lsm` by default, `::Memory` under
  `--ephemeral`) — see `animus-control/CLAUDE.md`'s `node.rs`/`mirror.rs`
  entries. This is no longer an optional shadow-mode mirror (ADR 0038 PR2's
  original shape): `Metadata` is `StateMachine::DRIVER_APPLIED`, so this
  engine is the durable home of the control plane's async apply task's
  published cache — there is no more engine-less control-plane deployment
  shape.
- **Data-only** — no local control `RaftCore` at all; `Metadata` comes from a
  polled/long-polled mirror of a separately-deployed control plane via
  `ControlHandle::Remote`. `animusd data --config FILE --node I` (or `data
  --seed ADDR`). `Node::bind_data` → `BoundDataNode::start_data_with`.

A config may **mix** combined-mode indices with control-only/data-only ones for
incremental migration. `--cluster-control N --cluster-data M` and
`start_split_cluster_with` are the in-process (dev) equivalent of a genuine
split deployment; each in-process node still gets its own `ClusterEdgeState` and
reaches others only through real forwarding/relay/mirror paths.

`BoundNode::start_with` and `BoundControlNode::start_control_with` share a
private `spawn_common_tail` helper (route/metrics/self-registration/serve/admin);
role-specific tasks (`bootstrap`, `peer_sync_loop`, the growth mirror,
`heartbeat_loop`, the reconciler, `auto_split_loop`, dynamo/cql listeners) are
appended by each `start_*` after it returns.

## Request routing (CP)

Five `ClientCtx` primitives resolve the tablet's group leader the same way via
`cp_route` (pure core: `topology::decide_cp_route`): `cp_read` (linearizable
ReadIndex), `cp_write`/`cp_delete` (Raft-committed, waited to durable+applied),
`cp_scan` (linearizable range read), and `cp_batch_write` (groups keys by tablet,
commits each group as one `KvCommand::Batch` entry — atomic within a tablet, not
across; backs DynamoDB `BatchWriteItem` and the admin seeder).

`cp_route` serves **locally** if this node hosts the leader; **forwards** one hop
(`ClientRequest::Forwarded { request, traceparent }`) to the leader's node if a
local replica gives a hint + a `client_route` exists; otherwise **waits** for the
local group to elect (never forwards to a non-leader, including itself, during
election). **One-hop invariant**: the receiver (`cp_serve_forwarded`) never
re-forwards.

**Hinted-retry forwarding** (`ClientCtx::cp_forward`, the single choke point for
every forward): a "not the leader here" refusal carries the refusing node's own
leader hint (`topology::format_not_leader_refusal`, a plain string suffix so old
and new binaries interoperate); `cp_forward` chases it — retry at the hint if
untried, else at another of the tablet's known replicas, bounded to one pass over
{hint} ∪ replicas within the overall `CLIENT_TIMEOUT`.

**Election-wait backoff (PR #106)**: when *every* candidate refuses with
`leader_hint=none` (the group is mid-election — a split-child/first-provision
formation window, or a crashed leader), one exhausted pass is not a failure.
`cp_forward` backs off `FORWARD_ELECTION_BACKOFF` (100ms, ≈ one election timeout,
lib.rs:470) and re-runs the pass, still hard-bounded by `CLIENT_TIMEOUT` — the
forwarded dual of the local path's `RouteDecision::Wait`. Gated on the tablet
being resolvable so an unmappable op still fails fast. Regression:
`tests/cluster_split.rs::single_shot_first_write_through_control_node_succeeds`.

**Write fences (ADR 0028)**: `cp_put_local`/`cp_delete_local`/`cp_batch_propose`
each (1) **pre-check** the target group's live `RaftKvNode::scope_range()` and
reject before proposing if any key falls outside it (returning a routing-failure
error so the caller re-resolves and reaches the correct child), and (2) **stamp**
that range as the proposed entry's `fence` (`put_fenced`/etc.). The pre-check is
load-bearing: a fenced-out entry still commits as a no-op, so a confirm keyed on a
coarser signal would falsely-ack; the embedded fence only covers the sliver
between pre-check and apply. `cp_get_local`/`cp_scan_local` run the read-side dual
(ADR 0033): a read resolving to a group whose live scope doesn't cover the
request errors retryably rather than serving a false "absent" (for scans, avoids a
silent truncation). See the in-crate `split_fence_tests`.

## Multi-participant transactions (ADR 0018 §2/PR4, recovery in PR5)

`ClientCtx::cp_txn(writes, preconditions) -> Result<HlcTimestamp, String>` is
the coordinator for a cross-tablet (possibly cross-table) atomic transaction,
reachable via `ClientRequest::Txn`. It groups `writes`
(`(table, key, Option<value>)`) by owning tablet; the **first** write's
tablet is the **anchor** (mints the `TxnId`/record key, via
`RaftKvNode::txn_stage_anchor` — passed every *other* participant's
`(table, span)` list up front, ADR 0018 §2/PR5, so the record's own
`intent_spans` names every participant, not just the anchor's own writes),
every other tablet is a **participant** (`txn_stage_participant`). Prepare
runs the anchor first, then every participant **concurrently**
(`futures::future::join_all`) — both through `ClientCtx::
txn_prepare_pushing` (ADR 0018 §2/PR6, task #16), not `txn_prepare`
directly: a stage call returning `Ok(..)` only means its entry *applied*,
never that it genuinely wrote an intent — `KvCommand::TxnStage`'s
apply-time writer-push-intents guard rejects (whole-or-nothing) a target
key already holding a *different* transaction's unresolved intent, exactly
like a fence/seal miss. `txn_prepare_pushing` verifies every staged key via
`ClientCtx::txn_verify` (the same wire-routed `RaftKvNode::
txn_verify_staged` a recovery push already uses) after each attempt,
retrying (`TXN_STAGE_PUSH_ATTEMPTS`, backed off by `TXN_STAGE_PUSH_
BACKOFF`) before returning a client-facing conflict error — without this, a
blocked stage would look identical to a genuine one, and the transaction
would go on to commit without that key's write ever having happened, a
worse atomicity violation than the durability hole this whole fix closes.
`staged` tracks every participant that needs resolving, the anchor's own
keys included (PR5: `txn_decide_anchor` no
longer resolves anything inline — see below). Any prepare failure, or a
failed pre-commit precondition re-check, proposes an abort on the anchor. On
success, `commit_ts` is the anchor's own `txn_commit_at_least` result,
floored at the max of every participant's acked stage ts — the **single
Raft commit on the anchor's record is the atomic commit point**.

**Every decide attempt reports the record's ACTUAL outcome, never what was
asked for (ADR 0018 §2/PR5 decision-semantics amendment)**:
`txn_decide_anchor` proposes commit or abort, then always re-reads
`txn_status_local` and returns a `TxnOutcome` — recovery makes duelling
deciders legal (a still-live coordinator's commit can lose to a concurrent
recovery abort, or vice versa; the anchor's own Raft log position is the
sole arbiter, never who proposed first), so `cp_txn` branches on the actual
outcome at every decide point (an abort attempt that turns out to have
raced a recovery commit reports success, not the original failure; a
commit attempt that lost to a recovery abort reports the abort, not a false
success).

**Resolve is asynchronous, post-ack, on the successful-commit path (ADR
0018 §2/PR5 — the PR4 amendment's own flagged deviation, now lifted)**:
once the anchor's commit is durable, `cp_txn` returns immediately and
spawns (`tokio::spawn`) a best-effort resolve of every participant, the
anchor's own keys included, in the background — safe now that
`txn_resolver_loop` exists as the safety net for whatever this spawn
doesn't get to. The abort paths still resolve synchronously before
returning (no successful ack to speed up on an error return).

**Internal-only `ClientRequest` variants — `TxnPrepare`/`TxnDecide`/
`TxnResolve`/`TxnStatus`/`TxnRecordView`/`TxnVerify` — are never sent
bare**, only wrapped in `Forwarded` (the top-level `handle_request`
dispatcher rejects a bare one with an error); their real handling lives in
`cp_serve_forwarded`'s match only. Routed by the **actual data key** being
staged/resolved/verified (`table` + `writes[0]`/`keys[0]`/`span.start`),
never `record_key` for `TxnPrepare`/`TxnResolve` — a non-anchor
participant's `record_key` names the anchor's record, which lives in a
*different* tablet's (possibly a different table's) keyspace entirely (see
`RaftKvNode::txn.rs`'s `record_table` doc). `TxnDecide`/`TxnStatus`/
`TxnRecordView` always target the anchor's own tablet, so routing by
`record_key` there is correct. These are data-plane RPCs, not
`MetaCommand`s — `is_relayable_command` (control-plane schema-DDL relay
gating) does not apply to them; grepped and confirmed per the house lesson
on adding a variant to a forwarded command enum. **`TxnDecide` no longer
resolves anything and its reply carries the record's actual `TxnOutcome`**,
not a bare ts (see above) — an internal-only wire shape change, no
back-compat concern.

**Foreign-intent read resolution** (`ClientCtx::cp_get_local_resolving`,
used by `cp_read`'s `Local` arm and `cp_serve_forwarded`'s `Get` arm — the
original `cp_get_local` stays test-only, used by the in-crate
`split_fence_tests`, which drives a raw `CpGroup` handle with no
`ClientCtx` around it): tries `RaftKvNode::linearizable_get_served_fast`
first; on `FastRead::Foreign`, routes a `TxnStatus` query to the intent's
actual record owner and finishes the read via
`RaftKvNode::resolve_intent_given_status` once decided. **ADR 0018 §2/PR5
(lifting the PR4 amendment's flagged deferral)**: a still-`Pending` (or
failed) status query now calls `ClientCtx::txn_recover` before giving up,
rather than immediately reporting "retry" — `txn_recover`'s own grace check
means an ordinary in-flight transaction is never disturbed by this. A
locally-`Pending` intent (the single-participant/anchor case) still falls
back to the bounded internal wait, unchanged from PR3 — `txn_resolver_loop`
is what eventually pushes a stale local record instead.

**In-doubt recovery (ADR 0018 §2/PR5)**: `ClientCtx::txn_recover(
record_table, record_key, txn_id, intent_ts_hint) ->
Result<TxnDecisionStatus, String>` is the "push" — any actor holding a
foreign-or-local `Pending` intent past `animus_cp_data::RECOVERY_GRACE`
(5s, liveness-only) may call it. Reads the record (`txn_record_view`, the
`TxnRecordView` recovery-view dual of `txn_status_local`); already decided
→ resolve and return; `Pending` and not yet stale → decline (`Pending`,
propose nothing); `Pending` and stale → verify every `(table, span)` in
`intent_spans` (`txn_verify`, does the owning tablet still hold a live
intent — `RaftKvNode::txn_verify_staged` over the wire); all staged →
propose `TxnCommit`, any missing → propose `TxnAbort`; re-read the actual
outcome (never trust the proposal) and resolve every participant
(`recovery_resolve`, grouping `intent_spans` by table). See the ADR's PR5
amendment for the full safety argument (why a recovery commit and a
coordinator's own commit are always the same decision, and why a recovery
abort racing a live coordinator is a legitimate, safe outcome, never data
loss).

**No record at all (ADR 0018 §2/PR5's orphan-record fix, §2b)**: a real,
already-acknowledged possibility — the anchor's own `TxnStage` can silently
no-op on a fence/seal miss, exactly like a participant's already could
(PR4's own documented gap, now applying to the anchor's own stage too), so
a pusher's `txn_record_view` query can come back empty even though a
participant genuinely staged. `intent_ts_hint` (the foreign-intent read
path's own `FastRead::Foreign`/`IntentInfo::version` — the orphaned
intent's applied timestamp) is the only trustworthy grace-clock substitute
in that case; with none supplied (the resolver loop's own sweep never has
one, since `pending_txns()` only ever tracks a genuine local record), the
call declines. Past grace on that substitute, `txn_recover` proposes an
**orphan-abort tombstone** (`txn_decide_anchor`'s `orphan_created_ts`
parameter → `RaftKvNode::txn_abort_orphan`) — always an abort, never a
commit (an absent record gives no participant list to verify "all staged"
against). A **late-arriving** genuine anchor `TxnStage` for that same
`txn_id` then finds the tombstone and no-ops instead of resurrecting it to
`Pending` — `apply_and_compact`'s own resurrection guard, not anything
`animusd` has to arrange.

`txn_resolver_loop` (`lib.rs`, data-role-gated, spawned alongside the
tablet-host reconciler and `auto_split_loop` in both `BoundNode::start_with`
and `BoundDataNode::start_data_with`, `TXN_RESOLVER_INTERVAL` = 1s, plain
fixed interval): for each tablet group this node currently **leads**, pushes
every `RaftKvNode::pending_txns()` entry via `txn_recover` and fans a
resolve out for every `unresolved_decided()` entry — the proactive half of
recovery, and what makes `cp_txn`'s async resolve (above) safe to leave
un-awaited. Three new metrics: `CpTxnRecoveredCommitted`/
`CpTxnRecoveredAborted`/`CpTxnResolverRuns`.

**A wire-reachable panic found (and fixed) while testing this**:
`RaftKvNode::txn_stage`'s anchor-key-length assert (ADR 0022, `TOKEN_BYTES`)
was a sound "caller invariant" before `ClientRequest::Txn` existed — no
untrusted caller could reach it with an arbitrary key. `cp_txn` now
validates every write's key length up front and returns a client-facing
error instead of ever reaching that assert. See `docs/engineering-
lessons.md` for the general lesson.

Tests: `tests/cp_txn.rs` (real 3-process cluster + a genuine pre-split
table) — multi-tablet atomicity, the follower-connected forwarding
regression (the identical transaction issued from every node in turn),
concurrent transactions each individually atomic, a violated precondition
aborting the whole transaction, and (ADR 0018 §2/PR5) a coordinator crash
between prepare and decide — driven by sending the internal `TxnPrepare`
wire requests directly (mirroring exactly what `cp_txn` does over the
network) and then simply never sending `TxnDecide`/`TxnResolve`, since
`cp_txn` runs synchronously inside one request handler with no separate
long-lived coordinator process to literally kill — converging to a
committed read from an uninvolved node within grace + resolver margin, plus
its dual (a commit already applied but never resolved, converging via
ordinary reads with no grace wait needed at all). The 2PC mechanics
themselves, and PR5's recovery/decision-semantics fix, are proven
deterministically at the primitive level in `animus-cp-data`'s
`tests/txn_multi.rs`/`tests/txn_recovery.rs`.

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

**`config()` returns `Option<BTreeSet<NodeId>>`, not a bare set (ADR 0037 PR2).**
`Local` is always `Some(raft.config())` — a genuine control-group replica reading
its own live `RaftCore` config. `Remote` has no local `RaftCore`, so it answers
the last control-voter set it has *observed on the wire* (`RemoteControlClient::
control_voters`, fed by `observe()` under the same freshness gate as the metadata
mirror) — `None` until the first `Status`/`WatchMetadata` reply lands. This is
deliberately an `Option`, not an always-populated `BTreeSet::new()` default as it
used to be: "never fetched yet" and "the control group genuinely has zero
voters" must stay distinguishable to any caller that cares (see the
engineering-lessons "handle has no local authority" entry) — most callers don't
and just `.unwrap_or_default()` it (`/admin/raft`'s `voters` field, the
`ClientResponse::Status::control_voters` wire field below).

**`ClientResponse::Status` carries `control_voters: BTreeSet<NodeId>`
(`#[serde(default)]`, ADR 0037 PR2)** — the answering node's own
`ctx.control.config().unwrap_or_default()` at reply time. This is the wire echo
of the *live* Raft config that actually governs control-plane quorum, distinct
from `Metadata.node_addrs`' `role: "control"` bookkeeping (a discovery hint: a
node can be registered with the control role and not currently be a live voter —
before its membership change lands, or after it's been removed). It rides the
same `Status`/`WatchMetadata` round trip `metadata_fresh()`/the mirror sync loop
already make, so a `Remote` node's own `RemoteControlClient` picks it up for
free — no new request type. A future control-plane membership-change admin
surface (later PR in the ADR 0037 stack) is the intended reader of this on a
`Remote`/CLI/dashboard caller that needs "who can I even try talking to."

**Discipline**: a read feeding a *non-retried, permanent* decision must use
`metadata_fresh()`, not `metadata_cached()`/`effective_metadata()` — a data-only
node's routinely-stale mirror makes that window wide. The type system can't catch
this (`Remote` and `Local` both compile). Grep every `metadata_cached()` call
site when adding a `ControlHandle` consumer. `provision_tablet` was fixed for
exactly this (RF silently pinned at 1); see the root `CLAUDE.md`
engineering-lessons log. **That fix only closed the READ side (a stale
`Remote` mirror) — it did not close the deeper hazard, which recurred later
under heavy concurrent load and got its own fix**: `provision_tablet`'s
`SetTabletPolicy` no longer derives a tablet's RF from `t.replicas.len()` (the
observed size of its *initial* replica set) at all, even off a maximally
fresh read — it always records the fixed target `MAX_REPLICATION_FACTOR`, so
a best-effort under-sized initial set self-heals via `reconcile_placement`
the moment enough candidates are `Active`, rather than the observed size
becoming a silently-permanent policy. See the engineering-lessons entry on
this recurrence and `tests/tablet_rf_self_heals.rs`.

**`Remote` internals** (`RemoteControlClient`): `seeds` (the control deployment's
client-API addresses), a polled `mirror`, and a `leader_hint`. `metadata_fresh()`
tries the hint first, else scans every seed. `ClientResponse::Status` carries
`leader_hint` and a `watermark: u64`; the long-poll `ClientRequest::WatchMetadata
{ last_seen }` (ADR 0035 PR5) gives a `Remote` node a real wake-on-commit signal
via `remote_metadata_watch_loop` (a genuine `Local` replica serves it, parking on
`metadata_watch().changed(last_seen)` up to an 8s server bound; a `Remote` node
rejects it outright). `RemoteControlClient` owns its own driven `MetadataWatch`
(this required making `animus_control::MetadataWatch::bump` `pub`).

**The ADR 0030 growth-node branch of `remote_metadata_sync_loop` uses the same
long-poll mechanism**, not the original fixed-200ms `Status` poll — a growth
node's `ClientCtx.control` stays `ControlHandle::Local` (a real, permanently
non-voting control-group member, not `Remote`), so it constructs a standalone
`RemoteControlClient::with_mirror(seeds, ctx.remote_metadata.clone())` sharing
`ClientCtx.remote_metadata`'s existing `Arc<Mutex<Option<Metadata>>>` directly
as its mirror, then drives it through the same `remote_metadata_watch_loop`.
Pure latency improvement — the reconciler's own wake source is unaffected (a
growth node's local raft never advances, so its `metadata_watch()` still never
fires; `RECONCILE_FALLBACK_INTERVAL` still drives its ticks, just off a
fresher mirror). Regression: `tests/cluster_growth.rs::
growth_node_observes_metadata_promptly_via_watch`. **Gotcha surfaced by this
port**: a `WatchMetadata` request already in flight to a node at the instant
it's killed via `Node::shutdown()` doesn't fail over quickly — `shutdown()`
can't abort an already-spawned `serve_clients` per-connection handler task
(fire-and-forget, no tracked `JoinHandle`), so the zombie handler's
`select! { changed(..), sleep(8s) }` always falls through to the timeout arm
(its watch can never advance once the driver is dead) and replies with
stale-but-plausible cached data up to 8s late. A fixed-sleep assertion right
after a test's node-kill can be outrun by this; poll to convergence instead
(see the engineering-lessons log).

**`WatchMetadata`'s reply is incremental (ADR 0038 PR5).** `ClientCtx::
watch_metadata` — after the long-poll resolves (wake-on-commit or the
`WATCH_METADATA_SERVER_TIMEOUT` bound) — tries the serving node's own
`RaftNode::watch_delta_since(last_seen)` first: if its bounded delta ring
(`animus_control::DeltaRing`) contiguously covers `(last_seen, watermark]`,
the reply is a cheap `ClientResponse::MetadataDelta { writes, watermark,
leader_hint, control_voters }` instead of a full `Status` clone — `writes` is
a `Vec<animus_control::mirror::KeyWrite>`, empty exactly when nothing changed
(the timeout-elapsed case, still cheaper than cloning `Metadata`). Falls back
to the original full `ClientResponse::Status` reply whenever the ring doesn't
cover the range (a fresh/lagging/just-recovered replica) **or** while this
node's own ADR 0030 growth-node mirror overlay is active
(`ClientCtx.remote_metadata` populated) — that overlay serves
`effective_metadata()` from a different source than this node's own
(permanently inert, on a growth node) local ring, so a delta off that ring
would answer the wrong question. A plain `ClientRequest::Status` request is
untouched — always the full reply, unconditionally; only `WatchMetadata`
gained the incremental option.

`RemoteControlClient::observe_delta` is the **single shared consumer** for
both `Remote` (a genuine ADR 0035 PR4 data-only node) and the ADR 0030
growth-node branch above — both drive it through the identical
`remote_metadata_watch_loop`, which now matches on both
`ClientResponse::{Status, MetadataDelta}`. `observe_delta` installs each
`KeyWrite` onto the cached `Metadata` via `animus_control::mirror::
apply_key_write` (never replaying a `MetaCommand` — this crate carries no
control-plane business logic to do that correctly). **Race guard**: since
`RemoteControlClient` is `Arc`-shared between the background watch loop and
any concurrent `metadata_fresh()` caller, a delta is only applied if the
mirror's *current* watermark exactly equals the delta's own `last_seen`
basis (checked and mutated under one lock, alongside the same tightening
applied to `observe`'s pre-existing `watermark >= watch.latest()` check) — a
concurrent full `observe()` moving the mirror in the meantime makes the
delta's sequential application unsafe to apply blindly (unlike a full
replace, which is order-independent modulo the monotonic watermark guard); a
stale delta is dropped, not mis-applied, and self-heals on the loop's next
iteration with a corrected `last_seen`. Regression:
`tests/watch_metadata.rs` (the wire server side, incl. a real control-only
process restart resetting the ring and forcing the correct fallback) and
`tests/cluster_growth.rs::growth_node_observes_metadata_promptly_via_watch`
(the growth-node path, proven via the shared call site).

## Tablet lifecycle

**The per-node tablet-host reconciler (ADR 0031 PR4) is the single owner of this
node's tablet lifecycle** — it replaced three separate loops (`cp_join_host_loop`,
`cp_gc_loop`, `cp_reconfigure_loop`) and their state. The pure `plan` +
`Reconciler` executor live in `animus_cp_data::host` (read that crate's
`CLAUDE.md`); `plan` decides every action from one `MetadataView` snapshot per
tick and executes them in fixed order (`NarrowScope` → `Host` → `Reconfigure` →
`Release`/`Reclaim`; merge adds `WidenScope`/`Absorb`). What stays in `animusd`
(`tablet_host_reconciler_loop`):

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
positional median. Auto-merge triggering is out of scope — merge is
operator-driven.

**Split / merge** (ADR 0028 / 0033) are each a single atomic control-plane command
(`MetaCommand::SplitTablet`/`MergeTablets`, epoch-CAS gated) — there is no
data-plane half. Split narrows the source's range and mints a sibling on the same
shared engine; merge widens `left` to absorb `right`, recording `right` in the
never-pruned `Metadata::merged_tablets` marker (needed because a
hosted-but-vanished tablet looks identical whether merged or its table dropped).
The reconciler reacts with `WidenScope`/`Absorb` (absorb tears down **without
erasing** — a sibling now serves the range). `trigger_split`/`trigger_merge`
propose and poll for the exact effect. Exposed via `POST /admin/tablet/{split,
merge}` + `ClientRequest::{SplitTablet,MergeTablets}` (relayable).

**Drop-table GC** (ADR 0024) is the reconciler's `Reclaim` action;
**removed-replica GC** (ADR 0029) is its `Release` dual (moved off this node while
the tablet still exists — a drain/repair/rebalance). Both run
`shutdown()`+wait-`is_stopped()` then `erase_scope()` + delete the per-tablet WAL.
Release is gated on the **local durable Raft config already excluding `base_id`**
plus an epoch-stability dampener (`RELEASE_CONFIRM_TICKS`). The release erase is
bounded by the tablet's **current replicated range** (`HostAction::Release`'s
`erase_bound`), never a stale-wide in-memory scope — the invariant ADR 0031 makes
structural. Drop + GC are convergent (a restart replays through historical map
states) — test post-restart state with a poll, never a fixed sleep. A new
`MetaCommand` that must commit from a follower-connected node must be added to
`is_relayable_command` (missing there is a bimodal per-process flake).

## Wire edges

All edges are production-only I/O (real tokio sockets, hand-rolled framing) and
route below the edge through the same `ClientCtx` CP primitives.

- **DynamoDB** (`dynamo.rs`, `RoleAddrs.dynamo`) — decodes `X-Amz-Target` +
  AttributeValue-JSON via `animus_dynamo::wire`. `CreateTable` proposes its key
  schema **and** GSI/LSI *definitions* into the replicated catalog (ADR 0013) and
  waits for commit (survives restart); a node reconciles its local registry from
  `Metadata::table_indexes` via `mirror_catalog_schema`/`sync_indexes`. Index
  *entry data* stays in-memory, maintained from observed writes and **lazily
  backfilled** on first index query (`backfill_index_if_needed`). Base-table
  `Query`/`Scan` use `cp_scan` (no in-memory key tracking). Surface also covers
  `UpdateItem`/`BatchWriteItem` (condition-gated, per-request/per-tablet
  atomicity only) and, since ADR 0018 §2/PR7, **atomic** `TransactWriteItems`
  (whole-or-nothing across however many tablets/tables it spans, via
  `ClientCtx::cp_txn`) and `TransactGetItems` (a quiescence-confirmed
  consistent multi-key read) — see that ADR's PR7 amendment for the
  condition-evaluation layering and a documented cross-node OCC limitation
  for a write action's own condition. `DeleteItem` writes a tombstone *value*.
- **CQL v4** (`cql.rs`, `RoleAddrs.cql`) — `STARTUP`/`OPTIONS` handshake +
  `QUERY`/`PREPARE`/`EXECUTE` via the pure `animus_cql` crate. `CREATE TABLE`
  proposes a typed schema into the replicated catalog (incl. clustering/compound
  keys). A partition is one CP value, so `INSERT`/`UPDATE`/`DELETE` are RMW under
  `rmw_lock`; the requested consistency level is accepted but moot (CP).
  Keyspaces are **replicated** (`CREATE KEYSPACE` proposes
  `MetaCommand::CreateKeyspace` into the control plane's `Metadata`, ADR 0013;
  `USE`/qualifier validation reads the replicated set via `keyspace_exists`,
  with a `ks.table`-prefix fallback). Only the **prepared-statement store**
  (`CqlState`) is per-node edge state (shared across connections *to the same
  node*, isolated between nodes, lost on restart); prepared ids are
  content-addressed (FNV-1a of the text).
- **Admin / debug** (`admin.rs`, `RoleAddrs.admin`, ADR 0020) — read-only `GET`
  views (`/admin/{config,status,peers,raft,raftkv,txns,storage/*,metrics,metrics/
  history,member/drain-status,health,control/members}`) + gated `POST` actions
  (`/admin/{tablet/split,tablet/merge,storage/flush,storage/compact,raftkv/
  reconfigure,drain,member/add,member/remove,control/member/add,control/
  member/remove}`) + data writes (`/admin/data/{dynamo,cql,drop-table,
  seed}`). Below the edge it only reads node state (aggregated live per request) or
  drives a gated action. **No auth — bind to a trusted interface.** The `animus
  admin` CLI consumes it. The bulk seeder (`action_data_seed`) writes real
  **DynamoDB items** — key attributes resolved from the replicated catalog
  schema (ADR 0013), key/value bytes built exactly as the DynamoDB edge's
  `PutItem` would (`dynamo::item_key` + `wire::encode_stored_item`, ADR 0022),
  so seeded rows read back through `GetItem`/`Query`/`Scan` — in
  `cp_batch_write_patient` batches, wrapped in its own `admin_seed` span (it
  bypasses `handle_client`, so it needs one to emit any trace). `key_display`/`parse_key_display` render a binary partition
  token as unpadded base64url; a plain-client key is verbatim/printable.
  `/admin/peers`'s `peers: [{admin, role}, ...]` field (ADR 0035 residual
  follow-up, `admin.rs::peers_view`) carries each node's deployment role
  straight off replicated `Metadata.node_addrs[*].role` — closing the gap
  where role was only knowable by fetching that specific node's own
  `/admin/config` first; `admin_addrs` itself is unchanged.
  **`GET /admin/txns` (ADR 0018 §2/PR7)** mirrors `/admin/raftkv`'s own
  node-local, one-entry-per-hosted-tablet shape (`CpGroup::txn_view`): each
  entry's `pending` lists this group's still-`Pending` anchored transaction
  records (record key, created timestamp, age vs. `RECOVERY_GRACE`, and a
  best-effort `intent_spans` summary — the one field costing a real
  `ReadIndex` round trip per pending entry, via `RaftKvNode::
  txn_record_view`, since a tablet anchors only a handful of these at once);
  `unresolved_decided` lists decided-but-not-yet-locally-resolved records.
  Pure observer, no gated action — the existing `txn_resolver_loop`/
  `ClientCtx::txn_recover` machinery already drives every record listed here
  to resolution with no operator action; manual resolution is deferred, and
  so is a dedicated dashboard tab (ADR 0021/0035 scope discipline).
  **`GET /admin/storage/control` (ADR 0038 PR4)** surfaces the
  **control-plane's own system-keyspace engine** stats (LSM levels/SSTables/
  memtable + WAL segments/durable_seq/rotations) — the control-plane
  analogue of `/admin/storage/lsm`+`/admin/storage/wal`, but keyed on
  `ctx.control_storage` (a second, read-only clone of exactly the engine
  handle passed to `RaftNode::start_with_metrics`) rather than a hosted CP
  tablet group, since a control-only node hosts none. `{"available": false}`
  on a data-only node (no local control role at all); on a **combined** node
  the numbers legitimately coincide with a hosted tablet's own
  `/admin/storage/lsm` — it's the exact same physical shared engine,
  `Metadata` just lives at a reserved key prefix within it.
  **`GET /admin/system-table?kind=&after=&limit=` (plan-syskv-ui, an ADR 0038
  addendum) browses this same engine's live rows**, one decoded system-
  keyspace entity at a time — the read-only counterpart to
  `/admin/storage/control`'s aggregate stats. Same `{"available": false}`
  shape on a data-only node. **Load-bearing**: scans
  `animus_control::syskv::reserved_scan_bounds()`'s `[start, end)` via one
  `StorageEngine::scan`, filtering by `kind` in memory afterward — never
  `StorageEngine::entries()`, which would scan the *whole* engine (every
  user table's data too, on a combined node sharing it with the CP data
  plane, ADR 0028); see the engineering-lessons entry before ever
  "simplifying" this to `entries()`. `applied_index` is a **dedicated point
  read** of the `_applied_index` watermark key, never derived from the
  (possibly empty/filtered/paginated) scan window. The `after`/`next_after`
  cursor is the base64url (`animus_dynamo::wire`, not `key_display` — a
  system key isn't a data-plane key) of the last item's raw engine key; the
  next page's lower bound is that key plus one `0x00` byte — exact and
  gap-free because `syskv` keys are provably prefix-free. Value decode
  mirrors `animus_control::mirror::apply_put` exactly (JSON passthrough for
  six kinds; `Counter` as a raw `u64`; `Keyspace`/`Merged`
  presence-only, always `null`); a numeric kind's `id` renders as a decimal
  string, not a JSON number. Every `EntityKind` is browsable, including the
  internal/legacy ones — full transparency by design, see
  `animus-control/CLAUDE.md`'s `syskv.rs` PR6 entry.
- **Web console** (`dashboard.rs` + assets, ADR 0021) — a self-contained
  vanilla-JS SPA, a pure client of `/admin/*` JSON (so responses carry CORS). Six
  views seeded by a `/admin/peers` fan-out; tabs are **role-gated client-side**
  (`applyRoleGating`, ADR 0035 PR7) — a data-only node shows a dedicated **Node**
  view (`dashboard_node.js`) instead of the cluster-wide tabs. The **Storage**
  tab (shown to control-only and combined nodes) carries a distinct "Control
  system keyspace" card (ADR 0038 PR4, `dashboard_storage.js`'s
  `loadControlStorage`/`updateControlStorageNodeOptions`) — its own node
  selector, filtered to nodes with a control role (`n.role === "control" ||
  "combined"`), independent of the per-tablet `st-tablet`/`st-node`
  selectors' hosting-based filter (which would otherwise always be empty for
  a control-only node, since it hosts no CP tablet group at all). That same
  card grew a **browse section** nested directly inside it (plan-syskv-ui,
  ADR 0038 addendum) — `dashboard_storage.js`'s `loadSystemTable`/
  `renderSystemTableKindOptions` against `GET /admin/system-table`: a kind
  filter (every kind, internal/legacy ones labeled `(internal)`/`(legacy)`),
  an "as of index N" watermark label, a table with `<details>`-based
  expand-to-full-JSON per row, and a forward-only "Next page" pager
  (`systemTableAfter`, reset on every fresh "Browse"/node-change). No new
  tab, no `ROLE_TABS` change — rides the same control-role node selector and
  gating the card already had. `loadSelf()`
  resolves this node's own role from a self-only fetch, kept separate from the
  slower cluster-wide fan-out. `/admin/config` carries a derived `role` string;
  `/admin/raft` carries a `control_mirror` object for the Node view. The
  Overview groups nodes as "Control plane" / "Data nodes" when any
  control-only node exists (a combined cluster keeps the flat list), and every
  reachable node's row — plus the Placement view's selected-node header —
  carries a `consoleLink()` (`dashboard_core.js`) to that node's OWN admin
  console, built from the origin the `/admin/peers` fan-out already resolved
  (empty for this page's own origin — a self-link is noise). **Cluster health
  means "is the data at risk," not "is anything in transition"** (ADR 0021 §7):
  `tabletStatus`'s ladder (`quorum-lost` → `under-replicated` → `healthy` →
  `forming`) only degrades on an actual redundancy/quorum loss; a split-child
  or freshly-provisioned tablet forming its Raft group with every assigned
  replica's node alive renders as a neutral `forming` pill, escalating to
  degraded only if stuck past 60s (`computeHealth`'s `overdueFormingCount`).
- **OTel** (`otel.rs`, ADR 0027) — `init_tracing(instance_id)` from `main.rs`;
  `current_traceparent`/`set_parent_traceparent` carry W3C trace context across a
  forwarded hop (`cp_forward` injects, the receiver's `handle_client`
  re-parents), so a forwarded write is one joined trace when export is enabled.
- **`GET /metrics`** (ADR 0015) shares the DynamoDB listener; `ClientCtx::
  metrics_text` aggregates both role sinks (control + raftkv) live at request time.

## Gotchas

- **A node runs one internal `ProdEnv`, on one id (ADR 0040 PR1)** — the control
  Raft rides `PRIMARY_STREAM` (stream 0, ADR 0026's default); every per-tablet
  Raft group this node hosts rides its own stream (`stream = tablet_id`, which
  floors at 1), so the two never collide on the one shared inbox. Before ADR
  0040, a combined node bound *two* `ProdEnv`s on two distinct ids (`control
  i` / `raftkv 300+i`) purely because one inbox was single-consumer — ADR 0026
  made that unnecessary by letting one id host several protocol instances,
  and ADR 0040 PR1 is "actually stop paying for the second id." The client API
  is a plain TCP server, *not* on the `Network` — a non-leader forwards over a
  fresh client connection.
- **`ClusterEdgeState` is scoped to one NODE** (ADR 0031 PR2), created fresh per
  node — even in `--cluster N`, which previously shared one instance across the
  cluster and masked cross-process bugs. Holds this node's own control handle, its
  hosted CP group handles (keyed by tablet), the DynamoDB `SchemaRegistry`, and
  the CQL `CqlState`. No process-global (`OnceLock`) mutable state.
- **`ClientCtx.data: Option<DataRole>`** groups the data-role-only fields
  (`rmw_lock`, `raftkv_metrics`, `base_id`). `ClientCtx::data()` **panics** if
  absent — safe only from paths that structurally can't run on a control-only node
  (dynamo/cql edges, `auto_split_loop`). `resolve_cp_route` must never panic — it
  matches `self.data.as_ref()` directly (control-only node ⇒ zero local replicas).
- **`--cluster N` without `--dir` reuses ONE fixed path** (`$TMPDIR/animusd`), and
  `--ephemeral` does NOT make the control/raftkv WALs ephemeral (it only selects
  the CP-data `StorageBackend`). Two concurrent `--cluster N` runs contend on the
  same on-disk WALs — always pass a fresh explicit `--dir` for a throwaway run.
- **The cluster's members are node ids** (ADR 0040 PR1 — before this, "the
  raftkv ids, not the control ids"; the two are now the same id space) —
  `bootstrap` (leader-only, idempotent) registers each data-role node's own id
  as `Active`. Failure detection runs over `ProdEnv`: each node's
  `heartbeat_loop_live` heartbeats the control group *as its own member id*,
  so the control leader's `detect_loop` marks a crashed node `Down`.
  **`heartbeat_loop_live`'s destination list is now live (ADR 0037
  hardening PR1, PR #134 — closing the ADR 0037 PR4 audit's deferred gap)**: it
  re-derives the control-group target list from `ctx.control.config()` every
  tick, instead of the bring-up-time `control_ids` snapshot the older
  `animus_control::node::heartbeat_loop` was pinned to forever (that function
  itself, and its `SimEnv` call sites, are unchanged — only `animusd`'s two
  real-node call sites moved to the new wrapper). A `ControlHandle::Remote`
  data-only node falls back to the static list until its first live
  `Status`/`WatchMetadata` reply lands. **Closing this needed a second,
  previously-undocumented fix the original deferral text never named**:
  `peer_sync_loop` (`lib.rs`) also merges `Metadata.node_addrs[*].internal`
  (ADR 0040 PR1 — was `.control`/`.raftkv`, now one field; this loop is also
  the sole survivor of what used to be a `peer_sync_loop`/
  `control_peer_sync_loop` pair, collapsed into one loop over one shared env's
  peer book) into the node's own peer book — without it, a live destination
  list alone is still inert, since `ProdEnv::send` silently drops a heartbeat
  aimed at an address-less peer.
  See `docs/engineering-lessons.md`'s entry on this PR for the
  two-staleness-axes mini-lesson (a static-destination-list audit must also
  check the transport address book) and ADR 0037's "Known deferrals" section
  for the full "Update: closed by PR #134" note.
- **Online growth (ADR 0030) is data-plane only** — the control group stays static;
  a grown node's control role is a permanent non-voter and mirrors `Metadata` via
  `remote_metadata_sync_loop` into `effective_metadata()` — long-polling
  `ClientRequest::WatchMetadata` (ADR 0035 PR5's mechanism, ported onto this
  branch too — see the `ControlHandle` section above), not a fixed-200ms
  `Status` poll. A replicated node
  address book (ADR 0032 PR1, `Metadata.node_addrs` + `route_sync_loop`) keeps
  `client_route`/`/admin/peers` live so forwarding reaches nodes grown in later.
- **A node's deployment role rides that same replicated address book**
  (`NodeAddrs.role: String`, ADR 0035 residual follow-up) — each of
  `BoundNode::start_with`/`BoundControlNode::start_control_with`/
  `BoundDataNode::start_data_with` stamps its own literal role
  (`"combined"`/`"control"`/`"data"`) at its `NodeAddrs` construction site, so
  `/admin/peers` can report every OTHER node's role straight from
  `Metadata.node_addrs` instead of the dashboard fanning out to each node's
  own `/admin/config` just to learn it. `#[serde(default = "combined")]` for
  WAL back-compat (every pre-ADR-0035 registration was combined-mode).
- **Decommission (ADR 0032 PR3)** = `drain` + `MetaCommand::RemoveMember`; check
  leadership *before* any metadata-dependent refusal (a follower's replica lags).
  Not a fence — a restarted process at the same raftkv id rejoins like a fresh
  join. **`admin_remove_member`'s control-voter refusal is now dynamic (ADR
  0037 PR4)**: it reads `self.control.config()` (the live Raft config), not
  the static `self.admin.control_ids` snapshot ADR 0030/0032 read — a node
  that *used to be* a control-core voter but has since been control-removed
  decommissions normally; a node that is still a *live* voter (even one
  added at runtime, an id the static list never knew about) is still
  correctly refused, now pointing the operator at the two-phase fix: `animus
  admin decommission --force-control-remove` (`animus-cli`) checks
  `GET /admin/control/members` up front and, if the target is a live voter,
  runs `control-remove` + polls to convergence *before* the ordinary
  drain → drain-status → remove flow (unchanged) even starts — without the
  flag it refuses immediately with the same message, before wasting a drain
  cycle on a target the final step would refuse anyway. Regression:
  `tests/decommission.rs::
  decommission_refuses_live_control_voter_then_succeeds_after_control_remove`.
  See the full `control_ids`/`admin.control_ids` static-vs-live audit in the
  ADR 0037 PR4 PR description (every other read is a legitimate seed/
  bootstrap use, left static on purpose).
- **Self-minted member ids (ADR 0040 Decision B/C, PR4) replace ADR 0036's
  monotonic allocator entirely** — `AllocateNodeId`, `ALLOC_ID_BASE`, the
  `next_alloc_id`/`node_id_allocations` ledger, `syskv::EntityKind::
  NodeIdAlloc`, `generate_join_nonce`, and `check_join_collision` are all
  **deleted**. A joining node self-mints (`NodeId::mint`, 22-char base64url
  off `animus_env::prod::PreBindRng` at the pre-bind CLI boundary — the
  narrow, reusable replacement for `generate_join_nonce`'s old bespoke
  OS-randomness exception) or proposes an explicit `--id`, then claims it
  via `MetaCommand::RegisterNode`'s registration CAS
  (`register_node_over_wire`/`claim_join_identity` in `lib.rs`, reached over
  the same `ClientRequest::ProposeSchema`/`Status` wire primitives every
  other join round trip already uses — no new wire type needed) **before
  ever binding a listener**: a minted collision (astronomically unlikely)
  re-mints and retries; a proposed-id collision fails loudly
  (`AlreadyExists`, naming the conflict). `is_relayable_command` (below) must
  allow `RegisterNode` — a joining process has no local control role at all
  yet, so relaying it is that process's *only* way to reach the real leader.
  **ADR 0040 PR1 simplification carries forward unchanged**: since a node
  has exactly one id (no more separate control/raftkv id pair), a
  self-minted join's id *is* its one identity, full stop — the discovered
  `original_control_ids` set simply doesn't contain it, which is what makes
  it a structurally-safe permanent non-voter.
  **`MetaCommand::RegisterNode` is also the *sole* self-registration
  mechanism now** — `spawn_common_tail`'s one-shot startup task (every node
  shape: a fresh bootstrap node, a growth node, or a permanently-non-voter
  control-only growth node with no other claim path at all) proposes it
  instead of the old address-only `RegisterNodeAddrs`, which is now
  **update-only** (rejects an id absent from both `members` and
  `node_addrs` — nothing to update yet). `RegisterNode`'s own CAS is keyed
  on `node_addrs` alone (not `members`/`labels` — see its doc in
  `animus-control` for why an equality check on those would race
  destructively against `UpsertMember`'s bootstrap insert or
  `admin_add_member`'s operator-labeled row) and **never claims a `members`
  row for a control-only registration** (`NodeAddrs.role == "control"`) — a
  control-only node has no `raftkv` role or storage engine and can never
  host a tablet, so appearing in `members` at all would make it a placement
  candidate and silently corrupt tablet placement the moment it's picked
  (caught by `tests/control_only.rs` going bimodal during this PR — see
  `docs/engineering-lessons.md`).
- **Orphan-member auto-reclaim sweep (ADR 0040 PR6)**: the mechanism itself
  (`Member.has_activated`, `Metadata::orphan_sweep_candidates`, the
  `RemoveMember` claim-without-member extension, the leader-side volatile
  `orphan_sweep_loop`) lives entirely in `animus-control` — see that
  crate's `CLAUDE.md`. This crate's whole contribution is plumbing the
  `orphan_sweep_after: Duration` knob from a config file/CLI flag down to
  `RaftNode::start_with_orphan_sweep_after` — `Duration::ZERO` disables the
  sweep outright. To avoid a wide, unrelated blast radius (touching the
  many existing test call sites of `run_node_with`/`run_node_control`/
  `start_cluster_with_auto_split*`/`start_split_cluster_with`, none of
  which care about this knob), **every existing public entry point keeps
  its exact signature**, defaulting internally to
  `animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER` (10 minutes); a
  parallel `_with_orphan_sweep_after`-suffixed (or, for the two
  `start_*cluster*` functions, `_and_orphan_sweep_after`/
  `_orphan_sweep_after`-suffixed) sibling function takes the explicit
  `Duration` and is what `main.rs`'s new `--orphan-sweep-after SECS` flag
  actually calls — mirroring the existing `auto_split_threshold`/
  `auto_split_bytes_threshold` layered-wrapper convention this file already
  uses, rather than introducing a `ClusterConfig` struct field (which would
  have required updating the struct literal at every one of that config's
  ~20 existing test call sites for one niche knob — a disproportionate
  sweep for what the CLI flag already covers for every real deployment
  shape). `run_node_growth`/`run_node_join` (`finish_combined_join`) are
  deliberately **not** given their own `_with_orphan_sweep_after` variant —
  a growth/join node always takes the default; wiring the flag through
  those two entry points as well is future work if ever needed, not
  required by this PR's scope. Only meaningful on a mode that runs a local
  control `RaftNode` (every mode except `data`, which has none).
  `/admin/raft`'s per-member view grew a `has_activated` field alongside
  the existing `believes_alive` one (same signal the sweep gates on); the
  Overview dashboard's node-row status text appends "(never activated)"
  for a `Down` member with `has_activated: false` — the one minimal,
  non-redesigning dashboard touch this PR makes.
- **Control-plane membership change (ADR 0037 PR3)**: `ClientCtx::
  admin_add_control_member`/`admin_remove_control_member` (`lib.rs`, near
  `admin_add_member`/`admin_remove_member`) grow/shrink the control group's
  *live* `RaftCore` config at runtime — local-control-leader-only, **not**
  relayed, **not** in `is_relayable_command` (the underlying primitive is
  `RaftNode::change_membership`, not a `MetaCommand` proposal, so there is no
  meaningful "relay" shape for it — only a genuine control-group voter's own
  in-process handle can call it). `POST /admin/control/member/{add,remove}` +
  `GET /admin/control/members` in `admin.rs`; `animus admin
  control-{add,remove,grow}` in `animus-cli`. Add takes an **operator-
  supplied** id (originally: below `animus_control::meta::ALLOC_ID_BASE`, a
  numeric range that no longer exists — see the later "Update" notes below
  for the self-minted omitted-`node` form and ADR 0040 PR4's full retirement
  of that range) and the new voter's **internal control-Raft** address
  (distinct from its
  admin/client/raftkv ports — `animus admin control-add` resolves it from the
  new node's own `/admin/config` so the operator only ever deals in admin
  addresses). **The PR3 known scope limit was closed by ADR 0037 PR4, since
  superseded by ADR 0040 PR1 (below is now historical — see the "Update"
  paragraph after it for the current mechanism)**: PR3 shipped with the
  freshly-added voter's address known only via `ProdEnv::merge_peer` called
  on **whichever node happened to be leader** at add time — a *later* leader
  (after a subsequent transfer or crash) had no path to independently
  rediscover it. PR4 added `NodeAddrs.control: Option<SocketAddr>`
  (`animus-control`'s `meta.rs`, `#[serde(default)]`, `None` for every
  statically-configured voter) — `admin_add_control_member` proposed it via
  `RegisterNodeAddrs` (replicated to every voter, same as the
  `raftkv`/`client`/`admin` axes always were), and every control-role node
  ran its own `control_peer_sync_loop` (near `peer_sync_loop`/
  `route_sync_loop`, same `PEER_SYNC_INTERVAL` cadence) to merge
  `Metadata.node_addrs[*].control` into its own control env via
  `ControlHandle::merge_control_peer` — so *any* node that might later become
  leader already knew every runtime-added voter's address, not just the one
  that added it. `admin_remove_control_member` pruned the field back to
  `None` on removal. Regression at the time:
  `tests/control_membership_admin.rs::
  runtime_added_voter_survives_leadership_change_to_a_different_original_voter`
  (self-removes the adder to force a transfer to a *different* original
  voter, then proves a fresh proposal still replicates to the runtime-added
  voter — this regression test itself is unaffected by the update below,
  since it exercises the *outcome*, not this mechanism's internals). See
  `docs/engineering-lessons.md` for the full war story (including a real
  self-registration/admin-action clobber race the regression test's
  bring-up had to sequence around).

  **Update (ADR 0040 PR1, one identity per node): `NodeAddrs.control`,
  `control_peer_sync_loop`, and `ControlHandle::merge_control_peer` are all
  gone outright**, not merely superseded — the two-id split
  (`control`/`raftkv`) this whole mechanism existed to bridge no longer
  exists, so there is no second control-only address left to separately
  replicate/sync at all. A runtime-added control voter's one `internal`
  address is either already known (an existing node self-registered via
  `NodeAddrs.internal` before being promoted) or supplied directly by the
  admin action, and the single unified `peer_sync_loop` (merging
  `Metadata.node_addrs[*].internal`, see the gotcha above) is what every
  node — control, data, or combined — already keeps current. See
  `animus-env/CLAUDE.md`'s `merge_peer` entry for the primitive that
  outlived the mechanism above, and ADR 0040's own amendment stanza on ADR
  0037 for the ADR-level pointer.
  Remove's original quorum-loss warning (down to 1 voter) was the only
  implemented trigger — the plan's second trigger ("every other voter
  believed Down") was originally dropped: pre-ADR-0040,
  `ControlHandle::believes_alive` was keyed to a distinct **raftkv** id space
  the control ids didn't share, so calling it with a control id was always
  `false`, not "unknown" — see the engineering-lessons "id-space mismatch"
  entry. ADR 0040 PR1 has since dissolved that mismatch structurally (one id
  per node), but the dedicated signal below remains the more precise one.

  **Update (ADR 0037 hardening PR2, PR #136, the quorum-guard liveness fix): a real
  survivor-liveness trigger now exists**, via a genuinely control-id-native
  signal instead of bridging `believes_alive`'s raftkv-keyed one:
  `RaftCore::peer_last_contact` (`animus-control/src/raft.rs`) tracks, per
  peer, the `now` of the leader's last `AppendEntriesResp` — success or
  reject, either proves reachability — in a volatile `last_contact:
  BTreeMap<NodeId, Nanos>` seeded at `become_leader` and never
  persisted/snapshotted (same lifetime discipline as `next_index`/
  `match_index`). `RaftNode::control_peer_believed_alive` (`node.rs`, its
  own `CONTROL_PEER_LIVENESS_TIMEOUT = 500ms`, deliberately not a reuse of
  `DETECT_TIMEOUT`) turns that into a bool: always `true` for self, `true`
  for a peer never yet contacted this leadership stint (grace for a
  just-added voter), else gated on the timeout.
  `admin_remove_control_member` now computes `live` = how many of the
  *resulting* voters (after this removal) pass that check, and refuses if
  `live` is below a majority of `remaining.len()` — naming the
  apparently-dead voter(s) and pointing at a new `force: bool` parameter
  (`POST /admin/control/member/remove {node, force}`, `#[serde(default)]`;
  CLI: `animus admin control-remove <leader> <id> [--force]`). **`force` is
  deliberately independent of `decommission --force-control-remove`** — the
  latter only means "run `control-remove` as part of decommission," never
  "and skip `control-remove`'s own safety checks"; `run_decommission`'s
  internal call always passes `force: false`. Removing the node that is
  itself the dead one needs no `--force` (it's excluded from `remaining` by
  construction — the guard only ever counts *other* survivors). Regression:
  `tests/control_membership_admin.rs`'s
  `removing_a_live_voter_while_another_is_already_dead_is_refused_without_
  force`/`..._succeeds_with_force`/`removing_the_actually_dead_voter_itself_
  needs_no_force`/`removing_a_voter_when_every_remaining_voter_is_alive_is_
  never_refused`, plus a `SimEnv` proof at the `RaftNode` level in
  `animus-control/tests/control_membership.rs::
  last_contact_ages_out_a_partitioned_peer_but_not_a_healthy_one`. The
  core-level `RaftCore::change_membership` still has no survivor-liveness
  guard by design (unchanged, still a pure single-server-delta mechanism) —
  the guard lives one layer up, in this crate's admin action, the only layer
  with a `RaftNode` handle to ask.
  Removing the current leader's own slot arms a `transfer_leadership` and
  returns the same not-leader refusal every other case here uses (never a
  silent success) rather than trying to complete the removal itself once it
  has stepped down.

  **Update (ADR 0037 hardening trio's PR3, re-based onto ADR 0040 Decision
  B/C in PR4): `control-add`'s omitted-`node` form now self-mints instead of
  drawing from an allocator**, closing the original "Coordination with ADR
  0036" deferral for good (the allocator it once wired in is deleted).
  `AddControlMemberReq.node` (`admin.rs`) is `Option<NodeId>`
  (`#[serde(default)]`); `admin_add_control_member`'s signature is `(node:
  Option<NodeId>, addr, labels) -> Result<NodeId, String>` — `Some(id)` is
  re-validated via `NodeId::propose` (the old "at/above `ALLOC_ID_BASE`"
  refusal is **deleted**, no ranges exist anymore); `None` mints via
  `NodeId::mint(leader.env())` (**not** `animus_env::prod::PreBindRng`'s
  pre-bind exception — this method runs in-process on a live leader a
  `SimEnv` test can and does drive, so the `Env`-seam rule applies here with
  no exception to invoke), re-minting up to `MAX_MINT_ATTEMPTS` times on a
  (practically unreachable) collision. Either way, if `node` isn't already a
  live voter: an **existing member** (already self-registered, e.g. a
  combined node being promoted) just gets its `internal` address updated
  (`RegisterNodeAddrs`, merged into whatever address book it already
  published — never blindly replaced with an empty one, which would look
  like a collision to a *different* node's earlier self-registration); an
  **unclaimed** id goes through `register_node`'s `RegisterNode` CAS — the
  old "already exists as a cluster member" refusal is **deleted** too
  (promoting an existing data-plane member to a control voter is the common
  case now, not a conflict — ADR 0040 PR1 already unified the id space, so
  there is no separate control-id range left to collide with). The
  response's `"node"` is the effective id either way. CLI: `control-add`
  disambiguates by **arity** (locked decision, no `--auto` flag) —
  `control-add <leader-admin-addr> <new-node-admin-addr>` (2 args,
  self-minted, `run_control_add_allocated`) alongside the unchanged
  `control-add <leader-admin-addr> <node-id> <new-node-admin-addr>` (3 args,
  operator-supplied, `run_control_add`) — see `animus-cli/CLAUDE.md`'s own
  entry for why the 2-arg form's single positional is a raw control-Raft
  address, not an admin address to resolve. Regression:
  `tests/control_membership_admin.rs::
  omitted_node_add_mints_an_id_and_converges_to_a_live_voter` +
  `concurrent_omitted_node_adds_mint_distinct_ids_and_both_become_voters` (two
  concurrent omitted-node adds each mint without colliding — a 128-bit mint
  colliding is astronomically unlikely, and the registration CAS would catch
  it structurally even if it happened — but their `change_membership` calls
  race like any other pair; the loser's own minted id is left as an
  orphaned/abandoned `Down` member, accepted ADR 0040 semantics, while a
  retried omitted-node call mints a second, distinct id that becomes a
  voter) + `add_control_member_collision_shapes` (an id that already names an
  existing data-plane member now *succeeds*, promotion not a conflict).
- **The CP group is durable by default** — one shared `LsmEngine` over the node's
  one internal env (ADR 0040 PR1), cloned into every tablet's `RaftKvNode`; acked
  writes survive restart. Files use a flat filename prefix (`LSM_PREFIX = "db-"`),
  not a subdirectory (`ProdEnv`'s disk doesn't create intermediate dirs).
  Node-start entry points are async+fallible (`io::Result`).
- **`Node::shutdown()` is a graceful teardown** — aborts the listener tasks and
  `ProdEnv::shutdown()`s the node's one internal env, freeing all five ports
  (ADR 0040 PR1's `internal`/`client`/`dynamo`/`cql`/`admin` stride — was six,
  split across two role envs, before) so a replacement can rebind the same
  addresses/dir. Dropping a `Node` without it leaves tasks running.
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
- **A merged-across-nodes admin view must carry each item's own identity** —
  `/admin/raftkv`'s `CpRaftView::node` carries the real hosting node id because the
  dashboard merges every node's response; the answering server isn't a reliable
  attribution once combined.
- **CP writes need no client-assigned version** — the Raft log index *is* the MVCC
  version, so per-key LWW reproduces the agreed order.
- Several gotchas here are instances of cross-cutting lessons — port-TOCTOU
  bring-up retries (`support::restart_same_addrs`), "a flaky `ProdEnv` test is a
  real bug", restart-test discipline (poll for catch-up, not leadership),
  converged-or-timeout polls for eventual properties, retry loops distinguishing
  never-accepted from accepted-unconfirmed. See the **engineering-lessons log
  (root `CLAUDE.md`)** for the general form of each.

## Tests

`cargo test -p animusd` — all tests are real-socket `ProdEnv` integration tests
that poll with timeouts, not deterministic assertions. The restart tests run both
incarnations in the same runtime, calling `Node::shutdown()` between them. Two
in-crate `#[cfg(test)] mod`s (`split_fence_tests`, `auto_split_median_tests`) live
in `lib.rs` because they need private handles. The ADR 0040 PR6 orphan-member
sweep has **no dedicated test file here** — its mechanism is entirely
`animus-control`'s (see that crate's `tests/orphan_sweep.rs`, the seeded
`SimEnv` fault-injection suite), mirroring how the ADR 0012 failure detector
it's patterned on is likewise tested there and not duplicated in this crate
beyond the general `self_heal.rs` smoke test; this crate's own contribution
(the config/CLI knob) is exercised implicitly by every existing test that
starts a node through the now-defaulted `run_node_with`/`run_node_control`/
`start_cluster_*` entry points.

Test-file map (`tests/`):

- `cluster.rs` / `per_process.rs` — in-process cluster / independently-started
  nodes from a shared config.
- `cluster_split.rs` — in-process split deployment (`start_split_cluster_with`):
  `in_process_split_cluster_serves_writes_and_reports_roles`,
  `fixed_control_node_write_read_is_deterministic` (20 keys through one fixed
  control node, no round-robin), `single_shot_first_write_through_control_node_
  succeeds` (the PR #106 election-wait regression).
- `control_only.rs` — bare control-only cluster + schema DDL relay + a mixed
  cluster (ADR 0035 PR3).
- `data_only.rs` — genuine split cluster, 3 control-only + 2 data-only (PR4).
- `data_join.rs` — `animusd data --seed` joining a split cluster (PR5).
- `watch_metadata.rs` — the `WatchMetadata` long-poll wire primitive (ADR 0035
  PR5) and, since ADR 0038 PR5, its incremental `MetadataDelta` reply: a
  fresh cluster's wake-on-commit reply is specifically asserted to be a
  `MetadataDelta` (not a full `Status`), and a real control-only process
  restart resets that node's ring, forcing a pre-restart watcher's
  `last_seen` to correctly fall back to a full `Status` while a caught-up
  watcher still gets the cheap trivial delta.
- `split_cluster.rs` — genuine multi-process split deployment scenarios: control
  failover, split+merge, failure repair, decommission, full restart (PR6).
- `cluster_growth.rs` — 3→5 online growth without restarting the original 3.
- `tablet_rf_self_heals.rs` — regression for the placement-policy fix above:
  provisions a table on a genuinely 2-node cluster (so the tablet's *initial*
  replica set is structurally sized 2, no timing race needed), grows to 3
  nodes, and asserts the tablet's replica set grows to 3 — only possible if
  `provision_tablet` recorded the policy's RF as the target
  (`MAX_REPLICATION_FACTOR`) rather than the initial observed replica count.
- `seed_join.rs` — combined-mode seed/join with an explicit `--id` (happy/
  collision/rejoin — the collision case now asserts `AlreadyExists` from
  `claim_join_identity`'s loud proposed-id-collision failure, ADR 0040 PR4).
- `seed_join_allocated.rs` (ADR 0040 PR4, superseding the ADR 0036 name/
  contents) — the self-minted-id counterpart: happy path, two concurrent
  minted joins get distinct ids (both become `Active`), the data-only dual,
  the ephemeral-identity restart semantics (a fresh join after the old
  process goes away mints a *new* id; the old one settles `Down` and is
  prunable via `RemoveMember` — the crash-mid-join orphan shape), and the
  `is_relayable_command` regression for `RegisterNode` through a
  follower-connected seed.
- `decommission.rs` — full drain → remove flow + refusal shapes.
- `control_membership_admin.rs` — control-plane membership-change admin API
  (ADR 0037 PR3): grow a control voter end to end (quiet non-voter →
  `POST /admin/control/member/add` → converges everywhere incl. a data-only
  node's `Remote` mirror); add collision shapes (existing voter is
  idempotent; existing data-plane member now *succeeds*, a promotion, ADR
  0040 PR4); remove's full
  refusal/warning matrix (idempotent unknown-node no-op, non-leader voter
  removes cleanly, leader self-removal arms a transfer and refuses rather
  than silently completing, down-to-1-voter warns, down-to-0 is refused);
  both mutating actions refuse on a follower (not relayable); a runtime-added
  voter survives a leadership change to a different original voter (PR4);
  plus a PR5 addition — two concurrent `control/member/add` calls race
  cleanly (loser gets a retryable `409`, its retry succeeds once the winner
  commits). **The hardening trio's PR2 (quorum-guard liveness fix) replaced
  the original PR5 §9 test that documented "removing a live voter while
  another is already dead succeeds with no warning" as an *accepted* risk**:
  the liveness-aware guard now refuses that removal outright
  (`removing_a_live_voter_while_another_is_already_dead_is_refused_without_
  force`), with a `--force` sibling proving the same operationally-risky
  removal (and its stranding consequence) is still reachable as an explicit
  escape hatch (`..._succeeds_with_force`), plus the dead-voter-removes-
  itself-needs-no-force and every-remaining-voter-alive-is-never-refused
  cases — see `animusd/CLAUDE.md`'s "Control-plane membership change" gotcha
  for the mechanism.
- `control_membership_split.rs` (ADR 0037 PR5) — the `split_cluster.rs`-style
  multi-process scenario: over a genuine split deployment (control-only +
  data-only processes), grow the control quorum by one at runtime, then
  replace an ORIGINAL voter (kill it for good, remove it, add a fresh
  replacement) via the real admin HTTP surface, with continuous data-plane
  writes spanning the whole scenario — proving runtime control-plane
  membership change composes with a real split deployment, not just the
  single-action coverage in `control_membership_admin.rs`.
- `cp_plane.rs` — CP round-trip (write one node, read another) + write latency.
- `cp_cross_process.rs` — cross-process forwarding to the leader's node.
- `cp_reconfigure.rs` — failure detection, group-follows-replica-set, auto-repair.
- `cp_rebalance.rs` / `cp_rebalance_gc.rs` — healthy rebalance + removed-replica GC
  (release, erase-bound, split-then-release).
- `drop_table_gc.rs` — drop-table `Reclaim` (incl. the relay bimodal case).
- `tablet_merge.rs` — end-to-end split → merge → read through the survivor.
- `batch_write.rs` — `cp_batch_write` / `PutBatch` forwarding.
- `durable_restart.rs` — write survives restart on LSM, lost on `--ephemeral`
  (data plane).
- `control_mirror_restart.rs` (ADR 0038) — a control-only node's real process
  restart over its **dedicated** system-keyspace engine: membership
  (original PR2/PR3 test) and, since PR4, the fuller schema-catalog +
  tablet-map shape too, both via the restarted node's own `metadata()` and an
  independently-reopened `LsmEngine` handle.
- `control_metadata_restart.rs` (ADR 0038 PR4) — rounds out the control-plane
  restart matrix: a **combined** node's restart recovers the schema catalog +
  members + tablet map via the exact same physical **shared** CP-data engine
  a hosted tablet's own data lives on; an **`--ephemeral`** control-only
  restart on a *fresh* directory does not inherit the previous incarnation's
  `Metadata` and re-bootstraps cleanly (see the engineering-lessons entry on
  why a same-dir `--ephemeral` restart is a different, not equivalent, claim
  — the control Raft's own WAL is real disk regardless of engine backend).
- `self_heal.rs` — concurrent-load smoke test (no deadlock).
- `dynamo_wire.rs` / `dynamo_extended.rs` / `dynamo_documents.rs` /
  `dynamo_indexes.rs` / `dynamo_schema.rs` — the DynamoDB edge (wire round-trip,
  conditional writes, document paths, GSI/LSI, replicated+restart-surviving
  schema/index).
- `dynamo_txn.rs` (ADR 0018 §2/PR7) — atomic `TransactWriteItems`/
  `TransactGetItems` over a genuine multi-process, pre-split-table cluster:
  cross-tablet atomic visibility through a follower-connected client, a
  failing `ConditionCheck` cancelling the whole transaction (the old
  serial-loop implementation's exact counter-example), `TransactGetItems`
  never observing a torn pair under a concurrent writer, same-node
  concurrent transactions racing a shared conditioned key resolving to one
  winner, and `/admin/txns` showing a pending record during a simulated
  coordinator stall (driven via the internal `TxnPrepare` wire request
  directly, mirroring `cp_txn.rs`) then clearing once recovery decides it.
- `cql_wire.rs` / `cql_clustering.rs` / `cql_durable_schema.rs` — the CQL edge
  (typed round-trip, compound keys, durable replicated schema).
- `admin_endpoint.rs` — admin views + gated actions + data writes + bulk seed.
- `system_table.rs` (plan-syskv-ui, an ADR 0038 addendum) — `GET
  /admin/system-table` end to end: seeds every `EntityKind` via the client
  protocol (a plain `Put` auto-provisions a `Tablet`+bumps its `Counter`,
  `ProposeSchema` reaches `Schema`/`Policy`/`Keyspace`/the legacy
  `CpMemberAddr`, a real split+merge produces a `Merged` marker; `Member`/
  `NodeAddrs` come from the bootstrapped node's own self-registration — ADR
  0040 PR4 retired the ADR 0036 allocator's dedicated `NodeIdAlloc` kind
  along with the allocator itself) and asserts every kind's exact
  value shape, the `kind` filter, and an unrecognized-kind 400; a separate
  test seeds many `tablet` rows and diffs a small-`limit` forward-only
  pager walk against one unlimited scan for gaplessness/no-duplicates.
  `control_only.rs`/`data_only.rs` cover the available-`true`-with-rows /
  available-`false` shapes on genuine control-only and data-only processes;
  `dashboard_endpoint.rs` covers the served asset/markup check.
- `dashboard_endpoint.rs` — SPA serve + CORS + deep links + peers + role gating.
- `metrics_endpoint.rs` — `GET /metrics` (leader-only counters per node).
- `otel_tracing.rs` — OTLP span export (decodes the protobuf payload).
- `schema_ddl_relay.rs` — schema DDL relay through a follower-connected node.
- `frame_cap.rs` — client-protocol frame-size cap.
- `support/mod.rs` — shared bring-up helpers (`#![allow(dead_code)]`;
  `restart_same_addrs`, `bring_up_split`, port-TOCTOU retries).
