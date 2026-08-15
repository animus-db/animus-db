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
(lib.rs:6725). `index_drain.rs` has a third, `gsi_drain_cursor_tests`, and
`dynamo.rs` a fourth, `stream_write_path_tests` (ADR 0042), for the same
reason (see each file's own entry below).

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
  (a node's five addresses + `role: NodeRole` = `Control`/`Data`/`Both` + an
  explicit **`id: NodeId`** field — every config entry names its own
  identity rather than it being re-derived from position;
  `ClusterConfig::from_json` hard-errors on a duplicate `id`), role-filtered
  accessors (`control_ids`/`data_ids`/`peer_book`), `generate`/
  `generate_split`, and the **five-port stride** (`base_port + 5*i +
  {internal,client,dynamo,cql,admin}`). `generate`/`generate_split` mint
  `"n{i}"`, **zero-padded** once the cluster has ≥ 10 nodes so
  lexicographic id order stays == numeric index order (`"n10" < "n2"`
  otherwise) — below that threshold ids stay the plain unpadded `"n{i}"`
  every existing test already assumes.
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
  route (ADR 0015) shares this listener. `dispatch` also forwards a
  `DynamoDBStreams_20120810.*` target to `dynamo_streams::execute`
  (below) — the two services share one listener/port.
- **`dynamo_streams.rs`** (ADR 0042 §3/§5/§6/§7/§9/§10/§11, PR6) — the
  DynamoDB Streams read API: `ListStreams`/`DescribeStream`/
  `GetShardIterator`/`GetRecords`. See the "DynamoDB Streams" entry under
  `dynamo.rs`'s own write-side section above for the full design (label
  resolution, the sealed-vs-open serve split, `StreamHotRead`) — this
  entry is just the module pointer.
- **`segment_janitor.rs`** (ADR 0043 §A9, round-3 PR7) — the **segment
  janitor**: a distinct, control-plane-**leader**-only background loop
  (`segment_janitor_loop`, self-gated every tick on
  `ctx.edge.leader_handle()`, the identical pattern
  `detect_loop`/`orphan_sweep_loop` use one layer down in `animus-control`)
  — retention two-phase reclaim and replica repair over the *whole*
  `stream_shards` catalog, a cluster-wide concern distinct from any one
  tablet's own per-node sealer/hot-trim arm (`index_drain.rs`). Spawned
  unconditionally by both `BoundNode::start_with_streams` (combined) and
  `BoundControlNode::start_control_with` (control-only, hardcoded to
  `DEFAULT_STREAM_RETENTION` — no CLI knob for that node shape yet, the
  same documented follow-up `StreamSealKnobs`/`SegmentStoreConfig`
  already have); **never** spawned on `BoundDataNode` (a data-only node
  never registers a local control `RaftNode`, so `leader_handle()` there
  is permanently `None`).

  **Phase 1, two-phase retention**: mark every unexpired row past
  `--stream-retention` (age from its own `seal_wall_ms`) *or* whose table
  has been dropped entirely (`Metadata::table_schema` no longer names it —
  see "the convergent drop-table cascade" below) via
  `ExpireStreamShards{remove: false}`; then, for every marked row, delete
  the segment object at every recorded replica still present in the
  cluster's own membership (`ClientCtx::data_opt().segment_store
  .delete_sealed`) and, once that succeeds (or nothing reachable was left
  to delete), physically remove the row (`ExpireStreamShards{remove:
  true}`). **The dead-replica rule**: a replica counts as
  confirmed-absent (no delete owed) only once **removed from membership
  entirely** — a merely `Down` member still gets a genuine delete attempt
  every tick (it might come back with its copy intact); a permanently
  dead-but-never-decommissioned member therefore blocks that row's
  physical removal forever (an accepted durability-over-availability
  tradeoff, not a bug — the operational remedy is decommissioning it).

  **The epoch-derivation guard (`may_remove_row` in the phase-1 loop) is
  the one correctness-load-bearing exception to "mark past retention,
  remove once deleted."** `index_drain::seal_now`'s `next_epoch` and
  `dynamo_streams::current_open_epoch` both derive a tablet's epoch from
  its own chain's **highest currently-existing row**, not an independent
  counter (round-3's whole "epoch = the chain length" design, ADR 0042
  §2) — a design that only holds while the catalog never *shrinks*.
  Physically removing a tablet's own current highest-epoch row while that
  tablet still exists (`meta.tablets.contains_key(tablet)`) would let a
  future seal silently recompute the *identical* epoch number for
  genuinely new data. So: only the **object** may ever be deleted for such
  a row (safe unconditionally — epoch derivation never reads object
  bytes); the **row** stays marked `expired` (already invisible to
  `DescribeStream`'s own enumeration) until either the tablet seals past
  it (no longer the max) or the tablet is dropped (`!meta.tablets
  .contains_key(tablet)` — the drop-table cascade's own exception, since
  nothing will ever derive an epoch for a gone tablet again). See
  `docs/engineering-lessons.md` for the story of how testing surfaced
  this — a corpus/integration gap the round-2/round-3 ADR text didn't
  anticipate, since PR6 never physically removed a row at all.

  **Phase 2, replica repair** (F5's own durability mandate): for every
  live row with a non-empty `replicas` (a `ClusterSegmentStore`-backed
  row — the `FsSegmentStore` opt-in always records an empty list and has
  no per-replica concept to repair), verify each recorded replica is a
  current `Active` member; for however many are not, `get_sealed` a live
  copy from whichever recorded replicas *are* `Active`, then
  `repair_replicas` (`animus_cp_data::cluster_segment_store::
  ClusterSegmentStore::repair`) it onto enough fresh targets to restore
  the row's own original replica count, and — if the resulting set
  differs — commit it via a **content-preserving** `SealStreamShard`
  re-proposal (round-3 PR7's amendment to that command's apply arm,
  `animus-control`'s own doc): same `table`/`label`/`view_type`/
  `hlc_range`/`count`/`seal_wall_ms`, only `replicas` updated. Never
  touches an expired row.

  **The convergent drop-table cascade**: `ExpireStreamShards` is
  deliberately **not relayable** (`is_relayable_command`'s own doc, below)
  — its only sanctioned caller is this control-plane-leader-only loop,
  which already holds a live `RaftNode` handle; `ClientCtx::drop_table`
  runs on whichever node a client happens to connect to, essentially never
  guaranteed to be that leader. Rather than adding a leader-only special
  case to `drop_table` itself (duplicating this loop's own two-phase
  decision the rare time it *is* the leader), phase 1's own retention rule
  already treats a row whose table has no schema at all as **immediately
  due** — "retention `0`" for a table that no longer exists to protect.
  `drop_table`'s existing cascade (unchanged: drop the schema, then the
  tablets) is exactly what flips that condition; **no new code in
  `drop_table` at all**. `ClientCtx::data_opt() -> Option<&DataRole>`
  (alongside the pre-existing panicking `data()`) is what lets phase 1's
  own marking/drop-rule run correctly even on a control-only leader with
  no `SegmentStoreHandle` at all — see the module's own doc for the
  documented control-only-leader scope gap this leaves (phases 2/3 skip
  there; a **pure** split deployment never runs them).

  **Metrics** (ADR 0015): `stream_segments_live`/`stream_repair_backlog`
  are levels (`MetricsHandle::set`, recomputed fresh every tick from the
  snapshot); `stream_segments_expired_total`/`stream_repairs_total` are
  genuine counters (`incr`/`incr_by`) — the former on a confirmed row
  removal, not the mark; the latter once per row whose replica-set update
  actually committed, not per replica copied.
- **`index_drain.rs`** (ADR 0041 §4 GSI drain; ADR 0042/0043 the seal arm +
  hot-trim rework, round-3 sealer PR) — the per-node **change-consumer
  loop** (`change_consumer_loop`, renamed from `index_drain_loop` since it
  is no longer GSI-specific; spawned alongside
  `tablet_host_reconciler_loop`/`auto_split_loop`), three arms per tick per
  led tablet:
  1. **GSI drain** (unchanged from ADR 0041): sweeps change records past the
     "gsi" cursor's own watermark (`drain_tablet`), reconciles each dirty
     partition's GSI rows into the index's own hidden table
     (`reconcile_partition`, derivative not delta-based — see its module
     doc), then advances the "gsi" `KIND_CURSOR` row
     (`animus_cp_data::cursor`) to the sweep's own max HLC **only after
     every dirtied partition's footprint update is durably confirmed**.
  2. **The seal arm** (`seal_tick`/`seal_now`, streamed tables only): on a
     size trigger (`CpGroup::approx_bytes_kind(KIND_CHANGE)` —
     deliberately **not** `approx_bytes`, which is base-kind-only, ADR
     0034; see the engineering-lessons entry on the bug this distinction
     fixed) or age trigger (the oldest unsealed record's wall-ms, this
     loop's own `env` clock), `seal_now` scans `pending_changes()` past the
     tablet's effective watermark (`Metadata::
     effective_stream_shard_watermark`, walking split-parent provenance),
     sorts by the packed-HLC key suffix (load-bearing — key order is
     token-then-pk-then-HLC, not commit order), builds a segment
     (`animus_cp_data::segment`), pushes it to this node's
     `SegmentStoreHandle` (`ClientCtx.data().segment_store`), then proposes
     and confirms `MetaCommand::SealStreamShard`. Never seals an empty
     pending set. `seal_now` is the **one** seal code path — also called,
     unconditionally (trigger-independent), by the internal-only
     `ClientRequest::ForceSeal` RPC (`lib.rs`, refused bare, handled only
     inside `cp_serve_forwarded`) that `ClientCtx::force_seal_tablet`
     drives — `dynamo.rs`'s `disable_stream` calls it for every tablet of a
     table before ever proposing `SetTableStream{None}` (F12-b's
     disable-triggered final seal).
  3. **The hot-trim arm** (`trim_janitor`, generalized from ADR 0041's
     original trim janitor): deletes change records every *expected,
     present* term has cleared — the "gsi" cursor term (unchanged) and,
     **iff the table's current schema has an enabled stream**, the
     catalog-derived stream watermark (`Metadata::
     effective_stream_shard_watermark`) — never a `"copier"` cursor row
     (round 2's tag/row scheme; deleted, along with `COPIER_TAG`/
     `expected_consumer_tags`, in round 3). **The F10/F12-b rule, reviewed
     hard**: an expected term with nothing to derive it from yet (no "gsi"
     row; a stream that has never sealed) blocks trim entirely (the safe
     default); the stream term applies *only* while the schema's stream is
     currently enabled — a disabled stream's un-reaped catalog rows do
     **not** re-add it, because by the time disable commits, the final
     seal has already moved every one of that label's records into a
     committed segment, so there is nothing left for a stream term to
     protect; and **zero expected terms at all means trim everything, not
     block everything** — reachable only for a table whose stream was
     disabled and has no GSI, which the loop's own outer gate
     (`gsis.is_empty() && !stream_enabled && !ever_streamed`) keeps
     visiting specifically so this arm gets a guaranteed chance to run,
     rather than depending on winning a race against `disable_stream`'s
     own commit landing first (a real bug this PR's own tests found and
     fixed — see the engineering-lessons entry). And tombstones stale
     **merge-residue** cursor rows (an absorbed sibling's own row,
     physically surviving in the widened scope, for a tag no longer
     expected) — deliberately *not* an unexpected row at this tablet's own
     token (a dropped index's stale row is separate, out of scope here;
     round 3 has no cursor tag left for a stream to ever leave one of its
     own). `ClientCtx::cp_kind_write_raw`'s confirmation probe is generic
     (any single write in the atomic batch) for exactly this reason.

  Two `#[cfg(test)] mod`s at the bottom of this file (mirroring `lib.rs`'s
  `split_fence_tests`): `gsi_drain_cursor_tests` (unchanged, ADR 0041/0042
  §7/§8's own crash/split/merge/trim regressions) and `stream_sealer_tests`
  (round-3 sealer PR) — the seal arm's triggers/sequence, the F10/F12-b
  hot-trim rework, and F11's split-key token alignment, needing `CpGroup`'s
  private `pending_changes`/`approx_bytes_kind`/`cursor_min_watermark`
  accessors and, for durability introspection, a **second** `FsSegmentStore`
  handle pointed at the same deterministic `<node dir>/segments` path
  `build_segment_store` roots the default cluster store's local building
  block at (there is no production read-path accessor yet — that's PR6).
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
| `join --seed ADDR[,ADDR...] [--id NAME] --base-port P [--ip A] [--dir D] [--ephemeral]` | combined-mode seed/join startup; `--id` proposes a durable identity, omitted self-mints one |
| `control --config FILE --node I [--dir DIR] [--ephemeral]` | run node I as a control-only node; `--ephemeral` (ADR 0038) selects a volatile in-memory system-keyspace engine instead of the durable on-disk default — `Metadata` does NOT survive a restart |
| `data --config FILE --node I [--dir DIR] [--ephemeral]` | run node I as a data-only node |
| `data --seed ADDR[,ADDR...] [--id NAME] --base-port P [--ip A] [--dir D] [--ephemeral]` | data-only seed/join; `--id` proposes a durable identity, omitted self-mints one |

`--auto-split K` (key count) and `--auto-split-bytes B` (byte size) are
independent OR-gated triggers — either, both, or neither. **`--node I` is
gone from `join`/`data --seed` entirely** — there is no index to derive a
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

## Deployment shapes (ADR 0035)

Three shapes, all built from the same role assemblies:

- **Combined** — every node runs both roles. `--cluster N` (one process) or
  `--config FILE --node I` (one process per node), against a `Both`-role config.
  `Node::bind` → `BoundNode::start_with`.
- **Control-only** — a small static metadata quorum, no CP **data** storage
  engine, no data role. `animusd control --config FILE --node I`.
  `Node::bind_control` → `BoundControlNode::start_control_with(.., backend)` —
  **fallible** (`io::Result<Node>`) and takes a `StorageBackend` (ADR 0038):
  it **unconditionally** provisions one small **dedicated** system-keyspace
  engine (`StorageBackend::Lsm` by default, `::Memory` under `--ephemeral`)
  — see `animus-control/CLAUDE.md`'s `node.rs`/`mirror.rs` entries. `Metadata`
  is `StateMachine::DRIVER_APPLIED`, so this engine is the durable home of
  the control plane's async apply task's published cache — there is no
  engine-less control-plane deployment shape.
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

**`cp_write`/`cp_delete` do NOT auto-provision a table's first tablet** —
unlike most of their write-side siblings (`cp_put`, `cp_kind_write`,
`cp_batch_write`, `cp_batch_write_patient`, `cp_txn` all do). A caller
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

## Multi-participant transactions (ADR 0018 §2)

`ClientCtx::cp_txn(writes, preconditions, write_conditions) ->
Result<HlcTimestamp, String>` is the coordinator for a cross-tablet
(possibly cross-table) atomic transaction, reachable via
`ClientRequest::Txn`. It groups `writes` `(table, key, Option<value>)` by
owning tablet; the **first** write's tablet is the **anchor** (mints the
`TxnId`/record key, via `RaftKvNode::txn_stage_anchor` — passed every
*other* participant's `(table, span)` list up front, so the record's own
`intent_spans` names every participant, not just the anchor's own writes),
every other tablet is a **participant** (`txn_stage_participant`).
`write_conditions` is `(table, key, expected)` own-key byte-level OCC —
`key` MUST be one of `writes`' own keys — threaded to the owning tablet's
`TxnStage.conditions`, checked at apply, not re-read; this is a
**structurally distinct** mechanism from `preconditions`, not an overload
of it (conflating the two caused a self-referential-stall bug — see ADR
0018's own follow-up amendment).

Prepare runs the anchor first, then every participant **concurrently**
(`futures::future::join_all`), through `ClientCtx::txn_prepare_pushing`:
a stage call returning `Ok(..)` only means its entry *applied*, never that
it genuinely wrote an intent, so this branches on `txn_prepare`'s returned
`animus_cp_data::StageOutcome` — `Staged` succeeds; `IntentBlocked` retries
(bounded, `TXN_STAGE_PUSH_ATTEMPTS`/`_BACKOFF`); `ConditionFailed`/`Fenced`
fail immediately. Any prepare failure, or a failed pre-commit precondition
re-check, proposes an abort on the anchor. On success, `commit_ts` is the
anchor's own `txn_commit_at_least` result, floored at the max of every
participant's acked stage ts — the **single Raft commit on the anchor's
record is the atomic commit point**. Every decide attempt (`txn_decide_
anchor`) re-reads `txn_status_local` and reports the record's **actual**
outcome, never what was asked for — recovery makes duelling deciders legal
(a still-live coordinator's commit can lose to a concurrent recovery
abort, or vice versa; the anchor's Raft log position is the sole arbiter).
Resolve is asynchronous and post-ack on the commit path: once the anchor's
commit is durable, `cp_txn` returns immediately and spawns a best-effort
resolve of every participant in the background, safe because
`txn_resolver_loop` (below) is the safety net for whatever that spawn
doesn't get to; the abort path resolves synchronously before returning.

**Internal-only `ClientRequest` variants — `TxnPrepare`/`TxnDecide`/
`TxnResolve`/`TxnStatus`/`TxnRecordView`/`TxnVerify` — are never sent
bare**, only wrapped in `Forwarded` (the top-level `handle_request`
dispatcher rejects a bare one with an error); their real handling lives in
`cp_serve_forwarded`'s match only. **Routed by the actual data key** being
staged/resolved/verified (`table` + `writes[0]`/`keys[0]`/`span.start`),
**never `record_key`** for `TxnPrepare`/`TxnResolve` — a non-anchor
participant's `record_key` names the anchor's record, which lives in a
*different* tablet's (possibly a different table's) keyspace entirely.
`TxnDecide`/`TxnStatus`/`TxnRecordView` always target the anchor's own
tablet, so routing by `record_key` there is correct. These are data-plane
RPCs, not `MetaCommand`s — `is_relayable_command` does not apply to them.

**Foreign-intent read resolution** (`ClientCtx::cp_get_local_resolving`,
used by `cp_read`'s `Local` arm and `cp_serve_forwarded`'s `Get` arm — the
original `cp_get_local` stays test-only, used by the in-crate
`split_fence_tests`): tries `RaftKvNode::linearizable_get_served_fast`
first; on `FastRead::Foreign`, routes a `TxnStatus` query to the intent's
actual record owner and finishes via `RaftKvNode::resolve_intent_given_
status` once decided. A still-`Pending`/failed status query calls
`ClientCtx::txn_recover` before giving up rather than immediately
reporting "retry."

**In-doubt recovery**: `ClientCtx::txn_recover(record_table, record_key,
txn_id, intent_ts_hint) -> Result<TxnDecisionStatus, String>` is the
"push" — any actor holding a foreign-or-local `Pending` intent past
`animus_cp_data::RECOVERY_GRACE` (5s, liveness-only) may call it. Reads
the record (`txn_record_view`); already decided → resolve and return;
`Pending` and not stale → decline; `Pending` and stale → verify every
`(table, span)` in `intent_spans` (`txn_verify`) — all staged → propose
`TxnCommit`, any missing → propose `TxnAbort`; re-read the actual outcome
and resolve every participant. **No record at all** is a real,
acknowledged possibility (the anchor's own `TxnStage` can silently no-op
on a fence/seal miss, just like a participant's); `intent_ts_hint` (the
orphaned intent's applied timestamp) is the grace-clock substitute for
that case, and past grace `txn_recover` proposes an **orphan-abort
tombstone** (`RaftKvNode::txn_abort_orphan`) — always an abort, never a
commit. A late-arriving genuine anchor `TxnStage` for that `txn_id` then
finds the tombstone and no-ops instead of resurrecting it to `Pending`.
See ADR 0018 §2 (and its follow-up amendments) for the full protocol and
safety argument.

`txn_resolver_loop` (`lib.rs`, data-role-gated, spawned alongside the
tablet-host reconciler and `auto_split_loop`, `TXN_RESOLVER_INTERVAL` =
1s): for each tablet group this node currently **leads**, pushes every
`RaftKvNode::pending_txns()` entry via `txn_recover` and fans a resolve
out for every `unresolved_decided()` entry — the proactive half of
recovery. Metrics: `CpTxnRecoveredCommitted`/`CpTxnRecoveredAborted`/
`CpTxnResolverRuns`.

**A wire-reachable panic found (and fixed) while testing this**:
`RaftKvNode::txn_stage`'s anchor-key-length assert (ADR 0022, `TOKEN_BYTES`)
was a sound "caller invariant" before `ClientRequest::Txn` existed — no
untrusted caller could reach it with an arbitrary key. `cp_txn` now
validates every write's key length up front and returns a client-facing
error instead of ever reaching that assert. See `docs/engineering-
lessons.md` for the general lesson.

Tests: `tests/cp_txn.rs` (real 3-process cluster + a genuine pre-split
table) — multi-tablet atomicity, the follower-connected forwarding
regression, concurrent transactions each individually atomic, a violated
precondition aborting the whole transaction, and a coordinator crash
between prepare and decide, converging via grace + resolver margin. The
2PC mechanics themselves, and the recovery/decision-semantics design, are
proven deterministically at the primitive level in `animus-cp-data`'s
`tests/txn_multi.rs`/`tests/txn_recovery.rs`. `tests/
txn_recovery_participant_spans.rs` regresses the participant-spans wiring
specifically: stages the anchor with the participant's span declared but
never stages the participant, confirming recovery decides `Aborted` and
the anchor's own key never becomes visible.

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

**The per-node tablet-host reconciler (ADR 0031) is the single owner of this
node's tablet lifecycle** — it replaced three separate loops and their
state. The pure `plan` + `Reconciler` executor live in
`animus_cp_data::host` (read that crate's `CLAUDE.md`); `plan` decides
every action from one `MetadataView` snapshot per tick and executes them in
fixed order (`NarrowScope` → `Host` → `Reconfigure` → `Release`/`Reclaim`;
merge adds `WidenScope`/`Absorb`). What stays in `animusd`
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

**`ClientCtx::drop_table` cascades to every GSI's hidden table (ADR 0041).**
A GSI's rows live in a *separate* table (`animus_dynamo::index_table_name`)
with its own tablets, so dropping only the base table's schema + tablets
would orphan it forever. The three steps run in a load-bearing order: (1)
read `metadata_fresh` and drop each **global** index's hidden table's
tablets via the same `MetaCommand::DropTableTablets` the base table itself
uses; (2) drop the base schema; (3) drop the base table's own tablets (base
+ colocated **LSI** rows + change log + footprints — all four
`StorageScope` kinds share one tablet group, so `CpGroup::erase_scope`
iterating `kind_scopes` reclaims every one; an LSI needs no separate
cascade step). A crash between any two steps leaves a state a re-run of
`drop_table` completes, since every step is independently idempotent.
**Belt-and-suspenders second sweep**: the GSI drain (`index_drain.rs`)
provisions a hidden table's first tablet lazily and can race a drop, so
after step 3 `drop_table` re-scans the tablet map itself (not the now-gone
`IndexDef`s) for any tablet named `<table>$<index>` and drops those too —
which also mops up any orphan a pre-fix drop left behind. Regression:
`tests/drop_table_index_cascade.rs`.

## Wire edges

All edges are production-only I/O (real tokio sockets, hand-rolled framing) and
route below the edge through the same `ClientCtx` CP primitives.

- **DynamoDB** (`dynamo.rs`, `RoleAddrs.dynamo`) — decodes `X-Amz-Target` +
  AttributeValue-JSON via `animus_dynamo::wire`. `CreateTable` proposes its
  key schema **and** GSI/LSI *definitions* into the replicated catalog (ADR
  0013) and waits for commit; a node reconciles its local registry from
  `Metadata::table_indexes` — the registry holds only *definition*
  bookkeeping, never index entries (there is no in-memory index at all). An
  indexed table's `PutItem`/`DeleteItem`/`UpdateItem` goes through
  `index_aware_write` → `ClientCtx::cp_kind_write`, committing the base row,
  its **LSI rows** and a **change-log record** as one `KvCommand::KindBatch`
  Raft entry. An LSI can ride that entry because it hashes by the base
  partition key; a **GSI cannot** (its rows live in their own hidden
  table's tablets) and is materialized asynchronously by the drain
  (`index_drain.rs`) from those change records. `BatchWriteItem` keeps the
  fast `cp_batch_write` path for an **unindexed** table but routes each
  `Put`/`Delete` through `index_aware_write` individually for an
  **indexed** one, atomic per-item only. **`TransactWriteItems` is the one
  op that can't participate**: a write action against an indexed table
  makes `run_transact` reject the **whole transaction** with a
  `ValidationException` — `cp_txn`'s `KvCommand::TxnStage` has no
  multi-kind-write extension yet, so staging just the base row would
  silently never produce the LSI rows/change-log record. The real fix (a
  `cp_txn` analogue of `cp_kind_write`) is a named `animus-cp-data`
  protocol follow-up in ADR 0041, not yet built. **ADR 0042 extends this same rejection to a streamed table**
  (`run_transact`'s per-action loop now checks `meta.table_stream`
  alongside `meta.table_indexes`), for the identical reason: `TxnStage`
  would silently never produce a streamed table's change-log record.

  **DynamoDB Streams (ADR 0042 §1/§2/§4/§9).** `TableSchema.stream:
  Option<StreamSpec>` (replicated, ADR 0013) rides through the identical
  `CreateTable`/`UpdateTable` surface as the key schema/indexes:
  `CreateTable`'s `StreamSpecification` (`StreamEnabled`/`StreamViewType`)
  is decoded into `Operation::CreateTable.stream_view_type`
  (`animus-dynamo`'s `wire` module, pure — no label minting there); when
  `Some`, `create_table` (this crate) mints a fresh label
  (`mint_stream_label`, below) and proposes `MetaCommand::SetTableStream`
  the same commit-wait shape the index-definition loop already uses
  (`enable_stream`, shared with `UpdateTable`'s enable path). **`UpdateTable`
  is new and stream-spec-only**: `wire::decode_update_table` rejects any
  `GlobalSecondaryIndexUpdates` up front (ADR 0041 §5's own deferred item)
  and requires a `StreamSpecification` — `StreamEnabled: true` decodes to
  `StreamUpdate::Enable(view_type)` (rejected by `update_table` if a stream
  is already enabled — the caller must disable first, matching ADR 0042
  §9's "no same-command relabel" contract), `false` to `StreamUpdate::
  Disable`. **`DescribeTable` is also new**: a pure read
  (`describe_table`) of the replicated catalog — key schema (+
  `AttributeDefinitions`, recovered from the catalog's typed `ColumnDef`s
  via `animus_dynamo::schema::key_attribute_types`, the reverse of
  `CreateTable`'s own `key_types` decode), index definitions, and
  `StreamSpecification`/`LatestStreamArn`/`LatestStreamLabel` when a stream
  is enabled (`wire::describe_table_response`, sharing `create_table_response`'s
  `TableDescription`-object builder). The synthetic ARN
  (`wire::stream_arn`) is `arn:aws:dynamodb:animus:0:table/<table>/
  stream/<label>` — fixed placeholder region/account, matching this
  adapter's existing ARN conventions. Round 3 needs no shard provisioning at
  all: the hot shard is just the table's own existing `KIND_CHANGE` change
  log (round-3 streams plan §A1), not a separate hidden per-stream table.
  **The sealer landed in the round-3 sealer PR** (see `index_drain.rs`'s
  entry above): `update_table`'s disable path now performs the F12-b
  final seal (`dynamo.rs::disable_stream`, forcing every tablet's own hot
  tail into a committed segment via `ClientCtx::force_seal_tablet` before
  ever proposing `SetTableStream{None}`).

  **The read path landed in PR6(`dynamo_streams.rs`, new module):** the
  four `DynamoDBStreams_20120810.*` operations, dispatched on the **same**
  listener as the item API (`dynamo.rs::dispatch` checks the target's
  prefix and routes to `dynamo_streams::execute` — the decided
  same-listener fork; every JSON shape and the iterator-token/shard-id
  codecs are pure, in `animus_dynamo::streams_wire`, this module is the
  read path's only impure layer).
  - **`ListStreams`/`DescribeStream`** are pure functions of `Metadata`
    (F7 — the store is never load-bearing for a metadata read):
    `ListStreams` enumerates the current enabled label per table plus
    every `DISABLED`-but-unreaped label with a catalog row still present
    (F12-b); `DescribeStream` builds the shard chain from
    `stream_shard_rows_for_label` (closed, `EndingSequenceNumber` set)
    plus, only while `enabled`, one open shard per `tablets_for_table`
    entry at `current_open_epoch` (this tablet's own chain length —
    mirrors `index_drain::seal_now`'s identical computation). `resolve_label`
    is the one function every operation funnels through for F12-b's
    label validity: the table's *current* schema label, or any label
    with at least one still-present catalog row — neither ⇒
    `ResourceNotFoundException`. `StreamShardRow`/`SealStreamShard` grew a
    `view_type` field (a small `animus-control` catalog amendment,
    `#[serde(default)]`) — a `DISABLED` stream's grace-window
    `DescribeStream` has no live `StreamSpec` to read a view type from
    once `SetTableStream{None}` commits, so a shard's own row carries the
    view type declared *at seal time* instead (`Metadata::
    stream_view_type`, the read accessor); a view type never changes
    mid-stream, so every row of one label agrees.
  - **`GetShardIterator`** mints a stateless `base64url({label, shard_id,
    position})` token (`animus_dynamo::streams_wire::encode_iterator`) —
    `position` is always the record HLC's own **exclusive** lower bound
    the next read filters on (`packed_hlc > position`), the same
    convention `segment::slice_to_hlc_range`'s `start_exclusive` and
    `index_drain::hot_read`'s `from_position` already use, so a token
    composes with either serve tier with no translation. `TRIM_HORIZON`/
    `AT`/`AFTER_SEQUENCE_NUMBER` read straight off the catalog row (sealed)
    or `effective_stream_shard_watermark` (open) with no round trip;
    `LATEST` on a sealed shard collapses to `hlc_range.1` (the
    immediate-null path); `LATEST` on a genuinely open shard needs one
    hot read (`ClientCtx::read_stream_hot_records(tablet, watermark,
    usize::MAX)`) to find the current max.
  - **`GetRecords`** resolves the shard id against the catalog **fresh at
    every call** (never cached from mint time) — this is what makes an
    open-shard iterator survive a seal that happens between polls (ADR
    0042 §2): a catalog row present ⇒ the **sealed** path (any node —
    `SegmentStoreHandle::get_sealed(&row.replicas, seg_id)`, then
    `segment::decode_and_slice(bytes, row.hlc_range)`, the superset-slice
    rule, ADR 0042 §10 — filtered/paginated, nulling `NextShardIterator`
    only once the sliced content is truly exhausted); absent ⇒ the
    **open** path (`ClientCtx::read_stream_hot_records`, forwarded to the
    tablet's own leader, no `ReadIndex` barrier, F8 — never nulls; an
    empty poll returns the *same* iterator, F4/§7), gated on the shard
    genuinely being the label's current live open epoch (else
    `TrimmedDataAccessException`). `ChangeRecord::event_name()` +
    `streams_wire::project_view`/`keys_from_images`/`stream_record_json`
    build each `Records[]` entry; `Keys` is recovered from whichever
    image is present (new preferred, old for a `REMOVE`) since both
    images always carry the full item.
  - **`ClientRequest::StreamHotRead { tablet, from_position, limit }`**
    (new internal-only RPC, mirroring `ForceSeal`'s exact shape/doc
    pattern) is the open-shard forwarding payload — refused bare (gating
    sites: the `request_kind`/bare-refusal arms in `handle_request`, and
    the real handling arm in `cp_serve_forwarded`, which calls
    `index_drain::hot_read` — grepped per the house lesson on adding a
    forwarded-command variant), answered with the existing
    `ClientResponse::Pairs` shape (no new response variant — the packed
    HLC rides each key's own trailing 8 bytes, the same suffix
    `change_record_key` already appends, recovered by the caller).
    `index_drain::hot_read` is `seal_now`'s read-only sibling: an
    identical `pending_changes()` scan/HLC-suffix-sort, filtered by
    `from_position` instead of the watermark, never sealing anything.
  - **`SegmentStoreHandle::get_sealed`** (new, alongside the existing
    `put_sealed`) is the sealed-tier read: `ClusterSegmentStore::get_from`
    for the default `Cluster` variant (any recorded replica), or a plain
    local `get` for the single-directory `Fs` opt-in (replicas ignored —
    there is no per-node replica concept when every node already shares
    the identical directory).

  `mint_stream_label` (ADR 0042 §4) is the proposer-side label mint: an
  ISO8601-shaped string derived from **this node's own `env.now()`**
  (`ClientCtx.env: ProdEnv`, a new field every `spawn_common_tail` caller
  now threads in — the *only* `Env`-seam access point `ClientCtx` exposes
  to the wire edges) suffixed with this node's own id (so two different
  nodes minting at a coincidentally identical elapsed time can never
  collide) — never the wall clock directly (ADR 0003's determinism-rule
  convention, even though this crate is production-only `ProdEnv` wiring).
  **Not a genuine calendar timestamp**: `ProdEnv::now()` is monotonic since
  **process start**, not wall-clock epoch, so the rendered date drifts from
  real time the longer a process has been up — an accepted cosmetic gap
  (a stream's identity is `(table, label)`, validated byte-for-byte, never
  parsed as a date), documented on the function itself. `iso8601_ish`/
  `civil_from_days` (Howard Hinnant's public-domain algorithm) are a small,
  dependency-free Gregorian calendar conversion — this crate takes no
  date/time crate dependency for one cosmetic label format.

  **The write-path gate (`kind_writes_for_item`'s `None` fast path) becomes
  `!table_takes_kind_write_path(meta, table)`** — a new shared predicate
  (`!indexes.is_empty() || stream.is_some()`) both this function and every
  write handler's `needs_old` computation call, kept as one function so the
  two can never silently drift apart. **A streamed-but-unindexed table now
  takes the `KindBatch` path too**: `indexes` is empty, so the LSI loop is
  simply a no-op, and the entry commits exactly base row + change record —
  this same change record *is* the round-3 hot shard the eventual sealer
  reads directly (round-3 streams plan §A1), no separate copier involved.
  **A real, independent correctness gap this surfaced**: `PutItem`/
  `DeleteItem` only fetched the prior item (`needs_old`) when a
  `ConditionExpression` or `ALL_OLD` was requested — an unconditional
  replace/delete on an indexed *or* streamed table therefore silently
  skipped the read `kind_writes_for_item`'s LSI diff (and now a stream's
  `OLD_IMAGE`/`NEW_AND_OLD_IMAGES` fidelity) actually needs. Both handlers'
  `needs_old` now also checks `table_takes_kind_write_path` (`UpdateItem`/
  the indexed `BatchWriteItem` branch already read unconditionally — only
  these two needed the fix). See `docs/engineering-lessons.md` for the
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
  the base table's key). An LSI `Query` is a **linearizable** scan of the
  *base table's own tablet* over its `KIND_LSI` scope (scoped to one base
  partition/tablet); an LSI `Scan` is table-wide, via `ClientCtx::
  cp_scan_kind_table` (`cp_scan`'s kind-scoped sibling, fanning a
  `KindScan` per overlapping tablet — its tail tablet needs a genuinely
  unbounded-above scan, since no finite byte string can bound an LSI row's
  keyspace, so the primitive derives the bound from the kind scope's own
  physical prefix). `ClientRequest::KindScan` is the LSI path's forwarding
  payload — **internal-only, refused bare**, the read-side dual of
  `KindWrite`. A hidden table with no tablet yet reads as **empty**, the
  same gate `ClientCtx::cp_get` uses. A **GSI** query/scan is eventually
  consistent (DynamoDB's own contract — the drain materializes
  asynchronously); an **LSI** one stays strongly consistent.
  `ConsistentRead: true` is accepted everywhere except a GSI `Query`/`Scan`,
  which rejects it (`ValidationException` — only `animusd`, with `Metadata`
  in hand, knows an index's kind).

  Regression: `animus-dynamo`'s `wire` unit tests plus `tests/
  dynamo_index_scan.rs`/`kind_scan.rs` end to end.

  Surface also covers `UpdateItem`/`BatchWriteItem` (condition-gated,
  per-request/per-tablet atomicity only) and **atomic** `TransactWriteItems`/
  `TransactGetItems` (via `ClientCtx::cp_txn`) — see ADR 0018 §2 for the
  condition-evaluation layering, including the follow-up amendment that
  gave a write action's own `ConditionExpression` full **cross-node** OCC
  (apply-time `write_conditions`, not just same-node `rmw_lock`
  protection). `DeleteItem` writes a tombstone *value*.
- **CQL v4** (`cql.rs`, `RoleAddrs.cql`) — `STARTUP`/`OPTIONS` handshake +
  `QUERY`/`PREPARE`/`EXECUTE` via the pure `animus_cql` crate. `CREATE TABLE`
  proposes a typed schema into the replicated catalog (incl. clustering/
  compound keys). A partition is one CP value, so `INSERT`/`UPDATE`/`DELETE`
  are RMW under `rmw_lock`; the requested consistency level is accepted but
  moot (CP). Keyspaces are **replicated** (`CREATE KEYSPACE` proposes
  `MetaCommand::CreateKeyspace`; `USE`/qualifier validation reads the
  replicated set via `keyspace_exists`, with a `ks.table`-prefix fallback).
  Only the **prepared-statement store** (`CqlState`) is per-node edge state
  (shared across connections *to the same node*, isolated between nodes,
  lost on restart); prepared ids are
  content-addressed (FNV-1a of the text).
- **Admin / debug** (`admin.rs`, `RoleAddrs.admin`, ADR 0020) — read-only `GET`
  views (`/admin/{config,status,peers,raft,raftkv,txns,storage/*,metrics,metrics/
  history,member/drain-status,health,control/members,system-table}`) + gated
  `POST` actions (`/admin/{tablet/split,tablet/merge,storage/flush,storage/
  compact,raftkv/reconfigure,drain,member/add,member/remove,control/member/
  add,control/member/remove}`) + data writes (`/admin/data/{dynamo,cql,
  drop-table,seed}`). Below the edge it only reads node state (aggregated
  live per request) or drives a gated action. **No auth — bind to a trusted
  interface.** The `animus admin` CLI consumes it. The bulk seeder
  (`action_data_seed`) writes real **DynamoDB items** — key/value bytes
  built exactly as the DynamoDB edge's `PutItem` would, so seeded rows read
  back through `GetItem`/`Query`/`Scan`. `POST /admin/data/dynamo`
  (`action_data_dynamo`) reaches **both** services on the DynamoDB
  listener — the item API and, for `ListStreams`/`DescribeStream`/
  `GetShardIterator`/`GetRecords` (bare op name or `DynamoDBStreams_
  20120810.`-qualified), the Streams read API (ADR 0042 §3) — by resolving
  `op` to a target and calling `dynamo::execute_routed`, the same
  prefix-fork function `dynamo::dispatch` itself uses; **never** call
  `dynamo::execute` from here directly, which skips that fork entirely
  (see `docs/engineering-lessons.md`'s "same-listener dispatch fork" entry
  for the bug this shortcut caused before the fix). `key_display`/`parse_key_display`
  render a binary partition token as unpadded base64url; a plain-client key
  is verbatim/printable. `/admin/peers`'s `peers: [{admin, role}, ...]`
  field carries each node's deployment role straight off replicated
  `Metadata.node_addrs[*].role`, so the dashboard never has to fetch a
  specific node's own `/admin/config` just to learn it.

  `GET /admin/txns` mirrors `/admin/raftkv`'s node-local,
  one-entry-per-hosted-tablet shape: `pending` lists this group's still-
  `Pending` anchored transaction records (record key, created timestamp,
  age vs. `RECOVERY_GRACE`, `intent_spans`); `unresolved_decided` lists
  decided-but-not-yet-locally-resolved ones. Pure observer — the existing
  `txn_resolver_loop`/`ClientCtx::txn_recover` machinery already drives
  every record listed here to resolution with no operator action.

  `GET /admin/storage/control` surfaces the **control-plane's own
  system-keyspace engine** stats, keyed on `ctx.control_storage` rather
  than a hosted CP tablet group, since a control-only node hosts none —
  `{"available": false}` on a data-only node; on a combined node the
  numbers legitimately coincide with a hosted tablet's own
  `/admin/storage/lsm` (same physical shared engine, `Metadata` at a
  reserved key prefix within it).

  `GET /admin/system-table?kind=&after=&limit=` browses that same engine's
  live rows. **Load-bearing**: scans
  `animus_control::syskv::reserved_scan_bounds()`'s `[start, end)` via one
  `StorageEngine::scan`, filtering by `kind` in memory — **never**
  `StorageEngine::entries()`, which would scan the *whole* engine (every
  user table's data too, on a combined node sharing it with the CP data
  plane, ADR 0028); see the engineering-lessons entry before ever
  "simplifying" this to `entries()`. `applied_index` is a dedicated point
  read of the watermark key, never derived from the scan window. Every
  `EntityKind` is browsable, including internal/legacy ones.
- **Web console** (`dashboard.rs` + assets, ADR 0021) — a self-contained
  vanilla-JS SPA, a pure client of `/admin/*` JSON. Tabs are **role-gated
  client-side** — a data-only node shows a dedicated **Node** view instead
  of the cluster-wide tabs. The **Storage** tab (control-only and combined
  nodes) carries a "Control system keyspace" card with its own
  control-role node selector and a nested browse section against `GET
  /admin/system-table`. The Overview groups nodes as "Control plane" /
  "Data nodes" when any control-only node exists. **Cluster health means
  "is the data at risk," not "is anything in transition"** (ADR 0021 §7):
  `tabletStatus`'s ladder (`quorum-lost` → `under-replicated` → `healthy` →
  `forming`) only degrades on an actual redundancy/quorum loss; a
  split-child or freshly-provisioned tablet forming its Raft group with
  every assigned replica's node alive renders as a neutral `forming` pill,
  escalating to degraded only if stuck past 60s. **Secondary indexes (ADR
  0041)** surface in the Data Browser (an Indexes card off the selected
  table's `schema.indexes`, plus an Index selector that adds `IndexName`
  to the Scan/Query payload), and a GSI's hidden `<base>$<index>` table
  is grouped under its base in the Tablets and Overview views
  (`splitHiddenTable`, `dashboard_core.js`) rather than shown as an
  unrelated table. **That hidden table has NO entry of its own in
  `status.schemas.tables`** — verified against a live cluster; it exists
  only as ordinary rows in `status.tablets[*].table` (and only once the
  drain lazily provisions its first tablet, i.e. after the first write to
  an indexed attribute) — so any dashboard code deriving "which tables
  exist" from the schema catalog naturally already excludes it, and code
  that needs to know about it must scan the tablet map instead.
- **OTel** (`otel.rs`, ADR 0027) — `init_tracing(instance_id)` from `main.rs`;
  `current_traceparent`/`set_parent_traceparent` carry W3C trace context across a
  forwarded hop (`cp_forward` injects, the receiver's `handle_client`
  re-parents), so a forwarded write is one joined trace when export is enabled.
- **`GET /metrics`** (ADR 0015) shares the DynamoDB listener; `ClientCtx::
  metrics_text` aggregates both role sinks (control + raftkv) live at request time.

## Gotchas

- **The DynamoDB Streams segment store + sealer knobs are wired via the
  `_with_orphan_sweep_after`-style layered-wrapper convention (ADR
  0042/0043, round-3 sealer PR)** — `BoundNode::start_with`/
  `BoundDataNode::start_data_with`/`run_node_with*`/`start_cluster_with*`
  all keep their exact pre-existing signatures, defaulting internally to
  `StreamSealKnobs::default()` (4 MiB / 4h, the ADR's own production
  defaults) and `SegmentStoreConfig::default()` (`Cluster`, the default
  K-replicated store); a `_streams`-suffixed sibling
  (`start_with_streams`/`start_data_with_streams`/`run_node_with_streams`/
  `start_cluster_with_streams`) takes the two explicit params. `main.rs`'s
  `--stream-seal-bytes B`/`--stream-seal-age SECS`/`--segment-store
  dir:PATH` flags (`--config/--node` and `--cluster N` only, so far — the
  split-deployment and data-only CLI paths are a named follow-up) call the
  `_streams` variants; a test that needs tiny seal thresholds (never the
  production defaults — see `index_drain.rs`'s `stream_sealer_tests`) does
  too. **`--stream-retention SECS` (round-3 PR7, the segment janitor's own
  knob) follows the identical convention** — `start_with_streams`/
  `start_cluster_with_streams`/`run_node_with_streams`/`start_cluster_inner`
  each gained one more trailing `Duration` parameter (defaulting to
  `DEFAULT_STREAM_RETENTION`, 24h, at every non-`_streams` call site,
  including every `start_cluster_with_auto_split*` wrapper), while
  `BoundControlNode::start_control_with` (control-only) hardcodes the
  default inline with no override yet — the same "split-deployment CLI
  path is a named follow-up" precedent this bullet's own opening sentence
  already established for the seal knobs/segment-store config. `main.rs`
  parses it identically to `--stream-seal-age`. `SegmentStoreHandle`
  (`Cluster(ClusterSegmentStore<ProdEnv,
  FsSegmentStore>)` or a bare opt-in `Fs(FsSegmentStore)`) and
  `StreamSealKnobs` live on `DataRole` (`ClientCtx.data()`), built by
  `build_segment_store` at node-assembly time — the **default** cluster
  variant roots its own per-node local `FsSegmentStore` at
  `<node dir>/segments` (a sibling of the `internal/` subdirectory
  `ProdEnv::bind` already owns; `BoundNode`/`BoundDataNode` gained a `dir`
  field to carry that path forward, since neither previously kept it past
  bind time) and is backed by a `ControlPlacementView` over this node's own
  control handle (live `Active` members; label-blind, matching
  `cluster_segment_store.rs`'s own current policy — a later PR that wants
  failure-domain-aware segment placement would extend this view).
- **`ClientRequest::ForceSeal { tablet }`** (round-3 sealer PR) is the
  internal-only RPC behind F12-b's disable-triggered final seal — addressed
  by tablet id directly (no client key to derive it from, unlike
  `KindWrite`/`KindScan`), refused bare, handled only inside
  `cp_serve_forwarded`. `ClientCtx::force_seal_tablet` is its caller-side
  wrapper (`dynamo.rs::disable_stream`, one call per tablet of the table
  being disabled) — a deliberately **simpler** retry shape than
  `cp_forward`'s hint-chasing loop (re-resolves routing from scratch every
  iteration rather than chasing a stale hint), acceptable for a rare,
  human-initiated admin-ish operation with no hot-path latency budget to
  protect. **Every send of an internal-only variant across the wire must
  wrap it in `ClientRequest::Forwarded`, even when the caller already knows
  it isn't the leader** — a first attempt called `ClientCtx::relay`
  directly with the bare `ForceSeal`, which compiled and passed every
  single-node test (the local branch never goes through `relay` at all)
  but failed loudly the moment a real multi-node test exercised the
  forwarding branch, exactly because the receiving side's bare-request
  refusal is designed to catch precisely that mistake. See
  `docs/engineering-lessons.md`'s Testing section for the general rule this
  is now an instance of (a forwarded-command test suite needs at least one
  non-leader-issued call).
- **`ClientRequest::StreamHotRead { tablet, from_position, limit }`** (PR6)
  is `ForceSeal`'s read-side sibling — the internal-only RPC behind
  `GetRecords`'/`GetShardIterator`'s open-shard path (ADR 0042 §7/§8):
  same addressing (by `tablet` directly), same bare refusal, same
  "handled only inside `cp_serve_forwarded`" contract, same reason
  (`is_relayable_command` doesn't apply — this is a data-plane RPC, not a
  `MetaCommand`). `ClientCtx::read_stream_hot_records` is its caller-side
  wrapper, copying `force_seal_tablet`'s exact retry shape (fresh
  `resolve_cp_route` every iteration, no hint-chasing) rather than
  `cp_forward`'s hot-path optimization — acceptable for a `GetRecords`
  poll, which already tolerates "not there yet" as part of the stream's
  own eventually consistent contract. Answered with the pre-existing
  `ClientResponse::Pairs` shape (no new response variant): the filtered/
  sorted/limited `(source_key, change_record bytes)` list, exactly what
  `index_drain::hot_read` (the leader-local, **no-`ReadIndex`** scan this
  RPC exists to reach — F8, never to be "upgraded" to a linearizable
  scan) returns. See `dynamo.rs`'s "DynamoDB Streams" entry above for the
  read path's own full design.
- **A node runs one internal `ProdEnv`, on one id (ADR 0040)** — the control
  Raft rides `PRIMARY_STREAM` (stream 0, ADR 0026's default); every per-tablet
  Raft group this node hosts rides its own stream (`stream = tablet_id`, which
  floors at 1), so the two never collide on the one shared inbox (a combined
  node used to bind *two* `ProdEnv`s on two distinct ids purely because one
  inbox was single-consumer, before ADR 0026 let one id host several
  protocol instances). The client API is a plain TCP server, *not* on the
  `Network` — a non-leader forwards over a fresh client connection.
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
  shrink the control group's *live* `RaftCore` config at runtime — local-
  control-leader-only, **not** relayed, **not** in `is_relayable_command`
  (the underlying primitive is `RaftNode::change_membership`, not a
  `MetaCommand` proposal, so only a genuine control-group voter's own
  in-process handle can call it). `POST /admin/control/member/{add,remove}`
  + `GET /admin/control/members`; `animus admin control-{add,remove,grow}`.
  **Add** takes either an operator-supplied id or, if omitted, self-mints
  one (`NodeId::mint`); if `node` isn't already a live voter, an existing
  member gets its `internal` address updated (`RegisterNodeAddrs`), else an
  unclaimed id goes through `register_node`'s `RegisterNode` CAS — no
  separate address-replication path is needed since the single unified
  `peer_sync_loop` (see the gotcha above) already keeps every node current.
  CLI `control-add` disambiguates by **arity** (2 args = self-minted, 3 args
  = operator-supplied id) — see `animus-cli/CLAUDE.md`. **Remove** has a
  genuine survivor-liveness guard: `RaftNode::control_peer_believed_alive`
  (`animus-control`, `CONTROL_PEER_LIVENESS_TIMEOUT = 500ms`) answers "is
  this control voter alive" without bridging the raftkv-keyed failure
  detector; `admin_remove_control_member` refuses if the *resulting* live
  voter count would fall below a majority, naming the dead voter(s) and
  pointing at a `force: bool` parameter (`--force`) — deliberately
  independent of `decommission --force-control-remove`, which only means
  "run `control-remove` as part of decommission," never "skip its safety
  checks." The core-level `RaftCore::change_membership` still has no
  survivor-liveness guard by design — that guard lives one layer up, in
  this crate's admin action, the only layer with a `RaftNode` handle to
  ask. Removing the current leader's own slot arms a `transfer_leadership`
  and returns the same not-leader refusal every other case here uses.
  Regression: `tests/control_membership_admin.rs`'s full add/remove/force
  matrix plus a `SimEnv` proof in `animus-control/tests/
  control_membership.rs`. See ADR 0037 (and ADR 0040's amendment on it) for
  the full design, and `docs/engineering-lessons.md` for the id-space-
  mismatch and self-registration/admin-action-clobber war stories.
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

`cargo test -p animusd` — all tests are real-socket `ProdEnv` integration
tests that poll with timeouts, not deterministic assertions (this crate has
no `SimEnv` — it is the assembly/wire layer over the two sim-tested crates
below it). The restart tests run both incarnations in the same runtime,
calling `Node::shutdown()` between them. Two in-crate `#[cfg(test)] mod`s
(`split_fence_tests`, `auto_split_median_tests`) live in `lib.rs` itself
because they need private handles (a raw `CpGroup`/the private
`byte_weighted_median` helper) that no external `tests/` file can reach;
`index_drain.rs`'s own `gsi_drain_cursor_tests` is a third (run via `cargo
test -p animusd --lib`, not the `tests/` tree) — the ADR 0042 §7/§8
cursor-based drain + trim janitor regressions, needing `CpGroup`'s private
`pending_changes`/`cursor_min_watermark`/`cursor_rows_with_token` and the
plain-client-protocol `ClientRequest::SplitTablet`/`MergeTablets` (an
arbitrary binary `split_key`, unlike the admin HTTP surface's UTF8-string
one); `dynamo.rs`'s own `stream_write_path_tests` is a fourth (ADR 0042
§1), needing `CpGroup`'s private `pending_changes`/`local_scan_kind_bounded`
(a new, non-linearizable bounded kind-scan wrapper, mirroring
`local_get_kind`'s existing shape) to prove a streamed-unindexed table's
write commits exactly base + change, no LSI/footprint row;
`index_drain.rs`'s own `stream_sealer_tests` is a fifth (round-3 sealer PR)
— the seal arm's triggers/sequence (size, age, empty-hot no-seal, the
exactly-at-watermark boundary), the F10/F12-b hot-trim rework (the
GSI+stream min-rule, and — reviewed hard — the
disabled-draining-does-not-block-trim rule), disable-as-final-seal with
epoch continuity across a disable/re-enable cycle, and F11's split-key
token alignment, needing `CpGroup`'s private `pending_changes`/
`approx_bytes_kind`/`cursor_min_watermark` and, to confirm a segment
genuinely landed, a second `FsSegmentStore` handle at the exact
`<node dir>/segments` path the default store roots its own local building
block at.

One binary per behavior; the file names describe them (`ls
crates/animusd/tests/`) — covering combined/control-only/data-only/split
deployment shapes and growth/decommission, control-plane and CP-data-plane
membership change, the DynamoDB/CQL/admin/dashboard wire edges (including
the ADR 0041 secondary-index suites, the ADR 0042 `SetTableStream`/
`DescribeTable`/`UpdateTable` streams surface plus PR6's
`ListStreams`/`DescribeStream`/`GetShardIterator`/`GetRecords` read path
(closed-shard chains, the iterator-survives-a-seal property, `Limit`
pagination, cross-node reads of both sealed and open shards, and F12-b's
disable grace window) in `dynamo_streams.rs`, and the ADR 0018 transaction
suites), the ADR 0043 §A9 segment janitor end to end in `stream_janitor.rs`
(two-phase retention with on-disk object deletion, a control-leader kill
mid-sweep, no empty-success gap across expiry, replica repair onto a fresh
target, the full disable-grace lifecycle, and the drop-table cascade
converging via the janitor alone — every retention-focused test seals two
epochs in sequence first, since the epoch-derivation guard never
physically removes a tablet's own current last epoch), the round-3 PR8
`streams_e2e.rs` suite (an auto-split mid-stream with a live consumer
walking the lineage handover, a real `LsmEngine` restart surviving the
catalog/segments/label, the `FsSegmentStore` opt-in, a GSI+stream table
proving ADR 0042 §8's trim min-rule coexistence, and the merge stopgap
rejected through the real admin API — using a `drain_tablet_lineage`
helper that walks a tablet's *whole* epoch chain, since a fixed shard's
`NextShardIterator` null only ends one epoch, not the whole stream),
restart/durability across every deployment shape, and the
`WatchMetadata`/system-table/OTel/metrics support surfaces.
`support/mod.rs` holds the shared bring-up helpers (port-TOCTOU retries,
split-cluster bring-up).
