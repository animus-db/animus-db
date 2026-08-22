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
gone. Streams implementation notes: [`docs/streams-notes.md`](../../docs/streams-notes.md).

**`lib.rs` is ~6800 lines** — grep for the symbol, don't scroll. It also holds
in-crate `#[cfg(test)] mod`s that need private handles the `tests/` tree
can't reach — e.g. `auto_split_median_tests` and `confirm_futility_tests`
(issue #268 — the confirm-loop fast-fail regression, needing a raw
`CpGroup` + the `pub(crate)` `ClientCtx::cp_kind_local`; `split_fence_tests`
and `hot_read_latch_tests` were deleted with their fence/latch subjects in
the ADR 0050 Train B rung-7 sweep). `index_drain.rs` has another,
`gsi_drain_cursor_tests`, and `dynamo.rs` another, `stream_write_path_tests`
(ADR 0042), for the same reason (see each file's own entry below).

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

- **`lib.rs`** (~6800 lines) — the node assembly and everything
  routing/hosting: `Node`/`BoundNode`/`BoundControlNode`/`BoundDataNode`,
  `ClientCtx`, `ClusterEdgeState`, CP routing, the tablet-host reconciler
  and auto-split loops, and the `ClientRequest`/`ClientResponse` wire
  types. See the sections below for the parts worth a contract.
- **`main.rs`** — thin CLI wrapper; dispatches the invocation modes (below) and
  wires `otel::init_tracing` + the Ctrl-C graceful-shutdown path.
- **`config.rs`** — `ClusterConfig`/`RoleAddrs` (per-process deployment
  config; every entry names its own **`id: NodeId`** rather than deriving
  it from position — `from_json` hard-errors on a duplicate) and the
  **seven-port stride** (ADR 0047 + ADR 0052: `base_port + 7*i +
  {internal,client,dynamo,cql,admin,intra,console}` — `intra` at offset 5
  (the client/intra-cluster RPC port split), `console` at offset 6 (the
  AnimusDB Data Console, ADR 0052 — a DynamoDB-shaped data app on its own
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
  2026-08-19 "Code patterns" entry on this exact port addition).
- **`control_handle.rs`** — the `ControlHandle` seam (ADR 0035 PR1):
  `Local(RaftNode<ProdEnv>)` for a node with real control Raft, vs.
  `Remote(RemoteControlClient)` for a data-only node reaching a separate control
  deployment over the network. `metadata_cached()` vs. `metadata_fresh()`
  freshness contract lives here.
- **`topology.rs`** — pure, side-effect-free routing decisions extracted from
  `lib.rs` for unit-testing (`decide_cp_route`, `tablet_for_key`,
  `format_not_leader_refusal`/`parse_not_leader_refusal`). All `pub(crate)`.
- **`dynamo.rs`** (~59 KB) — the DynamoDB JSON-over-HTTP edge; the `GET /metrics`
  route (ADR 0015) shares this listener. `dispatch` also forwards a
  `DynamoDBStreams_20120810.*` target to `dynamo_streams::execute`
  (below) — the two services share one listener/port.
- **`dynamo_streams.rs`** (ADR 0042 §3/§5/§6/§7/§9/§10/§11) — the
  DynamoDB Streams read API: `ListStreams`/`DescribeStream`/
  `GetShardIterator`/`GetRecords`. Full design (label resolution, the
  sealed-vs-open serve split, `StreamHotRead`) is in
  `docs/streams-notes.md` — this entry is just the module pointer.
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
  hot-trim rework, ADR 0045 §2 backfill seeder) — the per-node
  **change-consumer loop** (`change_consumer_loop`, renamed from
  `index_drain_loop` since it is no longer GSI-specific), four arms per
  tick per led tablet: the GSI drain, the seal arm, the **backfill
  seeder**, and the hot-trim arm. The backfill seeder runs once per index
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
- **`cql.rs`** (~42 KB) — the CQL (Cassandra) v4 binary-protocol edge.
- **`cql_client.rs`** — a minimal loopback CQL client the admin dashboard's CQL
  editor uses (`POST /admin/data/cql`) to drive this node's own CQL port.
- **`admin.rs`** (~58 KB) — the admin/debug HTTP-JSON endpoint (ADR 0020):
  read-only `GET` views + gated `POST` actions + the dashboard's data-write
  surface; also serves the SPA static assets.
- **`http.rs`** — shared hand-rolled HTTP/1.1 helpers (request parser + response
  writers) used by `dynamo.rs`, `admin.rs`, and `console.rs`.
- **`otel.rs`** — OTLP/HTTP distributed-tracing seam (ADR 0027); opt-in, no-op
  unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Scoped to this crate only.
- **`dashboard.rs`** + **`dashboard.{html,css}`** + **`dashboard_*.js`** — the
  "AnimusDB Console" SPA (ADR 0021): `include_str!`'d and served as
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
  — the AnimusDB **Data Console** (ADR 0052): a DynamoDB-shaped data app for
  application developers, on its own dedicated port (`RoleAddrs.console`) —
  never the admin port (documented no-auth, trusted-interface-only, ADR
  0020) and never a route on the DynamoDB wire listener. Bound on combined
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
map (the reconciler then `Reclaim`s it everywhere). A stale-routed write
to a frozen parent gets the retryable `FROZEN_REFUSAL` from every local
write/txn helper (`frozen_refusal`; bookkeeping-only kind batches exempt).
**Both `ClientCtx::cp_kind_write_item` (the evaluated arm) and
`cp_kind_write_raw` (the fast/marker arm) retry this internally** (issue
#288: pre-fix, neither had a retry loop at all — every Dynamo/CQL/raw-
protocol write funnels through one of these two since ADR 0049's write-path
unification, so a write racing the freeze window got a terminal 500
instead of the write landing on the child a moment later), mirroring
`cp_read`'s deadline-bounded loop and re-resolving `cp_route` each attempt.
A `Building` child runs **no consumer arms at all** (its token-truncated
cursor key would land in the parent's scope and poison the parent's
min-over-rows watermark). E2e: `tests/split_build.rs` (full workflow +
racing txns + post-freeze leader kill).

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

## Deployment shapes (ADR 0035)

Three shapes (combined/control-only/data-only), all built from the same
role assemblies — see ADR 0035 for the full design. **There is no
engine-less control-plane deployment shape**: `BoundControlNode::
start_control_with` **unconditionally** provisions one small dedicated
system-keyspace engine, since `Metadata` is `StateMachine::DRIVER_APPLIED`
and this engine is the durable home of the control plane's async apply
task's published cache (see `animus-control/CLAUDE.md`'s `node.rs`/
`mirror.rs` entries).

**Console binding (ADR 0052) follows the same split as `dynamo`/`cql`**:
combined and data-only bind `RoleAddrs.console` (real CP-data tablets to
show); control-only does not (`Node::bind_control` never reads
`addrs.console` at all, and `BoundControlNode::start_control_with` passes
`None` for `spawn_common_tail`'s `console_listener` — `Node::console_addr()`
panics there, mirroring `dynamo_addr()`/`cql_addr()`'s existing
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
  not hand the client the group's formation/election window; the CQL
  `CREATE TABLE` edge shares the same helper; regression:
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
  lock never needed the wider span for correctness.
  **The plain-table half of the old named gap is closed (ADR 0049)**: a
  plain table's conditioned `PutItem`/`DeleteItem` and `UpdateItem` now
  route through this same leader funnel (constant-true gate, below), so
  their conditions/RMW evaluate at the leader too; only CQL's own RMW
  (`cql.rs`) keeps the node-local-`rmw_lock`-only scope — deliberately,
  including after Train A's CQL rung, which moved CQL's *commit* onto the
  kind path but not its RMW's evaluation point (a CQL partition write has
  no derived state, so there is no U3 funnel to route it through; the
  cross-node RMW race stays a documented pre-existing gap of its own).
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
  evaluates at the tablet leader; CQL's own RMW keeps the gap — see the
  routing section's note on why the CQL rung deliberately did not move it),
  and a plain or CQL table's markers are **transient** (Train A rung 4):
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
- **CQL v4** (`cql.rs`, `RoleAddrs.cql`) — `STARTUP`/`OPTIONS` handshake +
  `QUERY`/`PREPARE`/`EXECUTE` via the pure `animus_cql` crate. `CREATE TABLE`
  proposes a typed schema into the replicated catalog (incl. clustering/
  compound keys). A partition is one CP value, so `INSERT`/`UPDATE`/`DELETE`
  are RMW under `rmw_lock`; **the commit itself rides the universal
  kind-write path (ADR 0049 Train A rung 2)** — `cql::kind_partition_write`
  commits one `KindBatch` entry per mutation (the partition's base row or
  whole-partition tombstone + an image-less marker record built by the
  shared `dynamo::marker_change_log`, change-key prefix = the partition's
  own `data_key` bytes, `base_sk` empty), so every CQL mutation is
  observable on the tablet's change log; in-crate regression
  `cql::cql_kind_write_tests` (real-socket, needs `pending_changes`). The
  requested consistency level is accepted but moot (CP). Keyspaces are **replicated** (`CREATE KEYSPACE` proposes
  `MetaCommand::CreateKeyspace`; `USE`/qualifier validation reads the
  replicated set via `keyspace_exists`, with a `ks.table`-prefix fallback).
  Only the **prepared-statement store** (`CqlState`) is per-node edge state
  (shared across connections *to the same node*, isolated between nodes,
  lost on restart); prepared ids are
  content-addressed (FNV-1a of the text).
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
  (external, DynamoDB/CQL-adjacent callers) and `RoleAddrs.intra` (every
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
  self-registration/admin-action-clobber war stories.
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
  `ProdEnv::shutdown()`s the node's one internal env, freeing all seven ports
  on a combined/data-only node (ADR 0040 PR1's `internal`/`client`/`dynamo`/
  `cql`/`admin` stride, plus ADR 0047's `intra` and ADR 0052's `console` — the
  pre-ADR-0040 stride was six too, but split across two role envs instead of
  one node/one port-block; a control-only node frees five, since it never
  binds `dynamo`/`cql`/`console`) so a replacement can rebind
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
  bounds the other, which is the rule to remember; note this driver runs
  only over `ProdEnv` (`animusd` has no `animus-sim` dependency), so
  simulated-clock reasoning does not apply to it either way. No code
  changed; the pre-pass scan stays.
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
group's `halted` flag.

One binary per behavior; the file names describe them (`ls
crates/animusd/tests/`) — covering combined/control-only/data-only/split
deployment shapes and growth/decommission, control-plane and CP-data-plane
membership change, the DynamoDB/CQL/admin/dashboard wire edges (including
the ADR 0041 secondary-index and ADR 0018 transaction suites), the ADR
0042/0043 streams surface end to end (`docs/streams-notes.md` has the
streams-specific test notes), the ADR 0051 TTL surface end to end
(`dynamo_ttl.rs` — enable/disable/describe, the AWS-faithful
immediate-visibility-then-eventual-reap contract, the future/wrong-type/
5-year-safety-window never-expire cases, the conditional-delete outcome,
and the stream `userIdentity`; its own follower-relay regression for
`UpdateTimeToLive` lives beside the rest of `schema_ddl_relay.rs`'s DDL
suite, not in `dynamo_ttl.rs` itself), restart/durability across every
deployment shape, and the `WatchMetadata`/system-table/OTel/metrics support
surfaces.
`support/mod.rs` holds the shared bring-up helpers (port-TOCTOU retries,
split-cluster bring-up).
