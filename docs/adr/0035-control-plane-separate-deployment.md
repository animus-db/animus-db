# ADR 0035 — Control plane as a separate deployment

- **Status:** Implemented — PR0 through PR7 are all shipped (§Delivery plan).
  Amends ADR 0030 (data-plane-only growth) and ADR 0032 (seed/join, address
  book) — see the note at the end of ADR 0030 and the top of ADR 0032.
- **2026-08-10 note:** a **control-only** node (§ this ADR's PR3) now
  **unconditionally** provisions a dedicated system-keyspace storage engine
  (ADR 0038) — it is no longer a storage-engine-free deployment shape.
  `Metadata` durably lives there (an async apply task owns it), not in an
  optional shadow mirror as ADR 0038's PR2 first shipped it. Everything else
  about the three deployment shapes this ADR describes is unchanged.
- **Amended by [ADR 0040](0040-self-minted-string-node-ids.md) (2026-08-11):**
  the Context section below (`config::control_id(i) = i` /
  `config::raftkv_id(i) = RAFTKV_ID_BASE + i`, a **combined** node binding
  two ids on two `ProdEnv`s) describes the pre-ADR-0040 scheme, now
  historical — a combined node binds **one** env on **one** identity (ADR
  0040 Decision A), so the "six-port-per-node stride" this ADR's config
  section describes is now **five** ports, and `RoleAddrs`/`NodeAddrs` carry
  one `id`/`internal` address instead of a `control`/`raftkv` pair. The
  three deployment *shapes* (combined/control-only/data-only) and their role
  assemblies are otherwise unaffected — this is a same-shape addressing
  simplification, not a new shape.
- **Date:** 2026-08-09

## Context

Every `animusd` process today assembles **both** planes on fixed,
index-derived ids: `config::control_id(i) = i` (this process's metadata Raft
voter) and `config::raftkv_id(i) = RAFTKV_ID_BASE + i` (this process's
per-tablet CP-data role), both bound from the same `ClusterConfig` entry at
index `i` (`crates/animusd/src/config.rs`). `ClientCtx.raft` is a bare
`RaftNode<ProdEnv>` — the process's own in-process control-plane handle — read
directly at roughly thirty call sites across `lib.rs`, `dynamo.rs`, `cql.rs`,
and `admin.rs` (`ctx.raft.metadata()`, `ctx.raft.last_applied()`,
`ctx.raft.propose(..)`, and friends). `bootstrap` (leader-only, idempotent)
registers **every control voter's own `raftkv` id** as an `Active` data
member — so today a control-plane voter is unconditionally also a data node;
there is no way to run one without the other.

ADR 0030 and ADR 0032 already built most of the machinery a *data-only* node
needs, but only as a **growth path**, layered on top of a normal node that
still always starts with a full local control role:

- ADR 0030's non-voter control core (`!control_ids.contains(&self.control_id)`)
  plus `remote_metadata_sync_loop` / `ClientCtx::effective_metadata()` proves a
  node can serve every CP-routing, hosting, and admin-status call correctly
  off a **polled mirror** of another node's `Metadata`, with no local control
  Raft replication of its own.
- ADR 0030 §3 ("Non-voter control id — verified, and found insufficient on its
  own") explicitly considered and rejected running a node with **no control
  role at all**, calling it "not viable without a much larger refactor than
  this slice warrants" — at the time, `BoundNode::start_with`'s structural
  requirement that every node owns a `ClientCtx.raft: RaftNode<ProdEnv>` made
  that true. This ADR is that larger refactor, made from having watched the
  mirror path work in production for two ADRs' worth of features (growth,
  then join) with zero correctness incidents traced to staleness.
- ADR 0032 PR1's replicated node address book and `route_sync_loop` close the
  one gap ADR 0030 left open (a node's own `client_route`/admin peer list
  going stale as the cluster grows) — a live, cluster-wide address book that a
  *pure* data node can rely on exactly as a growth node already does.

What's still missing is a deployment shape where the **control-plane role
itself** can be operated, scaled, and reasoned about independently of the
data plane: a small, fixed-membership metadata quorum that never has to
absorb data-node churn (every join/decommission today proposes through
whichever control voters happen to also be running as data nodes), and a
data fleet that can grow/shrink/restart without ever touching control-plane
Raft state at all. Splitting the *deployment* is also a prerequisite for
independently tuning each side's resource profile (the control group's WAL is
small and latency-sensitive; the data fleet's LSM engines are the
capacity-heavy side) and for a future control-plane-only upgrade or restart
that doesn't require touching every data node's process.

## Decision

We will add two new, additive `animusd` deployment modes on top of the
existing role assemblies, and decouple `ClientCtx`'s control-plane access
behind a seam so the data-node mode never needs a local `RaftCore` at all.

### Target topology

- **`animusd control`** — a small static group (e.g. 3 nodes) running only:
  the control `RaftNode` (metadata Raft — membership, tablet map, schema
  catalog, node address book), the placement `reconcile_loop`, the failure
  detector (`detect_loop`), and the client + admin endpoints that serve
  `Status` / `ProposeSchema` / `JoinInfo` / admin actions (including
  decommission) and the dashboard. **No storage engine, no `raftkv` env, no
  DynamoDB/CQL edges** — a control node never hosts a tablet and never speaks
  the data-plane wire protocols.
- **`animusd data`** — N nodes running only: the shared LSM engine, the
  `raftkv` env + per-tablet Raft groups, the tablet-host reconciler (ADR
  0031), and the client/DynamoDB/CQL/admin edges. **No local control
  `RaftCore` at all** — metadata comes from a mirror of the control
  deployment, i.e. the ADR 0030 growth-node path (`remote_metadata_sync_loop`
  / `effective_metadata()`) promoted from "what a growth node falls back to"
  to **the only way any data node ever sees `Metadata`**. Data nodes join via
  seeds pointing at the control deployment (ADR 0032's `animusd join`,
  generalized so a seed can be a control-only node); data-node membership is
  always dynamic, allocated at join — never baked into a shared
  control/data config index the way `control_id(i)`/`raftkv_id(i)` are today.
- **Combined mode retained** for dev and the existing test suite: `--cluster
  N` and today's `--config`/`--node` flows keep assembling both role sets in
  one process, refactored as **composition of the two role assemblies**
  (`animusd control`'s bring-up + `animusd data`'s bring-up, wired to the same
  in-process `RaftNode` via a `Local` handle) rather than a third, independent
  code path. Nothing about `--cluster N`'s or `run_node`'s external behavior
  changes.

### Key mechanism decisions

1. **A `ControlHandle` seam replaces `ClientCtx.raft`'s bare
   `RaftNode<ProdEnv>`.** `enum ControlHandle { Local(RaftNode<ProdEnv>),
   Remote(RemoteControlClient) }`. Reads split by freshness contract, mirroring
   the existing `metadata()`/`effective_metadata()` split:
   - `metadata_cached()` — mirror-acceptable staleness (the existing
     `remote_metadata_sync_loop` polling contract). Used by CP routing,
     tablet-host reconciliation, `/admin/status`, `peer_sync_loop`,
     `route_sync_loop`, and every other call site ADR 0030 already routes
     through `effective_metadata()`.
   - `metadata_fresh()` — must reach the control leader itself, no mirror
     substitution. Two call sites need this today and must keep needing it
     under the split: the `CreateTable` commit-wait poll (`dynamo.rs`/
     `cql.rs`, which today call `ctx.raft.metadata()` directly rather than
     through `effective_metadata()`) and the DynamoDB conditional-write
     existence gate. `Remote::metadata_fresh()` proxies a `Status` request to
     the control deployment (relayed to its leader, mirroring
     `propose_schema`'s existing relay shape) rather than returning a locally
     cached value.
2. **Config/identity decoupling.** Today `control_id(i) = i` and
   `raftkv_id(i) = 300 + i` derive from one shared per-process index
   (`config.rs`), `peer_book` bundles both roles' addresses into one map, and
   `bootstrap` registers every control voter's `raftkv` id as an `Active` data
   member. Post-split: control membership is its own static list, independent
   of any data-node index; data-node ids are allocated at join time (as ADR
   0032's collision guard already does for a growth/join node), not derived
   from a control-group position; per-role peer books (a control node's peer
   book lists only control voters; a data node's lists only other data
   nodes' `raftkv` addresses); `bootstrap` stops auto-registering control
   voters as data members **outside combined mode** — combined mode keeps
   today's behavior exactly (a combined node is simultaneously a control
   voter and a data member, as it always has been).
3. **Heartbeats stay on the internal `Env` `Network`.** A data node's
   `raftkv` env keeps heartbeating the control ids as `RaftMsg::Heartbeat`
   (unchanged detector semantics, ADR 0012) — failure detection does not move
   to a new transport. Data nodes get the control deployment's internal
   addresses from seed config; **the control deployment's addresses are the
   static root of discovery** for everything downstream (join, growth,
   rebalancing all resolve through it), exactly as the pre-growth control
   group already is the root of discovery in ADR 0030/0032.
4. **`metadata_watch()` stays in-process only — PR4 starts a `Remote` data
   node on the fixed-interval poll; PR5 upgrades it to a long-poll wire
   primitive keyed on the same primitive.** `MetadataWatch`'s `AtomicWaker`
   itself never crosses a network hop — that part of the original sketch
   holds. What changed in delivery: rather than inventing a new push
   mechanism, PR5 adds `ClientRequest::WatchMetadata { last_seen }` — a node
   parks on **its own** `MetadataWatch` (server-side, only a genuine
   `ControlHandle::Local` replica serves it — see below) for a bounded
   `WATCH_METADATA_SERVER_TIMEOUT` (8s) and replies with the current
   `Metadata` either way, extending the existing `ClientResponse::Status`
   shape with a `watermark: u64` field. A `Remote` data node's
   `RemoteControlClient` then owns its **own** same-process `MetadataWatch`
   (disconnected from any `RaftCore`, exactly as before) but now **drives**
   it itself — `observe()` calls `watch.bump(watermark)` on every reply — so
   `ControlHandle::metadata_watch()` for `Remote` hands the tablet-host
   reconciler a real wake-on-change signal instead of the permanently-inert
   default PR4 shipped. `remote_metadata_watch_loop` replaces the fixed
   200ms poll: long-poll (preferring the leader hint, then every seed),
   falling back to a plain `Status` poll + backoff only when every seed
   fails at the transport level. **A follow-up increment ports the ADR 0030
   growth-node branch of `remote_metadata_sync_loop` onto this same
   `remote_metadata_watch_loop` mechanism** — PR5 as originally shipped left
   it on the pre-existing fixed-200ms `Status` poll, since a growth node's
   `ClientCtx.control` is `ControlHandle::Local` (a real, permanently
   non-voting control-group member), not `Remote`, so it had no
   `RemoteControlClient` to reuse; the growth-node branch now constructs a
   standalone one via `RemoteControlClient::with_mirror`, sharing
   `ClientCtx.remote_metadata`'s existing `Arc<Mutex<Option<Metadata>>>`
   directly as its mirror, purely to drive the identical long-poll loop.
   See `crates/animusd/CLAUDE.md`'s dedicated bullet for the detail. Two
   hardening details worth recording: (1)
   `MetadataWatch::bump` had to become `pub` in `animus-control` so
   `RemoteControlClient` could drive it from outside the crate — a small,
   safe widening of an existing primitive, not a new one; (2) `observe()`
   gained a non-regression guard (skip the mirror-overwrite unless
   `watermark >= watch.latest()`), since *any* control node may answer, not
   necessarily the most caught-up one, and a lagging replica's reply must
   never regress a mirror that had already advanced further. This closes
   most of the latency gap between "control commits" and "data node
   observes it" without a new push mechanism, exactly as originally
   sketched — only the wire shape (a dedicated long-poll request/reply
   pair, not a bare push) differs from the one-line description above.
5. **The ADR 0028/0033 write fence and read-side scope check become
   load-bearing for every node, not just a crossover-window edge case.**
   Today every node's own `Metadata` view can only ever be *slightly* behind
   the control leader's (same cluster, tight replication or a bounded growth-
   node poll). Once **every** data node routes off a polled mirror as a
   matter of course, the routing decision a client op is dispatched on is
   *routinely* one poll interval stale, not just during a rare split/merge/
   rebalance crossover — so the pre-propose range check
   (`cp_put_local`/`cp_delete_local`/`cp_batch_propose`), the embedded
   per-command fence, and the read-side `scope_range()` pre-check
   (`cp_get_local`/`cp_scan_local`) are what keep a stale-routed op safe
   (retryable error, never silent corruption or a false "absent") rather than
   a correctness assumption this split would otherwise quietly violate. Call
   this out explicitly in the safety review for PR4/PR5: these mechanisms
   already exist and are already tested (root `CLAUDE.md`'s "write fence"/
   "read-side scope pre-check" entries), but their justification changes from
   "covers a narrow race window" to "covers the routine case" the moment a
   data node has no local control Raft to fall back on.

### Rationale: zero control Raft code on a data node, not a non-voter core

We choose "data nodes run **zero** control-plane Raft code" over "data nodes
run a **non-voter** control `RaftCore`" (the shape ADR 0030 §3 built and this
ADR's own Context section describes). The non-voter shape is *available* and
already proven safe (`is_voter()` gates campaigning cleanly, ADR 0030 §3), but
carries WAL, snapshot-recovery, and replay machinery on every data node for a
consensus group that node never actually participates in — pure overhead with
no correctness or latency benefit, since a non-voter's local `Metadata` never
advances via real replication anyway (ADR 0030 §3's own finding: "a non-voter
role's local `Metadata` never updates via real Raft replication, no matter
how long it waits"). The growth-node mirror path already proves the
poll-based `Remote` shape works correctly in production; there is nothing the
non-voter core buys a *permanent* data node that the mirror doesn't already
provide more cheaply. (A control-plane-follower-less **growth** node
transitioning to a real voter, ADR 0030's own follow-up note, is a different
question — about the *control* group growing, out of scope here — from
whether an ordinary data node should carry a dormant control core, which this
ADR answers no to.)

## Non-goals (v1)

- **Control-group membership stays static**, as ADR 0030 decided. Making the
  control deployment itself elastic (`RaftCore::change_membership` on the
  control plane) is a future ADR; this one only relocates the static group
  into its own deployment, it does not make it grow.
- **No change to the consensus protocols themselves.** `RaftCore` stays sync
  and `Env`-free; `animus-cp-data`'s per-tablet Raft groups, `animus-consensus`
  Accord, and every wire protocol are untouched. This is a deployment-topology
  and call-site-routing change, not a protocol change.

## Delivery plan

Seven stacked PRs, following this codebase's standing discipline of landing the
low-risk mechanical piece first (ADR 0031/0032's PR stacks):

1. **PR0 (this ADR).**
2. **PR1: `ControlHandle` seam.** Pure refactor — introduce `ControlHandle`
   wrapping today's `RaftNode<ProdEnv>` as `Local` only (no `Remote` variant
   wired up yet), migrate every `ClientCtx.raft` call site to
   `metadata_cached()`/`metadata_fresh()`/the handle's propose/relay methods.
   No behavior change; existing combined-mode tests are the regression
   coverage.
3. **PR2: config/identity decoupling.** Split `control_id`/`raftkv_id`
   derivation off the shared per-process index; per-role peer books; scope
   `bootstrap`'s auto-registration of control voters as data members to
   combined mode only.
4. **PR3: `animusd control`.** The control-only entry point and CLI subcommand;
   binds only the control Raft + placement/detector loops + client/admin
   endpoints.
5. **PR4: `animusd data` with `Remote` handle.** Implement
   `ControlHandle::Remote`, wire it into the data-only entry point
   (`animusd data --config FILE`) — a data node never binds a local
   `RaftCore`. The seed/join variant (`animusd data --seed`) landed in PR5,
   below, alongside the freshness work it depends on.
6. **PR5: long-poll watch + seed/join + staleness audit.** Shipped three
   bounded pieces: (1) `ClientRequest::WatchMetadata` — a long-poll wire
   primitive a genuine control replica serves off its own `MetadataWatch`;
   `RemoteControlClient` gains its own driven copy of the same primitive, and
   `remote_metadata_watch_loop` replaces the fixed-interval poll for a
   `Remote` data node (see §4 above for the mechanism, which differs in
   shape from the original one-line sketch); (2) `animusd data --seed` /
   `run_node_data_join` — the data-only counterpart of `animusd join`,
   reusing its `JoinInfo` discovery + `Status` collision guard via two
   factored-out helpers (`discover_join_info`/`check_join_collision`); (3) a
   staleness-classification pass over every `metadata_cached()`/
   `effective_metadata()` call site touched since PR1, fixing the ones
   feeding a permanent/one-shot decision (`cp_scan`'s tablet ranges,
   `trigger_split`/`trigger_merge`'s CAS preconditions, `drop_table`'s
   confirm poll, `create_keyspace`'s RYW check, `admin_drain`'s
   leader-then-metadata ordering) and documenting the ones left as-is
   (`/admin/raft`'s own-replica-diagnostic view; `trigger_split`/
   `trigger_merge`'s *epoch*-staleness tolerance, distinct from the
   permanently-empty-view bug fixed alongside it). See `crates/animusd/
   CLAUDE.md`'s "What's non-obvious" entries for the full detail.
7. **PR6 (implemented): per-process split-cluster integration tests +
   docs/dashboard.** End-to-end tests running real `animusd control`
   processes alongside real `animusd data` processes (not combined mode,
   `tests/split_cluster.rs` plus the PR3/PR4/PR5 coverage already in
   `control_only.rs`/`data_only.rs`/`data_join.rs`/`watch_metadata.rs`):
   control-leader failover under live data traffic, tablet split + merge
   triggered against the data fleet's own admin port, a data-node failure
   detected and repaired onto a spare, decommission of a data node gated to
   the control leader's admin port (a data node's own admin port refuses
   with a leader-routing hint — it never registers a local control handle
   at all), and a full stop/restart of every process recovering both
   control metadata and data. Dashboard and `CLAUDE.md` updates reflecting
   the new topology as a first-class, documented deployment shape rather
   than only combined mode.
8. **PR7 (implemented): role-gated dashboards.** PR6 taught the dashboard to
   *render* a split deployment (the derived `role` field, the
   `nodeDisplayId` fix); PR7 makes each node's **own** page match its role,
   rather than every node showing the same five-tab cluster Console
   regardless of what it actually is. A control-only or combined node's page
   is unchanged; a data-only node instead gets a dedicated Node view — its
   own identity/health, control-plane mirror status, hosted tablets, a
   node-scoped storage-debug panel, and a link to a reachable
   control/combined node's Console — since the cluster-wide views
   (Overview/Placement/Tablets/Storage's node picker) have nothing useful to
   show a node with no control-plane Raft state of its own and, being a
   single node, nothing to place or balance. One backend addition:
   `/admin/raft`'s `control_mirror` (watermark, leader-address hint,
   has-synced) exposes `ControlHandle`'s existing `metadata_watch().latest()`/
   `leader_addr_hint()`/`has_synced_metadata()` — all already built for PR4/
   PR5's `Remote` handle, just not previously surfaced to any client. Tab
   gating is entirely client-side (`dashboard_core.js`'s `ROLE_TABS`, keyed
   on `/admin/config`'s `role`), resolved from a fast, node-local-only probe
   so it can never stall on a slow/unreachable peer the way the existing
   cluster-wide fan-out can. See `crates/animusd/CLAUDE.md`'s dashboard
   section for the full detail.
9. **PR8 (implemented): per-node role in `/admin/peers`.** A residual
   follow-up flagged after PR6/PR7 shipped: `GET /admin/peers` returned only
   `this` + a flat `admin_addrs` list, so a consumer (chiefly the dashboard)
   could only learn a *specific* node's role by fetching that node's own
   `/admin/config` — meaning "label/gate by role" depended on every node's
   own fan-out succeeding, not just this node's. Closed by adding
   `role: String` to `animus_control::meta::NodeAddrs` (`#[serde(default =
   "combined")]`, since every pre-ADR-0035 `RegisterNodeAddrs` registration
   was, by construction, combined-mode) — each node stamps its own role at
   the exact point it already self-registers its address book, so no new
   proposal/relay/endpoint was needed. `/admin/peers` gained an additive
   `peers: [{admin, role}, ...]` field (`admin.rs::peers_view`) reading every
   node's role straight off replicated `Metadata.node_addrs`; the pre-existing
   `admin_addrs` field is unchanged. The dashboard (`dashboard_core.js`)
   captures this as a fallback (`node.role`) alongside each node's own
   `/admin/config` fetch (`node.config.role`, still preferred when it
   resolves), so a down control-only node now appears in the Overview list
   tagged `"control"` and marked unreachable instead of vanishing entirely,
   and the Node view's "Open cluster console" link can target a candidate
   whose own `/admin/config` fetch hasn't resolved yet.

### Rolling upgrade / mixed-version compatibility

A cluster transitioning onto ADR 0035 (an old binary/config on some nodes,
a new one on others, mid-rollout) is safe in both directions:

- **Old config, new binary.** Every new field this ADR added
  (`RoleAddrs.role`, the `Option`-wrapped `control`/`raftkv` addresses,
  `ClientResponse::Status`'s `leader_hint`/`watermark`, `NodeAddrs.role`) has a
  `#[serde(default)]` (or an equivalent custom default, for the address
  `Option`s — see `crates/animusd/CLAUDE.md`'s note on why a bare
  `#[serde(default)]` would be wrong there) that resolves to combined mode
  for any config/wire payload written before this ADR. A new binary reading
  an old config or talking to an old peer behaves exactly as it did
  pre-ADR-0035.
- **New config/binary talking to an old binary.** `ClientRequest::
  WatchMetadata` and the `leader_hint`/`watermark` fields are additive; an
  old binary that has never heard of them simply never sends them (the
  field decodes to its default) and never receives a `WatchMetadata` (no
  new caller ever issues one against a peer it doesn't know supports it —
  every long-poll caller is itself a new-binary `Remote` data node, which by
  construction only exists once at least PR4 has shipped).
- **What does NOT work**: a `--control-nodes`/`--data-nodes` split config
  (`ClusterConfig::generate_split`) will not *parse* on a pre-ADR-0035
  binary — the old binary's config schema has no `role` concept and no
  `Option`-wrapped address fields, so it either fails to deserialize or
  silently misreads a `null` address. This is harmless in practice: a
  pre-ADR-0035 binary also has no `animusd control`/`animusd data`
  subcommands to run that config with, so the failure mode is "the old
  binary can't run the new topology at all," not silent misbehavior. A
  rolling upgrade therefore upgrades the **binary** everywhere first (safe,
  combined-mode-equivalent), then migrates config/topology node by node.

## Consequences

- **Independent scaling and operations.** The control deployment can be sized
  and tuned for a small, latency-sensitive metadata quorum; the data fleet can
  grow, shrink, restart, or be rebuilt without any control-plane Raft state
  ever being touched — closing the coupling ADR 0030/0032's growth/join
  machinery worked around but never eliminated (a growth/join node was always
  additive to a combined-mode core that still existed).
- **A data node's `Metadata` view is routinely, not just occasionally, one
  poll interval stale.** This makes the ADR 0028/0033 write fence and
  read-side scope check permanently load-bearing for every write and read on
  every data node (§5 above) — accepted, since those mechanisms are already
  built, tested, and exactly designed for this failure shape; what changes is
  how often they're expected to actually fire.
- **Combined mode is preserved exactly**, so the existing test suite, dev
  workflow, and `--cluster N` docs need no behavioral rewrite — only an
  internal reorganization into "compose the two role assemblies." This bounds
  the risk of the refactor: if PR1/PR2 regress anything, combined-mode tests
  catch it before PR3/PR4 ever introduce a genuinely new topology.
- **The control deployment becomes a harder single point of failure for
  *availability of change*, not of reads/writes.** A data fleet with a live
  `Metadata` mirror keeps serving CP reads/writes even if the control
  deployment is briefly unreachable (same as today's growth-node behavior);
  what it loses is the ability to observe *new* placement/schema/membership
  changes until the mirror reconnects. This is the same trade-off ADR 0030's
  growth node already accepted, now generalized to the whole data fleet.
- **`RaftCore::change_membership` on the control plane remains unbuilt.** A
  future ADR that wants the control deployment itself to grow past its
  initial static size still needs that mechanism plus its own safety review;
  this ADR's split composes with it later without conflict.

## Engineering lesson

ADR 0030 §3 evaluated "a node with no control role at all" and rejected it as
"not viable without a much larger refactor than this slice warrants" — a
correct call at the time, since `BoundNode::start_with` had a hard structural
requirement (every node owns a local `RaftCore`) with no seam to route around
it. Two ADRs later, that same slice (the growth-node remote-metadata mirror,
built for a narrower reason — letting a non-voter observe `Metadata` at all)
turned out to already be the missing piece: once a mirror is proven correct
in production, "no control role at all" stops being a refactor and becomes a
generalization of code that already exists. **A prior ADR's "out of scope,
too large a refactor" framing is a decision made against the codebase as it
existed then — recheck it against what has actually shipped since, especially
when a later feature (here, growth *and* join) incrementally builds the exact
mechanism the earlier ADR said was missing.** Recorded in the root `CLAUDE.md`
Engineering Practices section.

## Amendment (2026-08-14, ADR 0042/0043)

A **third** role assembly joins the control/data pair this ADR established:
`animusd streams --config FILE --node I` (ADR 0043 §2), a node hosting only
DynamoDB stream-shard tablets — no local control `RaftCore` (a `Metadata`
mirror, exactly like a data-only node), and no ordinary table data either.
The rationale is payload-profile separation (a stream shard's sequential
append/retention-trim access pattern versus a data node's mixed point
reads/writes), not a control-plane-scaling concern this ADR's own two roles
address — so this is a genuinely new axis of role separation, not a
subdivision of the existing `data` role. A **combined** node
(`animusd --cluster N`, or `--config FILE --node I` with no explicit role)
carries all three roles at once, so single-process dev clusters and
existing combined-mode deployments need no config change. Placement
segregation reuses the existing label/residency policy machinery
(`required_labels`, ADR 0005) rather than any new mechanism this ADR would
need to account for — a streams-role node is simply labeled at startup, the
same primitive that already keeps a data-only node's tablets off a
control-only node in the two-role case.
