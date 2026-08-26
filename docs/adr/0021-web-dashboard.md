# ADR 0021 — Web dashboard over the admin JSON surface (observe + operator actions)

- **Status:** Accepted (implemented — follow-ups 1–4, 7–9 shipped; 5 awaits ADR 0018, 6 on demand).
  **Amended by [ADR 0053](0053-dynamodb-only-drop-cql.md) (2026-08-22):** the
  CQL wire adapter is dropped, and with it follow-up 4's CQL query panel
  (the `POST /admin/data/cql` proxy, `cql_client.rs`) and every other CQL
  reference below — the Write tab's DDL+DML editor is now Dynamo-only. Kept
  below as originally written, describing the dashboard as it was designed
  and shipped at the time.
- **Date:** 2026-08-04 (status updated 2026-08-14 — follow-up 9, the Streams tab)

## Context

ADR 0020 added a dedicated per-node admin HTTP/JSON listener — a complete,
determinism-clean introspection surface (config, full replicated `Metadata`,
both Raft layers, LSM/WAL/key-level storage internals, structured metrics,
health) plus a gated set of operator actions (split, flush, compact, reconfigure,
drain). It is consumed today only by `curl`/`jq` and the `animus admin`
CLI subcommands. ADR 0020 explicitly named the next step:

> *"A small web dashboard served on the admin port (static, self-contained) is a
> natural later addition now that the JSON exists."*

The motivation is **manual testing and debugging of a running cluster**. Once
cross-tablet transactions (ADR 0018) land, the system has enough moving
parts — per-tablet Raft groups, splits, placement reconciliation, MVCC versions,
2PC intents and transaction-status records — that reading them one `curl` at a
time across N nodes is slow and error-prone. A human bringing up `animusd
--cluster N` (or a per-process cluster) wants to *see*, in one place: which nodes
are up, where each tablet's leader sits, replication lag (`commit` vs `applied`
vs `durable` per group), the WAL tail and SSTable shape of a tablet, and the live
+ historical versions of a key — and to *drive* the operator actions (split a hot
tablet, force a flush/compaction, reconfigure a group, drain a node) and watch
the cluster react.

The forces that shape the design:

- **The data already exists as JSON (ADR 0020).** Nodes (`/admin/status`,
  `/admin/config`, `/admin/raft`), tablets (`/admin/status` map +
  `/admin/raftkv`), WALs (`/admin/storage/wal[/segment]`), and data
  (`/admin/storage/lsm`, `/admin/storage/key`) are all served. The dashboard is
  predominantly a **frontend**, not new backend introspection.
- **The admin surface is node-local by design (ADR 0020).** Each node serves only
  its own view — storage routes for a tablet this node does not host return 404;
  cluster-wide views are "assembled client-side by scraping each replica (same
  model as `/metrics`)." A dashboard inherently wants a **cluster** view, so the
  fan-out across nodes is the one genuinely new concern.
- **Determinism (ADR 0003) does not apply here.** The admin port is a
  `ProdEnv`-only I/O edge, exactly like the Dynamo/CQL listeners and `/metrics`;
  it never runs under `SimEnv`. A dashboard adds no determinism risk and needs no
  `Env`-seam discipline. (The *accessors* it ultimately reads already obey that
  discipline inside the `<E: Env>` crates — ADR 0020.)
- **No build toolchain, no new dependencies.** The repo is `cargo`-only with a
  hand-rolled HTTP server (`animusd/src/http.rs`, reused by `dynamo.rs` and
  `admin.rs`). Introducing a JS bundler / npm / a frontend framework would add a
  whole build ecosystem and a supply-chain surface for a debug tool. The
  dashboard must be **static and self-contained** — inline HTML/CSS/JS, served as
  bytes by the existing server — matching that precedent.
- **Operator actions are real mutations behind a port with no auth yet (ADR
  0020).** The dashboard exposing `POST` actions raises the stakes of the
  already-deferred auth question and demands explicit confirmation UX so a click
  can't silently split or drain.

This ADR settles: *whether* to build the dashboard, *what* it covers (read +
actions, per the scope decision), *how* it is served (static, self-contained, on
the admin port), and *how* it gets a cluster-wide view (client-side fan-out
first).

## Decision

**We will serve a static, self-contained, single-page web dashboard from the
admin port (ADR 0020) that visualizes the whole cluster (nodes, tablets, WALs,
data) read-only and exposes the gated operator actions behind explicit
confirmation. The cluster-wide view is assembled by client-side fan-out across
the per-node admin endpoints; a server-side aggregator is deferred until browser
reachability proves it necessary.**

### 1. Served self-contained from the admin port (two separable axes)

"Static" decomposes into two independent decisions; only the first is
load-bearing:

- **Deployment — self-contained, embedded, served from the binary (firm).** A new
  `GET /` (and `GET /admin/` / `GET /admin/ui`) on the admin listener returns a
  single self-contained HTML document, embedded in the `animusd` binary
  (`include_str!`) and written by the existing `http.rs` writer as `text/html`,
  with no external fonts/scripts/stylesheets and no CDN. This is what buys the
  properties that matter: **zero-install** (the dashboard is already at the admin
  port the moment a node is up — no second service to deploy alongside every
  node), **never version-skewed** against the JSON it renders (it ships with the
  node), and **no second runtime to operate**. This axis is firm.
- **Authoring — no build toolchain, but small vendored libraries are fine
  (open).** The repo is `cargo`-only; we will not add npm / a bundler / a
  framework build step (a second build ecosystem and a supply-chain surface, cf.
  `cargo deny` discipline). But "no toolchain" does **not** mean "no libraries":
  a single-file client-side library (e.g. Alpine, Preact + htm, htmx — ~10–15KB)
  may be **vendored** — committed as one pinned, auditable file and inlined into
  the page — keeping the binary self-contained and the build `cargo`-only while
  buying ergonomics over raw DOM code. The specific pick is left to
  implementation time; the constraint is only *vendored single file, no package
  manager, no bundler*.

In all cases the page is a client of the documented `/admin/*` JSON surface,
adding **no new server logic on the read path** beyond serving the asset.

**One steer for the library choice.** The admin surface returns **JSON** by
deliberate decision (ADR 0020: curl/`jq`/CLI-friendly, the shared contract).
Rendering therefore happens **client-side, from JSON** — which favors a
client-side reactive/templating lib (Alpine, Preact+htm) over a *hypermedia*
library like htmx, whose idiomatic model is the server returning **HTML
fragments**. Going fully-idiomatic htmx would mean the server also rendering HTML
fragments — a second representation of every endpoint in Rust, exactly the
new-server-read-path logic this section avoids. The **cluster fan-out reinforces
this**: the browser fetches from N nodes and *merges* before rendering (§2), which
is client-side data assembly, not htmx's one-request-one-swap. So: keep the
surface JSON-first and prefer a client-side render lib — unless we later decide
the dashboard warrants server-rendered HTML fragments, in which case htmx becomes
the natural fit.

### 2. Cluster-wide view via client-side fan-out (server aggregator deferred)

The dashboard's node inventory comes from `GET /admin/status` (the replicated
`Metadata` lists every member and the full tablet map — available from *any*
node). To assemble per-node detail (each node's Raft/storage/WAL view, which are
node-local), the **browser fans out** to each node's admin address and stitches
the results. Admin addresses are derived from the config/peer list the same way
the CLI resolves them.

This requires the browser to reach every node's admin port and the admin server
to permit cross-origin reads. We will:

- Add permissive **CORS** headers (`Access-Control-Allow-Origin`) on the
  read-only `GET /admin/*` responses, scoped to the admin listener only —
  acceptable because the port is already a trusted-interface debug surface (ADR
  0020 §"No auth yet") and is the prerequisite for any cross-node browser fan-out.
- Degrade gracefully: an unreachable node renders as "unreachable" in the grid
  rather than failing the whole view (a partitioned/dead node is exactly what the
  operator is looking at).

The ADR 0020 follow-up — a **server-side** `GET /admin/cluster/*` that the queried
node aggregates from peers — is **deferred**. It is the right answer if/when
browser-to-every-admin-port reachability is impractical (locked-down networks),
but it is strictly more server code and duplicates the fan-out the browser can do
for free against today's endpoints. We start client-side and promote to
server-side only on demonstrated need.

### 3. Scope: observe **and** operator actions, gated

**Read (the default view):**

- **Cluster / nodes** — a grid of members with `NodeStatus`
  (Active/Down/Leaving), roles, addresses, reachability, and the control-plane
  Raft summary (who is the control leader, term, `commit`/`applied`/`durable`).
- **Tablets** — the tablet map (key range, epoch, replica set) cross-joined with
  each replica's `/admin/raftkv` view: which node holds the leader, per-group
  term, and the `commit`/`applied`/`durable`/`snapshot` indices so replication lag
  is visible at a glance.
- **WAL** — per tablet/node: live segments with size + max seq, `durable_seq`,
  `rotation_count`; drill into a segment to see decoded `WalRecord`s (paged).
- **Data / storage** — per tablet/node: LSM level/SSTable shape (key & version
  ranges, file sizes, bloom, counters); a **key inspector** that shows every
  on-disk `(version, is_tombstone)` plus the live value for a queried key. Once
  ADR 0018 lands this is where transaction state surfaces — intents, the
  transaction-status record, MVCC versions by HLC commit timestamp (see §5).
- **Metrics / health** — the structured `/admin/metrics` snapshot and
  `/admin/health`, per node.

**Operator actions (gated):** the dashboard surfaces the ADR 0020 `POST`
actions — `tablet/split`, `storage/flush`, `storage/compact`,
`raftkv/reconfigure`, `drain` — each behind an explicit confirmation dialog that
restates the target and effect, and each rendering the action's returned
observable result (new tablet id, post-step voter set, …) inline so the operator
confirms without a manual re-query. Actions reuse the server-side routing/forward
logic ADR 0020 already built (the operator hits any node; the node forwards to the
authority); the dashboard adds **no** new mutation path.

### 4. Auth: localhost-only assumption, made explicit and visible

Exposing mutating actions through a click sharpens the deferred-auth question (ADR
0020 §"No auth yet"). This ADR does **not** add auth (still deferred to ADR
0020's follow-up), but:

- The dashboard, like the rest of the admin port, assumes the port is bound to a
  trusted interface (localhost / management interface). This is stated in-page (a
  visible banner) so the assumption is never silent.
- Auth in front of the admin port — and the `--admin-bind` interface override —
  remain the explicit prerequisite (now doubly so, because of write actions)
  before any non-localhost exposure. The dedicated-port choice (ADR 0020) is what
  keeps that follow-up clean; CORS is scoped to the admin port so it does not leak
  to the data listeners.

### 5. Sequencing relative to ADR 0018

This work lands **after ADR 0018** (cross-tablet transactions). The read panels
above are buildable against today's endpoints, but deferring lets the **data**
panel model transactions from the start rather than retrofitting: the key
inspector and a small transaction view show intents, the transaction-status
record (committed/aborted/pending), MVCC versions keyed by HLC commit timestamp,
and in-doubt/recovery state. ADR 0018's implementation should therefore keep its
new state reachable through the same `/admin/*` JSON shape (extend
`/admin/storage/key` with intent/version detail; add a transaction-status route)
so the dashboard consumes it the same way it consumes everything else.

### 6. Verification

The dashboard itself is untested by the determinism suite (it is a `ProdEnv` I/O
edge). What we test is the **asset is served** and the **endpoints it depends on
behave** — the latter is already covered by ADR 0020's per-node integration
tests. Add:

- A `ProdEnv` integration test that `GET /` returns the HTML asset with
  `text/html` and that `GET /admin/*` carries the CORS header (the fan-out
  prerequisite). Reuse the documented port-TOCTOU bring-up retry.
- Operator actions remain covered by ADR 0020's existing timeout-guarded
  `#[tokio::test(flavor = "multi_thread")]` action tests — the dashboard adds no
  new mutation path, so no new action test is required beyond the CORS/serve check.

The dashboard is a debug/operability tool, not a correctness surface; its bar is
"the JSON it renders is the tested JSON," not a new property to prove.

### 7. Health semantics: health ≈ "is the data at risk"

The cluster health pill/banner (`dashboard_core.js::computeHealth`) means **is
the data at risk**, not **is anything in transition**. A tablet mid-formation —
a split-child standing up its Raft group, a freshly-provisioned table's first
election, a rebalance/repair move catching up, or plain reconciler/admin-fan-out
lag — is not a data-risk state as long as every replica assigned to it is on a
live node: per ADR 0028, split/provision are each a single control-plane
command with no data-plane half (tablets are split-only, ADR 0044 — merge
briefly existed under this same shape, ADR 0033, before being removed), so
the data already sits safely in the source
replicas' shared storage engines the whole time the new group is forming. That
state renders as a neutral **`forming`** pill and does **not** degrade health —
otherwise every routine split would read as an outage.

What *does* mean the data is at risk: an assigned replica's node actually being
`Down` (**`under-replicated`** — redundancy genuinely reduced, repair pending)
or a tablet dropping below a quorum of live assigned replicas (**`quorum-lost`**
— the group can't commit, and one more failure loses data; always critical). A
lingering `Down` *member* that no tablet still depends on is, by the same logic,
not degrading by itself.

**Overdue-forming guardrail**: a formation that never converges — a stuck
election, a wedged reconciler — is a real problem and must not hide behind
"it's just forming" forever. The client tracks, per tablet, how long it has
been continuously observed `forming`; past 60 seconds it counts toward
`overdueFormingCount`, which *does* degrade health. This is plain
browser-side wall-clock state (`Date.now()`), not part of the deterministic
simulation surface (ADR 0003 scopes to `SimEnv`/`ProdEnv` Rust logic, not this
client-side SPA).

## Consequences

**Enabled:**

- One-glance manual testing of a live cluster: bring up `animusd --cluster N`,
  open the admin port in a browser, and see nodes, tablet placement + leaders +
  lag, WAL tails, SSTable shape, and key versions across the whole cluster —
  then split/flush/compact/reconfigure/drain from the same page and watch it
  react. This is the primary post-0018 manual-test workflow.
- Zero-install: the dashboard ships in the binary, needs no build step, no npm,
  no extra dependency, and works wherever a node runs.
- A natural home for ADR 0018's transaction state (intents, txn records, MVCC
  versions) and for future cluster views, all over the same JSON.

**Costs / risks knowingly accepted:**

- **CORS on the admin port.** Permissive cross-origin reads are required for
  browser fan-out and widen what the (still unauthenticated) admin port allows.
  Mitigated by scoping CORS to the admin listener and reaffirming the
  trusted-interface / localhost-only assumption — but it raises the priority of
  the deferred auth follow-up. Mutating actions exposed via a click make auth a
  hard prerequisite for any non-localhost exposure.
- **Client-side fan-out reachability.** The browser must reach every node's admin
  port; in a locked-down network it cannot, and the cluster view degrades to the
  reachable subset. The server-side aggregator (deferred) is the escape hatch,
  paid for only if needed.
- **A no-bundler SPA.** No build toolchain means a ceiling on UI sophistication —
  though a vendored single-file lib (Alpine / Preact+htm / htmx) takes most of the
  sting out without a package manager. Accepted: this is a debug tool, and the
  no-toolchain / no-supply-chain property is worth more than polish. If it
  genuinely outgrows a vendored lib, revisit then — not preemptively.
- **An embedded asset to keep current.** The dashboard tracks the `/admin/*`
  shape; an endpoint change must update the page. Mitigated by the page being a
  thin client of documented routes and by the serve/CORS integration test.
- **The admin port is now a data-write + DDL surface** (the Write tab, follow-up
  4): `POST /admin/data/{dynamo,cql}` run arbitrary DynamoDB ops and arbitrary CQL
  (including `CREATE`/`DROP`) against the live cluster, still with **no auth**. This
  is the consistent choice — one origin, one CORS surface, one future-auth boundary,
  and CQL *must* be server-proxied (no browser binary protocol) — but it makes
  auth-before-non-localhost-exposure a hard requirement, not a nicety, and is the
  strongest reason yet to land the deferred admin-auth follow-up. The proxies reuse
  the existing edges (Dynamo in-process; CQL via a loopback client to the node's own
  CQL port), so they introduce no new data path, only a new *entrance* to it.
  **Removed by ADR 0053** (2026-08-22): `POST /admin/data/cql` and the CQL
  loopback client are gone; the write surface is DynamoDB-only.

**Follow-up (each a green-keeping increment):**

1. ✅ Serve the static asset (`GET /` on the admin port, `include_str!`) + CORS on
   `GET /admin/*` + the serve/CORS integration test — the skeleton.
2. ✅ Read panels: nodes/cluster, tablets (map × raftkv with lag), built on
   client-side fan-out with graceful per-node degradation.
3. ✅ Read panels: WAL (segments + decoded records) and storage (LSM shape + key
   inspector + a browse-keys list, `/admin/storage/scan`).
4. ✅ **Data writes — a Write tab** (built ahead of the ADR-0018 sequencing, on
   request): DynamoDB CRUD and a full DDL+DML CQL editor, **proxied through the
   admin port** (`POST /admin/data/{dynamo,cql,drop-table}`). The Dynamo panel adds
   **table management** (list from the replicated catalog, create, drop) and
   restricts ops to a **dropdown of existing tables** (can't act on a non-existent
   table), with a **Form/JSON toggle** over one shared request model that syncs both
   ways and **prefills a selected table's key attributes** (partition + sort key,
   typed from the catalog — which now records declared key types, closing a gap where
   `CreateTable` dropped them) — Dynamo by reusing the edge in-process, CQL by driving the node's own CQL port as a
   loopback client (the browser can't speak the CQL binary protocol). This
   **extends the admin port into a data-write + DDL surface** — see the added
   consequence below. Operator actions (split/flush/compact/reconfigure/drain) wired
   to the UI with confirmation is still to do. **The CQL editor and its proxy
   are removed by ADR 0053** (2026-08-22) — the Write tab is Dynamo-only now.
5. Transaction view over ADR 0018's state (intents, txn-status record, MVCC
   versions) — co-developed with, or immediately after, ADR 0018's admin-JSON
   extensions.
6. (Deferred, on demand) server-side `GET /admin/cluster/*` aggregator if
   client-side fan-out reachability proves impractical.
7. ✅ **Real per-tab URLs.** Each top-level tab (Nodes/Tablets/Storage/Write) now
   has its own path, `/admin/ui/<tab>`; the admin server serves the same embedded
   SPA for any such path (`admin.rs::is_ui_path`, a prefix match — an unrecognized
   tab name falls back client-side to the default rather than 404ing), and the
   page uses `history.pushState`/`popstate` to keep the address bar in sync with
   the active tab. This closes the "refresh always resets to Nodes" gap: a
   refresh, a bookmark, or the browser back/forward buttons now land back on the
   tab that was open. Sub-tab state (the selected storage tablet/node, the Write
   panel's op/table) is intentionally **not** encoded in the URL yet — scoped out
   to keep this increment small; a future pass could promote it to query params
   the same way.

8. ✅ **The "AnimusDB Console" redesign.** The flat-tab debug dashboard (follow-ups
   1–4, 7 above) was replaced with a from-scratch visual/IA redesign implemented
   from a Claude Design mockup the user provided (project
   `f2b4c368-4267-4fd7-b4c6-e2775eb4ad0e`, file `AnimusDB Console.dc.html`) —
   still self-contained/no-toolchain (§1 unchanged), a **sidebar** of five views
   (Overview, Placement, Tablets, Data Browser, Storage) instead of a top tab
   row, and both a dark and a light theme (the mockup's `oklch()` palette,
   toggled and persisted client-side). Overview adds a health banner + stat
   tiles + a tablets-per-node balance chart; Placement is new (node cards +
   per-node tablet list, no resource gauges — see below); Tablets gets a
   filter + a detail panel with on-demand storage stats instead of the
   lanes/table toggle it superseded; Data Browser replaces the old Write tab's
   attribute-row form with a real Scan/Query/item-CRUD list+detail interaction;
   Storage folds in the old dashboard's WAL/LSM/key-inspector/browse-keys/
   bulk-seed tools unchanged, since the new design doesn't cover manual storage
   debugging at all. **Three things the source design showed are deliberately
   omitted, not faked**, because nothing in this codebase backs them: per-node
   CPU/mem/disk %, an activity/event log (distinct from OTel tracing and the
   `/admin/metrics/history` counter-snapshot ring buffer — see
   `animusd/CLAUDE.md`), and per-tablet election history (only current Raft
   state is tracked). This reaffirms §1's "no external assets" axis in a new
   way — the mockup's Google Fonts (`Inter`/`IBM Plex Mono`) links are not
   used; the console approximates them with system font stacks instead. No
   `/admin/*` JSON route changed; this remains a frontend-only follow-up.

9. ✅ **A Streams tab (ADR 0042/0043).** A sixth sidebar view (data-plane
   deployments only — combined/data, never control-only, since a
   control-only node hosts no CP data plane): a list of every currently
   `ENABLED` DynamoDB Stream plus any `DISABLED`-but-in-grace-window one
   (F12-b), per-node stream metric tiles (the console's first
   `/admin/metrics` consumer), and a detail panel showing the shard chain
   — the segment catalog (`status.stream_shards`) merged with a live
   `DescribeStream` call, grouped per tablet with each epoch's sequence
   range/record count/seal time/replicas — plus a live-tail poller
   (`GetShardIterator`/`GetRecords`, following `NextShardIterator`), all
   through the existing `POST /admin/data/dynamo` proxy. Health rendering
   follows §7 exactly: an open/unsealed shard is a neutral state, never a
   warning; the only warn signals are a real repair backlog, a trim-blocked
   node, or a seal failure. Enabling/disabling a table's stream is a
   **Data Browser** action instead (a per-table row next to that table's
   Indexes card), the same reasoning that already puts create/drop table
   there rather than in a dedicated tab. No new `/admin/*` JSON route —
   this rides the existing streams read API (ADR 0042 §3) through the
   admin proxy the same way the Data Browser already reaches the item API.

10. ✅ **The Streams tab also shows on control-only nodes.** #9's
    control-only exclusion was broader than the actual gap: a control-only
    node holds the full replicated `Metadata` (schemas incl. stream specs,
    and the `stream_shards` segment catalog), so the stream list and the
    shard-chain detail (`ListStreams`/`DescribeStream`, both pure functions
    of `Metadata`) render truthfully there — verified against a real split
    deployment, not assumed. Only the live-tail poller
    (`GetShardIterator`/`GetRecords`) genuinely needs a local CP data plane;
    it degrades in-view instead (a note + a `consoleLink` to a live
    data/combined node), rather than hiding the whole tab. See
    `docs/streams-notes.md`'s Console Streams tab section and
    `dashboard_streams.js`'s own doc for exactly which ops work from a
    control-only node's admin port and which don't (including a
    pre-existing backend gap this surfaced but left unfixed: `GetRecords`
    on a sealed shard panics via `ClientCtx::data()` there, and the
    open-shard path stalls ~10s before failing — both because that path
    assumes a local CP data plane a control-only node structurally never
    has).

This ADR builds directly on ADR 0020 (the admin JSON surface it renders and the
follow-up it fulfils), ADR 0018 (the transaction state it will visualize), ADR
0015 (the observe-only / aggregate-live discipline), and ADR 0003 (which it sits
*outside* of, as a `ProdEnv` I/O edge). The control plane (ADR 0001) remains the
metadata authority the dashboard reads.

## Amendment (2026-08-19, ADR 0052) — naming disambiguation

This dashboard is internally called "the AnimusDB Console" (this file's own
source comments, the page `<title>`) — a name [ADR 0052](0052-data-console-port.md)'s
**AnimusDB Data Console** now sits next to, on a different port, for a
different audience. The two are deliberately distinct: this dashboard stays
the **operator** surface — cluster health, placement, Raft, storage — on the
admin port, exactly as this ADR describes; the Data Console is an
**application-developer** surface — tables and items only, structurally
barred from showing anything this dashboard shows — on its own new port.
Neither renames nor merges into the other; see ADR 0052's own "Naming,
deliberately addressed" section for the full reasoning.

## Amendment (2026-08-25, ADR 0056) — branded "animusd admin"

This dashboard's own internal name — "the AnimusDB Console," per the
naming-disambiguation amendment just above — is retired as of the Ledger
design revision ([ADR 0056](0056-design-system.md)). It is branded
**animusd admin** now (page `<title>`, topbar wordmark, source comments):
the same **operator** surface this ADR describes — cluster health,
placement, Raft, storage, on the admin port — under a new name, not a new
or merged surface. Every "AnimusDB Console" reference in this ADR's own
body and in the amendment above is historical: read it as naming the
surface now called animusd admin. ADR 0052's data console is renamed
alongside it, to **animusd console** — see that ADR's own matching
amendment.
