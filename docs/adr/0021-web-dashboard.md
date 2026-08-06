# ADR 0021 — Web dashboard over the admin JSON surface (observe + operator actions)

- **Status:** Accepted (implemented — follow-ups 1–4, 7 shipped; 5 awaits ADR 0018, 6 on demand)
- **Date:** 2026-08-04 (status updated 2026-08-06)

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
   to the UI with confirmation is still to do.
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

This ADR builds directly on ADR 0020 (the admin JSON surface it renders and the
follow-up it fulfils), ADR 0018 (the transaction state it will visualize), ADR
0015 (the observe-only / aggregate-live discipline), and ADR 0003 (which it sits
*outside* of, as a `ProdEnv` I/O edge). The control plane (ADR 0001) remains the
metadata authority the dashboard reads.
