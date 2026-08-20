# ADR 0052 — The AnimusDB Data Console: a separate app, a separate port

- **Status:** Accepted — PR1 shipped the plumbing only (§Decision "What this
  PR ships"); PR2 (see the 2026-08-20 amendment below) ships the tables-list
  screen and this listener's first JSON endpoint. The table page and the
  create-table form remain follow-up PRs in the same stack.
- **Date:** 2026-08-19
- **Amends:** [ADR 0035](0035-control-plane-separate-deployment.md) (owns the
  deployment shapes and their port contracts — gains a seventh port and a
  per-shape console-binding rule); [ADR 0021](0021-web-dashboard.md) (the
  existing "AnimusDB Console" dashboard the admin port serves — see
  §"Naming, deliberately addressed" for how the two consoles coexist without
  colliding).
- **Depends on:** [ADR 0047](0047-intra-node-port.md) — the direct precedent
  for splitting one more concern off onto its own port rather than
  co-tenanting an existing listener.

## Context

Every wire edge `animusd` serves today is built for one of two audiences:

- **Cluster operators**, on the admin port (ADR 0020/0021) — node health,
  Raft state, tablet placement, replication, storage internals, and a
  gated set of operator actions. This surface is *explicitly* cluster-shaped
  by design, and ADR 0020 states its trust model plainly: "No auth yet …
  Phase 1/2 assume the port is bound to a trusted interface."
- **Application clients**, on the DynamoDB/CQL ports (ADR 0006) — binary wire
  protocols a driver library speaks, not something a human opens in a
  browser.

There is no surface today for the audience in between: an application
developer who wants to look at *their own* tables and items — browse a
table, run a query, inspect an item, try a `CreateTable` shape — without
either speaking a driver's wire protocol by hand or being handed the
operator dashboard, which shows far more (and different) information than
they need and, worse, information they should not need to interpret at all
(is a tablet `forming`? is a replica `Down`? none of that is this
audience's problem, and surfacing it invites them to start treating
cluster-internal detail as part of their application's contract).

The maintainer's design (2026-08-19) is a new **AnimusDB Data Console** at
`/console`: a DynamoDB-shaped data app for that missing audience, built the
same way the operator dashboard is (a self-contained embedded page, no
bundler, no external assets) but deliberately a **separate application**
from it — different audience, different content, different trust boundary,
eventually a different auth story.

## Decision

### The console never shows cluster state

This is the console's one load-bearing rule, restated because everything
else in this ADR exists to protect it: **the console must never surface
cluster-shaped state** — no nodes, no replicas, no tablets, no Raft, no
quorum, no leaders, no placement, no health. If a future screen needs to
explain *why* a request is slow or unavailable, it explains that in
DynamoDB's own vocabulary (throttling, a transient error to retry) — never
by naming a tablet, a replica, or an election. An application developer's
mental model is "my tables and my items," the same one DynamoDB itself
gives them; leaking cluster mechanics into that view would make the console
a worse operator dashboard *and* a worse data app, at once.

### Its own port, not a co-tenant

Three listeners already exist that a console screen could theoretically
live on, and each is wrong for a different reason:

- **The admin port** — wrong trust boundary. ADR 0020 documents it as
  no-auth, trusted-interface-only. Serving an application-developer-facing
  surface there would either (a) inherit that same no-auth posture for an
  audience with no business being inside the cluster's trusted network, or
  (b) force auth onto the *whole* admin port to protect the console screens,
  which would break the deliberately-open-for-operators contract ADR 0020
  established. Neither is acceptable, and there is no clean partial-auth
  story on one TCP listener.
- **The DynamoDB port** — wrong protocol. It speaks `X-Amz-Target`
  JSON-over-HTTP for a driver library, not an HTML/CSS/JS app for a browser.
  Multiplexing an HTML shell onto a wire-protocol listener (the way
  `dynamo.rs` already multiplexes `GET /metrics` alongside the item API) is
  a reasonable trick for a single extra route; it is the wrong shape for an
  entire second application with its own asset routes and, eventually, its
  own auth.
- **A new route on an existing listener, generally** — wrong precedent. ADR
  0047 already answered this exact question for node-to-node RPC traffic:
  rather than teach one port to refuse some of its own traffic to some of
  its callers, give the new traffic class its own port and let the
  operator's network topology (firewall rules, a Kubernetes Service, an
  ingress) express "this audience reaches this port" directly. The same
  reasoning applies here, one level up the stack: **the port boundary is
  the trust boundary**, and that only works if request classes with
  different trust levels don't share a listener to begin with.

So the console gets a **seventh port**, `RoleAddrs::console` /
`NodeAddrs`-adjacent (see "What this PR ships" below for why it does *not*
in fact join the replicated `NodeAddrs` book) — a dedicated, currently-no-auth
listener whose only job today is serving the console's own assets, with a
real auth story to follow before it is ever exposed outside a trusted
network (see "Non-goals").

### Which deployment shapes serve it

ADR 0035 established three deployment shapes. The console is bound
deliberately, not uniformly:

- **Combined** (`--cluster N`, `--config`/`--node` with no explicit role) —
  binds it. A combined node hosts CP-data tablets, which is exactly the
  console's subject matter.
- **Data-only** (`animusd data`) — binds it, for the identical reason: a
  data-only node hosts real tablets and serves the DynamoDB/CQL edges
  already, so it is the natural home for the console too.
- **Control-only** (`animusd control`) — does **not** bind it. A
  control-only node hosts no CP-data tablet at all (ADR 0035 §"Target
  topology": "no storage engine, no `raftkv` env, no DynamoDB/CQL edges"),
  so there is structurally nothing for the console to show if it ran there
  — the same reasoning that already keeps the `dynamo`/`cql` listeners off
  a control-only node. Binding a listener with nothing behind it would be
  worse than the gap: it invites a caller to depend on a control-only
  node's console port "working" in some degenerate way, when the honest
  answer is that a control-only node simply isn't a place an application
  developer's request should ever land. An operator deploying a split
  cluster points console traffic at the data tier, exactly as they already
  point DynamoDB/CQL traffic there.

### What this PR ships (plumbing only)

This stack's first PR is deliberately narrow — the same "plumbing first"
shape ADR 0047 used:

1. **The port itself.** `RoleAddrs` gains `console: SocketAddr`, appended at
   stride offset 6 (`base_port + 7*i + {internal:0, client:1, dynamo:2,
   cql:3, admin:4, intra:5, console:6}` — the stride was already six as of
   ADR 0047; this makes it seven). No `#[serde(default)]`, matching
   `intra`'s own no-default convention (root `CLAUDE.md`: "No back-compat
   until further notice" — a clean break, not an oversight).
2. **A listener, bound on combined and data-only nodes only** (per the
   previous section), serving a **minimal, self-contained placeholder
   shell** — `include_str!`'d HTML/CSS, an asset route, and a deep-link
   prefix that returns the shell for any `/console/ui/*` path (mirroring
   the operator dashboard's own `admin::is_ui_path` shape). The page states
   plainly that it is the console and that its screens are not built yet.
   No bundler, no build step, no external fonts/CDN — the identical
   constraint ADR 0021 already lives under.
3. **No JSON endpoints of any kind yet.** This listener currently serves
   static bytes only — there is no code path on it that could reach
   `ClientCtx` or any cluster state even by accident (the serving module
   takes no `ClientCtx` parameter at all, a structural enforcement of the
   "never shows cluster state" rule above, not just a documented one).
4. **`gen-config` and every CLI entry point** emit/accept the new port.

Later PRs in this stack add the real screens (tables list, a table's items,
a create-table form), each reading through `ClientCtx`'s existing CP
primitives the same way `dynamo.rs`/`cql.rs` already do — DynamoDB-shaped
reads/writes, never a cluster-state read.

**Deliberately NOT done in this PR, and not planned for this port's
replicated identity:** the console address does **not** join
`Metadata.node_addrs`/`NodeAddrs` (ADR 0032's replicated per-node address
book). Every existing field there exists because some *other node* needs
to resolve it — `admin` for the dashboard's cross-node fan-out, `intra`
for node-to-node RPC relay, `client`/`internal` for routing and Raft
traffic. The console has no such consumer: it is a leaf listener a browser
dials directly, never a target another node forwards to or mirrors. Adding
it to the replicated book would cost every one of `RegisterNode`'s CAS,
`route_sync_loop`'s sibling, and the join-info wire payload real
maintenance weight for a field nothing reads. If a future console feature
needs to *discover* another node's console address (unlikely, given the
console is explicitly non-cluster-aware), that is the moment to revisit
this, not before.

### Naming, deliberately addressed

The operator dashboard (ADR 0021) is *also* called "the AnimusDB Console"
in its own source comments and page title — a naming collision worth
naming explicitly so a future reader doesn't assume they're the same
thing. This ADR's console is titled **"AnimusDB Data Console"** in its own
page `<title>` and module doc, and this document, ADR 0021, and ADR 0035
each cross-reference the other so the distinction is discoverable from
either side. The two are not merged, not renamed to converge, and not
expected to ever share a listener — see "Its own port, not a co-tenant"
above for why that would defeat the point of splitting them at all.

## Non-goals (this ADR)

- **Auth.** Neither the console port nor the admin port has authentication
  today. That is an accepted, explicitly-flagged prerequisite before the
  console is exposed to anyone outside operations — the same posture ADR
  0020 already states for the admin port, restated here because this
  listener's *intended* eventual audience (external application
  developers, not just trusted operators) makes the gap sharper. Nothing
  in this ADR's shape blocks adding auth later — a dedicated port is what
  makes a future per-port auth policy possible at all, exactly as ADR 0047
  observed for the intra port.
- **The console's actual screens.** Tables list, table detail, item
  CRUD/query, create-table — all follow-up PRs in this stack, each its own
  reviewable unit per the root `CLAUDE.md` convention ("one milestone /
  logical change per PR").
- **Read-your-writes/consistency semantics for the console's future reads.**
  Whatever the console reads later will ride the same CP primitives
  (`ClientCtx::cp_read`/`cp_scan`/…) every other edge already uses — no new
  consistency model is introduced by this ADR.

## Consequences

- Every deployment shape's port count grows: combined and data-only bind
  **seven** listeners now (was six, ADR 0047); control-only stays at five
  (internal, client, admin, intra — no dynamo/cql/console, unchanged from
  ADR 0035).
- `Node::shutdown()` frees a seventh port on the shapes that bind it.
- Slightly higher port-exhaustion pressure under `cargo test
  --workspace` parallelism (7 ports/node on combined/data-only instead of
  6) — the existing port-TOCTOU bounded-retry idiom (root `CLAUDE.md`,
  `docs/engineering-lessons.md`) already absorbs this; every test in this
  stack that brings up a real node uses it.
- No behavior change to any existing wire edge, the admin surface, or the
  operator dashboard — this is a pure addition.

## Amendment (2026-08-20, PR2 — the tables-list screen and endpoint)

This stack's second PR ships the console's first real screen (a dense
tables-list) and, with it, this listener's first JSON endpoint: `GET
/console/api/tables`. Two decisions worth recording, both direct
consequences of this ADR's original "never shows cluster state" rule
rather than new ones.

**The endpoint returns a console-shaped projection, not `/admin/status`.**
The obvious shortcut would have been to let the console read the same
`Metadata` the admin surface's `/admin/status` already serializes whole —
it already has everything a tables list needs (the schema catalog) sitting
right next to everything this console must never show (nodes, tablets,
replicas, Raft state, placement). That shortcut was rejected. The reason
is the trust boundary this ADR draws, not merely a rendering preference:
**cluster shape must never reach this browser at all, not just go
unrendered by today's client code.** A JSON payload the client happens not
to render today is still one dependency-bump or one "quick add a field"
commit away from a leak — the boundary has to be enforced at the wire, the
same way PR1 enforced "no `ClientCtx`" structurally rather than by
convention in `console.rs` itself. So the endpoint is backed by a narrow,
purpose-built projection instead: `console::TableSummary` (a name, typed
key names, GSI/LSI counts, and two stream/TTL booleans — see
`console.rs`'s own module doc), built by a `console::TableSnapshotFn`
closure `lib.rs` constructs and hands to `console::serve` at startup.
`console.rs` itself imports no `Metadata`/`TableSchema`/`IndexKind`/schema-
catalog type at all; the one function that reads the schema catalog on the
console's behalf (`lib.rs::console_table_summaries`) lives outside this
module, so there is no cluster-shaped value ever in scope inside it to
serialize by accident. The same discipline applies to every future console
endpoint this stack adds — a new screen's data need is a new narrow
projection type, never a wider handle back toward `ClientCtx`/`Metadata`.

**Per-table item count and size are deliberately absent.** Both are
things a real DynamoDB console shows, and both were considered for this
screen. Neither exists today except as a client-side fan-out over every
node's `/admin/raftkv` (summed across replicas, tablets, and nodes) — which
would mean the console's very first JSON response leaking exactly the
cluster shape this whole ADR exists to hide, just to answer a size
question. The maintainer's call: these stay out until a real server-side
rollup exists (a single aggregate the control plane or a leader computes
and the console can read without seeing a single tablet), not before. This
is a scope decision, not an oversight — do not add a fan-out to
`/admin/*` from `console.rs`/`lib.rs`'s console-facing code to backfill
these two fields.
