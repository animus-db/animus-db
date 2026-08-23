# ADR 0052 — The AnimusDB Data Console: a separate app, a separate port

- **Status:** Accepted — PR1 shipped the plumbing only (§Decision "What this
  PR ships"); PR2 (see the 2026-08-20 amendment below) ships the tables-list
  screen and this listener's first JSON endpoint; PR3 (see the second
  2026-08-20 amendment below) ships a table's own page, Config tab; PR4 (see
  the third 2026-08-20 amendment below) ships that page's Items tab; PR5
  (see the fourth 2026-08-20 amendment below) ships that page's third and
  final tab, Stream data; PR6 (see the fifth 2026-08-20 amendment below)
  ships the create-table form, completing the console's screens — this
  stack has no further follow-up PR planned. **Amended by [ADR 0053](
  0053-dynamodb-only-drop-cql.md) (2026-08-22):** the CQL wire adapter and
  its port slot are dropped, so the "seven-port stride" this ADR
  established is now six again, differently composed; see the amendment
  note at the end of this document.
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

## Amendment (2026-08-20, PR3 — the table page's Config tab)

This stack's third PR ships the first screen that **mutates** anything: a
table's own page, Config tab — its key schema (read-only), its GSIs
(addable/droppable), its LSIs (read-only, create-time-only), its stream
(enable/disable/view-type), its TTL (ADR 0051, enable/disable/attribute),
and table deletion. Two decisions worth recording.

**The injected seam widens from one closure to a small `ConsoleBackend`
trait, but never in *kind*, only in *shape*.** PR2's `TableSnapshotFn` (a
bare `Fn() -> Vec<TableSummary>`) is exactly right for a read that takes no
parameters and cannot fail; it stays, untouched, for the tables list. The
Config tab's six operations (`table_detail`, `add_gsi`, `drop_gsi`,
`set_stream`, `set_ttl`, `delete_table`) each need a table name (and
sometimes a request body) and can fail, so a single closure's shape no
longer fits — an `async_trait::async_trait` trait
(`console::ConsoleBackend`) does instead. The trait lives in `console.rs`
itself, and every one of its method signatures is built entirely from
plain, owned console types this PR adds alongside it (`TableDetail`,
`GsiDetail`, `LsiDetail`, `AddGsiRequest`, `SetStreamRequest`,
`SetTtlRequest`, `ConsoleError`) — never `ClientCtx`, `Metadata`,
`TableSchema`, `IndexDef`, or any other cluster/schema-catalog type.
`console.rs` imports none of those types before this PR and imports none
of them after it either; only `lib.rs` (the trait's one implementor, on
`ClientCtx`) ever has a schema-catalog type in scope while building a
method's return value. The load-bearing property PR2 established — "the
console never shows cluster state" enforced by what a module can even
import, not by convention — survives the widening exactly because the
widening happened in the *number and asynchrony* of the seam's operations,
never in what crosses it.

**Every mutation reuses the real DynamoDB wire path — `crate::
dynamo::execute_routed` — instead of a parallel one, except the one
operation that isn't a DynamoDB operation at all.** Adding/dropping a GSI
and enabling/disabling/reconfiguring a stream are exactly `UpdateTable`
calls (`GlobalSecondaryIndexUpdates`/`StreamSpecification`); setting TTL is
exactly `UpdateTimeToLive`. `lib.rs`'s `ConsoleBackend` impl builds the
identical JSON body a real DynamoDB client would send and calls
`dynamo::execute_routed` — the same function the real DynamoDB listener's
`dispatch` and the operator dashboard's `POST /admin/data/dynamo` already
call — rather than re-deriving `MetaCommand::CreateTableIndex`/
`SetTableStream`/`SetTableTtl` proposals directly. This is not merely
convenient: it means the Config tab inherits every validation rule, every
commit-wait discipline, and every future fix to those code paths for free,
and it means a bug fixed once in `dynamo.rs`'s `create_index`/`drop_index`/
`update_time_to_live` is fixed for all three callers (the wire edge, the
admin dashboard, and now the console) rather than needing to be
independently rediscovered in a console-only reimplementation. **Table
deletion is the deliberate exception**: DynamoDB itself has no
`DeleteTable` in this adapter's supported subset (see `crates/animus-dynamo/
CLAUDE.md`), so there is no wire path to reuse — `delete_table` instead
calls the same `ClientCtx::drop_table` the admin dashboard's own
`action_drop_table` (`admin.rs`) calls, the console's equivalent of that
existing admin-only primitive, exactly as this ADR's "What to implement"
brief anticipated.

A GSI and an LSI are rendered from **different types** (`GsiDetail` carries
a lifecycle `status` and its own hash attribute; `LsiDetail` carries
neither) rather than one shared row shape with optional fields — an LSI is
a scope inside the table's own storage, not a separate materialized table,
and has no lifecycle to report; collapsing the two into one type would let
a future change accidentally add a "drop" affordance or a status pill to an
LSI row, which is not a real DynamoDB operation. The UI mirrors this at the
template level, not just the type level (`console.js`'s `gsiRowHtml`/
`lsiRowHtml` are two separate functions).

### An index key attribute's type is nullable, and the Add-GSI form asks for none

A base table's own partition/sort key always has a declared `S`/`N`/`B`
type: `CreateTable` requires it in `AttributeDefinitions` and the schema
bridge turns those into the table's `ColumnDef`s. An **index** key
attribute does not. `animus_control::IndexDef` stores only the attribute
*name*, and this adapter's `UpdateTable` decoder for
`GlobalSecondaryIndexUpdates` never reads `AttributeDefinitions` at all
(issue #319) — so a GSI added to a live table has key attributes with no
type recorded anywhere, while a GSI declared at `CreateTable` time on the
same table does.

The console models that difference instead of hiding it. `KeySummary`
(base-table keys) keeps `attribute_type: String`; `IndexKeySummary` (GSI/LSI
keys) uses `Option<String>`, and `console.js` renders `None` as a bare
attribute name rather than a parenthesised type. The Add-GSI form asks for
attribute *names* only — an earlier draft offered an `S`/`N`/`B` picker per
key, which the backend faithfully forwarded and the decoder silently threw
away, so the value came back as the fallback `S` no matter what was chosen:
a control that cannot survive its own round trip. `add_gsi` correspondingly
sends no `AttributeDefinitions`, since sending them would suggest they were
recorded.

Restoring the picker is the natural follow-up to #319, not a separate design
question — this ADR's position is only that the console must not claim a type
nobody stored.

## Amendment (2026-08-20, PR4 — the table page's Items tab)

This stack's fourth PR ships the table page's second tab: browse (`Scan`,
paginated) or narrow (`Query`, by partition key and optionally a sort-key
condition) a table's own rows, or one of its declared GSIs'/LSIs'; look up
one row directly by its exact key (`Get`, base table only — real DynamoDB's
own `GetItem` has no `IndexName`); and create/edit/delete one item at a
time. Three decisions worth recording.

**The tab strip is real navigation, not a shared-page pushState toggle.**
The operator dashboard's own tab idiom (`dashboard_core.js::activateTab`)
keeps one already-fetched status blob in memory and toggles which
`<section>` is visible, syncing the URL with `history.pushState`/
`replaceState` — the right shape there because every tab reads the same
data. This console already established a different, deliberate convention
for cross-screen navigation (this ADR's PR2/PR3 text, restated in
`console.js`'s own header comment): real `<a href>`s and full page loads,
reserving a same-page anchor nav for genuinely-one-fetch subsections (the
Config tab's own Settings/Indexes/Danger-zone jump nav). Config and Items
are not that: Items makes its own `scan`/`query` calls that Config never
touches, so treating them as one shared-fetch page and toggling visibility
would be modeling two different data screens as one. PR4 gives Items its
own real URL, one path segment past the table's own
(`/console/ui/tables/{name}/items`, vs. plain `/console/ui/tables/{name}`
for Config) — both still served by the identical static shell
(`console::is_shell_path` already matches any `/console/ui/*` prefix, so no
server route changed), and `console.js`'s router picks the tab from the
trailing path segment. This also makes "Config is the default tab" a
structural fact of the URL space rather than client state that could start
on the wrong tab: the tables-list row's link (`tableHref`, unchanged) always
lands on the bare table URL, which the router treats as Config.

**Items pass DynamoDB's own wire shape straight through — the one console
type in this whole app that is not a narrower projection.** Every other
type this module defines (`TableSummary`, `TableDetail`, `GsiDetail`, …)
deliberately narrows the replicated catalog into a console-only shape, for
the reasons ADR 0052's PR2 amendment gives. An item has no such catalog
to narrow *from*: DynamoDB rows are schemaless beyond their declared key
attributes, so there is no fixed "console item shape" to project onto
without either inventing a lossy one (dropping attribute types, flattening
nested `L`/`M` structures) or reinventing DynamoDB's own type system badly.
`console::WireItem` (`{"attr": {"S": "value"}}`, the same shape a real
DynamoDB client already sees) is passed through by every one of the five
new `ConsoleBackend` methods (`scan_items`/`query_items`/`get_item`/
`put_item`/`delete_item`); `console.rs` never interprets an attribute name
or value, only moves the map between the wire and the HTTP body.
`console.js`'s item editor is where this pays off directly: every DynamoDB
wire type (`S`/`N`/`B`/`BOOL`/`NULL`/`L`/`M`/`SS`/`NS`/`BS` — a real closed
set) gets its own editor or, for the four collection types, a "Raw (JSON)"
textarea holding that one attribute's exact `AttributeValue` — never a
partial recursive editor invented for this PR, and never a value the
system did not actually record (this PR's own version of the rule PR3's
`IndexKeySummary`/issue #319 fix already established). An attribute *name*
stays the one genuinely free-text control in the whole tab, same rule as
everywhere else in this console.

**`Query` has no pagination on this adapter, and PR4 does not add it.**
`Scan` paginates properly — DynamoDB's own `Limit`/`ExclusiveStartKey`/
`LastEvaluatedKey` contract, threaded through unchanged
(`console::ScanItemsRequest`/`ItemsPage`), with a "Load more" button
carrying the previous page's `last_evaluated_key` forward, never a fake
offset. `Query`, on inspection, cannot: `animus_dynamo::wire::decode_query`
never parses a `Limit` or `ExclusiveStartKey` at all, and `animusd::dynamo::
run_query` answers a whole partition in one native range scan with no cap.
This is a pre-existing gap in the underlying wire layer that PR4 did not
introduce and does not attempt to paper over client-side (a client-side-only
"Load more" would silently re-issue the identical unbounded query and either
return nothing new or duplicate the same rows) — `console::ItemsPage::
last_evaluated_key` is simply always `None` for a `Query` result, and
`console.js` never shows a "Load more" affordance in Query mode. A `Query`
is scoped to one partition, which is normally small, so this is a real but
minor gap; giving the DynamoDB `Query` operation itself real pagination is
its own follow-up in `animus-dynamo`/`animusd::dynamo`, not a console-side
fix.

One extension included because it fell out cleanly rather than needing new
machinery: scanning/querying a named GSI or LSI. `ScanItemsRequest`/
`QueryItemsRequest` both carry an optional `index_name` (a real closed set —
`console.js`'s Source `<select>` is populated from this same table's own
`TableDetail.gsis`/`lsis`, never free text), and `lib.rs`'s
`ConsoleBackend::query_items` resolves the partition/sort attribute *names*
to query by from the replicated catalog server-side (mirroring how
`add_gsi`/`table_detail` already read the catalog on the console's behalf)
rather than asking the client to know or type them — the same
`GET`/`POST`-body split every other DynamoDB operation on this adapter
already uses for `Scan`/`Query`'s own `IndexName` parameter.

## Amendment (2026-08-20, PR5 — the table page's Stream data tab)

This stack's fifth PR ships the table page's third and final tab: a table's
DynamoDB Streams shards, and the records inside them — built on the real
`ListStreams`/`DescribeStream`/`GetShardIterator`/`GetRecords` wire
operations (`crate::dynamo::execute_routed(self,
"DynamoDBStreams_20120810.<Op>", ..)`, the exact same function every
mutating Config-tab endpoint already calls for the `DynamoDB_20120810.*`
service, ADR 0052 PR3's own "reuse the real wire path" rule extended to the
sibling Streams service). Tab order and wording, per the maintainer's exact
spec: **Config, Items, Stream data**. Same routing idiom as PR4
(`/console/ui/tables/{name}/stream`, a real route, not a same-page toggle) —
Config/Items/Stream data are three genuinely different data-fetching
screens, the same reasoning PR4's own amendment already gave for Items, and
nothing about adding a third tab changes it.

### This is the PR where "never show cluster state" gets genuinely sharp

Every earlier tab's "no cluster shape" argument was easy: a `TableSummary`/
`TableDetail`/`WireItem` simply has no tablet/replica/node field to leak in
the first place. The Stream data tab is different, and worth reasoning
about explicitly rather than just asserting a choice, because **a DynamoDB
Streams shard is *implemented* as a seal epoch of one tablet's own change
log** (ADR 0042 §2, ADR 0043): `ShardId` is literally
`shardId-<tabletId>-<epoch>` (`animus_cp_data::segment::shard_id`). So the
question this PR actually has to answer isn't "does this field mention a
tablet" — it's "does this field, even though it never says the word
`tablet`, still let a viewer reconstruct cluster shape."

**The shard id itself, and its `ParentShardId` lineage, are surfaced
anyway — deliberately, not by oversight.** The reasoning: a shard id is
**DynamoDB's own public wire contract**, not this console's invention. A
real DynamoDB client receives exactly this string from `DescribeStream` and
passes it straight back to `GetShardIterator`; an AWS user routinely copies
a shard id out of the console or a log line to debug a consumer. The
console's whole reason to exist is to give an application developer the
same debugging vocabulary a real DynamoDB client already has (this ADR's
original "an application developer's mental model is 'my tables and my
items,' the same one DynamoDB itself gives them" — a shard id is squarely
inside that mental model, DynamoDB's own, not a peek behind it). Refusing
to show it would not protect the cluster-shape boundary; it would just make
the tab useless for the one workflow it exists to support ("why did my row
vanish, which shard is it in, let me read that shard's records"), while a
motivated reader could still infer the same tablet/epoch facts from timing
or from the admin port anyway if they had operator access — the boundary
this ADR protects is *audience*, not *obfuscating public identifiers a real
client already gets*.

**What stays off every response, and why it's structurally impossible to
add by accident.** None of `ConsoleBackend`'s three new methods
(`stream_shards`/`get_shard_iterator`/`get_stream_records`) or their
request/response types (`ShardSummary`, `StreamShardsPage`,
`GetShardIteratorRequest`, `GetStreamRecordsRequest`, `StreamRecordsPage`)
have a `TabletId`/`NodeId`/replica-set field anywhere in their signatures —
`console.rs` still imports no such type (same structural enforcement PR1
established, restated here because Streams is exactly the subsystem where
it earns its keep). Concretely, the things a shard id's digits do **not**
buy a viewer, because nothing here ever computes or forwards them:

- **Which node hosts the tablet, or which node is its Raft leader.** A
  shard id names a tablet's own identity, never its current placement —
  placement is looked up separately (`Metadata.tablets`/`node_addrs`), and
  no code path in this PR ever touches that lookup.
- **How many replicas back it, or their addresses.** A seal's own catalog
  row (`StreamShardRow`) carries `replicas`/`object_id` — genuinely
  storage-internal detail the durability/superset-slice machinery (ADR 0042
  §9/§10) depends on — and neither field is read by, or reachable from, any
  method this PR adds.
- **Whether the tablet is currently leaderless, mid-election, or
  mid-split.** None of that state is in scope for a `DescribeStream`/
  `GetRecords` call to begin with; this PR adds no new read of it.

The practical test applied to every field before it was added: *would a
real, unmodified DynamoDB client ever see this on the wire?* A shard id,
its parent lineage, a sequence number, a view type, a stream ARN, a
record's `eventName`/`Keys`/images/`userIdentity` — yes, all of them,
verified against this ADR's own compatibility target. A tablet's node
assignment, replica count, or leadership state — never; DynamoDB Streams
has no such concept for a client to see, and this adapter's console
doesn't invent one to show it either.

### The no-stream-enabled answer is data, not an error

A table with no stream is the common case (this ADR's own PR4 precedent:
"an application developer who wants to look at *their own* tables" — most
tables, most of the time, have no stream turned on). `stream_shards`
returns a plain `200` with `enabled: false` and an empty shard list, never
a `404`/`ConsoleError` — the same "found-or-not-found 200" discipline PR4's
`get_item` established for a missing key, generalized here to a missing
*feature* rather than a missing *row*. `console.js` renders this as a
plain message pointing at the Config tab's Settings section (where the
stream toggle actually lives), never an empty grid with headers and no
rows that would read as broken rather than simply off.

### Paging: the shard list gets `DescribeStream`'s own real pagination; records get the honest `NextShardIterator` walk

Unlike PR4's `Query` gap (no pagination existed to thread through at all),
`DescribeStream` genuinely does paginate on this adapter
(`ExclusiveStartShardId`/`LastEvaluatedShardId`, ADR 0042 §3 — "a busy
tablet churns roughly a shard a seal-age interval," so a long-lived
streamed table's shard count is a real, unbounded-over-time list). PR5
threads that through rather than fetching every page server-side in a
loop: `GET .../stream/shards[?exclusive_start_shard_id=...]`, with a "Load
more shards" button carrying the previous page's own returned
`last_evaluated_shard_id` forward — a flat string, so (unlike `Scan`'s
`ExclusiveStartKey`, a nested `AttributeValue` object) it fits cleanly in a
query parameter rather than needing `Scan`'s `POST`-with-a-body shape.

A shard's own records page over `GetShardIterator`/`GetRecords`'s real
`NextShardIterator` contract — the record-viewer equivalent of PR4's
`ExclusiveStartKey` walk, and just as load-bearing to get right: "Load more
records" always resends the previous page's own returned iterator, never a
client-computed position. A `null` `next_shard_iterator` (real DynamoDB's
own "this shard is exhausted" signal, ADR 0042 §2/§6 — which, per §7's own
"never invalidates an open-shard iterator" contract, only a **sealed**
shard's iterator actually reaches; an open shard's `GetRecords` always
returns a non-null iterator, "nothing new yet, poll again") renders as
"shard drained" rather than being silently treated the same as "poll
again later."

### The iterator-type control is a real closed-set control; a sequence number stays free text

Per this repo's standing design rule ("never offer a closed picker for
something that is genuinely free text, and never a free-text guess at a
genuinely closed set"): `ShardIteratorType` is one of exactly four values
(`TRIM_HORIZON`/`LATEST`/`AT_SEQUENCE_NUMBER`/`AFTER_SEQUENCE_NUMBER`,
`animus_dynamo::streams_wire::ShardIteratorType`'s own closed enum), so
`console.js` renders it with the same segmented control the Config tab's
stream-view-type picker already uses — never a text input a typo could
silently misroute. A sequence number (required only for the `AT_`/`AFTER_`
pair) stays a plain text input: it is a genuine value, not a member of a
small fixed set — the decimal packed-HLC string (ADR 0042 §5) a developer
copies out of an already-shown record's own `Sequence #` column, the exact
same "closed set gets a real control, a value stays text" line this ADR's
PR3/PR4 amendments already drew for stream view type vs. attribute name and
for `SortKeyQuery`'s three shapes vs. a key's raw value.

### `userIdentity` is real and worth surfacing — verified before rendering, not assumed

ADR 0051 §7 documents a TTL-reaper delete's stream record carrying
`userIdentity: {"PrincipalId": "dynamodb.amazonaws.com", "Type":
"Service"}` at the record's own top level. Before rendering it, this PR
verified the field is genuinely populated end-to-end — `ChangeRecord::
ttl_expired` set by the reaper's own kind-write path, threaded through
`streams_wire::stream_record_json` (present only when `ttl_expired`, absent
for every ordinary client write) — rather than assuming the ADR prose alone
proved it wired up. Confirmed both by the pre-existing unit test
(`streams_wire::tests::stream_record_json_carries_service_user_identity_
for_a_ttl_delete`) and by this PR's own end-to-end regression
(`console_stream.rs::ttl_deletion_carries_the_service_user_identity_
through_the_console`, which forces a real TTL expiry + reap and reads the
resulting `REMOVE` record's `userIdentity` back through the console port).
`console::StreamRecordsPage::records` passes DynamoDB's own `Record` wire
shape straight through unprojected — the same "no fixed console shape to
project onto" call PR4's `WireItem` already made, for the identical reason
(a stream record has no catalog to narrow from any more than an item
does) — so `userIdentity` reaches `console.js` exactly as the real wire
produces it; `console.js` renders its presence as a small "TTL expiry"
badge next to the event pill, exactly the fact a developer debugging "why
did my row vanish" needs and exactly the field real DynamoDB Streams uses
to convey it.

### Scope: the current stream only, deliberately, not the disable-grace-window pair

ADR 0042 §4/§11 (F12-b) lets a just-disabled stream and a freshly
re-enabled one coexist in `ListStreams`/`DescribeStream` for as long as the
old label's catalog rows haven't yet aged out — real wire-compatibility
behavior a driver library must handle correctly. The Stream data tab
deliberately does **not** surface that pair: `stream_shards`/
`get_shard_iterator` resolve against `Metadata::table_stream`, which is
`Some` only for a table's *current* enabled stream, `None` the instant it's
disabled (grace window or not). This is a scope decision, not a
rediscovered gap — the console's audience is a developer debugging their
*currently enabled* stream, and a browsable "here's your stream from two
enables ago, mid-retention-sweep" view is a real but genuinely separate
feature with its own UI questions (which of possibly several labels is
"the" one to show first?) that nothing in this PR's brief asked for. The
raw wire's own grace-window behavior is unaffected — a driver library
hitting the DynamoDB Streams service directly still gets the full
ADR 0042 §4/§11 contract; only this console tab's own view is narrower.
Revisit if a future task specifically asks for it.


## Amendment (2026-08-20, PR6 — the create-table form)

This stack's sixth and final PR ships the console's last screen: the
create-table form behind the tables-list screen's own `+ New table` button
(`/console/ui/tables/new`, previously a "not built yet" stub). One new
endpoint, `POST /console/api/tables`
([`console::ConsoleBackend::create_table`]), covers the whole of DynamoDB's
`CreateTable` surface this adapter supports in one call: table name, the
partition key (name + a real `S`/`N`/`B` type control), an optional sort key
(same shape), any local secondary indexes, any global secondary indexes
(each with its own hash key, optional sort key, and projection), a stream,
and TTL — then navigates to the new table's own page on success. Same
discipline as every PR before it: the request/response types
(`CreateTableRequest` and its nested `CreateKeyAttribute`/`CreateLsiRequest`/
`CreateGsiRequest`) are plain owned console types, and the endpoint reuses
the real `CreateTable`/`UpdateTimeToLive` wire operations via
`crate::dynamo::execute_routed` rather than a second write path (TTL is a
separate follow-up call, same shape the Config tab's `set_ttl` already
uses, because `CreateTable`'s own wire operation carries no TTL field at
all).

### LSIs are declarable *only* on this form, and that is a structural fact, not a policy choice

A DynamoDB local secondary index is create-time-only: it shares the base
table's own partition key, adds one alternate sort key, and can never be
added or dropped after the table exists — there is no `CreateLocalSecondaryIndex`/
`DeleteLocalSecondaryIndex` operation in DynamoDB at all, and this adapter
does not invent one either (`animus_control::IndexKind::Local` GSI/LSI
distinction; ADR 0045's whole convergent-add/drop cascade is GSI-only). So
the create-table form is not merely *a* place an LSI can be declared — it
is structurally the *only* place: `console::ConsoleBackend` has no
`add_lsi`/`drop_lsi` method, and adding one would misrepresent DynamoDB's
own contract. `console::CreateLsiRequest`'s own doc states this plainly for
a reader who lands there without this ADR in hand.

### What tracing `CreateTable`'s own decoder found about index key attribute types — a second instance of the #319 gap, previously untraced

ADR 0052 PR3's own amendment already established that a GSI added through
`UpdateTable` gets no recorded key-attribute type (issue #319, because
`animus_dynamo::wire`'s `GlobalSecondaryIndexUpdates` decoder never reads
`AttributeDefinitions`) — and its prose asserted, without having actually
traced the `CreateTable` path, that "a GSI declared at `CreateTable` time on
the same table does" get one. This PR traced it, per this ADR's own
brief and the root `CLAUDE.md`'s standing rule ("never offer a control
whose value cannot survive its own round trip — verify against the decoder,
not against a sibling operation's contract"), and **that assertion turns
out to be wrong**: `CreateTable`'s own decoder has the identical gap, for a
structurally identical reason.

Tracing `animus_dynamo::wire::decode_key_schema`/`decode_attribute_types`
and the `animus_dynamo::schema` bridge confirms it precisely:
`decode_attribute_types` does parse every entry of a `CreateTable`
request's `AttributeDefinitions`, indexes and all — but `schema::to_control`
(the function that turns those `(name, type)` pairs into the replicated
catalog's typed `ColumnDef`s) builds a `ColumnDef` **only** for the base
table's own `schema.partition_key`/`schema.sort_key`; every other entry in
`key_types`, including one naming a GSI's or LSI's own key attribute, is
silently never consulted. Separately, `schema::index_to_control` — the one
function that turns a decoded `SecondaryIndex` (GSI or LSI) into the
replicated `IndexDef`, called identically for every index a `CreateTable`
declares — never receives `key_types` as a parameter at all, and
`animus_control::IndexDef` itself has no type field to put one in even if
it did. So an index's key attribute has **no recorded type anywhere in the
catalog, regardless of whether the index was declared via `CreateTable` or
added later via `UpdateTable`** — the only way `console_index_key_summary`
(PR3's own `Option`-typed projection) ever resolves to `Some` for an index
key attribute is the structural coincidence where that attribute's name is
*also* the base table's own declared partition or sort key — true of every
LSI's hash attribute (always, by construction — an LSI shares the base
partition key), never true of an LSI's own alternate sort attribute or of
an ordinary GSI's hash/sort attributes.

The create-table form is built honest about this finding: `CreateGsiRequest`/
`CreateLsiRequest` ask for index key attribute *names* only, exactly the same
"no control whose value is discarded" call PR3 already made for the
Add-GSI form, now confirmed to hold on the create path too.
`crates/animusd/tests/console_create_table.rs::create_full_table_declares_everything_exactly`
is the regression: it declares a GSI and an LSI at `CreateTable` time with
key attribute names deliberately distinct from the base table's own (so
nothing resolves a type by name coincidence), then re-fetches the table
fresh through `GET /console/api/tables/{name}` and asserts every one of
those index key attributes reads back `attribute_type: null` — the create-path
sibling of the test PR3's own regression already runs for the `UpdateTable`
path. This ADR's text above is corrected to match; issue #319's restoration
of the picker, once it lands, should cover both paths (`CreateTable` and
`UpdateTable`) rather than only the one originally filed against.

One thing genuinely *does* survive a `CreateTable`-declared index intact,
unlike a key attribute's type: its **projection**
(`ALL`/`KEYS_ONLY`/`INCLUDE` + non-key attribute list). `decode_index_entry`
parses `Projection` for every declared index (GSI or LSI) regardless of
kind, and `schema::index_to_control`/`projection_to_control` carry it
through to the replicated `IndexDef` untouched — a genuinely closed,
durable set, unlike an index key attribute's type. So the create-table
form's GSI projection control (a real `ALL`/`KEYS_ONLY`/`INCLUDE` segmented
control, with a free-text non-key-attribute list shown only for `INCLUDE`)
is not the same defect the type picker would have been — it is added here,
along with a new `console::ProjectionSummary` field on `GsiDetail` (console-
shaped: `projection_type` + an `Option` non-key-attribute list), so the
Config tab's own GSI rows now render a projection too, for every GSI
regardless of how it was declared. LSIs get no projection control on this
form: DynamoDB defaults an unspecified `Projection` to `ALL`, and nothing
in this PR's brief asked for one — `LsiDetail` is left unchanged.

### The sort-key-gates-the-LSI-section flow, and the maintainer correction it exists to satisfy

Direct feedback on an earlier round of this exact form: *"I don't see a way
to add LSIs in the table creation form."* The earlier draft's mistake was
subtle — gating the LSI section on the sort key being present is *correct*
(an LSI structurally needs one), but the sort-key toggle defaulted **off**,
so a blank form opened with the LSI section permanently blocked and no
visible way to turn it on short of scrolling back up and guessing. The fix
is not to remove the gate — it is to make the gated state never the
*starting* state and, if reached anyway, never a dead end: the sort-key
toggle now defaults **on** (`console.js::renderCreateTableForm`), so a
blank form always has a live, immediate path to declaring an LSI; if a user
manually switches the sort key off while LSI rows are still present, the
LSI section's own blocked message points back at that same switch, which
stays visible and live directly above it (never hidden behind another
screen or a modal) rather than merely graying the section out with no
stated way forward. The reviewer's second correction on the same earlier
round — *"buttons to enable streams and TTL are weird"*, a two-chip
`ENABLED`/`DISABLED` segmented pair used for a plain boolean where
`console.js`'s existing `toggleSwitch` helper (added for the Config tab's
own TTL/stream toggles, PR3) was the right control all along — is likewise
not reintroduced: the create-table form's stream-enabled and TTL-enabled
controls are both `toggleSwitch`, and the segmented control is reserved
for the form's two genuinely closed sets (stream view type, GSI projection
type).

## Amendment (2026-08-22, ADR 0053)

[ADR 0053](0053-dynamodb-only-drop-cql.md) drops the CQL wire adapter and
its port slot entirely. This ADR's "seven-port stride" (`base_port + 7*i +
{internal:0, client:1, dynamo:2, cql:3, admin:4, intra:5, console:6}`,
§"What this PR ships" above) and its Consequences section's "combined and
data-only bind **seven** listeners now" are both historical as of ADR
0053: with `cql` removed, the stride is **six** again, renumbered as
`base_port + 6*i + {internal:0, client:1, dynamo:2, admin:3, intra:4,
console:5}`. The console-binding rule this ADR set (combined and
data-only bind it; control-only does not) is unchanged — only the slot
numbering shifts, and only for the roles at or after `admin`. The body
above is kept as originally written; see ADR 0047's own matching
amendment note for the full before/after port table.
