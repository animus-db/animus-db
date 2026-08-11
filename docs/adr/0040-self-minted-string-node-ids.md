# ADR 0040 — Self-minted string node identities and registration-CAS membership

- **Status:** Accepted — implemented (PR1–PR6, complete). PR1 (Decision A,
  one identity per node), PR2 (the opaque `NodeId` newtype), PR3 (Decision B
  in full — string representation, config `id` fields, minting groundwork),
  PR4 (Decision C in full — the `RegisterNode` registration CAS, retiring
  the ADR 0036 allocator — and the join-path/`control-add` half of
  Decision D), PR5 (the amendment stanzas on every ADR this one touches, the
  CLAUDE.md/engineering-lessons sweep, the vestigial `Coresident` deletion,
  and dashboard id-truncation polish), and **PR6 (this PR: the
  orphan-member auto-reclaim sweep — `Member.has_activated`, the
  control-plane leader's own volatile `orphan_sweep_after` timer, the
  claim-without-member cleanup `MetaCommand::RemoveMember` gained, and the
  config/CLI knob)** have all landed. Read the "Staged implementation"
  section for the couple of places the shipped code diverged from this
  ADR's own narrative prose — each is called out inline where it matters
  (Decision C's CAS key, in particular).
- **Date:** 2026-08-11

## Context

Three separate mechanisms grew, independently, around the fact that a node's
identity used to be an arithmetic derivation rather than a first-class,
independently-claimed value:

1. **Two ids per combined node.** Before this ADR, one `animusd` combined-mode
   process bound **two** `ProdEnv`s, two internal listeners, two on-disk
   directory trees, and two `NodeId`s: `control_id(i) = i` for the control
   plane's Raft group, and `raftkv_id(i) = RAFTKV_ID_BASE + i` (`RAFTKV_ID_BASE
   = 300`) for the per-tablet CP data plane. The two ids existed only because,
   pre-ADR-0026, one node id meant one inbox meant one protocol instance — a
   node hosting two protocols needed two ids. ADR 0026 multiplexed `Network`
   addressing onto `(node, stream)` pairs specifically to let one id host
   several protocol instances (the per-tablet CP groups migrated onto streams
   in that ADR's Stage B), which quietly made the two-id split's original
   justification obsolete without anyone revisiting the id scheme itself.
2. **A parallel, ad hoc "no real id yet" placeholder for growth.** ADR 0036
   added `MetaCommand::AllocateNodeId`, a monotonic allocator disjoint from
   `RAFTKV_ID_BASE`, so a joining node whose `--node I` an operator hadn't
   picked could still get a real, structurally-unique id. On top of that,
   combined-mode's own two-id split forced `config::synthetic_control_id_for`
   — a `raftkv_id | (1 << 63)` bit trick deriving a *third*, never-replicated,
   purely local placeholder id for the allocated join's control-Raft slot,
   because there was no small operator index to derive one from the way
   `control_id(index)` could. Three id spaces (`0..N` control ids,
   `RAFTKV_ID_BASE..` raftkv ids, `ALLOC_ID_BASE..` allocated ids, plus the
   top-bit-set synthetic space) coexisted by convention, not by any structural
   guarantee.
3. **A documented id-space mismatch bug class.** ADR 0037's quorum-guard
   liveness signal (`admin_remove_control_member`'s "is a majority of the
   *other* voters still reachable" check) could not reuse the existing
   failure detector (`ControlHandle::believes_alive`) at all: that signal is
   keyed on **raftkv** ids (heartbeats only ever run on the data role), so
   calling it with a **control** id was always `false` — permanently
   "believed dead" for reasons having nothing to do with real liveness. ADR
   0037 hardening PR2 had to grow an entirely separate, genuinely
   control-id-native mechanism (`RaftCore::peer_last_contact`) to work around
   this, rather than the two signals sharing one id space naturally.

Independently, cluster-allocated ids (ADR 0036) mint monotonic `u64`s from a
single control-plane counter — practically fine, but not the shape a
self-minting, collision-structurally-impossible scheme wants once ids stop
being small array indices. And every id in this codebase has, until now, been
a bare `u64` with no validation, no charset, and no notion of a node
*proposing* a durable identity of its own choosing (only an operator picking a
small integer, or the cluster handing one out).

## Decision

We are moving to **self-minted, validated, opaque string node identities**,
with uniqueness enforced by a replicated compare-and-swap at registration
time (never trusted probabilistically), delivered as a 6-PR stack so each
piece of blast radius stays independently reviewable:

### Decision A — one identity per node (**this PR, PR1**)

A node has **exactly one** `NodeId`, carried on **one** internal env
(`ProdEnv` in production, `SimEnv` under test). The control-plane Raft rides
`PRIMARY_STREAM` (stream 0, ADR 0026's default); every per-tablet Raft group
this node hosts rides its own stream (`stream = tablet_id`, which floors at 1
— ADR 0022/0023), so the two are disjoint by construction on one shared
inbox. This was possible with **zero renumbering of the stream space** because
ADR 0026 already made it possible — Decision A is "actually stop paying for a
second id" using machinery that already existed for an unrelated reason.

Concretely, in this PR: `RoleAddrs.control`/`.raftkv` merge into one
`RoleAddrs.internal` (a 6-port-per-node stride becomes 5);
`animus_control::meta::NodeAddrs.raftkv`/`.control` merge into one
`NodeAddrs.internal`; `animusd`'s combined-mode assembly
(`BoundNode::start_with`) binds one env and passes clones of it to both
`RaftNode::start_with_metrics` (control) and the tablet-host reconciler's
`RaftKvNode`s (data); the two pre-existing peer-sync loops
(`peer_sync_loop`/`control_peer_sync_loop`) collapse into one; failure
detection and control-voter liveness now read the same id space, so the
`believes_alive`/control-id mismatch this ADR's Context section describes is
**structurally dissolved**, not merely worked around (ADR 0037 hardening
PR2's dedicated `peer_last_contact` signal is *kept* regardless — see that
PR's own doc — because it answers a strictly more precise question than mere
id-space unification does: control-Raft-traffic reachability, not general
network reachability). `config::synthetic_control_id_for` and
`config::RAFTKV_ID_BASE` are deleted outright — a cluster-allocated join's
id *is* its one identity now, needing no derived placeholder.

`NodeId` stays a bare `u64` in this PR — Decision A is proved out first,
deliberately, *before* the type changes underneath it (see "Staged
implementation").

### Decision B — `NodeId` as a validated opaque string

`NodeId` becomes `NodeId(Arc<str>)`: `Clone`/`Eq`/`Ord`/`Hash` (loses `Copy`),
serialized as a transparent JSON string, `Display`/`Debug` as the raw value.
Accepted charset for a **proposed** id: `[A-Za-z0-9._-]{1,64}` — excluding `@`
(the leader-hint wire format is `leader_hint={id}@{addr}`) and
whitespace/`/`. `NodeId::mint(rng)` draws two `u64`s from the `Rng` seam and
base64url-encodes them (22 chars, unpadded) for a node that doesn't propose an
explicit id. This removes `generate_join_nonce`'s documented OS-randomness
exception (ADR 0003's one sanctioned deviation) — minting moves onto the
seam, through a small `Rng` impl `animus_env::prod` exports for the CLI
boundary, and through `SimEnv`'s existing seeded `Rng` for tests.

**One storage-boundary exception, as shipped in PR3**: `animus-consensus`
(the testbed-only Accord slice, ADR 0011/0018/0019) keeps `Timestamp`/
`Ballot.node` as a real `NodeId` — its `core.rs` reads it semantically (e.g.
`is_recovery_nominee`) — but its on-disk MVCC storage version encoding
(`mvcc_version(ts) = (logical << 16) | node_index`) needs a small, dense
bit-packed integer, which an opaque validated string can no longer provide
directly. Rather than introduce a crate-wide opaque `NodeIdx` type, this one
encoding folds in a node's **position in the sorted, closed replica set**
(`node_index`) computed at the point of use, not threaded through the
protocol core. See `animus-consensus/CLAUDE.md` for the full mechanism and
`mvcc_version`'s encoding-contract assertions.

### Decision C — registration-CAS membership (retires ADR 0036's allocator) (**PR4**)

`MetaCommand::RegisterNode { node, addrs, labels }` replaces both the
self-registration path and `MetaCommand::AllocateNodeId` entirely. **CAS key,
as shipped (diverges from this ADR's original narrative — see below): the
compare-and-swap is keyed on `Metadata::node_addrs` alone, not the full
`addrs`+`labels` pair.** An id absent from `node_addrs` claims the address
slot — inserting a `Down` `Member` with `labels` too, but *only* as a side
effect, and *only* if `members` doesn't already have an entry for it; a
byte-identical re-registration (comparing `node_addrs` only) is a no-op
(idempotent retry, and the ADR 0032 rejoin case); a *different* `NodeAddrs`
already on file is rejected — the real collision. A **minted** id whose claim
collides re-mints and retries (ports are never derived from ids under this
scheme, so nothing needs rebinding); a **proposed** id whose claim collides
fails loudly — a structural fix, not a pre-bind guess, for the residual race
ADR 0032 documented and accepted. A control-role registration
(`NodeAddrs.role == "control"`) additionally **never claims a `members` row
at all** — a control-only node can never host a tablet, so appearing in
`members` (eventually `Active`, once heartbeating) would silently make it a
tablet-placement candidate. This retires `Metadata.next_alloc_id`/
`node_id_allocations`, `ALLOC_ID_BASE`, `syskv::EntityKind::NodeIdAlloc`, and
`generate_join_nonce`'s OS-randomness exception.

**Why the CAS key diverges from a labels-inclusive comparison**: a
labels-inclusive CAS (the design this ADR originally described, and PR4's
starting point) broke two real scenarios that predate `RegisterNode`
entirely: a fresh bootstrap node's own self-registration racing
`bootstrap()`'s `UpsertMember` insert, and — more seriously — *any* node's
self-registration racing an operator's `admin_add_member`/
`admin_add_control_member` call. Both establish `members` (with real labels)
through a wholly separate path that carries no address for `RegisterNode` to
compare against, so a labels-inclusive CAS made whichever command lost that
race permanently `Rejected`. Keying on `node_addrs` alone — and only ever
inserting into `members` as a side effect, never overwriting an
already-established row's labels/status — closes that hazard structurally.
The control-role carve-out was found the same way: routing every node's
self-registration through one CAS command surfaced a second real bug
(control-only bootstrap nodes appearing in `members` and corrupting
placement) that a labels-inclusive design would have masked by rejecting the
second command outright instead of applying it wrongly. Both hazards are
recorded in `docs/engineering-lessons.md`'s PR4 entry.

### Decision D — config/CLI shape (clean break) (**config half landed PR3; join/`control-add` half landed PR4**)

Config files gain an explicit per-node `id: String` field (validated unique
at load). `gen-config` mints `"n0".."n{N-1}"` (zero-padded once `N >= 10` —
lexicographic string order, not numeric). `--config FILE --node I` keeps `I`
as a positional *index* into the config; the entry's own `id` is the
identity. `join`/`data --seed`'s `--node I` sugar is removed outright; `--id
NAME` proposes a durable identity, omitting it self-mints an ephemeral one
(ADR 0036's contract, carried forward). `control-add`'s omitted-`node` form
mints the same way (`NodeId::mint` off the leader's own bound `Env`) and its
operator-supplied form drops the `ALLOC_ID_BASE`-range refusal (no ranges
exist anymore) — an id that already names an existing data-plane member now
*succeeds* (promotion, not a conflict: ADR 0040 PR1 already unified the id
space, so there is no separate control-id range left to collide with). This
is a **clean break**: per this repo's standing "no live deployments" rule, no
config/wire/WAL back-compat with any pre-ADR-0040 deployment is provided or
attempted.

**No new client wire variant, as shipped**: a joining node's `RegisterNode`
claim (and `control-add`'s mint-then-claim) both reach the leader entirely
over the *existing* `ClientRequest::ProposeSchema`/`ClientResponse::Status`
round trip every other relayable `MetaCommand` already uses — propose, then
poll `metadata_fresh()` for the claim to land, the same shape
`trigger_split` already established. No dedicated join/registration wire
message was added or needed.

## Consequences

**What gets easier:**
- One env, one listener, one disk-dir tree, one id per node — a whole class
  of "which id space is this" bugs (the ADR 0037 mismatch, the
  `synthetic_control_id_for` placeholder, `RAFTKV_ID_BASE` arithmetic
  scattered through `animusd`/`animus-cli`) stops being possible by
  construction rather than by convention.
- Growth/join simplifies: an allocated/minted join's control-Raft slot needs
  no derived placeholder id at all (Decision A already delivers this; PR1's
  `run_node_join_allocated` no longer computes a synthetic control id — the
  minted id just *is* the one id, structurally a non-voter because the
  discovered `original_control_ids` doesn't contain it).
- Random ids are no longer trusted probabilistically (Decision C) — a
  collision is a correctness-critical event this scheme makes structurally
  unreachable rather than merely astronomically unlikely.

**What gets harder / costs knowingly accepted:**
- **Fresh clusters only.** Every PR in this stack that touches a persisted
  format (this PR's `NodeAddrs`/`RoleAddrs` shape; PR2's mechanical `u64`
  sweep is behavior-neutral; PR3's string representation breaks the control
  WAL's `serde_json` shape and `animus-cp-data`'s binary `codec.rs` node-set
  encoding) states it explicitly: no migration path, no wire/WAL back-compat,
  by this repo's standing rule (`no-live-deployments-fresh-clusters.md`).
- **Ordering churns from numeric to lexicographic** once PR3 lands the string
  type (a `RaftCore` has no id-based tie-break — term/log order only — so
  nothing here is a *safety* change, only which order a `BTreeMap`/sorted
  `Vec` of ids presents them in; see PR3's own ordering-consequences
  writeup). This PR (PR1) does not touch that — ids stay `u64`, so nothing
  numeric changes yet.
- **Perf**: a string `NodeId` costs an `Arc<str>` clone per message/map-key
  instead of a `Copy`, and a few extra bytes per wire frame (PR3). Assessed
  as noise at realistic cluster sizes; `SmolStr`/interning is a local upgrade
  if `engine_bench`/`prod_liveness` ever disagree.
- **The dashboard's `Object.keys(members).map(Number).sort(...)` idiom breaks
  outright** once ids are strings (PR3) — every such call site becomes a
  plain lexicographic sort; tablet-id sorts (still numeric) are unaffected.
- **The operator-initiated Down-entry orphan class is now auto-reclaimed, not
  merely bounded-and-operator-prunable (PR6, closing the stance this ADR
  inherited from ADR 0036).** A registered-but-never-activated claim — a
  crash-mid-join, or the losing racer of two concurrent omitted-id
  `control-add`s — no longer lingers forever waiting for an operator to
  notice it. `Member` gained a sticky `has_activated: bool` (set the moment
  a member is ever recorded `Active`, by any caller — the ADR 0012
  detector's `Down`→`Active` promotion or `bootstrap`'s direct `Active`
  insert alike — never cleared again), which is exactly what distinguishes
  "never showed up" (sweepable) from "was alive, currently down"
  (repair/decommission territory, never sweepable). The control-plane
  leader's own **volatile** timer (`animus_control::node::orphan_sweep_loop`,
  the same home and pattern as the ADR 0012 failure detector — no
  replicated wall clock, `Metadata::apply` stays a deterministic pure
  function) tracks how long each sweep-eligible claim
  (`Metadata::orphan_sweep_candidates`, itself pure) has persisted under
  *this* leadership stint, excludes anything in the **live control-voter
  set** (`RaftCore`'s own config — a fact `Metadata` cannot see, so this
  exclusion is the driver's job, not the state machine's), and proposes the
  existing `MetaCommand::RemoveMember` once `orphan_sweep_after` elapses
  (default 10 minutes; `Duration::ZERO` disables the sweep entirely;
  configurable via `animusd`'s config file/CLI flag). `RemoveMember` itself
  gained one extension: it now also prunes the **claim-without-member**
  shape (a `node_addrs` entry with no `members` row at all — exactly what a
  control-role `RegisterNode` produces, since it never claims membership),
  which it previously treated as an already-absent no-op, silently leaking
  the address-book entry forever. Leadership change resets the volatile
  timer (acceptable — convergent, just delayed: the new leader's own
  countdown simply starts over). The safety argument for the catastrophic
  case — a sweep proposal racing a genuine late activation must never
  remove an already-`Active` member — is structural, not timing-dependent:
  `RemoveMember`'s existing apply-time guard re-checks status fresh against
  whatever already committed ahead of it in the log, rejecting
  `Active`/`Joining` outright regardless of which order the two proposals
  were computed in; and `liveness_transitions` (the sole production
  producer of a promotion) only ever proposes one for a member it finds
  present in that same tick's fresh `Metadata` read, so a removed claim is
  never resurrected by a stray late heartbeat either. See
  `crates/animus-control/CLAUDE.md`'s `node.rs`/`meta.rs` entries for the
  mechanism and `crates/animus-control/tests/orphan_sweep.rs` for the full
  seeded fault-injection suite (crash-mid-join, the losing-racer and
  claim-without-member shapes, a slow-but-legit joiner activating in time,
  a once-active member merely going `Down`, a leader failover mid-countdown
  still converging, and the sweep disabled outright).

## Staged implementation

This is a 6-PR stack, each independently reviewable, stacked in this order:

1. **PR1 (landed)** — Decision A: one identity per node, `NodeId` still
   `u64`. Everything in "Decision A" above.
2. **PR2 (landed)** — `NodeId(u64)` newtype (stays `Copy`, no arithmetic,
   `Display`, a `nid(u64)` test helper); a behavior-, wire-, and WAL-byte-
   neutral mechanical sweep proving the type is opaque before any
   representation change lands.
3. **PR3 (landed)** — Decision B in full: `NodeId(Arc<str>)`, the charset,
   config `id` fields + `gen-config`/`--cluster*` minting, `syskv`/`mirror`/
   `codec.rs` encodings, the `ProdEnv` wire frame, dashboard JS sorts, CLI arg
   types. PR3 also shipped a deliberate one-PR shim (`meta::alloc_node_id`/
   `parse_alloc_id`, an `"alloc-{n}"`-prefixed string mint over the old
   `AllocateNodeId` counter) so the allocator kept working with string ids for
   exactly one PR before PR4 retired it outright.
4. **PR4 (landed)** — Decision C in full (`NodeId::mint`,
   `MetaCommand::RegisterNode`'s registration CAS, `is_relayable_command`) and
   the join-path/`control-add` half of Decision D: retires the ADR 0036
   allocator and PR3's shim alike (`AllocateNodeId`, `next_alloc_id`/
   `node_id_allocations`, `ALLOC_ID_BASE`, `alloc_node_id`/`parse_alloc_id`,
   `syskv::EntityKind::NodeIdAlloc`, `generate_join_nonce`'s OS-randomness
   exception — replaced by `animus_env::prod::PreBindRng`,
   `check_join_collision`, `ClientRequest::AllocateNodeId`/
   `ClientResponse::NodeIdAllocated`); `join`/`data --seed` drop `--node I`
   for `--id NAME` (validated, durable) or self-mint (omitted, ephemeral);
   `admin_add_control_member` mints via the leader's own `Env` and deletes the
   `ALLOC_ID_BASE`-range refusal (no ranges exist anymore) and the
   "already exists as a member" refusal (promoting an existing member to a
   control voter is the common case now, not a conflict).
5. **PR5 (this commit)** — cleanup/docs, per this ADR's own stanza-drafting
   promise above, delivered in one pass rather than incrementally re-editing
   the same ADRs across five separate PRs:
   - **Deleted**: the vestigial `Coresident`/`sibling` trait and its
     `ProdEnv` machinery (`bind_with_pool`/`PoolSlot`/the pool field/
     `shutdown_tasks`) and `SimEnv` impl — zero live call sites since the
     per-tablet CP groups migrated onto ADR 0026 streams; see
     `animus-env/CLAUDE.md`.
   - **Deliberately kept, not deleted**: `Metadata::cp_member_addrs`/
     `cp_member_tablets` and `MetaCommand::RegisterCpAddr` (`animus-control`).
     These looked, at first glance, like they'd become fully redundant with
     the unified `NodeAddrs.internal` this ADR's Decision A/B already
     deliver — and functionally, on a freshly bootstrapped cluster, nothing
     in `animusd`'s production paths proposes `RegisterCpAddr` anymore (it's
     already documented as "kept for WAL back-compat only" — see
     `animus-control/CLAUDE.md`'s `meta.rs` entry). But three things still
     genuinely read it: `animusd::peer_sync_loop` merges
     `Metadata.cp_member_addrs` into the node's live peer book every tick
     (harmless-but-live, not dead code); the ADR 0038 PR6 system-keyspace
     browse endpoint (`GET /admin/system-table`) is explicitly designed to
     surface every `EntityKind`, "including the internal/legacy ones — full
     transparency by design" (`animus-control/CLAUDE.md`'s `syskv.rs` PR6
     entry) — deleting the kind it browses would contradict that design
     goal, not fulfill it; and removing the field/command/`EntityKind`
     variant outright touches `Metadata::apply`, `mirror.rs`'s derivation +
     restart-rebuild round trip, `syskv.rs`'s key encoding, `admin.rs`'s
     JSON view, and roughly a dozen test call sites across
     `animus-control`/`animusd` — a genuine feature removal, not the "small
     deletion" this cleanup PR's scope calls for. Left in place; a future PR
     that wants to actually retire `RegisterCpAddr` should treat it as its
     own reviewable change, not a drive-by in a docs PR.
   - Amendment stanzas landed on ADRs 0012, 0026, 0030, 0032, 0035, 0036,
     0037, 0038 (this ADR's own Decision text above previews what each
     says); `docs/adr/README.md`'s index updated to match; the root
     `CLAUDE.md` + `animusd`/`animus-control`/`animus-env`/`animus-cp-data`
     `CLAUDE.md`s swept for stale pre-ADR-0040 id-scheme claims;
     `docs/engineering-lessons.md` verified/extended; dashboard
     truncate-with-tooltip polish for 22-char minted ids (overview/
     placement/raft tables).
6. **PR6 (landed)** — the orphan-member auto-reclaim sweep described above:
   `Member.has_activated`, `Metadata::orphan_sweep_candidates`, the
   `RemoveMember` claim-without-member extension, the leader-side volatile
   `orphan_sweep_loop` (`animus-control`), and the `orphan_sweep_after`
   config-file/CLI knob (`animusd`).
