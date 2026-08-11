# ADR 0040 — Self-minted string node identities and registration-CAS membership

- **Status:** Accepted — staged implementation. **PR1 of this ADR's 6-PR stack
  is what this commit lands: Decision A (one identity per node) only.**
  Decisions B, C, and D below are accepted design but not yet built; each
  lands in a later PR of the same stack (see "Staged implementation" below).
  Do not read this ADR as describing the code as it stands today in full —
  it describes the whole arc the stack is converging on.
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

### Decision C — registration-CAS membership (retires ADR 0036's allocator)

`MetaCommand::RegisterNode { node, addrs, labels }` replaces both the
self-registration path and `MetaCommand::AllocateNodeId` entirely. Apply-time
semantics: an unclaimed id inserts a `Down` member + address entry
(unchanged `Down → Active` promotion chain, ADR 0030 §1); a claimed id with a
byte-identical `NodeAddrs`+labels re-registration is a no-op (idempotent
retry, and the ADR 0032 rejoin case); a claimed id with a *different* entry
is rejected. A **minted** id whose claim collides re-mints and retries (ports
are never derived from ids under this scheme, so nothing needs rebinding); a
**proposed** id whose claim collides fails loudly — a structural fix, not a
pre-bind guess, for the residual race ADR 0032 documented and accepted. This
retires `Metadata.next_alloc_id`/`node_id_allocations`, `ALLOC_ID_BASE`,
`syskv::EntityKind::NodeIdAlloc`, and `generate_join_nonce`'s OS-randomness
exception.

### Decision D — config/CLI shape (clean break)

Config files gain an explicit per-node `id: String` field (validated unique
at load). `gen-config` mints `"n0".."n{N-1}"` (zero-padded once `N >= 10` —
lexicographic string order, not numeric). `--config FILE --node I` keeps `I`
as a positional *index* into the config; the entry's own `id` is the
identity. `join`/`data --seed`'s `--node I` sugar is removed outright; `--id
NAME` proposes a durable identity, omitting it self-mints an ephemeral one
(ADR 0036's contract, carried forward). This is a **clean break**: per this
repo's standing "no live deployments" rule, no config/wire/WAL back-compat
with any pre-ADR-0040 deployment is provided or attempted.

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
- **The operator-initiated Down-entry orphan class** (a registered-but-never-
  activated member lingering forever) is explicitly *not* fixed by this PR —
  it is the last PR of the stack (PR6): an auto-reclaim sweep on the
  control-plane leader's own volatile timer, keyed on a new
  `Member.has_activated` flag, proposing the existing `RemoveMember` after a
  configurable TTL. Until PR6 ships, the bounded-but-operator-prunable stance
  this ADR inherits from ADR 0036 remains the status quo.

## Staged implementation

This is a 6-PR stack, each independently reviewable, stacked in this order:

1. **PR1 (this commit)** — Decision A: one identity per node, `NodeId` still
   `u64`. Everything in "Decision A" above.
2. **PR2** — `NodeId(u64)` newtype (stays `Copy`, no arithmetic, `Display`,
   a `nid(u64)` test helper); a behavior-, wire-, and WAL-byte-neutral
   mechanical sweep proving the type is opaque before any representation
   change lands.
3. **PR3** — Decision B in full: `NodeId(Arc<str>)`, the charset, config `id`
   fields + `gen-config`/`--cluster*` minting, `syskv`/`mirror`/`codec.rs`
   encodings, the `ProdEnv` wire frame, dashboard JS sorts, CLI arg types.
4. **PR4** — Decision C: `RegisterNode` CAS + self-minting; retires the ADR
   0036 allocator (`AllocateNodeId`, the ledger, `ALLOC_ID_BASE`,
   `NodeIdAlloc`, `generate_join_nonce`'s exception, `check_join_collision`).
5. **PR5** — cleanup/docs: delete `Coresident`; amendment stanzas on ADRs
   0012, 0026, 0030, 0032, 0035, 0036, 0037, 0038 (this ADR's own Decision
   text above previews what each amendment will say); `docs/engineering-
   lessons.md` + crate `CLAUDE.md` sweep; dashboard id-truncation polish.
6. **PR6** — the orphan-member auto-reclaim sweep described above.

Until PR5 lands, the other ADRs this one amends (0012, 0026, 0030, 0032,
0035, 0036, 0037, 0038) are **not** yet stamped with amendment stanzas
pointing back here — that stamping is PR5's own job, done once, in one pass,
rather than incrementally re-editing the same ADRs across five separate PRs.
