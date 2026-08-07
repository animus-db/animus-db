# ADR 0032 — Seed/join membership: a replicated node address book, `animusd join`, and decommission

- **Status:** Accepted — implemented incrementally (PR1: replicated node
  address book; PR2: `animusd join`; PR3: decommission). **This document
  covers the full 3-PR lifecycle design; PR1 and PR2 are landed. PR3
  (decommission) is not yet implemented.**
- **Date:** 2026-08-07

## Context

ADR 0030 delivered online cluster growth (data-plane only, control group
static): a *pre-existing* node's operator runs `POST /admin/member/add` on
its behalf, and the new node itself is started with an **expanded**
`ClusterConfig` that already lists every node — pre-growth and grown — by
address. That config has to be constructed and handed to the new node's own
process out of band (an operator writes it, or a deployment tool assembles
it centrally). ADR 0030 explicitly called out the shape this leaves
incomplete, in its own words: *"a pre-growth node's `client_route` is a
static map built once at its own process start, so it cannot forward a
client op to a tablet leader that has since moved onto a newly grown
node."* Nothing in the running cluster ever learns a grown node's addresses
after the fact — every address a node knows about is either (a) baked into
its own config at startup, or (b) the internal `raftkv` address distributed
through `Metadata.cp_member_addrs` (Phase 2 — but that map is `raftkv`-only,
built for the internal Raft peer book, and was never meant to carry
client-API or admin-API addresses).

This is the seed/join model every masterless or Raft-backed store this
project takes inspiration from (Cassandra's gossip-seeded ring,
CockroachDB's `--join` flag) already solved the same way: a joining node
needs to know **some** existing member's address (a *seed*), and from there
learns the rest of the cluster's addresses **from the cluster itself** —
not from a config file assembled by an external operator that already knows
the final topology. AnimusDB's control group staying static (ADR 0030's
scope decision, unchanged here) means "join" can't mean "become a control
voter" — it means "become a real data-plane (`raftkv`) member, and make
every *other* node learn my addresses the same way I learn theirs."

Splitting that into three PRs follows the same "land the low-risk mechanical
piece first" discipline as ADR 0031's PR stack:

1. **PR1 (this PR): a replicated node address book.** Before a join
   *protocol* can exist, every node's full address set (not just its
   internal `raftkv` address) has to be something the cluster agrees on and
   any node can look up — closing ADR 0030's residual gap as a byproduct,
   independent of whether growth happens via the old `--admin/member/add` +
   hand-assembled expanded config, or the new `join` flow PR2 adds.
2. **PR2: `animusd join`.** A new node starts with only a *seed* address (one
   already-running node, of any kind) instead of a full `ClusterConfig`. It
   contacts the seed, receives a `JoinInfo` (the pre-growth `control_ids`,
   the current node address book, an operator-supplied node index derived
   from `animus-cli`'s own convention or explicitly passed), and
   self-registers via the same `RegisterNodeAddrs` + admin-add path ADR 0030
   already built — `run_node_growth`'s mechanics are reused, not replaced,
   the only new thing is *how the node learns what it needs to start*.
3. **PR3: decommission.** A `drain` (already exists, ADR 0020/0029 — marks a
   member `Leaving`, the placement reconciler + rebalancer + release-GC
   already relocate its tablets off it) followed by a `RemoveMember` command
   once draining is confirmed complete, pruning the member from
   `Metadata.members`/`node_addrs`/`cp_member_addrs` — gated at apply time on
   the member existing, being `Leaving`/`Down` (never mid-service), and no
   tablet still referencing it as a replica (else removal would silently
   drop a tablet below its replication factor with no repair path left, since
   the member is gone from the candidate pool entirely).

## Decision — PR1: the replicated node address book

### `NodeAddrs` + `Metadata.node_addrs`

A new `animus_control::meta::NodeAddrs { raftkv, client, admin }` struct
(all three as opaque `String` addresses, mirroring `cp_member_addrs`'
existing opaque-string convention — the control plane never dials any of
these, so it never needs to parse them) and a new `Metadata.node_addrs:
BTreeMap<NodeId, NodeAddrs>` field, keyed by the member's `raftkv` id (the
same id space `cp_member_addrs` already uses). `#[serde(default)]` keeps
every pre-ADR-0032 WAL record/snapshot decoding unchanged.

A new `MetaCommand::RegisterNodeAddrs { node, addrs }`, applied exactly like
its `RegisterCpAddr` sibling: idempotent no-op if the stored entry already
equals `addrs`, otherwise insert/overwrite. `RegisterCpAddr` (and
`Metadata.cp_member_addrs`) are **kept, byte-for-byte, for WAL back-compat**
— old WAL records/snapshots still decode and old code paths that might still
propose it (none, after this PR, but the variant must not become a decode
trap) still apply correctly — but `animusd` **stops proposing it** at
startup: every node now self-registers its **full** address set via
`RegisterNodeAddrs` once, instead of just its `raftkv` address via
`RegisterCpAddr`.

### Every existing consumer of a "just the raftkv addresses" or
"just the static, own-process config" map gains a `node_addrs` overlay

Three places in `animusd` previously assumed a node's reachable-address
picture was either (a) the internal `raftkv` peer book seeded from
`cp_member_addrs`, or (b) a client/admin address table built once from this
node's own config/bring-up and never revisited:

- **`peer_sync_loop`** (`raftkv`-role internal peer book): now overlays
  `Metadata.node_addrs[*].raftkv` on top of the existing `static ∪
  cp_member_addrs` union, so a node that only ever went through
  `RegisterNodeAddrs` (no `RegisterCpAddr` in its history — the normal case
  going forward) is still reachable for internal Raft traffic.
- **`ClientCtx.client_route`** (cross-node client-op forwarding /
  `propose_schema`'s relay and broadcast-fallback): changed from a plain
  `BTreeMap<NodeId, SocketAddr>` to `Arc<Mutex<BTreeMap<NodeId,
  SocketAddr>>>`, and a new sibling loop, **`route_sync_loop`** (same
  `PEER_SYNC_INTERVAL` cadence and static-base-∪-replicated-overlay shape as
  `peer_sync_loop`), keeps it live: each tick recomputes `static_route ∪
  Metadata.node_addrs[*].client` and swaps it in. This is the fix for ADR
  0030's own documented gap — a client connected only to an *original* node
  can now have its op forwarded to a tablet leader that has since moved onto
  a node grown in well after the original node's own startup, because the
  original node's `client_route` is no longer a process-start-only
  snapshot. Every consumer of the old plain-map field (`cp_forward_target`,
  `propose_schema`'s relay + broadcast fallback, the growth-node
  `remote_metadata_sync_loop` seed computation) now reads through
  `ClientCtx::route_addr`/`route_snapshot` instead of indexing the map
  directly, so none of them can hold the lock across an `.await`.
- **`/admin/peers`** (the web dashboard's cross-node fan-out seed, ADR 0021):
  now the union of this node's static `AdminInfo.admin_addrs` and every
  `Metadata.node_addrs[*].admin`, deduplicated and sorted for a stable
  fan-out list across polls — so the dashboard, loaded from any original
  node, discovers a grown node's admin port too.

Each of these follows the same *static seed ∪ replicated overlay,
recomputed every tick* shape `peer_sync_loop` already established for the
`raftkv` peer book (ADR "Phase 2.3a") — no new pattern, just the same one
applied to the two axes (`client`, `admin`) it was never extended to.

### Why not just widen `cp_member_addrs` itself?

`cp_member_addrs` is keyed on **CP group member ids**, which include
transient/derived ids that are not full cluster members in the
`Metadata.members` sense (a historical artifact of when split siblings could
mint distinct member ids — ADR 0026 Stage B / ADR 0028 later made a
tablet's group member id always equal its base `raftkv` id, but the map's
*shape* — "any CP-Raft-relevant id, no claim about cluster membership" —
still doesn't match what a node address book needs: "every actual cluster
member's full address set"). Reusing it would have meant overloading one
map with two different key populations and two different lifetimes (GC on
tablet-absence for the CP-address use, nothing comparable for a node's own
identity). A second, purpose-built map keyed cleanly on member id is
simpler to reason about than retrofitting a GC-scoped map to also serve an
unrelated purpose — the standard "one id space, one map" discipline this
codebase already applies elsewhere (see the root `CLAUDE.md`'s "one id space
must have one allocator" entry for the same shape of reasoning applied to
tablet ids).

## Decision — PR2 (implemented): `animusd join`

A new `animusd join --seed ADDR[,ADDR...] --node I [--ip A] [--base-port P]
[--dir D] [--ephemeral]` CLI path and a `ClientRequest::JoinInfo` request:
the joining node connects to one of `--seed` (any already-running node's
client address — old or newly grown, it no longer matters which, since PR1
makes every node's address book equally current), which replies with
`ClientResponse::JoinInfo { control_ids, peers, client_route, admin_addrs }`
— this cluster's **pre-growth** control group, the answering node's internal
peer book (`AdminInfo.peers`), its live client-op routing table
(`ClientCtx::route_snapshot`, ADR 0032 PR1's `route_sync_loop`), and every
known admin address. Any node answers `JoinInfo` from its own knowledge, no
forwarding needed. `run_node_join` then runs the exact same
[`BoundNode::start_with`] call [`run_node_growth`] does, passing the
discovered `control_ids` as `original_control_ids` — the ADR 0030 growth
machinery (`!control_ids.contains(&self.control_id)` detection,
`remote_metadata_sync_loop`, `effective_metadata`) engages automatically,
exactly as it does for a `run_node_growth` node started from an
operator-assembled expanded config.

Two deliberate shape differences from this ADR's original sketch, decided
during implementation:

- **`--node I` is a required, operator-supplied index**, not an
  "auto-derived" one — mirroring the existing `--node`/`--cluster` CLI
  convention everywhere else in `animusd`, rather than inventing a new
  auto-numbering scheme this PR doesn't need. A **collision guard** covers
  the case an auto-derived index would have tried to avoid entirely: before
  binding anything, `run_node_join` fetches a `ClientRequest::Status` reply
  and checks `Metadata.node_addrs` for an existing entry at
  `config::raftkv_id(index)` — an **identical** address book is a *rejoin*
  (a restart at the same index/addresses/dir) and proceeds; a **different**
  one fails startup loudly (`io::ErrorKind::AlreadyExists`) instead of
  silently colliding. This narrows, but does not eliminate, the race between
  two simultaneous joiners choosing the same index — `RegisterNodeAddrs`
  apply-time idempotence (PR1) is the actual backstop for that residual
  window, not this pre-bind check.
- **Member self-registration was folded into `BoundNode::start_with`'s
  existing growth-node block**, not added as a separate step inside
  `run_node_join`. `start_with` already detects "my own control id is not in
  the `control_ids` I was started with" to decide whether to run
  `remote_metadata_sync_loop`; that same block now also spawns a one-shot
  task calling `ClientCtx::admin_add_member` (idempotent — a no-op if
  already registered). This means **every** growth node self-registers
  automatically the moment it starts, whether reached via `run_node_growth`
  (an operator-assembled expanded config) or `run_node_join` (seed
  discovery) — so `run_node_growth`'s own callers no longer strictly need
  their own separate `POST /admin/member/add` call either, though it remains
  supported as an idempotent confirmation (`tests/cluster_growth.rs` keeps
  its explicit call, now proving that no-op path rather than the only path
  in).

One supporting change: the client-protocol `Status` handler now serves
`ClientCtx::effective_metadata()` instead of the bare local
`raft.metadata()`, exactly as `/admin/status` already did (ADR 0030) — a
growth node's local control raft never replicates, so without this a joiner
that picked a *grown* node as its seed would have received a
permanently-empty `Metadata` (a vacuous collision guard), and `animus
status` against a grown node would have shown an empty cluster. Safe for
`remote_metadata_sync_loop`'s own polling, whose seeds are always the
pre-growth control nodes (genuine voters, where `effective_metadata` is a
plain passthrough) — no mirror ever feeds another mirror.

## Decision — PR3 (not yet implemented): decommission

`drain` (ADR 0020/0029) already marks a member `Leaving` and lets the
placement reconciler + rebalancer + release-GC relocate every tablet off it
with no new mechanism. PR3 adds the second half: once draining converges
(no tablet still lists the member as a replica), an operator (or an
automated follow-up action) proposes `MetaCommand::RemoveMember`, applied
under three preconditions — the member exists, its status is
`Leaving`/`Down` (never mid-service — removing an `Active` member could
strand a tablet's replication factor with no warning), and no
`Metadata.tablets[*].replicas` still names it (the same invariant `drain`'s
own reconciliation is responsible for establishing first). On success the
member is pruned from `Metadata.members`, and its entries in
`Metadata.node_addrs` and `Metadata.cp_member_addrs`/`cp_member_tablets` are
pruned in the same apply (mirroring the existing ADR 0024 GC discipline for
tablet-scoped `cp_member_addrs` entries — keyed on current absence so a
replayed historical state can't resurrect a removed member).

## Consequences

- Closes ADR 0030's own documented residual gap (a pre-growth node's
  `client_route`/admin peer list as a process-start-only snapshot) as a
  side effect of PR1, independent of whether PR2's `join` flow is ever used.
- `RegisterCpAddr`/`cp_member_addrs` remain load-bearing for the internal
  `raftkv` peer book's WAL back-compat and are not deprecated — they are
  simply no longer the *only* source for that book, and no longer proposed
  by `animusd`'s own startup path (which now proposes the superset
  `RegisterNodeAddrs` instead).
- The `is_relayable_command` allowlist gains `RegisterNodeAddrs` (a
  follower-connected node must be able to relay its own self-registration to
  the control leader — the same bimodal-failure shape the root `CLAUDE.md`
  documents for every prior addition to this allowlist).
- PR2/PR3 build directly on PR1's `node_addrs` map and `route_sync_loop`
  without needing further plumbing changes to `ClientCtx`/`peer_sync_loop` —
  a join reaches every existing node's address book the same way growth
  already does, and a decommission's address-pruning reuses the exact GC
  shape ADR 0024 already established for `cp_member_addrs`.
- **PR2 landed** (2026-08-07): `animusd join`, `ClientRequest::JoinInfo` /
  `ClientResponse::JoinInfo`, `run_node_join`, and the collision guard — see
  its own decision section above for the two shape deviations from this
  ADR's original sketch (a required `--node` index instead of
  auto-derivation; member self-registration folded into
  `BoundNode::start_with`'s existing growth-node block rather than a
  separate step in `run_node_join`, which also means `run_node_growth`
  self-registers automatically now). `tests/seed_join.rs` covers the happy
  path (a 4th node joins off only the 3-node core's client addresses, no
  expanded config anywhere in the test, becomes `Active`, gains a real
  tablet replica via rebalancing, serves reads/writes through its own
  address and through a core node, and appears in an original node's
  `/admin/peers`), the collision guard (a second join at the same index with
  different addresses fails with `AlreadyExists`, cluster unharmed), and
  rejoin (shut the joined node down, join again at the same
  index/addresses/dir, recovers and serves — no collision-guard error, since
  an identical address book is a rejoin). PR3 (decommission) remains not yet
  implemented.
