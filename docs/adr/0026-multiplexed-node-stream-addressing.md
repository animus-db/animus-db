# ADR 0026 — Multiplexed `(node, stream)` addressing on the `Network` seam

- **Status:** Accepted — **Stages A and B both implemented.** Stage A (this
  document's original scope: the `stream` field + seam, `SimEnv`/`ProdEnv`
  implementations, determinism + concurrency proof) landed first; Stage B
  (migrating `animus-cp-data`'s `RaftKvNode` fully onto stream addressing,
  keyed by tablet id) landed as part of ADR 0028's shared-storage/
  single-command-split work. `Coresident`, the `ProdEnv` sibling pool
  (`CP_SIBLING_POOL`), and `cp_member_id`/`cp_base_id`/`cp_members_for`/
  `CP_SPLIT_ID_STRIDE` are **fully retired** for `animus-cp-data`/`animusd` —
  a tablet's CP group member id is simply its base `raftkv` id, at any split
  depth. See ADR 0028 for the shared-storage decision Stage B's completion
  was bundled with.
  **Amended by [ADR 0040](0040-self-minted-string-node-ids.md) (2026-08-11):**
  `(node, stream)` is now the **universal** address for every protocol
  instance this codebase runs, not just the CP data plane — a combined
  node's control-plane Raft rides `PRIMARY_STREAM` (stream 0) on the node's
  one identity, exactly the same env every hosted tablet's group rides its
  own stream on (ADR 0040 Decision A), closing the two-`ProdEnv`-per-node
  split this ADR's Stage B work didn't itself touch. `animus-env`'s
  `Coresident` trait and its `SimEnv`/`ProdEnv` impls (left in place by this
  ADR "in case a future capability needs the sub-trait pattern again") were
  **deleted outright** in ADR 0040 PR5 — the prediction that a future need
  might revive them never materialized, and the stream axis this ADR
  describes fully subsumed the one thing they were for.
- **Date:** 2026-08-06

## Context

`Network::recv` is scoped to one `NodeId` and is **single-consumer**: exactly one
task may `.await` a given node's inbox (root `CLAUDE.md`, "Cross-cutting gotcha —
a node's inbox is single-consumer"). This is fine as long as a physical node hosts
exactly one protocol instance per role. It stopped being fine the moment a single
physical node needed to host an *unbounded* number of same-role instances — one
per-tablet Raft group per split (ADR 0017 D) — because each new instance needs its
**own** inbox, and the seam's only tool for minting one is a whole new `NodeId`.

The escape hatch built for this (ADR 0017 D/#3b) is `Coresident`
(`crates/animus-env/src/lib.rs`): a `SimEnv`/`ProdEnv`-only sub-trait with one
method, `sibling(id) -> Self`, that mints a fresh env bound to a new `NodeId` on
the same physical node. It works, but it is a stack of compounding workarounds,
not a clean address space:

1. **`SimEnv::sibling`** is cheap — it just registers a new key in a map the
   simulator already owns (`crates/animus-sim/src/lib.rs`). No real cost, no
   bound.
2. **`ProdEnv::sibling`** cannot be that cheap: minting a new *inbox* in
   production means binding a new listening socket, which is `async` and
   fallible, while `sibling` must be `sync` and infallible (a split's apply path
   calls it from inside a driver loop, not from an `async fn` that could
   propagate an error cleanly through the Raft apply pipeline as designed today).
   The chosen fix (`crates/animus-env/src/prod.rs`) is a **pre-bound pool of
   spare listeners**, sized at `bind_with_pool` time
   (`animusd`'s `CP_SIBLING_POOL = 64`, `crates/animusd/src/lib.rs`). `sibling`
   pops a slot synchronously.
3. **The pool has a hard, unrecoverable ceiling.** Once it's empty, `sibling`
   **panics** — inside the split-hook task, so the failure mode is a panicked
   background task, not a propagated error. The tablet whose split needed that
   slot never gets a group; it is silently **leaderless** forever (writes to its
   range hang). This is a confirmed liveness cliff from the architecture audit:
   a workload that splits enough (bulk-seed + auto-split, deep recursive
   splits) *will* hit it unless the pool is sized ahead of time for a workload
   whose shape is not always known in advance.
4. Because each co-resident instance is a **distinct `NodeId`**, every place that
   needs to reason about "the tablet-1 replica on base node 7" has to
   *compute* that `NodeId` from the tablet id and the base id — `animusd`'s
   `cp_member_id(base, tablet) = base + tablet * CP_SPLIT_ID_STRIDE` (now in
   `crates/animusd/src/topology.rs`, extracted and unit-tested by PR #33) — and
   invert it back (`cp_base_id`) everywhere a group-internal id (a leader hint)
   must be resolved against state keyed in base ids (`client_route`,
   `Metadata.tablets[t].replicas`, `Metadata.members`). This is a whole
   translation seam that exists **only** because "which inbox does this
   instance have" and "what is this instance's stable placement identity" got
   conflated into one namespace (`NodeId`). The root `CLAUDE.md` engineering log
   has three separate entries about bugs this conflation caused (a depth-2
   compounding-derivation bug, a missing-inverse-translation bug, and the
   general "an id-translation seam must be applied in both directions" lesson).

The through-line: **a `NodeId` is being asked to mean two different things** —
"which physical/logical node" and "which inbox on that node" — and every
workaround above is compensating for the seam not having a second axis to
express the second meaning.

## Decision

We give `Envelope` (and `Network`) a second addressing dimension, **`stream:
u64`**, so "host a second protocol instance on the same node" becomes "open a
second stream on the node's existing inbox" — no new inbox, no minting, no pool,
no pre-sizing, no panic-on-exhaustion, and (once callers migrate, see Stage B+)
no id-translation seam, because a tablet's group member can simply **be**
`(base_node_id, tablet_id as stream)` instead of a derived `NodeId`.

### 1. Wire/trait shape

```rust
/// Stable identifier for a node in the cluster.
pub type NodeId = u64;

/// The default stream every pre-multiplexing call site is implicitly on.
/// Every existing single-instance protocol (the control plane, a non-split
/// tablet's CP group, everything before this ADR) sends and receives on this
/// stream, so it needs zero call-site changes.
pub const PRIMARY_STREAM: u64 = 0;

/// A message delivered to a node over the `Network`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    pub from: NodeId,
    /// Which logical stream on the destination node this message is for.
    pub stream: u64,
    pub payload: Vec<u8>,
}

#[async_trait::async_trait]
pub trait Network: Send + Sync {
    /// Hand a payload to the network for delivery to `to` on `stream`.
    async fn send_stream(&self, to: NodeId, stream: u64, payload: Vec<u8>);

    /// Await the next message addressed to this node on `stream`. `(node,
    /// stream)` is single-consumer — the same invariant `recv` always had,
    /// generalized from "per node" to "per node and stream".
    async fn recv_stream(&self, stream: u64) -> Envelope;

    /// Convenience: send on the primary stream (today's whole API surface).
    async fn send(&self, to: NodeId, payload: Vec<u8>) {
        self.send_stream(to, PRIMARY_STREAM, payload).await;
    }

    /// Convenience: receive on the primary stream.
    async fn recv(&self) -> Envelope {
        self.recv_stream(PRIMARY_STREAM).await
    }
}
```

`send`/`recv` become **default methods** implemented in terms of
`send_stream`/`recv_stream`. This is the crux of the backward-compatibility
story: `SimEnv` and `ProdEnv` only need to *implement* `send_stream`/
`recv_stream`; every existing call site in the codebase — every `env.send(to,
payload).await` and `env.recv().await`, in the control plane, `animus-cp-data`,
`animus-consensus`, the wire edges, every test — keeps compiling **unchanged**
and behaves **identically** (always stream 0, i.e. `PRIMARY_STREAM`). A grep for
`impl Network for` across the workspace found exactly three implementers
(`SimEnv`, `ProdEnv`, and one test double, `CrashEnv` in
`animus-storage/tests/lsm_group_commit.rs`) — the entire blast radius of the
trait-shape change.

### 2. `SimEnv`

`SimEnv`'s state re-keys its inbox structures from `NodeId` to `(NodeId, u64)`:

```rust
inboxes: BTreeMap<(NodeId, u64), VecDeque<Envelope>>,
recv_wakers: BTreeMap<(NodeId, u64), Waker>,
```

`send_stream` pushes a delivery event carrying `Envelope { from, stream,
payload }` (unchanged delay/jitter/drop/partition/crash modeling — the fault
model does not care about streams, it already keys on `(from, to)`). `fire_event`
delivers into `inboxes[(to, env.stream)]` and wakes `recv_wakers[(to,
env.stream)]`, exactly generalizing the existing single-stream logic. `crash`/
`stop` clear **every** stream belonging to a node (a node-prefix scan over the
tupled key, the same pattern `Disk::list` already uses for a node-prefix scan
over `(NodeId, String)`). No new RNG draw, no new timeline event shape — a
`(node, stream)` pair is just a wider dictionary key, so **the determinism
argument is unchanged**: the trace is still a pure function of `(scenario,
seed)`. It stays cheap and unbounded, matching `SimEnv::sibling`'s existing "just
a map key" cost model — opening the 10,000th stream on a simulated node costs
one more `BTreeMap` entry, same as `Coresident::sibling` costs one more entry
today.

### 3. `ProdEnv`

**Frame format.** The current wire framing on this branch (verified by reading
`crates/animus-env/src/prod.rs` directly, not assumed) is a fresh
`TcpStream::connect` per `send`, carrying one length-prefixed
`[from: u64][len: u32][payload]` frame; the receive side's accept loop spawns
one reader per accepted connection that loops `read_frames` over that
connection until EOF. (A separate, not-yet-merged branch,
`fix/env-dir-fsync-pooled-tcp`, changes the *connection* strategy to a cached
per-address stream with `TCP_NODELAY` — but leaves the *frame* format
unchanged. The design below is orthogonal to that change: a `stream` field on
the frame composes with either connection strategy, because demuxing happens
per-*frame*, not per-*connection*.) The frame gains one field:

```
[from: u64][stream: u64][len: u32][payload]
```

**Receive-side demux.** Each `ProdEnv` (and each `Coresident::sibling`, until
Stage B+ retires that path) owns a `Demux`:

```rust
struct Demux {
    queues: BTreeMap<u64, VecDeque<Envelope>>,
    wakers: BTreeMap<u64, Waker>,
}
```

behind `Arc<StdMutex<Demux>>`, plus one background **pump task** (spawned
alongside the existing accept loop, tracked in the same `tasks` abort-list so
`shutdown`/`shutdown_tasks` tear it down too) that drains the accept loop's raw
per-connection frames and files each into `queues[frame.stream]`, waking any
parked `recv_stream(frame.stream)`. `recv_stream(stream)` is a small
hand-rolled `Future` (the same shape as `animus-sim`'s `Recv`/`Sleep`): check
`queues[stream]` under the lock, pop if present, else register a waker and
return `Pending`.

**This is the self-review point (a)** — "does the design reintroduce a
single-consumer bottleneck at the demux layer?" **No**, and this is precisely why
`Demux` is keyed `BTreeMap<u64, VecDeque<Envelope>>` + `BTreeMap<u64, Waker>`
rather than one shared queue: two different streams' consumers never contend on
each other's data, only on the `StdMutex` guarding the *map structure* itself for
the handful of instructions needed to look up/insert one entry — the same
micro-contention every `BTreeMap`-behind-a-`Mutex` design in this codebase
already accepts (e.g. `SimState` itself, one lock for the whole simulator). It is
not a *serialization* bottleneck: nothing awaits while holding the lock, so
throughput on stream A is not gated on stream B's consumer being awake. If this
ever needs to scale past a single `Mutex`'s throughput (many thousands of
co-resident groups on one node hammering the network), sharding `Demux` by
`stream % K` is a local, additive change to this one struct — not a seam
redesign.

### 4. What this dissolves (and what it does *not*, yet)

**Dissolves the *rationale* for `Coresident`, but Stage A does not remove it.**
Once a consumer needs "a second protocol instance on this node," the answer is
now "open a second stream on the existing env" — pass a `stream: u64` into the
component instead of minting `sibling(new_node_id)`. Concretely (sketched, not
built in Stage A):

- `RaftKvNode<E, S>` would take a `stream: u64` (defaulting to `PRIMARY_STREAM`)
  and call `env.send_stream(peer, stream, msg)` / `env.recv_stream(stream)`
  instead of `env.send`/`env.recv` — still generic over `E: Env` (no
  `Coresident` bound needed at all for this part).
- `KvWire` messages already carry no addressing info beyond what `Envelope`
  gives them, so no wire-message change follows from this — see §5.
- The split hook (`in_band_split_hook`, `crates/animus-cp-data/src/lib.rs`)
  would call `env.clone()` (or hand the *same* env down) with a new stream id —
  no `Coresident` bound, no pool, no pre-sizing, no panic path. `CP_SIBLING_POOL`
  and the pool-exhaustion panic simply have no reason to exist once every
  co-resident group is a stream on the shared inbox rather than a distinct
  bound listener.

**Dissolves the *rationale* for the `base + tablet*STRIDE` member-id scheme**,
for the same reason: a tablet group's "member id" for routing purposes can be
the pair `(base_node_id, tablet_id)` — literally use the **tablet id as the
stream id** — instead of an arithmetically derived `NodeId`. Concretely, once
migrated:

- `cp_member_id(base, tablet)` / `cp_base_id(member, tablet)` in
  `crates/animusd/src/topology.rs` **disappear entirely** — there is no derived
  id to compute or invert, because routing state can carry `(base, tablet)`
  directly instead of a single collapsed `NodeId`. The whole class of bug this
  translation seam produced (the missing-inverse bug documented in the root
  `CLAUDE.md`, the depth-2 compounding-derivation bug) has no seam left to have
  a bug *in*.
- `cp_members_for(tablet, replicas)` simplifies from "map each base id through
  `cp_member_id`" to "the replica set *is* `replicas` — the tablet id lives in
  the stream, not the node id," i.e. it can shrink to `replicas.iter().collect()`
  or disappear if callers pass `(base, tablet)` pairs directly.
- `CP_SPLIT_ID_STRIDE` disappears (no arithmetic id space to keep wide enough to
  avoid collisions).

**Does not dissolve in Stage A**: none of the above is *implemented* yet.
`Coresident`, the `ProdEnv` sibling pool, `CP_SIBLING_POOL`, and every
`cp_member_id`/`cp_base_id`/`cp_members_for`/`.sibling(...)` call site in
`animus-cp-data` and `animusd` are **untouched** by this ADR's Stage A. Stage A
adds the seam; it does not yet migrate a single caller. This is a deliberate
staging choice (see Consequences) — swapping the addressing scheme under a
live, safety-critical Raft KV plane is a separate, carefully-sequenced change,
not something to bundle with the seam addition itself.

### 5. Backward compatibility / wire messages

**Every existing wire message type is untouched.** `RaftMsg`, `KvWire`,
control-plane `MetaCommand` proposals, `ClientRequest`/`ClientResponse` — all of
these are opaque `Vec<u8>` payloads the `Network` moves (per the root
`CLAUDE.md`: "Higher layers define their own message enums and (de)serialize
with `serde_json` over the `Vec<u8>` payloads the `Network` moves"). The `stream`
field lives in the **transport envelope**, one layer below any of those types,
so none of them change shape, and no `serde` schema anywhere changes. This is
what makes the change purely additive: it is invisible to every consumer that
only ever calls `env.send`/`env.recv` (the default methods), and it changes the
`ProdEnv` wire frame (an internal-only, same-binary-version protocol — this
system does not do rolling wire-compatible upgrades today) without touching any
serialized message.

## Self-review against failure modes

- **(a) Single-consumer bottleneck at the demux layer?** No — see §3; the lock
  only guards `BTreeMap` structure, not in-flight work, and sharding is a local
  follow-up if it's ever needed.
- **(b) Does a node's total stream count need to be bounded/discoverable, or can
  it grow unboundedly?** It can grow unboundedly, and — unlike the pool it
  replaces — **that is now actually fine, not a new hazard**: a stream is a
  `BTreeMap` entry (SimEnv) or a `VecDeque` + optional `Waker` (ProdEnv), not a
  bound OS socket. The old design's problem was never "unbounded co-resident
  groups," it was "unbounded co-resident groups against a *pre-sized* pool of
  OS-level resources (listening sockets)." Removing the pool removes the thing
  that needed pre-sizing. What *is* worth tracking, as an operational
  observability concern (not a correctness one, and explicitly **out of scope**
  for this ADR): an admin/metrics surface (ADR 0015/0020) could expose "live
  stream count per node" the way it already exposes Raft/LSM introspection, so
  an operator can see a node hosting an unusually large number of tablet groups.
  Deferred to whichever follow-up actually migrates a component onto streams
  (there's nothing to observe yet in Stage A, since nothing uses a second
  stream).
- **(c) Stream lifecycle — does a stream for a dropped tablet stop
  receiving, and does this need a new GC path?** (Tablet merge briefly
  raised the same question under ADR 0033; moot since ADR 0044 removed
  merge entirely — tablets are split-only.) It composes with the
  **existing** GC path (ADR 0024) rather than needing a new one: today, dropping
  a tablet already means `CpGroup::shutdown()` + tearing down that tablet's
  `Coresident` sibling env (`shutdown_tasks()`, never the pool-draining
  `shutdown()` — see `crates/animusd/src/lib.rs`'s `cp_gc_loop`). Once a tablet
  is a stream on the **parent's shared env** instead of its own sibling env,
  there is no separate env to tear down at all — "stop receiving for this
  tablet" becomes "stop calling `recv_stream(tablet_id)` and drop that stream's
  queue entry from `Demux`," which is strictly *less* teardown machinery than
  today's per-sibling env shutdown, not more. This ADR does not implement that
  simplification (it is part of the Stage B+ migration), but flags that the GC
  story gets simpler, not harder, and that no new lifecycle primitive needs
  inventing.

## Consequences

**Enabled:**

- Retires the confirmed liveness cliff: no pre-sized pool, no
  panic-on-exhaustion, no leaderless tablet from running out of pool slots.
- A path to deleting a whole class of id-translation code
  (`cp_member_id`/`cp_base_id`/`cp_members_for`/`CP_SPLIT_ID_STRIDE`) and the bug
  class it produced (documented three times over in the root `CLAUDE.md`
  engineering log).
- Lets "how many protocol instances can this node host" stop being a
  capacity-planning question answered by a constant (`CP_SIBLING_POOL = 64`)
  baked into `animusd`.

**Costs accepted:**

- A broader seam change (touches `animus-env`'s trait shape, both `Env`
  implementations, and — for Stage B+ — `animus-cp-data` and `animusd`), even
  though Stage A itself is small and purely additive.
- `ProdEnv`'s `Network` implementation gains real complexity (a background pump
  task + a `Demux` per env) where before it was "one `mpsc` receiver behind a
  `Mutex`." This is inherent to genuinely multiplexing a transport, not
  incidental.
- The wire frame format changes (`[from][stream][len][payload]`), which is fine
  today (no rolling-upgrade requirement) but is worth remembering if that ever
  changes.

**Explicitly out of scope for this PR (follow-up work):**

1. ✅ **Done (ADR 0028).** Migrating `RaftKvNode` fully onto stream addressing —
   every tablet a node hosts, not just split children, uses `start_hosted(env,
   ..., stream)` with `stream = tablet_id` on the node's one `raftkv` env.
2. ✅ **Done (ADR 0028).** `Coresident`/the `ProdEnv` sibling pool/
   `CP_SIBLING_POOL` are removed from `animus-cp-data` and `animusd` — every
   caller migrated in the same change that made tablet split control-plane-only
   (there was no more reason to mint a sibling inbox once every tablet uses a
   stream on the shared env instead).
3. ✅ **Done (ADR 0028).** `cp_member_id`/`cp_base_id`/`cp_members_for`/
   `CP_SPLIT_ID_STRIDE` are deleted; `animusd`'s CP routing tables (`client_route`,
   the edge's `CpGroup` registry) are keyed on the plain base `NodeId` — no pair
   type needed, since a tablet's group member id *is* the base id now (the
   tablet axis lives in the `stream`/`StorageScope`, not in the id).
4. **Stream-count observability** (self-review point (b)) — an admin/metrics
   surface for live stream count per node, useful once something actually opens
   more than one stream.
5. Sharding `Demux` if a single `Mutex` per env is ever shown to bottleneck a
   real many-stream workload (self-review point (a)) — no evidence this is
   needed yet.

This ADR builds on ADR 0003 (the `Env` seam), ADR 0009 (the `Env`-driven Raft
core), and ADR 0017 (the per-tablet Raft data plane, which is `Coresident`'s
only consumer today); it does not supersede ADR 0017's decisions, only offers a
narrower replacement for one mechanism (`Coresident`) it introduced.
