# Heartbeat send-site map (ADR 0044 phase 2, C-02 PR 1)

Investigation-only document. No production behaviour changes here — see
`docs/roadmap.md`'s C-02 entry and ADR 0044's "cheap-groups roadmap"
follow-up 2 ("Heartbeat amortization") for the plan this feeds.

Location note: the repo has no `docs/notes/` directory and no prior
"investigation PR" convention beyond flat `docs/<topic>-notes.md` files
(`docs/streams-notes.md`, `docs/wal.md`) that document an already-shipped
mechanism's design in an ongoing way. Since this document is closer to a
one-shot design investigation than an ongoing mechanism reference, it lives
under a new `docs/design/` directory per the task's own default, rather than
forcing it into the `-notes.md` convention.

All citations below are `file:line` on this commit's tree
(branch `claude/next-high-impact-improvement-tvc5uv-23`).

## 0. Terminology clash, resolved up front

There are **two, unrelated, identically-named "heartbeat" mechanisms** in
this codebase. Keeping them apart is the first thing this map has to do,
because ADR 0044's own phase-2 wording ("Coalesce liveness traffic per node
pair... never pay a per-group heartbeat cost") and the roadmap's C-02 entry
("`HEARTBEAT_INTERVAL` users across `animus-control`'s driver and
`animus-cp-data`'s host module") both compress the two together:

1. **The Raft protocol's own heartbeat** — an empty (or non-empty)
   `AppendEntries`, emitted by `RaftCore::tick` whenever `heartbeat_deadline`
   elapses (`crates/animus-control/src/raft.rs:1857-1886`), at
   `heartbeat_interval = Duration::from_millis(50)`
   (`crates/animus-control/src/raft.rs:854`, the field default;
   declared `crates/animus-control/src/raft.rs:673`). `RaftCore` is generic
   over the command/state-machine type (ADR 0009/0016) and is instantiated
   **once** for the whole cluster by the control plane, and **once per
   hosted tablet group** by `animus-cp-data::RaftKvNode` (ADR 0017). This is
   the mechanism ADR 0044 phase 2 is actually about: it multiplies with the
   number of tablet groups a node leads, because each group runs its own
   independent `RaftCore`/consensus-loop task.
2. **The ADR 0012 node-liveness heartbeat** — a separate, lightweight,
   content-free "I'm alive" ping (`RaftMsg::Heartbeat { node: NodeId }`,
   declared `crates/animus-control/src/raft.rs:300` — a pure no-op for
   `RaftCore::handle` itself, `raft.rs:2053`) sent by
   `animus_control::node::send_heartbeat`/`heartbeat_loop`
   (`crates/animus-control/src/node.rs:863-889`) at
   `HEARTBEAT_INTERVAL = Duration::from_millis(100)`
   (`crates/animus-control/src/node.rs:87`), from **every** node to
   **every** control-plane node, feeding `FailureDetector` (ADR 0012,
   `crates/animus-control/src/detector.rs`) so the control leader can mark a
   member `Down`/`Active`. This is a **per-node**, not per-group, signal —
   it already doesn't multiply with tablet-group count, so it is **not** a
   C-02 target (see §3).

The roadmap's "`HEARTBEAT_INTERVAL` users" phrasing literally names
mechanism 2's constant (`animus_control::node::HEARTBEAT_INTERVAL`), but the
cost problem ADR 0044 phase 2 describes ("never pay a per-group heartbeat
cost once a node pair hosts many groups together") is entirely about
mechanism 1, which uses a **differently-named, differently-scoped**
constant (`RaftCore::heartbeat_interval`, a struct field, not `pub const`).
Both are mapped below for completeness; §5's cost model and §6's batcher
design are about mechanism 1 only.

**A third, related clarification on the roadmap's own wording**: "across
`animus-control`'s driver and `animus-cp-data`'s **host module**" is not
quite where the per-group send site lives. `animus_cp_data::host` (`host.rs`)
is the per-node tablet-host **reconciler** (ADR 0031) — it decides which
tablet groups to host/reconfigure/release on this node, but it does not run
the per-group consensus loop or send any Raft message itself. The per-group
`RaftCore::tick`-driven send site is in `animus_cp_data::lib.rs`'s `drive`
function (§2 below) — one independent async task per hosted group, spawned
by `RaftKvNode::start*`, not by `host.rs`. This map corrects that pointer;
see root `CLAUDE.md`'s engineering-practices note ("ADR/guide prose lags;
grep the code") for why this kind of drift is expected and not itself a bug.

## 1. Every heartbeat send site

### 1a. Control-plane Raft heartbeat (mechanism 1, single instance)

- **Component**: `animus_control::RaftNode<E>`'s consensus loop, function
  `drive` (`crates/animus-control/src/node.rs:903-925` for the signature;
  the timer-tick branch is analogous to the cp-data one described in §2,
  driving `RaftCore::tick`).
- **Timer/interval**: `RaftCore::heartbeat_interval` = 50ms
  (`crates/animus-control/src/raft.rs:854`).
- **Fires**: once per **cluster** (there is exactly one control `RaftCore`
  cluster-wide) — never multiplies with tablet/table count. Only the
  control-group's own leader ticks a live heartbeat_deadline; every other
  control voter is a Raft follower of this one group.
- **What it sends**: `RaftMsg::AppendEntries` (empty when idle, real
  entries when there's a metadata write in flight) to every peer **and**
  learner **and** any still-departing peer
  (`crates/animus-control/src/raft.rs:2956-2989`, `broadcast_append`); plus
  a `RaftMsg::TimeoutNow` to an armed leadership-transfer target once it has
  caught up (`crates/animus-control/src/raft.rs:2978-2987`).
- **Address**: `env.send(to, bytes)` — `PRIMARY_STREAM` (stream 0)
  (`crates/animus-control/src/node.rs:1097`).
- **Suppressed by**: quiescence is **never** enabled for the control plane
  (ADR 0048 fork G — "the control plane never quiesces"; `RaftCore::
  quiesce_after` defaults `None` and nothing in `animus-control::node` calls
  `enable_quiescence`), so this always ticks. Suppressed only by not being
  leader.
- **Depended on by**: control-group followers' own election timers
  (`election_deadline`, reset on receiving a valid `AppendEntries` from the
  current leader — `crates/animus-control/src/raft.rs:2265` `handle_append_entries`),
  `RaftCore::peer_last_contact` (control-voter liveness,
  `CONTROL_PEER_LIVENESS_TIMEOUT` = 500ms,
  `crates/animus-control/src/node.rs:114`), `last_leader_contact`/
  `leader_within` (the ADR 0020 `/admin/health` hysteresis gate,
  `crates/animus-control/src/raft.rs:1882` sets it on every heartbeat tick).
  **Not** a C-02 target — a single instance cluster-wide has nothing to
  amortize across (§3).

### 1b. Per-tablet CP-data Raft heartbeat (mechanism 1, one instance per hosted group) — THE C-02 TARGET

- **Component**: `animus_cp_data::RaftKvNode<E, S>`'s consensus loop,
  function `drive` (`crates/animus-cp-data/src/lib.rs:8949`). One
  independent instance of this async task is spawned **per hosted tablet
  group**, by `RaftKvNode::start*`/`start_inner`
  (`crates/animus-cp-data/src/lib.rs:2181-2294`) — called once per
  `(tablet, node)` pair the host reconciler (`host.rs`, ADR 0031) decides to
  host, via `HostAction::Host`/`MaterializeSplitChild`.
- **Timer/interval**: the **same** `RaftCore::heartbeat_interval` = 50ms
  (`crates/animus-control/src/raft.rs:854`) — `RaftKvNode` reuses
  `animus-control`'s generic, sync `RaftCore<KvCommand, KvState>` unchanged
  (ADR 0016), so it inherits the identical tick/heartbeat mechanics as §1a,
  just instantiated once per group.
- **Fires**: once **per hosted group this node currently leads**, every
  50ms, independently timed per group (each group's own `heartbeat_deadline`
  is set relative to that group's own last tick/propose — deadlines across
  different groups are not phase-aligned).
- **Trigger path**: `RaftCore::tick` (`crates/animus-control/src/raft.rs:1843-1887`,
  shared code, generic over `C`/`S`) called from the per-group `drive` loop
  at `crates/animus-cp-data/src/lib.rs:9487-9493`; its heartbeat branch
  (`raft.rs:1857-1886`) evaluates the ADR 0044 phase-1 quiescence entry
  predicate first (`quiesce_entry_ok`, `raft.rs:3017-3044`) and, if not
  quiescing, calls `broadcast_append(SnapshotResend::Always)`
  (`raft.rs:1886`, shared with §1a — `raft.rs:2956-2989`).
- **What it sends**: `RaftMsg::AppendEntries` (wrapped `KvWire::Raft`,
  `crates/animus-cp-data/src/lib.rs:9497`) to every voter, learner, and
  departing peer **of that one group** (`raft.rs:2956-2964`); plus
  `RaftMsg::TimeoutNow` if a leadership transfer is armed and caught up.
- **Address**: `env.send_stream(to, stream, codec::encode_wire(&wire))`
  (`crates/animus-cp-data/src/lib.rs:9557-9558`, the "immediate" branch;
  `:9562-9563` and `:9579-9580` are the durability-gated release paths for
  messages that *do* make a durability claim, e.g. vote grants and append
  accepts — per `ships_before_durable`'s own call-site comment,
  `crates/animus-cp-data/src/lib.rs:9541-9547`, an outbound `AppendEntries`
  from the leader — heartbeat or carrying real entries alike — always ships
  immediately regardless of the WAL's own durability round ("replication,
  heartbeats and pre-vote traffic go out immediately, which is what keeps a
  group alive across a slow `fsync`"), so a heartbeat always takes the
  `:9557-9558` branch) — `stream = self.stream`, which **is the tablet id**
  (`crates/animus-cp-data/src/lib.rs:2278` `stream,` field; ADR 0026's own
  convention, confirmed in `animus-cp-data/CLAUDE.md`'s `start_hosted` doc:
  "`stream` doubles as this group's tablet id"). So node A's heartbeat to
  node B for tablet 7 is a **distinct `(B, stream=7)` frame** from its
  heartbeat to B for tablet 12 (`(B, stream=12)`), even though both ride
  the **same pooled TCP connection to B** (§4).
- **Suppressed by**: (i) not being the group's leader; (ii) ADR 0048
  quiescence — once `quiesce_entry_ok` passes, the group ticks
  `RaftMsg::Quiesce` once and its own `next_deadline()` returns `None`
  (`raft.rs:1296-1315`), so the driver's `select` drops the timer arm
  entirely (`crates/animus-cp-data/src/lib.rs:9332` reads
  `c.next_deadline()`) — **zero** heartbeats for an idle group, by default
  after 5s of no activity (`animusd`'s `DEFAULT_QUIESCE_AFTER_SECS`, see
  `crates/animusd/CLAUDE.md`'s Quiescence section). C-02's whole premise is
  that quiescence does **not** help an **active** group — see §5.
- **Depended on by** (per group, independently):
  - the group's own followers' `election_deadline` reset (the group's OWN
    failure detection — see §3, this is the load-bearing coupling);
  - `RaftKvNode::engine_applied_index`/durable-before-visible confirm
    machinery is unrelated to heartbeats specifically (only to commit
    advancing, which piggybacks on the same `AppendEntries` when there's a
    real entry to replicate — the empty-heartbeat case carries no new
    commit information beyond `leader_commit`, but a follower still uses
    that `leader_commit` to advance its own `commit_index`);
  - `RaftKvNode::voter_history()`'s "last observed config" bookkeeping is
    driven by the same consensus-loop iteration, not specifically the
    heartbeat send;
  - `RaftCore::last_leader_contact`/leader-liveness self-proof
    (`raft.rs:1871-1882`, the issue #595 fix — every hosted group's own
    leader refreshes this on every heartbeat tick, though nothing in
    `animus-cp-data` currently reads it the way `/admin/health` reads §1a's);
  - **ReadIndex is a *separate* message pair, not carried by this
    heartbeat** — see §1c.
  - leadership transfer (`TimeoutNow`) — per group, piggybacked on this
    same `broadcast_append` call once a transfer is armed for that group.

### 1c. ReadIndex confirmation (a related but distinct per-group message pair)

- **Component**: `RaftKvNode::read_barrier` and the consensus loop's
  `KvWire::ReadProbe`/`ReadProbeAck` handling
  (`crates/animus-cp-data/src/lib.rs:1168-1172` variant declarations;
  `:5975-5978` probe send; `:9456-9479` receive/ack).
- **Timer**: none — sent on demand, once per linearizable read that needs a
  quorum confirmation (not on the 50ms heartbeat clock at all, though the
  same doc at `crates/animus-cp-data/src/lib.rs:5970` notes "periodic
  heartbeats would also carry [the leader's read barrier], but an
  [immediate probe is faster]").
- **What it sends**: `KvWire::ReadProbe { term, epoch }` /
  `KvWire::ReadProbeAck { term, epoch }`, one **per group**, same
  `(node, stream=tablet_id)` addressing as §1b
  (`crates/animus-cp-data/src/lib.rs:5978`
  `self.env.send_stream(p, self.stream, probe.clone())`).
- **Why it matters for a batcher**: `RaftCore::next_deadline`'s
  own routing comment (`raft.rs:10296-10300` region, "`ReadProbe`/
  `ReadProbeAck` are a ReadIndex barrier the `RaftCore` never even
  [sees]") shows this rides the *same wire/stream* as the Raft heartbeat
  but is **not** part of `RaftCore`'s own message set at all — a batcher
  that intercepts `RaftCore`'s `broadcast_append` output would **not**
  automatically catch this traffic; it is a separate, KvWire-level
  mechanism layered beside the Raft heartbeat, addressed identically. Out
  of scope for the batcher described in §6 (which only targets the
  `RaftMsg::AppendEntries` heartbeat), but worth naming because it uses the
  identical `(node, stream)` address and could, in principle, ride the same
  future per-node-pair frame if a later PR chooses to widen scope.

## 2. Node-liveness heartbeat (mechanism 2 — NOT a C-02 target)

- **Component**: `animus_control::node::send_heartbeat`/`heartbeat_loop`
  (`crates/animus-control/src/node.rs:863-889`), and `animusd`'s live-
  destination-list variant, `heartbeat_loop_live`
  (`crates/animusd/src/lib.rs:9892-9901`, importing the shared constant and
  `send_heartbeat` at `crates/animusd/src/lib.rs:123`).
- **Timer/interval**: `HEARTBEAT_INTERVAL = Duration::from_millis(100)`
  (`crates/animus-control/src/node.rs:87`) — a **different constant** from
  §1's `RaftCore::heartbeat_interval` (50ms), despite the identical name.
- **Fires**: once **per node** (not per group, not per table) — every node
  in the cluster (control, data, or combined role) runs exactly one
  `heartbeat_loop`/`heartbeat_loop_live` task for its whole process
  lifetime, regardless of how many tablet groups it hosts.
- **What it sends**: `RaftMsg::Heartbeat { node }` — a content-free liveness
  ping carrying no term/commit/leader information at all
  (`crates/animus-control/src/node.rs:871-873`), to every node in the
  `control` target list (every control-group member — 3 in a `--cluster 3`
  default; `heartbeat_loop_live` re-derives this list every tick from
  `ctx.control.config()`, `crates/animusd/src/lib.rs:9894-9896`, rather than
  a bring-up-time snapshot).
- **Address**: `env.send(c.clone(), bytes.clone())` — `PRIMARY_STREAM`
  (`crates/animus-control/src/node.rs:876`).
- **Suppressed by**: nothing — runs unconditionally on every node, forever
  (this is the ADR 0012 failure-detection substrate, and per ADR 0048's own
  Context section is explicitly **not** gated on quiescence, by design: "the
  ADR 0012 failure detector is already a node-level liveness layer, wholly
  independent of any tablet group's own Raft stream").
- **Depended on by**: `animus_control::FailureDetector`
  (`crates/animus-control/src/detector.rs`, a pure interval+timeout
  detector) — the control leader's `detect_loop` marks a member `Down`
  after `DETECT_TIMEOUT` (500ms, `crates/animus-control/src/node.rs:92`) of
  silence, re-evaluated every `DETECT_INTERVAL` (100ms,
  `crates/animus-control/src/node.rs:96`).
- **Why this is not a C-02 target**: it is **already** exactly the shape a
  per-node-pair batcher would produce — one message per node, per interval,
  to a fixed small target set (the control group), independent of tablet
  group count. There is nothing to amortize because it never multiplied
  with `G` in the first place. Listed here only because the roadmap's own
  text names its constant.

## 3. The crux: per-group vs. per-node-pair coupling

This is what a batcher for §1b must preserve. Two questions, forced apart:

**What a receiver infers from one `AppendEntries` heartbeat's *arrival*
(regardless of content)**:

- **Per-group.** Receipt resets **that group's own** `election_deadline`
  (`crates/animus-control/src/raft.rs:2265` `handle_append_entries`, shared
  code — a follower only resets its timer for the specific `RaftCore`
  instance the message was handled against). Since every tablet group is
  its **own independent `RaftCore`**, this is inherently per-group: a
  receiver cannot infer "group 7's leader is alive" from "group 12's
  leader (possibly the same physical node) just heartbeated me" — they are
  two unrelated consensus instances that happen to be co-located. **A
  batched frame must still let the receiver demux and apply this signal
  per-group** — merging the *transport* (one frame) must not merge the
  *semantics* (one election timer per group).

**What a receiver infers from the heartbeat's *content***:

- **Per-group, all of it**: `term`, `leader` (the `NodeId` field on
  `AppendEntries`, `raft.rs:224`), `leader_commit` (which the receiver uses
  to advance its **own group's** `commit_index`), `prev_log_index`/
  `prev_log_term` (the consistency check, `handle_append_entries`). None of
  these compose across groups — group 7's term has no relationship to group
  12's term, even between the same two nodes.
- **ReadIndex confirmation (§1c)** is per-group by construction (a quorum
  ack for *this group's* current read epoch) and rides a **separate**
  message from the Raft heartbeat entirely — not mergeable into the same
  batching without extending scope past what ADR 0044 phase 2 describes.
- **Leadership transfer (`TimeoutNow`)** is per-group (targets one specific
  group's own transfer).
- **Failure detection (ADR 0012, mechanism 2, §2)** is explicitly
  **per-node**, not per-group — this is the one signal that is *already*
  correctly amortized, and is unaffected by anything in this ADR's phase 2.

**What IS per-node-pair, and already amortized (nothing new to build)**:

- The **transport**: `ProdEnv` pools exactly one outbound TCP connection
  per destination *address*, regardless of how many `(node, stream)` logical
  addresses ride it (`crates/animus-env/src/prod.rs:708-752`
  `send_frame_pooled`; per-address `Mutex` held across the whole frame
  write, `:713` comment). A node with `G` groups leading toward the same
  peer already shares **one TCP connection** for all `G` groups' heartbeats
  — the connection itself is not the cost.

**What is NOT per-node-pair today, and is the actual C-02 target**:

- The **frame count**. Every group's heartbeat is still a **separate call**
  to `env.send_stream` (`crates/animus-cp-data/src/lib.rs:9557-9558`), each
  of which acquires the per-address connection lock, writes one complete
  length-prefixed frame (`[from_len][from][stream][len][payload]`,
  `crates/animus-env/src/prod.rs:788-799` `write_frame`), and releases the
  lock — **`G` separate lock/write cycles per interval per destination
  node**, not one. Batching means: for a fixed destination node, coalesce
  every co-hosted group's own small heartbeat payload
  (`stream_id, term, leader_commit, prev_log_index, prev_log_term` — or a
  cheaper "nothing changed since last tick" delta) into **one** frame, sent
  **once** per interval, and have the receiver demux it back into `G`
  per-group deliveries before anything touches the per-group timers/state
  above.

## 4. Cost model today

**Constants**: `heartbeat_interval` = 50ms ⟹ 20 heartbeat ticks/second per
led group (§1b). Default replication factor `RF` = 3
(`crates/animusd/src/lib.rs:2082` `MAX_REPLICATION_FACTOR`), so a group has
`P = RF - 1 = 2` peers.

**Per node, as leader of `G` groups** (steady state, no writes — pure
heartbeat traffic; a real write piggybacks on the same `broadcast_append`
call and does not add extra sends, only extra bytes per frame):

```
outbound AppendEntries / sec (this node, as leader)
    = G × P × (1000 / heartbeat_interval_ms)
    = G × 2 × 20
    = 40 × G
```

Each triggers one `AppendEntriesResp` back from the follower, so round-trip
message volume touching this node purely from leading `G` groups is
`≈ 80 × G` messages/sec (40G sent, 40G acks received) — before counting
this same node's own traffic as a **follower** of groups it doesn't lead
(received `AppendEntries` + sent `AppendEntriesResp`, symmetric cost on the
other end).

**Per node pair, `--cluster 3` defaults (RF = 3 = cluster size)**: every
tablet in a 3-node, RF-3 cluster has **all 3 nodes** as replicas — so every
node pair `(A, B)` co-hosts **every** tablet in the cluster, not just some.
For `T` total tablets, split leadership evenly (`T/3` each), the heartbeat
traffic flowing between any one pair `(A, B)` today is:

```
messages/sec between A and B (both directions, heartbeats + acks)
    = (T/3 groups A leads, B follows) × (1 AppendEntries + 1 Resp) × 20/sec
    + (T/3 groups B leads, A follows) × (1 AppendEntries + 1 Resp) × 20/sec
    = (T/3 + T/3) × 2 × 20
    = 80 × T / 3
```

— i.e. **linear in `T`**, the tablet count, for a *fixed* number of node
pairs (`C(3,2) = 3` pairs total in a 3-node cluster). After a per-node-pair
batcher (§6): each direction sends **one** combined heartbeat frame per
interval regardless of how many groups it carries, so the same pair's cost
becomes `2 × 20 = 40` messages/sec **flat**, independent of `T` — the
literal "scales with node pairs, not groups" property the roadmap names.

**A realistic `G`**: ADR 0067/W-08b's throughput-derived minimum tablet
count (`min_tablets_for`, `crates/animusd/src/min_tablets.rs:154-177`,
referenced from `animusd/CLAUDE.md`'s "Throughput-derived minimum tablet
count" entry) is, conceptually, `max(1, ceil(RCU/max_rcu + WCU/max_wcu))`
(the real implementation sums both terms over a common denominator before
rounding once, `min_tablets.rs:167-174`, to avoid double-rounding — same
result for round inputs) with defaults `max_rcu = 3000`, `max_wcu = 1000`.
A single moderately-provisioned table at 9,000 RCU / 3,000 WCU already
needs `ceil(9000/3000 + 3000/1000) = ceil(3+3) = 6` tablets; several such
tables, or one large table that has organically
byte-split (ADR 0034) many times over its lifetime and — per ADR 0044's own
"shrink-in-place" note — **never shrinks back down**, easily reaches
`G` in the tens to low hundreds per node on a small, long-lived cluster.
At `G = 50` (a realistic accumulated count for a cluster that's been
running and splitting for a while, well short of anything exotic): `40 × 50
= 2,000` outbound `AppendEntries`/sec from just one leading node's
heartbeat traffic, **before** any real write load. This is exactly ADR
0044's own framing: "a small cluster cannot hide per-group overhead behind
fleet scale" (`docs/adr/0044-split-only-tablets.md:163-166`) — the node
count stays fixed at 3 while `G` (and thus the per-pair message rate) keeps
growing with every split, forever.

**What ADR 0048 (phase 1, quiescence) already removes, and what remains**:
quiescence removes the *entire* heartbeat cost above for a group with no
proposals and no client traffic for `quiesce_after` (default 5s) — such a
group's `next_deadline()` returns `None` and it posts zero timeline events
(`crates/animus-control/src/raft.rs:1296-1315`, `1866-1869`). What
**remains**, and is what C-02 targets, is the cost model above for every
group that is **not** idle — an actively-written-to table's tablets, which
by construction can never quiesce while under load, still pay the full
`40 × G_active` per-node rate with zero amortization across the co-hosted
group count. Phase 1 and phase 2 are complementary, not overlapping: phase
1 zeroes the idle term, phase 2 flattens the active term's dependence on
`G`.

## 5. Candidate batcher shape (design sketch for PR 2 — not implemented here)

**As built (C-02 PR 2, 2026-09-06)**: shape 2 below (a per-node
`HeartbeatBatcher` in `animus-cp-data`) is what shipped, matching this
sketch closely — `animus_cp_data::heartbeat_batch::HeartbeatBatcher`'s own
module doc has the as-built design in full; the "Open questions carried
into PR 2" section at the end of this document records exactly how each
open question below was closed. This section is kept as the original
investigation record.

**Where the seam sits**: at the per-group send call site,
`crates/animus-cp-data/src/lib.rs:9557-9558` (and its two durability-release
siblings at `:9562-9563`/`:9579-9580`, though a heartbeat never takes the
gated path — see §1b's own citation of `ships_before_durable`'s call-site
comment at `crates/animus-cp-data/src/lib.rs:9541-9547`). Two shapes
considered:

1. **A `Network`-level batcher in `animus-env`.** Intercept every
   `send_stream(to, stream, payload)` call whose payload is (decodably) a
   bare Raft heartbeat, buffer it keyed by `to`, and flush one combined
   frame per destination per interval. **Rejected as the primary
   mechanism**: `animus-env` is deliberately protocol-agnostic (it moves
   opaque `Vec<u8>` payloads — "higher layers define their own message
   enums and (de)serialize... over the `Vec<u8>` payloads the `Network`
   moves," root `CLAUDE.md` Conventions) and has no business decoding
   `KvWire`/`RaftMsg` to recognize a heartbeat. It would also have no way to
   know *which* groups are idle/quiesced without reaching back into
   `animus-cp-data` state.
2. **A `HeartbeatBatcher` in `animus-cp-data`, one per node, shared across
   every hosted `RaftKvNode` instance on that node** (favored). Precedent:
   `ProdEnv`'s existing one-connection-per-destination pooling
   (`crates/animus-env/src/prod.rs:708-752`) is exactly this shape one
   layer down — a per-destination aggregation point below the per-group
   logic. Concretely:
   - Each `RaftKvNode`'s `drive` loop, on reaching its own heartbeat branch
     (today: `raft.rs:1857-1886` inside `RaftCore::tick`), would **not**
     call `env.send_stream` directly for a bare (no-entries) heartbeat.
     Instead it registers `(destination_node, this_group's_stream_id, term,
     leader_commit, prev_log_index, prev_log_term)` into a shared,
     per-destination-node batch buffer (`Arc<Mutex<BTreeMap<NodeId,
     Vec<GroupHeartbeat>>>>`, one entry per node this node has at least one
     group talking to). A real (non-empty) `AppendEntries` carrying log
     entries — the replication case, not the pure-heartbeat case — is
     **not** batched: it ships immediately exactly as it does today
     (unaffected by this change; the batcher is heartbeat-only).
   - A single new per-node task (spawned once, not once per group) wakes
     every `heartbeat_interval` and, for each destination with a non-empty
     buffer, sends **one** frame containing the buffered `Vec<GroupHeartbeat>`
     over a **new, dedicated stream id** reserved for batched heartbeats
     (distinct from every real tablet's own `stream = tablet_id`, so it
     can never collide — `animus-tablet`'s token/escape discipline already
     establishes the pattern of a reserved namespace no real key can
     produce; the same idea applies to stream-id space).
   - The **receiver** demuxes this one frame: for each `GroupHeartbeat`
     entry, look up the locally-running `RaftKvNode` for that `stream_id`
     (a node already tracks its own hosted groups by tablet id, via
     `ClusterEdgeState`'s `raftkv` registry in `animusd`, or the
     `Reconciler`'s own hosted set in `animus-cp-data::host` — either is a
     plausible lookup table) and feed it a synthesized per-group
     `AppendEntries` (or a smaller purpose-built "heartbeat" variant
     carrying just `term`/`leader_commit`/`prev_log_index`/`prev_log_term`)
     through that group's own `RaftCore::handle`, exactly as if it had
     arrived on its own stream. **This preserves every per-group semantic
     in §3** — each group's own election timer, term, and commit index
     still update independently; only the wire framing is shared.
   - **What flag gates it**: an opt-in `RaftCore`-adjacent or `RaftKvNode`-
     level toggle, mirroring `enable_quiescence`'s own additive-default
     shape (`quiesce_after: Option<Duration>`, defaulting `None` — "today's
     behavior exactly, byte-for-byte," ADR 0048's Decision section). A
     `--heartbeat-batch` (or similar) CLI/config flag, off by default for
     PR 2, matching the stack's own "(2) batcher behind a flag; (3)
     cutover" plan.
   - **How `SimEnv` stays deterministic**: the batcher's own wake timer is
     an ordinary `env.sleep`, so it posts one `TraceEvent::Timer` per node
     per interval — no new RNG draw, no wall clock. The receive-side demux
     is a synchronous dispatch loop over already-decoded data, no new
     nondeterminism. `SimEnv`'s multiplexed `(node, stream)` addressing
     (`crates/animus-sim/src/lib.rs`, ADR 0026 section of that crate's
     `CLAUDE.md`) already supports an arbitrary reserved stream id with no
     new machinery.
   - **How the failure detector and election timers keep their semantics**:
     unaffected by construction — §3 already established that election-
     timer reset and ADR 0012 liveness are two different mechanisms; this
     batcher only ever touches §1b's per-group Raft heartbeat, and the
     demux step re-delivers each group's own signal to its own `RaftCore`,
     so a follower's election timer resets on the same logical event
     (a valid heartbeat from its current leader), merely delivered inside a
     shared frame instead of its own.
   - **Open questions for PR 2**:
     - Does a batched heartbeat still count once per group toward
       `Metric::CpAppendEntriesSent` (so §7's corpus and any dashboard
       reading it keep meaning "logical per-group heartbeats delivered"),
       or does it need a **second** metric for "physical frames sent" so
       the amortization is itself observable? (Recommend: keep
       `CpAppendEntriesSent` counting logical per-group heartbeats
       unchanged — it is what proves per-group semantics are preserved —
       and add a new counter for physical batched frames, so PR 3's test
       can assert the first stays proportional to `G` while the second
       goes flat.)
     - Should the batcher's own wake cadence track the **fastest** active
       group's `heartbeat_deadline` exactly (today: every group's deadline
       is independently timed, so a naive shared-interval wake could
       desync from any one group's own deadline bookkeeping — e.g. a
       group's `quiesce_entry_ok` check, which is evaluated *at* the
       heartbeat deadline, `raft.rs:1857-1869`) or does quiescence's own
       per-group entry decision need to move out of the batched path
       entirely (a quiesced group should still contribute nothing to the
       batch, which is a simple filter, but the group that is the *last*
       non-quiesced one sharing a destination needs its own quiesce
       decision to still fire correctly on schedule)?
     - How does `SnapshotResend`/`InstallSnapshot` interact — those are
       explicitly **not** heartbeats and already ship on their own,
       unbatched (`broadcast_append`'s `snapshot_resend` parameter,
       `raft.rs:2956`) — confirm the batcher's own filter (heartbeat vs.
       real replication) correctly excludes anything snapshot-related,
       which today rides the identical `RaftMsg::AppendEntries`/`Out`
       enum but is not a "nothing changed" heartbeat.
     - Where does `TimeoutNow` (leadership transfer, piggybacked on
       `broadcast_append` today) go — batched alongside, or always
       unbatched (it's rare, latency-sensitive, and per-group already)?
       Recommend: always unbatched, ship immediately as today.
     - Receiver-side demux needs a hosted-group lookup that is itself
       cheap and lock-light, given it runs once per heartbeat interval per
       destination on the hot path — reuse whichever registry `host.rs`'s
       `Reconciler` already maintains rather than building a second one.

## 6. Test plan for PR 2 / PR 3

**As built (C-02 PR 2)**: `crates/animus-cp-data/tests/
heartbeat_batch_corpus.rs`, seed knob `ANIMUS_HEARTBEAT_SEEDS` — cells (a)
frame-vs-logical scaling, (b) every per-group invariant holding under
batching, (c1) a genuine partition losing a whole batched frame still
elects correctly, (c2) a lossy-but-connected link doesn't spuriously
elect, (d) an unknown group in a received frame is dropped and counted,
(e) leader kill and follower kill both converge with batching on. Not a
separate file from `heartbeat_cost.rs` (PR 1's baseline, which stays as
the flag-off scaling proof) — this file's own module doc explains why it
co-hosts groups in one `Simulator` rather than PR 1's independent-worlds
shape. See the ADR 0044 phase-2 amendment for cell (a)'s measured numbers.

**Original plan below** (kept for the record — cell numbering/shape
differs slightly from what shipped, noted above):

**Corpus location**: a new `SimEnv` integration test file in
`animus-cp-data/tests/` (mirroring `tests/quiescence.rs`'s exact harness
shape — `RaftKvNode::start_with_metrics`/`start_hosted` per node, one
`MetricsHandle::recording()` per physical node id, shared across every
hosted group so counts aggregate exactly as a real node's shared metrics
sink would). Seed knob: **`ANIMUS_HEARTBEAT_SEEDS`**, defaulting to 1,
following every other corpus's `corpus::seeds_from_env` pattern
(`animus-test::corpus`).

**How to count sends**: `Metric::CpAppendEntriesSent`
(`crates/animus-cp-data/src/lib.rs:6248`, incremented once per outbound
`RaftMsg::AppendEntries` regardless of whether it carries real entries or
is a bare heartbeat) is already exactly the right counter for "logical
per-group heartbeats" (§5's recommendation is to keep its meaning
unchanged post-batcher). §7's baseline test (below, landed in this PR)
already proves this counter is usable and how to read it across several
independently-hosted groups sharing one set of physical node ids. PR 2/3
would add the "physical frames sent" counter recommended in §5's open
questions, read the same way.

**Scenario cells** (mirroring `tests/quiescence.rs`'s numbered-property
style):

1. **Baseline (this PR, §7)**: `CpAppendEntriesSent`, summed per
   destination node, scales linearly with the number of actively-hosted
   (non-quiesced) groups sharing that destination — proportional to `G ×
   P`. This is the PRE-BATCHER assertion.
2. **Flat post-batcher (PR 3, flips cell 1's own assertion)**: with the
   batcher flag on, the same workload's **physical frame count** (the new
   counter from §5) stays flat as `G` grows for a fixed node-pair count,
   while `CpAppendEntriesSent` (the logical count) keeps scaling with `G` —
   proving amortization happened at the wire layer without losing any
   group's own logical heartbeat delivery.
3. **Per-group semantics preserved under batching**: with the flag on,
   every group's own follower still resets its own election timer only on
   receiving *that group's* heartbeat (not a sibling group's) — a targeted
   partition of one group's traffic (via `NetConfig`/link-level partition,
   `animus-sim`'s `set_link_net_config`) while a co-hosted sibling group
   keeps flowing must still make the partitioned group, and only that
   group, time out and hold an election, while its sibling stays stable.
   This is the test that would catch a batcher that accidentally coalesces
   *semantics*, not just *transport*.
4. **Quiescence interacts correctly**: a mix of quiesced and active
   co-hosted groups sharing one destination — the batcher's own frame must
   carry only the active groups' heartbeats (a quiesced group contributes
   nothing, matching `tests/quiescence.rs`'s own property (ii) exactly),
   and un-quiescing one group (a write) must not perturb its already-quiesced
   siblings.
5. **Snapshot/replication traffic unaffected**: a group mid-`InstallSnapshot`
   or actively replicating real log entries (not idle heartbeats) ships
   exactly as many `AppendEntries`/`InstallSnapshot` messages as it does
   today, batched or not — the batcher only ever touches the
   nothing-to-replicate heartbeat case.
6. **Leadership transfer unaffected**: `TimeoutNow` for an armed transfer on
   one group still arrives promptly regardless of whether that group's
   destination also has batched heartbeat traffic in flight.
7. **Determinism**: same seed, same trace, batcher on or off — the
   batcher's own timer is an ordinary `env.sleep`, so this should hold by
   construction (`animus-sim/CLAUDE.md`'s existing determinism invariants);
   worth a direct `sim.trace_lines()` equality assertion per
   `tests/stream_addressing.rs`'s own `sustained_interleaved_writes_stay_
   isolated_and_reproducible` precedent.

## 7. Baseline measurement (landed in this PR)

`crates/animus-cp-data/tests/heartbeat_cost.rs`,
`append_entries_sent_scales_with_hosted_group_count_not_node_pairs`: hosts
1 group, then 5 groups, each an independent 3-node
`RaftKvNode<SimEnv, MemoryEngine>` group, all recording into the **same**
three `MetricsHandle`s (index-aligned by node id 0/1/2) — modeling "one
physical node's shared metrics sink aggregates every hosted group's own
traffic" exactly the way a real node's `env.metrics()` does for every
`RaftKvNode::start_hosted` call on it (see the file's own module doc for
why this needed no production-code change: `RaftKvNode::start_hosted`,
the real per-node-multi-group constructor ADR 0026 built, calls
`env.metrics()`, which is a no-op under `SimEnv` — `crates/animus-env/src/lib.rs:648`
— so an *observable* multi-group-on-one-node fixture would need a new
`start_hosted`-with-injectable-metrics constructor, which is exactly the
"production change" this PR's scope excludes; using `start_with_metrics`
across several independent `Simulator` worlds sharing one metrics-handle
triple sidesteps that with zero `src/` changes).

Runs each group's own settle window (1s: election + no-op replication) then
an idle window (5s: ~100 heartbeat ticks at the default 50ms interval),
with quiescence **never enabled** — deliberately, since quiescence is ADR
0048's already-shipped mitigation for the *idle* case and this baseline is
about the *active/ticking* cost C-02 exists to reduce (§4's "what remains"
paragraph). Asserts the ratio of summed `Metric::CpAppendEntriesSent`
between the 5-group and 1-group runs falls in `4.0..=6.0` — i.e.
genuinely scales with `G`, not flat — with generous bounds absorbing
election-settle jitter and independent-`Simulator`-world timing drift.
This is exactly the assertion §6 cell 2 (PR 3) flips to "flat as `G`
grows."

## Open questions carried into PR 2 — resolved, as built (C-02 PR 2)

Every question below is now closed by the landed `animus_cp_data::
heartbeat_batch` module (ADR 0044's 2026-09-06 phase-2 amendment has the
full account; this section just closes the loop on each bullet):

- **Receiver-side demux lookup table**: owned inside `animus-cp-data`
  itself (`HeartbeatBatcher`'s own `stream -> HeartbeatInbox` map,
  populated by each `RaftKvNode`'s own `drive` loop at start/teardown) —
  **not** `host.rs`'s `Reconciler` state or a new `animusd`-side index.
  This keeps the demux reachable under a bare `SimEnv` test with no
  `animusd` in the loop.
- **`CpAppendEntriesSent`'s meaning**: stayed "logical per-group
  heartbeat," as recommended — a batched heartbeat is still counted there,
  at `HeartbeatBatcher::register`, since it never reaches the ordinary
  outbound-send accounting. Two new counters, `Metric::
  CpHeartbeatFramesSent` (physical frames) and `Metric::
  CpHeartbeatDemuxDropped` (an unknown-group frame, dropped not delivered),
  observe the batcher's own physical-frame behavior.
- **Reserved stream id**: a constant, `HEARTBEAT_BATCH_STREAM = u64::MAX -
  2`, mirroring `PRIMARY_STREAM`/`SEGMENT_STREAM`/`BACKUP_SEGMENT_STREAM`'s
  own reserved-value convention exactly, as anticipated — both ends of a
  link must agree on it, so a locally-chosen value was never viable.
- **ReadIndex's `ReadProbe`/`ReadProbeAck` traffic**: stayed out of scope
  for phase 2, as planned — the batcher only ever intercepts a bare
  `RaftMsg::AppendEntries` from `RaftCore::tick`'s own heartbeat branch,
  never the separate `KvWire::ReadProbe`/`ReadProbeAck` messages. Left
  named here for a possible later phase, unchanged from PR 1's own note.

Additionally, two decisions the design sketch (§5) did not pose as
explicit open questions but PR 2 still had to make: **response direction**
(`AppendEntriesResp` for a demuxed heartbeat ships back on the responding
group's own stream, individually — never re-aggregated into a return
batch, since the demux is a serial on-arrival dispatch with no natural
aggregation point the send side's own deadline-driven loop has) and
**flag reach** (`--heartbeat-batch`/`cluster_settings.heartbeat_batch`
threads through the identical wrapper chain `--quiesce-after` already
uses, including that knob's own same documented gaps on
`--cluster-control`/`--cluster-data`/`join`/`data --seed`). See the ADR
amendment for the full reasoning behind both.

**Measured** (§7's own baseline test methodology, now with the flag on):
`crates/animus-cp-data/tests/heartbeat_batch_corpus.rs`'s cell (a), with
every group's leader forced to the same physical node so leadership
doesn't spread as `G` grows — 1 group vs. 5 groups: `frames1=238`,
`frames5=238` (ratio 1.00, flat) against `logical1=238`, `logical5=1190`
(ratio 5.00, still scaling with `G`) — confirming §4's own prediction
exactly: physical frames flatten to per-node-pair while the logical
per-group count is unchanged.

## Closing note: C-02 PR 3, the cutover (2026-09-06)

The stack's own "(1) investigation; (2) batcher behind a flag; (3)
cutover" plan is complete. PR 3 flipped `--heartbeat-batch`/
`cluster_settings.heartbeat_batch`'s default from off to **on**
(`animusd::main::DEFAULT_HEARTBEAT_BATCH = true`), keeping `--no-
heartbeat-batch` as the field opt-out, and added the one proof PR 2 could
not give by construction — real-thread `ProdEnv` liveness
(`crates/animusd/tests/heartbeat_batch_liveness.rs`, mirroring `tests/
cp_quiescence.rs`'s own role: `SimEnv` proves logic and ordering, not real
OS-thread/timer scheduling). No change to the mechanism itself
(`heartbeat_batch.rs`) — every decision this document and the ADR's phase-2
amendment recorded stands unmodified.

`heartbeat_cost.rs` (§7's baseline) now measures the DEFAULT (batching-on)
behavior instead of the pre-batcher one — physical frames flat, logical
count still scaling with `G`, identical numbers to §6's own cell (a)
measurement above, since it is now the same shape proving the same claim
as today's default rather than an opt-in capability. The flag-off proof
this displaced moved into `heartbeat_batch_corpus.rs` as an explicit
opt-out cell. See the ADR's 2026-09-06 phase-2-cutover amendment for the
full record.
