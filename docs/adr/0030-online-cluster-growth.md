# ADR 0030 — Online cluster growth: admin add-member + heartbeat-driven activation

- **Status:** Accepted — implemented in `animus-control`, `animusd`. Amends ADR
  0005/0012's placement + failure-detection reconciler and ADR 0029's automatic
  rebalancer.
- **Date:** 2026-08-07

## Context

ADR 0029 taught the cluster to spread existing tablets across `Active` members
automatically — but only once those members are registered at all. Auditing the
registration path found there was **no online growth path whatsoever**:

- The only `UpsertMember` proposers were `bootstrap` (leader-only, idempotent,
  registers exactly the *startup config's* `raftkv` ids) and `admin_drain`
  (marks an *existing* member `Leaving`). Once a `--config`/`--node` process
  starts with `n` nodes, `bootstrap` only ever knows about `raftkv_id(0)
  ..raftkv_id(n)` — a node added to the running cluster afterward is never
  proposed as a member by anyone, so it never becomes `Active`, and the
  placement reconciler / ADR 0029 rebalancer never consider it. ADR 0029's own
  grow-test (`cp_rebalance.rs`) starts all 5 nodes up front — it proves the
  *planner*, not growth.
- Separately, `bootstrap` registered every declared id `Active` *unconditionally*,
  with no liveness check, and the failure detector only judges members it has
  *heartbeated at least once* (ADR 0012). A declared-but-never-booted node was
  therefore a **permanent phantom**: `Active` forever, eligible for placement,
  with tablet groups seated on it sitting permanently under quorum.

The control plane's Raft group (`animus-control`'s `RaftCore`) has never
supported runtime membership change (ADR 0009: "the control plane never
reconfigures, so its config stays `= initial_config` always"). Growing *that*
group safely (`RaftCore::change_membership` exists and is exercised by the
per-tablet CP-data plane, ADR 0029 §1, but never by the control plane) is a
materially larger, riskier lift than this gap warrants, so it is out of scope
here — see the *Scope decision* below.

## Scope decision

**The control group stays static.** Online growth is **data-plane only**: new
`raftkv` members become real, heartbeating, placement-eligible cluster members
and receive real tablet replicas, but the *control* Raft group (the metadata
consensus group, ids `0..N` at the process's original bring-up) never grows.
This is deliberately narrower than "add a node with full control-voter
status" — see the *Investigation* section for why that would require either
`RaftCore::change_membership` on the control plane (explicitly out of scope)
or a coordinated restart of the whole pre-growth cluster (not "online").

## Investigation

### 1. The `Down` → `Active` promotion chain (verified, not just assumed)

`FailureDetector::observe(node, now)` (`animus-control/src/detector.rs`) starts
tracking a member on its **first** heartbeat and reports it alive from that
same instant (`last_seen.entry(node).or_insert(now)`). `detect_loop`'s pure
helper, `liveness_transitions`, maps `(Down, alive=true) → Active`
unconditionally (unlike the `Active → Down` transition, this one is **never**
gated by the post-election `LEADER_GRACE` window — a heartbeat is positive
evidence with no false-positive risk, ADR 0012). So: register a raftkv id
`Down`, let it heartbeat once, and the very next `detect_loop` tick (100ms)
proposes its promotion to `Active`. No changes were needed to this chain — it
already worked exactly as required; this ADR only *adds a caller* that
registers `Down` in the first place (see §2) and hardens what happens if no
heartbeat ever arrives (see §3).

### 2. Address propagation: the existing `RegisterCpAddr`/`cp_member_addrs` +
peer-sync loop turned out to be *architecturally the easy half* — the hard
half was **write path reachability from a node with no real control
membership** (see §4). Once a growth node's proposals reach a real control
leader at all, its own `register_cp_addr` self-registration (already
unconditional in `BoundNode::start_with`, unchanged) writes its `raftkv`
address into the same replicated `Metadata.cp_member_addrs` every other node
uses, and every node's existing `peer_sync_loop` (`animusd`) picks it up the
same way it already does for a split sibling or a repair spare — no new
mechanism needed there.

### 3. Non-voter control id — verified, and found insufficient on its own

A `RaftCore` whose id is not in the `all_nodes` it was constructed with is a
**permanent, harmless non-voter**: `is_voter()` (`config.contains(&self.id)`)
is `false` forever, and both `start_election`/`start_pre_vote` gate on it —
"a node removed from the configuration must not campaign… it stays a quiet
follower" (the exact safety property an already-removed voter relies on, now
reused for a node that was simply *never added*). This makes a control-plane-
follower-less role structurally safe to run. But it is **not sufficient on its
own**: the real control leader's own peer set is derived from *its* `config`,
which never learned of this id, so it never targets it with `AppendEntries` —
a non-voter role's local `Metadata` never updates via real Raft replication,
no matter how long it waits. Confirmed by construction (see `raft.rs::
apply_config`, `RaftCore::new`) and by `Node::bind`/`BoundNode::start_with`'s
existing structural requirement that every node has a `ClientCtx.raft:
RaftNode<ProdEnv>` — "no control role at all" is not viable without a much
larger refactor than this slice warrants (see *Consequences*).

**Superseded by ADR 0035:** that larger refactor is now decided. ADR 0035
replaces `ClientCtx.raft`'s bare `RaftNode<ProdEnv>` with a `ControlHandle`
seam (`Local`/`Remote`) precisely so a data node can run with **no control
role at all** — not even the non-voter shape verified above — reusing this
section's own `remote_metadata_sync_loop`/`effective_metadata()` mechanism as
the *only* way a data node ever sees `Metadata`, generalized from "growth-node
fallback" to "how every data node syncs."

### The two things this slice adds (§4, §5)

4. **A metadata mirror for a genuinely non-participating node.** A node
   started via the new `run_node_growth` entry point (`animusd`) passes the
   **pre-growth** control group as `control_ids` — a real, permanent,
   structurally-safe non-voter (§3) — and `BoundNode::start_with` detects this
   (`!control_ids.contains(&self.control_id)`) and spawns
   `remote_metadata_sync_loop`: it polls `ClientRequest::Status` against one of
   the pre-growth control nodes' client addresses (resolvable because the
   growth node's own `client_route`, built from the *expanded* config it
   starts with, already lists them) and caches the result in
   `ClientCtx::remote_metadata`. `ClientCtx::effective_metadata()` is a thin
   read that prefers this mirror when populated, else falls through to
   `self.raft.metadata()` (a no-op for every other node — `remote_metadata`
   stays `None`). The handful of call sites that must work correctly on a
   growth node — CP routing (`tablet_for`, `resolve_cp_route`), the per-node
   join-host loop (the mechanism that actually stands up a tablet replica once
   placed there), `peer_sync_loop`, `register_cp_addr`'s own commit
   confirmation, and `/admin/status` — read through it.
5. **A write-path fallback for the same node.** `ClientCtx::propose_schema`
   (the existing "propose locally if leader, else relay one hop to a *known*
   leader" primitive used by every relayable `MetaCommand`) had no way to
   proceed when it has **no locally-known leader at all** — true forever for a
   non-participating control role, since it never receives a heartbeat/
   AppendEntries telling it who leads. Added a last-resort branch: broadcast
   the relay to every other address in `client_route` (bounded, small) —
   `ProposeSchema`'s handler is itself a single, bounded relay (never a
   chain), so a real control member among them resolves the actual leader on
   its own. This is what lets a growth node's own self-registration
   (`register_cp_addr`) — and, if invoked there, the admin add-member action
   itself — actually reach the real cluster.

## Decision

We will add four pieces.

### 1. `POST /admin/member/add` (`animusd`)

Takes the new node's **raftkv** id (+ optional topology labels) and proposes
`MetaCommand::UpsertMember{status: Down}` via the existing relayable-proposal
path (`ClientCtx::admin_add_member` → `propose_and_await` → `propose_schema`).
`UpsertMember{status: Down}` is added to `is_relayable_command`'s allowlist —
deliberately scoped to `Down` only (a pattern guard, not the whole variant):
unlike `admin_drain`'s `Leaving` transition on an *existing* member (kept
local-leader-only by design, so it can't be triggered through a relay chain by
mistake), registering a *new* member `Down` grants no placement eligibility by
itself — the detector still requires a real heartbeat before it does anything
— so relaying it is safe, and it is the *only* way the action can ever reach
the real leader when called from a growth node whose own control role never
resolves one (see §5's write-path fallback). Idempotent: re-adding an
already-known id (any status) is a no-op success.

### 2. `run_node_growth` (`animusd`)

A new, additive entry point alongside `run_node`/`run_node_with`: binds a node
from an **expanded** `ClusterConfig` (lists every pre-growth node plus every
node added so far) but passes the **pre-growth** control group to
`BoundNode::start_with` in place of `config.control_ids()`. This is the one
place the growth workflow deviates from a normal bring-up; everything else
(peer book, `client_route`, admin fan-out) comes from the same expanded config
every node already uses. `start_with` derives "is this a real control-group
voter" itself (`control_ids.contains(&self.control_id)`) — no new parameter on
its own signature — and conditionally spawns `remote_metadata_sync_loop`.

### 3. Phantom-member hardening, in `animus-control`'s `detect_loop`

Chose **option (a)** from the two considered (keep `bootstrap` registering
`Active` immediately; harden the detector), not **option (b)** (register
`Down`, promoted only by heartbeat, same as the online-growth path). (b) was
tried first and reverted: `bootstrap`'s *every* declared node is expected to
already be starting in the very same process-bring-up window, so a
still-electing leader or a slow first heartbeat can commit `CreateTable`'s
tablet provisioning (which seeds a replica set from whichever members are
`Active` *right now*) against a transiently **under-replicated** membership —
`animusd/tests/cp_cross_process.rs` caught this directly (a 2-of-3 replica set
from one bootstrap member not yet promoted), a real, non-trivial regression.

(a) instead gives any `Active` member the detector has never tracked a
**synthetic first observation** the moment `detect_loop` notices it — starting
exactly the silence clock a real heartbeat would, so:

- a node whose real heartbeat arrives promptly (the overwhelming common case,
  `HEARTBEAT_INTERVAL` = 100ms ≪ `DETECT_TIMEOUT` = 500ms) is unaffected — its
  real heartbeats keep re-observing it long before the synthetic one goes
  stale;
- a node that never heartbeats at all — a declared-but-never-booted phantom —
  is judged dead once `DETECT_TIMEOUT` elapses with no further evidence, and
  demoted to `Down` by the ordinary `Active → Down` transition (gated by the
  same post-election `LEADER_GRACE` every other failure already is) — closing
  the hole structurally, with no new state and no change to the detector's
  external shape (`FailureDetector`'s API is untouched; the synthetic
  observation is just an ordinary `observe()` call from `detect_loop`).

A member already tracked (has genuinely heartbeated) is left untouched, so a
real heartbeat's instant is never overwritten by a coarser synthetic one.

**Blast radius, found the hard way (fixed in the same change):** several
existing `animus-control` sim tests (`placement_auto_reconcile.rs`,
`placement_rebalance.rs`, `placement_reconcile.rs`) modeled `Active` data
members by proposing `UpsertMember` directly, with no heartbeat simulated at
all — a legitimate way to test placement logic in isolation *before* this
change, but indistinguishable from a phantom *after* it. Fixed by having them
spawn `animus_control::node::heartbeat_loop` for every data node they declare
(mirroring `failure_detection.rs`'s existing pattern) and `sim.crash` the one
member a test manually marks `Down` (otherwise its own still-arriving
heartbeat immediately reverts the manual `Down` via the pre-existing,
unchanged recovery rule). `prod_liveness.rs`'s bulk "fat member" payload (pure
metadata-size filler, not real nodes) was switched from `Active` to `Joining`
— `Joining`/`Leaving` are the two statuses the detector has always
deliberately never judged — since registering ~130 fake `Active` nodes with no
heartbeat capability at all otherwise turned them all "phantom" simultaneously
and flooded the leader's WAL with `Down` proposals right as the test's real
subject (a follower catching up to a large snapshot) needed it responsive.

**A residual, accepted caveat:** a real deployment with a *very* large number
of genuinely live members could, in principle, see a burst of `Down`
proposals if enough of them are simultaneously untracked for longer than
`LEADER_GRACE` after a leader failover (a cold detector) — bounded in practice
by `HEARTBEAT_INTERVAL` ≪ `LEADER_GRACE` and by realistic cluster sizes; not
re-tuned here.

### 4. `animusd/tests/cluster_growth.rs`

Brings up a 3-node cluster (config declaring only 3), creates tables + writes,
then grows to 5 nodes with an expanded config (no restart of the original 3),
admin-adds the two new raftkv ids, and polls (converged-or-timeout) that: they
become `Active`, rebalancing spreads existing tablets onto them (imbalance ≤
1, checked via the data-plane's own hosted-voters view — not just replicated
metadata), a phantom that never boots stays `Down`, and reads/writes keep
working throughout. Per-process (`run_node`/`run_node_growth`), with the
documented port-TOCTOU bring-up retry.

## Consequences

- A `--config`/`--node` deployment can grow online: start new nodes from an
  expanded config, `POST /admin/member/add` each (from any reachable admin
  port — including the new node's own), and the existing ADR 0012 detector +
  ADR 0029 rebalancer do the rest with no further operator action.
- **The control group's size is fixed for the life of the cluster** — this is
  the load-bearing v1 limitation this ADR documents, not a bug: control-plane
  membership change (`RaftCore::change_membership`, already used by the
  per-tablet CP-data plane) was deliberately not attempted here. A future
  slice that wants a growable control group needs that mechanism plus a
  materially larger safety review; this ADR's data-plane-only growth composes
  with it later without conflict (a control-plane-follower-less node can
  simply stop being one once it gains a real voter slot).
- **A pre-growth node's own `client_route` is not extended when a new node
  joins** (it is a static map built once at that node's own process start).
  Concretely: a client connected to an *original* node can still fail to be
  *forwarded* to a tablet's leader if that leader has since moved onto a
  *newly grown* node — the original node's `client_route` has no entry for it.
  A growth node's own `client_route`, by contrast, is always complete (built
  from the expanded config it started from), so it can always resolve/forward
  correctly in either direction. The practical mitigation (and what
  `cluster_growth.rs` models): route new client traffic through a client list
  that includes the newly grown nodes' addresses, same as an operator would
  naturally do after growing a cluster. Closing this fully needs a second,
  replicated client-address map (mirroring `cp_member_addrs`, but for the
  client-facing port) — left as follow-up, not required for the stated
  "rebalancing works, reads/writes keep working" bar. **Update (ADR 0032
  PR1): closed.** `Metadata.node_addrs` + `route_sync_loop` keep every node's
  `client_route` live (static seed ∪ replicated overlay), so an original
  node now forwards correctly to a leader that has since moved onto a node
  grown in afterward — see ADR 0032's own doc for the mechanism.
- A growth node does not serve schema-catalog reads/writes
  (`table_schema`/`has_keyspace`, used by the CQL/DynamoDB wire edges) through
  its own mirror — only the CP routing / hosting paths were switched to
  `effective_metadata()`. Route DDL through an original control node in this
  v1 slice. **Update: closed.** `table_schema`/`has_table_schema` were
  switched to `effective_metadata()` (`ClientCtx::table_schema`, `lib.rs`);
  `has_keyspace` was removed as dead code once `create_keyspace` was moved to
  read `metadata_fresh()` directly (the same read-your-writes contract
  `create_table_schema` already used) — see the ADR 0035 PR5 staleness-audit
  note in `crates/animusd/CLAUDE.md` for the fix that also touched this path.

## Engineering lesson

A feature whose only enabling *registration* path is shaped by the startup
config silently caps the cluster at its born size — and a test that "proves"
growth by starting every node up front (as ADR 0029's own `cp_rebalance.rs`
does, by its own admission) only proves the *planner*, never the actual growth
*path*. The tell was in the problem statement itself: `bootstrap` computing
`raftkv_ids` from `control_ids.len()` looks like an implementation detail, but
it is the entire ceiling. Recorded in the root `CLAUDE.md` Engineering
Practices section.

## Amended by ADR 0035

[ADR 0035](0035-control-plane-separate-deployment.md) generalizes this ADR's
non-voter control-core mirror (`remote_metadata_sync_loop` /
`ClientCtx::effective_metadata()`) from "what a growth node falls back to"
into `ControlHandle::Remote` — the *only* way any data-only node ever sees
`Metadata`, with no local control `RaftCore` at all (not even a non-voter
one). This ADR's finding that a non-voter's local `Metadata` never advances
via real Raft replication, no matter how long it waits, is exactly the
observation ADR 0035 cites as proof the mirror path was already sufficient on
its own. The control group's static size (this ADR's own accepted
limitation, above) is unchanged by the split — ADR 0035 relocates the static
group into its own deployment, it does not make it elastic.
