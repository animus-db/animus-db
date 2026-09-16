# ADR 0005 — Placement groups + topology-aware data residency

- **Status:** Accepted. **Note (ADR 0019):** the residency-enforcement mechanisms
  this ADR describes as built for hinted handoff / read-repair / anti-entropy
  (`serve_replica_with_residency`, `AllowedTargets`, proven in
  `animus-data/tests/hinted_handoff.rs`) lived in the leaderless **AP data
  plane**, which was subsequently **deleted** with `animus-data` — those specific
  tests and code paths no longer exist in this workspace (retrievable from git
  history). The **placement model this ADR actually decided** — topology labels,
  placement groups, and the control-plane reconciler that satisfies them — is
  plane-agnostic and remains current: it drives CP-plane tablet placement today
  (`reconcile_loop`, `Metadata::rebalance`, ADR 0029) exactly as described below.
- **Date:** 2026-08-01

## Context

Operators need to control *where* data physically lives — for latency (keep data
near its users) and, increasingly, for legal **data residency** (e.g. EU
customer data must stay in EU jurisdictions). A flat, topology-blind placement
policy cannot express "these rows may only be replicated within these regions."

## Decision

We will make nodes carry **topology labels** (e.g. `region=eu-west`,
`zone=eu-west-1a`, `jurisdiction=EU`) in the control-plane membership state, and
introduce **placement groups**: named policies that constrain which topology
domains a tablet's replicas may occupy and how replicas spread across failure
domains. A tablet belongs to a placement group; the control-plane reconciler
chooses replica sets that satisfy the group's constraints.

This ADR records the model. Enforcement of residency across hinted handoff,
read-repair, anti-entropy, and backup is explicitly **later work** (out of scope
for the first milestones) — those paths can leak data across boundaries if not
designed with residency in mind, so they must be addressed deliberately.

## Consequences

- Residency and locality become declarative policy on the control plane rather
  than manual replica placement.
- Topology labels become first-class membership metadata from the start, so the
  membership model is designed to carry them even before placement groups are
  implemented.
- Residency is only as strong as its weakest path: hinted handoff, repair, and
  backup must all honor the same constraints, which is significant future work
  and a correctness (and compliance) risk if rushed.

## Status of implementation

The **placement selection engine** is implemented in `animus-placement` as a
pure, deterministic policy library: a `PlacementPolicy` (replication factor +
residency `required_labels` + an optional failure-domain `SpreadPolicy`),
`select_replicas` (fresh placement) and `replan` (churn-minimizing replacement
that keeps surviving replicas). It is dependency-light (only `NodeId`) to stay
out of a cycle with `animus-control`; the control plane builds `Candidate`s from
the replicated `Active` membership, calls it, and commits the result as a
`CasTabletReplicas`. `animus-control/tests/placement_reconcile.rs` drives this
end-to-end through real Raft under simulation, including a replica death and a
control-follower crash mid-reconcile, reproducible from a seed.

**Policies are now replicated and reconciliation is automatic.** A placement
policy is persisted *in* the control-plane `Metadata` via a `SetTabletPolicy`
`MetaCommand` (a `BTreeMap<TabletId, PlacementPolicy>`), so it survives leader
change and recovery and every replica sees the same policy. The decision —
`Metadata::reconcile` — is a pure, deterministic function of the metadata: for
each policied tablet it runs `replan` over the `Active` membership and emits a
`CasTabletReplicas` only when the current set violates the policy, so it is
idempotent (no churn at steady state). The **leader** drives it: `RaftNode`'s
`reconcile_loop` calls it on a slow `Env` timer (`env.sleep`, never wall clock)
and proposes the result; off-leader nodes propose nothing and a stale proposal
is rejected by the epoch guard. `animus-control` therefore now takes
`animus-placement` as a normal dependency (no cycle — placement does not depend
on control). `animus-control/tests/placement_auto_reconcile.rs` proves a marked
`Down` replica is replaced **automatically**, with no test-driven `replan`/CAS,
preserving residency + spread and moving only the dead replica, reproducible
from a seed.

*(The next two increments — repair-path and hinted-handoff residency — were
**removed with the AP data plane** (ADR 0019): `serve_replica_with_residency`,
`AllowedTargets`, and the cited `animus-data` tests no longer exist. They are
retained below as design record for the AP long shot; in v1 residency is
enforced by placement alone — the CP plane's replicas are exactly the placed
set, and there is no repair/hint path that could leak past it.)*

**Residency now extends to the repair paths.** The data plane's read-repair and
background anti-entropy (ADR 0010) are bound to a tablet's residency-eligible
placement on **both** sides, closing the leak where repair could push data to a
reachable but ineligible node. The sender side already only targets the tablet's
replica set (read-repair broadcasts to `TabletView::replicas`; anti-entropy to a
caller-supplied peer list — both the placement, hence residency-eligible). The
receive side is the new guard: `serve_replica_with_residency` takes the allowed
peer set and **drops any `Sync`/`SyncDigest`/`SyncPull` from a node outside it**,
so even a misconfigured or hostile peer cannot inject or solicit cross-boundary
data. The allowed set is derived from the same `PlacementPolicy::admits` the
control plane uses for placement. Quorum `Write`/`Delete`/`Read` need no new
guard: an ineligible node is never a replica in a `TabletView`, so it is never
sent one. Proven under simulation in `animus-data/tests/residency_repair.rs`: a
reachable non-EU node actively soliciting anti-entropy from EU replicas never
receives the EU data, and a direct `Sync` from it is rejected.

**Residency now also extends to hinted handoff.** Hinted handoff (ADR 0010)
buffers a hint at the coordinator for a replica that missed a committed
write/delete and replays it when the replica returns. It is residency-bounded the
same way repair is: the coordinator holds an `AllowedTargets` set (derived from
`PlacementPolicy::admits`, the same check used to place) and **records a hint for,
and replays a hint to, only an admitted target** — so a hint never crosses a
residency boundary, even to a reachable node. The replica's
`serve_replica_with_residency` receive guard is the backstop (it must admit the
holder/coordinator node, a trusted in-region participant, exactly as it must for
coordinator-driven read-repair). Proven under simulation in
`animus-data/tests/hinted_handoff.rs`
(`no_hint_is_buffered_or_replayed_for_a_residency_ineligible_replica`).

**The reconciler now runs in the production binary.** `animusd` registers the
**data nodes** as the cluster's `Active` members (not the control-group ids),
places its bootstrap tablet on the first `min(N, 3)` of them, and attaches a
`PlacementPolicy` via `SetTabletPolicy`. The leader's `reconcile_loop` is then
driven over `ProdEnv` timers, so when failure detection (ADR 0012) marks a data
member `Down`, the tablet is re-placed onto a live spare with no operator —
observable end-to-end over real TCP in `animusd/tests/self_heal.rs`. (The
bootstrap policy carries no labels yet, so it is a plain replication-factor
constraint; topology labels from config are future work.)

**Still deferred** (the remaining weakest-path work above): residency enforcement
across backup. The reconciler keeps a tablet's *surviving*
eligible replicas (minimal churn), so it repairs drift but does not itself
re-optimize an already-placed compliant-enough set — **that re-optimization is
now a separate, balance-driven complement**, `Metadata::rebalance` (ADR 0029),
driven by the same `reconcile_loop` on a slower cadence whenever repair has
nothing to do. A cluster-default policy, topology labels in `animusd`'s
deployment config, and operator-facing policy management (CLI/wire) are still
future work.

**2026-09-16 amendment (issue #957): repair grows a tablet as far as it
genuinely can, not only all the way to the full RF.** `Metadata::reconcile`
called plain `replan` (`animus_placement`) for repair; `replan`'s underlying
`choose` is all-or-nothing — it refuses outright
(`PlacementError::InsufficientCandidates`) the instant the eligible
candidate pool can't reach the policy's full `replication_factor`, even
when a smaller, strictly-improving move is possible. Since a tablet's
policy always records the *target* RF rather than the observed candidate
count at provision time (the design this ADR's own "reconciler now runs in
the production binary" section already establishes, and
`crates/animusd/tests/tablet_rf_self_heals.rs`'s own module doc restates),
an ordinary small cluster — an RF-3 policy with only 2 members ever
registered, say — could mint a tablet with fewer replicas than even that
2-node ceiling (a real, structural race: the very first tablet can be
provisioned before every founding member's own registration has committed
and applied, `docs/lessons/testing/2026-09-16-a-faster-bootstrap-time-
schema-proposal-makes-initial-tablet-placement-an-eventual-property.md`)
and then never self-heal even the achievable 1→2 repair, permanently,
until a 3rd candidate happened to make the full RF reachable in one step.
This was found root-causing exactly that flaky test — the boot-time
"wiped voter" cluster check (issue #667/#900) was the prime suspect but
was ruled out empirically (`cluster_check_pending`/`refused_as_voter` both
`false` on every replica throughout several captured stalls, with the
control plane's own consensus and apply task fully healthy and idle the
entire time — nothing was stuck at the Raft layer at all). The actual
defect was purely in the pure placement-policy engine's own contract.

**The fix**: a new, growth-only best-effort sibling,
`animus_placement::replan_repair`, is what `Metadata::reconcile` now calls
for repair. It behaves exactly like `replan` except when the eligible pool
is smaller than the policy's RF but still larger than the tablet's current
eligible survivors — real progress is possible — in which case it grows to
every eligible candidate instead of refusing. It is a **strict widening**:
whenever the full RF *is* satisfiable, or the tablet already holds as many
replicas as the cluster can currently support (an already-at-capacity set,
whether or not that satisfies the full policy), `replan_repair` returns
`replan`'s own plain, unmodified answer — including `Err` — which is what
keeps a stale, currently-ineligible replica in place rather than dropping
it the moment there's nothing better to replace it with (the growth-only
guarantee is deliberately independent of, and does not relax, the
never-shrink one). `replan` itself, and every one of its other callers
(the directed-Placing convergence phase, ADR 0062 §2, and fresh-tablet
placement via `select_replicas`), are unchanged — best-effort repair is
`Metadata::reconcile`'s own concern, not a change to the crate's general
contract. See `animus-placement/CLAUDE.md`'s entry for the full function
doc and `animus-control/tests/placement_auto_reconcile.rs::
leader_automatically_grows_an_undersized_tablet_when_rf_exceeds_the_
candidate_pool` for the live, Raft-driven regression (a 2-node cluster,
RF-3 policy, the leader's own timer-driven `reconcile_loop` — never
test-driven — growing a 1-replica tablet to 2 and converging idempotently
until a 3rd candidate reaches the full RF).
