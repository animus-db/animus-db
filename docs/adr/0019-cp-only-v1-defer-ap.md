# ADR 0019 — v1 ships the CP plane only; the leaderless AP data plane is deferred

- **Status:** Accepted (amends ADR 0001 for the v1 scope)
- **Date:** 2026-08-03

## Context

ADR 0001 set AnimusDB's founding shape: a **leaderless AP data plane** (Dynamo
lineage — tunable-quorum reads/writes, available under partition, convergent via
repair/anti-entropy, ADR 0010) paired with a small **strongly-consistent Raft
control plane** owning cluster metadata. ADR 0016/0017 then added a *second*,
**leaderful CP data plane** (per-tablet Raft, linearizable single-tablet KV) as a
modular alternative, and ADR 0018 sketched cross-tablet transactions over it.

Both data planes are substantially built:

- **AP** (`animus-data`): durable, self-healing (failure → `Down` → re-place →
  converge), wired into `animusd` as the default plane behind both wire adapters.
- **CP** (`animus-cp-data`): linearizable single-tablet KV, ReadIndex reads,
  membership change, tablet split, an Elle linearizability corpus, and — after v1
  Phase 1 — runnable + cross-process-routable in `animusd` (per-table mode
  selection, ADR 0017 #3a/#3b).

The v1 plan scoped **AP + CP both** as a "linearly-scalable" first release. But the
remaining v1 work — horizontal sharding (live tablet split + dynamic node-join +
rebalancing), plus each plane's transaction story — must be built **per plane**,
roughly doubling the hardest remaining effort, and means maintaining **two
consistency models** and **two transaction stories** (Accord for AP, 2PC/HLC for
CP). The maintainer's priority is **strong consistency (CP)**. Shipping one plane
*well* beats shipping two half-finished.

## Decision

**AnimusDB v1 ships the leaderful CP data plane only.** The leaderless AP data
plane is **deferred** — recorded here as a *long-shot future improvement*, not part
of v1.

This **amends ADR 0001** for the v1 scope: the Raft **control plane** is unchanged
(it remains the metadata authority), but the **data plane** is CP-only. AnimusDB v1
is therefore a **strongly-consistent, linearly-scalable, per-tablet-Raft store**
(closer to CockroachDB / TiKV) rather than a Dynamo-lineage AP store. ADR 0001 is
not superseded — its control-plane decision stands and its AP decision is the
foundation the deferred long-shot would revive.

## Consequences

**v1 scope shrinks to one well-defined target:**

- **CP becomes the default and only data plane.** The per-table `ReplicationMode`
  seam (ADR 0017 #3a) is **retained but forced to `Cp`** in v1 (an `Ap` selection
  is unsupported) — kept as the forward-compatibility hook for AP's eventual
  return, not removed.
- **`animusd`'s serving path drops the AP roles** (the migration below): the data
  replica (`serve_replica`), the `DataClient` quorum coordinator, anti-entropy, and
  hinted handoff leave the v1 node. The bootstrap tablet becomes a CP Raft group;
  client / DynamoDB / CQL reads and writes route to CP unconditionally.
- **Phase 2 sharding targets one plane.** Split *execution* is only the CP
  live-group division (ADR 0017 Stage D + `Coresident`); there is no AP
  metadata-split path to build. The shared substrate (shard map, key-range routing,
  node-join, address distribution) is unchanged — so dropping AP **halves the
  plane-specific work** without losing the "linearly scalable" headline.
- **Cross-tablet transactions** (ADR 0018, 2PC/HLC over the CP groups) remain the
  designated post-single-tablet step — now the *only* transaction story to build
  (Accord-over-AP is deferred with AP).

**Knowingly given up (the CP trade, already documented in ADR 0017):**

- **Availability under partition.** v1 has no sloppy-quorum AP path: a tablet is
  write-unavailable during election / quorum loss. v1 chooses linearizability over
  the AP availability ADR 0001 prized.
- The Dynamo-lineage identity for v1. (Reclaimable via the long shot.)

**Code disposition — deleted (updated decision):** this ADR originally intended to
*retain `animus-data` dormant-but-compiling*. The maintainer subsequently elected
the anticipated **physical removal**: `animus-data` (the AP plane), the
`animus-consensus` **AP-frontier** paths (`start_with_data_plane`/`start_with_router`
+ `DataSink`/`DataRouting` + data-plane reads), and the AP / frontier Elle corpora
in `animus-test` are **deleted**, not kept compiling — a leaner tree, the dual-plane
option preserved in **git history** + this ADR rather than in dormant code. The
long-shot revival below would re-introduce them from history (Accord, the
`ReplicationMode` seam, and the dual-plane/pluggable-replication ADRs remain on
record, so it stays feasible rather than a rewrite).

## The long shot (reviving AP, post-v1)

Re-wire `animus-data` into the node, restore per-table `Ap`/`Cp` selection (the
`ReplicationMode` seam is preserved for exactly this), and adopt Accord (ADR 0011,
already AP-aligned) as AP's transaction story. The dual-plane vision of ADR 0001 +
the pluggable-replication frame of ADR 0016 stay on record so this remains feasible
rather than a rewrite. It is a stretch goal, not a roadmap commitment.

## Migration (execution follow-on, not this ADR)

1. Make CP the unconditional plane in `animusd`: route the client / DynamoDB / CQL
   read/write paths to the CP group; bootstrap the initial tablet as a CP Raft
   group; force `ReplicationMode::Cp`.
2. Remove the AP roles from node assembly (`serve_replica`, `DataClient`,
   anti-entropy, hinted handoff, the `data`/`coord` `ProdEnv` roles and their
   `ClusterConfig` ports) from the v1 path.
3. **Delete `animus-data`** and the AP-frontier paths it backed in
   `animus-consensus` + `animus-test` (per the updated disposition above), trimming
   the v1 acceptance gate to the CP + control-plane + pure-Accord suites.
4. Proceed with Phase 2 (sharding) against CP only.

Steps 1–3 are **done** (the CP re-platform: edges→CP, AP roles removed,
`animus-raftdata`→`animus-cp-data`, `animus-data` deleted).

This ADR builds on ADR 0017/0018 (the CP stack it makes v1's whole data plane),
ADR 0016 (pluggable replication — the frame that made one-plane-at-a-time viable),
and amends ADR 0001 (whose AP data-plane decision becomes the deferred long shot;
its control-plane decision stands).

## Amendment (2026-08-23) — the long shot is closed; Accord and the `ReplicationMode` seam are deleted

This ADR deferred the AP plane and kept a deliberate escape hatch: the
"long shot" above, backed by two artefacts held in the tree for it — the
per-table `ReplicationMode` seam ("**retained but forced to `Cp`**  … kept as the
forward-compatibility hook for AP's eventual return, not removed") and the Accord
crate (`animus-consensus`), whose surviving purpose this ADR named as AP's
transaction story.

**Both are now removed, and the long shot is closed.** The reason is not a
change of appetite for availability — it is that the escape hatch became
unreachable.

**The argument.** AP is selectable only through `ReplicationMode`, a *per-table*
schema property. A per-table property has to be expressible by some wire the
cluster actually serves. When this ADR was written there were two adapters
(ADR 0006), and CQL was the one that could express it: `WITH` clauses on
`CREATE TABLE`, and per-query `ONE`/`QUORUM`/`ALL` consistency levels — a natural
surface for tunable-quorum AP. **ADR 0053 then dropped CQL**, leaving DynamoDB
as the only wire. DynamoDB's `CreateTable` has no replication-mode field, and
inventing one would break exactly the wire fidelity ADR 0006 and ADR 0053
committed to. So `ReplicationMode::Ap` became a forward-compatibility hook
forward-compatible with nothing: no client of any shipping wire can select it,
and the code path behind it (`animus-data`) was already deleted by this ADR's own
updated disposition.

Note the asymmetry this closes. The single consistency choice DynamoDB *does* let
a client make is `ConsistentRead` on an individual read — a strong read versus a
cheap eventually-consistent one. That is a read-path option over a
strongly-consistent store, not a replication mode, and it is served by the CP
plane ([ADR 0055](0055-eventually-consistent-reads.md), which makes that
forward reference concrete: `false` reads answer from any replica's applied
state, `true` reads keep the ReadIndex path). It is not a residual use for AP, and it was never one:
an AP plane answers a *write*-availability question that DynamoDB's protocol
gives a client no way to ask.

**Deleted, therefore:**

- `crates/animus-consensus` — the whole Accord implementation, its test suite,
  and its crate guide. ADR 0018 had already rejected Accord as the CP transaction
  mechanism (2PC-over-Raft was chosen instead), and this ADR deferred
  Accord-over-AP with AP, so its only remaining role was as the
  known-serializable reference system the Elle checkers were proven against
  (ADR 0014).
- The Accord Elle corpus in `animus-test` — `tests/support/` (the shared Accord
  harness), `tests/corpus.rs`, `tests/elle_accord.rs` — and with it the
  `ANIMUS_CORPUS_SEEDS` / `ANIMUS_CORPUS_FULL` knobs and their nightly CI tier.
- `ReplicationMode` itself, `TableSchema::mode`, and every call site.

**What this costs, stated plainly.** ADR 0014 built the Elle checkers *against*
Accord as a known-serializable system; deleting it means `check_cycles` keeps
working but loses the reference implementation it was originally validated
against. The surviving corpora that assert it — the multi-tablet transaction
corpus (ADR 0018) above all — carry that weight now, backed by the hand-built
negative controls in `animus-test/tests/negative_control.rs`, which are
independent of Accord and always were. That is a real reduction in the evidence
base, accepted in exchange for a materially smaller tree.

**Revival, if AP ever returns.** Unchanged in kind but now larger in degree: the
plane (`animus-data`), Accord, and the `ReplicationMode` seam all come back from
git history, and ADRs 0001, 0011, 0014 and 0016 remain on record as the design.
The new precondition is the one that closed the hatch — **a wire adapter that can
express a replication mode.** Reviving AP under a DynamoDB-only surface would
mean either a second adapter or a deliberate, documented departure from
DynamoDB's `CreateTable` contract; neither is a decision to make in advance.
