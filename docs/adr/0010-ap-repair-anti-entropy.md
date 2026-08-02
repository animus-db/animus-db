# ADR 0010 — AP repair: read-repair + background anti-entropy

- **Status:** Accepted
- **Date:** 2026-08-01

## Context

The leaderless data plane (ADR 0001) serves reads/writes against a tablet's
replica set with tunable quorums, choosing `R + W > N` so a read quorum
intersects every acknowledged write. That intersection is the *only* thing the
vertical slice relied on for correctness: a read sees the latest acknowledged
value because some replica in the read quorum also took part in the write
quorum.

But intersection is not convergence. A replica that misses a write — because it
was partitioned, briefly down, or simply outside the `W` that acknowledged —
stays stale indefinitely. Nothing pushes the value to it. Raw replica state
diverges, and the only thing masking it is that quorum reads keep intersecting a
fresh-enough replica. That is fragile (it degrades as more replicas lag) and it
leaves cold data permanently inconsistent across replicas, which a real Dynamo
lineage system repairs.

A second, lower-level obstacle: the `StorageEngine` `put` enforces a single
**engine-wide monotonic-version** contract (ADR 0008) — correct for a single
writer assigning increasing commit timestamps, but wrong for replication. A
repair re-applies a value at its *original* version, which may sit below the
replica's current engine-wide latest; `put` would reject it.

## Decision

Add the two classic Dynamo anti-entropy mechanisms, both built on a new storage
primitive:

1. **`StorageEngine::merge(key, value, version) -> bool`** — apply iff `version`
   is strictly newer than the key's *own* latest, ignoring the engine-wide
   monotonic floor. It is idempotent and commutative under per-key
   last-writer-wins, so replicas converge to the highest version seen per key
   regardless of delivery order. `put`'s global contract is unchanged (the
   control plane and the dynamo adapter still rely on it). Replica writes now go
   through `merge`, which is the correct semantics for a leaderless plane anyway.
   `StorageEngine::entries()` exposes the full live digest a sync reconciles
   against.

   **Deletes (added later).** A delete is the same per-key LWW idea applied to a
   tombstone: `StorageEngine::merge_tombstone(key, version) -> bool` applies a
   tombstone iff `version` is strictly newer than the key's own latest, exactly
   like `merge` applies a value. For anti-entropy to *propagate* a delete, the
   digest must retain tombstoned keys (the live `entries()` hides them), so
   `StorageEngine::entries_with_tombstones() -> Vec<(key, Option<value>,
   version)>` carries each key's latest record (`None` = tombstone). The
   data-plane gains a quorum `DataMsg::Delete`/`DeleteAck` (epoch-fenced like
   `Write`), and `DataMsg::Sync` now carries `(key, Option<value>, version)` so a
   tombstone reconciles through both repair paths just as a value does. Both
   storage engines (`MemoryEngine` and `LsmEngine`) implement the two new
   primitives.

2. **Read-repair** (lazy, on the read path): when a quorum read finds the
   responding replicas disagree — some returned an older version, or none — the
   coordinator pushes the winning `(value, version)` back to the tablet's
   replicas as a fire-and-forget `DataMsg::Sync`, which they `merge`. This
   repairs the replicas that took part in the read; it costs nothing when they
   already agree.

3. **Anti-entropy** (eager, background): `serve_anti_entropy` runs a per-replica
   timer loop that periodically reconciles with its peers. This converges
   replicas that are **never read**, which read-repair alone cannot. Both paths
   are fenced per tablet by epoch, exactly like ordinary writes (ADR 0002).

   **Range/segment digests (added later).** The original scheme *full-pushed*
   `entries_with_tombstones()` to every peer each round — `O(data)` even when the
   replicas already agree. It is now a **digest exchange** (the Merkle/range
   refinement): each round a replica sends a compact `SyncDigest` — its data
   bucketed into a fixed number of key *segments*, each summarized by an
   order-independent (XOR-folded) content hash plus an entry count. A peer
   compares it against its own digest (`digest::divergent`) and asks (via
   `SyncPull`) only for the segments that differ; the sender answers with a
   `Sync` of just those segments' entries (tombstones included, as before). A
   converged pair therefore transfers **no entry data at all**, and a pair
   differing in one key moves only that key's segment — not the whole dataset.
   The segment of a key and the hash of an entry are pure functions of their
   bytes, so the digest is deterministic across replicas and on replay (ADR
   0003).

   **Residency on repair (added later, ADR 0005).** Both repair paths are bound
   to a tablet's residency-eligible placement. The send side already only targets
   the tablet's replica set (read-repair → `TabletView::replicas`, anti-entropy →
   the caller's peer list). The receive side adds a guard:
   `serve_replica_with_residency(allowed)` **drops any `Sync`/`SyncDigest`/
   `SyncPull` from a node outside `allowed`**, so repair cannot move data across a
   residency boundary even to a reachable node. `serve_replica` (no allowed set)
   keeps the unrestricted behavior.

## Consequences

- Raw replica state now converges, not just quorum-read results. A replica that
  missed writes is repaired either on the next divergent read or by the next
  anti-entropy round — proven under simulation by partitioning a replica during
  a write and asserting convergence both with a read (read-repair) and with **no
  reads at all** (anti-entropy) in `animus-data/tests/repair.rs`.
- Anti-entropy is no longer a full-push: the **segment-digest exchange** moves
  only divergent ranges, so a converged pair exchanges only tiny digests and a
  single divergent key out of many converges for **less than one full-push round
  of bytes** — proven at the wire level (the simulator's `Send` trace) in
  `animus-data/tests/digest_anti_entropy.rs`. Read-repair still repairs only the
  replicas that responded within the read; stragglers rely on anti-entropy.
- **Residency holds on the repair paths** (ADR 0005): a residency-ineligible but
  reachable node never receives repaired data, and a repair message from outside
  the placement is rejected — proven in `animus-data/tests/residency_repair.rs`.
- **Anti-entropy follows the tablet's live epoch.** `serve_anti_entropy` takes
  the replica's `ReplicaHandle` and reads its current known epoch for the tablet
  (`handle.epoch(tablet)`) at the start of *each* round, stamping the outbound
  `SyncDigest` with it — not a constant captured when the loop started. This
  matters after a topology change: a placement reconcile bumps the tablet's epoch
  and the control plane advances each replica's known epoch (via
  `ReplicaHandle::set_epoch`), so a round still stamping the old epoch would be
  fenced by every up-to-date peer (ADR 0002) and a re-placed spare would converge
  only lazily via read-repair on its first read. Reading the epoch live keeps
  **background** convergence working across a reconcile, while a genuinely
  stale-epoch peer is still fenced — both proven under simulation in
  `animus-data/tests/repair.rs`
  (`anti_entropy_tracks_the_live_epoch_after_a_reconcile`,
  `anti_entropy_still_fences_a_genuinely_stale_epoch_peer`). The epoch is read
  under a brief lock released before any `.await` (the no-guard-across-await
  discipline). This closes the gap previously deferred in `animusd`, which passed
  a fixed `Epoch::INITIAL` into the loop.
- **Deletes now propagate** through repair: a data-plane `DataMsg::Delete`
  tombstones by per-key LWW (`merge_tombstone`), and the tombstone-carrying
  `Sync` digest converges it through both read-repair and anti-entropy — proven
  under simulation by isolating a replica that *holds the value* during a delete
  and asserting it converges to the tombstone with **no reads at all**, and that
  the lagging replica pushing its stale value back does not resurrect the key
  (`animus-data/tests/repair.rs`).
- **Tombstone GC (now implemented in `LsmEngine`).** Tombstones no longer live
  forever in the on-disk engine: `LsmEngine` reclaims a tombstone (and the
  versions it shadows) **during compaction** once it has aged below a configurable
  **GC floor** = `max_version - LsmOptions::tombstone_grace_versions`, and only
  when no deeper, uncompacted level could still hold an older value for the key
  (which would otherwise resurface). The reclamation is **invisible above the
  floor**: every historical read at a version in the retained window
  `(gc_floor, max_version]` reads exactly as before, so the differential proptest
  against `MemoryEngine` stays green for that window (it now asserts the only
  digest difference is below-floor reclaimed tombstones). The grace should be set
  **above the maximum anti-entropy lag** so a long-offline replica is still
  repaired with the delete before the tombstone is reclaimed; the interaction with
  a replica offline *longer* than the grace remains the operator's responsibility
  (the classic Dynamo trade-off). `MemoryEngine` still retains tombstones forever
  (it has no compaction); GC is an `LsmEngine` storage-layer reclamation, not a
  change to the data-plane convergence contract. See ADR 0008 and
  `animus-storage/tests/lsm_gc.rs`.
- ADR 0001 (two-plane) and ADR 0002 (epoch fencing) are unchanged; this refines
  the data plane's convergence story within them.
