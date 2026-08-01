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

2. **Read-repair** (lazy, on the read path): when a quorum read finds the
   responding replicas disagree — some returned an older version, or none — the
   coordinator pushes the winning `(value, version)` back to the tablet's
   replicas as a fire-and-forget `DataMsg::Sync`, which they `merge`. This
   repairs the replicas that took part in the read; it costs nothing when they
   already agree.

3. **Anti-entropy** (eager, background): `serve_anti_entropy` runs a per-replica
   timer loop that periodically pushes its full digest to its peers as a
   `Sync`. This converges replicas that are **never read**, which read-repair
   alone cannot. Both paths are fenced per tablet by epoch, exactly like
   ordinary writes (ADR 0002).

## Consequences

- Raw replica state now converges, not just quorum-read results. A replica that
  missed writes is repaired either on the next divergent read or by the next
  anti-entropy round — proven under simulation by partitioning a replica during
  a write and asserting convergence both with a read (read-repair) and with **no
  reads at all** (anti-entropy) in `custos-data/tests/repair.rs`.
- The sync wire shape is a **full-push** of the digest: simple and provably
  convergent, but `O(data)` per round. Sending only divergent ranges via a
  **Merkle-tree digest** is the obvious optimization and is deferred. Read-repair
  likewise repairs only the replicas that responded within the read; stragglers
  rely on anti-entropy.
- **Deletes do not yet propagate** through repair: the data-plane protocol
  carries only writes, so `merge` reconciles values, not tombstones. Tombstone
  anti-entropy (and its GC/grace-period concerns) is future work alongside a
  data-plane delete.
- ADR 0001 (two-plane) and ADR 0002 (epoch fencing) are unchanged; this refines
  the data plane's convergence story within them.
