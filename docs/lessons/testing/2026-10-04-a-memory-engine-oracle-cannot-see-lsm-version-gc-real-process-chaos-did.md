# A MemoryEngine-backed oracle cannot see LSM version GC; a real-process chaos run did

**Context.** The first R-01 (b) chaos runs (`docs/chaos.md`, Findings) lost acknowledged appends on
keys touched by aborted cross-tablet transactions, intermittently (~2 in 25 smoke runs). The sim
corpora over the same operations (`sim_cluster_dynamo_corpus`, `txn_serializable`) had never
failed.

**Lessons.**
- **Sim tiers default to `MemoryEngine`, which retains every MVCC version forever.** A code path
  that reads *history* (`TxnResolve`'s abort restores the pre-intent value via
  `storage.get_at(key, intent_version - 1)`) is only correct if the engine still holds that
  version. `LsmEngine` garbage-collects versions below `max_version - tombstone_grace_versions`
  during compaction, so the same path can read `None` there and write a tombstone.
- **A GC window expressed in raw version units silently changes meaning when the version space
  changes.** `LsmOptions::default().tombstone_grace_versions` is `1 << 20`, documented as "generous
  so GC is a no-op under ordinary use". Data-plane versions have been packed HLCs
  (`wall_ms << 20 | logical`) since ADR 0018 PR2, so that window is **one millisecond**. When a
  unit-bearing constant is inherited across a representation change, convert it explicitly and test
  it at the new scale.
- **Engine-level reproduction is cheap once you know the shape.** A 40-line `LsmEngine<SimEnv>`
  test (default grace, small flush thresholds; put a value, put an "intent" 3 s later, churn other
  keys, `get_at(intent - 1)`) returns `None` deterministically. When a real-process finding is not
  seed-reproducible, look for the engine- or component-level deterministic cell that exhibits the
  mechanism (B-3), instead of trying to make the process run reproducible.
- **Run the corpora that matter over the real engine too.** `ANIMUS_RAFTKV_LSM=1` exists; a
  transaction-abort-after-compaction cell would need an HLC clock far enough ahead of the grace
  window and a compaction between stage and abort, which the default sim timing never produces.
