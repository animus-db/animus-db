# An apply-time decision may read only the replicated log and durable engine state, never process memory

`TxnTracker::recently_resolved` (a bounded in-process map) decided whether
`KvCommand::TxnStage` applied or no-op'd. It was empty after a restart (not
rebuilt), empty after an `InstallSnapshot` (rebuilt from engine records only),
and silently evicted at its cap. So the *same committed log entry* applied
differently per replica: one rejected a duplicate stage of an already-resolved
transaction, another resurrected the intent. The apply-time read-modify-write
arms (`KindEval`, `KindEvalBatch`, pending-eval stages) then computed from
diverged bases, which is permanent value divergence (issue #1243; seen as a
`ConsistentRead: false` read missing acked appends). The comment on the map
even called "starting empty after a restart ... as safe as any other eviction"
— true for a best-effort optimisation, false for something that gates an apply
outcome.

Rule: `apply` is a pure function of (log prefix, durable state). A cache may
accelerate it, never decide it. If a decision needs a fact ("this txn already
resolved at this key"), write the fact into the engine in the same merge batch
as the event that created it, and read it back from the engine. Prefer a
per-key row (bounded, overwritten) over a per-event row (unbounded). Fixed with
`txn::resolved_marker_key` rows (`txn-resolved-marker` v1).

How to catch the class: a SimEnv test that applies the same entry on (a) a
replica that restarted after the fact, and (b) one that missed the fact and
caught up by `InstallSnapshot`, then asserts all replicas are identical. The
snapshot variant needs the lagging replica more than `COMPACT_RETENTION_CAP_ENTRIES`
(4096) entries behind: follower-aware compaction otherwise keeps the log and the
replica catches up by `AppendEntries`, which silently makes the test vacuous
(it passed on the buggy code until the pad count was raised). Grep apply arms for
`Mutex`/`Arc` reads that influence a rejection when touching apply code.
(`animus-cp-data` `tests/it/resolved_restage_replica_determinism.rs`.)

Corollary: a new durable row is a cross-node format the moment the snapshot
image carries engine rows (class G, see
`2026-10-05-classify-a-format-by-every-path-its-bytes-travel.md`). It broke
nothing here only because `engine_image` omits it while the gate is closed and
every scan that feeds clients/backups skips it; one unfiltered scan
(`local_scan_kind_ordered`) would have panicked decoding the marker value as an
envelope. Grep every raw `storage.scan` over the base scope when adding a row
family.
