# ADR 0022 — Murmur3 partition token (hash the partition key)

- **Status:** Accepted
- **Date:** 2026-08-05
- **Amends:** [ADR 0002](0002-tablets-unit-of-placement.md) (the *partition
  function*; the tablet/epoch/split-merge model is retained)
- **Paired with:** [ADR 0023](0023-table-scoped-tablets.md) (table-scoped tablets
  apply this token *per table*)

## Context

ADR 0002 chose **tablets** — contiguous `[start, end)` ranges over the raw
keyspace — and explicitly *rejected* Dynamo-style consistent hashing, to keep
range scans and targeted migration. In practice the raw keyspace concentrates
load: real key distributions are skewed (sequential ids, timestamps, a hot tenant
prefix), so the range tablet that owns the busy prefix becomes a hot shard, and
balancing degenerates into continuously chasing the load with median splits and
replica moves.

We want **even load distribution by construction**, without abandoning the CP
per-tablet Raft data plane (ADR 0017/0019) — each tablet must stay a single Raft
group serving linearizable reads/writes. Reviving the deleted leaderless AP plane
(ADR 0019) is out of scope.

## Decision

Prefix every data-plane key with a fixed-width **partition token** — a hash of
the *partition key* — so the keyspace becomes a hash ring while the tablet stays
an ordinary byte range. The token sits ahead of the partition key:

```
… || partition_token(pk) || escape(pk) || rk
```

- **Algorithm: MurmurHash3, x64 128-bit variant**, the same hash Cassandra's
  `Murmur3Partitioner` uses. The token is its **top 64 bits, big-endian**, so a
  `KeyRange` byte comparison over the token prefix *is* a numeric token
  comparison. Murmur3 avalanches well (far better spread than a low-entropy
  prefix hash), is non-cryptographic and cheap, and gives us Cassandra parity for
  the CQL adapter. It is implemented inline in `animus-tablet`
  (`partition_token` / `murmur3_x64_128`) — no new dependency, deterministic and
  seedless per ADR 0003.
- The token is computed over the **partition key only** (never the sort/
  clustering key), so all of one partition's rows share the token prefix and stay
  **contiguous and sort-ordered** — single-partition `Query` / clustering reads
  remain one contiguous range scan.
- Because the token is the leading bytes, tablets remain `[start, end)` ranges —
  now over the **hashed token space**. `KeyRange`, `contains`, `split_at`,
  `abuts`, the key→tablet router, the per-tablet Raft groups, and the split
  machinery are **unchanged**. A median-token split bisects load evenly because
  the hash already spread it.

`partition_token` lives in `animus-tablet`; the wire adapters compute it when
assembling a key.

## Consequences

- **Even load by construction.** A skewed key distribution is spread across the
  ring by the hash; no tablet owns a contiguous hot prefix. Median auto-split
  (ADR 0017 Phase 2.4) is retained for residual hot *partitions* but is no longer
  the primary balancing mechanism.
- **Cross-partition ordered scans are gone.** A scan that spans partitions
  returns rows in **token (hash) order**, not key order. Single-partition
  ordering is preserved. (How a full-table scan fans out is ADR 0023.)
- **CP is fully preserved.** Each tablet is still one linearizable Raft group; no
  AP-plane revival, ADR 0019 stands.
- **The hash is a frozen on-disk format.** `partition_token` must be
  byte-identical on every node and across restarts and versions — changing it is
  a data migration, not a code tweak. The empty-input Murmur3 vector `(0, 0)` is
  pinned by a unit test as a spec anchor.
- **ADR 0002 is amended, not replaced.** Tablets are still the unit of placement;
  the epoch is still the fencing token; split/merge still work. Only the *space
  the ranges partition* changed (raw keyspace → hashed token space).
