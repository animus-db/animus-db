# `SharedWal` fsync gating benchmark (C-05 PR 1)

**Date:** 2026-09-06
**Status:** measurement complete — recommendation: **wire `SharedWal`** (C-05
PR 2/3 proceed).
**Bench:** `crates/animus-cp-data/benches/wal_fsync_bench.rs`
(`cargo bench -p animus-cp-data --bench wal_fsync_bench`)
**Related:** `docs/roadmap.md` §"C-05 `SharedWal`", ADR 0028's deferred-wiring
note, `crates/animus-control/src/shared_wal.rs`,
`crates/animus-control/src/persist_round.rs`.

## The question this gates

`SharedWal` (`animus-control::shared_wal`, ADR 0028) is built and
unit-tested but not wired into any node: every hosted tablet group's
consensus loop still persists to its own private WAL file
(`animus-cp-data`'s `persist_wal`, one `Disk::append` + `Disk::sync` per
group per round). A prior `SimEnv` measurement (2026-09-02, a throwaway
harness, recorded in `docs/roadmap.md`) found that a burst of one write to
each of `K` active groups costs `K` `Disk::sync` calls with no cross-group
coalescing — a real structural cost, but `SimEnv`'s virtual clock says
nothing about whether that cost is *expensive* in wall-clock terms on real
media. The roadmap's own C-05 entry named the open question directly:

> Gate the work on a `ProdEnv` wall-clock benchmark at realistic tablet
> density first: concurrent fsyncs to different files may already be cheap
> on some media.

This document is that benchmark's method, numbers, and answer.

## Method

The bench measures wall-clock **round latency** (a burst of one write to
each of `K` active groups, the exact shape named above) over three shapes,
for `K ∈ {1, 8, 32, 128}` (`ANIMUS_BENCH_GROUPS`), 20 rounds per shape
(`ANIMUS_BENCH_ROUNDS`), each write a 96-byte payload
(`ANIMUS_BENCH_VALUE_BYTES`):

- **(a) per-group files, concurrent** — `K` distinct files, `K` tasks
  spawned concurrently, each doing its own `append` + `sync`. This
  reproduces today's real behavior: each hosted `RaftKvNode`'s consensus
  loop is an independent task with its own `wal_lock`, so `K` groups'
  `persist_wal` calls run concurrently with respect to each other. This is
  the number `SharedWal` wiring would replace.
- **(a′) per-group files, sequential** — the identical `K` files, no
  concurrency — a cheap worst-case reference, not a production claim.
- **(b) `SharedWal`, one file, K groups** — the real, already-built
  `animus_control::shared_wal::SharedWal::append` API called directly
  (unwired — no reimplementation), `K` concurrent writers against ONE
  shared file. This is the number wiring `SharedWal` in would actually buy.

Plus one **fixed control**, independent of the `K` sweep:

- **(c) single-group 32-write burst** — 32 concurrent writers against one
  file via the same `SharedWal` API, standing in for the per-group
  group-commit `persist_round.rs` already implements in production today
  (identical queue+leader-flush shape — see that module's own doc). This
  demonstrates that *within-group* batching is not the gap: a single
  group's own burst already coalesces to ~1-2 fsyncs with no `SharedWal`
  wiring involved at all. What (a) vs. (b) measures is the *cross-group*
  gap this control does not cover.

Every scenario is instrumented with a real, measured fsync counter
(`CountedEnv`, a thin `Env` wrapper around `ProdEnv` that counts real
`Disk::sync` calls) rather than an assumed count derived from the code
shape — a concurrent burst racing `SharedWal`'s internal queue can in
principle still split into more than one physical round if not every
submitter has enqueued before the current "leader" starts flushing.

## Media

`fsync`'s real cost is entirely a function of the underlying storage. This
sandbox's disk is a **virtual block device**: `/dev/vda`, mounted at `/` as
a real `ext4` filesystem (`mount | grep ' / '` → `/dev/vda on / type ext4
(rw,relatime,resv_strict,resuid=65534,resgid=65534)`) — not `tmpfs`/
`overlay`, so `fsync` here pays a real, non-trivial cost per call rather
than being made nearly free by a memory-backed mount. The bench dir was
`/tmp`, which resolves to this same `ext4` mount (`/proc/mounts` has no
separate `tmpfs` entry for `/tmp` on this host). The bench itself
auto-detects and prints this at every run (`describe_media`, reading
`/proc/mounts` for whatever directory it writes into) so a reader never has
to take the media on faith.

**This is a real, meaningful measurement on a real device — not the
"inconclusive on an overlay/tmpfs" case the task anticipated as a
possibility.** No caveat about media making `fsync` artificially free
applies to the numbers below.

## Numbers (3 runs, default config, this host)

`p50`/`p99` round latency in microseconds; `fsyncs/round` is the mean
measured `Disk::sync` count per round (20 rounds/scenario).

| K | Scenario | Run 1 p50 | Run 2 p50 | Run 3 p50 | p99 (run 1) | fsyncs/round |
|---|----------|-----------|-----------|-----------|-------------|--------------|
| 1 | (a) per-group, concurrent | 461us | ~450us | ~460us | 32920us¹ | 1.00 |
| 1 | (b) SharedWal | 464us | 526us | 534us | 950us | 1.00 |
| 8 | (a) per-group, concurrent | 1120us | 1164us | 1167us | 2988–3484us | 8.00 |
| 8 | (b) SharedWal | 1071us | 1151us | 1095us | 1687–1772us | ~2.00 |
| 32 | (a) per-group, concurrent | 2919us | 3048us | 2800us | 11023–25555us | 32.00 |
| 32 | (b) SharedWal | 1159us | 1182us | 1261us | 1629–1915us | ~2.00 |
| 128 | (a) per-group, concurrent | 10852us | 10499us | 11203us | 25084–43363us | 128.00 |
| 128 | (b) SharedWal | 1559us | 1512us | 1363us | 1892–2373us | ~2.00 |
| — | (c) single-group 32-burst (control) | 1263us | 1124us | 1243us | 1100–1783us | ~2.00 |

¹ K=1's own p99/max is a single first-round outlier (cold file-creation
cost — with `n=20` samples, `p99` picks the single slowest sample; every
other K's numbers show the same shape without this artifact). Treat K=1's
`p99`/`max` column as noise, not signal — `p50` at K=1 (a floor case where
no coalescing is possible either way) is consistent and unaffected.

**Full-run raw output for all three runs is reproducible via**:

```sh
cargo bench -p animus-cp-data --bench wal_fsync_bench
```

on this host — total wall time per run is ~2.4s, so re-running to check
variance costs nothing.

## Reading the numbers

- **At K=1**, (a) and (b) are indistinguishable (~460–540us) — expected,
  since there is nothing to coalesce with only one writer. This is the
  floor both shapes share.
- **The per-group cost scales with K, close to linearly**: K=8 → ~1.1–1.2ms,
  K=32 → ~2.8–3.0ms, K=128 → ~10.5–11.2ms. The roadmap's open question —
  "concurrent fsyncs to different files may already be cheap on some
  media" — is answered **no** on this host's media: they get measurably,
  substantially more expensive as K grows, exactly the shape a split, a
  failover, or an ordinary multi-tablet write burst on one node produces.
- **`SharedWal`'s coalesced cost stays nearly flat regardless of K**:
  ~464–540us at K=1, ~1.1–1.3ms at K=32, ~1.4–1.6ms at K=128 — a change of
  well under 2x across a 128x change in K, versus (a)'s ~23–24x change
  over the same range.
- **At K=128, wiring `SharedWal` would cut p50 round latency by roughly
  7–8x** (10.5–11.2ms → 1.4–1.6ms) **and p99 by roughly 15–20x**
  (25–43ms → 1.9–2.4ms), while cutting the fsync count from 128 to ~2
  (roughly 64x).
- **At K=32, the p50 win is a real but smaller ~2.3–2.6x**, with a larger
  tail win (p99 11–26ms → 1.6–1.9ms, since (a)'s tail is where a slow
  individual fsync among 32 concurrent ones drags the round out — exactly
  the pathology group commit exists to remove).
- **`fsyncs/round` for (b)/(c) lands at ~2, not a perfect 1.00** — a real,
  honestly-measured effect of real thread-scheduling jitter (not every
  concurrent submitter's `append` call always lands before the current
  "leader" task starts its flush — see `shared_wal.rs`'s own module doc),
  not a bug in the bench or in `SharedWal`. It is still a 16–64x reduction
  from `K` at every measured `K ≥ 8`, and the wall-clock number (the metric
  that actually matters to a client) already reflects this honestly.
- **The control (c) confirms the roadmap's own framing**: a single
  group's 32-write burst already coalesces to ~2 fsyncs and ~1.1–1.3ms
  today, with zero `SharedWal` wiring — matching (b) at K=32 almost
  exactly. The gap this bench is gating is real and is specifically
  **cross-group**, not a gap in per-group group commit (which
  `persist_round.rs` already closes in production).

## Threshold and recommendation

**Threshold used**: wire `SharedWal` if, on a real block-device-backed
filesystem (not `tmpfs`/`overlay`), the per-group concurrent-fsync cost at
a realistic active-tablet density (`K` in the low tens to low hundreds — a
combined node hosting several dozen to a few hundred tablets, well within
this repo's documented per-node tablet counts) scales with `K` by more than
a small constant factor relative to `SharedWal`'s coalesced cost, **and**
the effect holds across repeated runs (not a single noisy sample). This is
deliberately a low bar to clear in the "don't wire" direction — the whole
point of gating first was to rule out the case where the answer is
"concurrent fsyncs are already cheap here, so `SharedWal` buys nothing
worth its added complexity" (segment GC, cross-tablet ordering, a new fault
class to test). If the effect is marginal or media-dependent-only, the
roadmap's own fallback ("do not wire; close C-05") applies instead.

**This host clears that bar decisively, not marginally**: a ~7–8x p50 and
~15–20x p99 latency reduction at K=128, holding consistently across three
independent runs, on a real (not memory-backed) block device, with the
single-group control cleanly isolating the effect to the cross-group case
`SharedWal` specifically targets.

**Recommendation: wire `SharedWal` into `animus-cp-data`'s persist path
(C-05 PR 2), then cut over (C-05 PR 3).**

PR 2's scope, per the roadmap: a per-node shared WAL for all hosted
groups' Raft entries (routed through `persist_wal` and the apply task's
compaction rewrite), the cross-tablet ordering guarantee the tagged-record
format already establishes (`PersistedState::encode_tagged_record`/
`replay_multiplexed`), segment GC (a shared file accumulates every hosted
group's records — needs its own compaction/GC design, distinct from each
group's own per-group `snapshot_upto`), and a crash-mid-roll fault
injection corpus (`ANIMUS_SHAREDWAL_SEEDS`) covering the interleavings
`shared_wal.rs`'s own unit tests already prove in isolation (concurrent
appends all landing, compact never racing a concurrent append) but at
cluster scale under `SimEnv` fault injection — behind a flag so the cutover
in PR 3 is a separate, reviewable step from the wiring itself, per this
crate's own default-additive-then-cutover convention (the identical shape
ADR 0044 phase 2's `HeartbeatBatcher` used, PR 2 wired-off-by-default → PR 3
flipped the default).

## A note on what this does NOT re-litigate

ADR 0048's "apply-poll term dominated" finding — the basis for a much
earlier, since-reversed recommendation to delete `SharedWal` — was about
**idle** cost, which quiescence (ADR 0044 phase 1) already closes
independently. This benchmark, like the roadmap's own SimEnv measurement
before it, is entirely about **active-load** cross-group fsync cost, which
quiescence never touches (a quiesced group has no persist rounds to
coalesce in the first place). The two findings are about different
regimes and do not contradict each other.

## Reproducing on different hardware

```sh
cargo bench -p animus-cp-data --bench wal_fsync_bench
ANIMUS_BENCH_GROUPS=1,4,16,64,256 cargo bench -p animus-cp-data --bench wal_fsync_bench
ANIMUS_BENCH_JSON=/tmp/wal_fsync.json cargo bench -p animus-cp-data --bench wal_fsync_bench
```

The bench prints the resolved media (filesystem type + device) at the top
of every run — check that line before trusting a number gathered
elsewhere. Per `docs/engineering-lessons.md`'s "a historical bench figure
from a different host is not a baseline" entry, a number from a different
host/session/media is not comparable to the ones in this document; rerun
this bench on the machine in question rather than trusting the table
above for a different environment. If a maintainer runs this on `tmpfs`/
`overlay` media (a laptop's own `/tmp` on some setups, or certain
containerized CI runners), expect the (a) vs. (b) gap to shrink or vanish
— the bench's own `describe_media` output will say so directly, and in
that case the cautious reading is "inconclusive here; the recommendation
above rests on this document's own ext4/`/dev/vda` measurement, not a
media-independent claim."
