# ADR 0081 — LSM write stall and compaction IO limit: an L0 slowdown/stop that delays but never fails a committed entry, and a node-wide compaction bytes/s budget

- **Status:** Accepted (2026-10-18): the maintainer accepted every recommendation, including the recommended answer to each open question. Implemented the same day; the as-built amendment at the end records the measurement, what changed and what is deferred. The defaults below are kept; the measurement did not contradict them.
- **Date:** 2026-10-18
- **Origin:** issue #1300; the maintainer decision of 2026-10-12 (ADR 0079 decision 6) that an L0 write stall and a compaction IO rate limit get their own storage-engine ADR; the maintainer order "ADR first, then measure, then code".
- **Amends:** none. **Depends on:** ADR 0003 (determinism, the `Env` seam), ADR 0004/0008 (storage engine, `LsmEngine`), ADR 0016/0017 (CP data plane, the apply task), ADR 0050 (a private engine per tablet), ADR 0073 (upgrade compatibility), ADR 0074 (every queue is bounded), ADR 0078 (`LoadMonitor`; decision 4: advisory signals never fail health or the roll gate), ADR 0079 (compaction permit pool and its L0 bypass; decision 9, the shared `lag -> factor` helper), ADR 0080 (`ProposeResult::Busy`, the edge mapping to `ThrottlingException`; the 2026-10-18 amendment, follower applied-index bound, #1330).
- **Not in scope:** a `SimEnv` disk-cost model (ADR 0079 decision 7 stands: `DiskConfig` `sync_delay` is enough, see the test plan), charging memtables or compaction buffers to ADR 0080's `QueuedBytes` (its question 8 left that to this ADR; decision 7 below answers it), read-side IO limiting, a block cache, preemptible/sub-compaction (named as a follow-up in decision 4), the disk-full path itself (#1218/#1331/#1334 already own it; this ADR only composes with it).

## Context

The engine has two protections against falling behind, and neither is the one a production LSM needs.

1. **Compaction is trigger-only.** `compaction_trigger` (default 4, `DEFAULT_COMPACTION_TRIGGER`, `crates/animus-storage/src/lsm.rs:197`) only decides when an L0 to L1 compaction becomes *due* (`next_compaction_plan`, `lsm.rs:3159`). Nothing reacts to L0 growing past it except ADR 0079's permit-pool bypass at `2 x compaction_trigger` (`COMPACTION_L0_BYPASS_FACTOR`, `lsm.rs:596`), which only keeps the pool from making L0 worse. There is no L0 write stall and no IO limit.
2. **The only write backpressure is the memtable gate.** `await_backpressure` (`lsm.rs:1489`) polls every 1 ms (`BACKPRESSURE_POLL`, `:249`) while the memtable exceeds `flush_threshold_bytes x BACKPRESSURE_OVERSHOOT_FACTOR` (4, `:246`), at most `BACKPRESSURE_MAX_POLLS` = 10,000 times (`:253`, so about 10 s of virtual or real time), and then returns `Err` (`StorageFull` if the last maintenance run hit ENOSPC, else `Backend`, `:1506-1519`). It exists only with `LsmOptions::background_maintenance`, which `LsmOptions::production()` sets (`:324`) and every `animusd` engine uses (`animusd/src/lib.rs:6427, 8898, 14647`).

Maintenance is **one task per engine** (`maintenance_scheduled`, `trigger_background_maintenance`, `:1443-1478`) and each tablet has its own engine (ADR 0050), so a node with N written tablets runs up to N compactions at once. ADR 0079 added a node-wide pool for that concurrency, default `max(1, cores/2)` when `--cores` is set. It bounds *how many* compactions run, not *how many bytes per second* they move.

### What happens today when the gate returns `Err`: it is unsafe (evidence, not inference)

The gate runs **after** the write is durable and applied. `merge_batch` (`lsm.rs:2711`) and every other write path call `log_and_apply` (WAL record, one fsync, memtable apply) and then `after_write_maintenance` (`:1413`, called from the seven write sites at `:2654-2891`). So an `Err` from the gate says "this write took effect, and I am failing it anyway".

The only caller that matters is the CP-data apply task, and it applies **committed Raft entries**:

- `StallingEngine::retry` (`animus-cp-data/src/apply_stall.rs:88-117`) retries an engine call **only** on `StorageFull`: it pauses, sleeps 100 ms and replays the identical call, sound because a `StorageFull` operation changes nothing durable or visible and every apply-task write is idempotent per `(key, version)`. Any other error is returned as is.
- The bulk path then asserts: `flush_pending` (`animus-cp-data/src/lib.rs:12664-12669`) is `if let Err(e) = storage.merge_batch(..) { assert!(halted, "raftkv apply merge batch failed while running: {e}") }`, and `merge_seal_marker_or_halted` (`:12705-12716`) does the same for seal markers. A `Backend` error from the gate on a live group therefore **panics the apply task** (the file's own words: it "must never be softened into a swallowed error", since skipping a committed entry loses an acked write).
- So today's behaviour after 10 s of an overshot memtable is: with a `StorageFull` cause, the apply task re-runs the already-applied batch (harmless, but each retry waits another 10 s inside the gate); with any other cause (a failing disk, or maintenance simply slower than the writer for 10 s), the tablet's apply task panics. The second case is reachable by plain overload with no fault: a compaction that rewrites the whole of L1 (`lsm.rs:319`'s own doc) holds the maintenance lock, so the flush that would relieve the memtable waits behind it.

A bounded poll that ends in an error is the wrong shape for an engine under a consensus log. Whatever the stall does, it must be able to **delay** a committed entry for as long as it takes and must never **fail** one.

### Related facts

- **Flush and compaction share one lock.** `MaintenanceLock` (`lsm.rs:615`) serializes a flush against a compaction for the whole operation, and `maybe_flush_and_compact` (`:1313`) runs the flush first and then drives compactions to quiescence in the same task. A long L0 to L1 compaction (it rewrites every overlapping L1 table, `:319`) therefore blocks the flush, the memtable grows, and the memtable gate engages. Only one maintenance task runs per engine, so a compaction wait also delays a flush that is due.
- **Compaction is monolithic.** `run_compaction` (`:1700-1960`) reads all inputs into one merged map, GC-filters it, and writes the output partitions (`target_table_bytes` 2 MiB) one by one, with an `env.sync` per output file (`:1832`), before the single manifest swap. The only natural pacing points are before the run and between output partitions.
- **L0 tables are small by default.** `DEFAULT_FLUSH_BYTES` is 64 KiB (`:194`), so a flushed L0 table is about 64 KiB and the memtable hard cap is 256 KiB. RocksDB's 20/36 L0 thresholds assume 64 MB memtables; the numbers below are expressed as multiples of `compaction_trigger` for that reason.
- **Reads pay for L0.** Every L0 table can contain any key, so a point read probes each L0 table's bloom filter. L0 count is read amplification as well as a write backlog.
- **ADR 0080 already turns a slow apply into a retryable refusal.** `gated_propose` (`animus-cp-data/src/lib.rs:3836`, bound at `:3863-3900`) refuses `Client`-class commands with `ProposeResult::Busy` when `last_log_index - engine_applied` (and, since the #1330 amendment, the quorum-th highest *applied* index) exceeds 512 entries / 4 MiB, and `map_overload` (`animusd/src/dynamo.rs:11035`) renders `busy:` as 400 `ThrottlingException` (`TransactionCanceledException` reason `ThrottlingError` for transactions, `UnprocessedItems`/`UnprocessedKeys` for batches). #1331 added the same refusal while `is_storage_full()` (`lib.rs:3874`). `Internal` is bounded at 2x and `Liveness` (2PC decisions, `ReadCeiling`) is never refused. The apply task is the only writer of a tablet's engine; if it stalls, `engine_applied` freezes and that bound engages by itself.
- **Measurement baseline.** `docs/density-measurements.md` section E3 (single run): 600 puts/s of 4 KiB over 50 groups on `LsmEngine<ProdEnv>` is above this node's apply capacity (about 400-450 puts/s at 2 workers); write amplification about 3.5x on disk, sstable count steady (2447-2571 then falling), so compaction kept up there. Section H: RSS ratchets in 0.2-0.5 GB steps that coincide with compaction or flush bursts, and a closed-loop floor of 0.4-0.55 GB is "allocator retention plus LSM compaction and flush buffers". **No run has produced an L0 count or a compaction-bytes/s figure**, and no run has saturated disk IO before CPU. Everything numeric below is therefore provisional and labelled so.

## Decision

Seven decisions, all **node-local** (class L): L0 slowdown and stop thresholds, a stall that waits and never fails, a node-wide compaction write-bytes budget, a flush-first maintenance order, observability and knobs, the compatibility classification, and the test plan with the measurement that fixes the defaults. A short note on memory charging (the question ADR 0080 left here) is at the end of decision 5.

### 1. L0 slowdown and stop thresholds

Per engine, three L0 table-count levels, all derived from `compaction_trigger` `T` (default 4) so they stay ordered when `T` is tuned:

| Level | Default | Value at `T`=4 | Effect |
|---|---|---|---|
| trigger | `T` | 4 | an L0 to L1 compaction becomes due (unchanged) |
| bypass (urgent) | `2T` (ADR 0079, unchanged) | 8 | compaction skips the permit pool and the IO bucket's wait (decision 3) |
| **slowdown** | `3T` | 12 | writes to this engine are delayed (decision 2) |
| **stop** | `5T` | 20 | writes to this engine wait until L0 falls below `stop` (decision 2) |

`LsmOptions` gains `l0_slowdown_factor` (3) and `l0_stop_factor` (5), validated by `LsmOptions::validate` (`lsm.rs:370`, tests in `lsm_options_validation.rs`): `1 <= trigger`, `COMPACTION_L0_BYPASS_FACTOR < slowdown < stop`, or both `0` to disable (today's behaviour; every existing test and the control keyspace keep it). The stall exists only under `background_maintenance` (inline mode already compacts synchronously on the write path).

**Per engine, not per node.** The quantities a stall protects are per engine: this tablet's read amplification (L0 probes), this tablet's memtable and WAL growth, and this tablet's apply task, which is the only thing a stall blocks. A write delay on one tablet delays only that tablet's apply task; other tablets on the node are unaffected, which is the opposite of RocksDB's shared-instance stall and the reason a per-engine threshold is correct here. The *node* enters through the shared resource (compaction bytes/s and permits, decision 3), not through the thresholds. Scaling the thresholds with the number of engines on the node was considered and rejected: it makes one tablet's read amplification depend on its neighbours.

Alternatives considered:

- **RocksDB's 5x/9x (20/36 at `T`=4).** Rejected as a default: with 64 KiB tables 36 tables is only about 2.3 MiB, but the point is read amplification and the time a stall takes to ripple into the Raft backlog, and 36 L0 probes per point read is a large tax. `3T`/`5T` keeps the stop inside twice the bypass. To be confirmed by the measurement step; both factors are knobs.
- **Slowdown only, no stop.** Rejected: nothing then bounds L0 when compaction is slower than the writer.
- **A count of bytes in L0 instead of tables.** More faithful to compaction cost (a merge is proportional to L0 bytes plus overlapping L1), but table count is what read amplification and the existing triggers use, and the memtable gate already bounds bytes per table. Revisit only if tables stop being roughly equal in size (e.g. a very large flush threshold).
- **Making the memtable gate and the L0 stall one generalised `WriteStall` state** (cause: `Memtable`, `L0Slowdown`, `L0Stop`). Recommended and adopted: one state machine and one set of metrics, so the two causes cannot disagree.

### 2. What a stalled write does: wait, never fail; ADR 0080 does the refusing

**The stall sits at the engine, below Raft, and only delays.** It is a pre-write hook evaluated at the front of the engine's write path, before `log_and_apply` appends the WAL record. Three rules:

1. **Before the WAL append, outside every lock, and outside `applies_in_flight`.** A waiting writer then holds no durable-but-unapplied state, so a flush is never skipped by the `applies_in_flight == 0` hint (`lsm.rs:1327`), and a rejected-or-slow write has done nothing yet. This also moves the memtable gate from "after the write took effect" to "before it takes effect". The post-write `await_backpressure` call is removed.
2. **No time limit and no `Err` from the stall itself.** The 10,000-poll bound and its `Err` are deleted. A stalled writer sleeps on the `Env` clock (deterministic under `SimEnv`) in steps, re-evaluating the state each step, and returns `Ok` when the state clears. It cannot fail a committed entry because it never fails.
3. **A genuine engine fault is not a stall.** If maintenance fails repeatedly for a reason other than ENOSPC (a dying disk, a corrupt table), waiting forever hides it. The stall therefore tracks *consecutive* maintenance failures with no success in between (`background_error`) and, at `maintenance_failure_limit` = 8, returns `Err(Backend)` as the fail-stop signal it is today: the apply task's assert panics the tablet and the node restarts it, the existing semantics for an unrecoverable engine. The bound counts failures, not seconds, so slow-but-working compaction never trips it. An ENOSPC failure is `StorageFull` and is never a limit trip: the stall keeps waiting and the existing `StallingEngine` pause and `is_storage_full()` machinery own that state (#1218/#1331), exactly as before.

**Slowdown is a delay, not a wait.** In `[slowdown, stop)` each write batch of `b` bytes is delayed by `b / rate` where `rate` falls linearly from `l0_slowdown_write_rate` (provisional 16 MiB/s) at `slowdown` to `l0_min_write_rate` (provisional 256 KiB/s) at `stop - 1`. Rationale: a cliff at `stop` makes the Raft backlog fill in one burst and the Busy refusals arrive as a storm; a ramp gives the proposers a smoothly rising refusal fraction. Under the apply task the delay is per tablet. Memtable-over-cap (4x flush threshold) is treated as `stop` for the memtable cause; it has no slowdown band (as today).

**How it surfaces on the wire: composition with ADR 0080, no new variant.**

1. The apply task is blocked inside one engine call. Raft keeps appending and committing (the log is on the WAL, not the engine), so committed-but-unapplied entries accumulate and `engine_applied` stops moving. That is by design (the apply task is the single writer, `apply_stall.rs` header).
2. ADR 0080's propose bound reads exactly that gap. On the leader, `last_log_index - engine_applied` reaches 512 entries / 4 MiB and `gated_propose` returns `ProposeResult::Busy` for `Client`-class commands; a stalled **follower** is seen through #1330's quorum-th applied index, and only when a whole quorum is behind (one stalled follower never refuses writes, which is the correct outcome: commit proceeds without it).
3. `Busy` becomes `busy:` on the existing `ClientResponse::Error` string path and `map_overload` renders it: 400 `ThrottlingException`, `ThrottlingError` for a `TransactWriteItems`, `UnprocessedItems`/`UnprocessedKeys` for batches. That is the retryable, adaptive-retry-friendly error the issue asks for. A stalled write is never a 500, never a hang to `CLIENT_TIMEOUT` (the admission bound refuses before the log is full), and never `ProvisionedThroughputExceededException` (reserved for configured capacity, ADR 0065).
4. `Internal`-class work (TTL reaper, index drain, MREC apply) is bounded at 2x and therefore waits in the stalled engine just like clients do, the intended order (housekeeping yields). `Liveness`-class commands are never refused, are applied by the same stalled apply task, and so wait for the stall to clear; that is safe because the stall's escape is compaction, which does not depend on the apply task or on any Liveness command (decision 3's no-deadlock argument).

**What this does not do.** It does not make a `Busy` appear sooner than the backlog bound allows: at 4 KiB values 512 entries is 2 MiB, which at the 400-450 puts/s measured fills in about a second. A direct engine-to-admission flag (refuse `Client` writes at once while the leader's own engine is at `stop`, like `is_storage_full()` at `lib.rs:3874`) is possible and cheap, but it adds a second coupling and a flag whose clearing must be woken. Recommended: **not in v1**; add it only if the measurement shows the fill time or the extra backlog matters. Open question 2.

Alternatives considered:

- **Keep the bounded poll, raise the bound.** Rejected: any finite bound ends in the `Err` that panics the apply task (evidence above); a very large bound is an unbounded wait with extra steps and a confusing error at the end.
- **Fail fast: return a retryable error from the engine.** Rejected for the engine layer. A committed entry cannot be refused: the apply task has no "skip" and `flush_pending` asserts. Failing fast belongs at admission, before the entry is appended, which is exactly what ADR 0080 provides. The engine can only decide *when*, never *whether*.
- **Stall at admission only (no engine-level wait).** Rejected: without a wait, the apply task keeps feeding a memtable that compaction cannot drain, and admission would need the engine's L0 state on every propose. The engine wait is the backstop that bounds memtable and L0; admission is the signal that keeps the Raft log from growing behind it.
- **Delay the Raft commit instead (slow `AppendEntries` acks).** Rejected: it couples the engine to consensus liveness and would look like a slow follower, which is what ADR 0078's machinery is trying not to provoke.

### 3. A compaction IO rate limit: a node-wide write-bytes budget

**What is limited.** Bytes **written** by compaction (output SSTables), with the flush's bytes charged but never delayed (decision 4). Reads are not limited: they are served by the page cache in the common case, and the engine cannot tell. A fixed bytes/s on writes is the figure an operator can reason about against a disk's sustained write bandwidth.

**One budget per node, shared across engines**, created beside the `BackgroundBudget` (ADR 0079) and installed on each engine through the same seam as the permit pool (`CompactionGate`, installed by `LsmTabletFactory`, `animusd/src/lib.rs:14650-14656`): an `Env`-seam-clean, synchronous, `BTreeMap`-keyed token bucket in `animus-cp-data` (or `animus-env`, as ADR 0080's pure outbox is), with the current time passed in. No timers and no tasks: a refill is computed from `env.now()` at each request, so it is deterministic under `SimEnv` and testable as a pure function. Nothing reads the wall clock or `available_parallelism`.

**Shape: a bucket with debt.**

- `rate` bytes/s refill, `burst` = 1 s of rate (capacity), balance may go negative ("debt"). A request of `n` bytes is admitted when the balance is positive and then subtracts `n`; a request is never rejected for being larger than the burst, which matters because a compaction is monolithic (its cost can far exceed any sensible burst) and a limiter that can never admit it would be a livelock. The long-run average is bounded by `rate` plus one compaction.
- **Charge before the run, true up after.** The cost is estimated from the plan (sum of input table sizes, since an LSM merge writes about what it reads before GC) and corrected with the real output bytes when the run finishes (extra debt or a refund). The estimate is taken from the manifest before the maintenance lock is acquired.
- **Pacing points.** The wait happens before the run, never inside it: pacing between output partitions would hold the `MaintenanceLock` (and so block the flush) for the whole throttled duration. Output files are still fsynced one at a time as today (`lsm.rs:1832`), which bounds the size of any single fsync burst against the WAL's fsync path.

**A throttled compaction defers; it does not wait while holding anything.** When the bucket has no credit, the engine does exactly what it does when the permit pool is full: it releases everything, schedules the single pending retry (`schedule_compaction_retry`, `COMPACTION_DEFER_RETRY` 100 ms, `lsm.rs:1386`), and the maintenance task *ends*, so the next write can trigger a flush. Waiting in the maintenance task would delay the flush that is due (one maintenance task per engine, flush first, then compaction).

**Order with the ADR 0079 pool.** Take the **permit first, then the bucket**; if the bucket refuses, **drop the permit before deferring**. Holding a permit while waiting for credit is hold-and-wait: a few throttled engines would hold every permit and stall the pool for engines that are urgent. Permit = how many; bucket = how fast; urgent bypasses both.

**Escape hatch (the no-deadlock rule).** An engine whose L0 count is at least the bypass level (`2T`, ADR 0079's `l0_overcap`) is **urgent**: it is never refused by the pool (as today) and never deferred by the bucket; it runs now and **charges the bucket into debt**. The debt then delays only the *non-urgent* engines, which is the right priority (the tablet nearest its stall is served first) with no new fairness machinery. Because bypass (8) < slowdown (12) < stop (20), an engine is always urgent for twelve L0 tables before its writes can fully stop, and a limiter that is itself the cause of the stall cannot exist. The full argument that a stop always clears, given a working disk:

1. A stop blocks only writers of that engine; maintenance never waits on writers (writers never take the `MaintenanceLock`, `lsm.rs:611-613`).
2. Flush is never rate limited or pool-limited.
3. An urgent compaction is never refused or deferred by the pool or the bucket, and the pool's size never reaches 0 (ADR 0079 control (e)); `scale_by_lag` has a floor of 1.
4. An L0 to L1 compaction consumes every L0 table present when it runs (`CompactionPlan { source_level: 0 }`), so L0 count strictly falls to the count flushed since.
5. The only things that stop 1-4 are an engine fault (decision 2 rule 3) or a full disk (the existing `StorageFull` pause).

A negative control (decision 7, test plan) removes the escape and shows the stall never clears.

**Fixed rate, scaled by lag, no auto-tuning.** `rate_eff = scale_by_lag(rate, lag)` through ADR 0079 decision 9's shared helper (halved at 30 ms, quartered at 100 ms of scheduling lag), with a floor `rate_min` (provisional 4 MiB/s) so lag can never make the budget a starvation. Reasoning: a node whose scheduler is late is already losing the foreground race and should spend less disk and CPU on background work, and urgent engines bypass it anyway, so tightening is safe. RocksDB-style *auto-tuning* (the limiter raising and lowering its rate from observed demand) is rejected for now: it adds a feedback loop on top of the lag loop, its behaviour is hard to pin in a seeded corpus, and ADR 0078/0079 deliberately keep every knob fixed and legible in `/admin`. The urgent bypass already supplies the one adaptive property that matters (more compaction when L0 is high).

**Default: off (`0`, today's behaviour) until the measurement step names a number.** This is deliberate. The rate that is right depends on the disk, which the node cannot detect (and must not probe: a nondeterministic input outside the `Env` seam). The operator exposes it as an explicit override only (decision 5); it is not derived from CPU. The measurement step (decision 7) produces a recommended default as a function of the measured sustainable write bandwidth, and this ADR is amended with it before the default is turned on.

Alternatives considered:

- **A per-engine limit.** Rejected: the disk is shared; N engines each at `rate` is N x `rate`.
- **Limiting inside the `Disk` seam** (every `append`/`replace` through a bucket). Uniform, but it throttles the WAL and the manifest as well, i.e. the durable-before-visible path, and cannot tell a compaction from a flush. Rejected: the WAL fsync must never be rate limited.
- **Token wait inside the compaction** (RocksDB's per-request `Request`). Rejected for the lock-holding reason above; the engine's compaction is not incremental.
- **Concurrency pool only (ADR 0079 as is).** Insufficient in principle: one permit can still move an unbounded bytes/s, and an engine with one huge L1 can saturate a disk with one compaction. Whether it is insufficient *in practice* is the measurement step's question.

### 4. Flush versus compaction priority

**Flush has priority over compaction, in three senses.**

1. **Never limited.** The flush path takes no permit and no bucket credit. Its bytes are *charged* to the bucket (into debt) so the compaction budget reflects real disk use, but a flush is never delayed by it. A flush is what turns memtable pressure into L0 pressure, and L0 pressure is what compaction relieves, so flush must run for the stall to work at all.
2. **First in the maintenance order.** As today (`maybe_flush_and_compact` flushes before compacting), and a throttled or pool-deferred compaction ends the task (decision 3), so a flush that becomes due while compaction waits is never queued behind a wait.
3. **A flush waiter is served before a compaction waiter on the `MaintenanceLock`.** Today the lock wakes all waiters and registration order wins. A flush that arrives while a compaction holds the lock cannot preempt it, but when the lock is released the flush should win over a queued next compaction. Implemented as two waiter classes in the lock's pure state (deterministic: ordered `Vec`s, no timers). Counted by `storage_flush_blocked_micros`, which is what tells us whether more is needed.

**What this does not fix (named, not done).** A flush that arrives *while* a long compaction runs still waits for it, because the compaction is monolithic and holds the lock for the whole of it. If the measurement shows `storage_flush_blocked_micros` dominating stalls, the remedies are (a) let a flush run concurrently with a compaction, serializing only the manifest swap (a smaller critical section; it touches the crash-safety argument at the swap and needs the full `lsm_crash`/`lsm_disk_faults` corpora re-run), or (b) make compaction incremental (sub-compactions that release the lock between slices). Both are engine rewrites of the part the crash corpora protect most; neither is proposed until a measurement says the cheaper ordering above is not enough. Open question 4.

Alternatives considered:

- **Status quo (FIFO lock).** Rejected as the end state: it lets a queued compaction go ahead of a due flush for no reason.
- **Charge flush bytes as a hard cost to the same bucket.** Rejected: delaying a flush delays the very thing that relieves the stall.
- **A separate budget for flush.** More knobs, same disk. A shared bucket with flush charged-but-not-gated is simpler and shows up in the same gauge.

### 5. Observability, knobs and the operator surface

**Metrics** (additive `Metric` variants in `animus-env/src/metrics.rs`, no-ops under sim; names follow the `storage_*` and `bg_*` families):

| Metric | Kind | Meaning |
|---|---|---|
| `StorageWriteStalls{Cause}` | counter | writes that entered a delay or wait, cause in `memtable`, `l0_slowdown`, `l0_stop` |
| `StorageWriteStallMicros{Cause}` | counter | total time spent in the stall, by cause |
| `StorageEnginesStalled{Level}` | gauge | engines on this node currently slowed / stopped |
| `StorageL0TablesMax` | gauge + hwm | highest L0 count of any engine on the node |
| `StorageCompactionIoRate`, `StorageCompactionIoDebtBytes` | gauge | effective bytes/s after lag scaling; current debt (negative balance) |
| `StorageCompactionThrottledMicros`, `StorageCompactionUrgent` | counter | time deferred by the bucket; compactions that bypassed it at the urgent level |
| `StorageFlushBlockedMicros` | counter | time a due flush waited on the `MaintenanceLock` |
| `StorageMaintenanceFailureStreak` | gauge | consecutive maintenance failures (decision 2 rule 3) |

**Admin.** `/admin/health` gains an advisory `lsm_stall` object (`engines_slowed`, `engines_stopped`, `l0_max`, `compaction_io_rate`, `compaction_io_debt`) beside the background-permit object (ADR 0079). Per ADR 0078 decision 4, it is **advisory only**: a stall never fails the health check or the roll gate (a node under write load is working, not unhealthy). The per-tablet admin view already shows sstable counts (`lsm_sstables`) and gains the engine's stall level and L0 count. The shed counters ADR 0080 already feeds into the 5xx-ratio alert need nothing new: a stall surfaces as `OverloadShedBusy`, and the runbook (`docs/runbook/overload-and-throttling.md`) gains one paragraph on reading `StorageWriteStall*` next to it.

**Knobs.**

| Knob | Default (provisional) | Meaning |
|---|---|---|
| `LsmOptions::l0_slowdown_factor`, `l0_stop_factor` | 3, 5 (x `compaction_trigger`) | decision 1; `0` disables the L0 stall |
| `LsmOptions::l0_slowdown_write_rate`, `l0_min_write_rate` | 16 MiB/s, 256 KiB/s | decision 2 ramp |
| `LsmOptions::maintenance_failure_limit` | 8 | decision 2 rule 3 |
| `--compaction-io-bytes-per-sec` / `cluster_settings.bg_compaction_io_bytes_per_sec` | `0` (off) | decision 3; node-local, mirrors `--bg-compactions` |
| (constants) | burst = 1 s, `rate_min` 4 MiB/s, lag scaling via `scale_by_lag` | change by ADR |

`animusd` passes the L0 factors through `LsmOptions::production()` (so they are on by default for every tablet engine once the measurement step ratifies the numbers); the control keyspace engine (`production_raw_versions`, `animusd/src/lib.rs:8225`) keeps the stall off. **Operator:** `spec.nodeCapacity`'s sibling, a `spec.background.compactionIoMiBPerSec` explicit override rendered into the node-local `cluster_settings` block, **never derived** (the node cannot know the disk; the operator would be guessing from a CPU request). Unset means off. The operator's pod sizing note (ADR 0080: `floor + B + working set`) gains the engine memory figure the measurement step produces.

**Memory charging (ADR 0080 question 8).** ADR 0080 left `LsmEngine` memtable and compaction/flush buffers uncharged to `QueuedBytes` and handed the policy to this ADR. **Recommended: still do not charge them.** The engine's own bounds are the right layer: the memtable is bounded by the hard cap (4x flush threshold) per engine, and the compaction materialisation (the closed-loop floor of 0.4-0.55 GB, section H) is bounded by concurrency (the ADR 0079 permit pool) times input size. The IO budget does not raise peak compaction memory, because a throttled compaction defers *before* it reads its inputs (decision 3). A node-wide compaction-memory budget is a different mechanism (it would gate on input size, not rate); it is left until the measurement shows the permit pool does not bound it (step 1 records peak RSS per concurrent compaction). Open question 5.

### 6. Compatibility: everything here is class L

None of this is a cross-node or durable format (ADR 0073). The stall is in-memory engine timing that runs before the WAL append and writes nothing; the IO budget is an in-memory counter; the refusal is ADR 0080's existing `busy:` string. So: **L (node-local), no `Gate`, no cluster-version bump, no golden fixture, no harness transcoding**, and a rolling upgrade needs no ordering. The full table, with the mixed-version and crash-safety reading, is under "ADR 0073 compatibility classification" below.

### 7. Test plan and the measurement that fixes the defaults

The full test plan (Tier 0-3, the `ANIMUS_LSM_STALL_SEEDS` corpus, negative controls and the `ProdEnv` liveness test) is under "Test plan" below. The measurement step, which the maintainer's order puts before any code, is:

**Measure before fixing any default.** The maintainer's order is ADR first, then measure, then code; the measurement run is implementation step 1 and ratifies or replaces every provisional number, recorded in `docs/density-measurements.md` and an as-built amendment. It extends the `ProdEnv` `LsmEngine` density rig (ADR 0079 calibration step 1, `election_backpressure_liveness.rs`'s LSM open-loop command; ADR 0080 section H):

1. **Characterise first, change nothing.** With the mechanisms off, drive an open-loop write load that exceeds one compaction's bandwidth (4 KiB and 64 KiB values, 50 hot tablets and 300 hot tablets, 150 s or more: section H's plateau needs it) and record, per engine and per node, over time: L0 table count (hwm and distribution), memtable bytes, **compaction bytes written/s and count**, flush count, `storage_flush_blocked_micros`-equivalent (time a due flush waited on the lock), WAL fsync latency percentiles, apply lag, sustained disk write bandwidth (a `dd`/`fio` write baseline on the same volume), and RSS. Question answered: how high does L0 get, how long is a flush blocked by a compaction, and **does disk IO saturate before CPU?** (ADR 0079 decision 7's trigger for a `SimEnv` disk-cost model.)
2. **Reproduce the unsafe gate.** A cell where maintenance is slower than the writer for more than 10 s without a fault, to confirm the evidence above on a real engine (the apply task panics today); this becomes the always-on regression for decision 2.
3. **Then turn the mechanisms on one at a time**: stall only, then stall + IO budget at 100 %, 50 %, 25 % of the measured sustainable bandwidth, and report goodput, p99 write latency, refusal fraction, L0 hwm, and the effect on WAL fsync p99 (the question for decision 3's "does a compaction fsync burst inflate the WAL fsync" concern).
4. **Decide the defaults** from the data: the L0 factors, the ramp rates, the IO rate as a fraction of measured bandwidth, and whether the stall's effect on the Raft backlog needs the direct admission flag (question 2) or the flush-concurrency remedy (question 4).

No product change before step 1 except test knobs.

## ADR 0073 compatibility classification

Everything here is **L (node-local)**. No new `KvCommand`, no replicated `Metadata` field, no WAL record, no SSTable or manifest field, no snapshot field, no key layout, no frame, `ClientRequest` or `ClientResponse` variant, and no change to any message's meaning. The WAL (`LWL1` v2), manifest and SSTable formats are unchanged: the pre-write hook runs *before* the WAL append and writes nothing itself.

| Item | Class | Reason |
|---|---|---|
| L0 slowdown/stop, write delay/wait, memtable gate rewrite | L | In-memory engine behaviour; decides when this node's apply task proceeds. Not persisted; a restart recomputes L0 count from the manifest. |
| `LsmOptions` fields | L | In-memory options, never serialized. |
| Compaction IO bucket, flush charge, urgent bypass | L | In-memory `Env`-seam struct; no file or wire form. |
| `MaintenanceLock` waiter classes | L | Internal ordering; no persisted effect. A flush-first order does not change which manifests can be produced (the same swaps in a different order, both already reachable today). |
| `busy:` refusal and its `ThrottlingException` mapping | L (existing) | ADR 0080's classification; this ADR adds no string, no code, no variant. |
| Follower stall visibility | existing G | Rides ADR 0080's `Gate::FollowerAppliedIndex` (cluster version 6, #1330). Below it, a stalled follower is invisible to the leader's bound exactly as a slow follower is today; nothing new wedges. |
| `cluster_settings.bg_compaction_io_bytes_per_sec`, operator `spec` field | F-adjacent, additive | Optional, `skip_serializing_if`; the frozen `ClusterConfig` v1 fixture is untouched (ADR 0078/0079/0080 precedent). |
| `Metric` variants, admin JSON | not a format | Additive. |

No `Gate`, no cluster-version bump, no `required_gate` row, no golden fixture, no new transcoding in the upgrade-restart harness. **Mixed version and restart:** a stall changes only timing on the node that runs it, so a rolling upgrade needs no ordering; the roll's per-node health wait is unaffected because the new health fields are advisory. A downgraded or N-1 node simply has no stall (today's behaviour). The crash-safety invariants (manifest swap is the durability point, a segment is removed only after a manifest not naming it is durable) are untouched because no durable write is added or moved.

## Test plan

Every distributed behaviour gets a seeded, fault-injecting simulation (lessons 2026-10-04, 2026-10-06, 2026-10-14, 2026-10-18: each negative control must bite on an observable the mutation removes, with a workload that makes the mechanism matter; a liveness cell needs an actor that cannot run out of work). **No new `animus-sim` primitive**: compaction is made slow with the existing `DiskConfig` `sync_delay` and `replace` delay (`animus-sim/src/lib.rs:246-262`), which are fixed virtual latencies that draw no RNG; ADR 0079 decision 7 stands unless step 1 of the measurement shows IO saturating first.

**Tier 0, pure/property.** The token bucket (refill from a passed-in time, debt, a request larger than the burst is admitted, true-up, rate bound over a window, lag scaling and the `rate_min` floor, deterministic for equal inputs); the stall state machine (levels from L0 count and memtable, `0` disables, ordering invariants in `validate`, the linear ramp, monotone in L0); `MaintenanceLock` waiter classes (flush before compaction, FIFO within a class, no lost wakeup); permit-then-bucket ordering and "drop the permit before deferring".

**Tier 1, `SimEnv` corpus, new `animus-storage` `tests/it/lsm_write_stall.rs`, knob `ANIMUS_LSM_STALL_SEEDS` (default 1), `ANIMUS_LSM_STALL_CELL=<substring>`, `ANIMUS_SEED=<seed>` replays.** `LsmEngine<SimEnv>` with `background_maintenance`, a writer that cannot run out of work, a slowed disk. Common oracles: **no write ever returns `Err` from a stall** (the headline), every acked write is readable after the stall clears and after a crash and restart (the `lsm_crash` oracle), L0 count never exceeds `stop + (writes already past the check)` slack, the stall clears within a bound after the disk speeds up, and the engine's own invariants hold. Cells:

1. steady overload on a slow disk (L0 climbs, slowdown then stop, then clears); 2. memtable over cap behind a long compaction (flush blocked by the lock; the old gate would have errored at 10 s: run past 10 s of virtual time and assert `Ok`); 3. crash mid-stall, restart, resume; 4. ENOSPC during a stall (the write waits as `StorageFull`, the existing pause semantics, no limit trip, resumes when space returns); 5. a non-ENOSPC maintenance fault repeated (the failure streak reaches the limit and the engine returns `Err(Backend)`, fail-stop, not an unbounded wait); 6. torn/corrupt tails armed (`lsm_disk_faults` style) while stalled; 7. N engines sharing one budget (a node-wide bucket across 8 engines: the aggregate compaction write rate stays within `rate` + one compaction, an urgent engine is never delayed by the debt of others); 8. flush priority (a flush arrives behind a compaction waiter and is served first on release; `flush_blocked` is bounded by one compaction); 9. throttled compaction defers and ends the maintenance task (a flush that becomes due during the wait runs); 10. lag scaling (the effective rate falls at 30/100 ms lag and recovers, never below `rate_min`).

**Negative controls (each asserts the oracle FAILS):** (a) stall off (factors `0`): L0 grows without bound under the cell 1 workload; (b) the old gate (poll bound and `Err`): cell 2 returns `Err` after 10 s of virtual time; (c) stall wait placed *after* the WAL append (the old position): a flush is skipped by `applies_in_flight` and the memtable overshoots the cap; (d) no urgent bypass (bucket applies to every compaction): at a rate below the write rate L0 reaches `stop` and never clears (the deadlock the escape hatch exists to prevent); (e) permit held while waiting for credit: a few throttled engines starve an urgent one; (f) flush charged as a gated cost: a flush is delayed behind the bucket and the memtable gate engages; (g) wait inside the compaction (holding the lock): flush blocked for the throttled duration; (h) limit counted in seconds instead of failures: a slow-but-working compaction trips fail-stop.

**Composition cell, `animus-cp-data` `tests/it/` (new `lsm_stall_admission.rs`, same knob).** A real RF-3 group over `LsmEngine<SimEnv>` with a slow disk and an open-loop proposer: the apply task is stalled by the engine, `engine_applied` freezes, and ADR 0080's propose bound returns `Busy` for `Client` writes (the surplus refused, none lost); `Liveness`-class resolves are admitted, wait, and complete after the stall clears (no 2PC deadlock); a stalled follower outside the quorum causes no refusal and one inside a stalled quorum does (#1330); **the apply task never panics** (negative control (b) at this level: the old gate under the apply task panics the tablet); the raftkv linearizability oracle holds (a `Busy`-refused write is a definite failure, an acked write survives).

**Tier 2, cluster, `animusd` over `SimCluster`'s LSM backend and the DynamoDB wire.** Open-loop SDK-like clients (3 attempts, exponential backoff) against nodes whose disks are slowed by `sync_delay`: only `ThrottlingException`, `ThrottlingError`, `UnprocessedItems`/`UnprocessedKeys` and `ServiceUnavailable` ever appear (never a 500, never a response later than the bound), acked writes durable, the node gate's occupancy bounded. Added to the existing `sim_cluster_overload` family rather than a new test file.

**Tier 3, `ProdEnv` liveness (`animus-storage/tests/lsm_stall_prod.rs`, real threads, timeout-guarded `#[tokio::test(multi_thread)]`, converged-or-timeout polls).** `SimEnv` proves ordering and bounds, not real-thread liveness (house rule): (i) a writer over a real `LsmEngine<ProdEnv>` with a deliberately low IO rate reaches `stop`, observes the stall, and **resumes** once the urgent compaction runs; no `Err`, no deadlock, the lock waiters and the retry task make progress with real wakers; (ii) the measurement-step run doubles as the acceptance run: RSS flat after 60 s, L0 hwm at or under `stop`, WAL fsync p99 reported with the budget on and off; (iii) `animusd/tests/overload.rs`-style real-TCP check that a write storm against a stalled engine yields only the retryable codes. Both `lsm_crash` and `lsm_disk_faults` corpora are re-run unchanged in the same PR (no durable write moved).

**Docs/CI:** the new knob joins the `CLAUDE.md` test-scaling table and `corpus-deep.yml`'s nightly tier.

## Alternatives considered (overall)

- **Do nothing beyond ADR 0079's permit pool.** The pool bounds concurrency but not bytes/s, and the unsafe `Err` gate remains: a tablet's apply task can still panic under plain overload (evidence above). Rejected.
- **Put the entire answer at admission (ADR 0080) and leave the engine alone.** Admission cannot see the engine's L0/memtable state without a coupling, and a backlog bound never makes the memtable or L0 safe by itself: the engine must still be able to refuse to grow. The two layers are complementary: the engine waits, admission refuses.
- **One shared RocksDB-style `WriteController` with a single delayed-write rate for the node.** Rejected: the engines are independent (one apply task per tablet); a node-wide write delay would punish healthy tablets for one tablet's L0.
- **A smaller `L0` trigger for lower read amplification.** Orthogonal tuning; it increases write amplification and is not a stall. Left to the measurement step.
- **Compaction priority by L0 count across engines (a node-wide scheduler picking the most urgent engine first).** More complete than the urgent-bypass-plus-debt rule, but it needs a central queue and ordering state; the debt rule delivers the property (the nearest-to-stall tablet is served first) without one. Revisit if the measurement shows starvation of mid-L0 engines.

## Implementation plan

One implementation PR on one branch (repo operating mode), ordered commits, each green on all gates. This ADR is its own docs-only PR first.

1. **Measurement first** (decision 7; test knobs only): the unsafe-gate repro, the characterisation run, the saturation question. Record in `docs/density-measurements.md`; amend this ADR with the defaults and with whether question 2 or 4 is needed.
2. **Pure primitives:** the token bucket, the stall state machine, the `MaintenanceLock` waiter classes; Tier 0.
3. **Engine stall:** the pre-write hook, deletion of the poll bound and post-write gate, `LsmOptions` fields and validation, the failure-streak fail-stop; cells 1-6, controls (a)-(c), (h); the `lsm_crash`/`lsm_disk_faults`/`lsm_maintenance` corpora unchanged and green.
4. **Compaction IO budget:** the node-wide bucket beside `BackgroundBudget`, `CompactionGate` seam extension, defer-and-end semantics, urgent bypass, flush charge; cells 7-10, controls (d)-(g).
5. **Composition:** the `animus-cp-data` composition cell, the Tier 2 cells; metrics and `/admin/health`; the cluster-settings mirror, `animusd` flag, operator override.
6. **Tier 3** liveness test and the acceptance run; defaults turned on.
7. **Docs:** this ADR to Accepted with an as-built note; ADR 0079 (its decision 6 and the "L0 write stall" open items) and ADR 0080 (question 8) cross-references; `crates/animus-storage/CLAUDE.md`, `animus-cp-data`, `animusd`, `animus-operator` guides; `docs/runbook/overload-and-throttling.md`; `docs/roadmap.md`; `CLAUDE.md` knob table; `website/` if it states storage or overload behaviour; a `docs/lessons/` entry.

## Open questions for the maintainer

1. **Ship the stall and the IO budget as one workstream, with the IO budget default-off until measured?** Recommended: yes. The stall is a safety fix (it removes a path that panics a tablet's apply task) and should be on by default for tablet engines as soon as step 1 ratifies the factors; the IO budget is a tuning control whose right value depends on the disk, so it stays `0` (off) until the measurement names a number, and the operator offers an explicit override only. Alternative: split the unsafe-gate fix (decision 2) into its own earlier PR; it is small and independent of the thresholds and the budget, but the repo's operating mode prefers one workstream per PR.
2. **Should a leader refuse `Client` writes immediately while its own engine is at `stop`** (a flag like `is_storage_full()`, `lib.rs:3874`), rather than waiting for the backlog bound to engage? Recommended: no, not in v1. The backlog bound already turns the stall into `Busy` in about a second at the measured apply rate, with no new coupling and no flag to clear; add the flag only if step 1 shows the extra backlog or the delay to first refusal matters.
3. **Default L0 factors 3T slowdown / 5T stop and a 16 MiB/s to 256 KiB/s write-delay ramp?** Recommended: yes as provisional, replaced by step 1. They are multiples of `T` so they stay ordered under tuning, and they put the stop inside twice the urgent-bypass level. RocksDB's 5x/9x is the alternative if step 1 shows the compaction cannot keep L0 under 12 at realistic loads.
4. **Is "flush-first ordering plus defer-and-end" enough for flush-versus-compaction, or should flush run concurrently with a compaction now?** Recommended: enough for now; measure `storage_flush_blocked_micros`. Concurrent flush (serializing only the manifest swap) or incremental compaction are the remedies if a flush is routinely stuck behind a long L0 to L1 compaction, but each touches the part the crash corpora guard hardest and should be a separate, measured ADR amendment.
5. **Keep memtable and compaction buffers out of `QueuedBytes` (answering ADR 0080 question 8)?** Recommended: yes. They are bounded by the engine's cap and the permit pool; the IO budget defers before a compaction reads its inputs, so it does not raise peak memory. Revisit with a compaction-input-size gate only if step 1 shows peak RSS per concurrent compaction escapes the permit pool's bound.
6. **Fail-stop after `maintenance_failure_limit` = 8 consecutive non-ENOSPC maintenance failures (apply-task panic, node restart), or wait forever and only alarm?** Recommended: fail-stop, counted in failures not seconds. It is what the engine does today for a persistent fault, but only after an arbitrary 10 s; a stuck unrecoverable engine that waits forever is a node that looks healthy and serves nothing. The panic path is the existing, tested one.
7. **Operator surface: an explicit `spec.background.compactionIoMiBPerSec` only, never derived?** Recommended: yes. The node cannot see disk bandwidth, and deriving it from CPU would be guessing; a documented sizing note (a fraction of the volume's sustained write bandwidth, from the measurement) is the honest guidance. Alternative: a storage-class table in the operator, rejected as an unverifiable promise.
8. **Per-engine thresholds, not scaled by the number of engines on the node?** Recommended: yes (decision 1): a tablet's read amplification and memtable are its own, and a stall only blocks that tablet's apply task. The node-level coupling lives in the shared IO budget and the permit pool.

## As-built amendment (2026-10-18)

Implemented in one workstream (issue #1300). Everything is node-local class L: no `Gate`, no
format change, no fixture, no cross-node message (checked, see "Compatibility" below).

### What was built

- **Stall before the append** (`animus-storage` `stall.rs`, `LsmEngine::stall_before_write`).
  `StallPolicy::decide(l0, memtable_bytes, write_bytes)` returns `Go`, `Delay{cause, delay}` or
  `Wait{cause}`. It is called first in every mutating method (`put`, `merge`, `merge_tombstone`,
  `merge_batch`, `delete`, `delete_range`, `write_batch`), outside every lock. `LsmOptions` gained
  `l0_slowdown_factor`/`l0_stop_factor` (3 and 5 in `production()`, 0 in `default()` and
  `production_raw_versions()`), `l0_slowdown_write_rate` (16 MiB/s), `l0_min_write_rate`
  (256 KiB/s) and `maintenance_failure_limit` (8), validated by `LsmOptions::validate`
  (`bypass < slowdown < stop`, or both 0; `0 < min <= slowdown`; limit >= 1; the new error is
  `StorageError::InvalidStallOptions`). The old 10,000-poll memtable gate and its post-append
  `await_backpressure` are deleted.
- **Reconciliation with #1338** (which landed first): the failure-count limit is kept as written
  (8 consecutive non-ENOSPC maintenance failures, counted in failures, not seconds). The stall
  returns `Err` only for that and for `StorageFull`, and `StorageFull` now takes one **fresh
  maintenance run** first (`confirmed_full`): `background_full` is sticky until a successful run
  clears it and a waiting writer is the only thing driving maintenance, so a stale verdict would
  fail it forever after space came back. Decision 2's "no `Err` for elapsed time" and decision
  3's pre-append position hold.
- **Compaction write budget** (`animus-cp-data` `compaction_io.rs`, `BackgroundBudget`). An
  `IoBucket` in byte-nanoseconds with debt and a 1 s burst, charged before the run from
  `plan_input_bytes`, settled to the actual bytes written; `scale_by_lag` with a 4 MiB/s floor
  (never above the configured rate). Order is permit, then bucket; a throttled run drops its
  permit and ends (`schedule_compaction_retry`, 100 ms), so a throttled compaction never holds a
  pool permit. Urgent runs (L0 >= 2T, the existing `COMPACTION_L0_BYPASS_FACTOR`) are admitted
  into debt, which is what keeps the limiter from deadlocking a stall. Flush is charged
  (`io_charge`) but never delayed. The seam is three default methods on `CompactionGate`
  (`io_admit`, `io_settle`, `io_charge`), so every other gate implementation is unchanged.
  Default rate 0 = off.
- **Maintenance lock**: `MaintenanceLock` has flush and compaction waiter classes; flush waiters
  are woken first, a cancelled waiter hands its turn on (drop-safe, ids for deregistration). Time
  flush spends waiting is `storage_flush_blocked_micros`.
- **Observability**: 16 new metrics (`storage_write_stalls_{memtable,l0_slowdown,l0_stop}`, their
  `storage_write_stall_micros_*` totals, `storage_engines_slowed`, `storage_engines_stopped`,
  `storage_l0_tables_max`, `storage_l0_tables_hwm`, `storage_compaction_io_rate`,
  `storage_compaction_io_debt_bytes`, `storage_compaction_throttled_micros`,
  `storage_compaction_urgent`, `storage_flush_blocked_micros`,
  `storage_maintenance_failure_streak`), a node-wide `StallLedger` that aggregates the per-engine
  state into the gauges, and the advisory `lsm_stall` object on `/admin/health` (never fails a
  probe or the roll gate, ADR 0078 decision 4).
- **Knobs**: `animusd --compaction-io-bytes-per-sec` and `cluster_settings.
  bg_compaction_io_bytes_per_sec` (CLI wins, conflict is a startup error as for the other `bg_*`
  fields); the operator exposes only the explicit `spec.background.compactionIoMiBPerSec`
  (MiB x 2^20, `0`/unset renders nothing, never derived).
- **A defect found on the way**: a split child was opened without the node's compaction gate
  (`clone_engine`), so it was outside the permit pool and the budget. `install_node_seams` now
  covers open and clone.

### Measurement (before code)

Rig: `animus-cp-data` `tests/it/lsm_stall_measure.rs` (`#[ignore]`d sweep, `cargo test -p
animus-cp-data --test it lsm_stall_measure -- --ignored --nocapture`), `LsmEngine<SimEnv>`, 1 KiB
values, a node pool of 2 compaction permits, 60 s of virtual time per cell, the disk slowed only by
`DiskConfig::set_sync_delay` (ADR 0079 decision 7). Modes: everything off (the old position of the
gate, factors 0), the stall on, and the stall plus the budget at 100 %, 50 % and 25 % of the
sustained compaction rate measured in the off run. One writer per engine (see finding 3). Columns
for the cells that matter (full output in `docs/density-measurements.md` section I):

| Cell (engines x writers, sync delay) | Mode | acked/s | L0 high-water | flush blocked (ms) | compaction MiB/s |
|---|---|---|---|---|---|
| 1 x 1, 5 ms | off / stall | 90 | 4 | - | - |
| 8 x 1, 5 ms | off | 705 | 8 | 26,051 | 0.036 |
| 8 x 1, 5 ms | budget 100 / 50 / 25 % | about 716 | 8 | 2,379 / 1,270 / 1,264 | 0.021 |
| 32 x 1, 5 ms | off | 2,844 | 8 | 11,376 | - |
| 32 x 1, 5 ms | budget | unchanged | 8 | about 5,056 | - |
| 32 x 1, 30 ms | off | 516 | 8 | 44,622 | - |
| 8 x 8, 5 ms | off | memtable stalls about 2,014 s in 60 s | - | - | - |

Findings:

1. **Level 0 never exceeded 8, twice the trigger, in any cell, with everything off.** The urgent
   bypass (L0 >= 2T compacts even with an empty pool) and the memtable cap hold the line before
   the 3T slowdown is reached. The 3T/5T levels are a backstop for a compaction that cannot run
   (starved CPU, wedged disk), not something a slow disk alone reaches. The active cause under a
   slow disk is the **memtable**. This is why the L0 cells of the corpus use a `Wedge` gate that
   withholds even urgent compaction (outside the gate's contract, stated in the file docs).
2. **The budget does not cost goodput** in these cells (within noise of the off run) and cuts the
   time flushes wait behind compaction by 2x to 20x (26,051 ms to 1,264-2,379 ms at 8 x 1), because
   a deferred compaction does not hold the maintenance lock. The sustained compaction rate here is
   tiny (0.036 MiB/s), which is why the default stays off: there is no number to set until a
   workload on a real volume has one (open question below).
3. **Flush starves under concurrent writers.** With 8 writers per engine the memtable stall
   totals about 2,014 s in 60 s of virtual time and goodput collapses (124 acked/s for one engine
   with 8 writers): `flush` defers while `applies_in_flight > 0` and with several writers there is
   always one in flight, so the memtable reaches its hard cap with L0 low. A pre-existing flush
   liveness defect that the stall exposes; recorded as a lesson and filed as a follow-up, not
   changed here (it touches the part the crash corpora guard hardest).
4. The characterisation did not contradict the provisional defaults, so **none changed**: 3T/5T,
   16 MiB/s to 256 KiB/s, failure limit 8, burst 1 s, floor 4 MiB/s, retry 100 ms, budget off.

ProdEnv baseline on the sandbox volume (sequential write with an fsync per MiB): 170.5 MiB/s, the
order of figure the budget is sized as a fraction of (`docs/density-measurements.md` section I).

### Open questions, as answered

All eight recommendations were accepted and built as recommended: (1) one workstream, budget
default-off, explicit operator override only; (2) no immediate refusal at the stop level, the
propose bound does it (the composition cells show a backlog of about 64 entries before the first
`Busy`); (3) 3T/5T and the 16 MiB/s to 256 KiB/s ramp; (4) flush-first plus defer-and-end
(`storage_flush_blocked_micros` drops 2x to 20x, finding 2; concurrent flush stays a separate
measured amendment); (5) memtable and compaction buffers stay out of `QueuedBytes`; (6) fail-stop
after 8 consecutive non-ENOSPC maintenance failures, counted in failures; (7) the operator
override is explicit only; (8) per-engine thresholds.

### Tests

- Tier 0/1 (`animus-storage`): `stall.rs` unit tests, `maintenance_lock_order_tests` (4),
  `tests/it/lsm_write_stall.rs` (`ANIMUS_LSM_STALL_SEEDS`, `ANIMUS_LSM_STALL_CELL`): cells 1 to 6
  (ramp and stop, resume, no `Err`, crash mid-stall, with torn/corrupt tails, `StorageFull`,
  the failure-count limit) and controls (a) the old post-append gate, (b) the bounded poll, (c) a
  stall inside compaction, (h) a limit counted in seconds; each control fails its oracle.
- `animus-cp-data` `tests/it/compaction_io_budget_corpus.rs`: cells 7 to 10 (bounded non-urgent
  bytes, no urgent deferral, permit-then-bucket order, flush never delayed, lag scaling, zero
  rate is off) and controls (d) no urgent bypass, (e) permit held while throttled, (f) and (g)
  flush gated.
- Composition, `animus-cp-data` `tests/it/lsm_stall_admission.rs`: a stall surfaces as `Busy`
  through the ADR 0080 / #1330 bound and never as a panic; one wedged follower causes no refusal,
  both do; a `Liveness`-class resolve is admitted and completes after the stall; control (b) the
  old gate takes the apply task down.
- Tier 2, `animusd` `sim_cluster_lsm_stall`: DynamoDB wire over `SimCluster`'s LSM backend, only
  documented refusals, acked writes durable, a fresh write after; control: the old gate.
- Tier 3, `animus-storage` `tests/lsm_stall_prod.rs`: `ProdEnv`, timeout-guarded.

### Compatibility (verified)

Class L. The stall and the bucket are in-process state; no durable format (WAL, manifest, SSTable,
snapshot) changed, no wire or replicated enum gained a variant, `cluster_settings` gained one
optional node-local field (`bg_compaction_io_bytes_per_sec`, `#[serde(default)]`, ignored by an
older binary that never reads it), and `lsm_crash`, `lsm_disk_faults` and
`check-format-fixtures.sh` are unchanged.

### Deferred

- ~~Flush starvation by `applies_in_flight` under concurrent writers (finding 3)~~: fixed by issue #1341, see the amendment below.
- A default for the compaction write budget: needs a workload on a real volume; the sizing note in
  the runbook is the interim guidance.
- ~~Concurrent flush~~ (question 4): done in #1341, see the amendment below. Incremental or
  partial L0 to L1 compaction remains deferred: it is now the bound on many writers per engine.

## Amendment (2026-10-18, issue #1341): flush lane and flush admission

The maintenance model changed; decision 4 ("a flush is served before a queued compaction") is
superseded because a flush no longer queues behind a compaction at all.

- **Flush admission.** `flush` no longer skips while `applies_in_flight > 0`. Holding the flush
  lane it closes an entry gate (`flush_pending`), waits for the already-admitted applies to
  finish, and snapshots in the same critical section that sees the count at zero, so a partially
  applied batch is still never captured and the WAL watermark/GC argument is unchanged.
- **Two lanes.** Flushes serialize on `flush_lane`, compactions on `maintenance`; both build their
  files outside any lock and meet only in the `install` section (one `write_manifest` fsync, new
  manifest built from the current one, in-memory tables/readers swapped). SSTable seqs come from
  `Inner::alloc_seq`; `manifest.next_seq` at install is the max. Background mode has a separate
  flush task and compaction task, and failure state is tracked per lane. The IO budget is
  unchanged: a flush is still charged, never delayed, and never takes a compaction permit.
- **Effect** (`docs/density-measurements.md` I.1): the memtable cap no longer stalls writers
  behind a compaction ("flush blocked" 0 ms in every cell, it was up to 26 s); with the L0 stall on,
  the 1 engine x 8 writer cell rises from 154 to 169 acked/s (5 ms) and p99 falls from 364 to
  29 ms, and the bound becomes the cost of the L0 to L1 compaction. Control (g) now takes both
  lanes to keep modelling the old single lock.
- Class L, no durable format change (a crash can leave an unreferenced orphan SSTable below
  `next_seq`, which recovery never reads).
