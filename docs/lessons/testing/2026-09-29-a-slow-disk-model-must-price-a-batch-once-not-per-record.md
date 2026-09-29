# A latency-modelled disk must price a persist round once, not once per record — and a "stall" that is one long in-flight round is not a protocol bug

**Context**: issue #1092. `learner_snapshot_livelock_under_continuous_writer.rs`'s
corpus failed deterministically at depth 2 (seed `0xfa2b71bc5313f78c`): the
learner's applied index sat pinned for the run's whole final quarter while
commits climbed. It looked exactly like the earlier #1070/#1075 stalls and had
the same tell (`learner_applied=22414`).

**What it actually was**: not a Raft/InstallSnapshot defect. Instrumenting the
leader (`next_index`/`match_index`/`snapshot_served_through`), the learner's
`handle_append_entries`, and the driver's persist-round state showed the
learner receiving every `AppendEntries` and computing a success ack each time
— but the leader never saw one. The acks were being held by the consensus
loop's durable-before-visible gate (issue #279) waiting on persist round 5,
which was simply still running. `persist_wal` did one `Disk::append` **per
record**, and `SimEnv`'s `DiskConfig::set_sync_delay` applies to every
`append` *and* `sync`, so a 512-entry batch (`MAX_APPEND_ENTRIES_BATCH`) cost
513 × 20 ms ≈ 10.3 s of virtual time — longer than the 7.5 s final quarter.
Whether a seed passed depended only on where that one round happened to land
relative to the sample point. (On `ProdEnv` the same shape is an `open` +
`write` + `flush` per record.)

**Fix**: coalesce a round's records into one `env.append` followed by the one
`env.sync` (byte-identical on disk; the `SharedWal` branch already did one
physical append per round). Regression: `wal_round_single_append.rs` (red on
the per-record append: 1 of 301 entries durable after 20 latencies; green
after) plus the failing seed pinned in the livelock test file.

**Generalizable lessons**:
- When a "frozen" replica's peer receives messages and computes acks that the
  sender never sees, look at what is *holding* the ack (persist gate,
  `drained` vs `durable`) before suspecting the replication state machine.
  A tiny temporary trace of `drained`/`durable`/`persist_fut.is_some()` in the
  driver found this in one run after several rounds of leader-side tracing had
  not.
- A test comment that states a cost model ("one simulated fsync per received
  message, batching amortizes it") is a claim about the code under test —
  check it against the sim's actual semantics. Here the sim's documented
  per-`append` latency contradicted the comment, and the driver's per-record
  append is what made the comment false.
- A progress assertion over a fixed virtual window is only sound if no single
  in-flight unit of work can exceed the window; sizing the disk latency and
  batch cap so one round is a small fraction of it is part of the test's
  design, not an afterthought.
