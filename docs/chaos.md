# Real-cluster chaos

R-01 sub-track (b) ([`docs/roadmap.md`](roadmap.md),
[ADR 0074](adr/0074-production-readiness-exit-criteria.md), criteria B-1 to
B-4 in [`production-readiness.md`](production-readiness.md)): a harness that
runs **real `animusd` processes**, drives a continuous **recorded DynamoDB-wire
workload**, injects **real faults**, and runs the existing `animus-test`
oracles over the recorded history.

It does not replace the simulation corpora. Those remain the correctness proof
(root `CLAUDE.md`, ADR 0003). This checks what the simulator structurally
cannot: real sockets, real fsync, real thread scheduling and real `SIGKILL` of
real processes (the `ProdEnv` seams).

## Run it

```sh
export CARGO_TARGET_DIR=/path/to/target   # debug build is enough
# the ~3 minute smoke (one control-leader kill + one partition)
cargo test -p animusd --features chaos --test chaos chaos_smoke -- --nocapture --test-threads=1
# every scenario
cargo test -p animusd --features chaos --test chaos -- --nocapture --test-threads=1
# a longer / re-seeded run
ANIMUS_CHAOS_SECS=900 ANIMUS_CHAOS_SEED=42 \
  cargo test -p animusd --features chaos --test chaos chaos_mixed -- --nocapture --test-threads=1
```

The `chaos` cargo feature (`required-features` on the `chaos` test target in
`crates/animusd/Cargo.toml`) keeps this out of the per-push gates: plain
`cargo test` never schedules it, `cargo clippy --all-features` still compiles
and lints it. No root, `tc`, `ip netns` or `iptables` is needed.

| Knob | Default | Effect |
|---|---|---|
| `ANIMUS_CHAOS_SEED` | per-scenario name hash | fault-schedule + workload-op seed. Printed on every run. |
| `ANIMUS_CHAOS_SECS` | 90 (smoke), 150 (others) | length of the fault window; the run adds bring-up (~15 s) and heal/converge/oracles (~15 s). |
| `ANIMUS_CHAOS_NODES` | 3 | cluster size (3 or 5). |
| `ANIMUS_CHAOS_TABLETS` | 4 | tablets for the workload table (provisioned throughput so the ADR 0067 min-tablet loop splits it; `1` = one tablet). Several tablets means cross-tablet 2PC under chaos. |
| `ANIMUS_CHAOS_RECOVERY_SECS` | 60 | budget for post-heal availability and for each final read. |
| `ANIMUS_CHAOS_TXN` | on | `0` drops the multi-key transaction ops (bisecting aid). |
| `ANIMUS_CHAOS_DIR` | `$TMPDIR` | scratch dir for data dirs/configs/logs. Removed after the run. |
| `ANIMUS_CHAOS_OUT` | `$TMPDIR/animus-chaos-out` | where a **failed** run keeps `history.json`, `events.txt`, `op-trace.txt`, `violations.txt`, `summary.txt` (compact: one line per violation group, the `[replica-convergence]` verdict, per-node counters; what the CI annotation shows first), `counters.txt`, and each node's log. |

CI: [`.github/workflows/chaos.yml`](../.github/workflows/chaos.yml) runs the
smoke on PRs/pushes that touch the harness, and every scenario nightly with a
fresh random seed; failures upload the artifacts above. It is **not a required
check**.

## How it works

- **Processes.** One `animusd --config cfgN.json --node N --dir dataN` per
  node on loopback (`ClusterConfig::generate` addresses). `kill -9` and
  restart reuse the same data dir.
- **Fault proxy (userspace, `tests/chaos_support/proxy.rs`).** Node-to-node
  traffic is routed through a Rust TCP proxy per (node, port) so partitions
  and delay need no privileges. The node still *binds* `127.0.0.1:P`, but its
  config sets `advertise_host = 127.0.77.(i+1)` and lists every other node at
  the proxy address `127.0.77.(j+1):P` (the replicated address book
  `NodeAddrs`, which overrides the static book within 200 ms, is therefore the
  proxy address too). Clients (the workload, admin probes) use the real
  addresses.
  - A **cut** link *stalls* (stops reading, TCP backpressure builds, bytes are
    delivered in order on heal) or *resets* (closes, refuses new). It never
    silently discards mid-stream: that would corrupt the length-prefixed
    framing and test the harness, not the database.
  - The Raft wire (`internal`) is cut **per directed link**: the proxy sniffs
    the sender id from each connection's first frame (`[from_len][from]...`
    after the handshake preamble), so one-way and asymmetric partitions work.
  - The forwarding RPC (`intra`) carries no sender, so it is cut **per
    destination**: during a partition, traffic *to* every node outside the
    largest group is cut, while a minority node can still forward its own
    clients' requests outward. A minority node can therefore still *serve* a
    client by forwarding to the majority (as a client behind a partition that
    only isolates the Raft wire would see), but cannot lead or vote.
  - Delay holds each forwarded chunk for the configured latency.
- **Workload (`workload.rs`).** The same list-append model as the sim corpus
  (`src/sim_cluster_dynamo_corpus.rs`): `UpdateItem SET items =
  list_append(if_not_exists(items,:empty),:v)` with a globally unique `:v`, one
  writer client per key, six clients over 24 keys, mixed with two-key
  `TransactWriteItems` / `TransactGetItems` (2PC across tablets),
  `ConsistentRead: true` reads, and `ConsistentRead: false` reads. Each op goes
  to a uniformly random node (including dead ones). An op that may have reached
  a node and got no clean answer is recorded `info`, never `fail`; only a
  connect failure (no byte sent) is `fail` (animus-test/CLAUDE.md).
- **Oracles.** The recorded `animus_test::History` is fed unchanged to
  `check_cycles`, `check_durability` and `check_convergence`; the converged
  final state (a `ConsistentRead` of every key via node 0 and again via node 1)
  is appended to the history as reads so `check_cycles` also proves every
  workload read agrees with it. Two harness-level checks sit beside them:
  eventual-read prefixes (every `ConsistentRead: false` observation must be a
  prefix of the final list, as in the sim corpus) and **transaction
  atomicity** (a two-key `TransactWriteItems`' appends are both present or
  both absent).
- **Post-heal availability.** After the schedule everything is healed
  (partitions removed, paused nodes resumed, dead nodes restarted), and every
  node must accept a write and serve a consistent read-back within
  `ANIMUS_CHAOS_RECOVERY_SECS`. Time-to-serve is printed.
- **Also failed on:** any node that exits on its own, any `panicked at` in a
  node log, and a workload that barely ran (non-vacuity).

## Reproducibility, stated plainly

The fault **schedule** (what, which node, when, how long) is a pure function of
`(scenario, seed, window, nodes)` and is printed before the run, so a failure
re-runs the same faults with `ANIMUS_CHAOS_SEED=<seed>`. The **processes are
real and not deterministic** (ADR 0003: determinism is `SimEnv`-only), so a
replay is the same fault sequence against a similar, not identical, execution.
A failure may need several replays to recur. That is exactly the class of
bug the sim cannot find; B-3 (ADR 0074 section 1) says what to do with one:
once you can reduce it to something a seed reproduces, it becomes a seeded
corpus cell. Until then it is a finding to file with the seed, the history and
the node logs.

## Scenarios

| Scenario | Faults | Proves | Does not prove |
|---|---|---|---|
| `smoke` | one `kill -9` of the control leader (10 s), later one `Isolate` of a random node (12 s, stall) | acked writes survive a real SIGKILL + same-dir restart and a real partition; no serializability cycle, lost append, torn txn or non-convergence; every node serves after heal | anything about quorum loss, repeated faults, or timing faults |
| `kill` | control-leader kill first, then random node kills (a third of them the control leader), and a full-cluster power cut (`kill -9` all, restart all) | durability of acknowledged writes across real process death and across losing every process at once; recovery of the control plane and every tablet group from disk | power-loss durability below the page cache: `kill -9` leaves written-but-unsynced pages intact, so a missing `fsync` is **not** detected (that needs a dm-flakey / VM power cut) |
| `partition` | isolate one node, minority/majority split, one-way link cut; stall or reset | stale-leader reads and writes are refused or correct under real partitions; heal converges | partitions that also drop the forwarding RPC's source side (see the proxy notes above); packet loss/reordering (loopback TCP does not lose packets) |
| `pause` | `SIGSTOP`/`SIGCONT` a random node or the control leader for 4-14 s | a stalled-but-alive process (GC pause / VM stall / frozen leader) does not violate safety and rejoins | a clock jump (a frozen process's clock continues; see clock skew) |
| `delay` | 100-600 ms added latency on every link | spurious elections and leader flapping under a slow network do not lose or reorder acked writes | asymmetric slowness; bandwidth limits |
| `mixed` | random mix of all of the above | everything above in combination, for `ANIMUS_CHAOS_SECS` (use for the long run) | |

### Disk full on real filesystems

`chaos_disk_full` (`cargo test -p animusd --features chaos --test chaos chaos_disk_full -- --nocapture`;
CI job `chaos-disk-full`, also part of the nightly `chaos_` run) is a different
shape from the schedule-driven scenarios above: three real nodes, each with its
data dir on its **own 64 MiB tmpfs** (`ANIMUS_CHAOS_DISK_MB`), a continuous
recorded workload, and a ballast file that fills a mount to ENOSPC.

1. **One node full.** Fill node 0. Asserts `storage_full` shows on its
   `/admin/health`, writes keep being acknowledged through nodes 1 and 2 across
   several keys (a full tablet leader hands leadership over, #1219), a read
   through the full node is served; then the ballast is deleted and the node
   must report `storage_full: false` and accept a write with no restart.
2. **Every node full.** Fill all three. Loops until every node has refused a
   write, then asserts a named 503 `StorageFull` on every node (promptly,
   under 30 s), `overload_storage_full` moved, and eventual and consistent
   reads of an already-written key are served on every node (4 of 4 each).
   `ANIMUS_CHAOS_KEEP=1` dumps each node's `raftkv-n*.json` and
   `metrics-n*.json` at the end of this phase for diagnosis.
3. **Recovery.** Delete every ballast. Asserts `storage_full` clears on every
   node, a write is acknowledged through each node, and every process kept its
   pid (no restart). Then the usual final reads and oracles, a `panicked at`
   scan of the node logs and the non-vacuity floor.

It **skips with a message** where the process cannot mount (needs root or
passwordless `sudo -n mount`); `ANIMUS_CHAOS_REQUIRE_MOUNT=1` (set in CI) turns
the skip into a failure. Knobs: `ANIMUS_CHAOS_DISK_MB`, `ANIMUS_CHAOS_DISK_TXN=0` (drop the 2PC ops, on by default).

What tmpfs does not prove: tmpfs reports ENOSPC at `write`/`pwrite`; a
delayed-allocation filesystem (ext4/xfs) can report it at `fsync` or on a page
writeback, and a copy-on-write one can fail on overwrite. The mount is a stand-in
for the kernel's ENOSPC, not for every filesystem's timing of it.

**Findings (first runs, seed 283777889631356264, 2026-10-05):**

- **F-1 (resolved, #1228): reads are not reliably served while every node's disk is full.** An
  eventually-consistent `GetItem` of an already-written key timed out on every
  node in two of three runs (0 of 12 served), and was served in the third. A full
  follower acks nothing, not even a bare heartbeat (`docs/resource-bounds.md`
  section 3, "Follower side"), so a full leader loses quorum contact and neither
  the ReadIndex nor the freshness-gated replica read can serve. The documented
  "reads continue" holds in the one-full-node window only.
- **F-2 (resolved): with the multi-key transaction workload on, a disk-full
  window left a 2PC intent that was never resolved.** After space returned, a
  final consistent read of one key timed out indefinitely and the durability and
  txn-atomicity oracles fired (2 of 2 runs with 2PC ops on; 0 of 2 without). The
  node log showed recovery creating an orphan-abort tombstone for a txn whose
  anchor stage never landed, a later `TxnCommit` losing to that abort, and
  `TxnResolve's carried outcome does not match the anchor's own decided record
  - skipping resolve`. **Root cause: not disk-full at all, and not introduced by
  the disk-full commits.** The provisioned table's min-tablet split picked the
  byte-weighted median of the live rows, i.e. an item's own key, and only a
  *streamed* table's split key was rounded to a token boundary. With one item per
  partition key that item is the first row of its token, and a txn record (key
  `token || ...`, derived from the anchor's token) sorts *below* every item of
  that token, so the split put the anchor's item on the right child and its
  record's key range on the left one. The anchor stage applied on the right
  child, every `TxnCommit` and recovery was routed (by record key) to the left
  child where no record existed. The 2PC workload simply made the split key land
  on a txn-touched token. Fix: `decide::align_split_key` rounds every table's
  split key to its token boundary (down, else up). Regression:
  `sim_cluster_auto_split::h_a_split_never_separates_an_item_from_its_txn_record`
  (red before the fix with `anchor commit failed ... CP group leader moved after
  decide`), and `chaos_disk_full` now runs the 2PC ops by default. The same
  mechanism is what failed `chaos-smoke` on this PR's head (`lost acknowledged
  append` plus `txn-atomicity` half-applied, on the one partition key sitting on
  a split boundary) and is the likely actual cause of "Finding 1" below, whose
  tombstone-GC attribution was only "probable". One residual: a tablet whose
  range holds a single token still splits by sort key (the raw key is kept), and
  a transaction anchored on that token can straddle the cut.
  **Three further, independent root causes** turned up while driving the
  remaining failures to zero, all pre-existing and all fixed here:
  (2) *stale grouping across a split*: the coordinator groups a txn's keys by
  tablet from one metadata snapshot, but a stage is routed by the group's first
  key against the live map; a split in between let a two-key group stage whole
  on one child, so the other key's intent and committed value landed in a
  tablet that does not own it. `txn_stage_local` now refuses (before proposing)
  any group with a key outside the leader's range, with the allowlisted
  safe-to-retry-fresh refusal (`sim_cluster_auto_split` scenario (i), red
  before). (3) *`TxnId` collision across groups led by one node*: a `TxnId` was
  `(group's own Hlc ts, node)`, so two transactions anchored on two tablets
  led by the same node could mint the identical id; one's participant resolve
  then committed the other's freshly staged intent early and lost its other
  half. A non-primary group now qualifies the node with its stream
  (`n0#100`). Regression:
  `animus-cp-data` `txn_id_across_groups::two_groups_led_by_one_node_never_mint_the_same_txn_id`
  (red before).
  (4) *a txn decision ordered after the fork entry was applied to the frozen
  parent*: `TxnCommit`/`TxnAbort` (and the orphan-abort tombstone) were the one
  mutating apply arm without the seal check. The children of an in-place fork
  are cloned from the parent's **current** engine by the host reconciler,
  asynchronously and per replica, so a replica that cloned after the decision
  applied held the record `Committed` while one that cloned before held it
  `Pending` (seen live: the same tablet-3 resolve saw `cur=Pending` on two
  replicas and `cur=Committed` on the third) -- replica-divergent children and
  an acked commit whose participant intents never resolved, after which the key
  reverted to its prior value (every later append to it lost; the earlier
  unexplained "key 22 loses all appends" runs). The seal now makes a decision
  on a sealed record key a deterministic no-op (apply stays a pure function of
  the entry and state, ADR 0073 "apply never branches on a gate"), and the
  coordinator, seeing the record still `Pending` on a frozen group after its own
  decide, re-routes the same decision to the record's new owner
  (`txn_decide_anchor`). Regression:
  `split_tablet::a_txn_decision_ordered_after_the_fork_is_a_sealed_no_op`
  (red before: the frozen parent's record flipped to `Committed`).
- **F-3 (resolved, #1228): with every disk full, "some probe saw the 503" is a race, so it is
  measured, not asserted.** All-full means every follower acks nothing and
  each group loses its leader within about a second (F-1), after which a write
  times out instead of returning a 503 (observed with and without the 2PC
  clients, ~1 run in 3). The refusal path stays asserted without the race: the
  single-full-node phase requires the 503 strictly and `overload_storage_full`
  must increment on some node. Fixing F-1 itself (a full node must keep acking
  heartbeats so leaders survive) was done in #1228, and the refusal and the reads
  are asserted in phase 2 again (see `docs/resource-bounds.md`, "Every replica
  full"). Verified 6 of 6 `chaos_disk_full` and 3 of 3 `chaos_smoke` runs.

- **F-4 (resolved for the flapping-node form, see ADR 0074's 2026-10-09 amendment): a
  full leader handed leadership back to a node that had just regained a sliver
  of space, and consistent reads stalled.** Phase 2 reported `consistent
  [0, 0, 0]` (eventual 4 of 4) in about 1 run in 30 locally; the finalize was
  not the cause (the leadership hand-back that precedes it also appears without
  it). Per-group dump of a failing run: one tablet at term
  3, its leader's first-term entry uncommitted because both followers were full.
  Cause and the sustained-health fix are in the ADR amendment; the regression is
  `storage_full_step_down::a_voter_that_just_recovered_is_not_a_successor_until_it_stays_healthy`.
  The harness also tops the ballast up before each node's reads now (a node can
  regain a few KiB mid-window from its own WAL rewrite), and prints how many
  bytes that regained, so "every disk full" holds while it asserts. The lazy
  discovery of fullness (a full node that has received no write does not know it)
  is the remaining open edge.

### Faults not implemented, and why

| Fault | Status |
|---|---|
| **Clock skew** | Not in the bare harness. A real skew needs `libfaketime` (not assumed present) or Chaos Mesh `TimeChaos`; `deploy/chaos/time-skew.yaml` is the Kubernetes design. The node uses monotonic time for every deadline (ADR 0003), so the interesting surface is `env.wall_now()` (DynamoDB TTL, HLC wall component), not election timing. |
| **Slow disk** | Not in the bare harness: needs a FUSE/`dm-delay`/cgroup IO throttle (root). `deploy/chaos/io-latency.yaml` is the Kubernetes design. |
| **Disk full** | **Implemented as `chaos_disk_full`** (issue #1221), see "Disk full on real filesystems" below. The Kubernetes draft `deploy/chaos/io-disk-full.yaml` is still a "DO NOT RUN" design. |
| **Packet loss / reordering** | Loopback TCP cannot lose packets; `deploy/chaos/network-loss.yaml` is the Kubernetes design. |
| **Power loss (unsynced writes)** | `kill -9` does not discard the page cache. Needs a VM-level power cut or `dm-flakey`. |

## Which B-criteria this meets

| ID | Status | Why |
|---|---|---|
| B-1 | **Not met** | Implemented against real processes: process kill, network partition, delay, SIGSTOP stall, disk full (real tmpfs, needs `CAP_SYS_ADMIN`). Clock skew and slow disk (the other two named by the criterion) are not: see the table above. |
| B-2 | **Not met** | Every scenario records a history and runs the oracles, but see the findings below: the criterion says "passes". |
| B-3 | Not met | Policy in ADR 0074 section 1. Finding 1 is not seed-reproducible, but its engine-level mechanism reproduces deterministically (below); converting that into a regression cell is the fix PR's job. |
| B-4 | Met | `.github/workflows/chaos.yml` (PR smoke + nightly). |

## Findings (first runs, 2026-10-04)

Reported, not fixed: the harness PR does not change product code.

**Finding 1: acknowledged writes lost on keys touched by an aborted cross-tablet transaction
(probable root cause identified, not yet confirmed end to end; the 2026-10-05 F-2 root cause above, a
split key inside a token, produces this same signature and is the more likely culprit for runs after
the prior-value fix).** Seen in 2 of ~25 `smoke`
runs (seeds 2799062427773259430 at 45 s, and 308 at 90 s; ~1 in 12 for a given window, not
seed-reproducible). Symptom: one client's keys (those it transacts over) read as empty
mid-run after a `TransactionCanceledException`, then restart their list; the oracle reports
`lost acknowledged append`, `divergent read`, `txn-atomicity` (a half-applied transaction) and
eventual-read prefix violations, all on those keys. In seed 308 the wipe starts ~80 ms after a
cancelled two-key transaction on keys 11 and 5, with no node killed and none restarted at that
moment (the partition had healed 2 s earlier). Probable mechanism: `TxnResolve`'s abort branch
restores the pre-intent value with `storage.get_at(key, intent_version - 1)`
(`crates/animus-cp-data/src/lib.rs`). Production opens tablet engines with
`LsmOptions::default()`, whose `tombstone_grace_versions` is `1 << 20`; data-plane versions are
packed HLCs (`wall_ms << 20 | logical`), so that grace is **1 ms**. Once a compaction runs and the
intent is more than ~1 ms behind the engine's newest version, the shadowed committed value is
garbage-collected, `get_at` returns `None`, and the abort writes a tombstone. A 40-line
`LsmEngine<SimEnv>` test with default grace reproduces `get_at(intent_version - 1) == None`
deterministically (kept with the report, not committed: it fails by design). The sim corpora
use `MemoryEngine`, which never GCs versions, so they cannot see it. Evidence for it being the
cause of the chaos runs is the signature (aborted cross-tablet txn, one client's keys,
compaction-sized delay) plus the engine repro; an instrumented run logging abort-restore misses
was not done.

**Finding 2: an unconditional `UpdateItem` returns `ConditionalCheckFailedException`.** When a
transaction intent covers the key, the apply-time `KindEval` yields `ConditionFailed` for a write
with no condition at all (seen hundreds of times per run in `op-trace.txt`). DynamoDB returns
`TransactionConflictException` for a non-transactional write racing a transaction. Not a
correctness violation (the write definitely did not apply, the harness records it `info`), but a
wire-semantics deviation clients will mishandle.

**Finding 3 (resolved, #1204): a consistent read hung 60 s after a full-cluster power cut.** `chaos_kill`, seed
777: after `kill -9` of all nodes and restart, `GetItem(ConsistentRead: true)` of one key via node 0
never answered within 60 s. Reproduced on 2026-10-11 (the key differed, the symptom was the same).
**Root cause:** `txn_recover` only decides an in-doubt transaction once `now >= record.created_ts.wall_ms +
RECOVERY_GRACE`, but `created_ts` is an HLC (restored from the engine's persisted high-water mark on
restart) and `now` is `ProdEnv`'s process uptime, which restarts near zero. A transaction left in doubt
by a process that had been up 108 s was therefore not recoverable until the new process had been up
~113 s; the blocked key's reads, and every `TxnStage` that needed to push it, failed meanwhile. The gate
now also opens once the recovering node has itself watched the transaction in doubt for a grace
(`ClusterEdgeState::in_doubt_grace_elapsed`). Regression:
`sim_cluster_dynamo_transact::coordinator_crash_recovers_when_the_survivors_clock_restarted_behind_the_record`.

The CI smoke is kept as is (non-required workflow): it will go red intermittently until Finding 1
is fixed, which is the intended signal.

## When it fails

A red chaos run is a finding, not noise: the green invariant applies (root
`CLAUDE.md`, Session operating mode item 4). Do not retry it green, widen a
timeout, or quarantine it. Take the seed from the log, the `history.json`
and `op-trace.txt`/`events.txt`/node logs from the artifact, and:

1. confirm it is not the harness (does the same seed's schedule fail again;
   does `ANIMUS_CHAOS_TXN=0` change it; are the proxy's cuts the only thing
   different from a fault-free run);
2. file an issue with the seed, the violation lines, the history excerpt of
   the affected key and the node logs around the fault;
3. try to reduce it to a `SimCluster` scenario or corpus cell (B-3).

## Kubernetes leg (Chaos Mesh): design, not yet run

[`deploy/chaos/`](../deploy/chaos/) holds the manifests: `PodChaos`
(`pod-kill`), `NetworkChaos` (`partition`, `delay`, `loss`), `IOChaos`
(`latency`, and the blocked `ENOSPC` draft) and `TimeChaos` (clock offset),
selecting the operator-managed pods of an `AnimusCluster` named `chaos`. They
are **pinned (Chaos Mesh 2.7.2) and unvalidated**: this repo's dev sandbox
cannot run `kind`, so no manifest has been applied anywhere. The intended
shape, beside `scripts/e2e-kind.sh` (ADR 0060):

1. `kind` cluster, install Chaos Mesh at the pinned version, apply an
   `AnimusCluster` (3 nodes) from `deploy/operator/example.yaml`.
2. Run the same recorded workload and oracles against a port-forwarded client
   Service (the workload takes node addresses, so the code is reused).
3. Per scenario: `kubectl apply` one manifest, run for the window, `kubectl
   delete` it (heal), converge, run the oracles.
4. A `e2e-kind-chaos` job beside the existing `e2e-kind*` jobs, non-required.

What is missing for it: a `workload`/`oracle` entry point that is not a
`#[test]` (a small binary), the cluster-lifecycle shell (copy
`scripts/e2e-kind.sh`), and a way to find the control leader for
`PodChaos` (read `/admin/raft` through the port-forward). It buys what
loopback cannot: real CNI partitions, real packet loss, kernel-level IO
faults, real clock offsets, and pod rescheduling through the operator.
