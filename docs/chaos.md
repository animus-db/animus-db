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
| `ANIMUS_CHAOS_OUT` | `$TMPDIR/animus-chaos-out` | where a **failed** run keeps `history.json`, `events.txt`, `op-trace.txt`, `violations.txt`, and each node's log. |

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

### Faults not implemented, and why

| Fault | Status |
|---|---|
| **Clock skew** | Not in the bare harness. A real skew needs `libfaketime` (not assumed present) or Chaos Mesh `TimeChaos`; `deploy/chaos/time-skew.yaml` is the Kubernetes design. The node uses monotonic time for every deadline (ADR 0003), so the interesting surface is `env.wall_now()` (DynamoDB TTL, HLC wall component), not election timing. |
| **Slow disk** | Not in the bare harness: needs a FUSE/`dm-delay`/cgroup IO throttle (root). `deploy/chaos/io-latency.yaml` is the Kubernetes design. |
| **Disk full** | **Pending, deliberately not added.** Behaviour on a real node is undefined today (issue #1185; ADR 0074 section 2 / criterion D-7 define the contract it must meet). A scenario that can only fail is not a chaos test. `deploy/chaos/io-disk-full.yaml` exists only as a "DO NOT RUN" draft. |
| **Packet loss / reordering** | Loopback TCP cannot lose packets; `deploy/chaos/network-loss.yaml` is the Kubernetes design. |
| **Power loss (unsynced writes)** | `kill -9` does not discard the page cache. Needs a VM-level power cut or `dm-flakey`. |

## Which B-criteria this meets

| ID | Status | Why |
|---|---|---|
| B-1 | **Not met** | Implemented against real processes: process kill, network partition, delay, SIGSTOP stall. Clock skew, slow disk and disk full (the other three named by the criterion) are not: see the table above. |
| B-2 | **Not met** | Every scenario records a history and runs the oracles, but see the findings below: the criterion says "passes". |
| B-3 | Not met | Policy in ADR 0074 section 1. Finding 1 is not seed-reproducible, but its engine-level mechanism reproduces deterministically (below); converting that into a regression cell is the fix PR's job. |
| B-4 | Met | `.github/workflows/chaos.yml` (PR smoke + nightly). |

## Findings (first runs, 2026-10-04)

Reported, not fixed: the harness PR does not change product code.

**Finding 1: acknowledged writes lost on keys touched by an aborted cross-tablet transaction
(probable root cause identified, not yet confirmed end to end).** Seen in 2 of ~25 `smoke`
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

**Finding 3: a consistent read hung 60 s after a full-cluster power cut.** `chaos_kill`, seed
777: after `kill -9` of all nodes and restart, `GetItem(ConsistentRead: true)` of key 0 via node 0
never answered within 60 s while node 1 answered immediately (the other 23 keys were fine; the
durability/atomicity lines on key 0 in that run are artifacts of the missing final read). Node
logs show `txn_resolver_loop: unresolved_decided record has been unreachable past RECOVERY_GRACE`.
Likely an unresolved transaction intent on key 0 whose resolution is not driven by the read.
Artifacts were captured under `$ANIMUS_CHAOS_OUT/kill-777`; not investigated further.

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
