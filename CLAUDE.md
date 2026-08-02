# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

CustosDB is a masterless, linearly-scalable NoSQL database in Rust (Dynamo
lineage). It pairs a **leaderless AP data plane** (tunable quorum consistency)
with a small **strongly-consistent Raft control plane** that owns cluster
metadata. Correctness is established by **deterministic simulation testing**.

Status: pre-alpha. Implemented: the scaffold, the `Env` seam, storage (in-memory,
plus a **custom on-disk `LsmEngine`** — a real WAL + SSTable +
compaction LSM doing all I/O through the `Env` disk seam, so it is deterministically
crash-tested under `SimEnv`), the control-plane Raft (WAL durability + recovery +
log-truncating snapshots with `InstallSnapshot`),
the quorum data-plane vertical slice (with read-repair, background
anti-entropy convergence via segment-digest exchange of only divergent ranges,
delete/tombstone propagation, and residency-bounded repair), tablet split/merge
+ multi-tablet routing, the Elle-style recorder/checker (`custos-test`) — now
including an **end-to-end correctness test of the assembled stack** (control
plane + data plane at scale: 3 Raft nodes, 2 tablets, 6 replicas, 4 concurrent
clients) under fault injection (partition + leader kill + crash + heal), checked
for serializability/durability/convergence — a
DynamoDB-style item API over the core plus a **DynamoDB JSON wire protocol**
(`custos-dynamo`: CreateTable/PutItem/GetItem/DeleteItem/Query AttributeValue-JSON
translation, with a per-table schema registry, sort-key conditions =/BETWEEN/
begins_with, and a `ConditionExpression` subset for conditional writes), a
**runnable node + CLI** assembling the planes over `ProdEnv` and
serving clients over TCP, with a now **durable data plane** — each node's data
replica is backed by the on-disk `LsmEngine` over `ProdEnv` by default, so a
value acked to a client survives a process restart (the control plane already
persisted its Raft WAL; the data plane is no longer in-memory-only) — runnable
as one process (`custosd --cluster N`) or one process per node (`custosd --config
FILE --node I`, config via `gen-config`; `--ephemeral` selects the volatile
in-memory engine for dev runs),
which now also **serves the DynamoDB JSON protocol over HTTP**, routing those
requests through the same data-plane coordinator — plus a **minimal CQL v4 wire
protocol** (`custos-cql`: STARTUP/READY handshake + simple INSERT/SELECT-by-key,
routed through that same coordinator) — the **topology-aware placement
engine** (`custos-placement`: residency + failure-domain spread, with the leader
automatically reconciling tablet placement via control-plane `CasTabletReplicas`),
and a **slice of Accord-style leaderless transaction consensus**
(`custos-consensus`: PreAccept→Commit fast path + PreAccept→Accept→Commit slow
path, dependency tracking, consistent commit order, **durable storage-backed
execution** — each replica executes committed transactions in agreed order
against a real `StorageEngine` (`MemoryEngine` under sim) via a WAL it recovers
from on restart — and a **first slice of coordinator failover** (a replica can
recover a stranded transaction whose coordinator died, adopting a committed
decision or forcing the slow path); ADR 0011). Skeletons / future work:
the fuller CQL surface (a real type system, CQL grammar, keyspaces,
prepared statements) and the rest of the DynamoDB surface (Scan,
projection/filter expressions, `ReturnValues`, document/set types, secondary
indexes, durable/replicated table schemas), and the deferred remainder of Accord
(the full dependency wait-graph, the precise recovery ballot + duelling
recoverers + a failure detector, WAL snapshotting, live data-plane integration).

## Per-crate guides

Each crate has its own `CLAUDE.md` with local entry points and gotchas — read
the relevant one before working in a crate:

| Crate | Guide |
|-------|-------|
| `custos-env` | [crates/custos-env/CLAUDE.md](crates/custos-env/CLAUDE.md) |
| `custos-sim` | [crates/custos-sim/CLAUDE.md](crates/custos-sim/CLAUDE.md) |
| `custos-storage` | [crates/custos-storage/CLAUDE.md](crates/custos-storage/CLAUDE.md) |
| `custos-tablet` | [crates/custos-tablet/CLAUDE.md](crates/custos-tablet/CLAUDE.md) |
| `custos-control` | [crates/custos-control/CLAUDE.md](crates/custos-control/CLAUDE.md) |
| `custos-data` | [crates/custos-data/CLAUDE.md](crates/custos-data/CLAUDE.md) |
| `custos-test` | [crates/custos-test/CLAUDE.md](crates/custos-test/CLAUDE.md) |
| `custos-dynamo` | [crates/custos-dynamo/CLAUDE.md](crates/custos-dynamo/CLAUDE.md) |
| `custos-placement` | [crates/custos-placement/CLAUDE.md](crates/custos-placement/CLAUDE.md) |
| `custos-consensus` | [crates/custos-consensus/CLAUDE.md](crates/custos-consensus/CLAUDE.md) |
| `custos-cql` | [crates/custos-cql/CLAUDE.md](crates/custos-cql/CLAUDE.md) |
| `custosd` | [crates/custosd/CLAUDE.md](crates/custosd/CLAUDE.md) |
| `custos-cli` | [crates/custos-cli/CLAUDE.md](crates/custos-cli/CLAUDE.md) |

## Commands

```sh
cargo build --workspace --all-targets
cargo test --workspace
cargo test -p custos-control                       # one crate
cargo test -p custos-control --test control_raft   # one test binary
cargo test -p custos-control survives_leader_kill  # one test by name substring
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all --check
cargo deny check                                   # licenses + advisories (cargo install cargo-deny)
```

All five (fmt, clippy `-D warnings`, build, test, deny) must be green; CI runs
them. Commits require a DCO sign-off (`git commit -s`); this repo is also set up
for GPG-signed commits.

### Replaying a failed simulation

Every simulation run is a pure function of its seed. Tests print the seed in
assertion messages; replay with `CUSTOS_SEED=<seed> cargo test <name>`. The
`Simulator` is driven by `Simulator::new(seed)`.

## The load-bearing constraint: determinism

This is the single most important rule (ADR 0003). **All nondeterminism flows
through the `Env` seam.** In every crate except `custos-env`'s `ProdEnv` and
test code:

- No wall clock — use `env.now()` / `env.sleep()`, never `std::time` or
  `tokio::time`.
- No raw task spawning — use `env.spawn_task(..)`, never `tokio::spawn`.
- No real I/O — use `env.send`/`recv` and `env.append`/`sync`/`read`, never
  `std::net`/`std::fs`/`tokio::{net,fs}`.
- No unseeded randomness — use `env.next_u64()` / `env.gen_below(..)`, never
  `thread_rng`/`OsRng`.
- **No `HashMap`/`HashSet` in logic** — their iteration order is
  nondeterministic. Use `BTreeMap`/`BTreeSet`. This is lint-enforced via
  `clippy.toml`.

Components are generic over `E: Env` (monomorphized, never `dyn`). `Env` is a
supertrait combining `Clock + Rng + Network + Disk + Spawner`, scoped to one
node id. Production wiring uses `ProdEnv` (the only place real time/IO/RNG
live); tests use `custos-sim`'s `SimEnv`.

When a design decision changes, update the relevant ADR in `docs/adr/` in the
same change.

## Architecture (the parts that span multiple files)

### The `Env` seam and the simulator (`custos-env`, `custos-sim`)

`custos-sim::Simulator` owns one shared `SimState` (virtual clock, seeded
ChaCha RNG, per-node network inboxes with delay/drop/partition, a fake disk
distinguishing synced vs. un-synced bytes, and a cooperative run-queue). It
hands out a `SimEnv` per node via `sim.env(node_id)`. The run loop polls ready
tasks to quiescence, then advances virtual time to the earliest event (timer or
message delivery) and dispatches it — a custom single-threaded async executor.

Driving runs:
- `sim.run()` — to quiescence. **Do not use for protocols with perpetual timers
  (Raft heartbeats) — they never quiesce.**
- `sim.run_for(dur)` / `sim.run_until(deadline)` — bounded virtual time. Use
  these for anything involving the control plane.

Fault injection: `partition`/`partition_pair`/`heal`, `crash`/`restart`
(a crashed node drops un-synced disk + inbox and is muted: its sends are
dropped; `restart` re-arms the node's tasks so one parked on `recv()` resumes
receiving). `sim.trace_lines()` gives the stable history used for
byte-identical-trace assertions.

### Two planes (ADR 0001)

The **control plane** (`custos-control`) is consistent (Raft) and owns metadata
(`Metadata` = membership + tablet map). The **data plane** (`custos-data`) is
leaderless/AP and serves reads/writes. The decoupling is deliberate: the data
plane coordinator routes from a **cached `TabletView`**, so a control-plane
outage does not stop reads/writes — only topology changes (which bump a tablet's
epoch) need the control plane. The integration test
`custos-data/tests/two_plane.rs` exercises exactly this.

### Control-plane Raft (`custos-control`)

This is an **in-house Raft, not openraft** (ADR 0009) — openraft can't be driven
deterministically by `SimEnv`. The split:
- `raft::RaftCore` is a **synchronous, I/O-free** state machine. Time and
  randomness arrive as parameters (`now: Nanos`, `entropy: u64`); it returns
  outbound messages (`Vec<Out>`) and applies committed entries. This is what
  makes it deterministic and unit-testable.
- `node::RaftNode<E>` is the thin `Env` driver: it races `env.recv()` against a
  timer (`futures::select`), feeds the core, and ships the core's outbound
  messages.

Metadata mutations are `MetaCommand`s applied in log order; tablet placement is
changed via epoch-keyed **compare-and-swap** (`CasTabletReplicas`), evaluated
identically on every replica. The core emits WAL records the driver `fsync`s and
recovers from. The log is offset by a state-machine snapshot: on a threshold the
node snapshots and **truncates** the covered prefix, rewriting the WAL to its
live image (`persist.rs`, `node.rs`); a follower behind the compacted prefix is
caught up via a **chunked** `InstallSnapshot` (offset-addressed chunks of
`SNAPSHOT_CHUNK_BYTES`, reassembled and installed atomically by the follower);
recovery restores the snapshot and re-applies the tail. Restart-and-rejoin is
tested end-to-end via `Simulator::stop` (`custos-control/tests/restart.rs`).

### Data plane (`custos-data`)

`serve_replica` runs a per-node replica over a `StorageEngine`, enforcing
**epoch fencing**: an operation whose epoch is older than the replica's known
epoch is rejected. `DataClient` is the quorum coordinator: it broadcasts to a
`TabletView`'s replicas and returns as soon as a W (write) or R (read) quorum
responds. Choose `R + W > N` so reads see acknowledged writes.

`R + W > N` only makes quorum *reads* intersect; raw replica state still
diverges when a replica misses a write. **Repair/anti-entropy** (ADR 0010)
closes that: replica writes apply via `StorageEngine::merge` (per-key LWW) and
deletes via `merge_tombstone`, a divergent quorum read pushes the winner back
(read-repair), and `serve_anti_entropy` periodically reconciles with peers via a
**segment-digest exchange** (`SyncDigest`/`SyncPull`) that moves only divergent
ranges — not the whole digest each round — so even unread keys converge cheaply
(tombstones included, so deletes ride along). Repair is **residency-bounded**
(ADR 0005): `serve_replica_with_residency` drops repair traffic from peers
outside a tablet's placement, so it cannot leak data across a residency boundary
even to a reachable node. The data plane carries quorum `Write`/`Delete` and a
tombstone-aware `Sync`, so deletes propagate the same way writes do. The
`repair.rs` test partitions a replica during a write/delete and asserts
convergence both via a read and with no reads at all.

### Placement & residency (`custos-placement`)

A **pure, deterministic** policy engine (ADR 0005): given `Candidate`s (a node
id + its topology labels) and a `PlacementPolicy` (replication factor +
residency `required_labels` + optional failure-domain `SpreadPolicy`), it
chooses a tablet's replica set — `select_replicas` for a fresh tablet,
`replan` for a membership change (keeping eligible survivors so only the lost
replica moves). It depends only on `NodeId` (no dep on `custos-control`, which
would be a cycle); the control plane builds candidates from `Active` membership,
calls it, and commits the result as a `CasTabletReplicas`. Policies are
**replicated in `Metadata`** (`SetTabletPolicy`) and the **leader reconciles
automatically**: `RaftNode`'s `reconcile_loop` ticks on an `Env` timer and
proposes corrective `CasTabletReplicas` from the pure `Metadata::reconcile`.
End-to-end through real Raft under fault injection in
`custos-control/tests/placement_reconcile.rs` (caller-driven) and
`placement_auto_reconcile.rs` (automatic). Deferred: residency on the
repair/handoff/backup paths, a cluster-default policy, and operator-facing
policy management.

### Transaction consensus (`custos-consensus`)

A **first minimal slice** of Accord-style leaderless transactions (ADR 0011),
built in the same shape as the control-plane Raft: a synchronous, I/O-free
`AccordCore` (logical-clock timestamps, replica + coordinator state, returns
outbound messages) wrapped by a thin `AccordNode<E>` driver over `Env`. A
coordinator mints a unique timestamp `t0`, broadcasts `PreAccept`, and either
commits at `t0` in one round trip (fast path, when a fast quorum agrees on `t0`
and deps) or runs an `Accept` round to pick a higher execution timestamp and
union deps (slow path) before `Commit`. Conflicts are intersecting key sets;
the slice proves two conflicting transactions commit in a *consistent timestamp
order on every replica*. Each replica then **executes** committed transactions
in agreed `(execute_at, txn)` order (blocking on earlier-ordered conflicts)
against a real `StorageEngine` (the in-memory `MemoryEngine` under simulation):
the sync `AccordCore` decides the order and emits `ApplyEffect`s, the
`AccordNode` driver `merge`s each transaction's writes into the engine at its
execution timestamp. It is also **durable**: `AccordCore` emits `WalRecord`s the
driver fsyncs to `accord.wal` before acting and recovers on restart (replaying
its execution order into a fresh engine) — mirroring `RaftCore`'s WAL. A **dead
coordinator's transaction is recoverable**: another replica runs a
`Recover`/`RecoverOk` round and drives the transaction to a commit consistent
with whatever the original could have committed (adopt-committed, else force the
slow path). **Deferred:** the full dependency wait-graph, the precise recovery
ballot + duelling recoverers + a failure detector, WAL snapshotting, live
data-plane integration, and sharding — see ADR 0011 and the crate guide.

### Storage (`custos-storage`)

`StorageEngine` trait (put/get, `get_at` historical read, range scan, atomic
batch, range delete, MVCC `Snapshot`). Backed by `MemoryEngine` (a `BTreeMap`
MVCC store; the engine used under simulation) and a **custom on-disk
`LsmEngine<E: Env>`** — a real
log-structured merge tree (WAL → memtable → flushed, CRC-checksummed SSTables with
a block index + footer → size-tiered compaction → atomically-swapped MANIFEST,
recovered on open) that does **all** I/O through the `Env` `Disk` seam, so its
crash recovery is **deterministically simulation-tested** under `SimEnv` (ADR
0008). The trait is **async** (`#[async_trait]`): the I/O-ish methods are `async
fn` so the on-disk LSM can reach the async `Disk` seam behind the same trait,
while `snapshot()` / `latest_version()` stay synchronous. **Version contract:**
writers assign strictly increasing versions (enforced via `NonMonotonicVersion`);
given that, a snapshot taken at version `v` is isolated from later writes.

### A node's inbox is single-consumer

`Network::recv` for a node id has exactly one consumer. Do not run two protocols
(e.g. a control `RaftNode` and a data replica) on the same node id — give them
distinct ids (see how the tests assign control nodes `0..3` and data replicas
`3..6`).

## Conventions

- One milestone / logical change per PR; keep diffs reviewable.
- Every distributed behavior lands with a fault-injecting simulation test that
  is reproducible from a seed.
- Higher layers define their own message enums and (de)serialize with
  `serde_json` over the `Vec<u8>` payloads the `Network` moves.
