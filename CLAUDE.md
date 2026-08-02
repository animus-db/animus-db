# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

AnimusDB is a masterless, linearly-scalable NoSQL database in Rust (Dynamo
lineage). It pairs a **leaderless AP data plane** (tunable quorum consistency)
with a small **strongly-consistent Raft control plane** that owns cluster
metadata. Correctness is established by **deterministic simulation testing**.

Status: pre-alpha. Implemented: the scaffold, the `Env` seam, storage (in-memory,
plus a **custom on-disk `LsmEngine`** — a real WAL + SSTable +
compaction LSM doing all I/O through the `Env` disk seam, so it is deterministically
crash-tested under `SimEnv`), the control-plane Raft (WAL durability + recovery +
log-truncating snapshots with `InstallSnapshot`, plus **heartbeat-based failure
detection** that auto-marks members `Down`/`Active` (with a post-election grace
period so a freshly elected leader's cold detector doesn't falsely mark live
members `Down`) and cascades into placement re-reconciliation — ADR 0012, and a
**replicated table-schema catalog** in `Metadata` — `CreateTableSchema`/
`DropTableSchema`, durable + cluster-wide, ready for the wire adapters to consume
— ADR 0013),
the quorum data-plane vertical slice (with read-repair, background
anti-entropy convergence via segment-digest exchange of only divergent ranges,
delete/tombstone propagation, residency-bounded repair, and **hinted handoff** —
a write missing a down replica buffers a residency-bounded hint replayed when the
replica returns), tablet split/merge
+ multi-tablet routing, the Elle-style recorder/checker (`animus-test`) — now
including an **end-to-end correctness test of the assembled stack** (control
plane + data plane at scale: 3 Raft nodes, 2 tablets, 6 replicas, 4 concurrent
clients) under fault injection (partition + leader kill + crash + heal), checked
for serializability/durability/convergence — a
DynamoDB-style item API over the core plus a **DynamoDB JSON wire protocol**
(`animus-dynamo`: CreateTable/PutItem/GetItem/DeleteItem/Query/Scan/**UpdateItem**/
**BatchWriteItem**/**TransactWriteItems** AttributeValue-JSON translation over the
full type set — scalars plus the document types `M`/`L` and set types
`SS`/`NS`/`BS` — with **table key schemas now consumed from the control plane's
replicated catalog** (ADR 0013: `CreateTable` proposes a `CreateTableSchema` and
waits for commit, so a created table is durable + cluster-agreed and survives a
restart; GSI/written-key index stay in-memory), sort-key conditions
=/BETWEEN/begins_with, a `ConditionExpression` subset for conditional writes,
`ReturnValues` (`ALL_OLD`/`ALL_NEW`) on writes, **document-path projection
expressions** (`a.b`, `ProjectionExpression`/`AttributesToGet`) on reads, `Scan`
with `Limit`/`ExclusiveStartKey` pagination + `FilterExpression`, and **secondary
indexes** — any number of hash-only/composite **global** and **local** secondary
indexes queryable by `IndexName`, each with a declared projection
(`ALL`/`KEYS_ONLY`/`INCLUDE`)), a
**runnable node + CLI** assembling the planes over `ProdEnv` and
serving clients over TCP, with a now **durable data plane** — each node's data
replica is backed by the on-disk `LsmEngine` over `ProdEnv` by default, so a
value acked to a client survives a process restart (the control plane already
persisted its Raft WAL; the data plane is no longer in-memory-only) — and now
**self-healing**: the assembled node registers its data nodes as the cluster
members, runs the control-plane heartbeat/failure-detector + placement reconciler
+ data-plane anti-entropy over `ProdEnv` timers, so a killed data node is detected
`Down` and its tablet automatically re-placed onto a spare while reads keep
succeeding via the survivors (proven live in `animusd/tests/self_heal.rs`) —
runnable
as one process (`animusd --cluster N`) or one process per node (`animusd --config
FILE --node I`, config via `gen-config`; `--ephemeral` selects the volatile
in-memory engine for dev runs),
which now also **serves the DynamoDB JSON protocol over HTTP**, routing those
requests through the same data-plane coordinator — plus a **CQL v4 wire
protocol** (`animus-cql`: STARTUP/READY handshake; a scalar **type system**
(text/int/bigint/boolean/blob/uuid) with typed column metadata + bound values;
`CREATE KEYSPACE`/`USE`/`CREATE TABLE` (incl. **compound primary keys** — a
partition key + clustering columns) now **proposing the schema into the control
plane's Raft-replicated catalog** (ADR 0013) and waiting for commit, so a created
table is durable + cluster-agreed — plus `DROP TABLE`, `ALTER TABLE ... ADD`, and
`BATCH`; `INSERT`/`SELECT`/`UPDATE`/`DELETE` resolved against the **replicated**
schema — a partition (all rows sharing a partition key, ordered by clustering key)
is one data-plane value so reads/writes stay point ops, a `SELECT pk = ?` returns
rows clustering-ordered, and a `DELETE` emptying a partition tombstones the key;
the requested **consistency level** is honored (mapped to the data-plane R/W
quorum); plus **prepared statements** (PREPARE→Prepared, EXECUTE with bound
values) — all routed through that same coordinator) — the **topology-aware placement
engine** (`animus-placement`: residency + failure-domain spread, with the leader
automatically reconciling tablet placement via control-plane `CasTabletReplicas`),
and a **slice of Accord-style leaderless transaction consensus**
(`animus-consensus`: PreAccept→Commit fast path + PreAccept→Accept→Commit slow
path, dependency tracking, consistent commit order, **durable storage-backed
execution** — each replica executes committed transactions in agreed order
against a real `StorageEngine` (`MemoryEngine` under sim) via a WAL it recovers
from on restart — and a **first slice of coordinator failover** (a replica can
recover a stranded transaction whose coordinator died, adopting a committed
decision or forcing the slow path), **message retry/timeouts** (the driver
re-sends un-acknowledged round messages on a timer so a dropped fire-and-forget
`send` no longer strands a transaction), and a **data-plane frontier** — a
committed transaction's writes can land in the replicated AP data plane
(`animus-data` quorum), readable via ordinary quorum reads, atomically in agreed
order; transactions can also be **sharded** across tablets — a single
transaction's key set may span more than one tablet, each key's effect routed to
its own tablet's quorum while one global execution timestamp orders all shards —
and an **interactive read-modify-write** folds its read set into the committed
transaction's conflict tracking, with **adaptive (exponential) retry backoff**;
ADR 0011). Both wire adapters now **consume the control plane's replicated
table-schema catalog** (ADR 0013): `CREATE TABLE`/`DROP TABLE` propose a
`CreateTableSchema`/`DropTableSchema` and wait for commit, and reads/writes
resolve schema from the replicated `Metadata` — so a created table is durable +
cluster-agreed (proven surviving a node restart over real TCP in
`animusd/tests/cql_durable_schema.rs`), replacing the per-process in-memory
catalogs. Skeletons / future work:
the rest of the CQL surface (composite multi-column partition keys,
per-column `DELETE`, atomic logged `BATCH`, in-place `ALTER`,
range/`IN`/`ORDER BY`/`LIMIT`, collection/UDT types, paging, auth, `LWT`,
replicated **keyspace** metadata) and the rest of the
DynamoDB surface (atomic `TransactWriteItems` via Accord, `BatchGetItem`,
list-index document paths, `ADD`/`DELETE` update arithmetic, durable/replicated
secondary-index + written-key state, and a native quorum range scan) — note
**both adapters now consume the control plane's replicated table-schema catalog**
(ADR 0013), so `CreateTable`/`CREATE TABLE` is durable + cluster-agreed (their
in-memory per-process catalogs are gone) — and the deferred remainder of Accord
(the full dependency wait-graph, the precise recovery ballot + duelling
recoverers + a failure detector, WAL snapshotting, arbitrary caller-supplied
write values, and per-shard consensus replica sets / placement of the consensus
participants themselves).

## Per-crate guides

Each crate has its own `CLAUDE.md` with local entry points and gotchas — read
the relevant one before working in a crate:

| Crate | Guide |
|-------|-------|
| `animus-env` | [crates/animus-env/CLAUDE.md](crates/animus-env/CLAUDE.md) |
| `animus-sim` | [crates/animus-sim/CLAUDE.md](crates/animus-sim/CLAUDE.md) |
| `animus-storage` | [crates/animus-storage/CLAUDE.md](crates/animus-storage/CLAUDE.md) |
| `animus-tablet` | [crates/animus-tablet/CLAUDE.md](crates/animus-tablet/CLAUDE.md) |
| `animus-control` | [crates/animus-control/CLAUDE.md](crates/animus-control/CLAUDE.md) |
| `animus-data` | [crates/animus-data/CLAUDE.md](crates/animus-data/CLAUDE.md) |
| `animus-test` | [crates/animus-test/CLAUDE.md](crates/animus-test/CLAUDE.md) |
| `animus-dynamo` | [crates/animus-dynamo/CLAUDE.md](crates/animus-dynamo/CLAUDE.md) |
| `animus-placement` | [crates/animus-placement/CLAUDE.md](crates/animus-placement/CLAUDE.md) |
| `animus-consensus` | [crates/animus-consensus/CLAUDE.md](crates/animus-consensus/CLAUDE.md) |
| `animus-cql` | [crates/animus-cql/CLAUDE.md](crates/animus-cql/CLAUDE.md) |
| `animusd` | [crates/animusd/CLAUDE.md](crates/animusd/CLAUDE.md) |
| `animus-cli` | [crates/animus-cli/CLAUDE.md](crates/animus-cli/CLAUDE.md) |

## Commands

```sh
cargo build --workspace --all-targets
cargo test --workspace
cargo test -p animus-control                       # one crate
cargo test -p animus-control --test control_raft   # one test binary
cargo test -p animus-control survives_leader_kill  # one test by name substring
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all --check
cargo deny check                                   # licenses + advisories (cargo install cargo-deny)
```

All five (fmt, clippy `-D warnings`, build, test, deny) must be green; CI runs
them. Commits require a DCO sign-off (`git commit -s`); this repo is also set up
for GPG-signed commits.

### Replaying a failed simulation

Every simulation run is a pure function of its seed. Tests print the seed in
assertion messages; replay with `ANIMUS_SEED=<seed> cargo test <name>`. The
`Simulator` is driven by `Simulator::new(seed)`.

## The load-bearing constraint: determinism

This is the single most important rule (ADR 0003). **All nondeterminism flows
through the `Env` seam.** In every crate except `animus-env`'s `ProdEnv` and
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
live); tests use `animus-sim`'s `SimEnv`.

When a design decision changes, update the relevant ADR in `docs/adr/` in the
same change.

## Architecture (the parts that span multiple files)

### The `Env` seam and the simulator (`animus-env`, `animus-sim`)

`animus-sim::Simulator` owns one shared `SimState` (virtual clock, seeded
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

The **control plane** (`animus-control`) is consistent (Raft) and owns metadata
(`Metadata` = membership + tablet map). The **data plane** (`animus-data`) is
leaderless/AP and serves reads/writes. The decoupling is deliberate: the data
plane coordinator routes from a **cached `TabletView`**, so a control-plane
outage does not stop reads/writes — only topology changes (which bump a tablet's
epoch) need the control plane. The integration test
`animus-data/tests/two_plane.rs` exercises exactly this.

### Control-plane Raft (`animus-control`)

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
tested end-to-end via `Simulator::stop` (`animus-control/tests/restart.rs`).

The control plane also runs **heartbeat-based failure detection** (ADR 0012):
members heartbeat the control group on an `Env` timer (`RaftMsg::Heartbeat`,
intercepted by the driver and fed to a pure `FailureDetector` — never the
consensus core), and the leader's `detect_loop` proposes `UpsertMember{Down}`
when a member falls silent past a timeout and `{Active}` when it recovers
(idempotent, no flapping). Because the placement reconciler already reacts to
`Down`, a detected failure **cascades** into automatic tablet re-placement —
proven end-to-end (crash → `Down` → reconcile → restart → `Active`) in
`animus-control/tests/failure_detection.rs`. A freshly elected leader applies a
**post-election grace period** before it will mark anyone `Down`, since its
failure detector is per-node volatile and starts cold.

The control plane also owns a **replicated table-schema catalog** (ADR 0013):
`Metadata.schemas` maps each table to a `TableSchema` (partition key + ordered
clustering keys + typed columns — a shape that fits both the DynamoDB and CQL
adapters), mutated only by `MetaCommand::{CreateTableSchema, DropTableSchema}`
applied deterministically (create rejects duplicates + malformed schemas; drop is
idempotent). Living in `Metadata`, it is Raft-replicated and recovered from the
WAL/snapshot like all metadata, so a `CreateTable`/`CREATE TABLE` survives restart
and is agreed cluster-wide — replacing the adapters' per-process in-memory
catalogs. **Both wire edges consume it**: `animusd::{cql,dynamo}` propose
`CreateTableSchema`/`DropTableSchema` on DDL (routed to the leader via a
**per-cluster** set of registered control handles, held in `animusd`'s
`ClusterEdgeState`) and resolve reads/writes from the replicated `Metadata`. Proven in `animus-control/tests/schema_catalog.rs`
(propose → reject duplicate/malformed → leader kill → survives + survivors agree →
drop replicates) and end-to-end over the wire in
`animusd/tests/cql_durable_schema.rs` (CREATE TABLE + a row survive a node
restart).

### Data plane (`animus-data`)

`serve_replica` runs a per-node replica over a `StorageEngine`, enforcing
**epoch fencing**: an operation whose epoch is older than the replica's known
epoch is rejected. `DataClient` is the quorum coordinator: it broadcasts to a
`TabletView`'s replicas and returns as soon as a W (write) or R (read) quorum
responds. Choose `R + W > N` so reads see acknowledged writes. An ack means the
write durably applied: a replica only acks `ok` when its storage
`merge`/`merge_tombstone` succeeded, so a write that fewer than W replicas could
persist fails rather than being falsely reported committed (it matters now the
replica can be the durable on-disk LSM).

`R + W > N` only makes quorum *reads* intersect; raw replica state still
diverges when a replica misses a write. **Repair/anti-entropy** (ADR 0010)
closes that: replica writes apply via `StorageEngine::merge` (per-key LWW) and
deletes via `merge_tombstone`, a divergent quorum read pushes the winner back
(read-repair), and `serve_anti_entropy` periodically reconciles with peers via a
**segment-digest exchange** (`SyncDigest`/`SyncPull`) that moves only divergent
ranges — not the whole digest each round — so even unread keys converge cheaply
(tombstones included, so deletes ride along). The anti-entropy loop reads the
tablet's **live** epoch from the replica's `ReplicaHandle` each round, so after a
placement reconcile bumps the epoch a re-placed spare still converges in the
background (its digests carry the bumped epoch and are not fenced) rather than
waiting for read-repair on the first read. Repair is **residency-bounded**
(ADR 0005): `serve_replica_with_residency` drops repair traffic from peers
outside a tablet's placement, so it cannot leak data across a residency boundary
even to a reachable node. The data plane carries quorum `Write`/`Delete` and a
tombstone-aware `Sync`, so deletes propagate the same way writes do. The
`repair.rs` test partitions a replica during a write/delete and asserts
convergence both via a read and with no reads at all. A third convergence path,
**hinted handoff** (ADR 0010), acts on the *write* side: a hinting `DataClient`
(`with_hints`) buffers a hint for any tablet replica that missed a committed
write/delete (down/partitioned) into a `HintStore`, and a replay loop
(`serve_hint_handoff`, probe-based; or `serve_hint_replay`, send-only for a
shared inbox) delivers it via the ordinary `Sync` path the moment the replica
returns — promptly converging a briefly-unavailable replica without a read or an
anti-entropy round. It is residency-bounded (`AllowedTargets`, from
`PlacementPolicy::admits`): a placement-ineligible node is never hinted
(`hinted_handoff.rs`).

### Placement & residency (`animus-placement`)

A **pure, deterministic** policy engine (ADR 0005): given `Candidate`s (a node
id + its topology labels) and a `PlacementPolicy` (replication factor +
residency `required_labels` + optional failure-domain `SpreadPolicy`), it
chooses a tablet's replica set — `select_replicas` for a fresh tablet,
`replan` for a membership change (keeping eligible survivors so only the lost
replica moves). It depends only on `NodeId` (no dep on `animus-control`, which
would be a cycle); the control plane builds candidates from `Active` membership,
calls it, and commits the result as a `CasTabletReplicas`. Policies are
**replicated in `Metadata`** (`SetTabletPolicy`) and the **leader reconciles
automatically**: `RaftNode`'s `reconcile_loop` ticks on an `Env` timer and
proposes corrective `CasTabletReplicas` from the pure `Metadata::reconcile`.
End-to-end through real Raft under fault injection in
`animus-control/tests/placement_reconcile.rs` (caller-driven) and
`placement_auto_reconcile.rs` (automatic). Deferred: residency on the
repair/handoff/backup paths, a cluster-default policy, and operator-facing
policy management.

### Transaction consensus (`animus-consensus`)

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
slow path). It also serves **read-only transactions** (`submit_read`): a read is
ordered exactly like a write (timestamp + conflict deps) and, at its execution
timestamp, snapshot-reads each key (`get_at`) — observing the writes ordered
before it and none after, consistently on every replica — but writes nothing.
Dropped messages are **retried**: the driver re-sends the un-acknowledged
messages of any in-flight round on an `Env` timer (the sync core decides *what*
via `resend_pending`; replicas `CommitAck` so a committed coordinator stops
re-sending), so a lossy network no longer strands a transaction. A committed
write transaction can also be wired to the **replicated data plane**
(`AccordNode::start_with_data_plane`): on Apply its keys are written through the
`animus-data` quorum coordinator at the execution timestamp, so the transaction's
atomic, ordered effect becomes readable via ordinary data-plane quorum reads (no
dependency cycle — `animus-data` does not depend on consensus). A wired **read
transaction reads from that same data plane** (a quorum read at its execution
time, sound because the read's effect is emitted only after every earlier-ordered
write has applied to the quorum). An **interactive** `begin → read → decide →
write → commit` handle (`AccordNode::begin` → `InteractiveTxn`) runs a multi-step
read-modify-write under one Accord transaction — the handle is pure driver state,
committing through `submit_rw`, so the **session's reads fold into the committed
transaction's conflict set** (a concurrent write to a key it read is ordered
correctly) and conflicting interactive transactions are ordered consistently and
land atomically. Transactions may be **sharded**: `start_with_router` routes each
key's data-plane effect to **its own** tablet's quorum, so one transaction's keys
can span multiple tablets while the Accord round agrees one global execution
timestamp ordering every shard. The retry tick uses **adaptive (exponential)
backoff** — it backs off while a round is stuck (fewer redundant sends) and
resets on progress.
The driver's real-thread liveness (no mutex guard held across `.await`) is
guarded by a multi-threaded `ProdEnv` regression test, since `SimEnv` proves
order but not thread liveness. **Deferred:** the full dependency wait-graph, the
precise recovery ballot + duelling recoverers + a failure detector, WAL
snapshotting, arbitrary caller-supplied write values, and per-shard consensus
replica sets / placement of the consensus participants — see ADR 0011 and the
crate guide.

### Storage (`animus-storage`)

`StorageEngine` trait (put/get, `get_at` historical read, range scan, atomic
batch, range delete, MVCC `Snapshot`). Backed by `MemoryEngine` (a `BTreeMap`
MVCC store; the engine used under simulation) and a **custom on-disk
`LsmEngine<E: Env>`** — a real
log-structured merge tree (a **segment-rotating** WAL — numbered `wal-NNNNNN`
segments group-committed and GC'd whole on flush → memtable → flushed,
CRC-checksummed,
**LZ4-compressed** SSTables with a block index + footer + per-table **Bloom
filter** → **leveled compaction** (overlapping L0 flush tier, non-overlapping
L1+ runs, with **tombstone GC** reclaiming deletes aged below a grace floor) →
atomically-swapped, **compact binary** MANIFEST (also recording the
live WAL segments), recovered on open — which also reclaims orphan WAL segments a
crash left below the live set)
that does **all** I/O through the `Env` `Disk` seam,
so its crash recovery is **deterministically simulation-tested** under `SimEnv`
(ADR 0008). A point read skips a table whose key range or Bloom filter proves the
key absent. The trait is **async** (`#[async_trait]`): the I/O-ish methods are
`async fn` so the on-disk LSM can reach the async `Disk` seam behind the same
trait, while `snapshot()` / `latest_version()` stay synchronous. **Version
contract:** writers assign strictly increasing versions (enforced via
`NonMonotonicVersion`); given that, a snapshot taken at version `v` is isolated
from later writes. A `cargo bench -p animus-storage` harness measures engine
throughput/latency over `ProdEnv`.

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
