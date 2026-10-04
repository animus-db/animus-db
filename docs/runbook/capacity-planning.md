# Capacity planning

**Numbers are PENDING.** There are no published throughput, latency, memory, CPU or
density figures: B-01 (published benchmarks) and C-17 (scale and density testing) are open
roadmap items, and no benchmark-results document has merged to `main` (checked with
`git log origin/main` 2026-10-04). `website/performance.html` says the same. This page gives
the structure of a sizing exercise and what the code lets us say today. Criterion E-8
("capacity planning with published numbers") therefore stays open by dependency.
Conventions are in [README.md](README.md); disk is in [disk-full.md](disk-full.md).

## The model (from the code)

| Quantity | Rule | Source |
|---|---|---|
| Replication factor | `RF = min(nodes, 3)`; fixed, not configurable by the operator | ADR 0005; operator PDB module |
| Control voters | `controlNodes` (default `min(3, nodes)`); odd, at least 3 for fault tolerance; grow-only on Kubernetes | operator |
| Tablet | one Raft group on RF nodes; a table starts small and grows only by **splitting** (tablets never merge) | ADR 0044, 0058 |
| Splitting triggers | opt-in size (`--auto-split-bytes` / `cluster_settings.auto_split_bytes` / `spec.autoSplitBytes`), write rate (`--auto-split-ops-rate`), stream change rate; provisioned tables are pre-split to `ceil(RCU/3000 + WCU/1000)` | ADR 0034, 0042, 0067 |
| Idle cost | an idle group quiesces after 5 s (`--quiesce-after`, 0 disables): no timers, heartbeats or apply polling | ADR 0044, 0048 |
| Per-node hosted groups | unbounded by config; measured only for idle engines (about 8.1 KB per idle engine, `animus-storage/tests/idle_engine_cost.rs`) and the WAL fsync sweep at 1/8/32/128 groups (`docs/design/shared-wal-fsync-benchmark.md`). "Hundreds to thousands of groups per node" is **unmeasured** | roadmap C-17 |
| Write path durability | one fsync per group-commit round; a shared per-node WAL coalesces fsyncs across groups (default on) | ADR 0028 |
| Ports per node | six, base to base+5: internal, client, dynamo, admin, intra, console | operator `PORT_*` |
| Request limits | AWS-faithful, compiled in, no override: for example 1 MiB HTTP body cap on the edge | ADR 0072 |
| Failure timings | down after 500 ms, repair after 5 s more | [README.md](README.md) |

## Sizing worksheet (fill in with measurements, not guesses)

1. **Data**: logical bytes per table, growth per day, item size distribution, GSI/LSI
   count (each index is another full set of rows), whether Streams or PITR are on (change
   log plus sealed segments, [disk-full.md](disk-full.md)).
2. **Disk per node**: `(logical x index factor x RF / nodes)` steady state, times at least
   2 for compaction, rebalancing and snapshot headroom (conservative estimate, not a
   measurement). Keep backups and segment stores off the data volume (`s3://`, `fs:`).
3. **Throughput**: target RCU/WCU per table and the hottest partition key. Each tablet is
   served by one leader, so a single hot key range is bounded by one node. Provisioned
   throughput splits across the table's tablets (ADR 0065). Eventually-consistent reads
   can use any replica; consistent reads and all writes use the leader.
4. **Nodes**: at least 3 (RF 3, 3 control voters). Add nodes for disk or leaders-per-node
   balance; growth is online (join, rebalance one move at a time). More than 3 nodes does
   not raise RF, it spreads tablets. Failure tolerance: one node loss per RF-3 tablet
   and per 3-voter control group; plan N+1 capacity so that the survivors can absorb a
   failed node's replicas.
5. **Memory/CPU**: no data. Per-group Raft state and the single driver task per group are
   unmeasured (C-17); LSM defaults are a 64 KiB memtable and 2 MiB tables. Measure on the
   target hardware with the benches below before committing to a node shape.
6. **Network**: replication traffic is RF-1 copies of every write; a rebuild or rebalance
   streams whole tablet images. No numbers.
7. **Headroom and SLOs**: choose latency/availability objectives only after B-01
   produces per-operation percentiles.

## Measuring today

```sh
cargo bench -p animusd                                   # in-process 3-node ProdEnv cluster, latency classes, closed-loop sweep, leader-kill phase (unauthenticated, one host; developer tool)
ANIMUS_BENCH_NODES=3 ANIMUS_BENCH_ITEMS=10000 ANIMUS_BENCH_OPS=2000 ANIMUS_BENCH_VALUE_BYTES=1024 ANIMUS_BENCH_CLIENTS=1,8,32 ANIMUS_BENCH_JSON=out.json cargo bench -p animusd
cargo bench -p animus-storage                            # engine write/IO smoke
ANIMUS_BENCH_GROUPS=1,8,32,128 cargo bench -p animus-cp-data --bench wal_fsync_bench
```

These are developer tools (no coordinated-omission correction, no warm-up split,
single host); they size relative effects, not capacity. Their results on your hardware
are the first real data for this page.

## Maturity

Structure and rules are from code and ADRs. No measured capacity numbers exist anywhere in the
repository for production sizing.
