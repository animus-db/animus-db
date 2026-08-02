# Getting started

AnimusDB is a masterless, linearly-scalable NoSQL database (Dynamo lineage): a
leaderless AP data plane with tunable quorum consistency, paired with a small
strongly-consistent Raft control plane. This page gets you from a checkout to a
running node and shows where runtime metrics surface (ADR 0015).

> Status: **pre-alpha.** For *what* is implemented and *why*, read the ADR index
> ([`docs/adr/README.md`](adr/README.md)) and the per-crate `CLAUDE.md` guides.

## Build

```sh
cargo build --workspace --all-targets
```

The full gate set (all five must be green; CI runs them):

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build --workspace --all-targets
cargo test --workspace
cargo deny check          # licenses + advisories (cargo install cargo-deny)
```

## Run a cluster

The simplest way is the single-process dev cluster — one OS process hosting `N`
nodes, each on its own ephemeral ports:

```sh
cargo run -p animusd -- --cluster 3
```

Or one process per node, each pointed at a shared cluster config file:

```sh
cargo run -p animusd -- --config cluster.toml --node 0
cargo run -p animusd -- --config cluster.toml --node 1
cargo run -p animusd -- --config cluster.toml --node 2
```

Each node assembles three internal roles over `ProdEnv` (the only place real
time/IO/RNG live): a control-plane Raft node, a data-plane replica, and a
client-facing coordinator that serves the DynamoDB-JSON and CQL wire adapters.

## Where metrics surface

Observability is a **deterministic-safe seam** in `animus-env` (ADR 0015):
monotonic counters keyed by a closed `Metric` enum plus a leadership gauge,
recorded behind a cheap-to-clone `MetricsHandle`. Recording never touches the
wall clock, does no I/O, and uses no `HashMap` — so a simulation run stays a pure
function of its seed.

Today the **control-plane Raft driver** is instrumented. The exported counters
(all `control_`-prefixed) are:

| Metric | Meaning |
|--------|---------|
| `control_elections_started` | this node became a candidate at a higher term |
| `control_elections_won` | this node entered the leader role |
| `control_append_entries_sent` | an `AppendEntries` (replication/heartbeat) was sent |
| `control_append_entries_rejected` | a follower rejected an `AppendEntries` |
| `control_snapshot_installs` | a chunked `InstallSnapshot` completed on this node |
| `control_failure_detector_down` | the failure detector drove a member `Active`→`Down` (ADR 0012) |
| `control_failure_detector_up` | the failure detector drove a member `Down`→`Active` |
| `control_is_leader` | gauge: 1 if this node currently believes it is leader, else 0 |

A `ProdEnv` owns a real recording sink and renders a point-in-time text export:

```rust
// One line per counter plus the gauge, in stable order, no timestamp.
let text: String = prod_env.metrics_text();
```

For a control node specifically, the same export is reachable from its
`RaftNode` handle:

```rust
let text: String = raft_node.metrics().snapshot().to_text();
```

### The live endpoint

The seam deliberately does **no HTTP** — keeping it pure — so the live endpoint
is wired in `animusd`. A running node serves `GET /metrics` on its **HTTP
endpoint** (the same listener as the DynamoDB JSON wire — `Node::dynamo_addr()`),
returning the text export as `text/plain`:

```sh
curl -s <dynamo addr>/metrics
# control_elections_started 1
# control_elections_won 1
# control_append_entries_sent 42
# ...
# control_is_leader 1
```

The body is **aggregated across the node's three role sinks** (control, data,
coord), read at request time so it reflects live activity. A node runs three
internal `ProdEnv` roles on distinct ids, each recording into its own sink:
`RaftNode::start` records into the control env's sink, the data replica and the
coordinator into theirs. The handler sums the three snapshots counter-by-counter
(and takes the max of the leadership gauge, which only the control plane sets), so
both control- and data-plane counters surface from one endpoint. Today only the
control-plane counters move; data-plane counters surface automatically once
recorded, with no endpoint change. The export is timeless `name value` text; a
Prometheus scrape adds its own timestamp.

## Replaying a failed simulation

Every simulation run is a pure function of its seed. Tests print the seed in
assertion messages; replay with:

```sh
ANIMUS_SEED=<seed> cargo test <name>
```
