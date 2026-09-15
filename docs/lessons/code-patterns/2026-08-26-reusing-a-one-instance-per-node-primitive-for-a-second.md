# Reusing a "one instance per node" primitive for a second, independent instance needs the primitive to stop hardcoding its own singleton identity first — the same "exclusive resource per instance" class of bug as the `clone_engine` entry above, but at the network layer instead of disk (2026-08-26, ADR 0059 Train 1 PR②, `animus-cp-data:: cluster_segment_store`).

**Reusing a "one instance per node" primitive for a second, independent
instance needs the primitive to stop hardcoding its own singleton
identity first — the same "exclusive resource per instance" class of bug
as the `clone_engine` entry above, but at the network layer instead of
disk (2026-08-26, ADR 0059 Train 1 PR②, `animus-cp-data::
cluster_segment_store`).** `ClusterSegmentStore`'s own doc was explicit
and correct: "`(node, stream)` is single-consumer (ADR 0026), and this is
THE ONE task that consumes this node's `SEGMENT_STREAM` inbox" — but
`SEGMENT_STREAM` was a hardcoded `pub const`, not a constructor
parameter, because until this PR only one subsystem (DynamoDB Streams)
ever built one. Wiring `animusd::build_backup_store` — a second,
independent `ClusterSegmentStore` instance for on-demand backups,
constructed on every combined/data-only node by
`BackupStoreConfig::default() == Cluster` — reused the type without
reading that invariant as a constraint on the *type*, not just on "don't
call `start` twice from the same call site": both instances' serving
tasks called `env.recv_stream(SEGMENT_STREAM)` on the same node,
racing for one single-consumer inbox and silently stealing each other's
requests/replies. **This was NOT caught by `cargo build`, `cargo clippy`,
or a single test binary run in isolation** — every test still passed
individually, because a lone `ClusterSegmentStore` on its own inbox has
no contender. It surfaced only as intermittent, seed-independent
failures across *unrelated* `animusd` integration tests
(`dynamo_streams.rs`, `streams_e2e.rs` — 1 to 4 tests failing per run,
different tests and different symptoms — a timeout once, a spurious
"shard has been trimmed" 400 once) once every test node's bring-up
started constructing the second store by default, reproducing even with
`--test-threads=1` on a single binary in isolation (ruling out cross-test
contention as the cause — the two racing consumers live *inside* one
node's own bring-up). **The generalizable rule**: before reusing any
"exactly one of these per node/process" primitive for a second logical
consumer, grep its own doc for the word "exactly" or "the one" and verify
the singleton identity it's protecting (a stream id, a file prefix, a
port, a lock name) becomes a real per-instance parameter, not an
implicit constant — the type system cannot catch a second value of a
type whose identity is baked into a `pub const` rather than a field.
Fixed by threading a `stream: u64` field through
`ClusterSegmentStore::{new,with_k,start,start_with_k}` and `serve_loop`,
with the pre-existing `SEGMENT_STREAM` staying the streams call site's
explicit argument and a new, equally explicit
`animus_cp_data::backup::BACKUP_SEGMENT_STREAM` for the backup call
site. Regression:
`animus-cp-data/tests/cluster_segment_store.rs::
two_cluster_segment_stores_on_the_same_node_stay_isolated_by_stream`
(two instances, two streams, a same-object-id racing `put` from the same
node, asserting each store's own local copy holds its own payload).
