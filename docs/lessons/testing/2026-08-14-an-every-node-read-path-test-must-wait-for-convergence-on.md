# An "every node" read-path test must wait for convergence on *every* node, not just the one that drove the write

**An "every node" read-path test must wait for convergence on *every*
node, not just the one that drove the write** — a per-node `Metadata`
replica can lag its own control Raft's commit by a few milliseconds, and
a handler that reads `ClientCtx::effective_metadata()` (DynamoDB Streams'
`GetRecords`/`GetShardIterator`, ADR 0042 §3/§7 — resolved *fresh, per
call, per node*, by design, so an open-shard iterator can survive a seal)
resolves against *that node's own* snapshot. `dynamo_streams.rs`'s
`get_records_on_a_sealed_shard_works_from_every_node` originally polled
only `nodes[0]` for the seal to land, then queried all three nodes in a
tight loop — flaky roughly 1 run in 3 under `--test-threads=1`, always as
"node 1/2: shard must exhaust" failing (a genuinely sealed shard's
`GetRecords`, served by a node whose own catalog view hadn't caught up
yet, fell through to the open-shard branch instead, which never nulls).
Not a correctness bug in the handler — this is exactly the stream's own
documented eventually-consistent contract self-healing within
milliseconds — but a one-shot assertion right after the write is exactly
the "fixed-deadline one-shot assert on an eventual property" this
codebase's testing doctrine already warns against; the fix was polling
`nodes.iter().all(|n| ...)` before entering the per-node assertion loop,
not touching the handler. General rule: when a test's very *point* is "the
same operation must behave identically issued through every node," the
convergence wait that precedes the loop must also cover every node, or
the loop races the propagation it's supposed to be testing past, not the
behavior itself. (`crates/animusd/tests/dynamo_streams.rs`, ADR 0042/0043
round-3 PR6, 2026-08-14.)
