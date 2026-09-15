# A new orthogonal capability often *composes* existing single-instance pieces — don't reshape the proven core to add it.

**A new orthogonal capability often *composes* existing single-instance pieces —
don't reshape the proven core to add it.** Per-shard Accord consensus (one group
per tablet) landed as a thin driver layer (`ShardedOwner` hosting one untouched
`AccordNode` per local shard, routed by a `ShardRouter` *derived from the existing
tablet map* — no new control-plane state), leaving the sync `AccordCore`
byte-for-byte unchanged and the whole prior suite green. Look for the
by-composition path before editing a load-bearing state machine; and **a node
hosting several protocol instances needs one `Env`/inbox/WAL *per instance*** (the
inbox is single-consumer) — allocate a distinct id per (node, instance) and let
the caller own that allocation policy. (`animus-consensus` `shard.rs`.)
