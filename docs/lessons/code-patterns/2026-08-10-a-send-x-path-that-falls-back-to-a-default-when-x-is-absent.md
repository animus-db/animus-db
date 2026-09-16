# A "send X" path that falls back to a *default* when X is absent can ship a silently-corrupt value — make the absent case impossible (set X at every state transition that needs it), not `unwrap_or_default()`.

**A "send X" path that falls back to a *default* when X is absent can ship a
silently-corrupt value — make the absent case impossible (set X at every state
transition that needs it), not `unwrap_or_default()`.** The per-tablet CP Raft
ships its engine image as `snapshot_blob`, set by the driver only on *compaction*;
the leader's `snapshot_chunk_for` did `snapshot_blob.unwrap_or_default()`. A node
that caught up via a *received* `InstallSnapshot` advanced `snapshot_index > 0` but
never set its blob, so when it later became the source it shipped **0 bytes** — the
receiver decoded an empty image (`EOF while parsing a value, line 1 column 0`),
dropped it, and never caught up (surfaced as "CP split: new tablet never appeared",
a *leaderless* split child). The fix sets `snapshot_blob` on the *install* path too,
so the invariant `snapshot_index > 0 ⟹ blob.is_some()` holds and no ship is ever
empty — far better than a 0-byte default that *looks* like a valid transfer. **A
recursive/relay protocol must hold its invariant at the *second hop*: A→B works
off A's freshly-built state; B→C is what exposes that B never retained what it
received.** A unit test that drives only one hop (`A→B`) misses it — drive the
re-ship (`A→B`, then `B`-as-leader→`C`).
(`animus-control` `raft.rs::handle_install_snapshot`; regression
`driver_applied_sm.rs::caught_up_node_reships_non_empty_snapshot`.)
