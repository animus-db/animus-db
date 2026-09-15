# A new state-mutating replicated command needs the *same* CAS/precondition discipline as its sibling commands on the same resource — a missing guard is invisible until two proposers race.

**A new state-mutating replicated command needs the *same* CAS/precondition
discipline as its sibling commands on the same resource — a missing guard is
invisible until two proposers race.** `MetaCommand::SplitTablet` applied
unconditionally as long as its `split_key` fell inside the source tablet's
*current* range, with no check on the tablet's epoch — unlike its sibling
`CasTabletReplicas`, which already gates on `expected_epoch`. Two proposers
racing to split the same tablet at the same epoch (two independent
`animusd::auto_split_loop` instances, or an auto-trigger racing a manual one)
could each compute a different median from an equally-stale range view and
**both commit**: each `SplitTablet` mutates the source's range and mints a new
child id, and neither commit's precondition ever looks at the other's. But the
tablet's own per-tablet CP-data Raft group can only ever apply **one** real
`Split`, ever (an at-most-once apply-time guard there) — so the losing
metadata-level split's `new_id` becomes a permanent, leaderless,
metadata-only orphan tablet: present in `Metadata.tablets` with a real
range/replica set, but with no CP group anywhere in the cluster and no code
path that ever revisits it (the `auto_split_loop`'s existing "abandoned"
detection correctly stops *retrying* the losing key, but never cleans up the
`new_id` it already minted). Found live on a `--cluster 3 --auto-split 2000`
bulk-seed run (two orphaned tablets, `/admin/status` showed real ranges,
`/admin/raftkv` showed no group for either). Fixed by adding
`expected_epoch: Epoch` to `SplitTablet`, gated identically to
`CasTabletReplicas` — so the loser's step 1 (`propose_split_metadata`) now
fails cleanly (`Rejected("epoch mismatch")`), which `auto_split_loop` already
handles as "nothing was allocated, no orphan to track." **When adding a
command that mutates a resource another command already CASes, check whether
the new command needs the same guard — "my precondition happens to still
hold" is not the same as "no one else committed a conflicting change since I
read this."** (`animus-control` `meta.rs::split_rejects_a_stale_epoch_racing_a_concurrent_split`,
`tablet_split_merge.rs::racing_splits_at_the_same_epoch_only_one_applies`.)
