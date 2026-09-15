# CONFIRMED and fixed: a suspected latent cross-group LWW version hazard on split/merge (flagged in a PR #90 review comment) was real

**CONFIRMED and fixed: a suspected latent cross-group LWW version hazard on
split/merge (flagged in a PR #90 review comment) was real** — every tablet
a node hosts shares one physical `StorageEngine` (ADR 0026/0028), and
`animus-cp-data` stamps each write's MVCC version as its **own** group's
local Raft log index, which restarts low/independent for a fresh group. A
split's new sibling could carry a version no higher than what the *source*
group already stamped for a key now in the sibling's range; a merge
survivor's group keeps running but starts serving keys the absorbed
sibling's group versioned under a different, unrelated sequence. Either
way `StorageEngine::merge`'s per-key LWW silently no-ops the write (loud,
not silent corruption — the confirm loop's poll-for-exact-value-equality
times out — but the write never lands). Reproduced directly at the
`RaftKvNode` level with no control-plane machinery needed: write a key
through a whole-keyspace group at a high index, narrow it away, start a
**fresh** sibling group over the *same* shared engine scoped to that key's
range, write the key again — silently dropped
(`animus-cp-data/tests/cross_group_lww.rs`).
**Design space explored, and why the obvious-looking alternatives don't
work**: (1) seeding a fresh/widened group's floor from a **live,
per-replica** read (`storage.latest_version()`, or "whichever
`next_tablet_id` counter value happens to be current when this replica's
own tick fires") looks tempting since it needs no schema change, but two
*different replicas of the same group* can observe different values at
slightly different real-world moments — and since the group's `RaftCore`
log-index numbering (Host) or an already-running group's live floor
(merge's widen) must be **byte-identical across every replica** applying
the same command, a per-replica-timing-dependent floor either breaks Raft
log-matching outright (Host: divergent `snapshot_index` bases before any
election) or makes two replicas stamp *different* versions for the
identical committed write (merge: a bare local read has no cross-replica
agreement at all). (2) Using the **tablet's own id** as the floor works
cleanly for split (a fresh sibling's id is always allocated *after*, hence
numerically greater than, the source's) but not for merge in general: `left`
and `right` are chosen by **key-range adjacency**, not id order — a tablet
re-split from the *middle* of an existing chain mints a new id that can be
*numerically larger* than an unrelated tablet further right in key-range
order, so a later merge of that pair can have `right.id > left.id`, and
"bump past `right`'s id" would then either be a no-op or, worse, could
someday design itself into `left` permanently unable to out-version
`right`'s history. **The fix that actually holds**: a `version_floor: u64`
field on `animus_tablet::Tablet` itself (shared by both planes' `Tablet`
type, so no projection duplication needed) — `0` by default (byte-identical
to today, `#[serde(default)]` for back-compat), bumped **once, by the
control plane's own deterministic `apply`** at exactly the two moments a
cross-group version collision can occur: `SplitTablet` sets the new
sibling's floor to `source.version_floor + 1` (always exceeds anything the
source could have stamped, since a group's own local index realistically
never approaches the scale factor between rescopes — auto-split already
caps a tablet's key/byte count long before that); `MergeTablets` bumps the
surviving `left`'s floor to `max(left, right) + 1` (exceeds *both* sides,
closing the "which id is bigger" trap the id-based scheme fell into). Every
data replica reads this **already-agreed, replicated** value from
`Metadata`/`MetadataView` at `Host`/`WidenScope` time — never computes it
locally — so it is identical across replicas by construction, the same
discipline as every other epoch-CAS'd placement fact in this codebase.
`RaftKvNode`'s actual stamped version is `floor * SCALE + local_index`
(`effective_version`, `SCALE = 2^40`) — a group's own log index is
completely untouched (no Raft log-matching risk at all; `engine_applied`
still tracks the raw index), only the *storage-layer version number it
stamps* changes, and only for a tablet that has actually been through a
split/merge. **General lesson: when a per-group monotonic counter (a Raft
log index, a local sequence number) is reused as a version/ordering token
that must compare correctly *across* groups whose identities can change
over time (a split/merge/rebalance lineage), the floor that keeps groups
from colliding must be a value every replica reads identically from
already-replicated state — never derived from a live per-replica read (even
a "conservative always-safe upper bound" one), and the exact arithmetic
direction (which side's id/floor can legitimately end up numerically larger)
needs checking against the *actual* pairing rule (adjacency, not allocation
order) before trusting an id-based shortcut.** (`animus_tablet::Tablet::
version_floor`; `animus-control::meta.rs`'s `SplitTablet`/`MergeTablets`
apply; `animus-cp-data::RaftKvNode::start_hosted_with_floor`/
`bump_version_floor`/`effective_version`; regressions in both crates —
`animus-cp-data/tests/cross_group_lww.rs`,
`animus-control::meta::tests::{split_tablet_seeds_the_new_siblings_version_
floor_past_the_sources, merge_tablets_bumps_the_survivors_version_floor_
past_both_sides}`.) **Superseded twice over: `version_floor` itself was
retired by ADR 0018 PR2's range-seal design** (an ordering-based fence
replaced the version-space separation this entry's whole fix relied on),
**independently of this stack's merge removal** — this entry was already
describing dead code before ADR 0044 shipped.
