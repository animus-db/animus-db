# `leader_index_of` answers "who does this replica locally believe leads," which is the wrong question to ask about a node you just crashed (ADR 0061 rung D2 PR 1)

`SimCluster::crash` mutes a node (its tasks stay alive, its inbox is
cleared) — it does not stop its internal Raft state, and nothing tells a
muted node that its peers re-elected without it, since every message that
would carry that news is exactly what got muted. `is_leader_local`
(`leader_index_of`'s own per-node predicate) is a **local** read of that
frozen state, so calling `leader_index_of` again right after `crash` + an
election window can — and, on one seed, did — return the **crashed**
node's own id, not whichever survivor actually won the new election
(`assert_ne!(new_leader, leader)` failed with `left: 0, right: 0`). The
existing `sim_cluster.rs` scenario 3 already avoids this by never calling
`leader_index_of` post-crash at all: it picks any survivor node index and
routes the write through it, relying on `cp_kind_write_item`'s/
`cp_kind_write_raw`'s own hint-chasing `forward_to_tablet_leader` loop to
find the real new leader internally. The new `sim_cluster_dynamo.rs`
crash/restart scenario didn't follow that precedent on the first draft and
hit the exact failure the precedent exists to avoid. **General lesson: a
"who currently leads" accessor backed by one replica's own local state is
answering "what does THIS node believe," not "what is objectively true
right now" — it is unsafe to call on a node that was just faulted (crashed,
partitioned) until it has had a chance to actually learn the outcome. When
a test needs "the current leader" immediately after injecting a fault
against the previous one, route through the system's own forwarding/
hint-chasing instead of re-deriving the answer from a local accessor that
has no way to have heard it yet.**

**Amendment (2026-09-07, ADR 0061 rung D4 PR 2): the crashed node's own
stale belief is not merely a transient race window — it is PERMANENT for
as long as that node stays crashed (muted).** A `SimCluster::crash`ed
node's `RaftKvNode` never receives a higher-term message telling it to
step down (nothing gets through the mute), so it keeps reporting itself
leader forever, not just in the brief post-crash window the original
finding above describes. A scenario that specifically needs a NEW leader's
own id (not just "route a write correctly," which `cp_kind_write_raw`'s
hint-chasing already handles per the original finding) must scan only the
live node ids directly (`is_leader_local` per surviving id), never
`leader_index_of`/`SimClusterHandle::leader_index_of` unfiltered — a poll
loop that keeps calling the unfiltered accessor spins to its own timeout,
finding the same crashed leader on every single pass, never converging.
(`sim_cluster_auto_split.rs`'s own scenario d hit exactly this.)
