# A simulated node restart must keep the Raft state and the binary profile

Building `sim_cluster_mrsc` (region loss, then the Region returns) cost three
separate "the restarted node never rejoins / never leads" investigations, all
harness bugs, none product bugs:

1. **`SimCluster::restart` re-applies the node's *recorded* binary profile
   (default `Phase1`).** `set_all_node_versions` sets the live range but does
   not record a profile, so every restarted node came back as a Phase 1 binary
   that cannot decode version-2 batches: its control Raft sat at commit 0 for
   the whole poll. When a cell finalizes to a version above the default
   profile's, also call `set_binary_profile(n, Release(N))` per node.
2. **With the `Memory` engine backend a restarted tablet group replays an
   empty Raft state.** The issue #667 boot-time cluster check then treats it as
   a wiped voter that must never vote or campaign (correct for a wiped disk), so
   a `TimeoutNow` leadership transfer to it is silently ignored and the leader
   never goes home. The tell is `refused_as_voter() == true` on the restarted
   replica with a healthy, fully caught-up log. A cell that restarts nodes
   must use `new_with_node_labels_lsm` (the retained-disk backend), exactly as
   `sim_cluster_upgrade_corpus` does.
3. **The fixture has no background split driver**; a poll that waits for a
   split must call `drive_inplace_split_cutover` on every node each tick.

Also: a "never converged" timeout dump must print each replica's role, term,
known leader and caught-up index, not just the tablet map; every step above was
diagnosed from exactly those fields (`SimCluster::group_states`). And a
convergence assertion that is specific to one configuration (the witness form
elects the *other* full replica; the plain form legitimately leaves the leader in
the third Region while the preferred one is down) must be written for that
configuration, not copied between cells.
