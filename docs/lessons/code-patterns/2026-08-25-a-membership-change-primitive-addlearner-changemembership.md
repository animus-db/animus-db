# A membership-change primitive (`add_learner`/`change_membership`) only ever reconfigures a group's own agreed-upon peer SET — it does nothing to ensure the target actually has a live protocol instance running to receive the traffic that config change generates (2026-08-25, ADR 0058 Train 2 rung 3).

**A membership-change primitive (`add_learner`/`change_membership`) only
ever reconfigures a group's own agreed-upon peer SET — it does nothing to
ensure the target actually has a live protocol instance running to
receive the traffic that config change generates (2026-08-25, ADR 0058
Train 2 rung 3).** Building the in-place split's Stage 1 (add the union
of both children's homes as learners to the parent), it was tempting to
assume "the reconciler adds a learner, Raft's own `AppendEntries` flow
handles the rest" — but a node named only in a CHILD's own `replicas`
(never the parent's) has, before this design, no reason to ever host the
PARENT tablet at all: the ordinary `plan_join_host`/`Host` candidate test
is "am I in `Tablet::replicas`," which such a node structurally fails.
Calling `add_learner` on it anyway appends a config entry the LEADER
replicates outbound — into a network inbox nothing on the target node is
consuming, since no `RaftKvNode` for that tablet exists there yet. The
fix widened the host-candidate test itself (a node recruited via EITHER
child's `replicas` of an in-place split intent also hosts the parent, as
a quiet non-voter — the identical shape `plan_join_host`'s own "joining
an already-led group" branch already uses, just reached by a different
membership test), and a corresponding second fix was needed on the
RELEASE side: the same recruited node is never in the parent's own
`replicas` either, so the ordinary release check would fire on it
immediately, and — a learner is never in `RaftCore::config()` by
construction — the release path's own "config actually excludes me"
safety anchor reads trivially true for a still-a-learner recruit too,
offering no protection. **General rule**: before wiring any new
membership-change call site (a replica move, a split, a rebalance) onto
an existing Raft membership primitive, ask "does the target already have
a running protocol instance for this group, and by what path did it come
to have one" — a primitive that changes *agreed state* is not the same
thing as a primitive that ensures a *listener* exists to act on it, and
the existing candidate/release tests for "should this node host this
tablet at all" were both written before any workflow needed a node to
host a tablet for a reason OTHER than being in its own `replicas`.
(`crates/animus-cp-data/src/host.rs`'s `plan` phase 1's second
host-candidate branch and phase 3's `recruited_for_split` exclusion —
both later deleted, along with the rest of Train 2 Stage 1/2, by ADR
0062 rung 5; the general rule above still applies to any future
membership-change call site.)
