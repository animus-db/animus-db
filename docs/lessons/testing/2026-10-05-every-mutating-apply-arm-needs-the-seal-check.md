# Every mutating apply arm needs the seal check (a fork clones the parent's CURRENT engine)

**What happened.** chaos_disk_full (2PC ops on) reverted a key to its prior
value, losing every later acked append (~1 run in 6). Trace: the same
tablet-3 `TxnResolve` saw the txn record `Pending` on two replicas and
`Committed` on the third. A `TxnCommit` ordered just after the split's fork
entry had applied to the frozen parent. The children are cloned from the
parent's *current* engine by the host reconciler, asynchronously and per
replica, so whether the decision was in a replica's clone depended on timing:
replica-divergent children, and a commit the coordinator acked that the
record's real owner (the child) never saw.

**Rule.** Anything that clones "the parent's current engine" is only
deterministic if the parent engine is truly frozen after the fork entry. So
EVERY mutating apply arm must be a no-op on a sealed key, including the quiet
ones (`TxnCommit`, `TxnAbort`, the orphan-abort tombstone), not just user data
writes. When adding an arm, check it against the `Freeze`/`SplitTablet` seal;
`split_tablet.rs` is where that contract is tested.

**Why no gate.** ADR 0073: apply never branches on a gate and a semantic change
ships as a new proposer-gated variant. This is not a new semantic: the sealed
window was already replica-dependent (the clone race), so there is no
well-defined old behaviour to preserve, and the coordinator keeps the same
`TxnCommit`/`TxnAbort` entries. A mixed-version cluster is not supported until
ADR 0073 Phase 3.

**Also learned.** A test that "creates an orphan tombstone on a frozen group"
encoded the opposite of production (recovery routes the tombstone to the
record's current owner, the child); a foreign blocking intent produces the same
"anchor stage never landed" shape without the seal.
