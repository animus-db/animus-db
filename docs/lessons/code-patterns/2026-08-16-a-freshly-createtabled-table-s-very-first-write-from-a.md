# A freshly `CreateTable`d table's very first write from a second node can race the new tablet's own leader election / that node's tablet-host reconciliation — plausibly reachable on `main` today, independent of any particular write path.

**A freshly `CreateTable`d table's very first write from a second node can
race the new tablet's own leader election / that node's tablet-host
reconciliation — plausibly reachable on `main` today, independent of any
particular write path.** Found while chasing the above: `CreateTable`
returning `200` only guarantees the *catalog* entry (the schema/tablet-map
row) committed on the control-plane leader — it says nothing about
whether the new tablet's own CP Raft group has finished its (normally
sub-second) internal leader election, or whether every node's
tablet-host reconciler has yet observed it should host or relay for that
tablet. Two edge nodes issuing their very first write to a table
immediately after `CreateTable` returns can each hit this window and see
a hard `InternalServerError` (a genuine `relay_request` transport
timeout, or the propose confirm-poll's own timeout) — not a logical
rejection, a real failure. Not fixed here (out of scope for the ADR 0046
U3 stack that found it — flagged for a separate report); a few retried
warm-up writes reliably get past it, which is itself informative: the
window seems to close quickly once *any* write succeeds, consistent with
a one-time per-tablet cost (election + first reconcile tick) rather than
a sustained instability.
