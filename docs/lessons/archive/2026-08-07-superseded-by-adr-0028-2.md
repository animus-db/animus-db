# Superseded by ADR 0028

**Superseded by ADR 0028**: `DropOrphanTablet` (and the orphan it exists to
clean up) no longer exist — a split's single atomic command makes an orphan
structurally impossible. Retained for historical record. **A CAS guard closes the *concurrent* instance of a race; a *sequential*
instance of the same race needs its own answer — usually cleanup, not
another precondition.** The `SplitTablet` epoch CAS above stops two
proposers from *both* committing at the same epoch, but does nothing when
the first mint's own commit has already advanced the epoch by the time a
second, later trigger reads it: that second `SplitTablet` is now proposed
against the *current* epoch, so the CAS sees no conflict and lets it through
— even though the underlying CP-data group can still only ever apply one
real `Split`, so one of the two mints is doomed regardless. Live-observed as
permanent orphans continuing to accumulate (one per abandonment, unbounded)
under `--cluster 3 --auto-split 2000` + leader churn, well after the CAS fix
above landed — the "did I catch the bug" question was scoped to concurrency,
and a sequential path around the same guard was still open. **A CAS answers
"is this still the state I read"; it does not answer "did someone already
reserve the *outcome* my proposal is racing toward" when a valid,
non-conflicting intermediate state change is enough to let a doomed proposal
through.** The fix here isn't a stronger CAS (there's no cheap read at
propose time that would tell the loser it's doomed) — it's giving the
*already-existing* "abandon" detection (comparing the group's real
`applied_split_key()` against the proposer's own key) a real cleanup action
(`MetaCommand::DropOrphanTablet`) instead of only suppressing further
retries. When a race has both a concurrent and a sequential shape, expect to
need two different mechanisms — a precondition for the first, a
detect-and-reclaim for the second — not one guard stretched to cover both.
(`animus-control` `meta.rs::drop_orphan_tablet_removes_a_never_seeded_split_child`;
`animusd` `ClientCtx::drop_orphan_tablet`, `auto_split_loop`'s abandon branch,
`trigger_split`'s step-2 failure path;
`animusd/tests/cp_plane.rs::a_lost_split_race_does_not_leave_a_permanent_orphan`
reproduces the *sequential* shape deterministically via two manual splits —
no timing race needed, since the precondition is just "propose against an
already-narrowed range.")
