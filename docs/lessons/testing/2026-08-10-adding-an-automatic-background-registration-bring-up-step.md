# Adding an automatic background registration/bring-up step makes any test's "not yet registered" pre-assertion a race, not an invariant — sweep for assertions on the *absence* of state the new automation now establishes.

**Adding an automatic background registration/bring-up step makes any
test's "not yet registered" pre-assertion a race, not an invariant — sweep
for assertions on the *absence* of state the new automation now
establishes.** Folding growth-node membership self-registration into
`start_with` (ADR 0032 PR2) broke `cluster_growth.rs`'s sanity check that
a freshly-started growth node "should not be a member before admin-add" —
intermittently (the self-registration + heartbeat promotion can complete
before the test's first poll, or not), the worst kind of breakage. The
dual of the documented "removed shortcut → grep for tests that relied on
it" lesson: an *added* automation invalidates assertions about the
pre-automation quiescent state. The honest fix is to delete the stale
pre-assertion and let the convergent post-state assertion (it *does*
become `Active`) carry the proof. (`animusd` `tests/cluster_growth.rs`.)
