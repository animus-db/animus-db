# A real leadership-transfer end-to-end test (`transfer_leadership` + `TimeoutNow` over real `ProdEnv`/tokio) must poll-and-retry the *whole operation* against "whoever is leader now", not assert a single deterministic hand-off to a pre-picked target.

**A real leadership-transfer end-to-end test (`transfer_leadership` +
`TimeoutNow` over real `ProdEnv`/tokio) must poll-and-retry the *whole
operation* against "whoever is leader now", not assert a single
deterministic hand-off to a pre-picked target.** Writing the ADR 0037 PR3
self-removal test (leader removes its own control-voter slot, which arms a
transfer to the other remaining voter first), a first draft asserted
"exactly one specific other node becomes leader within N seconds" and hit a
real, reproducible stall under `cargo test`'s real scheduling: the old
leader's own step-down (satisfying the *server-side* 5s poll inside the
admin action) and the *test's* separate poll for "some other node is now
leader" can straddle a transient flip-flop (the target wins via
`TimeoutNow`, but the old leader's election timer also fires and it wins a
subsequent term back) that a 100ms-granularity poll can miss entirely,
leaving the test waiting on a leader identity that already changed again.
The robust shape (and the more realistic one — this is what an operator's
own retry already has to do) is a bounded loop that re-checks who is
currently leader among the surviving voters on every iteration and retries
the mutating call there, rather than snapshotting a target once. See
`crates/animusd/tests/control_membership_admin.rs::
remove_control_voter_refusals_transfer_and_quorum_warnings`.
**This exact anti-pattern reappeared** in a newer sibling in the same file
(`runtime_added_voter_survives_leadership_change_to_a_different_original_voter`,
ADR 0037 PR4), and flaked CI on `main` — failing *both* attempts of the
retried `prod-liveness` tier. It regressed to the one-shot shape because the
transfer was only *scaffolding* for that test's real subject (address
propagation after a runtime-added voter), not the property under test: when a
test forces a leadership transfer merely as setup, it still needs the
poll-and-retry-the-mutating-call shape above, or the scaffolding becomes the
flake. What makes the one-shot call unsafe is invisible from the call site —
`RaftCore::transfer_leadership` arms a deadline of one *raw, un-randomized*
`election_base` (150ms, not the `[base, 2*base)` range followers draw from),
`tick()` clears it silently with no log or metric, and the admin action's own
5s poll never re-arms, so a dropped transfer and an in-flight one are
indistinguishable to the caller (both surface as HTTP 409). Those two
product-side gaps are tracked in #313.
(`crates/animusd/tests/control_membership_admin.rs`, 2026-08-20.)
