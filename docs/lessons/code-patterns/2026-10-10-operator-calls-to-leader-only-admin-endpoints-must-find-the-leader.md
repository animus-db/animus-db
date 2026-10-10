# Operator admin calls to leader-only endpoints must locate the leader, and the fake must refuse non-leaders

**Context:** issue #1177. `drain_and_remove_node` posted `/admin/drain`,
`/admin/member/drain-status` and `/admin/member/remove` to the pod being
scaled away. The first and last are control-plane-leader-only and not relayed,
so a non-leader answers 409 "not the control-plane leader"; scale-down only
worked when the departing pod happened to be the leader. Every operator test
passed because `FakeAdminClient` accepted these calls on any ordinal.

**Lesson:** when a fake stands in for a server with a role-dependent
refusal (leader-only, voter-only), model the refusal in the fake, defaulting
to a non-trivial role assignment, so a caller that ignores roles fails in unit
tests rather than only in kind/e2e. On the caller side, a `Local` control
handle gives no leader address hint, so try candidates in turn (rotate only on
the specific not-leader marker, surface any other error) and pin the accepting
pod for the rest of the multi-step sequence; if no candidate accepts, return an
error rather than skipping the step.
