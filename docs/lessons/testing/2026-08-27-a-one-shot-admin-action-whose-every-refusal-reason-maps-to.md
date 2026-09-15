# A one-shot admin action whose every refusal reason maps to the same HTTP status is not itself proof of progress — a test (or a caller) must distinguish "armed and in flight" from "failed outright" some other way, or retry on the status alone (issue #405).

**A one-shot admin action whose every refusal reason maps to the same HTTP
status is not itself proof of progress — a test (or a caller) must
distinguish "armed and in flight" from "failed outright" some other way,
or retry on the status alone (issue #405).** `tests/
heartbeat_live_destinations.rs::
heartbeat_reaches_a_runtime_added_voter_after_it_becomes_leader` called
`POST /admin/control/member/remove` (`ClientCtx::
admin_remove_control_member`'s leader-self-removal branch) **once**,
asserted `status == 409`, and then separately polled the new voter's own
`is_control_leader()` for up to 15s. But `RaftCore::transfer_leadership`
only arms if the target's `peer_match` has already caught up to
`commit_index` **at that exact instant** (its own doc: deliberately no
"eventually" — a caller that needs that is expected to re-arm every tick,
which this one-shot admin action does not), and the control group's own
background churn (the failure detector's liveness `UpsertMember` for the
freshly-added voter, `reconcile_loop`'s placement bookkeeping) can advance
`commit_index` past the runtime-added voter's replicated position in the
narrow window between the test's own voter-set convergence check and this
call — a window real machine contention widens. Every refusal this action
can return — "could not arm," "armed but the target never finished
stepping up within its own internal poll," "already not the leader" —
maps identically to HTTP 409 (`admin.rs::action_remove_control_member`
converts every `Err` the same way), so a single 409 says nothing about
which of those happened: pre-fix, a "failed to arm" 409 (plausible under
load, since arming needs the target's replication caught up at that exact
instant) left the test's own 15s poll waiting on a transfer that was never
actually driving forward — no code anywhere was going to retry the arm,
since this admin action deliberately does not retry itself (a human
operator does, per its own error text, which literally says "retry").
**General rule**: when a test (or any caller) issues a single mutating
admin call and then polls for a *separate* side effect, check whether that
admin call's own documented contract is "fire once, the effect either
starts or the response tells you to retry" — if so, the poll must retry
the **call itself**, not just wait on the side effect, or a single failed
attempt strands the poll for its entire budget with nothing left able to
produce the effect. Fixed by folding the single call + separate poll into
one loop that checks the side effect first and re-issues the admin call
(tolerating repeated 409s) until it succeeds — safe here specifically
because a re-arm of an already-armed *same* target is a documented no-op
that doesn't reset the transfer deadline, so a retry can never make a
transfer already succeeding worse. (`crates/animusd/tests/
heartbeat_live_destinations.rs`, `crates/animusd/src/lib.rs::ClientCtx::
admin_remove_control_member`, `crates/animus-control/src/raft.rs::
RaftCore::transfer_leadership`.)
