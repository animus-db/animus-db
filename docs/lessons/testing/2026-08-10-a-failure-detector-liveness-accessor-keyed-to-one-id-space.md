# A failure-detector/liveness accessor keyed to one id space (raftkv ids) will silently return a wrong-but-plausible answer if called with an id from a *different* id space (control ids) — it never panics, it just lies.

**A failure-detector/liveness accessor keyed to one id space (raftkv ids)
will silently return a wrong-but-plausible answer if called with an id
from a *different* id space (control ids) — it never panics, it just lies.**
`ControlHandle::believes_alive`/the underlying `FailureDetector` only ever
observes heartbeats from **raftkv** ids (`heartbeat_loop` runs on the data
role only, ADR 0012); calling it with a **control** id (as a first draft of
the ADR 0037 PR3 quorum-loss warning did, checking "are all other control
voters believed Down") returns `false` unconditionally for every control
id, not "unknown" — so the warning fired on *every* removal, not just the
risky ones, and a naive test would have "passed" by coincidence (the
warning was expected on the specific case being tested) while being wrong
in general. Caught by testing the *negative* case too (a removal that
should carry no warning) and getting one anyway. The fix was to drop the
liveness-based trigger entirely rather than bridge id spaces by convention
(`RAFTKV_ID_BASE + control_id` is a *naming* convention for combined-mode,
not a structural guarantee for an operator-chosen or control-only id) —
when there is no real signal in the id space you actually have, don't
guess one from a different id space's accessor just because it type-checks.

**Update: closed by the ADR 0037 hardening trio's quorum-guard liveness
fix (PR 2, PR #136)** — not by bridging `believes_alive` after all, but by growing a
genuinely **control**-id-native signal instead: `RaftCore` already knows
exactly who has recently acked an `AppendEntriesResp` (success or reject),
since that's the leader's own control-Raft traffic — no id-space crossing
needed at all. A volatile `last_contact: BTreeMap<NodeId, Nanos>`
(`animus-control/src/raft.rs`, seeded at `become_leader`, stamped in
`handle_append_resp`, deliberately never persisted — same lifetime as
`next_index`/`match_index`) backs a new `RaftNode::
control_peer_believed_alive` (its own `CONTROL_PEER_LIVENESS_TIMEOUT`,
not a reuse of `DETECT_TIMEOUT`). The general lesson still holds — it's
*why* the fix had to grow a new signal in the id space that actually has
one, rather than solving it by finally writing the id-bridging code this
entry warned against.
