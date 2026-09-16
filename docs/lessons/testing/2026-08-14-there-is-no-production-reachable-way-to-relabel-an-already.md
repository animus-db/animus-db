# There is no production-reachable way to relabel an already-`Active` cluster member today — a real, small operational gap, not just a test inconvenience.

**There is no production-reachable way to relabel an already-`Active`
cluster member today — a real, small operational gap, not just a test
inconvenience.** `POST /admin/member/add`/a join's `labels` parameter
only ever *claim* a fresh identity; `ClientCtx::admin_drain`'s
local-leader-only `UpsertMember{Leaving}` preserves whatever labels are
already on file but never sets new ones; the generic wire path
(`ClientRequest::ProposeSchema(UpsertMember{..})`) is gated by
`is_relayable_command` to `status: NodeStatus::Down` only — proposing
`Active` with new labels through it is rejected outright, by design (the
gate that keeps `admin_drain`-class actions local-leader-only). Building
a sim test that needed to label 3 of 5 already-bootstrapped nodes (ADR
0043 §2's optional stream-shard isolation, PR B6) found the one
production-reachable workaround instead of adding new admin API surface
for a test-only need: propose `UpsertMember{labels: new, status: Down}`
(passes the gate) on a member that is genuinely still alive and
heartbeating — the very next `detect_loop` tick observes it alive and
re-promotes it to `Active` via `transition()`, which reads the label
*just committed* off `Metadata` and preserves it verbatim. The node
never actually goes anywhere; it just flaps through `Down` for one
detector tick. This is the general shape worth remembering: when a
cluster-state field has an update path gated to a narrower status than
the one you need, look for whether some *other* legitimate transition
already reads that field fresh and would carry your change through it —
cheaper and more honest than adding a new mutation path whose only real
caller would be a test. (`crates/animusd/tests/
stream_shard_label_isolation.rs::label_node`, 2026-08-14.) If a real
operator need for relabeling an active member ever surfaces, that's a
genuine follow-up (a dedicated admin action, not this flap trick).
