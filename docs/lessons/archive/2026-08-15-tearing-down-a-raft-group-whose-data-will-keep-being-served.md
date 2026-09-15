# Tearing down a Raft group whose data will keep being SERVED (not erased) must drain the group's committed log into the engine first — `shutdown()` halts the async apply task at its next loop-top check WITHOUT draining, and deleting the group's WAL then destroys the only local copy of the committed-but-unapplied tail.

**Tearing down a Raft group whose data will keep being SERVED (not erased)
must drain the group's committed log into the engine first — `shutdown()`
halts the async apply task at its next loop-top check WITHOUT draining, and
deleting the group's WAL then destroys the only local copy of the
committed-but-unapplied tail.** Found via ADR 0033's own 3-node merge
integration test flaking ~1-in-5 *in isolation* (per the standing rule, a
flaky `ProdEnv` test is a real bug): a write acked by the absorbed group's
leader right before the merge was applied to *that leader's* engine (ack
requires leader-local apply) but not yet to a follower's — commit-index
propagation runs up to one heartbeat behind, while the reconciler's
event-driven `metadata_watch` fires the `Absorb` teardown on the very
commit that made the merge visible, i.e. *designed* to race that window.
The follower's engine then permanently lacked the acked key, and if that
node hosted the merge survivor's leader, linearizable reads answered a
definitive "key absent" forever — indistinguishable from data loss. The
same non-draining shutdown is **harmless for `Release`/`Reclaim`** (their
teardowns erase the data anyway; other replicas serve) — which is exactly
why it was never noticed: the invariant "a torn-down group's unapplied
tail doesn't matter" was true for every teardown that existed before merge
added one whose data lives on. Three-part fix, each load-bearing: the
`Absorb` teardown drains (commit covers the local log, engine-applied
covers commit) while the driver is still live; `plan` defers the
survivor's `WidenScope` until the absorb confirms (drain-before-widen —
the planner's fixed emission order alone would have widened *first*); and
the read path stopped conflating two "None"s — a ReadIndex barrier
failure and a genuinely-served absent — plus gained the read-side dual of
ADR 0028's pre-propose range check (a get/scan whose group's live
`scope_range()` doesn't contain the request errors retryably; for scans
the un-widened scope was otherwise a *silent truncation*, since
`linearizable_scan` filters rows through the live scope). **Two general
checks: (1) when a new feature makes a previously-universal teardown
invariant ("this group's data dies with it") false for one new path, audit
the teardown's every step against the new path — the WAL delete that was
cleanup before is data loss now; (2) grep read paths for `Option`-collapse
points where "couldn't serve" and "served: absent" merge into one value —
the Get/Scan arm asymmetry (Get mapped `None` to absent, Scan mapped it to
an error) was the tell.** The deterministic regression drives the write →
merge-view tick with zero intervening sim time, so the apply task provably
hasn't run — no wall-clock race needed.
(`animus-cp-data::host::Reconciler::teardown`'s Absorb drain + `plan`'s
`absorbing` gate; `RaftKvNode::linearizable_get_served`; `animusd`
`cp_get_local`/`cp_scan_local`; regressions:
`reconciler_corpus.rs::scenario_merge_widens_and_absorbs`,
`host::tests::widen_is_deferred_while_the_absorbed_sibling_is_still_hosted`,
`animusd` `split_fence_tests`' read/scan duals.)
