# A restarted Raft replica re-applies its recovered log from the start, so any consumer keyed on replicated state passes through *historical* states — a loop acting on *absence* (a GC/teardown) must be convergent, and its post-restart assertions must poll.

**A restarted Raft replica re-applies its recovered log from the start, so any
consumer keyed on replicated state passes through *historical* states — a loop
acting on *absence* (a GC/teardown) must be convergent, and its post-restart
assertions must poll.** The drop-table GC (ADR 0024) keys on "tablet no longer in
the map"; during post-restart replay the map transiently *contains* the dropped
tablet again, so the join-host loop briefly re-hosts an empty zombie group — then
replay reaches the drop and the GC reclaims it. That round-trip is correct
(convergent, ids never reused), but a test that one-shot-asserts "files still
gone" after a fixed post-restart sleep flakes bimodally: it catches the zombie
mid-flight. Wait for replay to complete (`last_applied == commit_index` ≥ the
full log via `/admin/raft`), then poll to the converged state — the restart
instance of the standing "eventual properties get a converged-or-timeout poll"
rule. (`animusd` `tests/drop_table_gc.rs`.)
