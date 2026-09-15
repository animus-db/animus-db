# `RaftKvNode::start_scoped` pins every group to `PRIMARY_STREAM` — a `SimEnv` test that starts more than one tablet group on the *same* set of node ids (any split/merge scenario sharing physical nodes across tablets, ADR 0026 Stage B) must use `start_hosted(.., stream = tablet_id.0)` instead, or the two groups' Raft traffic cross-talks on one node's shared inbox and corrupts both.

**`RaftKvNode::start_scoped` pins every group to `PRIMARY_STREAM` — a
`SimEnv` test that starts more than one tablet group on the *same* set of
node ids (any split/merge scenario sharing physical nodes across tablets,
ADR 0026 Stage B) must use `start_hosted(.., stream = tablet_id.0)`
instead, or the two groups' Raft traffic cross-talks on one node's shared
inbox and corrupts both.** The DynamoDB Streams lineage-walk corpus's
`combined_chaos` scenario (a leader-kill *and* a split on the same 3 node
ids) initially livelocked leader election on the *parent* group the
instant a sibling group started — `elect()` never found a stable leader
even after 4 seconds of simulated time, because every `AppendEntries`/
vote message either group sent was being delivered into whichever
group's `RaftCore` happened to poll the shared inbox next, so both groups
saw a stream of messages that made no sense to their own `RaftCore`.
Every existing test that only ever runs ONE group per node id
(`raftkv_linearizable.rs`, `txn_serializable.rs`'s three *independent*
node-id sets) or that already knew to use `start_hosted`
(`cross_group_lww.rs`/`narrow_scope.rs`'s split fixtures) never hits
this; a new self-contained corpus copying the wrong sibling function is
an easy trap. Production code never has this bug (`animusd`'s own
`cp_join_host`/`host::Reconciler` always calls `start_hosted` with
`stream = tablet.0`) — this is purely a test-harness footgun, but a
silent, hard-to-diagnose one (the symptom is "election never converges,"
not an obvious "wrong stream" error). (`crates/animus-test/tests/
stream_lineage_corpus.rs`, ADR 0042/0043 round-3 PR8, 2026-08-14.)
