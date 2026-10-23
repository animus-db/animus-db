# A history-sized background sweep must not share a task with a liveness-critical pass

**Context (issue #1369, soak seed 1001).** After ~54 minutes of kill -9
restarts a consistent read of one key answered nothing for 60 s through every
node. A `Pending` 2PC record anchored on a tablet this node had just taken
over was never recovered; the key's intent was under it.

**Mechanism.** `txn_resolver_loop` did two jobs in one serial task, tablet by
tablet: recover stale `Pending` records, and fan out a `TxnResolve` for every
decided-but-unresolved record. The second job's input is not "recent work": the
`TxnTracker` is rebuilt from the record rows on restart, every decided record
ever anchored becomes unresolved (nothing durable says a resolve ran), and the
sweep proposes once per record, serially. At ~50 ms a proposal on a debug
build, ~6500 records meant minutes per sweep, longer than the 120 s restart
interval, so the backlog never drained and a `Pending` record on a later (or
newly led) tablet waited behind it. The log showed no error: no stuck warning,
no inconclusive-verify metric. The recovery call was simply never reached.

**Rule.**
- A pass whose cost grows with accumulated history, or with how long a node
  was down, gets its own task. The liveness-critical pass (anything a client
  read or write can be blocked on) runs on a task whose latency is bounded by
  its own work only.
- Check leadership/eligibility when a pass reaches each group, not when it
  starts: a long pass otherwise acts on a stale picture.
- A hang with no signal is itself a finding: if a recovery path can be
  starved, give it a metric that proves it ran (the pass counter alone said
  the loop was alive, not that it had reached the record).

**Test.** `sim_cluster_txn_resolver_backlog`: one node leads a low-id tablet
with a decided backlog and a higher-id tablet with a stale `Pending` record,
over slow links. The production layout recovers it within seconds; the control
(one task, backlog first) must blow the bound. Follow-up: the backlog itself
(re-proposing resolves for records resolved long ago) is still unbounded work
per restart.
