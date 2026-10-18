# A dueling-seal sim test needs a shorter winner prefix AND a stale catalog, built from slow links

Four past bugs (#572/#574 forced seal on the local route, #580/#582 split-driver
final seal, #302/#314 stale quiesce veto) were only pinned by real-thread tests.
Re-pinning them in `SimCluster` (`sim_cluster_seal_races.rs`) taught:

- **The periodic loop is not the race, `seal_now` is.** `change_consumer_loop`
  is `ProdEnv`-only, but its seal arm ends in one `seal_now`/`pitr_seal_now`
  call. A sim task running that call is a faithful duelling sealer; a loser is
  decided inside `seal_now` (same `(tablet, next_epoch)`, winner's range
  shorter than the loser's).
- **Two conditions, both constructed.** The loser must (1) read a catalog that
  lacks the winner's row and (2) scan more records than the winner. Slow the
  tablet-leader <-> control-leader links (`set_link_net_config`, zero jitter so
  proposals cannot reorder; move control leadership off the tablet leader with
  `transfer_control_leadership_to`) and commit one more write on the fast
  quorum before starting the loser. Poll in 1 ms slices for the write ack, then
  start the path under test at that instant. The end state (winner = 3 records,
  retried residual = 1) proves the loss happened without a metric.
- **A split freezes the log, so the winner must be a lagging replica.** After
  the fork the parent's change log stops growing; a shorter-prefix winner is a
  seal run on a follower whose L->F link is slow, with its proposal delayed
  (F->C slow) until after `BeginSplitInPlace` committed and C->L slow so the
  leader's catalog is stale when the driver ticks.
- **Revert each layer separately.** The stream and PITR twins have independent
  retry code; reverting one fails only its own tests (checked both ways for the
  local route and the split driver).
- **A freshness stamp only protects a group that was swept once.** The quiesce
  veto's `fresh_through` defaults to `u64::MAX` ("no constraint"), so a sim
  sweeper must visit each group while awake before its period is stretched;
  and `MIN_QUIESCE_AFTER` is `max(INDEX_DRAIN_INTERVAL, AUTO_SPLIT_INTERVAL)`
  (2 s), not 200 ms, so the stand-in sweeper's period must exceed 2 s.
- **A retired tablet is not "no such tablet" for long.** A walk that reaches an
  already-torn-down tablet gets a ~40 s forward "not leader ... retry" loop, not
  the lookup miss `classify_grow_response` folds into a skip; the #454 window
  is the instant between the metadata flip and group teardown, too narrow to
  construct deterministically.
