# A restart-heavy SimCluster corpus needs the LSM backend and client-side op caps

Found while building `sim_cluster_roll_orchestrator` (ADR 0073 Phase 3, P3-C). Two harness
traps looked like product bugs and cost a debugging session each; neither is one.

1. **`Memory` backend restarts are wiped disks.** `SimCluster::restart` over `SimEngineBackend::
   Memory` rebuilds the control `RaftNode` with `MemoryEngine::new()`: the system-keyspace
   mirror of `Metadata` is gone while the Raft log (on the sim disk) is not. Until the log is
   compacted this is invisible (replay from index 1); after that the restarted control node
   reports the leader's applied index with a *partial* `Metadata` (two of four members, no
   tablets) and never repairs it, so `cluster-version`/`roll` views off that node are wrong. The
   mixed-version corpus rolls few enough entries to dodge it. A corpus that restarts every node
   under load must use `SimCluster::new_with_lsm_engines` (what a process restart actually keeps).
   Symptom to recognise: `control_raft_indices` equal on all replicas, `metadata(n).members`
   different.

2. **A request to a stopped node never completes.** `crash(n)` mutes the node, `restart(n)`
   drops its tasks; a client call already in flight on that node's ctx waits on timers that died
   with the old process, so the client loop hangs and `run_until_clients_done` times out ("the
   workload did not finish") on a perfectly healthy cluster. Cap each client op on the client's
   own never-crashed env (`select` against `env.sleep`) and record a capped op as indeterminate.
   Do not add the cap to the shared client loop: it registers extra timers and perturbs every
   other corpus's schedule.

Also: inject faults **between** ticks (before the observation), not between a decision and its
execution, or the oracle (which re-derives truth at the decision) blames the decision for a
fault it could not have seen.
