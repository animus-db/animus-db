# A restarted sim group never re-quiesces, so assert the veto's input

Issue #1380 (decided txn records re-resolved after every restart, so followers
never drained and groups could not quiesce) first got a sim test that asserted
"every group quiesces after the restart". It failed on the fixed code, with the
trackers verifiably clean.

- **Probe the environment before blaming the fix.** A txn-free three-node LSM
  `SimCluster` with CP quiescence on, crashed and restarted node by node, is
  quiesced before and not quiesced 60 s after, with or without the new gate. A
  restart in the sim leaves groups awake for reasons unrelated to the tracker.
  The quiescence assertion therefore could not distinguish fixed from control.
- **Assert the input of the veto instead.** The veto is a function of the txn
  tracker. The test asserts the rebuilt tracker holds stubs only, and that the
  anchor group's commit index does not move after the restart (nothing was
  re-proposed). The negative control (gate never opened) re-queues every record
  and re-proposes resolves, so the pair discriminates.
- **Baseline the commit index before the sweeps start.** Sampling the baseline
  after the sweeps ran made a re-proposing run look quiet.
- **`SimCluster::restart` resets the binary profile.** In an era-on cluster a
  restarted node comes back as a Phase 1 binary and sits as PreCandidate. Set
  `Release(MAX_SUPPORTED)` before the restart and the node version after it, as
  the mixed-version corpus does.
- **Compact the log before restarting** or the control also looks fixed: the
  replayed tail re-applies the resolves and re-drains the tracker, hiding the
  rebuild bug that only the engine rows reproduce.
- **The sim burns virtual minutes per client call**, so a reap-age checkpoint is
  measured from the observed settle time, not assumed.
