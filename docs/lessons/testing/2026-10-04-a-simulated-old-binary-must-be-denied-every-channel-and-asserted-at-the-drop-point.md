# A simulated old binary: assert at the production drop point, and on state, not only on messages

Context: ADR 0073 Phase 2 mixed-version corpus (P2-D).

- The "no Phase 1 node ever receives a byte it cannot decode" assertion is a
  cfg-gated receive-site log taking the same branch as an undecodable message. A
  simulator-level delivery tap would need a per-stream decoder and cannot see
  inside `InstallSnapshot` chunks, and the log doubles as the negative control's
  observable.
- A snapshot carries `Metadata` JSON the current binary happily parses, so also
  assert on state: a Phase 1-profile node's applied `Metadata` has no versioning
  fields.
- Per-node profile configuration silently resets on restart paths (`SimCluster::
  restart` rebuilds `RaftNode` with defaults). Funnel construction, restart and
  growth through one `apply_profile` helper.
- Inject a premature emission by acting as the buggy proposer in the test body,
  bypassing the guard (precondition P), not with a production "break me" switch;
  reserve source mutations for the guard itself and record each in the PR.
- A corpus cell that crashes a node the shared client loop still routes to makes
  "acks during the window" legitimately sparse: put the non-vacuity check on a
  window that cannot be starved.
- A fixture node that was never given a profile has no cap installed at all, so
  "every node starts Phase 1" is not the same as "every node is capped as Phase 1":
  install the Phase 1 profile explicitly. The cluster-tier delivery assertion was
  vacuous for Phase 1 nodes until a mutation check ("cap logs but delivers") passed
  the negative control and exposed it.
