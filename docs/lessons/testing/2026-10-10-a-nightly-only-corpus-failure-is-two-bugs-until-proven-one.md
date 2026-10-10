# A nightly-depth corpus failure can be several unrelated bugs; bisect each seed, and "bad" must be tested

Issue #1249: `upgrade_mixed_cluster` (`ANIMUS_UPGRADE_SEEDS=40`) was red every
night from the first night it ran, and the red looked like one regression. It
was three independent things, found by replaying the failing seeds locally
(`ANIMUS_SEED=<seed> ANIMUS_UPGRADE_CELL=<cell> cargo test -p animusd --lib
<test>`, ~6 s each, deterministic):

1. **A production hang, `write_path::wait_applied_past`.** The helper's doc
   promised a forced re-check every `CP_CONFIRM_POLL_MAX`, but an inner `loop`
   re-parked until the awaited index applied, so a confirm wait on an index that
   never applies (a stopped or restarted node's old incarnation, a partitioned
   leader, a halted apply task) never returned to its caller's `deadline`
   check. It was latent since 2026-09-09 and surfaced when #1257 changed
   timing and a client write landed 3 ms before a node restart. Symptom:
   "the workload did not finish within its budget".
2. **A one-shot sample of an eventual property in the negative controls**
   (`wedged_control` read once at an arbitrary virtual instant). A healthy
   follower one entry behind the leader's commit read as wedged. Fixed by
   polling to the expected set (`wedged_settled`).
3. Nothing else: the nightly's first red night was simply the first night the
   step ran at depth (the corpus landed after the previous nightly started).

Rules:

- A new deep tier is untested at its nightly depth until it has run there.
  Run the nightly's exact env once before merging the tier, not only the
  per-push depth.
- A CI log you cannot fetch is not a blocker: the corpus names cell + seed and
  replays by `ANIMUS_SEED`. Run the nightly config locally and collect every
  failing seed; do not assume one cause.
- When bisecting, **test the endpoint you declare bad.** `git bisect start bad
  good` with an untested `bad` reports it as "first bad" when the culprit is
  later or the failure is a different one (here c37cde54 was reported first
  bad and passed the same seed). Bisect each distinct failing seed separately.
- A "forced re-check" or "bounded wait" claimed in a doc comment is a contract
  to test: park on an index that never applies and assert the caller reaches
  its own deadline (`wait_applied_past_deadline_tests`).
- To locate a hung client op, print `start`/`end` per op with the node id and
  virtual time and add `eprintln!` at each await of the suspect path; the op
  with a start and no end, and the last await it entered, name the hang.

Regressions: `write_path::wait_applied_past_deadline_tests`,
`sim_cluster_mixed_version_corpus_issue_1249_pinned_seeds`.
