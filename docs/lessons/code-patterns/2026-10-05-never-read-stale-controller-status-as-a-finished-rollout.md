# Never read a controller's stale status as "the rollout is finished"

**Found by**: ADR 0073 Phase 3, P3-D design (the partition driver for the operator's StatefulSet).

Right after the operator applies a changed pod template with `partition = replicas`, the
StatefulSet's `status.currentRevision`/`updateRevision` still describe the *previous* spec (equal
to each other) until the StatefulSet controller observes the new generation. A reconciler that
reads "revisions equal, nothing updated" as "steady state" and resets the partition to 0 rolls
every pod ungated, which is exactly the window the apply was built to close.

Rules that generalize:

- Gate any "it is done / nothing to do" inference on `status.observedGeneration >=
  metadata.generation`; until then keep the last-written value (`Stage::Hold`) and look again.
- The decision to *lower* a safety knob (a partition, a budget) must come only from a positive,
  current observation, never from the absence of a signal.
- Test it with a seeded object whose generation is ahead of `observedGeneration`
  (`a_stale_statefulset_status_never_resets_the_partition`); the mutation "treat stale as
  current" must fail a named test.
