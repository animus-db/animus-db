# A rare mixed-version failure is explained by modelling the old binary's apply, not by more CI runs (issue #1315)

`upgrade-previous-release` lost acked appends in ~4 of 45 CI suites and in none
of ~70 local ones. Re-running it cannot separate "an R-1 defect" from "a roll
defect", and the artifact was not fetchable.

- **Model the old side as a test-only apply mode** (`animus-cp-data` feature
  `legacy-r1-model`, `legacy_r1_model::set(node, true)` before a node starts)
  and replay one log through both semantics in a SimEnv cell
  (`tests/it/legacy_r1_mixed_stage.rs`). It reproduced both CI symptoms in
  seconds: a stale `TxnStage` that R-1's in-memory `recently_resolved` rejects
  and the current build's durable marker does not (or the converse) resurrects an
  intent on one replica; a later retried resolve then rewrites the key to the old
  transaction value there (a lost acked write) and the intent blocks a later
  transaction there (a half-applied acked txn).
- **A replica cannot tell "never resolved" from "resolved by a binary that wrote
  no marker"**, so no apply-side rule on the current side closes this; a
  durable-state fix (#1243) only helps replicas that all run it.
- **Make the next failure self-explaining in the job log**: when artifacts are
  unreachable, print a bounded digest on a violation (lost values with ack times
  and txn partners, restart times on the same clock, every node's own view of
  the keys, per-node counts of the "stage rejected/blocked" log lines). In a
  PASSING run the same digest showed 13 "TxnStage rejected" per node, so stale
  stages are routine, not rare.
- Check the pinned R-1 against the amendments that fixed the bug class before
  blaming the roll: `git grep recently_resolved <pin>` showed the pin predates
  #1243/#1251/#1242.
