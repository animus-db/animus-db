# A snapshot field withheld for N-1 compatibility makes replicas diverge

**Found by**: issue #1251 (the `txn_stage_replay_stability` corpus diverged at
cluster version 1, seed 2882520953).

Once an apply decision reads durable state (the #1243 resolved marker), that
state is part of the replicated state machine: every replica must hold the same
set, whatever its path (live, replay, snapshot install). The marker was a
class G row that `engine_image` *omitted* while its gate was closed, to keep a
previous-release replica from seeing it as a client row. The omission was safe
for N-1 and wrong for everyone else: a snapshot-installed replica at version 1
(which includes every unfinalized cluster, since the era starts at 1) lacked
markers and decided a stale re-stage differently.

Rules that generalize:
- "Do not send it to N-1" is a constraint on the *channel*, not a licence to
  drop the data. Find a channel the old reader ignores. Here: the image body is
  unchanged and the entry carries a row kind in no scope; the old
  `install_engine_image` drops unknown kinds (verify against the pinned R-1
  reference with `git show <ref>:<file>`), a new receiver maps it back.
- Failing closed only on the replica that lacks the data is not a fix: it
  rejects what the others accept, the same divergence reversed.
- A gated down-conversion also changes encodings (a v2 intent ships as v1): a
  raw-row identity check at the closed gate must normalize those first, or it
  reports a designed difference instead of the real bug.
- Run each replica-identity corpus at BOTH sides of every gate it touches; the
  schedule that hid this was green at version 2 for a day.
- A snapshot sent by an N-1 node cannot carry what N-1 never had; that residual
  ends at finalize and belongs in the ADR inventory row.

Regression: `tests/it/resolved_restage_replica_determinism.rs`,
`tests/it/txn_stage_replay_stability.rs` (`txn_replay_corpus_at_cluster_version_1`).
