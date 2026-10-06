# WAL replay re-applies old entries over an engine that already holds their future

On restart a replica replays its log tail from `snapshot_index` (the last
compaction) over its own durable engine, which is durable per call and so
holds everything applied before the kill — typically thousands of entries past
the replay start. An apply arm whose *decision* reads engine state therefore
sees future state on replay and can decide differently than the live apply (and
than every other replica) did. The per-key `merge` version guard hides this for
arms whose only effect is a plain per-key merge: a replayed write loses to the
later row at its key. (Not every single-key arm: `KindEval`/`KindEvalBatch`
re-decide from state and write derived rows on unique keys — issue #1247, fixed
the same way.) It does
**not** protect a whole-or-nothing arm over several keys: the decision flips
on one key's later state while the merge still lands on another key that has no
later write.

Seen as issue #1242: a stale/duplicate (or since-unblocked) two-key `TxnStage`,
rejected live, was accepted on replay and resurrected an intent on one replica;
that intent then blocked every later stage on the partner key there, so acked
transaction appends were dropped on that replica only — plain writes to the
other key unaffected, which is what made it look like a read-path bug. It
survived the #1243 fix (durable markers) because the marker is per key and
replay sees later markers.

Rules:
- Treat replay as part of the apply contract, not recovery trivia. For any
  multi-key conditional arm, ask "what if every other key's row were from the
  future?". A version strictly above the entry's own `ts` at any key it reads
  proves a later entry ran; skip the whole entry (equal is the entry's own,
  possibly partial, write and must re-apply). Read that version
  **tombstone-aware**: `get` hides a deleted key, and "deleted since" is exactly
  the future state replay must notice (a stage rejected live by "A absent"
  passes on replay once A is deleted).
- The guard is unreachable live except for `SeedBatch` (restore merges rows at
  carried source versions): a stage hitting such a key is Fenced live too,
  deterministically on every replica — a liveness edge on a not-yet-served table.
- A replica-identity assertion must compare raw rows **including tombstones,
  anchor records and resolved markers**; `entries()` hides all three and so
  hides this whole bug class.
- A restart test must replay **over a retained engine** (`SimEnv`
  `stop` + start with the same `MemoryEngine`); wiping the engine or only
  checking after catch-up by `AppendEntries` makes it vacuous.
- Bisect a replica divergence by snapshotting every replica's raw rows after
  each schedule step; the first step that differs names the restart that
  replayed it. A temporary `eprintln!` of per-apply decisions with the node id
  showed the replayed decision differing from the live one in one run.
- The chaos run's tell: only transaction values missing from one replica,
  starting at the first entry applied after its restart.

Regression: `animus-cp-data` `tests/it/txn_stage_replay_stability.rs`.

Follow-up, issue #1247 (`KindEval`/`KindEvalBatch`): the same bug through a
single-key arm, because *derived* rows (change-log on `prefix||ts||ordinal`, LSI
rows keyed by item attributes) sit on keys other than the decided one, so
per-key LWW on the base row protects none of them. Extra rules it taught:
- Whether to guard with `>` or `>=` depends on the atomicity of the entry's
  writes. `TxnStage` merges key by key, so an equal version is its own possibly
  partial write and must re-apply (`>`). `KindEval*` lands everything in one
  atomic `merge_batch` (a single WAL record), so an equal base row proves the
  whole entry landed and a re-evaluation would read its own post-state (`ADD`
  applied twice, a `not_exists` item failing on its own write): use `>=`.
- Look for decisions that feed *later items of the same entry*: a rejected item
  consumes no change-record ordinal, so a replay that flips an earlier item's
  outcome shifts every later item's key — an orphan row even when each item
  "looks" idempotent. Guard such an entry at entry granularity.
- The direction of a divergence can be the opposite of the first hypothesis
  (the restarted replica had an extra row because of a *shifted key*, not a
  resurrected write). Bisect with a per-step `assert_identical`, and print the
  restarted node and each item's `old` image on live vs replay.
- Cheap live-path cost: reuse the `get` the arm already does; only a miss
  (absent or tombstoned) pays the tombstone-aware scan.

Regression: `tests/it/kind_eval_replay_stability.rs`.
