# A catalog's "who satisfies this obligation" predicate must be the SAME accessor everywhere it's asked, or a stale report double-counts (ADR 0059 §6)

The backup-vs-split race (a pinned tablet retires mid-capture; its live
`split_lineage` descendants take over its share) has a subtler failure
mode than "the retired tablet's own report is missing": it's still
*possible* for the retired tablet to have reported successfully **before**
splitting (a `RecordBackupTabletComplete` that legitimately committed
first, on an ordinary unrelated split racing an already-finished tablet).
That report is real and harmless to leave on file — but once the split
lands, the retired id's own row in `Metadata::backup_tablet_progress`
becomes a **stale, superseded** entry: the tablet's current live
capture-frontier is its two children, and if a naive "sum every progress
row tagged with this backup id" accessor is used to build the final
manifest, the retired parent's full-range capture and its two children's
own (later, independent, narrower-range) captures all land in the
manifest side by side — a real content bug (every row in the split
range appears in the backup twice), not just an efficiency loss. The fix
was to derive one canonical accessor
(`Metadata::backup_manifest_tablet_progress`, built on
`live_split_descendants`) that always answers "the tablet whose id is
authoritative for the completed capture of this pinned tablet's range,
right now" — and to route *every* consumer through it: the readiness
check (`backup_ready_to_complete`), the manifest assembly (the completion
aggregator), and even the pre-existing `backup_total_bytes` display
figure (which had the identical latent double-count bug before this PR,
just never triggered — no code path had ever produced two progress rows
covering the same range before ADR 0059 §6 made that possible). General
rule: when a catalog gains a "this identity's obligation may transfer to
a different identity later" relationship (a retired-and-replaced key), a
single row's own presence is never sufficient to answer "has this
obligation been met" — build one shared accessor that resolves identity
to its *current* authoritative set first, and audit every existing
consumer of the raw per-identity map for the same latent assumption, not
just the new caller that motivated the change.
- **A per-tick consumer arm superseded by a dedicated driver during one
  lifecycle phase needs the SAME exclusion guard as every sibling arm the
  driver also supersedes — audit them together, not one at a time (issue
  #298, `animusd::index_drain::change_consumer_loop`).** `trim_janitor`
  was correctly gated `if !splitting` (a `Splitting` tablet's own endgame
  driver holds trim for the whole build/freeze window), but `seal_tick` —
  two lines below it, in the same per-tick loop, over the same
  `splitting` boolean already in scope — was not, so it could fire
  concurrently with that same driver's own dedicated final-seal loop for
  the identical tablet. Reproduced directly: the same record delivered
  twice under two *adjacent* epochs of one tablet (never within one
  epoch), which is the tell that distinguishes this from a same-epoch
  proposal collision (that shape is already caught and retried around by
  a content-aware confirm) — two independently-computed, non-colliding
  epoch numbers whose *coverage* silently overlapped instead. The general
  rule: when a lifecycle phase hands one concern (trim, seal, drain,
  whatever) to a dedicated driver for its duration, grep every *sibling*
  per-tick arm in the same loop for the identical exclusion condition —
  a driver superseding one arm and not its neighbor is exactly the kind
  of asymmetry a reviewer skims past because the superseded arm's own
  gate reads correctly in isolation.
- **"A read feeding a non-retried, permanent decision must use
  `metadata_fresh()`" extends to "a decision that becomes immutable
  bytes the instant it's made" — not just literal one-shot writes (issue
  #298, `animusd::index_drain::seal_now`).** This crate's own
  `CLAUDE.md` already names the first case (a schema commit-wait, a
  conditional-write existence gate). `seal_now`'s watermark/next-epoch
  computation looked like a routine, retried-if-wrong read — it feeds a
  loop that keeps calling `seal_now` until nothing is left to seal, so a
  wrong read *seems* self-correcting. It isn't: the read decides which
  bytes go into a segment that is written to the object store and
  cataloged in the same call, and a sealed segment is never revised —
  only ever superseded by a later, non-overlapping one. Under
  `effective_metadata()`'s ordinary staleness (a local node's own
  just-committed control-plane proposal not yet reflected in its own
  cache), a "retry" is really a *second, independent* decision computed
  against a floor that doesn't yet exclude what the first one already
  covered, silently producing overlapping immutable output with no later
  correction point. The generalized discipline: a decision is
  "non-retried" for this purpose if *what it decides* is committed to
  something immutable in the same call, even if the *calling loop*
  retries — the loop retrying doesn't make the decision retriable if
  each attempt's own output can never be taken back.
- **An "unreachable from this caller by construction" branch is a claim
  about the absence of concurrent structural change, not a permanent
  fact — re-derive it whenever splits (or any other lifecycle event that
  moves/removes state) enter the picture (issue #298, `animusd::
  txn_resolver_loop`).** The comment justifying `None` as `txn_recover`'s
  orphan-path hint was correct the day it was written: `pending_txns`
  only ever tracks a genuine, locally-anchored `Pending` record, so the
  record-absent branch that hint feeds "can't" run for this caller. That
  reasoning implicitly assumed the record stays reachable at its logical
  position for the whole recovery window — true until a transaction
  record turned out to be an ordinary logical key of its anchor tablet,
  riding the identical split clone/trim path every other row does. Once
  splits could relocate or (per a related, still-open investigation)
  possibly drop that row, the "unreachable" branch became reachable, and
  with no hint it had no fallback at all — it reported "still pending"
  forever, matching an observed transaction stuck reporting
  "TransactionConflict: ongoing" for a full test budget. The fix (passing
  the `created_ts` this caller already had on hand, previously discarded
  as `_created_ts`) is a no-op when the original assumption still holds
  and only takes effect in exactly the case the comment said couldn't
  happen. The general check: when a comment justifies skipping a
  fallback because "X can't happen from this caller," ask whether X's
  impossibility depends on nothing else in the system moving the state X
  would need — a lifecycle mechanism added *after* that comment was
  written (here, in-place split) is exactly the kind of change that can
  quietly invalidate it without touching the caller at all.
