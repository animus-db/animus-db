# Adjudicating a race in consensus does not order the physical writes below it — a shared mutable storage key underneath an agreed decision is its own, independent hazard, and the fix is to remove the sharing, not to make the adjudication "more correct."

**Adjudicating a race in consensus does not order the physical writes
below it — a shared mutable storage key underneath an agreed decision is
its own, independent hazard, and the fix is to remove the sharing, not to
make the adjudication "more correct."** Root-cause of the "handful of
records missing from every segment and the open tail alike" symptom the
entry above reported but didn't chase down: two independently-computed
DynamoDB Streams seal attempts for the tablet's SAME open epoch (the
realistic trigger is a brief dual-leadership window during a
write-burst-induced re-election, not merely a same-leader crash-retry)
both derived the identical deterministic `SegmentStore` id
(`{table}/{label}/{tablet}/{epoch}`) and raced their physical `put`s
there. `Metadata::apply`'s `SealStreamShard` arm correctly picked one
winner (first-committer-wins on content) — the **consensus layer was
never wrong**. But the segment store underneath it had no matching
adjudication: `SegmentStore::put` was "idempotent overwrite,
last-write-wins" by contract, so whichever attempt's `put` physically
landed *last* won the bytes, with **no relationship** to which attempt's
*proposal* won the catalog. The design's own safety argument for a
retry ("a harmless superset, because a reader always slices to the
catalog row's own committed range") tacitly assumed the later-landing
`put` is always the *larger* one — true for a sequential same-leader
retry (a strictly later scan, by HLC monotonicity), but false the moment
two *independent* attempts race, and when the later `put` happened to be
the *smaller* one, the gap between it and the catalog's own committed
range was silently, permanently lost — a real data-loss bug that shipped
and stayed undetected because the frozen fault-injection corpus's own
harness read-and-applied against one shared `&mut Metadata` reference
every call, structurally unable to express "two attempts from two
different snapshots." A hand-scripted repro (not a seed sweep) is what
actually found it. **The fix is structural, not a smarter tie-break**:
make every attempt write at its own unique id
(`animus_cp_data::segment::segment_object_id` — a deterministic prefix
plus a suffix derived from the proposer's node id, its current Raft
term, and a fresh `Rng` draw, so two attempts anywhere, ever, cannot
collide) and make the store itself **write-once** (an identical-content
re-put is a safe no-op; a *differing*-content re-put is a hard error).
With no shared mutable key, there is nothing left for a "better"
adjudication rule to get right — the losing attempt's object is simply a
permanent, harmless orphan, reaped by a dedicated sweep
(`animusd::segment_janitor::reap_orphans`) rather than something a later
`put` ever revisits. **General form, worth checking on any future
"consensus decides X" design**: whenever a committed decision names a
key into some OTHER store (a filesystem path, an object-store key, a
cache entry) that isn't itself inside the consensus log, ask whether two
*independently-computed* decisions for the same logical identity could
ever name the *same* key in that other store — if so, the store's own
overwrite semantics are a second, unguarded decision point consensus
never actually adjudicated. Fixed in `fix/segment-seal-write-once-ledger`
(ADR 0042/0043 as-built amendments); red-then-green repro:
`animus-test`'s `stream_lineage_corpus.rs::
dueling_seals_orphan_hot_range`.
