# When one primitive gains an optimization, check its documented siblings for the identical gap before assuming it's isolated.

**When one primitive gains an optimization, check its documented siblings
for the identical gap before assuming it's isolated.** `ClientCtx::
cp_scan_kind_table` (the LSI `Scan` table-wide fan-out, ADR 0041 §5) never
threaded its caller's `limit` into each tablet's own `KindScan` — it
fetched every overlapping tablet's whole matching sub-range and truncated
once, in the coordinator, after every reply was already in hand. Its
base-scope sibling `cp_scan` had threaded `limit` all the way to
`RaftKvNode::local_scan`/`linearizable_scan` since ADR 0023's original
audit; `cp_scan_kind_table` was added later (ADR 0041 §5) by pattern-
matching `cp_scan`'s *shape* without carrying forward that specific
optimization, and nothing caught it because the two are behaviorally
identical either way — just one wastes wire payload and coordinator
memory on a table whose per-tablet share vastly exceeds a small `Limit`.
A parity gap like this survives review precisely because it's invisible
at the call site and invisible in tests that only check final
correctness, never per-tablet reply size. **Precise wording matters when
fixing it**: this is a *per-tablet cap*, not "pushdown" — `StorageEngine::
scan` has no limit parameter of its own, so a tablet still reads its whole
matching sub-range off the engine; only the wire reply and coordinator
memory shrink. Calling it "pushdown" in a commit message or ADR note
overclaims a reduction in engine I/O that never happened. (ADR 0041 §5
as-built amendment, 2026-08-16.)
