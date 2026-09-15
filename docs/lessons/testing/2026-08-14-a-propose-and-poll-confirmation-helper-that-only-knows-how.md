# A propose-and-poll confirmation helper that only knows how to probe one specific write shape (here, "a `KIND_CHANGE` deletion in the batch") silently stops confirming anything the instant a caller's batch stops containing that shape.

**A propose-and-poll confirmation helper that only knows how to probe one
specific write shape (here, "a `KIND_CHANGE` deletion in the batch") silently
stops confirming anything the instant a caller's batch stops containing that
shape.** `ClientCtx::cp_kind_write_raw`'s original probe searched the batch
for a `KIND_CHANGE` entry with `value: None` and, finding none, returned
`Ok` right after `Accepted` — correct for the old design (every reconcile
batch always deleted at least one record), silently wrong for the ADR 0042
cursor rework (a footprint-only or cursor-only batch has no such entry at
all), which would have left every reconciliation and cursor bump confirmed
by nothing more than "appended to the leader's log locally," reopening
exactly the fence-miss-looks-like-success gap the original probe existed to
close. Fixed by confirming the batch's **last** write generically (`local_get_kind(kind,
key) == expected_value`) instead of searching for one specific shape — sound
because the whole batch is one atomic, whole-or-nothing Raft entry, so any
single write's landed effect proves every other write in the same entry
landed too. The general form: when a confirmation mechanism special-cases
"the shape my one caller happens to produce," a new caller with a
differently-shaped (but equally atomic) batch silently degrades the
confirmation rather than failing loudly — prefer a probe that works for
*any* member of an atomic batch over one keyed to a specific write's
content. (`crates/animusd/src/lib.rs`, `ClientCtx::cp_kind_write_raw`,
2026-08-14.)
