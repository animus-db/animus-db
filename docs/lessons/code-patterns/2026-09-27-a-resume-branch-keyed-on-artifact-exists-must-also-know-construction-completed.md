# A resume branch keyed on "artifact exists" must also know the artifact's multi-step construction completed

`animus-cp-data::host::Reconciler::materialize_split_child` builds a split
child in two separate steps against its own durable engine: `clone_engine`
(copy the parent's data), then `trim_split_child` (drop the sibling's own
range and the whole CHANGE/CURSOR scopes — a child must never inherit either,
ADR 0046 principle 3). The crash-idempotency contract's resume branch
(`EngineFactory::probe(child.id)` reporting the engine already exists) used
`probe` alone as "already fully materialized" and skipped straight to
reopening and hosting the group — **but `probe` only proves step one
happened**, not step two. A crash, or a genuine `delete_range` failure,
between the two left a durable, `probe`-visible engine that was cloned but
never trimmed, and the resume branch could not tell the difference: it
skipped the trim forever, permanently serving the sibling's rows and leaking
the parent's whole change log/cursors into the child.

The general shape: whenever a resumable multi-step build writes its
intermediate artifact durably before its later steps run, "the artifact
exists" and "the artifact is finished" are two different facts, and a resume
branch keyed on the first is unsound the moment a later step can itself fail
or be interrupted. The fix pairs the existence check with a **second,
step-specific durable marker** written by the *last* step of the sequence
(`trim_marker::trim_marker_key`, mirroring `seal.rs`/`ceiling.rs`'s
engine-marker discipline) — and, crucially, the resume branch's remedy for
"exists but marker absent" (here: re-run the intermediate step) has to be
independently provable safe, not merely convenient. That proof usually rests
on the same sequencing the marker exists to protect: here, the child's own
Raft group can only ever start *after* the marker write succeeds, so an
absent marker proves the group has never run and holds no committed state a
re-run could clobber — a fact worth stating explicitly in the code, not left
implicit.

When auditing a similar resume path elsewhere, ask: does the existence check
this branch trusts actually cover every step between "started" and "safe to
skip," or only the first one?
