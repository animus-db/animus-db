# A point read's on-demand intent resolution can mask a physical write-side bug from any test that only ever reads via a point `Get` — use a raw physical-storage probe, or at minimum a `Scan`, to prove a resolve actually landed.

**A point read's on-demand intent resolution can mask a physical
write-side bug from any test that only ever reads via a point `Get` —
use a raw physical-storage probe, or at minimum a `Scan`, to prove a
resolve actually landed.** Both `RaftKvNode::local_get` and a
linearizable point read resolve a still-`Pending` intent *at read time*
(`resolve_once_step`/`resolve_decided`) the moment they can determine
the covering transaction's decided status — which they usually can, since
the transaction's own `TxnCommit`/`TxnAbort` record is a separate,
independently-correct write from the per-key resolve this bug breaks.
A test asserting only "the value reads back correctly" after some fix
can pass for a completely different reason than the fix working: the
physical intent never got rewritten to `Committed` at all, and the read
path quietly served the right answer anyway by re-deriving it from the
record every time. Caught while writing this incident's own regression:
an `animus-cp-data` `SimEnv` test asserting on `RaftKvNode::local_get`
showed a misrouted, fence-rejected resolve as "succeeded" (the *read*
came back correct) even though the physical envelope tag was still
provably `Intent`, not `Committed` — the fix was to read the raw stored
bytes directly (`StorageEngine::get`, checking the envelope's leading
tag byte) instead of going through any resolve-aware accessor. A `Scan`
is a partial substitute at the wire level (`resolve_scan_rows` omits a
row whose transaction it cannot determine is decided, rather than
chasing it down) — but only a *foreign* record lookup is genuinely
gated on cross-tablet routing; on a small cluster where every node
happens to host every tablet (see the next entry), even a `Scan` can
still resolve on demand via engine co-location, and only a true raw
physical read is unconditionally trustworthy. (ADR 0018 §2 write-loss
amendment, torn-pair-fix stack PR3, `animus-cp-data/tests/
fenced_commands.rs`, 2026-08-15.)
