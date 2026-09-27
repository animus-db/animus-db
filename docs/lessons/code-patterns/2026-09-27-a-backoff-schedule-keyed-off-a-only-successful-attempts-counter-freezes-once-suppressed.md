# A backoff schedule keyed off a "successful attempts" counter freezes once it starts suppressing

Fixing `animus-control`'s `InstallSnapshot` heartbeat-resend flood (a chunk
resent every heartbeat forever, unbounded in elapsed time for a
permanently-stuck peer — see `animus-control/CLAUDE.md`'s `SnapshotResend::
Backoff` entry) needed an exponential-backoff schedule: resend an
outstanding chunk only when the number of attempts for that offset is `0`
or a power of two.

The obvious implementation reused the field already sitting right there,
`snapshot_chunk_sent`'s `resends` count — "how many times this exact offset
has actually been resent." Wiring the power-of-two check off that counter
directly compiled, passed a quick smoke test, and was wrong: `resends`
only advances on a call that returns `Some` (i.e. actually sends). The very
first time the schedule suppresses a call (attempt count 3, not a power of
two), the counter that decides the NEXT check never moves — the suppressed
call returns early before reaching the increment. The schedule freezes
forever at whatever count it was suppressed at, never reaching the next
allowed power of two. This wasn't a subtle edge case: it broke the peer's
transfer completely (confirmed via a temporary `eprintln!`: stuck printing
`resends_so_far=3` on every single subsequent tick, forever), and it
surfaced immediately as two hard test failures in an unrelated corpus
(`animus-control/tests/control_corpus.rs`'s `chunked_snapshot_*_3` cells)
rather than a timing flake — the transfer genuinely never started.

**The fix**: track a SEPARATE counter for the backoff schedule specifically
— one that advances on every attempt, suppressed or not — distinct from
whatever counter tracks "how many times did this actually go out," which a
`Capped` sibling policy elsewhere legitimately needs to mean something
narrower. Two counters serving two different questions ("should I try
again yet" vs. "how many times did I actually send") must not share one
field just because they're both keyed by the same peer/offset pair and
happen to start at the same value.

**The general lesson**: before gating a periodic retry/backoff decision on
an existing counter, ask whether that counter advances on EVERY decision
point or only on the SUCCESSFUL ones. A counter that only advances on
success cannot, by construction, ever recover from a suppression it caused
— the schedule needs its own "how many times have we been asked" counter,
separate from "how many times did we actually act." This is a variant of
the standing rule on any defer/backoff gate (root `CLAUDE.md`'s
engineering-practices log): "what retires this, on every path that can
make it stale" — here, the missing path was the schedule's own suppressed
branch never feeding back into the value the next decision reads.

Caught here because the crate's own convention (any change to shared
`RaftCore` gates on running the FULL sibling crate's test suite, not just
the one under active development) ran `animus-control`'s own corpus and it
failed hard rather than silently regressing a live cluster's snapshot
catch-up.
