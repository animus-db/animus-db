# A bounded retry against a live producer is a starvation flake by construction — snapshot the point-in-time state instead of retrying until it goes quiet (2026-08-26, `LsmEngine::clone_to`)

The issue #298 fix (immediately above the code this touches, ADR 0058 rung
2/Train 2) closed a correctness hole in `clone_to` — a single `flush()`
call could silently no-op while `applies_in_flight > 0`, permanently
dropping an acked-but-still-memtable-only row — by retrying `flush()` in a
loop (`CLONE_FLUSH_MAX_RETRIES = 1_000`, 1ms apart) until the memtable read
empty. That fix was correct about the *hole* but wrong about the *shape of
the fix*: "retry until X goes quiet" only terminates if X is guaranteed to
go quiet within the retry budget, and nothing here guaranteed that. A
*persistent* concurrent writer — exactly `lsm_clone_concurrent.rs`'s own
regression workload, and a real shape in production (a frozen split
parent's own frozen-tablet exemption still lets consumer-bookkeeping
writes land) — can refill the memtable faster than any bounded number of
flushes drains it, so the loop doesn't just get *slow* under contention, it
has no liveness guarantee **at all**: CI (PR #404, unrelated to this crate)
reported the identical code failing with "memtable still non-empty after
1000 flush retries" on a busier runner, while the same seed had passed
locally days earlier. This is the general shape of a *starvation flake*,
not an infra fluke, and the tell is structural, not statistical: a fixed
retry bound racing an **unbounded** producer will eventually lose on some
runner, no matter how generous the bound — raising it only moves the
flake's probability, never removes it (explicitly avoided here per the
task's own instruction, and worth calling out as a trap: "just retry more"
is the natural first fix to reach for and is never actually a fix for this
shape of race).

**The actual fix drops the goal of "reach quiescence" entirely.** `clone_to`
doesn't need the memtable to ever go empty — it needs to capture every
write ACKED before the call started, and a single point-in-time snapshot
already guarantees that for free: the write path (`log_and_apply`) only
returns to its caller after a record is both WAL-synced and applied to the
memtable **under the same lock** `clone_to` snapshots through, so any
write a caller has already observed as acked is provably present at the
next acquisition of that lock, no matter how the retry-vs-write race plays
out. One best-effort `flush()` (kept purely as an optimization — it
produces the pre-existing pure-SSTable-only clone shape when the source is
quiescent) followed by one atomic snapshot of `(manifest.tables,
memtable-contents)` is sufficient and involves no retrying, no sleeping,
and no dependency on the writer ever pausing. Whatever the snapshot finds
still resident in the memtable is written out as one new SSTable **inside
the clone's own namespace** (never the source's — the fix touches nothing
about the source's manifest/WAL/files beyond the pre-existing hard-link
scheme), sized to exactly that one point-in-time snapshot regardless of
how long or how fast the writer keeps going afterward. Bounded, one-shot
work replaces unbounded-in-principle retrying.

**General rule**: when a fix for "a producer can race my read of shared
state" takes the shape of "retry until the state stops changing," stop and
ask whether the actual correctness contract needs the state to stop
changing at all, or only needs a *consistent snapshot* of it. The retry
loop is usually solving the wrong problem — it's trying to wait out an
unbounded producer instead of taking a well-defined instant of that
producer's output, and any fixed bound put on such a loop is a deferred
flake, not a fix, discoverable only by asking "what happens if the
producer never stops" rather than "what happens in the common case." See
`crates/animus-storage/CLAUDE.md`'s `clone_to` entry for the as-built
mechanism and `crates/animus-storage/tests/lsm_clone_concurrent.rs`'s
`clone_to_completes_under_a_writer_that_never_pauses` for the liveness
regression (reproduces the CI failure directly against the old
bounded-retry code, real multi-thread `ProdEnv`, `SimEnv` cannot reach this
race — see that file's own module doc).
