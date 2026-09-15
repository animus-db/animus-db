# A crash/restart scenario must reuse the SAME `Simulator` instance across the restart — a second `Simulator::new(seed)` is a brand-new, empty simulated world sharing nothing but the RNG seed (C-05 PR 2, `sharedwal_fault_corpus.rs`)

Writing the shared-WAL crash-mid-round corpus cell, the first draft
structured the "reconstruct after crash" phase as a fresh `let sim2 =
Simulator::new(seed);` — reasoning, wrongly, that reusing the same seed
would reproduce the same simulated disk state the first `Simulator` had
accumulated (including the crash's own torn-tail/corruption effects). It
does not: `Simulator::new(seed)` seeds only the deterministic RNG stream
a run *drives itself* with going forward: a fresh `Simulator` starts with
a completely empty in-memory disk, no matter what seed it shares with an
earlier, unrelated `Simulator` value. The bug surfaced immediately and
unambiguously — a write confirmed durable before the "crash" read back as
`None` after "recovery," which looks exactly like a real durability bug
in the mechanism under test until traced back to the test's own fixture
shape.

**Fix**: use one `let mut sim = Simulator::new(seed);` for the whole
scenario. `sim.crash(node)` mutes tasks and tears the node's buffered
(unsynced) disk state per `DiskConfig`'s fault settings; `sim.stop(node)`
removes tasks while preserving durable disk state; `sim.restart(node)`
re-arms tasks and clears the `crashed` flag — all three act on the SAME
simulated disk, so reconstructing fresh `SharedWal`/`RaftKvNode` handles
afterward genuinely recovers from what the crash left behind. This
mirrors a pattern already established in `crates/animus-control/src/
shared_wal.rs`'s own unit tests (e.g.
`survives_two_crash_restart_cycles_with_interleaved_tablets`) — grep for
that shape before writing a new crash/restart scenario rather than
re-deriving it.

**General form**: a `Simulator`'s seed determines its own *future*
random choices, not a snapshot of accumulated *state* — two `Simulator`
values sharing a seed are two independent empty worlds that will make the
same sequence of random decisions if driven identically from scratch,
never two views onto the same disk. A crash/restart scenario needs
exactly one `Simulator` value, mutated in place through `crash`/`stop`/
`restart`, for its whole lifetime.
