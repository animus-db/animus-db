# `SimEnv`'s cooperative scheduler needs an injected `sync_delay` to make "concurrent" disk writes genuinely overlap — with none configured, one task's whole persist round runs to completion before the executor ever polls a second one (C-05 PR 2, `sharedwal_fault_corpus.rs`)

The shared-WAL coalescing cell (`scenario_a_coalescing`) is meant to prove
that a burst of writes to many hosted tablet groups, issued back-to-back
with no intervening `run_for`, coalesces into far fewer physical
`SharedWal` writes than groups. The first draft showed **zero**
coalescing (16 groups, 16 groups' worth of physical writes) even though
the mechanism under test was already correctly wired. The cause was not
in `SharedWal` at all: `SimEnv`'s disk operations resolve synchronously
(no forced yield) unless a `DiskConfig::set_sync_delay` is configured, so
under the default zero-delay disk, each group's driver task ran its
entire persist round — enqueue, win the internal leader race, flush,
release — to completion before the single-threaded cooperative executor
ever got a chance to poll a *second* group's task. With no two groups'
writes ever pending at the same instant, `SharedWal`'s own coalescing
logic had nothing to coalesce — correct behavior, wrong test setup.

**Fix**: arm `DiskConfig::set_sync_delay(Duration::from_millis(20))`
before issuing the burst. A nonzero delay forces a real virtual-time
suspension inside each `env.append`/`env.sync` call, which lets the
executor advance other groups' tasks and let them enqueue behind the
current "leader" before it finishes its round — after this fix, 16
writes coalesced into 2 physical writes, stably across 8 seeds.

**General form**: a `SimEnv` test whose whole point is to prove
*concurrent* activity coalesces/interacts (group commit, batching, a
race between two async tasks) needs an artificially injected disk delay
(or an equivalent yield point) to make the interleaving happen at all —
otherwise the cooperative single-threaded scheduler silently serializes
what production concurrency would have genuinely overlapped, and the
test proves nothing about the property it was written to check. This
generalizes the "SimEnv proves logic and ordering, not real-thread
liveness" lesson one level further: without a deliberate yield point, it
may not even prove *ordering* under concurrency, because nothing forces
two tasks to actually interleave.
