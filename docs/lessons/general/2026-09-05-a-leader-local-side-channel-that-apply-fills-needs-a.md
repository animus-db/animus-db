# A leader-local side channel that apply fills needs a registration step that is provably ordered before apply, not merely likely to be (ADR 0054 step 2, `KindEvalResults`)

Building the leader-local result payload for `KvCommand::KindEval`
(`animus-cp-data`), the first draft registered "this node wants entry N's
payload" in a bounded map **after** `propose_ordered`-style code returned
`ProposeResult::Accepted`, using the freshly-known `index`. That is exactly
the natural place to put it, and it is unsound in general: the apply task is
a *separate*, independently-scheduled consumer of the same `core`'s
committed effects, and under `ProdEnv` it can run on a different OS thread.
Nothing stops it from draining and applying the just-appended entry — and
therefore looking up (and finding nothing in) the interest set — in the gap
between "propose returns" and "the next line registers interest," so a
registration written this way is a race that usually wins, not a guarantee.

The fix costs nothing extra: `propose_ordered`'s own shape already holds a
`core: MutexGuard` across "build the command, call `core.propose`, note the
accepted result" — and the apply task cannot drain a freshly-committed
effect without first acquiring that *same* lock (`apply_and_compact`'s
`core.lock().drain_apply()`). Moving the registration step **inside** that
still-held critical section, before `drop(core)`, converts "usually before"
into "provably before, on every executor" for free — the apply side is
*structurally* unable to observe the entry until the registration's own
lock release happens, no timing assumption required. `RaftKvNode::
propose_kind_eval` does this: `self.kind_eval_results.lock()...register(index)`
runs one statement before the `drop(core)` that ends the section
`propose_ordered` itself already delimits.

The general form: whenever a proposer wants to leave a note for its own
entry's future apply to find, ask whether apply's own path to that entry
shares a lock with the proposer's own critical section — if it does (as it
almost always will for anything routed through `propose_ordered`'s shape),
writing the note inside that section is free correctness a "make it happen
right after accept" version of the same code would only get by luck. The
inverse mistake — registering the note in a *different*, unrelated lock, or
after the shared lock has already been released — reintroduces exactly the
race this pattern exists to close, and a `SimEnv`-only test suite cannot
catch it: `SimEnv`'s single-threaded cooperative scheduler never actually
interleaves the two sides at the vulnerable instant (there is no `.await`
between "propose returns" and "the next synchronous statement runs"), so
the bug is invisible in the deterministic corpus and would only show up as
an intermittent `ProdEnv` flake under real concurrent load — the same
"`SimEnv` proves logic and ordering, not real-thread liveness" class of gap
the root `CLAUDE.md` already names for locks/wakers/group commit, extended
here to "a side-channel handoff between two lock users" as a new instance
of the same family.
