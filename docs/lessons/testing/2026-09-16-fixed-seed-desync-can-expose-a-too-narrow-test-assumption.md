# A fixed-seed desync can expose a too-narrow test assumption, not just a real bug

Found bisecting issue #945 (`corpus-deep` red on `reconciler_corpus`,
`torn_tail_crash_restart_replica_recovers_s30`) to PR #907 (issue #900's
boot-time cluster check).

## The trap

The scenario crashes one replica (`c`) with a torn WAL tail, restarts it,
confirms it rejoins as a voter, then does a **fresh** write and asserts the
restarted replica eventually observes it:

```rust
let leader = if ha.is_leader() { &ha } else { &hb };
leader.put(b"post_recovery".to_vec(), b"still_replicates".to_vec());
assert!(wait_until(..., || hc2.local_get(b"post_recovery") == expected), ...);
```

This silently assumes leadership is always on `a` or `b` — never on `c`
itself. Issue #900's fix (an unrelated boot-path change, see the
code-patterns lesson on the same issue) shifted the entropy schedule enough
that, for seed variant `_s30`, leadership landed on `c` — the very replica
the test just finished proving is a fully legitimate voter. `ha.is_leader()`
and `hb.is_leader()` were both `false`, so the `else` branch issued the write
to `hb`, a non-leader, which never committed it — the recovered replica
never saw the write not because it failed to catch up, but because the
write never actually landed anywhere real.

Diagnostic proof: instrumenting the assertion to print `ha`/`hb`/`hc2`'s
`is_leader()` on every poll showed `hc2.is_leader() == true` for the entire
window the test was "waiting" — not a convergence timeout, a wrong target.

## The fix

Check all three replicas, not two:

```rust
let leader = if ha.is_leader() {
    &ha
} else if hb.is_leader() {
    &hb
} else {
    assert!(hc2.is_leader(), "exactly one of a/b/the recovered replica must be leader");
    &hc2
};
```

This is not a seed re-pin (the lesson this generalizes from,
`docs/lessons/testing/2026-09-15-boot-path-entropy-desyncs-fixed-seeds.md`,
covers the case where a re-scan finds a fresh seed for a since-changed
target outcome) — the test's own intent ("a recovered replica keeps
replicating") holds for whichever replica actually leads next, so the
robust fix removes the narrow assumption instead of chasing a new seed that
happens to avoid it.

## The generalizable rule

When a fixed-seed test failure follows an entropy/topology-perturbing
change elsewhere in the boot path (see the sibling lesson), don't stop at
"which replica is leader shifted, so the write went to the wrong one" and
conclude it's just bad luck to re-pin. First check whether the test's own
leader-selection logic was ever actually robust to leadership landing
anywhere valid — a hardcoded `if a { a } else { b }` over a 3+ replica group
is a latent gap regardless of what shifted the odds; the entropy change
just made it visible. Prefer `if a {a} else if b {b} else {assert!(c); c}`
(or a helper that finds the one `is_leader()` replica among an arbitrary
set, mirroring `heartbeat_batch_corpus.rs`'s `leader_index`) over hardcoding
which two of N replicas a scenario "should" pick.
