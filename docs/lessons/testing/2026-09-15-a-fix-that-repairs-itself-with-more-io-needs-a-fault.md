# A fix that repairs itself with more I/O needs a fault-injection technique that spares that repair's own I/O (issue #883, `SharedWal::flush`)

Issue #883's fix (`SharedWal::flush`'s `Append` branch, `animus-control::
shared_wal`) closes a hazard where a tolerated `sync` failure left a round's
own already-buffered-by-`append` bytes sitting in the shared WAL file's
un-synced tail, for a completely unrelated, healthy caller's own later,
successful `sync` to durably launder. The fix repairs the file itself,
synchronously, the moment the tolerated `sync` fails: read the file back,
confirm its tail is exactly the bytes this round just appended, and
atomically `Disk::replace` the file with everything before that tail —
before `drive()` can hand the file to the next queued op.

**The first regression attempt reused the existing `DiskConfig::
set_error_prob(1.0)` technique (the one cell (e), issue #838's own
regression, already uses) and got a result that stayed red even with the
fix applied.** The reason: `error_prob` fires uniformly on *every* disk op
by deliberate design ("one shared roll," `DiskConfig`'s own doc) — arming it
before the doomed round's `sync` call also poisons the FIX's OWN repair
`read`/`replace` calls, which run synchronously, with no scheduling
boundary, immediately after the failed `sync` and before the test driver
ever regains control to "heal" the disk. A `SimEnv`/`futures`-executor task
only yields at a real `Poll::Pending`; none of `sync`, `read`, or `replace`
ever produce one (no configured delay applies to them), so the whole
"`sync` fails → read the file back → replace it" sequence is one atomic
burst from the scheduler's perspective. There is no window in which a test
can re-arm the disk config between the failure and the repair — the repair
is not a separate round trip the test can catch mid-flight, it is part of
the SAME physical op's own failure handling.

**The general rule: when a fix's own error-handling path issues *additional*
I/O to repair state (not just report the failure), a fault-injection
technique that fails indiscriminately across every op of that same physical
resource will poison the repair path identically to the original failure,
and a regression built on it will look red-then-still-red instead of
red-then-green — not because the fix is wrong, but because the test cannot
tell "the fix's repair also failed" apart from "the fix doesn't exist."**
Before concluding a fix doesn't work from a stubborn red result, check
whether the SAME armed fault is also hitting the fix's own corrective
calls — `git stash` the fix and confirm the *unfixed* code produces the
identical red assertion; if it's byte-identical either way, the fault
window, not the fix, is suspect.

The resolution here was to add a new, narrowly-scoped `DiskConfig::
set_sync_error_prob` knob (`crates/animus-sim/src/lib.rs`): unlike
`set_error_prob`/`set_enospc_prob`, it applies ONLY to the `sync` op,
leaving a `read`/`replace` issued moments later on the same disk free to
succeed — modeling the realistic case a tolerated-failure teardown race
actually is (a local, one-off `fsync` hazard, not a fully dead disk or
mount). It composes with the pre-existing thresholds on the same shared
roll (`inject_disk_fault`), drawing no extra RNG and changing no behavior
for any config that never sets it, so every pre-existing `DiskConfig`-driven
test and corpus stays byte-for-byte reproducible. **When a fix's own repair
mechanism needs "this op fails but a different, closely-following op on the
same resource succeeds" to be provable at all, that is a sign the fault
model needs a more precise knob, not that the existing uniform one should
be stretched (e.g., via a wider "heal window") to fit a shape it was never
designed to express.**
