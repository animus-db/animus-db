# A one-time "enable this feature" flag set on a driver task's first async line races the very thing it's supposed to gate — set it synchronously, before any task is spawned, or thread it through anything that wholesale replaces the object it lives on.

**A one-time "enable this feature" flag set on a driver task's first async
line races the very thing it's supposed to gate — set it synchronously,
before any task is spawned, or thread it through anything that wholesale
replaces the object it lives on.** ADR 0038 PR2's `animus-control` shadow
mirror (`RaftCore::mirror_capture`, `node.rs`'s `RaftNode::start_with_mirror`)
hit this exactly: the natural-looking `mirror_loop` task's first line was
`core.lock().enable_mirror_capture()`, called *after* `env.spawn_task`, in
a separate async task from `drive()`. Two failure modes stacked: (1) a
scheduling race — `drive()`'s own first poll could process buffered
traffic before `mirror_loop` ever got a turn, silently applying commands
with capture still off; (2) `drive()`'s own WAL-recovery step
(`*core.lock() = RaftCore::recovered(..)`) **replaces the entire core
wholesale** on restart, discarding whatever flag was already set on the
pre-recovery instance — so even fixing (1) by moving the enable call to
before `spawn_task` wasn't sufficient, because the flag lived on an object
that gets thrown away. The fix needed both: enable the flag synchronously
at construction (closes the scheduling race for a fresh start) **and**
read-then-reapply it explicitly across the recovery swap (closes the
restart case). This was only caught because the crash-recovery test
(`mirror_engine.rs::mirror_survives_a_crash_and_resumes_from_the_persisted_
watermark`) asserted full post-restart content equality, not just "the
node came back up" — a weaker assertion (e.g. "some mirror content
exists") would have passed while silently dropping every post-restart
command from the mirror forever. **General rule**: any `RaftCore`-level
opt-in flag/callback a driver attaches must be re-examined against
*every* place the core itself gets rebuilt in-place (`recovered`,
snapshot install, anything doing `*core = ...`), not just the driver's own
spawn-time setup — and a differential/crash test must compare the
**full** rebuilt state, not just liveness, to have a chance of catching
this class of bug.
