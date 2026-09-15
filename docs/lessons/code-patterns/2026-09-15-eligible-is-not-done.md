# `did_work` must mean "state provably changed," never "an eligible branch was entered" — issue #811

The 2026-09-09 note in `docs/engineering-lessons.md` ("A genuine process
restart of a live, fully-caught-up voter inside an otherwise-active 3-node
group pegs one CPU core indefinitely") reported this as root-caused but not
found. It was found this session, in the exact place its own final paragraph
suspected: `apply_and_compact`'s own `did_work` bookkeeping. That earlier
note's "not root-caused" stands as accurate for its own moment — this file
records what it was missing, not a correction of a mistake.

## The bug

`animus-cp-data::apply_and_compact`'s compaction section (and, found while
checking the sibling per this repo's own convention,
`animus-control::meta_apply_and_compact`'s identical section) ended with an
unconditional `did_work = true;` on exiting the whole `if (threshold_hit ||
image_needed) && !halted { .. }` block — regardless of whether anything
*inside* that block actually mutated durable state. `apply_loop`'s only
yield point is `if !did_work { select(ApplyPending, env.sleep(..)).await }`
— a spuriously-true `did_work` skips it every single iteration, forever.

The trigger: a replica that catches up via a completed `InstallSnapshot`
gets its `RaftCore::snapshot_index`/`last_applied` advanced **in memory**
immediately, and (in `animus-cp-data`, not `animus-control` — see below)
the engine's own durable applied-watermark marker advanced **durably** in
the very same install. The WAL rewrite that would make the *core's own*
advance durable happens only later, inside this same compaction block,
gated on `behind >= COMPACT_THRESHOLD` — which is `false` right after an
install (`behind == 0`). If the process genuinely restarts (`stop` + a
fresh `start` on the same durable engine) before that gate next fires,
`RaftCore` recovers from the still-stale, pre-install WAL (old, low
`snapshot_index`/`last_applied`) while `engine_applied` is re-seeded high
from the engine's own already-advanced marker. `snapshot_upto(ea)` clamps
its target to `last_applied`, which is now permanently stuck below `ea`
with nothing to advance it (advancing it requires the *consensus loop*
task to run, and that task can never be scheduled — see below) —
`take_snapshot_dirty()` never returns `true` again, `behind` never
shrinks, and the eligibility gate never closes.

## Why this pinned an entire CPU core, not just one task

The fast (no-op) path through this block has exactly one `.await`
(`wal_lock.lock().await`), which — uncontended, under `SimEnv` — resolves
to `Poll::Ready` on its very first poll, never registering a waker. With
`did_work` wrongly `true`, `apply_loop`'s `loop { .. }` calls
`apply_and_compact` again immediately, with no genuine suspension point
anywhere in between. The whole function, called in that tight loop, never
returns `Poll::Pending` from the *outer* task's `poll()` — a true
Rust-level infinite loop inside one `Future::poll()` call. Since `SimEnv`'s
executor is single-threaded and cooperative, that one `poll()` call never
returning means the executor's own driving loop is stuck inside it
*forever* — not just starving the one task, but every other task in the
whole simulated world (the node's own consensus loop included, which is
what would otherwise have re-advanced `last_applied` and let the gate
close on its own). This is why "the other two replicas stay live" didn't
matter: nothing downstream of the stuck node could ever be serviced again,
because the executor itself never got back to a state where it could poll
anything else.

**The general lesson**: a livelock hidden behind a boolean-returning "did I
make progress" function is only as trustworthy as that function's own
truthfulness at every exit path — including the "I was eligible to try,
but the attempt itself no-op'd" path, which is easy to conflate with "I
tried and it worked" when the code reads top-to-bottom as one `if`
block ending in a single, seemingly-obvious `did_work = true`. Prefer
computing the flag from the concrete artifacts an attempt actually
produced (`bytes.is_some()`, `image.is_some()`, a row count, …) over
setting it once at the *entry* to a block that has more than one internal
early-return.

## Why the control-plane sibling has the same code hazard but a narrower trigger

`animus-control::meta_apply_and_compact`'s `install_syskv_image` does
**not** durably advance the `applied_index_key` watermark the way
`animus-cp-data::install_engine_image` does — so a restart immediately
after an install re-seeds `engine_applied` *low* (matching the stale,
recovered core), never racing ahead of it. The specific, deterministic
InstallSnapshot-then-restart trigger doesn't reproduce here today. But the
identical code shape (`did_work = true` unconditional at the end of the
compaction attempt) is still capable of the same total livelock via a
narrower race: a crash landing between the durable per-commit watermark
write (inside the ordinary ADR 0038 effects-apply `merge_batch`) and the
still-pending WAL rewrite that would have kept `snapshot_index` in sync.
Fixed identically, with a white-box regression that models the
disagreement directly (a never-advanced `RaftCore`, `engine_applied` set
past `SNAPSHOT_THRESHOLD`) rather than chasing the timing window — see
`crates/animus-control/src/node.rs`'s
`apply_and_compact_does_not_spuriously_report_work_when_stuck_below_the_engine_watermark`.
**When one plane's driver copies another's shape (this repo has two:
`animus-cp-data`'s and `animus-control`'s DRIVER_APPLIED apply tasks,
deliberately mirroring each other's structure), a defect found in one is a
reason to re-read the other's *own* version of the same block line by
line — a passing test suite on the sibling proves only that its own
narrower trigger hasn't fired yet, not that the code shape is safe.**

## Testing a spin with no `.await` yield point

`SimEnv`'s own `run_for`/`run_until`/`SimStats.task_polls` are all driven
by the same single-threaded executor the spin itself froze — none of them
can bound or even observe a task whose `poll()` call never returns. The
regression for the end-to-end shape
(`crates/animus-cp-data/tests/restart_caught_up_voter.rs`) drives the whole
scenario on a background `std::thread`, sending its result over an
`mpsc::channel`, and asserts on the **main** test thread's
`Receiver::recv_timeout` instead of anything inside the simulated world.
This turns a reintroduced spin into a clean, bounded test failure (20s
wall-clock, chosen generously above the sub-100ms a healthy pass actually
takes) instead of a hung `cargo test` process — the same asymmetry that
made this bug possible to diagnose live at all only via an external `gdb
-p <pid>`/CPU-percentage observation, never from inside the simulation. A
white-box unit test that can construct the disagreement directly (as the
`animus-control` regression above does) is strictly preferable when the
function under test isn't `pub` and a full end-to-end scenario would need
either an easy deterministic trigger (this crate's own install-then-restart
shape) or a genuine timing race (the control-plane sibling's crash-window
shape, not attempted as an end-to-end test here) — reach for the
background-thread/wall-clock-timeout pattern only when no white-box seam
exists.
