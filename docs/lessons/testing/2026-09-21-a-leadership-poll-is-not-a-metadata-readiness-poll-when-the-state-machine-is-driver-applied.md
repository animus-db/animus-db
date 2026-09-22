# A leadership poll is not a metadata-readiness poll when the state machine is `DRIVER_APPLIED`: order the apply task's startup seed before the consensus loop's first tick, or every "leader ⇒ its metadata is current" assumption is a restart race

**What happened (issue #1024).** `animusd::watch_metadata::
restarted_control_node_resets_its_ring_and_pre_restart_watchers_fall_back`
went red on `main` twice in two days, each time in ~0.6s — a fast
assertion, not a timeout — while passing on every local rerun, including
80 loops under CPU pressure (six busy loops on four cores, then pinned to
one contended core with a disk-fsync hog). The job log was unreachable
from the fixing session (the log download redirects to a blob-storage
host the sandbox's egress policy denies), so the failing assertion had to
be inferred from timing: the whole pre-long-poll body of that test takes
~0.7s here, so 0.632s lands right after the restarted node's election —
at `assert!(post_restart_watermark >= pre_restart_watermark)`.

**The mechanism.** Since ADR 0038 PR3 `Metadata` is `DRIVER_APPLIED`:
`node.rs`'s consensus loop (`drive`) owns the core and its ticks, and a
separately spawned apply task (`meta_apply_loop`) owns `cache`,
`engine_applied` and the `MetadataWatch` bump. On a restart, `drive` read
the WAL, installed `RaftCore::recovered` (which arms a 150–300ms election
timer), spawned the apply task, and entered its tick loop. The apply
task's first act — the one-time seed: a full system-keyspace engine scan
plus the `_applied_index` read, then publishing `cache`/`engine_applied`/
`watch` — ran concurrently with that timer, and nothing ordered the two.
A single voter whose engine scan lost the race (a CI runner's shared disk
under 267 tests is exactly where a scan stalls for hundreds of
milliseconds) was **leader over `Metadata::default()`, watermark 0**, and
answered `Status` accordingly. `is_control_leader()` reads the core only;
it never implied the seed had run. The remote mirror's `observe()`
already ignores a watermark regression, which is why nothing worse than a
test assertion ever surfaced — but the reconciler and failure-detector
loops read `cache` and act when leader, so a leader over an empty,
pre-seed cache is a genuine mechanism hazard, not a test premise.

**The fix** is an ordering invariant, not a wider poll: `drive` now runs
the apply task's startup seed inline, after installing the recovered
core and before its first tick, then spawns the steady-state apply loop.
A node therefore cannot campaign, vote as a recovered voter, or become
leader before its durable `Metadata` is published — leadership *does*
imply readiness now, so the test's one-shot assertions became the
property being guaranteed rather than a race. If the seed outlives the
election deadline armed at recovery, the first tick simply starts an
election immediately; nothing waits on the seed but the seed.

**Why the local loops never reproduced it.** The losing side of the race
is an engine *scan* on real disk, not CPU: busy loops starve the tokio
workers evenly, so the election timer (also a task) slows down with the
scan. Only I/O latency that hits the scan and not the timer opens the
window — a shared CI disk does that, a local NVMe with a `dd` hog mostly
does not. The deterministic reproduction was therefore a `SimEnv` test
with a delegating `StorageEngine` wrapper that sleeps virtual time inside
`entries()` (`animus-control/tests/restart_seed_before_election.rs`),
which fails on the old ordering at the first `is_leader()` and passes on
the new one, replayable from its seed.

**What to do.**

- When a state machine's visible state is published by a task other than
  the one that decides leadership, write down (and enforce in the boot
  path) which happens first. "Leader" is a consensus fact; "my metadata
  is current" is an apply-task fact. A test — or a production caller —
  that polls the former and reads the latter is asserting an ordering
  the code must actually provide.
- Prefer ordering the seed before the first tick over gating every
  reader: one boot-path `.await` closes the window for `Status`,
  `WatchMetadata`, the reconciler and the detector at once, without
  touching any public signature. `RaftCore::set_state_machine_behind`
  (issue #554) already exists for the *other* trigger of the same hazard
  — the CP data plane's engine loss-and-reopen mid-run — but it was never
  wired for the control plane's boot-time seed, and retrofitting a second
  ad-hoc flag onto leadership would still leave "is the async
  initialization done?" answered by proxy; the boot ordering answers it
  structurally.
- To reproduce an I/O-vs-timer race, inject latency into the I/O
  *specifically* (a delegating engine wrapper sleeping virtual time), not
  into the whole box. CPU pressure slows both sides of the race equally
  and proves nothing either way; 80 green loops under load are not
  evidence the race is absent.
- When a CI log is unreachable, bound the failing assertion by wall
  time: measure how long each phase of the test takes locally, and match
  the reported duration to the phase boundary. Then make every bare
  `assert!` in that test print its observed state so the next occurrence
  needs no inference.
