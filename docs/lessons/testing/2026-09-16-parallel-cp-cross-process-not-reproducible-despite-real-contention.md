# A wall-clock flake's own reported mechanism must be checked against what the code that path actually runs through can do (issue #627)

Issue #627 reported `crates/animusd/tests/cp_cross_process.rs` failing two of
its three tests, once, under the default parallel test-thread count on a
4-core box: a forwarded write's own retry loop received
`ClientResponse::Error(_)` continuously for the whole 25s budget. The
issue's own "likely mechanism" was the documented port-TOCTOU race
(`support::free_addrs` releases probed ports before rebinding them, so a
sibling test's cluster could in principle steal one).

**That mechanism cannot explain this specific symptom, and the reasoning
generalizes past this one issue.** By the time `bring_up`/`bring_up_deadline`
returns `Ok`, every node in the returned `ClusterConfig` has already
completed a **synchronous** `TcpListener::bind()+listen()` (the same
kernel-semantics argument issue #592's own write-up establishes for
`ECONNREFUSED`, `docs/lessons/general/2026-09-04-a-retry-s-own-justification-must-survive-the-same-kernel.md`)
— nothing can subsequently "steal" a live, already-listening socket; the
only way another process binds that same port is if this test's own node
released it first, which happens only on `shutdown_graceful`. So every
`call()` in the write loop connects to this test's **own** node, always —
confirmed structurally, not just by absence of a repro. A stolen-port
failure would in any case surface as `call()`'s bare
`.expect("connect")` panicking (connection refused/reset), not as a
graceful `ClientResponse::Error` reply from a live peer — and the reported
failure was the latter.

**Reproduction, in order** (all on a 4-core sandbox matching the report):
1. ~40 direct invocations of the built test binary in default-parallel
   mode, unloaded: 0 failures.
2. The same, run concurrently with a second `animusd` integration test
   binary (`cluster_growth`, itself real-disk/real-election heavy) looping
   in the background: 0 failures in `cp_cross_process`, runtime ~1.3–1.4s
   throughout — contention barely registered.
3. `taskset -c 0,1`/`taskset -c 0` (pinning up to 5 concurrent copies — up
   to 90 competing tokio worker threads — onto 1–2 of the 4 cores) plus
   concurrent `cluster_growth` copies: `cp_cross_process` runtime grew
   ~2.5–3x (to 3–3.9s) under this real, measured contention, but never
   failed across 20+ runs. In the **same** batch, `cluster_growth` itself
   genuinely failed (`growth_node_observes_metadata_promptly_via_watch`,
   "write of growth_watch_probe/[107] never committed", 24s) and other runs
   pushed its own wall time to 30s — proof the induced contention was real
   and capable of tripping a genuine timeout in this codebase, just not
   (yet) in `cp_cross_process`.
4. Added real disk-fsync pressure (parallel `dd ... oflag=dsync` loops)
   alongside CPU pinning: no change — `cp_cross_process` stayed in the
   2–2.6s range. This sandbox's disk did not reproduce whatever I/O
   contention the original report's box may have had.
5. Ran via `cargo test -p animusd --test cp_cross_process` itself (not just
   the raw binary) repeatedly, both before and after the fix below: always
   green.

**Outcome**: could not reproduce the reported symptom despite deliberately
inducing contention severe enough to break a *different* test in the same
binary run. The forwarding path this test exercises
(`forward_to_tablet_leader`/`cp_forward`) already carries several rounds of
load-driven hardening from earlier issues (#316, #585, #900 — dead-guess
chasing, hinted-vs-guessed hop timeouts, retrying a merely-slow-but-live
leader) that collectively target exactly this failure class, which may be
why a fresh repro is hard to force. The true mechanism remains
**unconfirmed** — same resolution shape as issue #592's own write-up, which
this investigation followed as a template. Two things shipped anyway,
because they are correct independent of the mechanism:

- **Self-diagnosing error text**: the write loops in all three tests used to
  swallow the `ClientResponse::Error` payload on every retry
  (`ClientResponse::Error(_) => sleep(...)`); they now capture the last
  error and fold it into the timeout panic message, so a future occurrence
  (or a CI occurrence, where re-running locally isn't an option) is
  immediately actionable.
- **`bring_up` migrated to `support::bring_up_deadline`**: this test file
  had never been migrated off the old fixed-16-attempt/50ms bring-up retry
  the shared helper was built to replace (`support/mod.rs`'s own doc on
  `bring_up_deadline`) — the exact `decommission.rs`/`seed_join.rs`/
  `seed_join_allocated.rs`/`cluster_growth.rs` duplication that helper
  already closed for those four files. This is a real, independently
  justified DRY/contention-tolerance improvement to the bring-up phase, not
  a fix for the reported (post-bring-up) write-loop symptom — bring-up was
  never what failed in the reported run (its own distinct panic message
  never appeared in the report). Deliberately **not** touched: the write
  loops' own 25s budget — widening it without a confirmed mechanism is
  exactly the "retry papering over a flake" the green invariant forbids.

**General form**: when a flake's own filed theory names a specific race,
walk the exact code path the observed symptom went through (here: a
`ClientResponse::Error` reply, which requires a live peer connection, which
requires this test's own bind to have already won) before accepting the
theory — a theory that would require the symptom to look different (a
connect panic, not a graceful error reply) doesn't fit no matter how
plausible the general shape ("ports get stolen under parallel contention")
sounds. And when a synthetic repro attempt fails, corroborate that the
contention was real (here: a sibling test in the same batch failing) before
concluding the target test is simply exempt — a negative result is only
solid evidence once you know your induced load could have tripped the bug
if the reported theory were the right one.
