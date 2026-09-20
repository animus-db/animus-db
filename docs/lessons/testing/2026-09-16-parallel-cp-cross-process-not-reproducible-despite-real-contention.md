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

## Outcome (2026-09-20, PR for #627)

This entry's own "cannot be stolen after bring-up" argument is correct as
far as it goes, but it only reasons about `call()`'s own connect target
**once `bring_up_deadline` has already returned `Ok`** — it says nothing
about the **window before that**, inside a single bring-up attempt itself,
which a follow-up investigation
(`docs/lessons/testing/2026-09-20-allocate-test-ports-by-binding-and-holding-never-probe-and-release.md`)
found carries two real, code-confirmed structural hazards that this entry's
own reasoning never covered:

1. **The `free_addrs` release window itself.** `free_addrs` probed every
   port via a bind-then-release, so the instant it returned, every one of
   those ports was free for *any* process to steal — including a sibling
   test binary's own concurrent `free_addrs` call, or this same fixture's
   own next retry attempt after a partial failure.
2. **Partial-start-then-teardown, with fixed node ids and no cluster
   identity on the wire.** `bring_up_deadline`'s old shape bound and fully
   *started* one node at a time (`run_node`, not just `Node::bind`) — so if
   node *k* failed, nodes `0..k-1` were already live processes with running
   background tasks, torn down only via `shutdown_graceful()` before the
   whole attempt retried with **freshly reallocated** ports. `shutdown_graceful`
   aborts tracked tasks, but `serve_requests` spawns one **untracked**,
   fire-and-forget task per accepted connection (this crate's own
   `handle_connection` gotcha) — a request already mid-flight on such a
   task at the instant of teardown keeps running on the same runtime, still
   holding a live `ClientCtx`/`env` clone, and can still dial out via
   `self.relay`. Every attempt reuses the same fixed node ids
   (`"n0"`, `"n1"`, …), and the raw Raft/`ClientRequest` wire carries **no
   cluster/attempt identity of any kind** — so a survivor from a torn-down
   attempt that happens to dial a port a *later* attempt or a sibling test
   has since bound lands on a live peer with no way for either side to
   detect it came from a different incarnation of the cluster. Issue #627's
   own symptom (a forwarded write receiving a graceful `ClientResponse::
   Error` for the whole 25s budget, from a live peer) fits this shape
   exactly — a live-but-foreign peer answering with a plausible-looking
   refusal, not a connect failure.

**What the fix guarantees**: `bring_up_deadline`/`bring_up_deadline_tls`/
`start_single_node` now bind every node's six listeners on `127.0.0.1:0`
directly via `Node::bind` (OS-assigned at bind time, atomically, never
released) and hold every one of them open until each node itself starts —
binding every node of a cluster *before* starting any of them, so a bind
failure on node *k* leaves **zero** tasks running for **any** node, and no
later step can ever race a still-open port, because nothing in this path
ever releases one. There is no retry loop left to reintroduce either
hazard. See the new lesson file above for the general rule this
generalizes into, and `crates/animusd/src/lib.rs`'s
`start_bound_node_with_streams_quiesce_and_ttl_sweep_interval`/
`run_bound_node` (the new bind/start-split production entry points this
fixture is built on) for the mechanism.

**Reproduction numbers (2026-09-20, same 4-core sandbox, built test binary
run directly in default-parallel mode, `timeout 120` per iteration, the
09-16 contention recipe — `taskset` pinning, two extra looping copies of
the binary, two `dd oflag=dsync` fsync loops)**:

| Phase | Load | Before fix | After fix |
|-------|------|-----------:|----------:|
| A | unloaded, 100 iterations | 0 failures, median 1.34s | 0 failures, median 1.29s |
| B | 2 cores, 3 concurrent copies + fsync, 150 iterations | 0 failures, median 2.27s | 0 failures, median 2.20s |
| C | 1 core, 3 concurrent copies + fsync, 100 iterations | 0 failures, median 3.45s | 0 failures, median 3.23s |
| D | phase-B load, `bring_up_allocation` interleaved 50/50, 100 iterations | — | 0 failures |

So the outer symptom did **not** reproduce before the fix either (350
iterations, contention measurably real: median wall time rose 2.6x from
phase A to C), exactly as this entry's own 09-16 attempt found; the
evidence for the fix is therefore the structural argument above (no
release window can exist; nothing starts before every bind succeeds) plus
the traced hazards, not a red-to-green flip. The after-fix run adds the
new allocator regression test under the same contention (phase D).

