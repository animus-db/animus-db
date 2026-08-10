# Engineering lessons (living — keep this current)

This is the repo's **append-only institutional memory**, moved out of the root
`CLAUDE.md` (which stays a thin, always-loaded entry point) so it can keep
growing without weighing down every session. The standing instruction in the
root `CLAUDE.md` still governs it: whenever you — human or agent — discover a
non-obvious lesson, gotcha, or better way of working *during a task*, **append
a one-line-or-more entry here, with the *why*, in the same change**. Prune or
merge entries that become obsolete; entries whose specific mechanism has since
been deleted or replaced move **verbatim** to
[`engineering-lessons-archive.md`](engineering-lessons-archive.md), leaving a
one-line pointer when the underlying lesson still generalizes.

Read the section relevant to your task before starting work; grep it when
debugging anything that feels like it might have happened before.

### Testing
- **Promoting a per-file test helper (`free_addrs`, `start_single_node`) into
  the shared `tests/support/mod.rs` makes every consumer that doesn't call
  every helper trip `dead_code` under `cargo build`/`clippy -D warnings` — this
  is inherent to the `mod support;`-per-binary-crate structure (each
  `tests/*.rs` file is compiled as its own crate, so per-binary dead-code
  analysis only sees the subset of `support` items *that binary* references),
  not a sign the consolidation is wrong.** `#![allow(dead_code)]` at the top of
  `tests/support/mod.rs` is the standard, correct fix for a shared
  multi-consumer test-support module — cheaper and more honest than adding a
  synthetic use of every helper in every file. Before unifying near-duplicate
  helpers across files (e.g. several `start_single_node` copies that differ
  only in whether they take a `backend: StorageBackend` param, or whether they
  call `run_node` vs `run_node_with` — the latter is just the former with
  `StorageBackend::default()`), diff the actual bodies first: identical
  bodies unify trivially; a narrower shape (fewer params) can be expressed as
  the wider shared shape with an explicit default argument at each narrower
  call site, so unification doesn't require the shared version to be
  polymorphic. (`crates/animusd/tests/support/mod.rs`.)
- **When timing a wake/propagation mechanism, start the clock *after* the
  triggering commit is confirmed, not around the whole write** — otherwise
  the measurement is dominated by unrelated upstream latency (a schema
  commit-wait poll, provisioning a fresh tablet) and the assertion's bound
  has to be loosened to avoid flaking, which quietly defeats the point of
  the test. Porting the ADR 0030 growth-node metadata mirror onto the ADR
  0035 PR5 long-poll `WatchMetadata` mechanism, a first-draft regression test
  started the timer immediately before a `Put` that both provisioned a fresh
  table *and* triggered the mirror update, measuring ~370ms — technically
  under a 600ms bound, but that duration was almost entirely the write's own
  schema-provisioning round trips, not the watch propagation the test
  existed to prove. Moving the clock to start only after the write call
  returned (which already guarantees the triggering commit landed) dropped
  the measured latency to consistently single-digit milliseconds — a
  materially tighter, more honest bound with real teeth against a regression
  to the old fixed-200ms poll, and no closer to flaking under parallel
  test-suite load than the original number was. (`animusd`
  `tests/cluster_growth.rs::growth_node_observes_metadata_promptly_via_watch`.)
- **A long-poll `WatchMetadata` request already in flight to a node at the
  exact moment that node is killed via `Node::shutdown()` does not fail over
  quickly — it can zombie-park for the full server-side timeout before
  replying with stale data, because `Node::shutdown()` doesn't (and, short of
  a larger refactor, can't cheaply) abort an already-spawned per-connection
  handler task.** `serve_clients`'s accept loop spawns each connection's
  `handle_client` as a fire-and-forget `tokio::spawn` with no stored
  `JoinHandle` — `shutdown()` aborts the accept loop itself and the node's two
  internal `Env` role tasks (so no *new* connections are accepted and the
  Raft drivers stop), but an already-accepted connection's handler task is
  untracked and keeps running. Porting the ADR 0030 growth-node mirror onto
  the ADR 0035 PR5 long-poll mechanism, this turned a previously-harmless gap
  into a real, reproducible (3/3) test regression: `ClientCtx::watch_metadata`
  parks on `select! { changed(last_seen), sleep(WATCH_METADATA_SERVER_TIMEOUT) }`
  (8s) — once the driver that would `bump()` that watch is dead, `changed()`
  can never resolve, so the zombie handler always falls through to the
  `sleep` arm and eventually replies with whatever `effective_metadata()`/
  `watch.latest()` the (now-frozen, but still-`Arc`-alive) `RaftCore` held at
  the moment of death — a normal-looking `ClientResponse::Status`, just stale
  by up to 8 seconds. The old fixed-200ms `Status` poll never had this
  hazard (a plain `Status` request replies immediately, no server-side park,
  so a dead node just fails the connect fast every ~200ms tick and the next
  seed is tried in the same tick); the long-poll design's very mechanism (ask
  a node to hold the connection until something changes) is what creates the
  window. `tests/cluster_growth.rs::
  dashboard_health_recovers_after_grown_cluster_loses_an_original_node` had a
  **fixed 3-second sleep** after killing a node before asserting
  leaderless/under-replicated counts — exactly the "fixed post-fault beat,
  not a converged-or-timeout poll" anti-pattern this log already warns
  against for eventual properties, and the ~8s worst case this hazard can now
  add comfortably blew through it. Root-caused by adding temporary
  `eprintln!` tracing to the watch loop (removed before commit) and observing
  an 8.007s-elapsed reply logged right at the moment the test's own kill
  should have been in flight. Fixed the test, not the mechanism: converted
  the fixed sleep + one-shot assertion into a bounded poll (500ms interval,
  30s budget) recomputing the health rollup each iteration — a real fix for
  a real regression, but a fully general one, since **any** fixed-sleep
  assertion following a node kill in this codebase's test suite is exposed to
  the same class of latency once a long-poll mechanism is anywhere in the
  path being waited on. **General checks:** (1) before shipping a long-poll
  primitive, ask whether the harness's own "kill a node" mechanism actually
  frees that node's in-flight server-side handlers, not just its listeners —
  if not, every consumer of the long-poll inherits a worst-case-timeout
  latency hazard on node death that a plain request/reply protocol never had;
  (2) a fixed sleep-then-assert immediately after triggering a fault is
  fragile independent of this specific bug — prefer a bounded poll whenever
  the property being asserted is eventual, especially right after a fault
  injection. (`animusd::watch_metadata`/`remote_metadata_watch_loop`;
  `tests/cluster_growth.rs`.)
- **A cluster-bring-up test helper that gates on `any(is_control_leader)` is
  wrong for a test that restarts a single node of a multi-node cluster** — the
  restarted node rejoins as a follower (the majority never went down), so it
  never reports itself leader and the helper times out waiting for a
  leadership signal that was never the actual readiness condition. A
  single-node cluster's own restart test hides this (a 1-of-1 group is always
  its own leader). For a restart-one-node-of-N test, wait for the node to
  *catch up* instead — poll its admin/Raft view until `last_applied ==
  commit_index && commit_index >= snapshot_index + log_len` (no leadership
  requirement) — which is also the correct replay-completion gate before any
  convergent post-restart assertion. (`animusd` ADR 0029 release-GC restart
  test.)
- **Adding a new heavy multi-node `ProdEnv` integration test raises CPU/IO
  contention on every *other* test binary running in parallel under `cargo
  test`, and a pre-existing hard latency-bound assertion (e.g. a median write
  latency under some millisecond ceiling) can flake purely from that added
  load — no code regression in either test.** Confirm such a failure by
  re-running the victim *in isolation* before treating it as real; a
  release/GC-style loop that is a genuine no-op on a steady cluster (its
  predicate returns empty, then iterates nothing) cannot be the cause. Same
  family as the documented "a flaky ProdEnv test is a real bug" rule, with the
  refinement that a *newly-added* heavy test can itself be the load source —
  so the right move is isolate-and-reconfirm, not loosen the victim's bound.
- **The in-process `--cluster N` shared edge state masks cross-process leader-routing
  gaps — test cross-process paths *per-process*.** In `--cluster N` every node shares
  one `ClusterEdgeState`, so an operation that needs to reach *both* the control
  leader **and** a per-tablet CP-group leader (e.g. the tablet-split trigger:
  `SplitTablet` metadata on the control leader + `propose_split` on the CP leader)
  works from any node, because the shared edge reaches both in-process. **Per-process**
  (one `ClusterEdgeState` each) those two leaderships can sit on *different* nodes, so
  the same call silently fails on every node unless the trigger is forwarded
  cross-process. The split-over-`ProdEnv` and re-host tests therefore drive the split
  *in-process* (`cp_rehost.rs`) and the reconfigure/failure tests run *per-process*
  (`cp_reconfigure.rs`) to exercise the node-local admin views + real failure
  detection. When a path resolves a leader, ask "which leader, and is it the same node
  as the other leader this path needs?" — and add a per-process test if not.
  **Update (ADR 0031 PR2, 2026-08-07): the shared `ClusterEdgeState` root cause
  this entry describes is gone** — `--cluster N`'s in-process bring-up
  (`start_cluster_with`) now creates a distinct edge-state set **per node**,
  exactly like one-process-per-node, and populates `client_route` the same way
  `run_node_with` does, so an in-process node genuinely forwards/relays to
  reach a leader hosted elsewhere rather than finding it locally via a shared
  registry. `--cluster N` and one-process-per-node are now the same code path
  in every way that matters to this class of bug. (`cp_rehost.rs`, referenced
  above as the in-process split test, no longer exists — split is now a
  single control-plane command with no data-plane half to rehost, ADR 0028 —
  but the general lesson stands as a *pattern to watch for*: any future
  process-scoped convenience shortcut (a shared registry, a shared cache, a
  shared claim set) that an in-process multi-node test harness introduces for
  convenience can silently mask the same class of cross-process gap, so audit
  new shared state the same way.) The general "which leader, is it the same
  node" question, and "test cross-process paths per-process," remain sound
  advice for any *new* multi-leader coordination this repo adds.
- **Match a consistency-checker harness to what the layer *offers*; don't shoehorn
  a transactional workload onto a non-transactional layer — build a sibling harness
  that reuses the *checkers*, not the workload.** Adding an Elle corpus for the
  leaderful Raft KV plane (ADR 0017), the obvious move was a `Topology` variant of
  the Accord corpus — but that harness drives **multi-key transactions** and the
  Raft plane is **single-tablet, non-transactional KV** (one key per op), so the
  workload simply doesn't map; forcing it would mean an enum fork through every
  method *and* a workload that misrepresents the plane. Instead a self-contained
  `raftkv_linearizable.rs` reuses just the proven `check_cycles`/durability/
  convergence + `Recorder` model over a single-key list-append workload. And note
  the counter-intuitive soundness: **serializability is a sound, meaningful check
  on a single linearizable Raft group** (not only on Accord) — the group *is* the
  serialization authority, so a forked/stale read shows as a cycle; there's no
  eventually-consistent read path to manufacture torn-read false positives (the
  hazard that bans `check_cycles` on the AP `Frontier`). The teeth-proof
  (`negative_control.rs`) is shared because the checker is.
- **A flaky `ProdEnv` integration test is a real-world bug, not a determinism
  hole — the determinism guarantee (ADR 0003) is `SimEnv`-only.** The `animusd`
  tests run over `ProdEnv` (real sockets/time/threads) and *poll with timeouts,
  not deterministic assertions* — so an intermittent failure there means a
  genuine timing/durability race, exactly the class `SimEnv` can't catch. Debug it
  (don't just bump the timeout): `create_table_survives_node_restart` flaked
  because (a) its post-restart probe raced the Raft **catalog recovery** — gate on
  the recovered artifact (`await_table_schema` polls `has_table_schema`), the
  pattern the sibling GSI test already used; and (b) deeper, the control plane
  **applied + acked a proposal before its WAL was fsynced** (apply-before-fsync),
  so an abrupt teardown lost the acked schema. Both are now fixed — see the
  durable-before-visible pattern below. **A real-time restart test must wait for
  the recovered state and tear down gracefully.**
- **Durable-before-visible: never expose state a crash could lose.** A node must
  not make a committed entry client-visible (readable / ack-returnable) until it is
  fsynced. The control plane enforces this with a `durable_index` watermark the
  driver advances *after* `env.sync(WAL)`, gating `apply`
  (`min(commit_index, durable_index)`) — so `metadata()`, and any proposer waiting
  on it, only sees durable state (ADR 0009; mirrors `animus-data` `ack_durability`
  and `animus-consensus` `persist_then_ship`). Two consequences worth remembering:
  a core/component driven by hand must **simulate the fsync** (advance the
  watermark) or its applied state never moves; and gating *follower* visibility on
  the follower's own fsync **widens cross-node replication races** — a read on a
  follower right after a create on the leader must wait for the definition to
  replicate to that node (`await_table_*`), not assume the leader's ack made it
  visible everywhere.
- **Two independent, un-jittered fixed-period polling loops that can each "win" a one-shot outcome are a real, silent flake source.** (Found in `cp_reconfigure_loop`'s cadence race with `reconcile_loop`; that mechanism is superseded by ADR 0031 PR4 — the reconciler is event-driven now, no cadence to tune. Full entry archived in `docs/engineering-lessons-archive.md`.)
- **Determinism (ADR 0003) proves logic and ordering, not real-thread liveness.**
  `SimEnv` is single-threaded + cooperative, so a `Mutex` guard held across an
  `.await`, a lost waker, or a leader-election/group-commit deadlock can pass
  every sim test and only hang under the real multi-threaded `ProdEnv`. Any
  concurrency primitive (locks, waker handoffs, group commit, leader election)
  needs a **real `#[tokio::test(flavor = "multi_thread")]` over `ProdEnv`,
  timeout-guarded** so a deadlock fails loudly. (Found via the WAL group-commit
  deadlock; pattern in `animus-storage/tests/lsm_concurrent.rs`.)
- **Don't do slow, non-consensus work on the single task that must service Raft
  liveness — a per-loop stall past the election timeout becomes a self-sustaining
  leader-election storm, invisible to `SimEnv`.** The CP-data driver ran engine
  apply (a batch of LSM `merge`s) + compaction *inline* on the same loop as
  `select(recv, timer)`; under bulk-write load that block took ~180–300ms — longer
  than the 150ms election timeout — so the leader couldn't heartbeat and followers
  couldn't process AppendEntries in time → they campaigned → the deposed leader's
  in-flight writes were truncated → those writes hit the 10s client timeout and
  retried → term climbed continuously and throughput collapsed to ~15/s (a fixed
  count, `≈ concurrency / one-election-cycle`, that *looks* like a per-write latency
  floor but is churn). `SimEnv`'s virtual time never trips a wall-clock election
  timeout, so the whole suite was green. Fix: move apply + compaction to a **separate
  task**, leaving the consensus loop to only persist + step + send (→ term flat,
  ~15–20× throughput). Two split invariants worth remembering: (1) once apply is
  async, the core's `last_applied` **leads** the engine, so anything that reads the
  engine after gating on an index (linearizable ReadIndex) must gate on a separate
  *engine-applied* watermark, not `last_applied`; (2) if two tasks write one WAL
  file, serialize them (async lock) and make the compaction rewrite bounded by
  engine progress + discard the other task's pending records (WAL `replay` is
  push-based → duplicates otherwise). **The guard is a `ProdEnv` load test asserting
  the term barely moves under a bulk seed** (`animusd`
  `seed_load_does_not_storm_cp_elections`) — the exact liveness property `SimEnv`
  can't see. Same family as the group-commit-deadlock entry above: real-time,
  timeout/assertion-guarded. (`animus-cp-data` `drive`/`apply_loop`.)
- **`cargo bench -p animus-storage` (real `ProdEnv`) is a smoke test the
  deterministic suite is not** — it surfaced that same deadlock. Run it when
  touching the write/IO path.
- **A property checker only has teeth under the workload that can exercise it.**
  An Elle serializability check over *disjoint keys / single-writer-per-key* is
  near-trivial (no cross-transaction conflicts → no cycles). Point a
  serializability checker at the layer that *claims* it (Accord), drive
  **conflicting** transactions, and include a **negative control** (a known
  non-serializable history the checker must reject) so a passing run means
  something. The AP/LWW data plane should be checked for what it offers
  (read-your-writes, convergence), not serializability.
- **Split assertions by *property class*, not just by layer: safety scales to
  adversarial depth, eventual/liveness properties do not.** Serializability is a
  *safety* property — it must hold on every interleaving, so it is sound to assert
  as a hard check across a deep, fault-heavy, many-seed corpus (it held 7,560/7,560
  in the Elle deep tier). Convergence + durability are *eventual* properties
  (anti-entropy + coordinator retry) — "did it converge within the test's fixed
  post-heal drain?" is only sound on a bounded, non-pathological set. At seed-depth
  a compound fault (`lossy`+`stop_restart`) can legitimately leave convergence in
  flight when the drain ends — observed on **both** the pure-Accord and
  data-plane-frontier topologies (opposite seeds), with **no** safety violation. So
  scale the safety check to depth; keep the eventual checks bounded (or, later,
  give them a *converged-or-timeout* poll instead of a fixed-drain snapshot). A
  fixed-deadline assertion on an eventual property reads as a flaky test, not a
  bug. (ADR 0014 deep-tier findings.)
- **Prefer a frozen, *generated* scenario corpus over a live-randomized test.**
  Generate scenarios (cluster + workload + an explicit fault schedule) with
  randomness for breadth, but **materialize them into a committed, named set** so
  the suite is reproducible and a failure maps to a specific scenario — not a
  one-off RNG state. Aim for structured/combinatorial coverage of the fault
  matrix (fault type × target class × timing × workload); keep bug-finding
  scenarios in the corpus forever as regressions. (Done: ADR 0014 / `animus-test`
  `corpus.rs` — ~119 frozen, name-seeded scenarios over Accord.)
- **For a *true black-box* Elle check, store the datatype and observe it — don't
  reconstruct it from the ordering layer's log.** Reconstructing each read's list
  from `AccordNode::applied_order` (the old register modelling) limits the
  checker's teeth to cross-replica *divergence*: a single globally-agreed but
  non-serializable order can't show as a cycle, because the lists are derived from
  the very order under test. With **arbitrary write values** (ADR 0011) each key
  now stores a real list and reads observe stored bytes
  (`AccordNode::read_value_result`), so `check_cycles` is genuinely black-box
  (`animus-test/tests/support/mod.rs`). Read "final state" straight from stored
  values on **two distinct replicas** (a real cross-replica agreement check), and
  use **single-writer-per-key** so per-key LWW doesn't lose appends — and build
  each append on the client's own authoritative list, not a begin-time quorum read
  (the apply flips `is_applied` before its fire-and-forget data-plane write lands,
  so a begin-time read can be stale and lose the client's own earlier appends).
- **Judge an *eventual* property (convergence, durability) with a converged-or-timeout
  poll, not a fixed-drain snapshot — then it scales to depth like a safety property.**
  A fixed post-heal `run_for(N)` then a one-shot check imposes a false deadline: at
  adversarial seed-depth a compound fault can leave anti-entropy still in flight when
  the drain ends, so the check flakes without revealing a bug — which is why the
  frontier corpus was once pinned to the bounded base set. Instead drive a *bounded*
  poll (`run_for` an increment, re-read, re-check; stop early once it holds) up to a
  generous budget; only budget exhaustion is a genuine failure. Keep it a pure
  function of the seed (`run_for`/`run_until` only, no wall clock). This let
  `frontier_corpus_converges_and_is_durable` scale to the full deep tier. (ADR 0014;
  `animus-test` `support/mod.rs::run_scenario_with`.)
- **A test helper that binds `:0`, reads `local_addr()`, then *drops* the listener
  has a port TOCTOU — retry the (allocate-fresh-ports + start) as a unit.** The
  freed ephemeral port can be stolen by another test binary before the real bind,
  so the subsequent `run_node` rebind fails `AddrInUse` intermittently under
  `cargo test --workspace` (it flaked the `animusd` restart tests' *first* bring-up).
  Wrap the bring-up in a bounded retry that re-allocates fresh ports each attempt
  (`start_single_node` → `(Node, ClusterConfig)`). A same-address **restart** must
  reuse the captured config (it's testing same-address recovery), so it can't
  re-allocate — retry the *rebind in time* instead (the thief is another binary's
  momentary `free_addrs` probe): `tests/support/mod.rs::restart_same_addrs`. The
  window was once "acceptably tiny", but every retried bring-up added to the suite
  raises probe pressure on everyone else — under `--workspace` load the restart
  tests flaked ~2 in 5 full runs until retried. Both retries are bounded, so a
  genuinely occupied port still fails. (`animusd` tests.)
- **A same-address restart test that rebinds N nodes sequentially multiplies
  a single node's port-TOCTOU exposure by N — the fix is a bigger retry
  budget AND fewer nodes, not just one of the two.** Building the ADR 0035
  PR6 full-split-cluster restart test (stop control trio + data fleet, rebind
  every node on its own dir/address, assert recovery), `Address already in
  use` recurred under `cargo test --workspace`-level contention even after
  raising the single-node restart bound (`support::restart_same_addrs`'s 5s)
  to 30s, because this test does the rebind race *five times* in one run
  instead of once. First ruled out a lingering-`TIME_WAIT` explanation by
  checking the vendored `mio` source directly (`mio::net::TcpListener::bind`
  already sets `SO_REUSEADDR`), confirming every failure really was another
  process's live bind on that exact port, not a socket-close-ordering bug in
  `Node::shutdown()`. Fix was two changes together: raise the per-node bound
  further (60s — a full-outage restart is rare enough that patience is
  cheap) *and* shrink the fleet this specific test needs to rebind (one data
  node instead of two — replication/HA across multiple data nodes is
  already covered by other tests in the same file, so this test only needs
  to prove "every process comes back", not "multiple data replicas each
  come back"). Also added a diagnostic (`ss -ltnp`, best-effort) attached to
  the panic message so a *future* recurrence carries forensic evidence
  (who holds the port) instead of just "address already in use" — cheap
  insurance for a flake that is inherently hard to reproduce on demand.
  **General rule: when a bounded-retry mitigation for a known race is
  ported into a test that repeats the racy operation multiple times per
  run, the exposure is multiplicative — widen the bound AND look for a way
  to do the operation fewer times, don't just widen the bound.**
  (`animusd/tests/split_cluster.rs::full_split_cluster_restart_recovers_metadata_and_data`.)
  **Correction (see the next entry): the "not a socket-close-ordering bug in
  `Node::shutdown()`" conclusion above was wrong.** Checking that `mio` sets
  `SO_REUSEADDR` only rules out *TIME_WAIT*-reuse contention; it says nothing
  about a **live** listener still bound by this *same* process, which is
  exactly what a not-yet-unwound aborted task looks like from the outside —
  externally indistinguishable from "another process is squatting on it."
  The 60s bound + smaller fleet made the symptom rare enough to ship, but the
  test kept flaking under `--workspace` load months later until the actual
  mechanism below was found and fixed at the source.
- **`abort()` on a `tokio::task::JoinHandle`/`AbortHandle` only *requests*
  cancellation — it does not wait for the task to stop, and the resources
  that task owns (most importantly, a `TcpListener` it's blocked accepting
  on) are only released once the runtime actually polls and drops it, which
  can lag arbitrarily behind `abort()` returning under CPU contention.**
  This is the real root cause the previous entry's fleet-shrink/bound-raise
  mitigated but didn't fix: `ProdEnv`'s internal accept-loop task owns the
  env's `TcpListener` by value (moved into the `tokio::spawn`ed future), and
  both `ProdEnv::shutdown()` and `animusd::Node::shutdown()` were fire-and-
  forget — call `abort()` on every task, then return immediately, with no
  wait for the cancellation to actually take effect. A same-address restart
  test that calls `shutdown()` and immediately rebinds is thus racing its
  *own* not-yet-dropped listener for the port: under light load the runtime
  polls (and drops) the cancelled task within microseconds, so the race
  window is usually too small to hit; under `cargo test --workspace`-level
  CPU contention (dozens of test binaries and their own worker threads
  fighting for the same cores) that window can stretch for seconds — long
  enough to occasionally outlast even a 60s bounded rebind-retry, because
  every failed rebind attempt in that retry loop was itself contending for
  the same scarce CPU time the cancelled task needed to finally get polled.
  Fixed by adding `ProdEnv::shutdown_and_wait`/`Node::shutdown_and_wait`
  (`crates/animus-env/src/prod.rs`, `crates/animusd/src/lib.rs`): `abort()`
  every task as before, then poll `is_finished()` on each handle (bounded,
  a few seconds) before returning, so the caller only proceeds once the
  listener is *provably* dropped. `Node::shutdown_graceful` — what every
  restart test already calls before rebinding — now ends in this instead of
  the plain hard-abort `shutdown`, so the fix required no test changes.
  **General rule: `abort()` (or any cancellation-request API) is a request,
  not a synchronous guarantee — code that aborts a task and then immediately
  reacquires a resource that task owned (a port, a file lock, a fd) must wait
  for confirmed termination (`is_finished()`/`JoinHandle::await`), not just
  call `abort()` and move on. And when ruling out a race by checking a
  socket option (`SO_REUSEADDR`), be precise about which race it rules out
  (TIME_WAIT-reuse) versus which it says nothing about (a still-live
  listener, in this process or another) — a clean diagnostic that answers
  the wrong question reads as confirmation and can misdirect the next
  person for months.** (`animus-env`, `animusd`;
  `animusd/tests/split_cluster.rs::full_split_cluster_restart_recovers_metadata_and_data`.)
  **Same general rule, a fresh instance (ADR 0038 PR3, ProdEnv liveness
  tests over a real `LsmEngine`)**: a test's teardown calling the plain
  `ProdEnv::shutdown()` (abort-and-return, not `shutdown_and_wait`) then
  immediately `std::fs::remove_dir_all(dir)` can yank a directory out from
  under a still-unaborted background task's in-flight file write — observed
  as the control plane's apply task (`node.rs`'s `meta_apply_and_compact`)
  panicking on `env.replace(WAL, ..).await.expect("wal compaction")` with a
  `NotFound`-class I/O error, logged from a `tokio-rt-worker` thread after
  the foreground test had already reported `ok` (a background-task panic
  doesn't fail the test unless something joins/unwraps that handle). Not a
  new bug introduced by the apply-task split — the same `env.replace(WAL,
  ..)` call already raced identically when it lived inline on `drive()`
  pre-cutover — just newly visible because a `ProdEnv` liveness test now
  exercises a real on-disk engine, and confirmed pre-existing by reproducing
  it with only the *unmodified* `large_metadata_catch_up_stays_live` test
  (`MemoryEngine`-backed, no PR3 code path involved). Left unfixed here
  (per this repo's own "root-cause + fix incidental live bugs as their own
  PR" discipline) — noted as a candidate follow-up: either every `ProdEnv`
  liveness test's teardown should use `shutdown_and_wait` before deleting
  its temp dirs, or `meta_apply_and_compact`'s WAL replace should tolerate a
  torn-directory error the way `animus-cp-data`'s own compaction path does
  (checked against a `halted` flag before asserting) — the latter needs a
  shutdown/halted signal `animus-control::RaftNode` doesn't have yet.
  **Environmental confound noted while debugging this (2026-07):** the day's
  elevated failure rate (3 of 4 full-workspace runs) partly coincided with an
  unrelated long-lived `animusd --cluster-control 3 --cluster-data 5` process
  (started from a developer's own terminal, hours earlier) permanently
  holding ~25 ports in the machine's ephemeral range
  (`/proc/sys/net/ipv4/ip_local_port_range`, ~4096 ports wide here). It can
  never be the *exact* port a test's `free_addrs()` probe collides on (the
  kernel never hands `bind("…:0")` a port that's actually still listening),
  but shrinking the effective ephemeral pool measurably tightens every
  probe-then-drop-then-rebind race described above and in the port-TOCTOU
  entries — more probes chasing fewer numbers means the freed slot a
  `free_addrs()` probe just released is more likely to already be someone
  else's next pick by the time the real bind happens. **Before writing off a
  test-infra flake as purely a code bug, `ss -ltnp` the ephemeral range for
  long-lived squatters** — the fix here was still a real self-inflicted race
  in `shutdown`/`shutdown_and_wait` (confirmed by clean stress runs on this
  same, still-polluted machine after the fix), but the environmental factor
  is real too and worth ruling in/out explicitly rather than silently
  absorbing it into "the test is flaky."
- **Never `let _ = storage.merge(...)` on the write path** — an ack must mean the
  write durably applied; surface storage errors so a non-durable write isn't
  counted toward the quorum (`animus-data` `ack_durability.rs`).
- **A timeout-based failure detector can't tell *slow* from *dead* — its bound is
  load-bearing, and the frozen corpus is what catches an over-aggressive one.** A
  replica watching a transaction it doesn't coordinate only sees *phase* changes,
  not the coordinator slowly gathering a quorum, so "recover after N quiet ticks"
  will recover a live-but-slow (or transiently-partitioned-then-healing)
  coordinator if N is too small. Worse, recovering a transaction that *would* have
  committed re-orders it after every conflict committed meanwhile (Accord recovery
  bumps the timestamp), and where execution is **LWW-by-execution-timestamp** that
  silently loses later same-key writes. The bound must exceed a realistic
  slow-commit / partition-and-heal window; safety also wants the recovered commit
  ballot-fenced so a healed coordinator's late commit can't revert it. An
  over-aggressive 600ms bound passed every targeted consensus test but failed the
  `animus-test` Elle corpus (`wide_write`/`isolate_one`) — **run the frozen corpus
  (`cargo test -p animus-test`) after any change to recovery/execution timing**, it
  exercises interactions a single-feature test never will. (ADR 0011 failure-detector
  slice.)
- **A serializability checker must observe the layer that *claims* serializability,
  not an eventually-consistent projection of it.** The Elle corpus observed Accord
  through the **AP data-plane frontier** (a current quorum read); under a
  data-replica fault a committed multi-key write is acked *before* it is
  quorum-durable (fire-and-forget), so a later read can see one key's new value but
  not the other's — a torn read that `check_cycles` correctly flags as a cycle,
  even though **Accord's order is fine**. The signature is unmistakable: cycle-only
  failures, **never** no-fault, convergence + durability always green. Fix is *not*
  to weaken the checker — point it at the serialization authority (pure Accord:
  local execution + versioned-snapshot reads, `Topology::Authoritative`) and check
  the AP frontier for **convergence + durability** only. (ADR 0014 topology split.)
- **A frozen corpus is broad but shallow — one cell × one seed misses
  schedule-dependent bugs; scale *depth* (seeds/cell), env-gated and tiered.** The
  119-cell corpus explored each structural configuration down a single
  name-hashed interleaving; multiplying seeds per cell
  (`ANIMUS_CORPUS_SEEDS=K`, default 1, nightly 40) is what surfaced the
  frontier-read unsoundness above on the *first* deep run. Keep variant 0 = the
  canonical frozen name+seed (so `K=1` is byte-identical and no regression seed
  moves) and `_sNN`-suffix the rest; gate the cost so default `cargo test` stays at
  the frozen base while the deep tier (`ANIMUS_CORPUS_FULL=1` too) runs in a nightly
  CI job, not per-push. (ADR 0014 coverage-expansion increment.)
- **A harness's client poll granularity bounds which timing windows it can catch —
  a "passing" corpus proves nothing about sub-poll windows.** The 2026-08-06 audit
  confirmed a ReadIndex linearizability hole (a new leader serves reads before its
  current-term no-op commits, ADR 0017 §3) that `raftkv_linearizable.rs` structurally
  cannot fire: the stale window is ~one message round-trip after an election, but the
  client polls at 100ms, so it never samples the sliver — and the single-writer
  re-propose model heals the evidence. When a protocol has a known
  narrow-window rule (ReadIndex no-op, lease expiry, config overlap), write a
  targeted sim test that *drives into the window* (sub-poll granularity, read
  immediately on the new leader), don't rely on corpus luck.
- **An adversarial-verify pass is worth it before acting on audit/review findings —
  and re-verify against the branch you'll edit.** Of the audit's 6 highest-stakes
  claims all 6 confirmed, but two materially changed shape under verification (the
  storage flush bug's trigger is admin flush/compact, not client writes; the
  ~15/s seed throughput was primarily the 50ms confirm-poll cap, not the election
  storm) — and several perf findings were already fixed on `main`, which had moved
  past the audited checkout (pre-vote, single-write-latency, cp-batch-put). A
  finding is (claim × trigger path × branch); verify all three.
- **File reads taken shortly after a branch checkout can transiently disagree
  across tool families (Read/Edit vs Bash) — verify you're actually at HEAD
  before trusting either.** An agent building against `origin/perf/cp-data-
  snapshots-codec` initially saw a stale 1053-line `animus-cp-data/src/lib.rs`
  via `Read` while the true tip was 1482 lines with a materially different
  architecture (split consensus-loop/apply-task, wake-on-propose, a binary wire
  codec) — caught only because a test file referenced methods the "current"
  file didn't have. `git show HEAD:path` gave a third answer on repeated calls.
  Recovery: `git status --short` + `git diff --stat HEAD -- path` both empty is
  the only trustworthy "am I at HEAD" check; for a file where Bash-side
  build/test is the actual gate, `git checkout -- path` + direct Bash
  edits (sed/perl) are safer than Read/Edit if this is suspected. (PR #31.)
- **A `SimEnv` test must never `block_on` an operation that internally polls
  `env.sleep()` (e.g. `linearizable_get`/`linearizable_scan`)** — those only
  resolve while `Simulator::run_for` is advancing virtual time; calling one
  directly under `block_on` hangs forever with no panic, burning wall-clock
  silently. Spawn it as a task and drive it via `sim.run_for` instead (the
  `lin_read`-style helper pattern in `tests/read_index.rs`). (PR #31.)
- **Distinguishing "crash-torn tail" from "mid-file corruption" needs a
  positional proof, not a magnitude heuristic — scan forward for the next
  valid checksummed frame; if one exists, the failure is real corruption.**
  A torn-and-happens-to-look-corrupted tail and genuine mid-file corruption
  can produce equally implausible declared lengths, so "does the length look
  sane" can't tell them apart. The WAL's binary frame decoder resolves a
  parse failure by resyncing forward: tolerate it as a crash-torn tail only
  if NO later valid frame is found in the buffer; otherwise it's a hard
  error. (`wal_resync_point`, PR #32.)
- **A test suite built entirely on bare `block_on` cannot observe a
  `env.spawn_task`-backed background feature — check the harness before
  defaulting a new async-offload feature on.** Storage's tests never drive
  `Simulator::run_for`/`run_until`, so a new "move maintenance to a spawned
  task" feature would silently never run under the existing suite. Shipped
  correctly as additive and default-OFF rather than rewriting the test
  harness to flip it on. Corollary of "SimEnv proves logic, not real-thread
  liveness" — but also a warning to CHECK the harness shape before assuming a
  feature can default on. (PR #32.)
- **Extracting a "pure decision" from a method that intentionally short-circuits
  an expensive call must preserve that laziness explicitly, or the refactor
  silently becomes a hot-path perf regression.** `resolve_cp_route` avoided
  `RaftNode::metadata()`'s full deep-clone on the common "local leader" /
  "known hint" paths by checking cheap facts first; pulling the branching out
  as a pure `decide_cp_route` function required the wrapper to keep gathering
  metadata-derived facts lazily (only in the one branch that needs them)
  rather than eagerly computing everything before calling the pure function.
  When extracting logic mechanically, check what expensive input the original
  short-circuited around, not just what it decided. (PR #33.)
- **Before extracting a flagged "untested pure function," check whether it's
  already a thin call-through to a pure/tested implementation elsewhere.**
  `next_free_tablet_id` looked like animusd's problem (the audit flagged the
  *caller*, `trigger_split`) but the allocator itself was already pure and
  unit-tested in `animus-control::Metadata` — nothing to extract, just a
  caller that wasn't using it (fixed separately in PR #21). (PR #33.)
- **Extending a shared trait's addressing with a new axis: make the primitive
  methods the ones every implementor must write, and re-derive the old surface
  as *default* methods over a well-known constant.** Adding multiplexed
  `(node, stream)` addressing to `Network` (ADR 0026, replacing the
  `Coresident` sibling-pool escape hatch's rationale) needed every existing
  call site (`env.send(to, payload)` / `env.recv()`, nearly the whole
  codebase) to keep compiling and behaving identically. Making `send_stream`/
  `recv_stream` the trait's required methods and `send`/`recv` **default**
  methods that forward to them with a `PRIMARY_STREAM` constant meant the only
  code that had to change was the *three* concrete `Network` implementors
  (`SimEnv`, `ProdEnv`, and one test double) — every caller was untouched,
  because a default method is in scope exactly like a required one once the
  trait is in scope. Grep every `impl <Trait> for` site *before* estimating
  blast radius; it is often far smaller than "everywhere the trait's methods
  are called." (PR #34.)
- **In a worktree session, an absolute-path tool call (Read/Edit/Write) is not
  scoped by the shell's `cd` — pin every path under the worktree root
  explicitly, every time.** A `Bash` `cd /path/to/main/repo && ...` changes the
  *shell's* cwd for subsequent Bash calls, but Read/Edit/Write take literal
  absolute paths and don't care what the shell's cwd is — so it is easy to
  `cd` into the main checkout for one command (e.g. to run cargo from a
  familiar path) and then keep handing Read/Edit/Write paths that *look*
  worktree-rooted but are actually bare `/repo/...` paths resolving into the
  main checkout, silently editing a different working tree than intended.
  The tell was a `git status` on what should have been the worktree suddenly
  reporting the *main repo's* branch name, and a test binary not picking up an
  edit that Read/Edit had just reported succeeding — both mean the tool and the
  build are looking at two different files. Recovery: `git diff` the
  suspect-wrong checkout, confirm which hunks are genuinely new (not
  pre-existing unrelated dirty state) before touching anything, revert only
  those, and re-apply them (a filtered `git apply --include=<path>` off a saved
  patch is faster and safer than re-doing every edit by hand) in the correct
  location. Never `git checkout --`/reset a dirty file without first diffing
  it to confirm every hunk is yours. (PR #34.)
- **A fault-schedule runner that heals immediately after the last fault gives
  single-fault scenarios a zero-length outage — give scenarios an explicit fault
  window.** The raftkv corpus healed partitions the instant the last fault landed,
  so its partition cells were near-vacuous (nothing was ever asked of the cluster
  *while* partitioned). New cells carry `Scenario::window` (outage duration with
  traffic spanning it); old cells keep window 0 for byte-identity. Check any new
  fault harness for this: "did traffic actually run during the fault?" (PR #23.)
- **A "recovery tolerates X" claim must be tested through the NEXT write cycle,
  not just one reopen.** The LSM tolerated a torn WAL tail on replay (skipped the
  torn line) but reused the un-truncated active segment, so the next acked record
  was appended after garbage and a SECOND restart silently dropped it — the
  crash-recovery instance of the "prove recursive ops at depth ≥ 2" rule: recover,
  write, recover again, then assert. (PR #24's fault injection; fix = seal the
  recovered segment.)
- **A test asserting data LOSS can be load-bearing on a consensus bug — when a
  correctness fix flips it, invert the test, don't weaken the fix.** A restart
  test asserted acked data on the memory backend is lost across restart; that
  "expected loss" actually depended on a sole recovered voter never re-advancing
  commit over its WAL tail (a real bug). The ReadIndex-gate fix surfaced it; the
  test now asserts survival via Raft-WAL replay. (PR #25.)
- **When a process-scoped convenience shortcut is removed, grep for tests that
  quietly relied on it to *assert something the removed shortcut made trivially
  true* — not just tests that time out.** Making `--cluster N`'s in-process
  `ClusterEdgeState` per-node (ADR 0031 PR2, closing the gotcha above) broke
  exactly one of ~90 `animusd` tests: `cql_wire.rs`'s cross-connection
  `EXECUTE` assertion, which `PREPARE`d a statement via node 0 then
  `EXECUTE`d it via a connection to node 1 to "prove the prepared store is
  shared across connections" — true only because the old shared edge made
  every node's `CqlState` the same object. Per-node, that's not a bug to fix,
  it's the **correct, intended new behavior** (a real one-process-per-node
  deployment never shared this either) — so the honest fix is to change what
  the test proves: reuse a **second connection to the same node** (`conn0b`)
  for the cross-connection assertion, and keep the cross-*node* connection
  for what's actually still cross-node-safe (reading committed CP-plane
  data). The signature to watch for isn't a hang/timeout (this failed with a
  clean, immediate `Error` response) — it's an assertion whose comment
  literally describes the removed shortcut's own guarantee ("shared across
  connections/nodes/processes"); grep test comments for the word "shared" (or
  "cluster-wide", "any node") near the specific state you're scoping down,
  not just the obvious call sites. Every other test that exercised
  cross-node behavior already did so through a *real* mechanism (replicated
  `Metadata`, `cp_route` forwarding, `propose_schema` relay), so removing the
  shortcut made those tests exercise more real code, not less — 100% of the
  rest of the workspace suite passed unmodified, including several
  (`cp_plane.rs`, `cp_rebalance.rs`, `cp_reconfigure.rs`) that now genuinely
  drive cross-process-style forwarding in-process for the first time instead
  of resolving everything locally through the shared registry. (`animusd`
  `tests/cql_wire.rs::cql_wire_prepare_execute_typed_round_trip`.)
- **A test that hand-drives a real Raft transfer/membership change must retry
  the whole arm/propose sequence on a poll tick, never assert success on one
  attempt — even when the immediately-preceding `is_leader()` check was
  synchronous and just returned true.** Building the ADR 0031 PR5 reconciler
  lifecycle corpus, two hand-rolled "force a real membership removal" test
  helpers passed every run at low seed depth and then hit
  `NotLeader{leader: Some(<the exact node just confirmed as leader>)}` from
  `change_membership`/`transfer_leadership` at `ANIMUS_RECONCILER_SEEDS=60`
  and `=150` — a real, already-documented core behavior (`propose`/
  `change_membership` **freeze**, returning `NotLeader` with the *transfer
  target* as the "leader" hint, while a leadership transfer is armed
  elsewhere in the group; see this file's "two-layer gate" entry) that a
  single-shot assert cannot distinguish from a genuine failure. No amount of
  sleeping between the `is_leader()` check and the propose call closes this,
  because the freeze can arm *after* the check — the fix is to fold the
  whole "check → act" sequence into the body of a bounded retry poll (`check
  condition; if not met, attempt the action; return false; poll again`) and
  only fail once the bound is exhausted, exactly like every production retry
  loop in this codebase already must (`ProposeResult::Accepted` isn't
  `committed`, and `NotLeader` isn't necessarily permanent). This is the same
  discipline as the standing "a retry loop over a Raft write must distinguish
  never-accepted from accepted-unconfirmed" entry, just showing up inside a
  *test's* orchestration code instead of production code — seed depth is what
  surfaced it, at low depth every run happened to avoid the race window.
  (`animus-cp-data/tests/reconciler_corpus.rs::remove_replica_for_real`,
  `scenario_partition_blocks_release`.)
- **`RaftKvNode::linearizable_get`/`linearizable_scan` only ever serve on the
  confirmed leader — calling them on a follower returns `None` unconditionally
  (the ReadIndex ban unconditionally fails for a non-leader), not a slow or
  stale read.** A test that wants to confirm a write *replicated* to a
  follower (as opposed to confirming linearizability) must read that
  follower with `local_get` (a raw, non-linearizable engine read), not
  `linearizable_get` — calling the latter on whichever handle isn't currently
  leading is not "testing the follower," it's testing a guaranteed `None`,
  and asserting `Some(value)` against it fails deterministically regardless
  of how long you wait. Caught immediately (first run) by the ADR 0031 PR5
  reconciler corpus's 2-replica scenarios asserting both replicas' handles
  via `linearizable_get` — fixed by reading the leader linearizably and
  polling the follower with `local_get`.
- **Making a simulator/executor handle `Clone`-able (when its fields are
  already `Arc`-backed shared state) is a small, safe, additive change worth
  making the moment a test needs to carry fault-injection capability *into* a
  spawned async task** — don't route around the missing `Clone` with an
  awkward workaround (a channel back to the outer synchronous scope, a
  second parallel handle type, restructuring every scenario to interleave
  fault injection from the outside). `animus-sim::Simulator` held only an
  `Arc<Shared>` + a `u64` seed, had no `Drop`, and its per-node handle
  (`SimEnv`) was already `Clone` for exactly this reason — so adding
  `#[derive(Clone)]` to `Simulator` itself cost nothing and immediately
  unblocked a harness where each scenario's own spawned "driver" task needs
  to call `&self` fault methods (`stop`/`crash`/`partition_pair`/`heal`/
  `env`) while the outer test thread keeps a separate handle for the `&mut
  self` `run_for`/`run_until` driving loop. Check for a `Drop` impl and
  whether every field is itself cheaply `Clone`-able before assuming a type
  wasn't made `Clone` for a real reason — here it clearly wasn't, it just
  hadn't been needed yet. (`animus-sim::Simulator`;
  `animus-cp-data/tests/reconciler_corpus.rs`.)
- **The rebalancer converges the *global* imbalance to `max − min ≤ 1` and
  stops — it makes no per-table promise, so a test must not route an op
  through ONLY a just-grown node for an *arbitrary* table.** Building the ADR
  0032 PR2 seed/join test, "the joined node hosts a replica of *some* tablet"
  is the stable rebalancing signal, but writing through only that node's
  client address for a table it does *not* replicate flakes bimodally
  (~40%): `resolve_cp_route`'s no-local-replica branch forwards blindly to
  *some known replica* of the tablet — not its leader — and the receiving
  `cp_serve_forwarded` never re-forwards (routing is bounded to one hop), so
  a forward that lands on a follower errors "not the leader here" on every
  retry with the same first-listed replica. Two sound test shapes: gate on
  the *specific table* the node actually replicates (poll `/admin/status`'s
  per-tablet `table` + `replicas` and pick that table for the
  through-only-this-node ops), or give the client every node's address
  (`cluster_growth.rs`'s round-robin `put`). The one-hop-blind-forward
  behavior itself is a known production shape (the client is expected to
  retry with fresh routing), not a bug this test should have papered over
  with a longer timeout. (`animusd` `tests/seed_join.rs::table_with_replica`.)
  **Generalizes beyond "just-grown node + arbitrary table" (ADR 0035 PR5):
  ANY node with zero local replicas of anything hits the identical
  fixed-non-leader-pick flake** — a control-only node (ADR 0035 PR3/PR4)
  *structurally* never has a replica of any tablet, so a test asserting a
  `Put`/`Get` succeeds through one fixed control node's client address alone
  flakes on whichever of the tablet's replicas happens to win that
  particular Raft election (a genuine ~50/50 for RF=2, not tied to growth/
  rebalancing timing at all). Same fix: round-robin across every node's
  client address, control **and** data, so the round-robin is guaranteed to
  hit a node that resolves correctly (a real replica) even when the
  control-node leg of the same loop lands on the wrong pick.
  (`animusd` `tests/data_only.rs::split_cluster_serves_reads_and_writes_across_data_nodes`.)
  **Update (hinted-retry forwarding, closes the hazard): `ClientCtx::cp_forward`
  is now the single choke point every CP forward call goes through, and it
  retries.** A "not the leader here" refusal (`cp_serve_forwarded`) now
  carries the refusing (replica-hosting) node's own leader hint —
  `topology::format_not_leader_refusal`/`parse_not_leader_refusal`, a plain
  string suffix so old and new binaries still interoperate — and `cp_forward`
  chases it: retry at the hint's address if untried, else at another of the
  tablet's known replicas, bounded to one pass over {hint} ∪ replicas and to
  the existing per-hop `CLIENT_TIMEOUT` budget for the whole sequence (not
  per attempt). The one-hop invariant is unchanged — only the *forwarder*
  retries, the receiver still never re-forwards. A node with zero local
  replicas now resolves deterministically through a single fixed address, so
  the round-robin test crutches above are no longer needed for *this*
  hazard specifically (`tests/data_only.rs`/`tests/cluster_split.rs` reverted
  to a fixed control-only node's address; `tests/cluster_split.rs::
  fixed_control_node_write_read_is_deterministic` is the focused regression).
  **Second update (user-hit live, one release later): a bounded retry pass
  over {hint} ∪ replicas closes "wrong replica" but not "no leader YET" —
  when every candidate refuses `leader_hint=none` (the whole group is
  mid-election: a split-child/first-provision formation window, or a crashed
  leader), giving up the moment one pass exhausts surfaces the refusal to
  the client even though the election resolves within a couple hundred ms
  and the deadline budget is barely touched.** `cp_forward` now backs off
  `FORWARD_ELECTION_BACKOFF` (~one election timeout) and re-runs the pass,
  still hard-bounded by the same overall `CLIENT_TIMEOUT` — the forwarded
  dual of the local path's `RouteDecision::Wait`, which already waited out
  its own group's election for exactly this reason. Gated on the tablet
  being resolvable so an unmappable op keeps failing fast. General check
  for any bounded retry-over-candidates loop: "every candidate refused" has
  two distinct causes — *wrong candidates* (retrying the same set is
  useless, return) vs. *right candidates, transient state* (they'll succeed
  shortly, wait and re-ask) — and a loop that only handles the first
  converts every instance of the second into a spurious client-visible
  error. (`tests/cluster_split.rs::
  single_shot_first_write_through_control_node_succeeds` — ONE un-retried
  Put racing the provisioning/formation window.)
  The general lesson stands for any *other* future one-hop-forward gap this
  pattern doesn't cover.
- **Adding an automatic background registration/bring-up step makes any
  test's "not yet registered" pre-assertion a race, not an invariant — sweep
  for assertions on the *absence* of state the new automation now
  establishes.** Folding growth-node membership self-registration into
  `start_with` (ADR 0032 PR2) broke `cluster_growth.rs`'s sanity check that
  a freshly-started growth node "should not be a member before admin-add" —
  intermittently (the self-registration + heartbeat promotion can complete
  before the test's first poll, or not), the worst kind of breakage. The
  dual of the documented "removed shortcut → grep for tests that relied on
  it" lesson: an *added* automation invalidates assertions about the
  pre-automation quiescent state. The honest fix is to delete the stale
  pre-assertion and let the convergent post-state assertion (it *does*
  become `Active`) carry the proof. (`animusd` `tests/cluster_growth.rs`.)
- **An ADR's prose is not the source of truth for whether a gap still exists —
  grep the actual code (and the test tree) for the mechanism before implementing
  the fix it describes.** Assigned to close ADR 0013's "cross-process schema-DDL
  proposal forwarding is future work" gap, a `CLAUDE.md` engineering-practices
  scan (the `is_relayable_command`/`propose_schema` entries already documented
  above) plus a direct grep turned up that the relay (`ClientCtx::propose_schema`
  — propose locally on the leader, else relay `ClientRequest::ProposeSchema` one
  hop, with a broadcast fallback for a leader-less ADR 0030 growth node), the
  gating allowlist, and a **dedicated per-process regression test**
  (`animusd/tests/schema_ddl_relay.rs`, proving `CreateTableSchema`/
  `SetTableMode`/the atomic `ReplaceTableSchema` all commit via a
  follower-connected node) were already implemented and merged — the ADR text
  had simply never been updated after the feature landed (probably in the same
  PR that added `is_relayable_command` itself, under a different task name that
  didn't reference ADR 0013 by number). No code changes were needed; the actual
  work was updating ADR 0013's Decision/Consequences to state reality (it also
  had two *other* stale "future work" claims in the same document — CQL keyspace
  replication and atomic `ALTER TABLE`, both also already shipped — found only
  by cross-checking every claim in the file, not just the one paragraph the task
  named). **General check before starting any "close gap X" task: does the
  mechanism already exist? A stale ADR describing a gap as open is itself a bug
  to fix (a doc PR), and shipping a redundant/parallel implementation on top of
  an already-working one would be worse than doing nothing.**
- **The mirror image also happens: an ADR's "out of scope, too large a
  refactor" call is a decision made against the codebase *as it existed
  then* — recheck it against what has actually shipped since, don't treat it
  as permanent.** ADR 0030 §3 evaluated "a data node with no control role at
  all" and rejected it as not viable "without a much larger refactor than
  this slice warrants," because at the time `BoundNode::start_with` had a
  hard structural requirement (every node owns a local `RaftCore`) with no
  seam to route around it. Two ADRs later (0030's own growth-node
  remote-metadata mirror, then ADR 0032's join/address-book work), the exact
  mechanism that claim was missing had already been built and proven correct
  in production — for a narrower reason each time (letting a non-voter
  observe `Metadata` at all; keeping addresses live as the cluster grows) —
  so designing ADR 0035 (control plane as a separate deployment) was mostly
  recognizing that the "much larger refactor" had already happened
  piecemeal, not writing it from scratch. **When starting any task, check
  whether a prior ADR already declared a closely related shape out of scope —
  if so, re-derive whether that scope decision still holds given every
  feature that has shipped since, rather than treating the old ADR's "not
  viable" as a permanent wall.**
- **The CP write-forward path has no retry-on-"not the leader here," unlike
  the read path — a test that forwards to a group which hasn't finished
  hosting/electing yet must retry client-side, or it flakes on a real,
  pre-existing timing window, not a bug the test introduced.**
  `ClientCtx::cp_read`/`cp_scan_one` retry internally on the `"; retry"`-class
  error shape (`read_should_retry`), but `cp_write`'s `Forward` branch
  (`ClientCtx::cp_write`) does not — it returns whatever the forwarded node's
  `cp_serve_forwarded` answers verbatim, including a clean, non-`"; retry"`
  "not the leader here" if the forward lands before the receiving node's own
  tablet-host reconciler has stood the freshly-provisioned tablet's group up
  and elected. `cp_route`'s own doc says the client is expected to retry with
  fresh routing on exactly this shape, so it is a documented contract, not an
  oversight — but it means a **first write right after provisioning a fresh
  table**, forwarded to a node whose reconciler hasn't caught up yet, can
  legitimately fail once. The window is usually sub-millisecond in combined
  mode (the reconciler reacts to an event-driven `metadata_watch` wake on the
  same node that just committed the tablet), but widens to the reconciler's
  500ms fallback-poll interval on any node reached only through a mirrored
  `Metadata` view (an ADR 0030 growth node, or an ADR 0035 control-only
  node's forward target) — exactly the shape a control-only-cluster
  integration test is likely to exercise for the first time. Write such a
  test's first-write assertion as a bounded retry poll (`loop { put; if ok
  return; sleep; }` under a `timeout`), not a bare one-shot assert — the same
  "converged-or-timeout, not a fixed one-shot" discipline this file already
  documents for eventual properties, just showing up on the write path this
  time. (ADR 0035 PR3 `tests/control_only.rs`'s mixed-cluster test, caught on
  the very first run.)
- **`tokio::join!` over two bare `&self` teardown calls (e.g.
  `Node::shutdown_graceful`) is the right way to manufacture a genuinely
  SIMULTANEOUS multi-fault scenario in a `ProdEnv` test — sequential kills,
  even back-to-back with no `sleep` between them, understate overlap,
  because the first call's async teardown can fully finish before the
  second one's even starts.** Building the ADR 0035 multi-fault chaos tests
  (control-leader failover + a data-node failure at the same instant;
  `crates/animusd/tests/split_cluster.rs`), `shutdown_graceful` takes `&self`,
  so `tokio::join!(control_nodes[i].shutdown_graceful(),
  data_nodes[j].shutdown_graceful())` races both teardowns on the executor
  instead of resolving one before starting the other — the same
  "concurrent, not sequential" property `tokio::join!` already gives any pair
  of independent futures. **A related, less obvious point found while
  designing the crossover-racing sibling test** (a tablet split triggered at
  the same time as a decommission/drain of one of that tablet's current
  replicas): `admin_drain` (`ClientCtx::admin_drain`) only proposes
  `UpsertMember{status: Leaving}` and returns — it does **not** itself touch
  any tablet's replica set. The actual replica evacuation happens
  asynchronously, on the placement reconciler's own event-driven schedule,
  entirely decoupled from when the admin call returns. So a test wanting to
  exercise "a drain landing mid-split-crossover" does not need
  millisecond-precise interleaving between the two admin RPCs — firing both
  via `tokio::join!` is sufficient, because the actual race (the split's
  freshly-minted child inheriting the parent's about-to-be-evacuated replica
  set) is created by the reconciler's background convergence timing, not by
  the order the two admin calls happen to land in. Both new tests passed on
  their very first run and stayed stable across 5+ isolated re-runs plus
  repeated full-file runs — no product bug surfaced (the fences/reconciler
  work ADR 0028/0031/0033 already built handled the crossover correctly).
- **A single-server `change_membership` delta is computed against the
  *current* config, not the original one — so a second growth step must add
  relative to the config the first step already produced, never restate the
  original set with one more id swapped in.** Writing
  `animus-control/tests/control_membership.rs`'s "gate reopens after commit"
  case (PR1 of the control-plane membership-change stack), growing
  `{0,1,2} -> {0,1,2,3}` and then, after it committed, trying
  `{0,1,2,3} -> {0,1,2,4}` (drop 3, add 4) was rejected as a *multi-server*
  delta (`symmetric_difference` = `{3,4}`, count 2) — correct behavior, wrong
  test expectation. The fix was `{0,1,2,3} -> {0,1,2,3,4}` (append, don't
  swap). `RaftCore::change_membership`'s `delta != 1` check is symmetric
  difference against `self.config` (whatever the latest log entry set it
  to), never the group's *original* `all_nodes` — a caller chaining several
  growth/shrink steps must always diff against the *current* `config()`
  right before each call, not a value computed once up front.
- **A real `#[tokio::test(multi_thread)]` ProdEnv liveness test (`animus-
  control/tests/prod_liveness.rs::large_metadata_catch_up_stays_live`) can
  fail under `cargo test --workspace`'s full parallel run while passing
  instantly (both before and after the same code change) in isolation
  (`cargo test -p animus-control --test prod_liveness`) — pure CPU/thread
  contention from dozens of concurrently-running test binaries starving its
  real-time catch-up budget, not a regression.** Confirmed by running the
  isolated binary against both the working tree and a `git stash`-clean
  checkout of the same commit: both pass in ~2s solo. Per the root
  `CLAUDE.md`'s "a flaky ProdEnv test is a real bug, don't bump the
  timeout" rule, the right response to a `--workspace`-only failure in a
  test that is unrelated to your diff is to **reproduce it in isolation
  first** — if it's solid alone (and, ideally, also solid alone on the
  pre-change commit), it's machine-load flakiness in the *harness*, not a
  logic bug your change introduced, and the fix (if any) belongs in that
  test's real-time budget/CI parallelism, not in the unrelated change you're
  landing.
- **A real leadership-transfer end-to-end test (`transfer_leadership` +
  `TimeoutNow` over real `ProdEnv`/tokio) must poll-and-retry the *whole
  operation* against "whoever is leader now", not assert a single
  deterministic hand-off to a pre-picked target.** Writing the ADR 0037 PR3
  self-removal test (leader removes its own control-voter slot, which arms a
  transfer to the other remaining voter first), a first draft asserted
  "exactly one specific other node becomes leader within N seconds" and hit a
  real, reproducible stall under `cargo test`'s real scheduling: the old
  leader's own step-down (satisfying the *server-side* 5s poll inside the
  admin action) and the *test's* separate poll for "some other node is now
  leader" can straddle a transient flip-flop (the target wins via
  `TimeoutNow`, but the old leader's election timer also fires and it wins a
  subsequent term back) that a 100ms-granularity poll can miss entirely,
  leaving the test waiting on a leader identity that already changed again.
  The robust shape (and the more realistic one — this is what an operator's
  own retry already has to do) is a bounded loop that re-checks who is
  currently leader among the surviving voters on every iteration and retries
  the mutating call there, rather than snapshotting a target once. See
  `crates/animusd/tests/control_membership_admin.rs::
  remove_control_voter_refusals_transfer_and_quorum_warnings`.
- **A failure-detector/liveness accessor keyed to one id space (raftkv ids)
  will silently return a wrong-but-plausible answer if called with an id
  from a *different* id space (control ids) — it never panics, it just lies.**
  `ControlHandle::believes_alive`/the underlying `FailureDetector` only ever
  observes heartbeats from **raftkv** ids (`heartbeat_loop` runs on the data
  role only, ADR 0012); calling it with a **control** id (as a first draft of
  the ADR 0037 PR3 quorum-loss warning did, checking "are all other control
  voters believed Down") returns `false` unconditionally for every control
  id, not "unknown" — so the warning fired on *every* removal, not just the
  risky ones, and a naive test would have "passed" by coincidence (the
  warning was expected on the specific case being tested) while being wrong
  in general. Caught by testing the *negative* case too (a removal that
  should carry no warning) and getting one anyway. The fix was to drop the
  liveness-based trigger entirely rather than bridge id spaces by convention
  (`RAFTKV_ID_BASE + control_id` is a *naming* convention for combined-mode,
  not a structural guarantee for an operator-chosen or control-only id) —
  when there is no real signal in the id space you actually have, don't
  guess one from a different id space's accessor just because it type-checks.
- **A "resulting count" quorum-loss guard only catches the failure mode where
  the node being removed is the *only* thing that changed — it is blind to
  "a different survivor was already dead before this call."** Writing ADR
  0037 PR5's end-to-end failure-case sweep, the shipped `admin_remove_control_
  member` guard (refuse `< 1` remaining voters, warn at exactly `1`) looks
  complete because every test up to that point only ever killed *the node
  being removed itself* — so "how many voters remain" and "how much fault
  tolerance remains" always agreed. The gap only appears once a *different*
  voter is already dead when the removal happens: going from an odd-sized
  group (majority tolerates one failure) to an even-sized one (majority
  tolerates none) while that other voter stays down can leave 2-or-more
  *counted* voters with only 1 *functioning* one — a resulting count of 2
  looks safe to a count-only guard and gets no warning at all, yet the group
  is now permanently wedged (the removal's own config-change entry can never
  itself commit, so no further membership change can ever succeed either).
  Proven directly, not just argued in prose, by
  `animus-control/tests/control_membership.rs::
  removing_a_live_voter_while_a_third_is_already_dead_can_strand_the_group`
  (core level: a stranded 2-voter config with one dead never commits
  anything again) and `animusd/tests/control_membership_admin.rs::
  removing_a_live_voter_while_another_is_already_dead_can_silently_strand_
  the_group` (through the real admin path: the removal succeeds with no
  warning, then every subsequent membership-change attempt fails with
  "already in flight," forever). **Any "how many are left" quorum check
  needs its own explicit test for "one of the OTHER survivors is already
  gone," not just "how many remain after this one action"** — a test suite
  that only ever removes/kills one node at a time will pass a guard that
  still ships a silent stranding hazard. Recorded in ADR 0037's Consequences
  section as a knowingly-accepted risk rather than a defect, since fixing it
  for real needs the raftkv/control id-space unification the previous entry
  above already explains why PR3 didn't attempt.
- **Testing "does `--ephemeral` truly mean a clean slate" needs a fresh
  directory per incarnation, not just flipping the backend flag on a
  same-dir restart — same-dir + `--ephemeral` is not the same claim as
  same-dir + durable, and asserting the wrong one gives a false pass.**
  Writing ADR 0038 PR4's control-plane restart matrix, the naive test for
  "`--ephemeral` control-only restart loses `Metadata`" was: propose a
  schema, hard-shutdown, restart on the *same* dir with `StorageBackend::
  Memory` again, assert the schema is gone. That assertion is false for a
  small scenario (a handful of proposals): the control Raft's own `raft.wal`
  is always real `ProdEnv` disk I/O regardless of the system-keyspace
  engine's backend choice (`animusd/CLAUDE.md`'s pre-existing "`--ephemeral`
  does NOT make the control/raftkv WALs ephemeral" gotcha, previously only
  documented for the data plane), and compaction is gated on a threshold
  (`SNAPSHOT_THRESHOLD = 64` uncompacted commands, or a follower genuinely
  needing an `InstallSnapshot`, `animus-control/src/node.rs`) — with well
  under that many commands and no follower, the full uncompacted log tail is
  still on disk, so a same-dir restart replays it back into the fresh
  (empty) memory engine and the "ephemeral" schema reappears anyway,
  regardless of engine backend. The correct way to test "does a genuinely
  fresh, stateless incarnation start empty" is a **fresh directory** per
  incarnation (mirroring what a real ephemeral pod replacement actually is —
  new disk, same address identity) — same-dir `--ephemeral` restart is a
  *different*, real, and separately-interesting claim ("does this specific
  small-scale scenario's residual WAL tail get replayed"), not a substitute
  for it. See `crates/animusd/tests/control_metadata_restart.rs::
  ephemeral_control_only_restart_does_not_carry_over_metadata`.
- **A bounded ring's "gap" check is off-by-one in a way that's easy to get
  backwards, and the wrong direction still passes a shallow test.** Building
  ADR 0038 PR5's per-node `DeltaRing` (`animus-control/src/delta_ring.rs`):
  the natural-seeming assertion "if an old entry got evicted, a caller who
  needed it must fall back" is only true when the caller's `last_seen` is
  *behind* the evicted entry's index — if `last_seen + 1` lands exactly on
  the ring's current front (the caller's next-needed index is exactly the
  oldest one still retained), that is full coverage, not a gap, even though
  something *older* than the front was indeed evicted. A first draft test
  asserted the wrong outcome for exactly this boundary case
  (`writes_since(2, 3)` after indices 1 and 2 were evicted, front now at 3)
  and initially failed against genuinely-correct implementation code — the
  fix was the test's expectation, not the ring. **When testing a
  "contiguous coverage" predicate over a bounded/evicting structure, write
  out the boundary case (`last_seen + 1 == front.index`) as its own explicit
  assertion with the reasoning spelled out in a comment** — it's the one a
  reviewer (or a future edit) is most likely to get backwards, and a test
  that only checks the interior cases won't catch a subtly-inverted
  condition.
- **`PutOk`/`Accepted`-style replies from this codebase's relay/propose paths
  mean "accepted for commit," never "committed and reflected in `Metadata`
  yet"** (a corollary of the durable-before-visible discipline, but easy to
  forget when writing a *test* rather than production code): a test that
  proposes via `ClientRequest::ProposeSchema` and then immediately reads a
  watermark/counter expecting it to have advanced will intermittently (or,
  in one case while building ADR 0038 PR5's restart-fallback test,
  *consistently*) observe the pre-commit value, because the apply task's
  publish is a separate, asynchronous step the reply doesn't wait for by
  design (`ClientRequest::ProposeSchema`'s own handler doc: "the caller
  confirms the commit via replicated `Metadata`"). Every existing test that
  gets this right polls (`propose_and_await`-style) for the actual effect
  (a member/keyspace/tablet appearing) rather than trusting the immediate
  reply — copy that idiom, don't invent a fresh assumption about what an ack
  means.
- **A task prompt's reference to a specific function/mechanism by name can be
  stale by the time you implement it, if an earlier PR in the same stack
  already redesigned it out of existence — grep before assuming the
  follow-up still applies.** ADR 0038 PR5's brief named a specific residual
  item from PR3 ("mirror_loop's fixed 50ms poll → wake on the apply task's
  publish signal") as a small thing to fold in. No such function exists:
  PR3's cutover renamed/redesigned the shadow-mode PR2 `mirror_loop` into
  `meta_apply_loop`/`meta_apply_and_compact`, which already backs off on a
  short idle-only timer (`APPLY_IDLE_POLL`, 5ms) and otherwise stays in
  lockstep behind commit under load — not the "fixed 50ms poll regardless of
  activity" shape the follow-up described. The item was already resolved by
  a prior PR in the same stack; re-implementing "a fix" for it would have
  been either a no-op or, worse, a regression dressed up as progress. This
  is the root `CLAUDE.md`'s "grep before implementing a documented gap"
  practice applying just as much to a task-prompt's own named follow-up as
  to an ADR's prose.

### Code patterns
- **A "full replace" update to `Arc`-shared cached state tolerates a bare
  monotonic-watermark check-then-mutate race; an "apply an incremental delta
  onto the existing cache" update to the *same* shared state does not, and
  reuses the same guard incorrectly if you don't also make it atomic.**
  `animusd`'s `RemoteControlClient` (`control_handle.rs`) is shared between a
  background watch loop and any concurrent `metadata_fresh()` caller. Its
  pre-existing `observe()` (a full `Metadata` replace) read
  `self.watch.latest()`, decided whether to overwrite, then wrote and bumped
  the watch — three separate steps, but safe anyway because a full replace is
  order-independent modulo the monotonic-watermark guard: two concurrent
  replaces racing only risk a *stale* value winning temporarily, never a
  *corrupted* one, and the next reply self-heals it. ADR 0038 PR5 added
  `observe_delta()`, which installs a batch of `KeyWrite`s onto the *existing*
  cached value — a genuinely sequential operation that is only correct if the
  cache is exactly at the delta's own `last_seen` basis at the moment of
  application. Copying `observe()`'s three-separate-steps shape for
  `observe_delta()` would have created a real corruption window: a
  concurrent full `observe()` could advance the mirror between this method's
  watermark check and its mutation, and the delta would then apply on top of
  the *wrong* base, silently producing an internally-inconsistent `Metadata`
  no later reply would ever detect or fix (unlike the full-replace race,
  this one doesn't self-heal). The fix was to make **both** methods acquire
  the mirror's lock first and do the check-decide-mutate-bump sequence while
  holding it, so the two can never interleave — a case where hardening one
  method's atomicity is forced by what gets *added next to it*, not by any
  bug in the original method taken alone. **When adding a second mutator to
  `Arc`-shared cached state that already has one "eventually consistent,
  order-tolerant" writer, check whether the new one's update rule is actually
  order-*dependent* — if so, the existing writer's looser discipline has to
  tighten to match, not just the new one.
- **A per-role internal `Env` peer address book (`ProdEnv::set_peers`) that
  is only ever installed once, at process bring-up, from static config has no
  path for a peer added *after* bring-up to become reachable — even once a
  higher-level replicated membership change (e.g. `RaftCore::
  change_membership`) accepts it.** The `raftkv` role already has this solved
  generically (`animusd::peer_sync_loop`, a periodic static-base ∪
  replicated-`Metadata.node_addrs[*].raftkv`-overlay rebuild); the **control**
  role never needed it before ADR 0037 because the control group was static
  (ADR 0030's scope decision) — so implementing "add a control voter at
  runtime" (PR3 of the ADR 0037 stack) rediscovered the same class of gap
  `ProdEnv::send`'s own doc already anticipates ("an unknown peer is just
  another way the message is dropped... Raft retries once the address
  lands" — but only if *something* makes the address land). Scoped down for
  PR3 to a narrower, still-correct fix rather than porting the full
  `peer_sync_loop` pattern to the control role: a new `ProdEnv::merge_peer`
  (add one entry without replacing the whole book) called by the admin
  action on the **local leader's own** env only, immediately before
  `change_membership` — sufficient for the leader to replicate to a freshly
  added voter, but *not* for a different, later leader (after a subsequent
  transfer/crash) to independently rediscover that voter's address; that
  gap is named, not silently left for a future maintainer to rediscover the
  hard way. See `crates/animus-env/src/prod.rs::ProdEnv::merge_peer`'s doc
  and `ClientCtx::admin_add_control_member`'s doc (`animusd`).
  **Update (ADR 0037 PR4): this gap is now closed**, by finishing the port
  the paragraph above predicted — `animus-control::NodeAddrs` gained a
  `control: Option<SocketAddr>` field (replicated via the existing
  `RegisterNodeAddrs`, `None` for every statically-configured voter) and
  `animusd` gained `control_peer_sync_loop`, a genuine per-tick
  static-∪-replicated overlay for the control role (mirroring
  `peer_sync_loop`, but `merge_peer`-incremental rather than
  `set_peers`-rebuilding, since there is no separate static control peer
  book parameter to layer under here — each node's static book was already
  installed once, directly, at `RaftNode::start`). Regression:
  `crates/animusd/tests/control_membership_admin.rs::
  runtime_added_voter_survives_leadership_change_to_a_different_original_voter`
  (self-removes the adder to force a transfer to a *different* original
  voter, then proves a fresh proposal still reaches the runtime-added
  voter). Building that regression test surfaced a second, unrelated race
  worth its own note (below): a non-voter's self-registration can never
  observe its own commit, so its bounded retry keeps re-proposing (and can
  clobber a concurrent admin action's write) for the full
  `SCHEMA_COMMIT_TIMEOUT`.
- **A newly-joined control-only *non-voter*'s own self-registration retry
  can never observe its own commit landing — so it keeps re-proposing (and
  can clobber a concurrent writer's update to the same replicated entry)
  for the *entire* bounded retry window, not just until the first
  successful commit.** `ClientCtx::register_node_addrs`'s doc already
  describes it as "best-effort... re-proposing each tick" bounded by
  `SCHEMA_COMMIT_TIMEOUT` (10s) — the *intended* shape is: propose, then
  stop once `effective_metadata()` (this node's own applied view) reflects
  it. That confirmation path silently assumes the caller's own applied
  state eventually reflects the commit — true for every existing caller
  (a combined/data node is either already a real voter, or an ADR 0030
  growth node reading the `remote_metadata_sync_loop` mirror through
  `effective_metadata()`), but **false for a genuine control-only non-voter**
  (ADR 0037's own "quiet non-voter until `change_membership` adds it"
  shape): its `ControlHandle::Local` has no mirror substitution and its own
  `RaftCore` never receives real replication while it isn't a voter, so
  `effective_metadata()` stays a permanently-empty default the whole time —
  the confirmation condition can *never* become true, so the loop keeps
  firing on every `SCHEMA_POLL_INTERVAL` tick until the full 10s elapses,
  regardless of whether the relay actually landed on the first attempt.
  Building the PR4 regression test above, calling `admin_add_control_member`
  (which stamps `NodeAddrs.control`) while this window was still open let a
  *later* self-registration retry (still proposing the node's original,
  `control: None` self-registration) silently overwrite the admin action's
  write straight back to `None` — reproduced 100% of the time when the
  admin action ran immediately after the non-voter's own bring-up, and
  confirmed by direct inspection (dumping the non-voter's own applied
  `Metadata`, which stayed completely empty throughout — the "never
  observes its own commit" half of the diagnosis, not a race that only
  sometimes loses). Fixed at the call site, not the mechanism: the test
  now waits for self-registration to land **on the real cluster** (an
  original voter's applied `Metadata`, not the non-voter's own) *and* then
  waits out the remainder of the fixed 10s retry-exhaustion window before
  driving any other write to that same node's address-book entry —
  mirroring what the real operator runbook's own "confirm it's up first"
  step (plan §3) already achieves in practice (a real "start the process,
  then go confirm health" gap is almost always ≥10s). **General check
  before trusting a "propose then confirm via my own state" retry helper
  for a new caller class**: does *this* caller's own read of "did it land"
  ever actually observe the commit, or does it structurally read a view
  that can't reflect it yet (a permanent non-voter, a disconnected mirror,
  a stale cache)? If it can't, the retry isn't "best-effort until
  confirmed" — it's "unconditionally retry for the full bound," which is a
  much bigger window for a concurrent writer to lose a race in.
- **Converting a required address field with an ephemeral-fallback default
  (`SocketAddr` + `#[serde(default = "default_ephemeral_addr")]`) to
  `Option<SocketAddr>` and reusing a bare `#[serde(default)]` silently changes
  what "missing from JSON" means — from "give it a working ephemeral value"
  to "this role isn't run here."** Splitting `animusd`'s `RoleAddrs.control`/
  `raftkv` into per-role `Option<SocketAddr>` (ADR 0035 PR2 — a data-only node
  has no `control` address, a control-only node has no `raftkv` address),
  `#[serde(default)]` on `Option<T>` defaults a wholly-missing key to `None`,
  not to the old ephemeral fallback — so an ancient config (predating the
  `raftkv` field, hence always missing it, and also predating `role` so it
  defaults to `Both`) would deserialize as "`Both`-role but no `raftkv`
  address," an internally inconsistent state the actual entry points (`Node::
  bind`) then reject as a hard error. Caught immediately by a same-PR back-
  compat unit test built specifically to probe this case
  (`oldest_json_shape_missing_optional_fields_loads`) rather than discovered
  later against a real old config. Fix: give the field its own named default
  function returning `Some(default_ephemeral_addr())`, so a wholly-absent key
  still means "ephemeral, combined mode" while an explicit JSON `null` (only
  ever written by a role-aware producer) still means `None`. **When narrowing
  a field's type from `T` to `Option<T>` for a new "this doesn't apply"
  case, re-derive what a *missing key* should mean — don't assume
  `#[serde(default)]`'s blanket `None` matches the old defaulted-`T`
  behavior; write a test that deserializes the actual old-shape JSON (not
  just the new struct's `Default`) to prove it.** (`animusd::RoleAddrs`,
  `config.rs`.)
- **Splitting a peer/address book by role must still satisfy any consumer that
  legitimately spans roles — enumerate cross-role wiring before assuming
  "role A's book" and "role B's book" are each other's complement.**
  Decoupling `animusd`'s single `peer_book()` into `control_peer_book()` (ADR
  0035 PR2) surfaced that a **data**-role node's `raftkv` env is not a pure
  data-role consumer: `heartbeat_loop` (ADR 0012 failure detection) runs *on*
  that env and sends `RaftMsg::Heartbeat` to the **control** ids — so a
  future data-only node whose `raftkv` env peer book was installed as
  `raftkv_peer_book()` alone would have the control ids simply absent from
  its own book, and every heartbeat would have nowhere to route, silently
  killing failure detection for the whole data fleet with no error anywhere
  (`set_peers` with a missing entry just drops the send — no panic, no log).
  The fix is not a new book, just documentation + a test proving it: the
  correct book for that env is the **union** (`raftkv_peer_book() ∪
  control_peer_book()`, i.e. `peer_book()` itself) — call this out explicitly
  in the narrower book's doc comment, and add a unit test that demonstrates
  the negative (the narrow book alone lacks the ids a real consumer needs)
  before asserting the union has them. General check when splitting a
  previously-unified resource by role/tier: for each new narrower view, ask
  "does anything that conceptually belongs to the *other* side still need to
  read this one" — a cross-cutting concern (heartbeats, tracing, metrics) is
  exactly where this hides, because it rides on a role's transport without
  being that role's own data. (`animusd::config::{control_peer_book,
  raftkv_peer_book}`.)
- **A health/status rollup that gates on a *proxy* signal rather than the
  actual risk that signal stands in for can diverge from reality forever,
  because the two clear on different triggers. General check for any rollup
  built from "X is down/unhealthy ⇒ overall is unhealthy": does the thing
  being protected (data replication, request-serving capacity) actually
  recover on a faster/different path than the raw signal does — and if so,
  gate on the protected property, not the signal.** (Original mechanism —
  gating on a lingering `Down` member instead of per-tablet status —
  superseded by the "health ≈ is the data at risk" ladder
  (`quorum-lost`/`under-replicated`/`healthy`/`forming`); full writeup moved
  to `docs/engineering-lessons-archive.md`.)
- **A CSS/pill class name that is also read as a domain-semantic status token
  invites silent scope creep: reusing one for an unrelated visual purpose
  quietly gives that purpose the first status's meaning.** The Tablets view's
  "over auto-split threshold" indicator was implemented as `pill("under-
  replicated", "over " + threshold)` — reusing the `under-replicated` status
  class purely because it happened to render orange. That's harmless-looking
  until something (a filter, a rollup, a screenshot-driven bug report) reads
  the class name as "this tablet actually lost redundancy," which it didn't —
  it was just big. Fixed by introducing a presentation-only `.warn` pill class
  distinct from any status the health rollup ever computes, so a "just a
  warning color" use can never be mistaken for a data-risk status again.
  **When a class/enum name serves double duty as both a CSS selector and a
  domain value some other code branches on, a "just reuse it for the color"
  shortcut is a latent correctness bug, not a style nit — give purely-visual
  reuses their own name.** (`animusd::dashboard_tablets.js`, `dashboard.css`'s
  `.warn`/`.forming`/`.quorum-lost` classes.)
- **A quorum primitive's "who do I need acks from" and "how many acks do I
  need" must both read the group's *live* Raft config — never a peer set
  captured once at construction, even one that looks read-only/immutable.**
  Building automatic replica rebalancing (ADR 0029) needed a *healthy* replica
  move (not just failure repair), which for the first time could rotate a
  majority of a tablet's Raft group onto nodes that were never in any
  surviving replica's original peer set. `RaftKvNode`'s ReadIndex barrier
  (`animus-cp-data`) had silently keyed both its ack-quorum threshold
  (`majority()`) and its probe fanout on `all_nodes` — the group's peer set at
  *hosting time* — instead of `RaftCore::config()` (the live, dynamically
  updated voter set already used everywhere else in the same crate). This was
  invisible for the entire life of the feature it was built for: every
  membership change before ADR 0029 was a same-size, pre-known swap (a
  failure-repair spare was already listed in every replica's `all_nodes` from
  the moment the group formed), so the stale and live sets never actually
  diverged. The break only showed up once a *different* feature (rebalancing)
  exercised a membership shape (a full rotation) the original code was never
  tested against — a stale-quorum leader could only ever self-ack, so every
  linearizable read on that tablet timed out and reported the key **absent**,
  indistinguishable from real data loss from outside. A second, compounding
  bug in the same feature made it worse: `animusd`'s CP-routing short-circuit
  (`resolve_cp_route`) trusted "I have a locally registered group handle" as
  proof of being a *current* replica — true before ADR 0029, false during the
  new removed-replica GC's deliberate grace window — so a node that had just
  been rebalanced off a tablet, but not yet GC'd, waited forever instead of
  forwarding to the tablet's actual current replicas. **General check when
  adding a new way an existing invariant can change** (here: "a group's peer
  set can evolve after hosting," where before it was fixed for a group's whole
  lifetime): grep every place that invariant's *original* form was cached or
  assumed stable, not just the one mechanism you're adding to change it — an
  optimization that skips re-deriving a fact from live state ("no `Metadata`
  clone needed, I already have a local handle") is exactly where this hides,
  because it was correct on every input anyone had tried before. Caught by
  building a genuine end-to-end integration test (`animusd/tests/
  cp_rebalance.rs`, a 5-node cluster with tables provisioned before growth) —
  no unit or sim test at either layer alone exercised a *full* replica-set
  rotation through a *linearizable read*, only through `local_get`/config
  equality. When writing a regression test for "a stale peer keeps
  responding," make the peer actually stop responding (`shutdown()`), not
  just remove it from the current config — a still-live departed peer can
  accidentally still ack on a bare term match and mask the very bug the test
  exists to catch. (`animus-cp-data::RaftKvNode::majority`/read-barrier probe
  fanout; `animusd::resolve_cp_route`'s `has_local_replica` gate.)
- **A two-layer gate where the selector and the actuator use different
  thresholds fails silently — and a primitive's `bool`/`Result` return value
  that encodes "did this actually take effect" must never be discarded, however
  statement-shaped the call looks.** ADR 0029's leadership-transfer primitive
  had exactly this shape: `RaftKvNode::reconfigure_step`'s step 4 *selected* a
  transfer target with `peer_match(n) >= commit_index()`, but
  `RaftCore::transfer_leadership` only *armed* at `peer_match(target) ==
  last_log_index()` — a stricter threshold the selector never checked — and
  the caller wrote `self.transfer_leadership(target);` with the returned
  `bool` dropped on the floor. `propose` is fire-and-forget (it appends to the
  leader's local log and returns before any replication round trip), so on a
  write-hot tablet `last_log_index` moves the instant a write is accepted
  while every peer's `peer_match` still reflects the *previous* entry — the
  two thresholds disagreed at essentially every sampling instant, so the arm
  failed *forever*, and nothing ever surfaced it: no error, no log, no metric,
  just a rebalance move that silently never completed for any tablet whose
  move needed to relocate its leader. The correct fix is standard Raft §3.10
  semantics, not just threshold alignment: relax the arm gate to match the
  selector (`>= commit_index`), but that alone reintroduces the original
  danger (arming to a target that isn't actually at `last_log_index` yet), so
  **freeze `propose`/`change_membership`** while a transfer is armed (return
  `NotLeader`, hinting the target) so the log stops growing and replication
  can close the remaining gap, gate the actual `TimeoutNow` send on the
  target *reaching* `last_log_index`, and **abort** (clear the arm, resume
  proposing) if a deadline passes with no step-down — else a target that
  crashes right after arming strands the group frozen forever. A related,
  narrower bug in the same function compounded it: the down-extra search
  reused a generic "lowest non-self extra" helper and only *then* filtered it
  on down-ness, so a `Down` extra sorting after a healthy one was invisible —
  the step fell through to a *different*, catch-up-gated removal path, which
  could stall behind an unrelated survivor's lag. **General checks:** (1) when
  a value is computed once to pick a candidate and re-derived/re-checked
  inside the primitive that acts on the candidate, diff the two conditions —
  "selects X" and "arms X" must agree on what "eligible" means, or the
  narrower one silently wins every time; (2) grep for every call to a
  bool/Result-returning mutator where the result is bound to `let _ =` or not
  bound at all — if the primitive's doc says "returns whether it took effect,"
  a discarded result is a designed-in blind spot; (3) a "search for the first
  match of predicate P" helper reused with an *unrelated* predicate applied
  only to the first result (`extra().filter(down.contains)`) is a common way
  to accidentally scope a search to "the first element of the base sequence,"
  not "the first element satisfying the actual predicate" — write the combined
  predicate into the search itself. (`animus-control` `RaftCore::
  transfer_leadership`/`propose`/`change_membership`/`broadcast_append`;
  `animus-cp-data::RaftKvNode::reconfigure_step`; regressions in
  `animus-control/tests/leadership_transfer.rs`,
  `animus-cp-data/tests/leader_transfer_reconfigure.rs` — the hand-driven
  variant is the one proven to fail against the pre-fix source — and
  `animus-cp-data/tests/reconfigure_down_extra_priority.rs`.)
- **`tokio::fs::File` writes are not ordered or durable until `flush().await` —
  a dropped handle completes its write in the background, so two sequential
  appends via separate handles can land INVERTED on disk, and a later `sync` on a
  fresh fd can fsync before the buffered write reaches the page cache.** This
  broke "ack means durable" under ProdEnv and was the long-standing
  `lsm_concurrent::scans_survive_concurrent_compaction` flake (an SSTable
  recovered with its index at offset 0). Always `flush().await` before dropping a
  write handle; found independently twice (PRs #26, #27). Corollary of the
  documented "a flaky ProdEnv test is a real bug" rule.
- **Commit the election no-op in `become_leader` itself (`maybe_advance_commit`
  after the append)** — a leader that only advances commit on propose/ack strands
  a sole voter's recovered WAL tail (nothing re-drives commit until the next
  propose), and any gate on "current-term entry committed" (ReadIndex §6.4, the
  membership-change gate) would deadlock a single-node group. (PR #25.)
- **Metadata-level dedup of a proposal only picks one *winner* — it does not stop other legitimate callers from invoking a side-effecting state-machine command, which must therefore be idempotent at APPLY time, not just deduped at the propose layer.** (Found in the pre-ADR-0028 two-phase split; superseded by ADR 0028's single-command split. Archived in `docs/engineering-lessons-archive.md`.)
- **An operator/admin action that calls straight into an engine bypasses the
  single-writer contract the normal path establishes — audit every admin surface
  against the layer's concurrency assumptions.** `LsmEngine` is safe on the client
  path because the per-tablet Raft apply loop is its sole writer, but
  `POST /admin/storage/flush|compact` call `flush_now`/`compact_now` from the admin
  connection's task, racing that loop — and `flush()` (snapshot → unlocked build →
  unconditional `memtable.clear()`, no flush-in-progress flag) then erases an acked
  concurrent write, whose WAL segment a *later* flush GCs: permanent loss. The
  concurrency tests miss the quadrant (the concurrent-writer test never flushes;
  the flushing test has one writer) — test "forced maintenance under live load"
  explicitly. (2026-08-06 audit; ADR 0008/0020 notes; fix = serialize
  flush-vs-apply and flush-vs-flush.)
- **One id space must have one allocator — a second allocation path silently breaks
  the invariant the first one carries.** Tablet ids are never-reused *because*
  provisioning allocates via `next_free_tablet_id()` (folds in the monotonic
  `next_tablet_id`); `trigger_split` allocated `max(live ids)+1` instead, so
  drop-highest-table-then-split re-mints the freed id — and a replica still holding
  the dropped tablet's files re-hosts them as the new tablet (ADR 0024 violation;
  GC can never reclaim them since the id is live again). The apply-side validation
  only rejected collisions with *present* tablets, so nothing self-healed. Route
  every mint through the one allocator, and make the replicated apply reject ids
  below the monotonic counter so a divergent client can't reintroduce it.
- **A new state-mutating replicated command needs the *same* CAS/precondition
  discipline as its sibling commands on the same resource — a missing guard is
  invisible until two proposers race.** `MetaCommand::SplitTablet` applied
  unconditionally as long as its `split_key` fell inside the source tablet's
  *current* range, with no check on the tablet's epoch — unlike its sibling
  `CasTabletReplicas`, which already gates on `expected_epoch`. Two proposers
  racing to split the same tablet at the same epoch (two independent
  `animusd::auto_split_loop` instances, or an auto-trigger racing a manual one)
  could each compute a different median from an equally-stale range view and
  **both commit**: each `SplitTablet` mutates the source's range and mints a new
  child id, and neither commit's precondition ever looks at the other's. But the
  tablet's own per-tablet CP-data Raft group can only ever apply **one** real
  `Split`, ever (an at-most-once apply-time guard there) — so the losing
  metadata-level split's `new_id` becomes a permanent, leaderless,
  metadata-only orphan tablet: present in `Metadata.tablets` with a real
  range/replica set, but with no CP group anywhere in the cluster and no code
  path that ever revisits it (the `auto_split_loop`'s existing "abandoned"
  detection correctly stops *retrying* the losing key, but never cleans up the
  `new_id` it already minted). Found live on a `--cluster 3 --auto-split 2000`
  bulk-seed run (two orphaned tablets, `/admin/status` showed real ranges,
  `/admin/raftkv` showed no group for either). Fixed by adding
  `expected_epoch: Epoch` to `SplitTablet`, gated identically to
  `CasTabletReplicas` — so the loser's step 1 (`propose_split_metadata`) now
  fails cleanly (`Rejected("epoch mismatch")`), which `auto_split_loop` already
  handles as "nothing was allocated, no orphan to track." **When adding a
  command that mutates a resource another command already CASes, check whether
  the new command needs the same guard — "my precondition happens to still
  hold" is not the same as "no one else committed a conflicting change since I
  read this."** (`animus-control` `meta.rs::split_rejects_a_stale_epoch_racing_a_concurrent_split`,
  `tablet_split_merge.rs::racing_splits_at_the_same_epoch_only_one_applies`.)
- **A CAS guard closes the *concurrent* instance of a race; a *sequential* instance of the same race needs its own answer — usually cleanup, not another precondition.** (Found in the pre-ADR-0028 orphan-tablet cleanup, now structurally impossible; archived in `docs/engineering-lessons-archive.md`.)
- **A retry loop keyed on a resource id must recheck the resource still exists — a precondition that only checks its own transient state silently assumes the resource itself is immortal.** (Found in the pre-ADR-0028 `auto_split_loop` pending-retry map, since deleted; archived in `docs/engineering-lessons-archive.md`.)
- **Before reaching for "remember everything" to disambiguate an edge case, check whether a cheap, independent check at the point of irreversible action can bound the state to O(1) instead.** (Found in the pre-ADR-0028 `current_split_bound`, since the data-plane half of split was removed entirely; archived in `docs/engineering-lessons-archive.md`.)
- **To wake a `select`-parked `<E: Env>` driver loop from another task, race a
  `futures::task::AtomicWaker` + `AtomicBool` future — never a tokio-only primitive
  (`Notify`/`watch`), which SimEnv can't drive.** The CP data-plane driver used to
  leave a freshly-proposed Raft entry parked until the next ~50ms heartbeat tick; the
  fix (single-write latency, ADR 0017) has the proposer raise a flag + `wake()` and
  the consensus loop race a third `select` arm that resolves on it, then
  `replicate_now` immediately. `AtomicWaker` is executor-agnostic: under `SimEnv` the
  synchronous `wake()` marks the driver task ready for the next run-loop poll (fully
  deterministic, no wall clock); under tokio `ProdEnv` it resolves the register/wake
  race. Two disciplines keep it correct: the waiting future **registers the waker
  *before* checking the flag** (else a wake between check and park is lost), and
  **consumes the flag** (`swap(false)`) on resolve so it doesn't busy-spin. Pair the
  wake with the *consumer-side* poll it unblocks: a fast propose is pointless if the
  ack path still polls on a coarse fixed interval — `animusd`'s `cp_put_local` confirm
  loop was cut from a fixed 50ms to a ~200µs→5ms adaptive back-off in the same change
  (median lone-write latency 52ms → 11ms, `cp_plane.rs::single_write_latency_is_low`,
  a `multi_thread` `ProdEnv` liveness test — the sim can't measure real-thread
  latency). (`animus-cp-data` `ProposeSignal`; `RaftCore::replicate_now`.)
- **A new `ClientRequest` variant that can be *forwarded* must be handled in BOTH
  the main serve loop AND `cp_serve_forwarded` — a single-node test can't catch the
  missing half.** `animusd` CP ops route locally or **forward one hop** to the
  leader's node wrapped in `ClientRequest::Forwarded`; the receiver dispatches the
  inner request through `cp_serve_forwarded`, a *separate* match from the top-level
  serve loop. A batch (`PutBatch`) added only to the serve loop works whenever the
  connected node happens to host the tablet leader and silently errors ("unexpected
  forwarded request") when it must forward — the same bimodal per-process failure
  shape as the `is_relayable_command` allowlist gap. When adding a forwardable
  variant, grep for the request enum's name across *both* match sites and add the
  arm to each; regression-test it through a **follower/non-leader-connected** node
  in a per-process cluster. (`animusd` `cp_serve_forwarded`; batch put, ADR 0017.)
- **WAL group commit only coalesces *concurrent* writers — a *sequential* apply
  loop pays one `fsync` per op and needs an explicit batch primitive.** The
  per-tablet CP-data Raft apply loop (`flush_and_apply`) applies a run of committed
  commands from **one** task, `await`ing each `merge`/`merge_tombstone` in turn; the
  WAL group commit (which amortizes the `fsync` across writers *ready in the same
  drain cycle*) sees exactly one in-flight writer, so every command paid a full
  `fsync` (~5-9ms on real disk; ~180ms for a 20-40 command batch). The fix is a
  `StorageEngine::merge_batch(Vec<MergeOp>)` that logs the whole run as **one WAL
  record + one `fsync`** then applies it under one lock (defaulted to a per-op loop
  so `MemoryEngine`/others are unaffected; `LsmEngine` overrides it). The apply loop
  accumulates a run of `Put`/`Delete` into a batch and drains it before any command
  that must *read* committed state (`Cas`, `Split`) so the read still sees prior
  writes. Measured ~9.7x apply throughput (1851→17918 puts/s), fsyncs 30x fewer.
  **Lesson: "we have group commit" does not make sequential writes cheap — group
  commit is a concurrency optimization; a single-task write run needs a batch API.**
  (`animus-storage` `merge_batch`; `animus-cp-data` `flush_and_apply`.)
- **A "send X" path that falls back to a *default* when X is absent can ship a
  silently-corrupt value — make the absent case impossible (set X at every state
  transition that needs it), not `unwrap_or_default()`.** The per-tablet CP Raft
  ships its engine image as `snapshot_blob`, set by the driver only on *compaction*;
  the leader's `snapshot_chunk_for` did `snapshot_blob.unwrap_or_default()`. A node
  that caught up via a *received* `InstallSnapshot` advanced `snapshot_index > 0` but
  never set its blob, so when it later became the source it shipped **0 bytes** — the
  receiver decoded an empty image (`EOF while parsing a value, line 1 column 0`),
  dropped it, and never caught up (surfaced as "CP split: new tablet never appeared",
  a *leaderless* split child). The fix sets `snapshot_blob` on the *install* path too,
  so the invariant `snapshot_index > 0 ⟹ blob.is_some()` holds and no ship is ever
  empty — far better than a 0-byte default that *looks* like a valid transfer. **A
  recursive/relay protocol must hold its invariant at the *second hop*: A→B works
  off A's freshly-built state; B→C is what exposes that B never retained what it
  received.** A unit test that drives only one hop (`A→B`) misses it — drive the
  re-ship (`A→B`, then `B`-as-leader→`C`).
  (`animus-control` `raft.rs::handle_install_snapshot`; regression
  `driver_applied_sm.rs::caught_up_node_reships_non_empty_snapshot`.)
- **A per-message O(state) serialize on a Raft consensus loop is a latent
  election-storm hazard, and a *cache* to fix it must not double the work it replaces
  — reuse the one serialization everywhere the state is needed.** The control-plane
  `snapshot_chunk_for` re-serialized the whole `Metadata` **per 1KB InstallSnapshot
  chunk**; on a multi-MB metadata a follower catch-up shipped ~thousands of chunks
  (~50ms serialize each), pinning the loop far past the 150ms election timeout — a
  self-sustaining storm during any large-state catch-up (the control-plane twin of
  PR #16's CP-data apply/compaction storm). Fix: **cache the serialized image once
  when `snapshot_index` advances and slice it per chunk** (O(chunk)). But the naive
  cache *doubled* compaction cost — the blob serialize **plus** the WAL `Snapshot`
  record's own metadata serialize — so reuse the cached bytes for the WAL too
  (`serde_json` `RawValue` embeds the pre-serialized image verbatim; byte-identical,
  guarded by a round-trip test). Two morals: (1) the cache must be pinned to
  `snapshot_index`'s state, serialized **eagerly at snapshot time** (in-core
  `metadata` advances past the base between compactions, so lazy-at-ship would ship
  a state *ahead of* its claimed index → the follower double-applies its log tail);
  (2) **this hazard is invisible to `SimEnv`** (virtual time never trips the
  wall-clock election timeout) — the teeth is a wall-clock-timed transfer
  (`install_snapshot.rs::large_snapshot_ships_in_o_chunk_time_not_o_state`: fix ~ms
  vs regression ~46s), because a *live* `ProdEnv` cluster catch-up races
  leadership/AppendEntries and won't reliably traverse a long chunk-stream.
  (`animus-control` `raft.rs::snapshot_chunk_for`/`snapshot_upto`/`encoded_wal_image`,
  `persist.rs::encode_snapshot_record_from_blob`.)
- **When mirroring a fix onto a *sibling* subsystem, assess honestly — the sibling
  may have a *different-shaped* version of the hazard, or a bounded one not worth the
  same risky refactor.** PR #16 moved CP-data's async **engine apply + compaction**
  off its Raft loop (a >150ms self-sustaining stall). The control plane applies its
  state machine **in-core, synchronously** — no async apply to move — so its only
  loop-blocking O(state) work is snapshot-shipping (fixed above, cheaply) and the
  compaction WAL-rewrite serialize. The latter is a *single* stall (~50ms at ~1MB,
  ~120ms at ~3MB), under the election timeout at realistic scale and **not**
  self-sustaining (once per 64 applied entries). Moving it fully off the loop would
  couple the install→WAL-rewrite ordering into a second task on the most
  safety-critical Raft (real risk) for a bounded, rare, extreme-scale stall — so it
  was **measured, documented, and deferred**, not force-fit. A well-reasoned "the
  sibling's hazard is smaller; here's the measurement" is a valid outcome.
- **A recursive operation that "works" once may be relying on a depth-1 coincidence — prove it at depth ≥ 2.** (Found in the pre-ADR-0028 split-hook/member-id derivation, since deleted by ADR 0026 Stage B / ADR 0028; archived in `docs/engineering-lessons-archive.md`.)
- **Distinguish "seed a fresh child" from "join an existing group empty" by a durable monotonic signal, not a race.** (Found in the pre-ADR-0028 split-handoff design, since removed — a fresh split child needs no handoff seeding at all; archived in `docs/engineering-lessons-archive.md`.)
- **Which physical engines a node hosts is *local* durable state, not derivable from replicated `Metadata`.** (Found in the pre-ADR-0028 `cp-hosted` marker, since removed — every tablet on a node now shares one engine opened once at start; archived in `docs/engineering-lessons-archive.md`.)
- **Keep a replicated map in stable canonical ids; translate to any locally-derived ids at the edge, not in the replicated state itself.** (Found in the pre-ADR-0028 base/member id split, since removed — a tablet's CP group member id is now always its base id; archived in `docs/engineering-lessons-archive.md`.)
- **Drive cross-plane reconfiguration by *pull from replicated state*, not a new
  push command — it keeps the dependency edge one-way and the seam testable.**
  Wiring the control plane to reconfigure a per-tablet Raft KV group on a node
  failure (ADR 0017 C), a control→data "reconfigure now" message would have forced
  `animus-control` to depend on `animus-cp-data` (a cycle — data already depends on
  control for `RaftCore`) and to track each group's leader. Instead the decision
  already lives in replicated `Metadata` (the placement reconciler's epoch-CAS), so
  each group **leader pulls** its tablet's desired voter set and reconfigures
  *itself* (`reconfigure_step` + `spawn_reconfigure_loop`) — no reverse dependency,
  no leader-reporting needed for the trigger, and the data side takes the metadata
  source as a **closure** (`Fn() -> Option<BTreeSet<NodeId>>`) so the crate stays
  decoupled from the control-plane driver type. Mirrors the proven `reconcile_loop`
  split: decision pure + elsewhere, timing in the loop. Reconfigure toward a target
  **one single-server step per tick** (the `change_membership` contract), letting a
  multi-server move converge over successive ticks rather than failing.
- **Extend the `Env` seam with a *sub-trait* bound only where used, not by widening the supertrait — capabilities not every env has stay opt-in.** (Found building `Coresident`/`sibling()`, since superseded by ADR 0026 Stage B / ADR 0028; the pattern itself remains the right one for a future capability. Archived in `docs/engineering-lessons-archive.md`.)
- **To store a generic type behind one registry/handle field, fix the concrete
  type parameter when the variation isn't needed at the call site — don't reach for
  a trait object.** Routing a CP-mode table to a hosted `RaftKvNode<E, S>` (ADR 0017
  #3a) needed the `animusd` edge state to hold the group handle and call
  `put`/`linearizable_get`/`is_leader` on it. `RaftKvNode` is generic over its
  engine `S`, so a `Vec<RaftKvNode<ProdEnv, _>>` field would need an
  `async_trait` object (the methods are async) — extra machinery for variation that
  doesn't exist here: the CP plane is *always* durable, so `S = LsmEngine<ProdEnv>`
  is the only instantiation. Fixing it (a `type CpGroup = RaftKvNode<ProdEnv,
  LsmEngine<ProdEnv>>` alias — also silences `clippy::type_complexity`) kept the
  edge registry a plain `Vec<CpGroup>`, no trait object, no async-trait dep. The
  AP data replica *is* type-erased (`Box<dyn Any>`) because its backend genuinely
  varies (LSM vs Memory); the CP group's does not.
- **Adding an Nth internal role to a fixed-stride multi-role node is a wide but
  mechanical ripple — change the stride, every literal, and the arity together.**
  `animusd` packs each node's roles into consecutive ports (`base + stride*i`); the
  CP `raftkv` role bumped the stride 6→7 and touched every `RoleAddrs` literal
  (config gen + 5 test sites), `peer_book`, `Node::bind`'s arity, the `[ProdEnv; N]`
  shutdown array, and the conventional id base (`300+i`). A `#[serde(default)]` on
  the new addr field keeps *older configs* loading, but struct **literals** still
  need the field — so the compiler walks you through the sites; expect it and do
  them in one pass.
- **Generalizing a type over a state machine: prefer *two plain type params*
  (`<C, S>`) over *one param with an associated type* (`<SM: Trait<Command=C>>`) —
  `#[derive]` can't see through associated types.** Making `RaftCore` generic over
  its command + state machine (ADR 0016 step 2), a one-param `RaftCore<SM>` would
  force manual `Clone`/`Debug`/`Serialize` impls on every container holding
  `SM::Command` (the derive generates `impl<SM: Clone>`, which does *not* imply
  `SM::Command: Clone`). Two plain params (`C = MetaCommand`, `S = Metadata`) let
  every derive Just Work, and **defaulted** params keep all existing references
  source- and serialization-compatible (the generic is erased in JSON, so the WAL
  bytes are unchanged). One residual gotcha: `#[derive(Default)]` still adds a
  spurious `C: Default` bound — hand-write `Default` where a field is
  `Vec<_>`/`Option<_>` (needs no inner `Default`). And a no-arg constructor like
  `RaftCore::new()` needs a type annotation at call sites that don't otherwise pin
  the params (bare `let x: RaftCore = …` or `Vec<WalRecord>` on a `decode`).
- **No process-global mutable state (`OnceLock`/`static`) for per-instance
  concerns.** It leaks across tests in one binary (multiple in-process clusters
  share it) and conflates instances in any multi-tenant context. Thread state
  through a per-instance context instead (the wire edges' `ClusterEdgeState` via
  `ClientCtx`, not process statics). If you must keep a static, make sure tests
  tear instances down (`Node::shutdown()`) and use unique names/keys per test.
- **Never hold a `std::sync::Mutex` guard across an `.await`** in `<E: Env>`
  code — it breaks `Send` (often a *compile* error via `spawn_task`'s bound) and
  risks nondeterminism. Take the lock, mutate, drop it; do I/O lock-free.
- **`serde_json` cannot serialize a map keyed by a struct** — it fails at
  *runtime*, not compile time (`expect("...serializes")` panics). A
  `BTreeMap<Timestamp, _>` (or any non-string/non-integer key) in a `Serialize`
  type must ride as a `Vec<(K, V)>` instead. Bit when adding a WAL `Snapshot`
  record carrying `BTreeMap<TxnId, _>` (animus-consensus); integer-keyed maps
  (`BTreeMap<u64, _>`) are fine (stringified), struct-keyed are not.
- **Tightening a quorum/threshold can *expose* a latent ordering bug elsewhere —
  re-derive the safe bound from first principles and check the whole pipeline.**
  Making Accord's fast quorum precise (`N-1`, down from `ceil(3N/4)`) let two
  *conflicting* txns legitimately commit at the same `logical` timestamp (ordered
  by the node tiebreak); the downstream MVCC `version` was `logical` alone, so
  per-key LWW kept the wrong (first-applied) winner. Encode the *full* order
  (`(logical<<16)|node`) wherever a total order is collapsed to one `u64`. Also:
  pair a quorum bound with its *recovery* procedure — the smaller "optimized"
  Accord/EPaxos fast quorum needs the full witness-recovery; the simplified
  slow-path recovery requires the larger `N-1` bound.
- **Replicate the *definition*, keep the *bulk data* at the edge — and split them
  cleanly.** When promoting per-process state to the control plane (ADR 0013), move
  only the small, must-agree *shape* (e.g. a secondary-index definition: name/keys/
  projection) into replicated `Metadata`; leave the large derived *data* (the index
  entries) edge-local, rebuilt from observed writes. Make the edge reconcile its
  in-memory machinery *from* the replicated definitions (a `sync_indexes`-style
  method that preserves entries on an unchanged shape, clears on a changed one) so a
  restart recovers the shape from Raft, not local memory. Additive `MetaCommand`
  variants + a `#[serde(default)]` new field keep older snapshots/consumers working.
  (Found replicating DynamoDB GSI/LSI definitions; `animus-control` `schema.rs`.)
- **Count a metric at the site that knows the *real* outcome, not the attempt.**
  A counter recorded where an op is *requested* over-counts when a downstream
  helper silently no-ops (e.g. `HintStore::record` drops a hint on a residency or
  LWW-supersede miss). Have the helper *return* whether it acted and count on that
  (`data_hints_stored` counts only hints actually stored). This keeps the closed
  `Metric` enum (ADR 0015) append-only/byte-reproducible and the seam observe-only
  — instrumenting must never change the path it measures. **When a metric is the
  *delta* of a pre-existing monotonic counter** (e.g. recording WAL rotations from
  `GroupCommit::rotation_count` around each `commit`, or block reads off a shared
  introspection `AtomicU64`), it is easy to wire the source increment yet forget the
  `metrics.incr_by(delta)` — the source counter moves but the metric stays 0. A
  "counter moved under a known workload" sim test catches exactly this (it did:
  `storage_wal_segment_rotations` read 0 while the WAL had rotated 349 times).
- **A per-instance observability seam (e.g. ADR 0015 metrics) has *one sink per
  `Env`/role*, so the integration layer must aggregate, not pick one.** A node
  runs several `ProdEnv` roles on distinct ids (control/data/coord), each with its
  **own** `metrics()` sink — `RaftNode::start` records into the *control* env's,
  the replica/coordinator into theirs. A `/metrics` handler that read only one
  (e.g. `node.raft.metrics()`) would silently drop the others' counters. Capture
  every role's handle and sum the snapshots **at request time** (live, not cached);
  capture the soon-to-be-moved handles before the envs are consumed. (`animusd`
  `ClientCtx::metrics_text`.)
- **An admin/introspection surface is a pure *observer* over per-instance handles,
  aggregated live — and per-instance state makes it meaningful *per node*, not
  cluster-wide.** The admin interface (ADR 0020) only *reads* node state (Raft
  accessors, promoted `LsmEngine` introspection — kept snapshot-shaped so it can't
  perturb the measured path, like the metrics seam) or drives an explicit gated
  action; it never changes the path it inspects. Two consequences bit during the
  build: (1) **metrics/Raft counters are per-node sinks** — a *follower's*
  leader-only counters (`elections_won`, `append_entries_sent`) are legitimately 0,
  so an admin/metrics endpoint is sound only *per node* (scrape the leader for
  leader-only state; the test asserts election counters only on the control leader).
  (2) **the in-process `--cluster` shared `ClusterEdgeState` lists *every* node's CP
  group handle**, so one node's `/admin/raftkv` shows all replicas, while a
  one-process-per-node deployment (separate edge each) is node-local — match the
  test's bring-up (`run_node` per node) to the semantics you assert. Reuse the
  documented port-TOCTOU retry for any `free_addrs`-style `ProdEnv` bring-up.
- **Don't react to "I was superseded" by *immediately* re-proposing higher** —
  that is the classic duelling-proposers **livelock** (two recoverers ratchet each
  other's ballot forever within one logical instant, an unbounded message storm).
  Break ties **deterministically** (e.g. only the higher-id contender retries; the
  other stands down and adopts the winner's result) or back the retry off in time.
  This also hangs a `SimEnv` test rather than failing it: the single-threaded
  cooperative executor just spins at one virtual instant (100%+ CPU, no progress,
  no panic), so **run new sim tests under a `timeout`** the first time — a hang
  there is a same-instant unbounded-work loop, not slowness. (Found wiring Accord
  recovery ballots; `animus-consensus` `core.rs::handle_superseded`.)
- **Prefer a live read of the durable layer over observation-built in-memory
  state.** The DynamoDB edge once tracked written item keys in-memory to fake a
  range scan; that set is lost on restart and stale on a follower that never saw a
  write. Replacing it with the data plane's native quorum range scan
  (`DataClient::scan`, reading live storage in key order) made `Query`/`Scan`
  correct after a restart — and the *regression that proves it* is a scan **after a
  node restart wipes the registry** (`animusd/tests/dynamo_schema.rs`), not just a
  same-process scan. When you delete a derived cache, test the path that the cache
  used to mask.
- **A cross-cutting seam (metrics, tracing) must be *additive* and observable
  without touching `SimEnv`.** Add it to `Env` as a method with a **no-op default**
  (a real shared no-op handle, not an `Option`, so record sites need no guard) —
  the supertrait and every `E: Env` impl stay untouched. Keep it deterministic:
  no wall clock (timestamps come from `Clock::now`), no I/O, no `HashMap` (snapshot
  into a `BTreeMap`). To let a *sim test* read what a component records, thread a
  recording handle into the component (e.g. `start_with_metrics`) rather than
  overriding `SimEnv` — so `animus-sim` needs no change. (ADR 0015 / `animus-env`
  `metrics.rs`.)
- **A new orthogonal capability often *composes* existing single-instance pieces —
  don't reshape the proven core to add it.** Per-shard Accord consensus (one group
  per tablet) landed as a thin driver layer (`ShardedOwner` hosting one untouched
  `AccordNode` per local shard, routed by a `ShardRouter` *derived from the existing
  tablet map* — no new control-plane state), leaving the sync `AccordCore`
  byte-for-byte unchanged and the whole prior suite green. Look for the
  by-composition path before editing a load-bearing state machine; and **a node
  hosting several protocol instances needs one `Env`/inbox/WAL *per instance*** (the
  inbox is single-consumer) — allocate a distinct id per (node, instance) and let
  the caller own that allocation policy. (`animus-consensus` `shard.rs`.)
- **A *per-node* decision must dedup on *per-node* state, never on shared registry state — a registry that doesn't distinguish callers by node silently answers "does anyone in the cluster satisfy this," not "do I."** (Found in the pre-ADR-0031 `cp_join_host_loop`/`minted`/shared `ClusterEdgeState`; both halves are superseded by ADR 0031 PR2+PR4. Archived in `docs/engineering-lessons-archive.md`.)
- **A Raft group *forming or re-forming* (no live leader) needs the full voter config;
  only a *new spare joining a led group* starts as a non-voter — and the restart
  signal is on-disk data, not the epoch.** WAL recovery does **not** restore voter
  status from a non-voter `all_nodes` start, so a node re-hosting a tablet it already
  has data for must pass the **full** config explicitly. Gating on epoch misfired: a
  split bumps the original replicas' epoch, so a post-restart re-host of a split
  parent looked like a "join" → non-voter → no election. Use `latest_version() > 0`
  (engine has data ⟹ re-forming) as the signal. (ADR 0023, originally `animusd`
  `cp_join_host`; since ADR 0031 PR4 the decision lives on unchanged as
  `TabletFacts::has_data` in `animus_cp_data::host` — gathered by
  `Reconciler::gather_facts` via `StorageScope::has_data`, the shared-engine
  successor to `latest_version()`.)
- **With provisioning in band (a tablet's group forms on first access, not at
  startup), a node that *is* a replica of a not-yet-hosted tablet must WAIT, not
  forward.** Routing's "I host no replica → forward to any route" fallback misfires
  during the formation window when a replica-to-be hasn't stood its group up yet — it
  forwards to a node that doesn't host the leader → "forwarded CP op: not the leader
  here". Gate the forward on "this node is **not** in the tablet's replica set"; a
  replica waits for its own election. And **don't paper over formation latency with a
  synchronous serve-wait on the provisioning path** — it made the first write block on
  full formation (regressing a restart test); `cp_route` already waits, so provisioning
  returns once the tablet is in `Metadata`. (ADR 0023, `animusd` `resolve_cp_route`.)
- **An id-translation seam must be applied in *both* directions — the identity case masks the missing one.** (Found in `cp_member_id`/`cp_base_id`, since removed by ADR 0026 Stage B / ADR 0028; archived in `docs/engineering-lessons-archive.md`.)
- **When the key format changes (e.g. ADR 0022's token prefix), sweep *every*
  key-building write path, not just the wire edges — a path that bypasses the shared
  layout partitions a different keyspace.** The admin bulk-seed endpoint kept writing
  raw `prefix+index` bytes via `cp_write` after the DynamoDB/CQL builders gained the
  Murmur3 token prefix, so seeded tables split at raw-key medians (readable ranges in
  the dashboard — the visible symptom), sequential seeds piled into one tablet's tail
  (the exact skew the token removes), and a mixed seed+edge table would interleave two
  key layouts in one engine. `cp_write`/`cp_read` take the key verbatim, so nothing
  below the edge catches this — grep for `cp_write` callers and check each builds the
  ADR 0022 layout. (`animusd` `admin.rs::seed_key`.)
- **The token prefix is a *wire-edge/seeder* convention, not a storage invariant —
  a transform that renders/parses a stored key must detect the layout by content,
  not assume it.** The ADR 0022 `token || escape(pk) || rk` layout is built *above*
  `cp_write`; the DynamoDB/CQL edges and the bulk seeder add the token, but the
  plain-client `Put` stores its key **verbatim** (`cp_put_local`, un-prefixed). So a
  dashboard key view that hex-formats "the first `TOKEN_BYTES` bytes as the token"
  mangles a plain key (`admin-key` → `61646d696e2d6b65:y`). Gate the split on the
  leading run actually being **non-printable** (a Murmur3 token almost always has a
  non-printable byte; a printable key is shown as text) so both key populations
  render correctly. Same "keys aren't uniform below the edge" root as the seed-key
  entry above. (`animusd` `admin.rs::key_display`/`parse_key_display`; the
  `admin_endpoint` test writes a *plain-client* `admin-key` — it caught exactly this.)
- **A restarted Raft replica re-applies its recovered log from the start, so any
  consumer keyed on replicated state passes through *historical* states — a loop
  acting on *absence* (a GC/teardown) must be convergent, and its post-restart
  assertions must poll.** The drop-table GC (ADR 0024) keys on "tablet no longer in
  the map"; during post-restart replay the map transiently *contains* the dropped
  tablet again, so the join-host loop briefly re-hosts an empty zombie group — then
  replay reaches the drop and the GC reclaims it. That round-trip is correct
  (convergent, ids never reused), but a test that one-shot-asserts "files still
  gone" after a fixed post-restart sleep flakes bimodally: it catches the zombie
  mid-flight. Wait for replay to complete (`last_applied == commit_index` ≥ the
  full log via `/admin/raft`), then poll to the converged state — the restart
  instance of the standing "eventual properties get a converged-or-timeout poll"
  rule. (`animusd` `tests/drop_table_gc.rs`.)
- **A new variant in a replicated command enum must be added to every *gating*
  match, not just `apply` — a missed relay allowlist is a bimodal per-process
  flake.** `animusd`'s cross-process proposal path gates on `is_relayable_command`;
  a `MetaCommand` variant missing there **works whenever the connected node happens
  to be the control leader** (proposed locally) and silently times out ("did not
  commit") when it must relay to another node's leader. The compiler can't catch a
  `matches!` allowlist, and single-node tests never exercise the relay. When adding
  a variant, grep the enum's name for gating `matches!`/match sites (allowlists,
  admin filters) and update them in the same change; regression-test the new
  command through a **follower-connected** node in a per-process cluster.
  (`DropTableTablets`; caught by `drop_table_gc.rs`'s 3-node test going bimodal.)
- **Adding a Raft *pre-vote* step changes what a single hand-driven `tick`
  produces — update the election-driving tests, but single-node `tick` stays
  a leader.** Pre-vote (ADR 0009) makes an election-timeout `tick` yield a
  `PreCandidate` + `PreVote` (no term bump), *not* a `Candidate` + `RequestVote`;
  every test that hand-drives a *multi-node* election (`tick` then feed
  `RequestVoteResp`) must now also feed a `PreVoteResp` grant to reach the pre-vote
  quorum first — but a *single-node* group still elects on one `tick` (self is a
  pre-vote majority, which short-circuits straight to the real election), so those
  tests are unchanged. The correctness invariant a pre-vote must hold: it **never**
  mutates a node's term/vote/role (both `PreVote` and `PreVoteResp` bypass the
  step-down-on-higher-term rule) — the sole exception is a *rejecting* `PreVoteResp`
  carrying a higher real term, which reverts a stale pre-candidate to a follower at
  that term. Assert this directly (`pre_vote.rs`: a live-leader lease rejects and
  the term is untouched); the multi-node `SimEnv` teeth is that an *isolated*
  follower's repeated pre-vote rounds leave the stable leader's term unchanged
  (without pre-vote it would ratchet the term every timeout and disrupt on heal).
- **A two-step operation where step 1 is a cheap, always-visible write and step 2 is the expensive, failure-prone "make it real" step must never let a background loop discard a step-2 failure — that silently strands step 1's effect forever.** (Found in the pre-ADR-0028 two-phase `auto_split_loop`; superseded by ADR 0028 — split has no step 2 anymore. Archived in `docs/engineering-lessons-archive.md`.)
- **An `opentelemetry-otlp` exporter's `.with_endpoint(url)` takes `url` as the
  exact, final request URL — it does *not* append the OTLP signal path
  (`/v1/traces`) the way the SDK's own env-var resolution does for the generic
  `OTEL_EXPORTER_OTLP_ENDPOINT`.** Reading that env var by hand and forwarding it
  straight into `.with_endpoint(..)` (ADR 0027's `animusd::otel` seam) silently
  posted every span export to the endpoint's bare root (`POST /`) instead of
  `POST /v1/traces` — a real collector would 404 this with zero indication it was
  a config bug, not a network one, since the exporter reports one generic
  `HttpClient.NetworkError` regardless of cause. Either let the builder resolve
  the endpoint itself (don't call `.with_endpoint(..)` at all — it then reads
  `OTEL_EXPORTER_OTLP_ENDPOINT`/`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and appends
  the signal path correctly), or reproduce the append by hand if the endpoint must
  be threaded explicitly for testability (`animus_db` did the latter, so a test
  seam could pass an arbitrary receiver address without `unsafe`-mutating process
  env). Caught by decoding the exporter's actual protobuf payload in
  `animusd/tests/otel_tracing.rs`, not by the exporter reporting success.
- **`SdkTracerProvider::force_flush`/`shutdown` block the calling OS thread until
  the exporter's HTTP call completes — call them via `spawn_blocking`, never
  directly inside an async fn on a `#[tokio::test]`'s default current-thread
  runtime.** The default runtime has exactly one worker thread; blocking it
  synchronously starves every other task scheduled on it, including a test's own
  in-process receiver task waiting to `accept()`/`read()` the very HTTP request
  the flush is trying to send — a same-process instance of the "don't hold a lock
  across `.await`" deadlock family, just with a blocking call standing in for the
  lock. The symptom is a flush that hangs for its full timeout and then reports a
  generic network error, which reads exactly like a broken exporter rather than a
  starved runtime. (`animusd/tests/otel_tracing.rs`.)
- **A tracing seam wired at one client-facing entry point doesn't cover every
  caller of the primitives underneath it — an internal path that "emulates a
  client" by calling the same primitives directly, bypassing the wrapped entry
  point, needs its own span or context-propagating calls inside it become
  silent no-ops.** `handle_client` wraps every accepted request in a
  `client_request` span (ADR 0027), so `cp_forward`'s
  `otel::current_traceparent()` has an active span to inject when a *client*
  write forwards to another node. The admin bulk seeder
  (`admin.rs::action_data_seed`) calls `ctx.cp_batch_write` directly — never
  through `handle_client` — so it wrote real data with zero spans exported no
  matter how much it wrote, a gap invisible from the code (no error, no
  warning — `current_traceparent()` just returned `None`, its documented
  no-op-when-there's-nothing-to-propagate behavior, indistinguishable from
  "export is disabled"). Fixed by giving it its own `admin_seed` root span
  (mirroring `client_request`'s granularity) with per-chunk `admin_seed_batch`
  children. The general check: when adding a new internal caller of a
  primitive whose forwarding path reads ambient span context, ask "does this
  caller sit under a span at all" — not just "does the primitive still work."
- **Run `cargo test --workspace` after *each* merge, not just at the end of a
  batch.** Batching the gate run let a regression onto main via an earlier
  merge before it was caught. All five gates (fmt, clippy `--all-features
  -D warnings`, build, test, `cargo deny`) green per merge.
- **`cargo deny` can be silently broken** (e.g. the repo's own `AGPL-3.0-only`
  missing from the allow-list) and it can't run in every local env — CI runs it;
  treat it as a real gate, not optional.
- **When a CI check fails on your PR, first check whether it also failed on
  already-merged PRs — a check that has *never* passed is misconfigured, not a
  verdict on your change.** The DCO check failed identically on every PR
  (including merged ones) because the workflow invoked `tim-actions/dco` without
  its *required* `commits` input (instant `Unexpected end of JSON input`), and —
  second layer, only visible after the first fix — the repo's restricted default
  token lacked `pull-requests: read` for the commit-listing step (`Resource not
  accessible by integration`). Two generalizable checks: (1) an action's
  README/`action.yml` lists required inputs — a bare `uses:` of an action that
  needs inputs fails on every event; (2) a workflow calling the REST API needs
  its permissions declared explicitly under restricted default-token settings.
  Verify a workflow fix by letting the PR that changes it exercise itself
  (`pull_request` workflows run from the PR's merge ref). (PR #83.)
- **Don't `git add -A` while resolving a merge** — it can sweep agent worktree
  dirs in as embedded git repos. Stage explicit paths; `.claude/worktrees/` is
  gitignored to prevent it.
- **Doc files (`CLAUDE.md`, ADRs) conflict predictably** when parallel changes
  each edit the "what remains" lists — resolve by *unioning the done-states*
  (each side is usually stale only for the *other* change's feature).
- **`ProposeResult::Accepted` means "appended to the leader's local log," never "committed" — every proposer must confirm, and a bare boolean flag isn't always enough to confirm the caller's *specific* request.** (Found in the pre-ADR-0028 `propose_split_data`/`applied_split_key`, since removed; `cp_put_local`'s confirm-by-index is the still-live instance of this lesson. Archived in `docs/engineering-lessons-archive.md`.)
- **A retry loop over a Raft write must distinguish "never accepted, retry is
  free" from "accepted, unconfirmed" before resubmitting — the latter doubles
  outstanding work under exactly the conditions that caused the timeout.**
  Diagnosing `--auto-split 2000` failures that looked like a runaway/election
  storm, a live reproduction (isolated cluster, sustained bulk-seed under
  load) showed every Raft term — control plane and every per-tablet CP group
  — stayed flat the whole time; `commit_index` kept climbing well past
  individual write attempts already reported as failed. So the writes weren't
  stuck, just slower than the 10s client timeout (measured ~12-27ms fsyncs on
  this host vs. sub-ms on real NVMe — a slow/virtualized disk under a growing
  number of independent per-tablet Raft WALs). The admin bulk-seeder's retry
  loop (`action_data_seed`) turned that slowness into a pile-up: on **any**
  `cp_batch_write` error, including a bare confirm-timeout, it resubmitted the
  same entries — but `ProposeResult::Accepted` only means appended to the
  leader's local log, not committed, so a confirm-timeout after `Accepted`
  almost always means "still committing," and resubmitting appends a
  **second, fully duplicate** Raft entry for the same data on top of one that
  was probably going to land anyway — safe by per-key LWW, but it doubles
  fsync/replication load, compounding under the very slowness that caused the
  timeout. Fixed by splitting propose from confirm
  (`cp_batch_propose`/`poll_probe` in `animusd`) so a patient retry
  (`cp_batch_write_patient`) can poll an already-accepted entry a second time
  instead of re-proposing, while still proposing fresh on a genuine routing
  failure (leader moved — e.g. a tablet split mid-seed, where `cp_route`
  re-resolving on each attempt is exactly what's needed). General check for
  any retry loop wrapping a Raft write: does a bare timeout distinguish
  "definitely not accepted anywhere" from "accepted, just slow"? If not, a
  slow/contended commit path gets a retry storm instead of patience.
  **This recurred immediately in a sibling code path** (superseded by ADR
  0028 — `auto_split_loop`'s `pending` map and `propose_split_data`/
  `propose_and_confirm_split`/`cp_split_here` no longer exist; retained for
  historical record) — worth treating as a
  *pattern* to sweep for, not a one-off: `auto_split_loop`'s `pending` map
  (the step-2 `propose_split` retry) has the identical shape, just already
  half-fixed — `confirm_split` was already a poll-only primitive (propose and
  confirm were never fused there the way `cp_batch_local` fused them), but the
  retry loop still called `propose_split_data` (propose **and** confirm)
  fresh on every ~2s tick regardless of whether the prior attempt reached
  `Accepted`. `Split` apply is idempotent (a group splits once; re-application
  is a no-op) so this was never a correctness bug, purely a wasted-work one —
  same fsync/replication doubling, same live-repro signature (flat Raft terms,
  `commit_index` still climbing). Fixed the same way: `propose_and_confirm_split`
  takes a `confirm_rounds` count, and the pending-retry call (plus
  `cp_split_here`, the cross-process counterpart, which can't tell if its
  caller is about to retry) passes 2 instead of 1 — poll the already-accepted
  entry a second time before the *next* tick would otherwise re-propose.
  Lesson beyond the original one: when a retry-amplification bug is found and
  fixed in one place, grep for the same *shape* (propose-then-poll, called
  again from a loop on bare timeout) elsewhere in the same subsystem — it is
  rarely truly a one-off.
- **The "sweep for the pattern" advice above was followed for two known sites
  and still missed the actual common root: the shared helper both of them (and
  most other schema proposals) sit on top of.** `ClientCtx::propose_and_await`
  — the generic "propose a `MetaCommand`, poll `Metadata` for its commit"
  helper backing `propose_split_metadata`, `register_node_addrs` (formerly
  `register_cp_addr`, superseded by ADR 0032 PR1),
  `create_table_schema`, `replace_table_schema`, `drop_table`, and
  `drop_table_schema`'s own hand-rolled copy of the same loop — called
  `propose_schema` unconditionally on **every** `SCHEMA_POLL_INTERVAL` (50ms)
  tick regardless of whether the previous call had already reached a leader's
  log, for up to `SCHEMA_COMMIT_TIMEOUT` (10s) ⇒ up to ~200 duplicate proposals
  per call. `SplitTablet` apply's `new_id`-exists guard makes a duplicate
  harmless (cleanly rejected), so this was pure wasted WAL/replication work —
  but under `--cluster N`'s auto-split loop running on every node concurrently
  (see the sibling cross-node-contention entry), that waste compounds directly
  into the 10-minute-long "split metadata did not commit in time" stalls seen
  live: three nodes' independent retry storms flooding the control-plane log
  fast enough that nothing drains within any single 10s window. Fixed by
  having `propose_schema` report whether it has reason to believe the command
  reached a leader's log (a local `Accepted`, or a relay that didn't visibly
  error) and having `propose_and_await` only resubmit immediately when it
  knows the prior attempt went nowhere, otherwise backing off
  `SCHEMA_PROPOSE_PATIENCE` (1s) before trying again — mirroring
  `propose_and_confirm_split`'s confirm-before-resubmit shape one level up the
  call graph. **Corollary: "sweep for the pattern" means grep the shared
  primitives a retry loop calls into, not just the two sites a bug report
  named** — the pattern's most common instance was hiding one layer below
  where it had already been fixed twice.
- **A "only the owning node acts" gate near a shared registry must check whether the registry actually distinguishes callers by node — or it silently answers "does anyone in the cluster satisfy this," not "do I."** (The sibling cross-node-contention bug referenced above: `auto_split_loop`'s `ctx.edge.cp_leader(tablet)` gate, scoped to the shared `--cluster N` registry rather than per-node. Superseded by ADR 0028, which removed the two-phase split contention this guarded; archived in `docs/engineering-lessons-archive.md`.)
- **An "abandon and forget" exit from a retry loop must still leave the cooldown state a *fresh* attempt would have set — otherwise the resource is eligible again on the very next tick, not after backing off.** (Found in the pre-ADR-0028 `auto_split_loop` abandon path, since removed; archived in `docs/engineering-lessons-archive.md`.)
- **A `spawn_task`'d background disk-I/O task must be gracefully joined via its
  own `is_stopped()`-style contract before `Env`/runtime teardown — an outer
  `AbortHandle::abort()` is not enough, because it races the runtime's own
  blocking-pool teardown and can surface as a raw runtime-internal panic
  instead of a clean stop.** `animusd`'s Ctrl-C path (`shutdown_graceful`)
  flushed the control-plane WAL, then called `Node::shutdown()`, which
  `ProdEnv::shutdown()`-aborts every task the node's two internal envs own —
  including the CP-data apply task (`animus-cp-data`'s `apply_and_compact`).
  `RaftKvNode::shutdown()`/`CpGroup::shutdown()` already document "a graceful
  driver halt, not a kill" (a flag observed *between* full apply passes), and
  the drop-table GC path (`cp_gc_tablet`) already uses the correct
  shutdown-then-poll-`is_stopped()` pattern before touching files — but
  process-level teardown skipped it and went straight to the hard abort. If
  the apply task was mid-`storage.merge(..).await` (a `tokio::fs` op, which
  internally runs on tokio's blocking thread pool), aborting the task while
  its blocking op was still in flight surfaced as a `tokio`-internal panic —
  `Backend("background task failed")` / `Backend("task was cancelled")` — on
  every real `animusd` shutdown, harmless to durability (an un-acked write
  just isn't durable yet) but a noisy, uncontrolled crash instead of a clean
  exit. Fixed by adding `ClusterEdgeState::shutdown_all_cp_groups` (snapshot
  the registered handles out of the lock, call `.shutdown()` on each, then
  poll `.is_stopped()` bounded by `CP_GC_STOP_TIMEOUT` — the exact
  `cp_gc_tablet` shape) and calling it from `shutdown_graceful` before the
  hard-abort `shutdown()`. **General check: when a component documents its own
  graceful-stop contract, make sure every caller that tears it down — not just
  the one call site the contract was originally written for — actually uses
  it**, especially a process-exit path that looks unconditionally safe because
  "the process is exiting anyway." [`cp_gc_tablet` itself is gone as of ADR
  0031 PR4 — its shutdown-then-poll-`is_stopped()` shape now lives in
  `animus_cp_data::host::Reconciler`'s own `Release`/`Reclaim` teardown; the
  lesson and `ClusterEdgeState::shutdown_all_cp_groups` this entry added are
  otherwise unaffected and still current.]
- **A cached per-node handle derived from replicated state needs an explicit re-sync step for every way that state can change in place — "it was correct when constructed" is not "it stays correct."** (Mechanism superseded by ADR 0031 PR4 — the reconciler's planner now emits an explicit `NarrowScope` action instead of a per-tick patch-up. Archived in `docs/engineering-lessons-archive.md`.)
- **A feature whose only enabling *registration* path is shaped by the startup
  config silently caps the cluster at its born size — and a test that "proves"
  growth by starting every node up front only proves the planner, never the
  actual growth path.** ADR 0029's rebalancer worked perfectly in
  `cp_rebalance.rs` (5 nodes started together, `Active` from bootstrap) — but a
  cluster grown *after* bring-up had no path in at all: `bootstrap` computes
  the raftkv ids it registers from `control_ids.len()` at the process's own
  start, so a node added later is never proposed as a member by anyone, ever.
  The tell was in the problem statement, not the code: "the passing grow-test
  starts all 5 nodes up front by its own admission" is a giveaway that the
  test exercises the *decision* (given a balanced-vs-imbalanced membership,
  does the planner converge) but never the *registration* that would put a
  genuinely-new node into that membership in the first place. General check
  when auditing "does X actually support growth/scale-out": find the one
  function that turns "a node exists" into "the system knows about it," and
  ask whether it can only ever run at the size the system started at.
  Delivering online growth (ADR 0030) then surfaced a second-order version of
  the same lesson: hardening a *different* gap (a declared-but-never-booted
  node staying a permanent placement-eligible phantom, since the failure
  detector only judges members it has heard from) by making the detector treat
  an untracked `Active` member as demotable broke several *existing*
  `animus-control` sim tests that had, for years, modeled "Active data members"
  by proposing `UpsertMember` directly with **no heartbeat simulated at all** —
  a fine way to test placement logic in isolation right up until a change
  makes "declared but silent" meaningfully different from "declared and about
  to heartbeat." A change to shared detection/liveness semantics needs its
  blast radius checked against every test that manages membership *without*
  wiring up the corresponding liveness mechanism, not just the tests for the
  feature being changed. (`animusd::bootstrap`,
  `animus-control::node::detect_loop`; `animusd/tests/cluster_growth.rs`;
  `animus-control/tests/{placement_auto_reconcile,placement_rebalance,
  placement_reconcile,prod_liveness}.rs`.)
- **A teardown that erases "my own scope" must re-derive the scope from replicated state at the point of irreversible action — not trust an in-memory cache that a *different* code path is responsible for keeping current.** (Mechanism superseded by ADR 0031 PR4 — `HostAction::Release` now carries the erase bound directly, computed by the one planner. Archived in `docs/engineering-lessons-archive.md`.)
- **A safety mechanism that exists and is unit-tested but has zero production
  callers is dead code with a green suite — second instance of the
  `narrow_scope` pattern above, on the *write* side this time.** ADR 0028's
  crossover-window write fences (`RaftKvNode::put_fenced`/`delete_fenced`/
  `put_batch_fenced`, a `fence: KeyRange` embedded in the proposed command
  and checked at apply time) landed additively with a thorough sim suite
  (`animus-cp-data/tests/fenced_commands.rs`) — but `grep -rn "_fenced"
  crates/animusd/src` found **zero** callers: `cp_put_local`/
  `cp_delete_local`/`cp_batch_propose` (reached by every client write,
  including every `cp_serve_forwarded` counterpart) all called the
  *unfenced* `put`/`delete`/`put_batch`, which stamp `fence =
  KeyRange::whole()` — so the apply-time check was a permanent no-op in the
  one place it needed to matter. The trigger: a node whose `Metadata` view
  hasn't yet observed a `SplitTablet` commit still resolves a child-range
  key to the parent's (now too-wide) group via `cp_route`'s `Local` branch
  (no re-resolution once routed); the unfenced write then applies onto the
  shared engine's physical key the child now logically owns, shadowing or
  corrupting it via LWW — invisible to every existing test because nothing
  drove a write into that specific crossover window. Fixed by adding an
  additive `RaftKvNode::scope_range()` accessor (a `StorageScope::range()`
  getter underneath) and stamping it as the fence on every real proposal.
  **The sharper lesson is the second half of the fix, not the wiring
  itself:** the fence alone is not sufficient, because `cp_put_local`/
  `cp_delete_local` confirm success by polling for the proposed value (or
  its absence) to read back from **local** storage — and a fenced-out entry
  still commits and applies as a deterministic no-op, silently advancing
  any coarser "did this commit" signal (e.g. `engine_applied_index()`
  alone) right along with it. Had the confirm loop been keyed on such a
  signal instead of exact value equality, wiring the fence alone would have
  turned "silently corrupts the child" into "silently falsely-acks a write
  that never happened" — a *different* silent-failure mode, not a fix. The
  actual fix pairs the fence with a **pre-propose range check**: reject
  before ever proposing if a key falls outside the group's current
  `scope_range()`, returning the same error shape a routing failure already
  produces so the caller's retry re-resolves `cp_route`; the embedded fence
  then only has to cover the much smaller residual race between that check
  and the entry's actual apply. **General checks this generalizes to:** (1)
  when auditing a safety mechanism for "is it wired in," also ask "does the
  *confirmation* path downstream of it use a signal precise enough that a
  mechanism turning a write into a no-op is distinguishable from the write
  actually succeeding" — a coarse confirm signal can convert a newly-fixed
  correctness bug into a new, differently-shaped one; (2) a regression test
  for this class of bug needs access to the private routing internals (here,
  a specific tablet's `CpGroup` handle) to *force* the stale-routing shape
  deterministically, since the real race is not reliably reproducible over
  wall-clock timing — when the integration crate under `tests/` can't reach
  what's needed (its types are only `pub(crate)`/private), an **in-crate**
  `#[cfg(test)] mod` (a child module of the module holding the private
  items, hence able to see them) is the right tool, not a workaround.
  (`animus-cp-data::RaftKvNode::scope_range`; `animusd`
  `cp_put_local`/`cp_delete_local`/`cp_batch_propose`,
  `split_fence_tests::stale_routed_write_for_a_split_childs_key_is_rejected_not_lost`.)
- **A change-notification primitive built on a monotonic watermark re-checked
  fresh on every poll, instead of a one-shot consumed flag, eliminates the
  wake-before-park race class by construction — no special-case handling
  needed.** `animus-cp-data`'s `ProposeSignal` (wake-on-propose) is a flag: it
  registers the waker, checks-and-swaps an `AtomicBool`, and — like any
  consumed-flag design — depends on the register-before-check ordering to
  avoid losing a wake that lands between "check" and "park." Building
  `animus-control::RaftNode::metadata_watch()` (ADR 0031 §trigger, a *caller*-
  facing "has the applied index moved past what I last saw" notification
  rather than a single internal consumer's wake), the natural shape is instead
  an `AtomicU64` watermark: `changed(last_seen)`'s `poll` just checks
  `current > last_seen` — true state, not a consumed edge — so a change that
  already happened before the future was ever created or polled resolves
  immediately on the very first poll, with no dependence on registration
  timing at all. The register-before-check discipline is still followed (for
  the case where the change happens *after* the first poll, before a
  subsequent one), but the *design* no longer has a race to reason about for
  the "already happened" case — it isn't consuming evidence that could be
  consumed by nobody. General rule when building a wake primitive: if the
  "did the awaited thing happen" question can be phrased as a comparison
  against a monotonically increasing counter/index/version (not just "did an
  edge fire"), prefer that framing — it is strictly more robust than a
  one-shot flag and costs nothing extra (a `fetch_max` instead of a `swap`).
  Keep the flag-consuming shape only when the event genuinely has no ordered
  value to compare against (a bare "something happened, go do your own
  re-check" nudge, which is what `ProposeSignal` actually needs — the
  consensus loop doesn't care *how many* proposals queued, only that it should
  wake up and drain). (`animus-control::node::MetadataWatch`.)
- **When extracting a pure planner over a retry-until-success async teardown
  loop, the planner must NOT eagerly mutate its own successor state to reflect
  "the action I just emitted will succeed" — because the real execution is
  async and can fail/time out, and the planner has no way to know.** Porting
  `animusd`'s `cp_gc_tablet`-driven reclaim/release teardown into
  `animus-cp-data::host::plan` (ADR 0031 PR3), the real code only removes a
  tablet from its `minted` claim set *after* shutdown + erase + WAL deletion
  all actually succeed (a timeout re-registers the handle and leaves `minted`
  untouched, so the next tick retries the whole teardown). A naive pure
  `plan(state) -> (actions, next_state)` that removes the tablet from
  `LocalState::hosted` the moment it emits a `Reclaim`/`Release` action would
  silently break that retry contract the instant the caller wires it in: a
  timed-out teardown's tablet would vanish from `hosted` in the *returned*
  state regardless, and if the caller trusts that as ground truth for its next
  `plan` call, the tablet is never revisited again — a permanent leak with no
  error, indistinguishable from a successful teardown from the planner's own
  point of view. Fixed by leaving `hosted` untouched for `Reclaim`/`Release`
  and giving the caller an explicit `LocalState::confirm_torn_down` to call
  **only** once its own async teardown has actually completed — so an
  un-confirmed action is simply re-planned identically on the next call,
  mirroring the real loop's tick-based retry exactly. General check when
  extracting a pure "decide what to do" function out of a loop that also does
  fallible I/O for the same resource: does the loop's *bookkeeping* removal
  happen at decision time or at confirmed-completion time in the original code
  — if the latter, the pure function's successor state must preserve that
  asymmetry (add eagerly is fine when the action can't practically fail;
  remove must wait for a caller-reported confirmation). Verified with a
  dedicated unit test that drives `plan` twice without confirming and asserts
  the identical action re-appears, then confirms and asserts it stops.
  (`animus-cp-data::host::{LocalState::confirm_torn_down, plan}`;
  `host::tests::a_pending_reclaim_is_replanned_until_confirmed_torn_down`.)
- **When replacing N polling loops with one event-driven watch, inventory the
  consumers whose watch source structurally never fires before deleting the
  polls — the periodic fallback arm is load-bearing for them, not a safety
  net; and any guard that gates the new unified loop must be keyed on *every*
  node-type's own signal, or it permanently blocks the type it wasn't written
  for.** Wiring the ADR 0031 PR4 reconciler trigger
  (`select!(metadata_watch.changed(..), sleep(500ms))`), two growth-node (ADR
  0030) hazards were only visible by asking "for which consumer does the
  watch never fire": (1) a growth node's own control raft never advances (a
  permanent non-voter of a group it never replicates), so `metadata_watch`
  never wakes it — only the fallback tick ever drives its reconciler, reading
  the `remote_metadata_sync_loop` mirror via `effective_metadata()`; deleting
  the old fixed-period loops without the fallback would have silently frozen
  every grown node's tablet hosting forever, with zero errors. (2) The
  pre-recovery guard the old GC loop used (`raft.last_applied() == 0` → skip,
  so a default-empty pre-recovery `Metadata` doesn't read as "everything
  dropped") is keyed on exactly the signal a growth node never raises — so
  the unified loop's guard had to become `last_applied() == 0 && remote
  mirror is empty`, or the same guard that protects a normal node's restart
  would have blocked a growth node's reconciler from ever ticking at all.
  Also: after any watch-arm wake, coalesce to the source's freshest value
  (`watch.latest()`) rather than the value the future resolved with — a
  burst of commits under bulk load must collapse into one reconcile tick,
  not one per applied entry. (`animusd::tablet_host_reconciler_loop`,
  `RECONCILE_FALLBACK_INTERVAL`; `tests/cluster_growth.rs` is the regression
  that proves the growth node still functions.)
- **Widening a process-start-immutable field into a live, periodically
  re-synced one: change the field's *type* first, then let the compiler
  enumerate every consumer — don't grep for call sites by hand.** Making
  `ClientCtx.client_route` (a plain `BTreeMap`, filled once at node start)
  live (ADR 0032 PR1, closing ADR 0030's `client_route`-staleness gap) meant
  wrapping it in `Arc<Mutex<_>>` and adding a `route_sync_loop` sibling to the
  already-proven `peer_sync_loop` (same static-seed-∪-replicated-overlay
  shape, same cadence). Every direct `.get()`/`.values()` access to the old
  plain-map field (`cp_forward_target`, `propose_schema`'s relay + broadcast
  fallback, a routing fallback search, the growth-node
  `remote_metadata_sync_loop` seed computation) became a **type error** the
  moment the field's type changed, so the compiler itself produced the exact
  call-site list — a mechanical, self-auditing sweep, unlike the many
  documented gaps in this codebase that are *silent* to the compiler (a
  missing `is_relayable_command`/`cp_serve_forwarded` match arm, a stale
  cached invariant). Route every such access through small
  lock-scoped accessor methods (`route_addr`/`route_snapshot`, cloning out
  under the lock) so no caller can end up holding the guard across an
  `.await`. The *test* fallout from the same change was not compiler-caught,
  though: a test asserting directly on the **superseded** state
  (`cp_member_addrs`, no longer populated by `animusd`'s own startup path
  once `RegisterNodeAddrs` replaced `RegisterCpAddr` as the self-registration
  command) failed at runtime, not compile time — when retiring a producer in
  favor of a superset command that keeps the old command only for WAL
  back-compat, grep tests for direct field/assertion checks on the
  old-producer's output, not just callers of the old propose function.
  (`animus-control::meta::NodeAddrs`/`RegisterNodeAddrs`; `animusd`
  `route_sync_loop`; `tests/cp_plane.rs::cp_member_addresses_register_and_replicate`.)
- **In a multi-refusal admin action that is deliberately local-leader-only,
  check leadership FIRST — every other refusal that reads local `Metadata`
  is only trustworthy once leadership is confirmed, since a follower's
  replica can genuinely lag the leader's own just-committed state.**
  `ClientCtx::admin_remove_member` (ADR 0032 PR3 decommission) originally
  checked "is the member drained" (via `self.raft.metadata()`) before
  checking "am I the leader" (`self.edge.leader_handle()`) — reads that
  happened to agree on the *leader* node (where `self.raft` and the leader
  handle are the same underlying core), but on a **follower** under load a
  just-converged release-GC move can still be in flight over Raft
  replication, so the follower's own stale metadata reported "still
  referenced by 1 tablet" instead of the intended "not the control-plane
  leader; retry on the leader" routing error — the wrong refusal reaching the
  operator, not a wrong *decision* (the follower correctly refused, just for
  a misleading reason). Invisible in an isolated single-test run (no
  contention, replication is near-instant); it flaked exactly once under
  `cargo test --workspace`'s parallel load, the same class of timing hazard
  the "flaky ProdEnv test is a real bug" rule already covers, just showing up
  as a wrong error string rather than a wrong outcome. Fix: check leadership
  before any metadata-dependent refusal, mirroring "resolve the authority
  first, then ask it questions" — the same shape as checking `is_leader()`
  before trusting a quorum-derived fact elsewhere in this codebase.
  (`admin_remove_member`; `tests/decommission.rs`'s follower-refusal
  assertion.)
- **A rebalance-dependent test needs enough independent tablets that the
  pre-growth cluster is *not already balanced* — one table can leave a
  joined/grown node with zero replicas forever, not just an ambiguous
  choice of which table to route through.** `rebalance_step` only proposes a
  move while it improves the *global* `max − min` imbalance; with exactly
  one table (one tablet, RF = the pre-growth node count) every pre-growth
  node already holds exactly one replica and the joined node holds zero —
  `max − min == 1`, already at the stopping condition, so the rebalancer
  never moves anything and a test polling for "the joined node gained a
  replica" times out completely (not flakily — every run). This is a
  sharper version of the already-documented "the rebalancer converges the
  *global* imbalance and makes no per-table promise" lesson (which is about
  which table a test must route through once *some* replica has moved) —
  the additional wrinkle is that with too few tablets, the imbalance can be
  zero from the start and *no* replica ever moves. Fix: seed several
  independent tables (`tests/decommission.rs` uses three, mirroring
  `tests/seed_join.rs`'s `TABLES`), so the pre-growth distribution is
  imbalanced enough to guarantee at least one move onto the new node.
- **When two different root causes produce the identical observable absence,
  don't try to reconstruct which one happened from the remaining state —
  record an explicit signal at the moment the distinction is still known.**
  Wiring tablet merge (ADR 0033, the data-plane dual of ADR 0028's split), a
  per-node reconciler observing "a tablet I used to host vanished from the
  replicated tablet map" must react completely differently depending on
  *why*: merged into a sibling (tear the group down, but the data is still
  live — a survivor now serves it on the same shared engine, so **never
  erase**) vs. the whole table dropped (tear down **and erase** — nothing is
  left to serve that range). Both produce the exact same absence from
  `Metadata.tablets`, and the tempting inference — "does some other tablet's
  range now cover mine, so it must be a merge survivor" — is unsound: two
  different tables' still-unsplit tablets can have byte-identical default
  ranges (`KeyRange::whole()`), and by the time the reconciler is deciding
  what to do, the vanished tablet's own table identity is gone from view too
  (it's not in the map anymore), so there's no way to disambiguate a
  same-table survivor from an unrelated table's coincidentally-matching
  tablet. The fix was a tiny, explicit, **permanently-retained** replicated
  marker (`Metadata::merged_tablets: BTreeSet<TabletId>`, ADR 0033) set at
  the one moment the distinction is unambiguous (the `MergeTablets` apply
  itself, which knows exactly which tablet it just absorbed) — cheap because
  tablet ids are never reused (so the marker never needs pruning and can
  never resurrect a wrong decision for a later id), and correct by
  construction instead of by inference. **General check when a planner reacts
  to "X disappeared" from a coarser view: are there multiple legitimate
  reasons X can disappear that demand different actions, and if so, is there
  actually enough information left in the coarser view at decision time to
  tell them apart — or does the distinguishing fact need to be captured
  explicitly, closer to where it was still known, even at the cost of a
  small permanent marker?** (`animus-control::Metadata::merged_tablets`;
  `animus-cp-data::host::{HostAction::Absorb, MetadataView::merged}`.)
- **Tearing down a Raft group whose data will keep being SERVED (not erased)
  must drain the group's committed log into the engine first — `shutdown()`
  halts the async apply task at its next loop-top check WITHOUT draining, and
  deleting the group's WAL then destroys the only local copy of the
  committed-but-unapplied tail.** Found via ADR 0033's own 3-node merge
  integration test flaking ~1-in-5 *in isolation* (per the standing rule, a
  flaky `ProdEnv` test is a real bug): a write acked by the absorbed group's
  leader right before the merge was applied to *that leader's* engine (ack
  requires leader-local apply) but not yet to a follower's — commit-index
  propagation runs up to one heartbeat behind, while the reconciler's
  event-driven `metadata_watch` fires the `Absorb` teardown on the very
  commit that made the merge visible, i.e. *designed* to race that window.
  The follower's engine then permanently lacked the acked key, and if that
  node hosted the merge survivor's leader, linearizable reads answered a
  definitive "key absent" forever — indistinguishable from data loss. The
  same non-draining shutdown is **harmless for `Release`/`Reclaim`** (their
  teardowns erase the data anyway; other replicas serve) — which is exactly
  why it was never noticed: the invariant "a torn-down group's unapplied
  tail doesn't matter" was true for every teardown that existed before merge
  added one whose data lives on. Three-part fix, each load-bearing: the
  `Absorb` teardown drains (commit covers the local log, engine-applied
  covers commit) while the driver is still live; `plan` defers the
  survivor's `WidenScope` until the absorb confirms (drain-before-widen —
  the planner's fixed emission order alone would have widened *first*); and
  the read path stopped conflating two "None"s — a ReadIndex barrier
  failure and a genuinely-served absent — plus gained the read-side dual of
  ADR 0028's pre-propose range check (a get/scan whose group's live
  `scope_range()` doesn't contain the request errors retryably; for scans
  the un-widened scope was otherwise a *silent truncation*, since
  `linearizable_scan` filters rows through the live scope). **Two general
  checks: (1) when a new feature makes a previously-universal teardown
  invariant ("this group's data dies with it") false for one new path, audit
  the teardown's every step against the new path — the WAL delete that was
  cleanup before is data loss now; (2) grep read paths for `Option`-collapse
  points where "couldn't serve" and "served: absent" merge into one value —
  the Get/Scan arm asymmetry (Get mapped `None` to absent, Scan mapped it to
  an error) was the tell.** The deterministic regression drives the write →
  merge-view tick with zero intervening sim time, so the apply task provably
  hasn't run — no wall-clock race needed.
  (`animus-cp-data::host::Reconciler::teardown`'s Absorb drain + `plan`'s
  `absorbing` gate; `RaftKvNode::linearizable_get_served`; `animusd`
  `cp_get_local`/`cp_scan_local`; regressions:
  `reconciler_corpus.rs::scenario_merge_widens_and_absorbs`,
  `host::tests::widen_is_deferred_while_the_absorbed_sibling_is_still_hosted`,
  `animusd` `split_fence_tests`' read/scan duals.)
- **A "weighted median via one accumulate-and-threshold pass" is only correct
  when no single item can dominate half the total weight — once one can,
  scan every achievable cut point and pick the closest to half, don't commit
  to whichever side a running sum happens to cross the threshold on.**
  Building the byte-weighted split point for ADR 0034's byte-based
  auto-split (replacing the plain positional median with one that bisects a
  materialized tablet's *bytes*, not its key count, under skewed value
  sizes), the obvious-looking first implementation walked pairs in order,
  accumulated a running byte total, and returned the first key at which the
  running total reached half the whole. That is subtly wrong whenever one
  key's own value is a large fraction of the total, because it commits to
  the *first* crossing instead of comparing it against the *next* candidate
  cut: 20 tiny keys totaling 100 bytes, then two huge keys y0/y1 of ~10,000
  bytes each (total 20,104, half 10,052) — the naive walk returns y0 as the
  split key the instant the running total (100 + y0's ~10,002) first crosses
  half, giving a 100-byte/20,004-byte split (the 20 tiny keys vs. both huge
  ones); but the *very next* candidate cut — after y0 instead of before it —
  gives 10,102/10,002 (the tiny keys + y0, vs. y1 alone), far closer to
  even. The naive walk can never find this, because once it has returned at
  the first crossing it never looks at the next candidate to see if it's
  actually closer. The fix scans every
  achievable interior cut point (a key boundary, since a key's own bytes can
  never be split) and keeps whichever prefix sum is closest to half — the
  best any key-boundary split can do, and a strict improvement discovered
  only by writing a unit test with a deliberately skewed distribution and
  checking both sides' actual byte shares, not just "did a split happen."
  (`animusd::byte_weighted_median`; `auto_split_median_tests`.)
- **A new `StorageEngine` trait method with a working default (not just a
  stub) lets every existing/future implementor answer immediately, and only
  the one backend that needs a cheaper path has to override it — the
  `merge_batch` precedent, reused.** Adding `approx_bytes_in_range` (ADR
  0034) for the byte-based auto-split trigger, the default implementation is
  simply *exact* (scan the range and sum key+value lengths) — correct for
  `MemoryEngine` (and any future engine) for free, with no `Option`/`None`
  fallback needed anywhere upstream. Only `LsmEngine` overrides it with a
  cheap, non-materializing estimate from metadata it already holds
  (memtable range-query + SSTable-overlap `file_size` sum), which is where
  the actual "cheap, not exact" tradeoff belongs. This is strictly better
  than the older sibling pattern (`CpGroup::approx_key_count`, LSM-only,
  returns `None` on the memory backend) for any *new* per-engine estimate —
  prefer "default = exact, override = cheap" over "default = absent" when
  the exact computation is itself cheap enough for a non-hot-path backend.
  Related: an unbounded-above logical range (`KeyRange.end: None`, the
  common "one big not-yet-split tablet" case) must not degrade a scoped
  estimate into an engine-wide scan just because the *logical* range has no
  upper bound — `StorageScope::physical_bounds` computes a bounded
  **physical** upper bound via the standard prefix-upper-bound trick
  (increment the last non-`0xFF` byte of the scope's own key prefix) instead
  of falling back to `entries()` the way the one-time `has_data` check
  tolerates. A cheap per-tick gate and a one-time hosting-decision check can
  reasonably make different cost tradeoffs for the same "unbounded range"
  shape — check which one you're building before reusing the other's
  fallback.
- **Before implementing a task framed as "close this documented gap," grep the
  actual code — an ADR/CLAUDE.md's "still deferred"/"future work" language can
  lag well behind a fix that already shipped.** Tasked with closing ADR 0013's
  "index entry data isn't replicated, so a restarted/uninformed node's GSI
  query silently returns incomplete results" gap, the lazy
  backfill-from-base-table-scan design the task asked to *evaluate* had
  already been implemented and end-to-end tested (`animusd`'s
  `backfill_index_if_needed`/`SchemaRegistry::backfilled`, commit `46e25b5`) —
  only the ADR's "Still deferred" section and the crate's own `CLAUDE.md` bullet
  had never been updated to say so. Grepping for the gap's own likely
  mechanism names (`backfill`, `sync_indexes`, the registry struct) before
  writing new code turned "implement X" into "harden X's one remaining edge
  case and fix the stale docs" — a much smaller, correct-scoped change than a
  reimplementation would have been (and a reimplementation risks silently
  reverting a previously-fixed bug the existing tests already guard).
- **A lazy backfill that scans the base table *without holding* the
  maintenance-state lock (so the scan doesn't stall concurrent writes) must
  not let its replay blindly overwrite a key a real write already touched more
  recently — the replay's snapshot is stale by construction.** The DynamoDB
  GSI/LSI backfill (above) runs a network scan with the registry unlocked,
  then replays every scanned `(key, value)` through `note_put` under the lock;
  a concurrent write's own `note_put` (reflecting the item's *current* value)
  can land in between, and since both calls target the same registry method,
  whichever runs *last* wins — if that's the replay, it silently reverts the
  real write's already-correct index entry to the pre-write value, with no
  error and no signal that the write's index update was ever undone (the base
  item stays correct; only the *index's* bookkeeping regresses, which is what
  a later `Query` against the index reads). Fixed with
  `SchemaRegistry::touched_since_backfill`: every `note_put`/`note_delete`
  marks its key while a backfill is pending, and the replay skips any key
  already found there rather than reapplying its own scanned value — so the
  replay can only ever *seed* a key nobody has independently indexed
  correctly, never *revert* one. Cleared on `mark_table_backfilled` so the
  tracking set stays bounded to the (normally brief) in-flight window, not the
  table's lifetime. General shape to watch for: any "scan without the lock,
  then replay under the lock" pattern needs an explicit "was this touched
  more recently than my scan" check before the replay writes anything, or a
  race that regresses already-correct state passes silently (proven via a
  deterministic *unit* test replaying the exact call order by hand — no
  wall-clock timing needed to demonstrate a lock-ordering race like this one).
  (`animus-dynamo::registry::{SchemaRegistry::touched_since_backfill,
  TableState::touched_since_backfill}`; `animusd::dynamo::
  backfill_index_if_needed`; ADR 0013.)
- **When a general-purpose accessor is refactored to become mirror/cache-tolerant,
  grep every internal caller of that accessor for one that is actually a
  commit-wait/read-your-writes poll on its own just-proposed command — the poll
  must keep bypassing the new tolerance, not just the one example the task
  happened to name.** Building the ADR 0035 PR1 `ControlHandle` seam, the task
  named exactly two call sites that must stay fresh (a `CreateTable`
  commit-wait poll, a DynamoDB conditional-write existence gate) as the
  rationale for adding a `metadata_fresh()` read alongside the new
  cache-tolerant `effective_metadata()`. But the *shared helper methods*
  those two examples sit next to (`ClientCtx::table_schema`/`has_table_schema`,
  and `dynamo.rs`'s free `metadata(ctx)` fn) are each called from **two kinds
  of site at once**: plain lookups (which should become mirror-tolerant, the
  actual growth-node bug being fixed) and a proposal's own commit-wait
  predicate (which must not, or the poll could pass "committed" off a mirror
  that's still a poll interval behind, or wait forever on one that's stuck).
  Switching the shared helper's body wholesale would have silently broken
  every commit-wait poll built on top of it — including `drop_table_schema`,
  which wasn't one of the two named examples but has the exact same shape as
  the one that was (`create_table_schema`). The fix is to keep the general
  helper cache-tolerant and have every internal commit-wait caller bypass it
  with an explicit fresh read instead of routing through the helper — found by
  tracing every caller of the helper being changed, not by trusting the task's
  own worked examples to be exhaustive. (`animusd::control_handle::
  ControlHandle::{metadata_cached, metadata_fresh}`; `ClientCtx::
  {effective_metadata, metadata_fresh, table_schema, has_table_schema}`;
  `dynamo.rs::{metadata, metadata_fresh}`.)
- **When a staleness window that was previously "narrow/theoretical" becomes
  "routine" (a new node shape now hits it constantly instead of almost
  never), re-audit every cache-tolerant read that feeds a *non-retried,
  permanent* decision — not just the reads already flagged as commit-wait
  polls.** Building ADR 0035 PR4's data-only node (`ControlHandle::Remote`,
  no local control Raft at all — reads are a genuinely poll-interval-stale
  mirror as a matter of course, not a rare crossover), `provision_tablet`'s
  "no tablet yet" branch picks a table's *initial, permanent* replica set
  from a cache-tolerant `metadata_cached()` scan of `Active` members —
  `CreateTablet` only ever succeeds once per table (idempotent,
  first-committer wins), so a stale read there doesn't cause a transient
  hiccup a later retry heals, it silently and *permanently*
  under-replicates the tablet (nothing re-checks and grows an
  already-recorded RF policy afterward). This was already a latent hazard
  for a normal (`Local`) node — real control-Raft replication lag is
  sub-millisecond — so it had never actually fired; a data-only node's
  *routinely* stale mirror widened the window enough that a fresh
  integration test (two data nodes, one write immediately after both were
  confirmed `Active`) flaked on almost every run (replication factor pinned
  at 1 instead of 2), not once in a blue moon. Fixed by switching that one
  read from `metadata_cached()` to `metadata_fresh().await` — the same
  principle already applied to the schema commit-wait polls (see the entry
  above), just not yet audited onto this call site, because nothing before
  ADR 0035 made its staleness window wide enough to notice. General check
  when a change makes an existing staleness assumption materially truer:
  grep every `metadata_cached()`/`effective_metadata()` call site the new
  node shape can reach, and ask "if this read is one poll interval behind,
  does anything downstream treat the result as permanent?" — not just "is
  this a commit-wait poll." (`animusd::ClientCtx::provision_tablet`;
  `tests/data_only.rs`.)
- **A newly-built bounded primitive over a shared resource doesn't retroactively
  fix every existing unbounded call site that has the same shape — grep for
  siblings, don't assume the one call site you built it for was the only one.**
  ADR 0034 added `StorageScope::physical_bounds()` specifically so the
  byte-estimate auto-split gate wouldn't degrade into a whole-engine scan on an
  unbounded-above tablet (a node's tablets share one `StorageEngine`, ADR
  0028). But `RaftKvNode::local_scan`'s own unbounded-above branch (`end:
  None`) — used by `/admin/raftkv`'s `raft_view` (every hosted tablet, every
  request) and by `erase_scope()` (every `Release`/`Reclaim` teardown) — kept
  falling through to `storage.entries()`, a genuine **whole node engine** scan,
  not just this tablet's own data. Invisible on a lightly-loaded dev cluster;
  live-observed as `/admin/raftkv` hanging indefinitely (20s+, no response) on
  every node of a cluster that had grown 3→5 and auto-split down to a 20KB
  threshold (many tablets sharing each node's engine, actively rebalancing) —
  every request paying O(hosted tablets × whole node engine) instead of
  O(each tablet's own data). Fixed by routing the unbounded branch through the
  same `physical_bounds()` primitive `approx_bytes()` already used, falling
  back to `entries()` only for the one case with no finite bound at all
  (`StorageScope::whole()`, no prefix). This also transparently fixed
  `linearizable_scan`'s unbounded case (the real DynamoDB `Scan`/CQL
  full-table-`SELECT` path), which called `local_scan` and had the identical
  gap — a production-facing improvement the debug-endpoint symptom didn't
  even hint at directly. **When a fix like this lands for one call site, grep
  every other call site with the same shape (any `entries()`/unbounded scan
  over a scoped resource) before considering the class of bug closed.**
  (`animus-cp-data::RaftKvNode::local_scan`.)
- **A client-side view that needs to know "what am I" (this instance's own
  role/identity) must resolve that from a fast, self-only probe, kept
  structurally separate from any slower fan-out the same page also
  performs — never derive it from, or gate its resolution on, the fan-out
  itself.** Gating the AnimusDB Console's tabs on the serving node's own role
  (ADR 0035 PR7) could have derived that role by waiting for the existing
  cluster-wide `/admin/peers`-seeded fan-out (`loadAll()`) to complete and
  then finding "this node" in the results — but that fan-out's per-peer
  fetches have no timeout and can each take as long as the slowest/most
  unreachable OTHER node in the cluster, so gating first paint on it would
  let one dead peer freeze the sidebar (and thus the whole page) on every
  node, not just the one actually talking to that peer. The fix
  (`dashboard_core.js`'s `loadSelf()`) fetches only `SEED`'s own endpoints —
  never a peer — and `loadAll()` calls it first, before the slower fan-out,
  so first paint and tab gating both resolve at local-fetch speed
  regardless of peer health. **The dual lesson, from the same feature**:
  before writing a NEW discovery probe for a client-side page, check
  whether an EXISTING fan-out the page already performs contains the
  answer. The "Open cluster console" link needed to find some other
  reachable control/combined node — which sounds like it needs its own
  peer-probing logic, but `loadAll()`'s existing cluster-wide fan-out
  already fetches every peer's `/admin/config` (and thus its `role`) for
  the other views, so the link just reads that same `STATE.nodes`
  data — zero new requests. General check for either half: does this
  page already have a self-only endpoint to lean on before joining a
  slower cluster-wide operation, and does this page already fetch the
  data a new discovery need is asking for, before writing a second path to
  get it. (`crates/animusd/CLAUDE.md`'s dashboard section, ADR 0035 PR7.)
- **A doc/guide claim that a mechanism is "used by X" must be verified against
  *call sites*, not symbol existence — a superseded mechanism often survives as
  a compilable, even unit-tested, vestige.** During the 2026-07 CLAUDE.md
  restructure audit, `Coresident::sibling` + the listener pool,
  `ProdEnv::shutdown_tasks`, and Accord's `ShardedOwner`/`ShardRouter` were all
  documented (in three different guides) as live, load-bearing plumbing; every
  symbol still existed and compiled, but a `grep` for *callers* found zero —
  each had been superseded (streams, ADR 0026) or trimmed (ADR 0018/0019)
  without its guide entry being updated. This is the dual of the existing
  "before implementing a documented gap, grep the code — the mechanism may
  already exist" rule: before trusting a doc's "X uses this," grep for the
  consumers, not the definition. A doc-staleness audit is cheapest done per
  mechanism ("who calls this?") rather than per claim.
- **CONFIRMED and fixed: a suspected latent cross-group LWW version hazard on
  split/merge (flagged in a PR #90 review comment) was real** — every tablet
  a node hosts shares one physical `StorageEngine` (ADR 0026/0028), and
  `animus-cp-data` stamps each write's MVCC version as its **own** group's
  local Raft log index, which restarts low/independent for a fresh group. A
  split's new sibling could carry a version no higher than what the *source*
  group already stamped for a key now in the sibling's range; a merge
  survivor's group keeps running but starts serving keys the absorbed
  sibling's group versioned under a different, unrelated sequence. Either
  way `StorageEngine::merge`'s per-key LWW silently no-ops the write (loud,
  not silent corruption — the confirm loop's poll-for-exact-value-equality
  times out — but the write never lands). Reproduced directly at the
  `RaftKvNode` level with no control-plane machinery needed: write a key
  through a whole-keyspace group at a high index, narrow it away, start a
  **fresh** sibling group over the *same* shared engine scoped to that key's
  range, write the key again — silently dropped
  (`animus-cp-data/tests/cross_group_lww.rs`).
  **Design space explored, and why the obvious-looking alternatives don't
  work**: (1) seeding a fresh/widened group's floor from a **live,
  per-replica** read (`storage.latest_version()`, or "whichever
  `next_tablet_id` counter value happens to be current when this replica's
  own tick fires") looks tempting since it needs no schema change, but two
  *different replicas of the same group* can observe different values at
  slightly different real-world moments — and since the group's `RaftCore`
  log-index numbering (Host) or an already-running group's live floor
  (merge's widen) must be **byte-identical across every replica** applying
  the same command, a per-replica-timing-dependent floor either breaks Raft
  log-matching outright (Host: divergent `snapshot_index` bases before any
  election) or makes two replicas stamp *different* versions for the
  identical committed write (merge: a bare local read has no cross-replica
  agreement at all). (2) Using the **tablet's own id** as the floor works
  cleanly for split (a fresh sibling's id is always allocated *after*, hence
  numerically greater than, the source's) but not for merge in general: `left`
  and `right` are chosen by **key-range adjacency**, not id order — a tablet
  re-split from the *middle* of an existing chain mints a new id that can be
  *numerically larger* than an unrelated tablet further right in key-range
  order, so a later merge of that pair can have `right.id > left.id`, and
  "bump past `right`'s id" would then either be a no-op or, worse, could
  someday design itself into `left` permanently unable to out-version
  `right`'s history. **The fix that actually holds**: a `version_floor: u64`
  field on `animus_tablet::Tablet` itself (shared by both planes' `Tablet`
  type, so no projection duplication needed) — `0` by default (byte-identical
  to today, `#[serde(default)]` for back-compat), bumped **once, by the
  control plane's own deterministic `apply`** at exactly the two moments a
  cross-group version collision can occur: `SplitTablet` sets the new
  sibling's floor to `source.version_floor + 1` (always exceeds anything the
  source could have stamped, since a group's own local index realistically
  never approaches the scale factor between rescopes — auto-split already
  caps a tablet's key/byte count long before that); `MergeTablets` bumps the
  surviving `left`'s floor to `max(left, right) + 1` (exceeds *both* sides,
  closing the "which id is bigger" trap the id-based scheme fell into). Every
  data replica reads this **already-agreed, replicated** value from
  `Metadata`/`MetadataView` at `Host`/`WidenScope` time — never computes it
  locally — so it is identical across replicas by construction, the same
  discipline as every other epoch-CAS'd placement fact in this codebase.
  `RaftKvNode`'s actual stamped version is `floor * SCALE + local_index`
  (`effective_version`, `SCALE = 2^40`) — a group's own log index is
  completely untouched (no Raft log-matching risk at all; `engine_applied`
  still tracks the raw index), only the *storage-layer version number it
  stamps* changes, and only for a tablet that has actually been through a
  split/merge. **General lesson: when a per-group monotonic counter (a Raft
  log index, a local sequence number) is reused as a version/ordering token
  that must compare correctly *across* groups whose identities can change
  over time (a split/merge/rebalance lineage), the floor that keeps groups
  from colliding must be a value every replica reads identically from
  already-replicated state — never derived from a live per-replica read (even
  a "conservative always-safe upper bound" one), and the exact arithmetic
  direction (which side's id/floor can legitimately end up numerically larger)
  needs checking against the *actual* pairing rule (adjacency, not allocation
  order) before trusting an id-based shortcut.** (`animus_tablet::Tablet::
  version_floor`; `animus-control::meta.rs`'s `SplitTablet`/`MergeTablets`
  apply; `animus-cp-data::RaftKvNode::start_hosted_with_floor`/
  `bump_version_floor`/`effective_version`; regressions in both crates —
  `animus-cp-data/tests/cross_group_lww.rs`,
  `animus-control::meta::tests::{split_tablet_seeds_the_new_siblings_version_
  floor_past_the_sources, merge_tablets_bumps_the_survivors_version_floor_
  past_both_sides}`.)
- **A one-pass ADR-vs-code drift audit needs to check three distinct places for
  each "future work"/"deferred" claim, not just the ADR line the grep hit —
  and a claim can go stale for two structurally different reasons.** Sweeping
  every ADR in `docs/adr/` (2026-08-10), the two reasons split cleanly: (1)
  **shipped-since** — the gap was closed by later work and the ADR was simply
  never revisited (ADR 0006 kept "keyspace metadata replication is future
  work" and "`ALTER TABLE` is a non-atomic drop+recreate" stale for two full
  ADRs' worth of shipped work, even though the *sibling* ADR that closed both
  gaps — 0013 — was itself already correctly updated; same pattern hit two
  crate `CLAUDE.md`s, `animus-cql` and `animusd`, which described the
  pre-ADR-0013 per-process keyspace tracking well after the code moved to a
  replicated `MetaCommand::CreateKeyspace`/`ReplaceTableSchema`). (2)
  **superseded-by-deletion** — the ADR describes a mechanism whose *entire
  subsystem* was later deleted (ADR 0005's residency-enforcement-via-hinted-
  handoff and ADR 0010's read-repair/anti-entropy/`HintStore` both live
  entirely in the leaderless AP data plane, physically removed per ADR 0019;
  their "still deferred" bullets describe gaps in code that no longer exists,
  which is a different fix — a pointer note, not a "shipped" rewrite — from
  class (1)). A **third**, easy-to-miss failure mode is *intra-document*
  staleness: ADR 0017 had a leftover "Implemented now (Stages A+B)... **Not
  yet:** dynamic membership, tablet split" recap sentence that was never
  updated when the *same file*'s later sections marked Stages C and D ✅ Done
  — so an audit must diff every "not yet" claim against the rest of *its own
  document*, not just against other ADRs/CLAUDE.md files. Method that worked:
  grep every ADR for forward-looking language, then for each hit check (a) the
  same file's own later sections/amendment blocks (several — 0016, 0021, 0024,
  0026, 0030, 0035 — already self-correct via inline "✅ Done"/"Amended by ADR
  NNNN" notes, so a naive line-number grep would falsely flag them), (b) the
  actual code via grep for the mechanism's likely name, and (c) whether the
  described subsystem still exists in `crates/` at all. Conservative default:
  when a claim is genuinely nuanced (ADR 0005's "topology labels from config
  are future work" — true for deployment-config labels, but a wire-level
  admin API to set labels on a growth-added node now exists), leave the ADR
  text alone and flag the nuance in the audit's PR body rather than rewriting
  toward a verdict that isn't clean. (PR closing the 2026-08-10 audit; see
  `docs/adr/{0002,0005,0006,0010,0017}` and
  `crates/{animus-cql,animus-env,animusd}/CLAUDE.md`.)
- **A per-node fact that a fan-out consumer needs (role, in this case) is
  cheapest to replicate alongside the node's own self-registered state, not
  to fetch by adding a second round trip per node.** `/admin/peers` used to
  return only addresses; the dashboard already had to fetch every peer's
  `/admin/config` just to learn ITS role, meaning "gate/label by role"
  structurally depended on that peer's own fan-out succeeding first — the
  exact coupling the ADR 0035 PR7 "self-only probe" lesson above warns
  against, just one layer further out (there, gating *this* node's own tabs
  on a peer fan-out; here, labeling *other* nodes' rows on each of *their*
  individual fetches succeeding). Since a node only ever authoritatively
  knows its own role, the fix is not a new endpoint or a bulk-query
  mechanism — it is adding the fact to the exact structure that already
  replicates "this node's own stuff, once, at startup"
  (`animus_control::meta::NodeAddrs`, mutated by `RegisterNodeAddrs`): every
  node stamps its own role into the very `NodeAddrs` it already
  self-registers, and any reader gets every node's role for free from
  `Metadata.node_addrs`, already synced, already fanned-out via the one
  `/admin/peers` call. **General check when a fan-out/dashboard-style
  consumer needs a per-node fact that changes rarely and each node already
  knows about itself: is there an existing "self-registered, replicated,
  read-by-everyone" structure to add the field to, rather than a new query
  the consumer must issue per node (which inherits that node's own
  reachability as a precondition for learning a fact about it)?** Kept
  strictly additive: the pre-existing `admin_addrs` field was untouched, a
  new `peers: [{admin, role}]` field carries the addition, and the dashboard
  treats the peer-sourced role as a *fallback* behind each node's own richer
  `/admin/config` fetch (which still runs, for the other fields it returns)
  rather than replacing that fetch outright — so a node whose own fan-out
  fails now degrades to "shown, correctly labeled, marked unreachable"
  instead of "invisible". (`animus-control` `meta::NodeAddrs::role`;
  `animusd` `admin.rs::peers_view`, `dashboard_core.js::loadAll`; ADR 0035
  residual follow-up.)
- **When a second producer writes bytes some edge will later decode, call the
  edge's own encoder — never re-serialize "the same shape" by hand — and
  regression-test by reading back through the real consumer, not the storage
  view.** Making the admin bulk seeder write DynamoDB-compatible items, the
  key was correctly built via the shared `dynamo::item_key`, but the value was
  hand-rolled as `serde_json::to_vec(&item)` — which *looks* identical to what
  `PutItem` stores, except the edge actually wraps every stored value in a
  one-level envelope (`wire::encode_stored_item`'s `item` vs `tombstone`
  variants, the `DeleteItem` sentinel), so every seeded row decoded as
  "corrupt stored item". Nothing at compile time connects the two sites; the
  storage-scan assertion (bytes landed, keys well-formed) passed fine. It was
  caught only because the same PR added a `GetItem`-through-the-DynamoDB-edge
  readback assertion. The two halves of the lesson reinforce each other: the
  hand-rolled serialization is the *bug class*, and the read-back-through-the-
  consumer test is the *only gate that catches it* — a byte-producer PR
  without one proves layout, not compatibility. (`animusd`
  `admin.rs::action_data_seed`, `dynamo.rs::item_key`,
  `animus-dynamo` `wire::encode_stored_item`;
  `admin_endpoint.rs::admin_seed_writes_synthetic_keys`.)
- **A monotonic allocator with a disjoint base range gives a *hard*
  uniqueness guarantee and needs no pre-check — prefer it over a best-effort
  collision guard whenever the state machine itself can enforce uniqueness.**
  `animusd join --node I` (ADR 0032) protects against two operators picking
  the same index with a pre-bind `Status` read compared for exact
  address-book equality — the ADR's own doc names the *real* backstop as
  `RegisterNodeAddrs`'s idempotent apply, i.e. the pre-check narrows but does
  not close the race. Adding `MetaCommand::AllocateNodeId` (ADR 0036) —
  mirroring the existing tablet-id allocator
  (`Metadata::next_tablet_id`/`next_free_tablet_id`, ADR 0023) instead of
  inventing a new mechanism — makes two racing proposals land on two
  distinct ids *by construction*: the monotonic floor plus an apply-time
  presence check is evaluated identically on every replica, so no epoch-CAS
  and no pre-bind guess is needed at all. General shape to reach for: when a
  cluster hands out an id/slot/index anywhere, ask whether a client-side
  guess-then-verify is standing in for a server-side monotonic allocator that
  could instead make the race structurally impossible.
- **Adding a variant to *any* wire enum that flows through an exhaustive
  `match` needs a grep of every match site for *that enum*, not just the
  usual `is_relayable_command`/`ClientRequest` dispatch allowlist.** Adding
  `ClientResponse::NodeIdAllocated` broke `animus-cli`'s
  `print_response(&ClientResponse)` — a plain, exhaustive `match` with no
  wildcard arm, in a crate `is_relayable_command`'s doc comment never
  mentions because it isn't a *command*-gating site at all; it's a
  *response*-rendering one. The compiler caught this one (non-exhaustive
  match is a hard error), but only because the match had no `_ =>` catch-all
  — a wildcard arm would have silently swallowed the new variant with no
  error and no runtime symptom until someone noticed the CLI printed
  nothing useful for it. Lesson generalizes past this one enum: before
  calling a "new variant" change done, `grep` for every `match` (and
  `matches!`) over the enum's type name across the whole workspace, not just
  the crate where the variant was added. (`animus-cli/src/main.rs::
  print_response`; ADR 0036.)
- **A "handle has no local authority for this" fallback should return
  `Option`/`Result`, not a silently-empty collection — even when the empty
  collection is currently harmless.** `ControlHandle::Remote::config()`
  (no local `RaftCore`, ADR 0035) answered a bare `BTreeSet::new()` — correct
  in isolation (a data-only node genuinely knows nothing at start-up), but it
  made "I have never learned anything yet" and "I have learned the group has
  zero voters" the same bit pattern. That distinction stayed free as long as
  nothing downstream branched on it — the moment ADR 0037 PR2 needed `Remote`
  to echo a *discovered* live voter set learned over the wire, the existing
  return type had no slot for "unknown", forcing a signature change anyway
  (to `Option<BTreeSet<NodeId>>`) that a wider `-D warnings` blast radius
  would have caught immediately if the type had been honest from the start.
  General check before shipping a `Remote`/mirror/no-local-authority arm of
  any handle: if this value could later be *learned* rather than *known
  outright*, return `Option` (or a dedicated "unknown" variant) now, even if
  every current caller would `.unwrap_or_default()` it anyway — the cost of
  being wrong is a signature-widening PR later that touches every call site,
  not a graceful extension. (`animusd` `control_handle.rs::ControlHandle::
  config`, `RemoteControlClient::control_voters`.)

- **A "which ids are the control plane" read has (at least) two structurally
  different purposes — a *seed* for a node's own bring-up vs. a *live
  authority* for a running correctness decision — and only an explicit,
  named audit catches every instance of the second kind hiding behind the
  first kind's static source.** Auditing every `control_ids`/`admin.
  control_ids`/`ClusterConfig::control_ids()` read in `animusd` for ADR 0037
  PR4 (the plan's own named deliverable, mirroring the ADR 0029 ReadIndex-
  quorum lesson's warning that this exact class of bug is invisible until a
  *real* membership change exercises the divergence): the overwhelming
  majority are legitimately static — `RaftNode::start`'s initial `all_nodes`
  at process bring-up, `ClusterConfig::control_ids()`'s config-file-derived
  helper (there is no "live" analogue for a plain config accessor), and
  `ClientRequest::JoinInfo`'s reply (a joining node's *seed*, which the
  replicated `node_addrs` overlay + this same PR's `control_peer_sync_loop`
  already keep current after that point, same as every other ADR 0032 PR1
  seed-then-overlay axis). Exactly **one** site was a live-authority
  decision wearing a static-seed's clothes: `admin_remove_member`'s
  control-voter refusal, fixed to read `self.control.config()` (see the
  `docs/engineering-lessons.md` entry on `ControlHandle::config()`'s
  `Option`, and `animusd/CLAUDE.md`'s decommission gotcha). **One further
  site was flagged, not fixed, as an accepted, narrower, deliberately
  out-of-scope gap**: `heartbeat_loop`'s `control_ids` parameter (a raftkv-
  role node's heartbeat *destination* list, captured once at that node's own
  process start) never gets a live-overlay refresh the way `peer_sync_loop`/
  `route_sync_loop`/this PR's own `control_peer_sync_loop` do for their
  respective axes — so a raftkv node started before a control voter was
  added at runtime never heartbeats that voter directly, and if it later
  becomes leader, this specific already-running raftkv node's heartbeats
  keep missing it (a *bounded*, self-healing gap in practice: the *other*
  raftkv nodes docker/replicas/heartbeats still reach it, and a restart of
  the affected node picks up the current `control_ids` again) rather than a
  silent total loss of failure detection. Fixing it properly is the same
  "port `peer_sync_loop`'s pattern to a new axis" shape this PR already did
  twice (`control_peer_sync_loop` for the control role's own peer book,
  PR2's `node_addrs`/`Status.control_voters` wiring for discovery) — sizing
  it as its own follow-up rather than a third instance crammed into this PR
  keeps the diff reviewable, per this file's own standing don't-conflate-
  unrelated-fixes discipline. **General rule for any future "is this id
  part of X" read**: ask whether getting it wrong for one tick is (a)
  "a joining node briefly doesn't know about a very recent peer, self-heals
  next sync tick" (fine, static) or (b) "a decision that, once made, is hard
  or impossible to undo, or degrades a safety/liveness property with no
  self-healing path" (must read the live authority) — and write the answer
  down at the call site, not just in an audit PR's description, so the next
  reader doesn't have to re-derive it.

  **Update: the flagged `heartbeat_loop` gap above is now closed (PR #134,
  the ADR 0037 hardening trio's PR 1)** — and closing it surfaced a second,
  previously-undocumented gap the original text above never named at all,
  which is itself the generalizable lesson: **a "this destination list is
  static" finding must also check the transport *address book* — the two
  are separate staleness axes that fail together, and fixing only the list
  leaves the send silently dropped anyway.** `heartbeat_loop`'s
  `control_ids` argument was one axis (*which ids* to heartbeat); the
  raftkv env's own peer book was the other (*where* to actually reach each
  of those ids) — `peer_sync_loop` already refreshed the book from
  `Metadata.cp_member_addrs`/`node_addrs[*].raftkv` on a timer, but never
  from `node_addrs[*].control`, so a runtime-added control voter's address
  never landed there. A destination list naming a live id with no matching
  address book entry is not a partial fix — `ProdEnv::send`'s fire-and-forget
  contract means the gap stays *exactly* as silent as the one being fixed
  (no error, no log at above debug, just a heartbeat that never arrives).
  The fix, once both axes were identified, was mechanically the same shape
  each time this class of bug has appeared in this ADR's own stack
  (`control_peer_sync_loop`'s `.control` merge for the control role's own
  peer book, PR2's `node_addrs`/`Status.control_voters` wiring for
  discovery): (a) a new animusd-local `heartbeat_loop_live` re-derives the
  destination list every tick from `ctx.control.config()` instead of the
  bring-up-time snapshot `animus_control::node::heartbeat_loop` was pinned
  to (that function itself, and its `SimEnv` call sites, are deliberately
  untouched — the static-list contract is still correct there); (b)
  `peer_sync_loop` gained the missing `node_addrs[*].control` merge,
  alongside its existing `.raftkv`/`cp_member_addrs` ones. **General rule
  for any future "is this destination list live" audit**: grep the sending
  env's own peer-book refresh loop in the same pass — a live list and a
  stale book produce the identical externally-visible symptom (silence),
  so a test that only asserts on the destination-list computation, without
  driving a real send through a real socket, will not catch the second
  axis. `tests/heartbeat_live_destinations.rs::
  heartbeat_reaches_a_runtime_added_voter_after_it_becomes_leader` catches
  both together: it grows a control voter at runtime, forces a
  deterministic 2-voter leadership transfer onto it, and polls the new
  leader's own `/admin/raft` view for a *sustained* `believes_alive: true`
  across several `DETECT_TIMEOUT` windows — a test that fixed only the
  list (or only the book) would still fail this, since the other half's
  silent drop is indistinguishable from "no fix at all" to anything short
  of a real end-to-end delivery check.

- **A one-time "enable this feature" flag set on a driver task's first async
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

- **Porting a `DRIVER_APPLIED` cutover onto a state machine that ISN'T a
  unit placeholder is a smaller change than it looks — the generic core
  needs zero edits.** ADR 0038 PR3 flipped `Metadata` (a large, real struct
  with actual business logic — the control plane's whole `Metadata::apply`)
  to `StateMachine::DRIVER_APPLIED = true`, expecting to need a new unit
  placeholder type the way `animus-cp-data`'s `KvState` is one. It didn't:
  `RaftCore<C, S>`'s `metadata: S` field is only ever touched by the
  trait-impl's `apply()` (never called once `DRIVER_APPLIED`) and the
  now-dead `WalRecord::Snapshot { metadata: S, .. }` embedding (a harmless,
  never-populated `Metadata::default()` for a `DRIVER_APPLIED` plane, exactly
  as trivial as a real unit type) — so `Metadata` doubles as both "the real
  struct an external apply task privately owns" and "the harmless generic
  `S` parameter satisfying `RaftCore`'s trait bounds," with no new type and
  no `raft.rs` edits beyond deleting the now-meaningless
  `RaftCore<MetaCommand, Metadata>::{metadata, members, placement_view}`
  inherent methods. **When porting a `DRIVER_APPLIED` cutover, check whether
  the "real" state machine can just BE the generic `S` before reaching for a
  placeholder type — the core's own genericity (ADR 0016) was already built
  to make this a no-op.**
- **Seed a `DRIVER_APPLIED` apply task's durability watermark from the
  *engine's own* persisted marker, never from the recovered core's
  `last_applied()` — they can legitimately disagree, and only one of them is
  actually correct after a real crash.** After `RaftCore::recovered()`, a
  core's `last_applied()` reflects only the last **compacted** snapshot
  base; the engine's own watermark (written every apply pass, far more
  often than compaction runs) can already be well *ahead* of it. Seeding
  from the core's `last_applied()` (mirroring `animus-cp-data`'s own
  `engine_applied.store(core.last_applied())`, which is *correct* there only
  because the data plane's per-key merges are independently idempotent under
  replay) would, for a state machine like `Metadata` whose commands aren't
  all trivially safe to reapply twice (counters, nonce ledgers, epoch-CAS —
  each *happens* to be idempotent by its own construction, but relying on
  every future command variant continuing to be is fragile), silently
  redeliver an already-engine-durable prefix on top of a freshly
  engine-rebuilt cache. The robust fix: read the engine's own watermark key
  at the apply task's startup, rebuild the cache from *that* index, and
  **filter drained effects by `index > watermark`** rather than trusting
  that redelivering the whole tail is harmless. Regression:
  `animus-control::node.rs`'s own `#[cfg(test)]`
  `apply_and_compact_replays_only_the_tail_beyond_the_watermark` /
  `..._is_a_no_op_when_the_watermark_already_covers_everything` — white-box
  tests that drive the private apply function directly with a hand-seeded
  watermark, precisely and deterministically, rather than trying to time a
  real crash to land at an exact index.
- **A `SimEnv` test that restarts a node (`Simulator::stop` + a fresh
  `RaftNode::start` on the same id) must reuse the *same* `StorageEngine`
  handle across the restart, not construct a fresh one — `MemoryEngine::new()`
  at the restart call site silently and completely discards everything a
  real (disk-backed) engine would have kept, and the bug can hide for a
  long time under small test workloads.** Ported en masse while mechanically
  fixing every `RaftNode::start(..)` call site across
  `animus-control`/`animus-cp-data`'s test suites for ADR 0038 PR3 (adding
  the now-mandatory engine argument), several restart-style tests
  (`restart.rs`, `schema_indexes.rs`, `control_membership.rs`) got a
  throwaway `MemoryEngine::new()` at their *second* `RaftNode::start` call —
  which happened to still pass, because none of those scenarios crossed the
  Raft log's own compaction threshold, so the full uncompacted WAL alone was
  enough to replay the whole state from scratch regardless of what the
  "restarted" engine held. That's a coincidence of test scale, not a proven
  property — the instant a scenario compacts before the simulated crash, a
  fresh engine would silently lose the compacted prefix with no error.
  **Fix pattern**: create one `MemoryEngine` per node up front (`MemoryEngine`
  clones share state, exactly like a real engine reopened from the same
  directory), and re-clone that *same* handle into every `RaftNode::start`
  call for that node id, including at restart — never call `::new()` a
  second time for an id that already existed. **General rule when adding a
  mandatory storage-engine parameter to a `start`-style constructor across a
  large test suite**: grep for every call site that *replaces* an existing
  instance (`nodes[i] = Type::start(..)`, a second `start` for the same id)
  separately from fresh first-time starts, and audit each one for whether
  the "durable" resource being threaded through needs to survive that
  specific call — a blind find-and-replace macro fix is exactly how this
  class of bug hides.

### Parallel-agent orchestration
- **Partition work by disjoint crate ownership — exactly one owner per shared
  crate/file.** The assembly points (`animusd`, `animus-control`) are
  chokepoints; if several agents must touch `animusd`, split by *file*
  (`dynamo.rs` / `cql.rs` / `lib.rs`) and expect a small `lib.rs` merge.
- **Verify agent output yourself** (build + gates), don't trust the report —
  especially for safety-critical changes (a `SimEnv`/determinism edit) and after
  any agent died mid-run.
- **When an agent dies (API overload/stall/error), inspect its worktree before
  re-launching** — its partial work is often intact and finishable (or resumable
  via `SendMessage`); a lost worktree means redo. **Don't thrash re-launches
  during an API overload** — wait for it to ease.
- **Tell agents to keep public signatures stable** when a sibling depends on them
  (additive changes only), and to **stop-and-report rather than loop** on a
  transient API error.
- **A worktree-isolated agent must never `cd` into the main checkout — even
  once, even to "just look."** The harness already starts the agent's Bash
  tool in its own worktree's directory; a `cd /path/to/main/checkout && ...`
  prefix is pure reflex (muscle memory from non-worktree sessions), not
  something the task ever required, and every subsequent command in that
  shell then runs — and, if the agent commits, *commits* — against the
  user's real local `main`, not the isolated branch it was supposed to be
  building. This happened once in the ADR 0037 stack (a prior agent's stray
  commit landed on the user's main and had to be found and dealt with
  separately from the actual PR work). The correct discipline is simpler
  than remembering not to `cd`: never construct a command with a `cd`
  prefix pointing outside the current working directory at all — if a path
  needs to be absolute, write the absolute path directly into the command
  (`git -C "$(pwd)" ...` or, more simply, no `-C`/`cd` at all, since the
  tool's cwd is already correct) rather than reaching for `cd first-dir &&`.
  Separately: a tool that reads files by absolute path (e.g. `Read`) is
  **not** guaranteed to be worktree-scoped the way the Bash tool's cwd is —
  hardcoding the *main checkout's* path (as opposed to the assigned
  worktree's path) in a `Read`/`Edit` call silently reads/writes the wrong
  copy with no error (for `Read`) or a clear refusal (for `Edit`/`Write`,
  which do check). Confirm `git rev-parse --show-toplevel` once at the start
  of a session and reuse *that* prefix for every absolute path, rather than
  assuming the repo's conventional root path is the same as the assigned
  worktree.
- **After editing a file via the Write/Edit tools, verify the change actually
  reached the filesystem the Bash tool (and thus `cargo`) sees — don't trust
  a "success" result alone.** Debugging this PR's own heartbeat-liveness
  regression test (ADR 0037 hardening PR1, PR #134) burned well over an
  hour chasing a phantom distributed-systems bug — a control-only leader's
  `FailureDetector` losing `believes_alive` ~500ms after a runtime-added
  voter took over leadership, permanently, never self-healing — that was
  entirely explained once `git diff --stat`/`grep` **run through Bash**
  showed the fix (`heartbeat_loop_live`, the `peer_sync_loop` address-book
  merge) had *never actually landed* in `crates/animusd/src/lib.rs`: the
  Write/Edit tool session had a stale/cached view of that file, diverged
  from what a fresh `cat`/`Write` heredoc through Bash produced for a
  *different* file in the same debugging session (a test file created via
  `Write`, invisible to `ls`/`grep` through Bash until the same content was
  independently written through Bash itself). Every `cargo build` in
  between kept succeeding — which felt like confirmation the edits had
  landed, but a clean build of *unchanged* code is indistinguishable from a
  clean build of the intended change; a passing build proves nothing about
  which source it built. **Concrete mitigation**: immediately after any
  Write/Edit-tool change to a file a build depends on, run `grep`/`git diff
  --stat` on that exact path **through the Bash tool** for a string unique
  to the new content, before writing a single line of test/debug code
  against the assumption it landed — this costs one command and would have
  caught the divergence at the first edit instead of an hour into a wild
  goose chase. If Bash's view and the Edit tool's view ever disagree on a
  file's content (one tool sees a change the other doesn't, or a
  freshly-`Write`-created file is invisible to `ls` through Bash), treat it
  as a real tooling desync, not a typo — stop trusting that tool's Read
  cache for the affected file and do all further edits to it through Bash
  (`cat > file <<'EOF' ... EOF` / `python3` in-place patch) until a fresh
  `Read` of the file (forced by an external touch, which this session's
  harness surfaces as a "file was modified externally" notice) demonstrably
  matches Bash's own view again.
