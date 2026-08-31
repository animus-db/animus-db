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

> **Note on deleted subsystems (2026-08-23).** Entries throughout this log cite
> **Accord** (`animus-consensus`, `AccordNode`/`AccordCore`, the
> `animus-test` Accord corpus in `tests/support/` + `corpus.rs` +
> `elle_accord.rs`) and the per-table **`ReplicationMode`** seam. All of that
> was **deleted** by [ADR 0019](adr/0019-cp-only-v1-defer-ap.md)'s 2026-08-23
> amendment — with CQL dropped (ADR 0053), DynamoDB's wire cannot express a
> replication mode, so AP became unselectable and Accord's remaining role
> vacuous. Those citations are kept deliberately and read as **historical**:
> the lessons they carry are general (checker teeth and workload design,
> collapsing a total order into one `u64`, composing rather than reshaping a
> proven core, one `Env`/inbox/WAL per hosted protocol instance) and apply
> directly to the surviving corpora — the CP raftkv corpus (ADR 0017) and the
> multi-tablet transaction corpus (ADR 0018). They are **not** moved to the
> archive, because unlike a superseded *lesson* the lesson here still stands;
> only its illustration is gone. The code is retrievable from git history if a
> citation needs chasing.

### Testing
- **A `dynamo_retry`+`CreateTable` fixture helper must tolerate
  `ResourceInUseException` on the retry, not just retry 500s (2026-08-31,
  issue #461).** `create_table` (`crates/animusd/src/schema.rs`) calls the
  blocking `await_table_serveable` before it acks 200 — a real wait that can
  itself time out server-side and surface as a retryable 500, *after* the
  schema commit it was waiting on already landed durably. A test's
  `dynamo_retry` helper that retries only on `status == 500` then replays
  the identical `CreateTable` into the duplicate-name check ahead of that
  wait, which correctly answers `400 ResourceInUseException` — a legitimate
  outcome of retrying a non-idempotent-from-the-client's-view operation, not
  a test failure. `dynamo_expression_surface.rs`'s `setup()` hard-asserted
  200 and had no such tolerance, so it flaked under exactly this timing.
  Fixed by treating a `ResourceInUseException` `CreateTable` reply as
  success **and** re-probing serveability explicitly in that branch (a
  `ConsistentRead` `GetItem` run through the same `dynamo_retry` helper) —
  the 400 path skips the server's own `await_table_serveable` wait, so
  "the table exists" and "the table is serveable" are not the same fact
  once you take that branch. A repo-wide grep at fix time
  (`ResourceInUseException` in `crates/animusd/tests/`) found no existing
  shared helper doing this — every other hit was a test deliberately
  asserting the *rejection* case — so there was nothing to reuse; if a
  second `setup()` trips the same race, factor the tolerant-CreateTable +
  readiness-reprobe pair into `tests/support/mod.rs` rather than hand-rolling
  a third copy.
- **Adding a new AWS-documented request-shape limit needs a workspace-wide
  sweep for pre-existing test helpers that already build a request past it,
  not just a decode-level unit test (2026-08-23, DynamoDB batch/transaction
  caps).** Enforcing `BatchWriteItem`'s real 25-item-per-call cap
  immediately broke three already-green `animusd` integration tests that
  predate the cap and built genuinely oversized single-call batches to
  drive load gently: `batch_write.rs`'s throughput comparison (200 items in
  one call), `backfill_seeder.rs`'s 300-row populate (chunked at 100), and
  `update_table_drop_index.rs`'s `batch_put_items` helper (also chunked at
  100). None of them were wrong when written — nothing enforced the cap
  yet, so "one big batch" was a legitimate way to minimize consensus
  rounds. `grep -rn 'chunks(100)\|chunks(200)\|...'`-style searches across
  every crate's `tests/` tree found and fixed those three — **but missed a
  fourth, `animusd::dynamo::stream_write_path_tests::
  batch_write_on_a_marker_table_commits_one_entry_per_tablet`, an in-crate
  `#[cfg(test)] mod` living inside `src/dynamo.rs` itself (40 items in one
  call), caught only by CI actually running `cargo test -p animusd --lib`**
  (see this crate's own `CLAUDE.md` "Tests" section for the list of
  in-crate test modules — `lib.rs`, `index_drain.rs`, and `dynamo.rs` each
  hide one or more, for the same reason: they need a private handle no
  external `tests/` file can reach). The lesson from that miss: a sweep for
  this class of regression must cover `src/**` `#[cfg(test)]` modules
  **as well as** the `tests/` integration-test tree in every crate the
  change touches — grepping only `tests/*.rs` silently skips exactly the
  in-crate tests a crate's own `CLAUDE.md` flags as needing special
  handling in the first place. The crate's own decode-level unit tests,
  however thorough (at-cap accepted, one-over rejected), only prove the new
  check rejects a synthetic oversized request — they can't prove the rest
  of the workspace (either tree) doesn't already build one for an unrelated
  reason. **General form**: any new hard cap on a previously-uncapped wire
  shape (item size, batch/transaction item count, index count, key count,
  …) needs a workspace-wide sweep of BOTH `tests/*.rs` and every `src/**
  #[cfg(test)]` module before the change is considered validated, since a
  test helper written before the cap existed has no reason to already
  respect it — and running the actual `cargo test -p <crate> --lib`/
  `cargo test -p <crate>` targets, not just a source grep, is what catches
  what the grep pattern didn't anticipate. Fix the test helpers to chunk
  (the same technique a real client SDK uses), not the new cap.
  (`crates/animusd/tests/batch_write.rs`, `backfill_seeder.rs`,
  `update_table_drop_index.rs`, `crates/animusd/src/dynamo.rs`.)
- **A regression's own split key can quietly exempt it from the bug it
  claims to cover — check whether the fixture's boundary is realistic, not
  just whether the test passes.** Issue #355 suspected (from code reading)
  that a split's right child's `"gsi"` cursor key (`animusd::index_drain`'s
  `drain_tablet`, `cursor::cursor_key(&group.scope_range().start,
  GSI_TAG)`) is token-truncated below the child's own `range.start`,
  routing the watermark write to the LEFT sibling instead — confirmed true
  by direct reproduction (`index_drain.rs::gsi_drain_cursor_tests::
  split_right_childs_gsi_cursor_after_a_non_token_aligned_split_issue_355`,
  `#[ignore]`d, expected to fail until fixed): the watermark stayed `None`
  and the change log stayed pinned at 8 records across 20 drain ticks
  (~8s), and the cursor row for the right child's own token was found
  physically present on the LEFT sibling's engine, absent on the right's
  own. The existing sibling regression
  (`split_right_childs_cold_start_re_reconciles_from_zero_without_
  corrupting_the_gsi`) asserts the *opposite* — the cursor genuinely
  advances — and is not wrong; it splits at `BOUNDARY`, a bare
  `TOKEN_BYTES`-long numeric constant, which is *itself* already
  token-aligned by construction. A real split's key
  (`byte_weighted_median`, chosen from actual row content) is essentially
  never that short, so the existing test's green result says nothing about
  the shape production splits actually produce — it happens to sidestep
  the exact precondition (`range.start` longer than `TOKEN_BYTES`) the bug
  needs. The general rule: when a regression's fixture picks a boundary/key/
  input by a rule visibly simpler than what production uses to pick the
  same thing (a round numeric constant standing in for a real row's byte
  string, a fixed-length id standing in for a variable-length one), treat
  its passing as evidence about *that specific shape* only — check whether
  the simplification also strips out the precondition the suspected bug
  needs before trusting the green run as a disproof. GSI *correctness*
  (every item still queryable) was unaffected either way — only the
  cursor/trim bookkeeping breaks, matching `drain_tablet`'s own comment
  that a watermark stuck at `None` means "reconcile everything, always
  correct, just not incremental" (a liveness/efficiency defect, not a data
  correctness one) — the same shape `advance_backfill_cursor`'s own doc
  documents for the analogous, already-fixed backfill-cursor bug, though
  the mechanism here is different: the backfill cursor used to be *locally
  rejected* by a range fence (fixed by writing unfenced, directly on the
  known-leader `group` handle); the `"gsi"` cursor write goes through
  `ClientCtx::cp_kind_write_raw`, which **routes by the write's own key**
  (`cp_route(table, &first_write_key)`) — so a token-truncated cursor key
  doesn't get rejected at all, it gets *misrouted and successfully applied
  on whichever tablet's declared range the truncated key actually falls
  in*, which is silently indistinguishable from success unless you read
  the raw physical key back off the sibling's own engine
  (`CpGroup::local_get_kind` — bound-free, reads any key physically present
  regardless of the tablet's declared range) rather than inferring the
  landing site from the key-construction code alone.

  **Fixed (2026-08-23).** `animus_cp_data::cursor::cursor_key` now embeds
  the tablet's own `range.start` **verbatim** (never truncated), with a
  trailing 2-byte big-endian length so `parse_cursor_key` can still recover
  the tag unambiguously without relying on a fixed byte offset (the old
  scheme's whole reason to truncate in the first place). The key is now
  `range_start` extended with more bytes, so it always compares
  lexicographically `>=` its own tablet's `range.start` by construction —
  the routing half of `KeyRange::contains` is satisfied unconditionally,
  not by luck of the split boundary's own byte content. The repro test
  (`gsi_drain_cursor_tests::split_right_childs_gsi_cursor_after_a_non_
  token_aligned_split_issue_355`) now asserts the fixed end-state
  precisely — watermark advances, change log trims to zero, and the
  physical cursor row lives on the right child's own engine and is absent
  from the left sibling's — rather than only "eventually not `None`."
  **Pre-fix stray rows**: a cluster that hit this bug before the fix could
  have a truncated cursor row physically sitting on a sibling tablet's
  engine forever, silently depressing that sibling's own `"gsi"`
  min-over-rows watermark (redundant re-reconciliation there, never a
  correctness or non-convergence issue — the same "reconcile everything,
  always correct, just not incremental" property the bug's `None` case
  already had). No migration/cleanup was written for it — the root
  `CLAUDE.md`'s no-back-compat policy means clusters are recreated from
  scratch, and a convergent sweep to find and delete such rows would need
  to distinguish a truncated stray from a genuine (if unlikely) same-prefix
  collision, which isn't a cheap, obviously-safe addition, so it was left
  alone per the same policy rather than over-engineered.
- **A hand-rolled HTTP test helper that reads the response with
  `read_to_end` MUST send `Connection: close` — an HTTP/1.1 request without
  it deadlocks against a keep-alive server, and the hang lands on the
  *first* request, before any of the test's own bounded assertions can
  fire.** The server (correctly, per HTTP/1.1 defaults) keeps the connection
  open and parks waiting for a next request; the helper waits for EOF that
  never comes. The ADR 0041 GSI-drain e2e test shipped with exactly this
  bug in its `dynamo()` helper — every *other* dynamo test's helper sends
  `Connection: close`, this one was written fresh and dropped it — and the
  result was a test that hung ~47 minutes (until externally killed) at
  `CreateTable`, while masquerading as the drain bug it existed to expose.
  Corollary, the meta-lesson that cost the real time: **a WIP handoff's
  "known broken" note describes the last run its author observed, not
  necessarily the committed code — re-verify the recorded failure signature
  (run the test, watch *where* it stops) before debugging from the note.**
  The note said "times out waiting for the first index rows" (a clean 30s
  bounded panic); the committed helper couldn't even reach that assertion.
  The two bugs were independent, and fixing the noted one first while the
  unnoted one hid behind it turned a 30-second failure into an apparent
  hang. (`animusd` `tests/dynamo_gsi_drain.rs`, 2026-08-13.)
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
- **A real-thread `ProdEnv` load-generation harness that resolves "the
  leader" once and hammers that one handle for the whole run is itself a
  latent bug, independent of whatever property the test exists to check** —
  and a heavy multi-task `ProdEnv` load is exactly the kind of CPU
  contention that triggers the ADR 0017 starved-consensus-loop election
  shape it's supposed to be immune to. `crates/animus-cp-data/tests/
  prod_concurrent_ts_monotonic.rs` hammered a real 3-node group with 24
  concurrent client tasks against one `leader` handle captured at test
  start; on a contended CI runner an election deposed it mid-run and every
  task kept proposing against the now-stale handle, surfacing as
  `linearizable_get ... did not return what was just put (left: None)` or
  `txn_write did not complete (leader stepped down?)`. The second, subtler
  bug compounded it: the put-side helper treated `put() -> Accepted{index}`
  plus `engine_applied_index() >= index` as proof the write **committed** —
  but `Accepted` only ever means "appended to the leader's own log," never
  "committed" (this file's durable-before-visible entry, and the root
  `CLAUDE.md`'s standing lesson, already say this — the harness itself
  hadn't internalized its own documented rule). After an election, the
  deposed leader's uncommitted entry gets truncated and the new leader's
  own entries re-occupy that same index, so the index-advance wait passed
  while the write never actually landed. **Fix, generalizable to any
  multi-task `ProdEnv` harness driving a leaderful group under load:**
  thread the whole node slice (not one handle) into every load-generating
  helper; re-resolve "whoever is leader now" on *every* propose attempt and
  every confirm read, never cache it across an `.await`; and confirm a
  write only by reading the expected value back (never by index-advance
  alone), retrying the *whole* propose on a stale/absent confirmation —
  sound because retrying an already-landed write at the same key/value is
  idempotent. All of it bounded by a converged-or-timeout deadline, per the
  general rule above, never a fixed one-shot wait. (Issue #278 item 6.)
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
  **Coda — the candidate follow-up above was taken (2026-08-10):** swept the
  same bare-`shutdown()`-then-`remove_dir_all` idiom at the 5 remaining racy
  teardown sites — `animus-control/tests/prod_liveness.rs` (2),
  `animus-control/tests/control_membership_prod.rs` (1),
  `animus-consensus/tests/accord_concurrent.rs` (2) — to
  `shutdown_and_wait().await`; `animus-storage/tests/lsm_concurrent.rs` and
  the `animusd` integration tests already used the waiting idiom, and
  `animus-cp-data`'s own compaction path already checks a `halted` flag
  (ADR 0033), so both were left as models rather than swept. **General rule
  to take away: any test teardown that follows a `shutdown()` with removing
  the directory/files that shutdown's background tasks were still writing to
  must use `shutdown_and_wait()`, not bare `shutdown()`** — bare `shutdown()`
  remains the *correct* choice for a test that is deliberately simulating a
  crash (no orderly teardown to race) rather than tearing down a clean
  liveness harness.
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
- **A liveness test's *background workload* must be a well-behaved Raft
  client too, whenever its convergence assert demands every command commit —
  fire-and-forget proposing is only acceptable when the assert doesn't count
  the commands.** The sustained-churn `ProdEnv` liveness test dripped 300
  `propose(..)` calls with the result discarded, then asserted all 300
  members converged; one mid-churn leadership transition (explicitly within
  the test's own `MAX_TRANSITIONS` tolerance!) silently dropped the ~34
  commands proposed across it — `NotLeader` returns were never retried, and
  appended-but-superseded entries have no propose-time signal at all — so
  all three nodes sat flat at 266/300 for the entire 20s budget (issue
  #269). The flat-count shape is the diagnostic: slow I/O shows *progress*
  across the poll; identical, motionless counts mean the commands were never
  committed, and no budget bump can fix that. The fix is the two-sided
  client discipline the production entries above already prescribe, applied
  to the workload loop: retry `NotLeader` against the re-resolved leader
  (never-accepted, retry is free), and confirm commits against the acting
  leader's *applied* state, re-proposing what it lacks (accepted-unconfirmed
  — safe here because the command is idempotent). Note the tension this
  resolved: a test that *tolerates* N leadership transitions while asserting
  exact convergence is inconsistent unless its proposer survives those
  transitions. (`animus-control/tests/prod_liveness.rs::
  sustained_metadata_churn_over_a_real_engine_stays_live`.)
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
- **A whole-tree sweep for "bare one-shot write assert" flakes (issue #278
  item 7) should grep for an existing local retry helper (`async fn put`/
  `put_until_ok`/`put_retry`-shaped) *before* writing new retry logic at each
  site** — the same file class (a real-socket `ProdEnv` integration test)
  very often already has a hand-rolled bounded-retry helper sitting right
  next to one or more still-unprotected raw `ClientRequest::Put`/`PutBatch`/
  `Delete` call sites that simply never got routed through it (an omission
  from whoever added the later call site, not a missing mechanism) — the fix
  is usually "call the existing helper" or "extract the existing inline
  retry loop into one," never a parallel reimplementation. The specific
  class this sweep targeted: issue #268's fix made the CP data plane's
  confirm-loop futility case fail FAST instead of burning a slow timeout,
  which turned every bare one-shot write assert across the test tree into a
  latent flake under runner contention (a transient `"; retry"`-class error
  that used to be masked by a 10s stall now surfaces in milliseconds). The
  canonical retry shape (`crates/animusd/tests/split_build.rs::put`): a
  bounded-deadline loop retrying on ANY `ClientResponse::Error`, panicking on
  a non-`Error` unexpected reply or deadline expiry — puts are idempotent, so
  a later read-back is still the real assertion. Two judgment calls that
  matter when sweeping: a deliberate negative test (asserting a write IS
  refused, e.g. delete-against-a-never-created-table) must stay one-shot; a
  test whose whole point is single-shot latency/behavior (e.g.
  `cluster_split.rs::single_shot_first_write_through_control_node_succeeds`,
  which exists specifically to prove the *forwarder's own* internal
  election-wait backoff makes ONE client call succeed with no client-side
  retry) must not be loosened into a retry loop, or it stops testing what it
  says it tests.
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
  **This exact anti-pattern reappeared** in a newer sibling in the same file
  (`runtime_added_voter_survives_leadership_change_to_a_different_original_voter`,
  ADR 0037 PR4), and flaked CI on `main` — failing *both* attempts of the
  retried `prod-liveness` tier. It regressed to the one-shot shape because the
  transfer was only *scaffolding* for that test's real subject (address
  propagation after a runtime-added voter), not the property under test: when a
  test forces a leadership transfer merely as setup, it still needs the
  poll-and-retry-the-mutating-call shape above, or the scaffolding becomes the
  flake. What makes the one-shot call unsafe is invisible from the call site —
  `RaftCore::transfer_leadership` arms a deadline of one *raw, un-randomized*
  `election_base` (150ms, not the `[base, 2*base)` range followers draw from),
  `tick()` clears it silently with no log or metric, and the admin action's own
  5s poll never re-arms, so a dropped transfer and an in-flight one are
  indistinguishable to the caller (both surface as HTTP 409). Those two
  product-side gaps are tracked in #313.
  (`crates/animusd/tests/control_membership_admin.rs`, 2026-08-20.)
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

  **Update: closed by the ADR 0037 hardening trio's quorum-guard liveness
  fix (PR 2, PR #136)** — not by bridging `believes_alive` after all, but by growing a
  genuinely **control**-id-native signal instead: `RaftCore` already knows
  exactly who has recently acked an `AppendEntriesResp` (success or reject),
  since that's the leader's own control-Raft traffic — no id-space crossing
  needed at all. A volatile `last_contact: BTreeMap<NodeId, Nanos>`
  (`animus-control/src/raft.rs`, seeded at `become_leader`, stamped in
  `handle_append_resp`, deliberately never persisted — same lifetime as
  `next_index`/`match_index`) backs a new `RaftNode::
  control_peer_believed_alive` (its own `CONTROL_PEER_LIVENESS_TIMEOUT`,
  not a reuse of `DETECT_TIMEOUT`). The general lesson still holds — it's
  *why* the fix had to grow a new signal in the id space that actually has
  one, rather than solving it by finally writing the id-bridging code this
  entry warned against.
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

  **Update: closed by the ADR 0037 hardening trio's quorum-guard liveness
  fix (PR 2, PR #136)**, via the previous entry's `control_peer_believed_alive`
  signal — `admin_remove_control_member` now computes `live` = how many of
  the *resulting* voters are actually reachable, refusing when that's below
  a majority (naming the apparently-dead voter(s), pointing at a new
  `--force` escape hatch that is deliberately **independent** of
  `decommission --force-control-remove` — the two flags are separate,
  neither implies the other). The core-level primitive
  (`RaftCore::change_membership`) still has no survivor-liveness guard by
  design — that stays a pure single-server-delta mechanism, unchanged; the
  guard lives one layer up, in `animusd`'s admin action, the only layer with
  a `RaftNode` handle to ask. The regression test this entry named
  (`animusd/tests/control_membership_admin.rs::
  removing_a_live_voter_while_another_is_already_dead_can_silently_strand_
  the_group`) is renamed+flipped to `..._is_refused_without_force` (proving
  the refusal) with a `..._succeeds_with_force` sibling (proving the escape
  hatch still reaches the exact same stranding consequence this entry
  documents, now as informed consent rather than an unconditional default);
  the core-level test
  (`removing_a_live_voter_while_a_third_is_already_dead_can_strand_the_group`)
  is unchanged, since the core itself was never in scope for this fix.
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
- **`Metadata.schemas` (a `SchemaCatalog`) serializes as `{"tables": {...}}`,
  not a bare map — a test reading `/admin/status` JSON must index through
  that wrapper.** Building plan-syskv-ui's `system_table.rs`, a first-draft
  `await_status` predicate checked `status["schemas"].get("orders")` and
  timed out at 15s even though the `ProposeSchema(CreateTableSchema)` call
  had already returned `PutOk` and the endpoint under test was working
  correctly — the predicate was checking the wrong JSON shape, not waiting
  on a slow commit. `SchemaCatalog` is `#[derive(Serialize)]`'d as a
  one-field struct (`{ tables: BTreeMap<TableName, TableSchema> }`) so its
  iteration order stays deterministic and its accessor surface can grow
  without widening `Metadata` — a deliberate wrapper, not an oversight —
  but that means it does **not** serialize as a bare `{"orders": {...}}`
  map the way every other `BTreeMap`-typed `Metadata` field
  (`members`/`tablets`/`policies`/`node_addrs`/`cp_member_addrs`) does. A
  15s-timeout failure with no other symptom (no error reply, no 4xx/5xx,
  the proposal genuinely committed by the time you check by hand) is the
  signature of "polling the wrong JSON path", not "the thing is actually
  slow" — check the field's Rust type before writing the predicate, not
  just its name.
- **`await_leader`/`is_control_leader()` proves *a* leader exists, not that
  *this specific node's* own self-registration has landed in the ADR 0038
  `DRIVER_APPLIED` apply task's published cache yet — a single-shot assert
  right after it is exposed to the apply task's inherent one-hop lag.**
  Adding a `GET /admin/system-table` smoke-check to
  `control_only.rs::control_only_cluster_elects_leader_and_serves_status`
  (iterates every node right after `await_leader`), a first draft asserted
  each node's own system-keyspace browse already showed its
  `RegisterNodeAddrs` row — and flaked under `cargo test --workspace`-level
  contention (passed every isolated run, failed once mixed with the rest of
  the suite's load): a freshly-elected leader's own election no-op can be
  the *only* command the async apply task (`meta_apply_loop`) has drained
  and mirrored so far (`applied_index: 1, count: 0` in the failure), with
  this node's own `RegisterNodeAddrs` proposal still sitting in the Raft log
  or the apply task's queue, not yet in the engine it mirrors into. Same
  family as the `PutOk`-doesn't-mean-committed entry above, but one layer
  further downstream: even a *committed* command isn't necessarily
  *mirrored into the system-keyspace engine and published* yet, because ADR
  0038 PR3 put an async apply task between "committed" and "visible in
  `metadata()`/the system-keyspace browse". Fixed by turning the single-shot
  assert into a bounded poll (10s) that re-fetches `/admin/system-table`
  until a `node_addrs` row appears, matching this codebase's standing
  converged-or-timeout discipline for eventual properties — the general
  form: *any* assertion against a freshly-elected cluster's *replicated,
  apply-task-mirrored* state needs the same poll, not just assertions
  against explicitly-slow operations.
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
- **When a registration/claim CAS is meant to be the "sole claim path," audit
  every *existing* call site that currently establishes the same identity
  through a wholly different, older command before tightening the CAS's
  companion update-only command — a design that only reasons about "the new
  join flow" misses the others and ships a bimodal per-process hang.**
  Retiring the ADR 0036 allocator for ADR 0040's `MetaCommand::RegisterNode`
  CAS, the obvious design compares the *proposed* `NodeAddrs` **and**
  `labels` against whatever's on file, rejecting a mismatch as a genuine
  collision — and it looks right in isolation (a fresh unit test proves the
  fresh-claim and reject-on-mismatch cases cleanly). It broke
  `animusd`'s `runtime_added_voter_survives_leadership_change_to_a_different_
  original_voter` integration test (a real, pre-existing scenario, not a new
  one this PR added) with a 15-second timeout, not a compile error: a
  permanently-non-voter *control-only* growth node
  (`BoundControlNode::start_control_with`) has **no other command that ever
  claims its membership** — no `bootstrap()` insert (it's outside the
  pre-growth set), no `admin_add_member` call (that branch only exists in
  `BoundNode::start_with`'s combined-mode growth path) — so its *own*
  self-registration is the *only* thing that ever creates its `members` row,
  and a labels-strict CAS run against a `Metadata` where that row doesn't
  exist yet works fine there. The failure mode that unit tests alone don't
  reach: a **combined-mode** growth node's `admin_add_member(node, real_labels)`
  (via `UpsertMember`) and its own `spawn_common_tail` self-registration race
  *independently* (two unrelated `tokio::spawn` tasks with no ordering
  between them) — whichever wins first "claims" membership with its own
  labels, and the *other* command's differing labels then permanently fail
  a labels-inclusive CAS comparison against a `Metadata` that already has a
  `members` row but not yet a `node_addrs` one, since there is nothing there
  to ever become "identical" (the losing command retries forever, always
  rejected, since the winner's row never changes). **Fix**: key the CAS on
  the *one field only this command ever writes* (`node_addrs` alone, not
  `members`/`labels`), so "member already claimed by some other, decoupled
  command with no address yet" is treated as an unclaimed *address* slot,
  never a collision — the actual identity/address collision this CAS exists
  to prevent is always visible in that one field regardless of which
  membership-establishing command got there first. **General rule**: before
  shipping a CAS meant to be the sole path for claiming X, grep for every
  *pre-existing* command that can independently create a partial version of
  X (here: three different call sites all capable of inserting a `members`
  row with no matching `node_addrs` entry) and design the collision key
  around the field the CAS actually owns, not the union of every field a
  fully-formed claim eventually has — then add a regression unit test for
  exactly the "claimed via a different command, no address yet" shape, not
  just the two obvious cases a fresh design starts with. (`animus-control`'s
  `meta.rs::register_node_claims_an_address_for_a_member_already_claimed_
  without_one`; ADR 0040 PR4.)
- **Routing every node's self-registration through one shared command can
  silently make a role that must never be placement-eligible show up in the
  placement-eligible set — a second, unrelated bug the same "unify the
  claim path" change can introduce even after the first one (above) is
  fixed.** Once `RegisterNode` became the sole path that inserts a `members`
  row, a **control-only** node's own self-registration inserted one too
  (labels + `Down` status, the same as every other node) — and the existing
  `Down → Active` promotion chain (ADR 0030 §1, unchanged) promoted it the
  moment it started heartbeating, same as any data-capable node. Nothing
  about `RegisterNode`'s own apply logic was wrong in isolation; the bug was
  purely in *scope* — a control-only node has no `raftkv` role and can never
  host a tablet, so its mere presence in `members` silently makes it a
  placement candidate the moment the reconciler considers `Active` members,
  corrupting replica-set assignment with no error anywhere in the write
  path. Caught by `animusd/tests/control_only.rs` going bimodal ("put via
  control node did not forward... Elapsed") — a downstream symptom several
  layers removed from the actual cause, not a direct assertion on
  membership. **Fix**: gate the `members`-row insert on the registering
  node's own declared role (`NodeAddrs.role == "control"` skips it
  entirely) — the address book claim (`node_addrs`) still succeeds for every
  role; only the placement-eligibility side effect is role-gated. **General
  rule: when unifying several roles' registration/bootstrap paths onto one
  shared command, explicitly enumerate which side effects that command
  produces are safe for *every* role versus which are only safe for a
  subset — a command that "just inserts a row" can smuggle in an implicit
  eligibility grant that was previously only reachable from a role-specific
  code path.** (`animus-control::meta.rs::
  register_node_never_claims_membership_for_a_control_role_registration`,
  `animusd/tests/control_only.rs`; ADR 0040 PR4.)
- **A test-only deterministic double for a trait bounded `Send + Sync` (the
  `Env`/`Rng` seam, ADR 0003) must use atomics, not `Cell`, even though the
  double never crosses a real thread boundary in the test itself.** A
  scripted `Rng` built to prove `NodeId::mint`'s draw shape from a fixed
  sequence used `Cell<usize>`/`Cell<u64>` for its cursor/fallback-counter —
  compiles fine as a bare struct, then fails with "cannot be shared between
  threads safely" the moment `impl Rng for ScriptedRng` is written, because
  the *trait itself* requires `Send + Sync` (every `Env`/`Rng` implementor
  must be usable from `tokio::spawn`'d code in production) — the compiler
  enforces this at the `impl` site regardless of whether any given test
  actually spawns the double across threads. Fix: `AtomicUsize`/`AtomicU64`
  with `Ordering::Relaxed` (a single-threaded test never contends on them, so
  the ordering choice is moot) — same shape as any other `Send + Sync`
  interior-mutability need in this codebase. General rule: a test double for
  an `Env`-seam trait is never exempt from that trait's own bounds just
  because the specific test using it happens to be single-threaded — check
  the trait's supertrait bounds before reaching for `Cell`/`RefCell`.
  (`animus-env/src/lib.rs::tests::ScriptedRng`, ADR 0040 PR4.)
- **When a fix adds a wait-for-async-precondition to ONE phase of a
  multi-phase test, audit every other phase for the same shape — the race
  doesn't know which phase it's in.** ADR 0040 PR4 fixed
  `control_membership_split.rs`'s GROW phase to poll for a joining node's
  background self-registration (`spawn_common_tail`'s fire-and-forget
  `register_node` task) landing before calling `admin/member/add` — two
  legitimate `RegisterNode` proposals for the same id otherwise race with
  different `NodeAddrs` payloads, and the registration CAS *correctly*
  rejects the loser ("already claimed by a different registration"). The
  REPLACEMENT phase of the same test performed the identical
  join-then-admin-add sequence with no such wait and inherited the identical
  race, surfacing (rarely under load at first, then ~1-in-2 in isolation as
  timing shifted with unrelated merges) at
  `control_membership_split.rs:454`. Not a CAS bug — the CAS did its job;
  the test raced two registrars it was responsible for sequencing. Fix:
  the same poll, mirrored onto the replacement phase. The general audit
  question: "this test just gained a wait — where else does it (or its
  siblings) perform the same async-then-act sequence without one?"
- **To test a monotonic counter's overflow-carry fallback (bump the next field
  up, reset this one to 0) without looping to the boundary, prime state one
  step *below* the boundary and let the very next real operation supply the
  final `+1` — don't prime state *at* the boundary and expect it to still be
  there.** Testing `Hlc`'s logical-overflow carry (ADR 0018 §2 PR1,
  `crates/animus-cp-data/src/hlc.rs`) by calling `witness` with
  `remote.logical = 2^LOGICAL_BITS - 1` to "prime" the clock at the top of its
  budget failed: `witness`'s own receive rule adds `+1` to the max of the two
  logical values *as part of computing the primed value*, so priming at the
  boundary itself triggered the overflow carry immediately, leaving the
  "primed" state already at `(wall+1, 0)` instead of at the intended
  boundary — the assertion on the primed value failed, not the assertion on
  the follow-up overflow. Fix: prime one below the boundary
  (`2^LOGICAL_BITS - 2`) so the priming call's own `+1` lands exactly at the
  boundary with no overflow yet, then a second, separate call is the one that
  overflows and is asserted on. General rule: when a test needs to "jump to
  right before X happens," check whether the very setup step used to get
  there is itself governed by the same increment rule being tested — if so,
  it will overshoot by exactly one step past where the story expects it to be.
- **A DRIVER_APPLIED design (ADR 0038) makes every cache-backed read lag Raft
  by an amount bounded only by apply-task starvation — so a test polling such
  a read must poll for *forward progress* of the apply watermark, never
  against a flat deadline.** The `decommission_drains_removes_and_allows_id_
  reuse` flake: `/admin/status` reads `Metadata` off the async apply task's
  `cache`, deliberately decoupled from the consensus loop so a slow engine
  merge can't trip an election — which means under `cargo test --workspace`-
  scale CPU contention the apply task can sit frozen for 30s+ (instrumented:
  `commit_index`/`last_applied` converged in <1s while `engine_applied_index`
  made zero progress for a full 60s, then caught up fine) with nothing wrong.
  A flat 30s deadline turns that legitimate lag into a "flake"; bumping it
  just moves the cliff. The principled shape: poll the apply task's own
  watermark (`/admin/raft`'s `engine_applied_index`), fail only when it
  *stops advancing* for a generous idle window with the awaited effect still
  absent (that is a real stall, not contention), plus a large overall
  backstop against livelock-shaped progress. This is the converged-or-timeout
  rule's second-order refinement: when the property's convergence has no
  contention-independent bound, the timeout must be on *progress*, not on
  *arrival*. (`animusd/tests/decommission.rs`, `ControlHandle::
  engine_applied_index`.) `animusd/tests/cluster_growth.rs` had the identical
  anti-pattern at several call sites (member-promotion, rebalance-convergence,
  post-kill tablet repair, `/admin/peers` propagation) and got the same
  treatment; the shape is now factored into a shared `support::
  poll_until_or_stalled` helper (`animusd/tests/support/mod.rs`) rather than
  hand-rolled per file.
- **A retry loop whose confirmation read can never observe its own success is
  a resurrection cannon.** The same investigation's production half:
  `register_node`'s propose-then-confirm loop confirmed via
  `metadata_fresh()`, which on a growth/non-voting node structurally never
  advances (ADR 0030: its local Raft log doesn't move) — so every growth
  node's one-shot self-registration re-proposed an already-committed
  `RegisterNode` blindly for the whole `SCHEMA_COMMIT_TIMEOUT`, and a
  drain+remove landing inside that window let a stale re-propose recreate
  the just-removed member (apply can't tell a stale duplicate from a fresh
  claim), which a live heartbeat then promoted straight back to `Active`.
  Two generalizable checks: (a) for every propose-and-await confirmation,
  ask "can *this caller's* read path ever observe the effect?" — a
  confirmation source that is correct for one node shape (voter) can be
  structurally blind on another (growth mirror); (b) an idempotent-looking
  re-propose is not idempotent across an intervening *delete* — retry loops
  for claim-style commands must stop as soon as any read shows the claim
  ever existed, not only when the freshest read does. (`animusd::
  register_node`'s `effective_metadata()` fallback.)
- **`Simulator::run_for(dur)` always advances the clock to the full
  deadline once idle — it does not "return early" just because the future
  you were driving already resolved.** `run_until`'s loop drains every ready
  task, then either fires the next scheduled timeline event (if before the
  deadline) or, once none remain, jumps `clock` straight to the deadline and
  returns. A helper that calls `run_for(Duration::from_secs(2))` once *per
  read* to drive a spawned `linearizable_get` therefore advances that read's
  serve timestamp by a full 2 (virtual) seconds every single call — fine for
  an ordinary read-reflects-a-write assertion, but fatal for anything that
  cares about *how close together* consecutive reads' timestamps are (ADR
  0018 §2/PR2b's ceiling-amortization test: the naive per-read-`run_for`
  version advanced each read's ts by seconds, trivially exceeding the
  500ms `HLC_MAX_OFFSET` window between every pair and making every read
  propose its own ceiling). Fix: drive a whole sequential batch — spawn one
  task that loops the operation N times with no artificial gap, then call
  `run_for` **once** with a budget sized for the whole batch — which is also
  what keeps consecutive real-world back-to-back operations' timestamps
  genuinely close, matching the workload the amortization is meant to
  cover. (`crates/animus-cp-data/tests/ts_cache.rs`.)
- **Never `block_on` a `RaftKvNode` linearizable read (`linearizable_get`/
  `_scan`, `read_at`/`scan_at`) — it hangs forever, not just "runs
  synchronously."** `futures::executor::block_on` polls its future on its
  own local executor, entirely separate from `Simulator`'s; a read barrier's
  `.await` points (confirmation polling, at minimum) are timeline events
  registered against the `Simulator`'s own clock, resolved only when
  `Simulator::run_for`/`run_until` is *actively called* to step them. Call
  `block_on` on such a future and nothing ever drives that clock forward —
  the calling thread blocks on a future that can structurally never
  complete. The fix (already documented in `animus-cp-data/CLAUDE.md`'s
  Tests section, but easy to violate by habit when reaching for a "just
  read this value" one-liner alongside genuinely synchronous calls like
  `node.put(..)`): always drive a linearizable read as a spawned task +
  `run_for`, the same shape every other test in the suite already uses —
  never mix in a bare `block_on` for "just one more read" partway through a
  test that's otherwise correctly using the spawned-task pattern. A hang
  with near-zero CPU time consumed over the whole wall-clock duration (not
  a busy spin) is the tell: something is waiting on a clock nobody is
  advancing. (`crates/animus-cp-data/tests/ts_cache.rs`.)
- **Prove a new regression test actually catches the bug by running it
  against the pre-fix code, not just against the fix.** A test that passes
  post-fix is consistent with "the test works" *and* with "the test asserts
  the wrong thing and would pass either way" — the two are indistinguishable
  from a single green run. The cheap check: `git stash push -- <fixed
  file(s)>`, re-run the new test, confirm it fails with the expected
  symptom, `git stash pop`. For the ADR 0041 drop-table-cascade fix
  (`ClientCtx::drop_table` in `animusd/src/lib.rs`), this caught nothing
  wrong — but it's the difference between "I wrote an assertion" and "I
  verified the assertion is load-bearing," and it costs one extra
  `cargo test` invocation. Worth doing for any fix landing with exactly one
  new regression test, especially when the bug is an *omission* (a cascade
  step that never ran) rather than a wrong-value computation, since an
  omission bug is the shape most likely to also be missing from a
  carelessly-written test.
- **A cascading delete across replicated definitions must read the
  definitions *before* deleting whatever they're keyed on, and needs a
  second sweep keyed on a structural invariant for anything that can be
  provisioned concurrently with the delete.** `ClientCtx::drop_table` (ADR
  0041 §5) enumerates a table's GSI `IndexDef`s via `metadata_fresh` before
  dropping the base schema — reversing the order would delete the base
  schema (and the defs riding on it) first, leaving nothing to enumerate on
  a retry after a mid-drop crash. But enumeration-then-cascade only catches
  what existed at enumeration time; a background process that lazily
  provisions the very thing being cascaded (here, the GSI drain
  provisioning a hidden table's first tablet) can race a fresh one into
  existence afterward. The fix pairs the definition-keyed pass with a
  second sweep keyed on a structural invariant that survives the
  definitions' deletion — here, the tablet map's own `<base>$<index>` naming
  convention (`animus_dynamo::split_index_table_name`), not the (by-then-gone)
  `IndexDef`s. The second sweep is what also makes the fix retroactive: it
  cleans up orphans left by every **pre-fix** drop, for free, since it
  depends on nothing the fix itself created. (`crates/animusd/src/lib.rs`,
  `ClientCtx::drop_table`, 2026-08-13.)
- **A multi-consumer cursor bump must be gated on the WHOLE sweep succeeding,
  never fused into each individual partition's own commit entry — even
  though the two look interchangeable at first glance.** Reworking the GSI
  drain from "consuming is trimming" to a cursor (ADR 0042 §7/§8), the
  obvious design was: each `reconcile_partition` call writes its own
  footprint update *and* bumps the tablet-wide "gsi" cursor to this tick's
  overall max HLC, in the same atomic entry (mirroring the old design's
  "footprint + delete the records it covers, one entry" shape). That is
  unsound: the cursor is a **single row covering every partition in the
  tablet**, not a per-partition value, so bumping it to the *tick-wide* max
  the instant the *first* partition's entry lands would claim every
  **other**, not-yet-reconciled partition's records (up to that same max) as
  consumed too — a crash between the first and second partition's entries
  then leaves the cursor over-claiming coverage the second partition never
  got, and the trim janitor would delete its records regardless, silently
  and permanently freezing that partition's GSI rows stale (no change record
  survives to ever re-trigger it). The fix: compute the sweep's overall max
  HLC once, reconcile every dirty partition sequentially (propagating any
  error immediately, before the loop advances), and only *after* the whole
  loop returns `Ok` does a single trailing write bump the cursor — by that
  point every partition the max HLC could implicate has already had its own
  footprint update independently confirmed durable, so a crash before the
  trailing write just leaves the cursor wherever it was (safe, re-covers
  everything on the next tick) and a crash after it is the fully-covered
  case. The general form: a watermark that summarizes N independent
  sub-operations is only safe to advance once *all* N have been individually
  confirmed, not on the first one succeeding, even if advancing it earlier
  would be "usually" correct. (`crates/animusd/src/index_drain.rs`,
  `drain_tablet`, 2026-08-14.)
- **A propose-and-poll confirmation helper that only knows how to probe one
  specific write shape (here, "a `KIND_CHANGE` deletion in the batch") silently
  stops confirming anything the instant a caller's batch stops containing that
  shape.** `ClientCtx::cp_kind_write_raw`'s original probe searched the batch
  for a `KIND_CHANGE` entry with `value: None` and, finding none, returned
  `Ok` right after `Accepted` — correct for the old design (every reconcile
  batch always deleted at least one record), silently wrong for the ADR 0042
  cursor rework (a footprint-only or cursor-only batch has no such entry at
  all), which would have left every reconciliation and cursor bump confirmed
  by nothing more than "appended to the leader's log locally," reopening
  exactly the fence-miss-looks-like-success gap the original probe existed to
  close. Fixed by confirming the batch's **last** write generically (`local_get_kind(kind,
  key) == expected_value`) instead of searching for one specific shape — sound
  because the whole batch is one atomic, whole-or-nothing Raft entry, so any
  single write's landed effect proves every other write in the same entry
  landed too. The general form: when a confirmation mechanism special-cases
  "the shape my one caller happens to produce," a new caller with a
  differently-shaped (but equally atomic) batch silently degrades the
  confirmation rather than failing loudly — prefer a probe that works for
  *any* member of an atomic batch over one keyed to a specific write's
  content. (`crates/animusd/src/lib.rs`, `ClientCtx::cp_kind_write_raw`,
  2026-08-14.)
- **In a real `ProdEnv` test, "the tablet map shows the tablet" and "this
  node has actually started hosting its `CpGroup`" are two different,
  separately-converging facts — polling only the first before reaching for
  `ClusterEdgeState::local_cp` is a real (if usually narrow) race, not
  paranoia.** A split or merge test that fetches a fresh child's/survivor's
  `CpGroup` handle immediately after `Metadata` shows the new tablet count
  can hit `local_cp` returning `None` — the per-node tablet-host reconciler
  (ADR 0031) still needs its own tick to stand the group up locally.
  Poll-for-`Some` (`local_cp(tablet).is_some()`) before ever unwrapping it,
  the same way every other eventually-true fact in these tests is awaited,
  rather than chaining an `.expect(..)` straight off a `Metadata` poll.
  Separately: a merge survivor's *widened* `StorageScope` — needed before an
  absorbed sibling's own physically-still-present rows (e.g. its own cursor
  row, ADR 0042 §7) become visible through the survivor's scans — is
  *also* a distinct, later-converging fact from "the tablet map shows one
  tablet again"; assert on it with its own poll, not a single check right
  after the merge's own convergence. (`crates/animusd/src/index_drain.rs`,
  `gsi_drain_cursor_tests`, 2026-08-14.)
- **There is no production-reachable way to relabel an already-`Active`
  cluster member today — a real, small operational gap, not just a test
  inconvenience.** `POST /admin/member/add`/a join's `labels` parameter
  only ever *claim* a fresh identity; `ClientCtx::admin_drain`'s
  local-leader-only `UpsertMember{Leaving}` preserves whatever labels are
  already on file but never sets new ones; the generic wire path
  (`ClientRequest::ProposeSchema(UpsertMember{..})`) is gated by
  `is_relayable_command` to `status: NodeStatus::Down` only — proposing
  `Active` with new labels through it is rejected outright, by design (the
  gate that keeps `admin_drain`-class actions local-leader-only). Building
  a sim test that needed to label 3 of 5 already-bootstrapped nodes (ADR
  0043 §2's optional stream-shard isolation, PR B6) found the one
  production-reachable workaround instead of adding new admin API surface
  for a test-only need: propose `UpsertMember{labels: new, status: Down}`
  (passes the gate) on a member that is genuinely still alive and
  heartbeating — the very next `detect_loop` tick observes it alive and
  re-promotes it to `Active` via `transition()`, which reads the label
  *just committed* off `Metadata` and preserves it verbatim. The node
  never actually goes anywhere; it just flaps through `Down` for one
  detector tick. This is the general shape worth remembering: when a
  cluster-state field has an update path gated to a narrower status than
  the one you need, look for whether some *other* legitimate transition
  already reads that field fresh and would carry your change through it —
  cheaper and more honest than adding a new mutation path whose only real
  caller would be a test. (`crates/animusd/tests/
  stream_shard_label_isolation.rs::label_node`, 2026-08-14.) If a real
  operator need for relabeling an active member ever surfaces, that's a
  genuine follow-up (a dedicated admin action, not this flap trick).
- **`animus-sim` has no `tokio` dependency at all — a new test file/module
  in that crate cannot use `#[tokio::test]`, even though every other
  async-heavy crate in the workspace does.** Writing the `SimSegmentStore`
  fault-injection tests (ADR 0043 §A7, `animus-sim/src/segment_store.rs`)
  as `#[tokio::test] async fn ...` compiled fine locally with `cargo check`
  scoped to just that file mentally, but failed at `cargo build
  --all-targets` with a missing-macro/missing-crate error — this crate's
  own convention (`tests/disk_faults.rs`, `tests/determinism.rs`) is
  `#[test] fn ...` (plain, sync) that spawns the async workload via
  `env.spawn_task(async move { .. })` and drives it to completion with
  `Simulator::run_for(dur)`/`run_until_quiescent(max_steps)`, reading the
  result back out of a shared `Arc<Mutex<_>>` afterward. This isn't
  cosmetic: the simulator's executor is the *only* thing that can resolve a
  `Clock::sleep` (it needs the virtual timeline actually advanced), and a
  panicking assertion inside the spawned block propagates out of the
  `run_*` call exactly like any other panic, since polling happens
  synchronously on the test's own thread — so the pattern costs nothing in
  either determinism or assertion ergonomics versus a real `async fn` test,
  it just has to be spelled differently. General check before writing a new
  test in an unfamiliar crate: grep that crate's existing `tests/*.rs` for
  its actual async-driving idiom before assuming `#[tokio::test]` — "this
  crate is full of `.await`" does not imply "this crate depends on tokio."
  (`crates/animus-sim/src/segment_store.rs`, ADR 0043 round-3 PR2,
  2026-08-14.)
- **A request/reply RPC over the `Network` seam cannot use `tokio::sync::
  oneshot` (or any executor-specific channel) to correlate a reply with its
  request — `SimEnv` callers have no tokio runtime present at all.** Building
  `ClusterSegmentStore`'s K-replica `put`/`get`/`delete` fan-out (ADR 0043
  §A7b), the natural shape — send a request, `await` a oneshot the reply
  handler completes — compiles fine in a crate that already depends on
  `tokio` (`animus-cp-data` does, for one real-thread test), but silently
  never resolves under `SimEnv`: nothing in the simulator's own cooperative
  executor drives a tokio channel's waker. The house pattern (already
  established by `RaftKvNode`'s own `ReadProbe`/`ReadProbeAck` read-barrier
  confirmation, `lib.rs`) is a **shared `Mutex<BTreeMap<req_id, Option<Reply>>>`
  plus an `env.sleep`-based poll loop**: register a slot keyed by a
  monotonically-increasing `req_id` before sending, have the single serving
  task's dispatch loop `stash` the decoded reply into that slot by `req_id`
  when it arrives (on the *same* stream the request went out on — a
  request and its reply are just two variants of one wire enum, sharing one
  inbox), and have the waiting call `peek` the slot in a bounded
  `loop { check; if done break; if deadline break; env.sleep(POLL).await }`.
  This is strictly more verbose than a oneshot but works identically under
  both `SimEnv` and `ProdEnv` with no executor-specific primitive anywhere —
  the same reason `AtomicWaker` (not a tokio-only waker) is what
  `RaftKvNode`'s wake-on-propose signal uses. General check before reaching
  for a channel/oneshot/notify primitive in a crate whose tests run under
  `SimEnv`: does it come from `std`/`futures` (executor-agnostic) or a
  specific async runtime crate? If the latter, it needs the poll-a-shared-
  slot shape instead. (`crates/animus-cp-data/src/cluster_segment_store.rs`,
  ADR 0043 round-3 PR3, 2026-08-14.)
- **`Simulator::crash`'s crashed-check happens at *delivery* time, not send
  time** (`fire_event`'s `Event::Deliver` arm, `animus-sim/src/lib.rs`) —
  which makes "kill a node while a message to it is still in flight" a
  deterministic, seed-reproducible scenario rather than a race to script by
  hand: set a nonzero `NetConfig::base_delay`, send, sleep for less than
  that delay, then `crash` the target — the message is guaranteed to still
  be in the timeline (not yet delivered) at the moment of the crash, so it
  is dropped exactly as if the node had died before the message arrived.
  Used to test `ClusterSegmentStore`'s "node death mid-put" case
  deterministically instead of accepting a flaky race or, worse, only ever
  testing "target already down before the call starts" (a strictly weaker
  scenario the two are easy to conflate). (`crates/animus-cp-data/tests/
  cluster_segment_store.rs`, ADR 0043 round-3 PR3, 2026-08-14.)
- **The `dynamo_index_scan` full-workspace flake signature, adjudicated
  2026-08-14**: an intermittent `raftkv wal sync` expect-panic
  (`animus-cp-data/src/lib.rs`, around the WAL-sync `.expect(..)` in the
  apply task's `flush_wal`, roughly line 4098) surfacing on a tokio worker
  thread only during `cargo test --workspace`-scale multi-node `ProdEnv`
  teardown — the persist task racing the node's own shutdown for the same
  disk handle, the same "`abort()` is a request, not a guarantee" family
  already documented above (`ProdEnv::shutdown`/`Node::shutdown` abort
  spawned tasks without waiting for them to actually stop, so a
  still-in-flight `sync()` can observe a half-torn-down env). Confirmed 5/5
  green solo (`cargo test -p animusd --test dynamo_index_scan`) — the panic
  only reproduces under concurrent whole-workspace CPU/IO contention, never
  in isolation. **Before suspecting the state machine (a real
  `assert_ts_monotonic`-class bug) for a teardown-adjacent panic in any
  `ProdEnv` integration test, run the one failing test binary solo first** —
  if it's consistently green alone, the failure signature is almost
  certainly this same shutdown-race family, not new logic in whatever this
  session happens to be touching. Named here as its own entry (rather than
  folded into the existing "abort() is a request" entry) so a future grep
  for `dynamo_index_scan` or `raftkv wal sync` finds the adjudication
  directly. (`crates/animus-cp-data/src/lib.rs`,
  `crates/animusd/tests/dynamo_index_scan.rs`, adjudicated during ADR
  0042/0043 round-3 PR4, 2026-08-14.)
- **"Reachable only via a gate that widened for exactly this case" needs an
  end-to-end test, not just a component-level one — the gate and the
  function it feeds can each look locally correct while their *composition*
  drops the very case the gate was widened for.** Building the DynamoDB
  Streams sealer's hot-trim rework (F10/F12-b), the per-tablet loop's outer
  gate (`gsis.is_empty() && !stream_enabled`, `index_drain.rs`) skips a
  tablet once its stream disables and it has no GSI — correct for a table
  that *never* streamed, wrong for one that just finished a disable's final
  seal: skipping it forever means the hot-trim arm never runs again to
  actually delete the now-fully-sealed hot tail, whose correctness had been
  silently depending on a *race* (the periodic loop happening to tick, with
  the schema not yet flipped, in the narrow window between the final seal's
  own commit and `SetTableStream{None}`'s). One test
  (`disabled_draining_stream_does_not_block_trim`, 2 writes) passed reliably
  because that race happened to resolve in its favor every run; a materially
  identical second test (`disable_final_seal_then_reenable_continues_the_
  epoch_chain`, 3 writes) reproducibly timed out, because the tiny
  extra work shifted the race the other way. Neither `trim_janitor` in
  isolation (its own unit-shaped tests all passed — "no expected term ⇒
  block" was internally consistent) nor the outer gate in isolation looked
  wrong; only running the *disable-then-verify-convergence* sequence
  end-to-end, twice, with slightly different timing, exposed that the gate
  needed widening (`ever_streamed`, keep visiting a tablet that has ever
  sealed) **and** `trim_janitor`'s own "no expected term" branch needed to
  flip from "block" to "trim unconditionally" (the two fixes are a pair —
  widening the gate alone would have reached the old "block" branch and
  changed nothing). General rule: when a background loop's own top-level
  gate decides "does this item still matter to me," and a later lifecycle
  event (disable, drop, expire) can make the answer flip from yes to no,
  write the test that drives *through* that transition and polls for the
  eventual-consistency property on the other side — a gate widened for a
  new terminal state, paired with a function whose fallback branch was
  never re-examined for that same state, is exactly the shape that passes
  every unit test and flakes (or silently stalls) in integration.
  (`crates/animusd/src/index_drain.rs`, ADR 0042/0043 round-3 PR5,
  2026-08-14.)
- **A "bytes" accessor's own scope is part of its contract, not an
  implementation detail — check which `StorageScope`/row-kind it measures
  before reusing it for a new trigger.** `RaftKvNode::approx_bytes` was
  deliberately narrowed to the **base** kind scope by ADR 0034's own fix
  (so auto-split stops reacting to change-log churn) — a fact stated
  plainly in that method's doc and this crate's own `CLAUDE.md`, and easy
  to miss when reaching for "the byte estimate" to build a *different*
  trigger. The Streams sealer's size trigger needs `KIND_CHANGE`'s own
  bytes specifically (ADR 0043 §A3's "When": "`KIND_CHANGE` scope
  `approx_bytes`") — calling the existing `approx_bytes()` compiled, ran,
  and even passed several tests (small test tables happen to write base
  rows and change records of comparable size, so the wrong scope's number
  still crossed the same threshold at roughly the same time), until an
  end-to-end auto-split test on a *streamed* table exposed the mismatch
  indirectly. Fixed by adding a kind-scoped sibling
  (`RaftKvNode::approx_bytes_kind(kind)`/`CpGroup::approx_bytes_kind`) that
  takes the row-kind's own `StorageScope` instead of assuming the base one
  — never widen an existing narrowly-scoped accessor back out, add a
  sibling with the same shape over a different scope. General rule: before
  wiring an existing "cheap estimate" accessor into a new caller, re-read
  its own doc for *which* scope/kind/range it was deliberately narrowed to
  and *why* — a byte/count estimator that looks generic by name can be
  pinned to one specific scope for a reason that has nothing to do with
  your new use case. (`crates/animus-cp-data/src/lib.rs`,
  `crates/animusd/src/index_drain.rs`, ADR 0042/0043 round-3 PR5,
  2026-08-14.)
- **A relayed internal-only `ClientRequest` variant must be wrapped in
  `Forwarded` at *every* call site that sends it across the wire, not just
  handled correctly on receipt** — the receiving side's "refuse if sent
  bare" gate exists precisely to reject exactly the mistake of sending it
  unwrapped, so a caller that forgets the wrapper doesn't hang or corrupt
  state, it fails **loudly and immediately** with the refusal's own error
  message. Adding `ClientRequest::ForceSeal` (the DynamoDB Streams
  disable-triggered final seal, round-3 sealer PR) initially called
  `ClientCtx::relay(addr, ClientRequest::ForceSeal { .. })` directly instead
  of `relay(addr, ClientRequest::Forwarded { request: Box::new(ForceSeal
  {..}), .. })` — every unit test passed (they all happened to run on a
  single node, where the *local* branch of `force_seal_tablet` never goes
  through `relay` at all), and the gap was caught only by
  `dynamo_streams.rs`'s existing `update_table_stream_enable_and_disable_
  through_every_node` test, which specifically issues the disable from a
  **non-leader** node and therefore exercises the forwarding branch. The
  loud, specific error (`"...must be sent wrapped in Forwarded"`) made the
  diagnosis immediate once a real multi-node path exercised it. General
  rule: a new forwarded-command variant's own test coverage must include at
  least one call from a node that is **not** the tablet's leader — a
  same-node test suite can pass in full while every cross-node send is
  broken, because the wrapping mistake only manifests on the wire, not
  in-process. (`crates/animusd/src/lib.rs`, ADR 0042/0043 round-3 PR5,
  2026-08-14.)
- **An "every node" read-path test must wait for convergence on *every*
  node, not just the one that drove the write** — a per-node `Metadata`
  replica can lag its own control Raft's commit by a few milliseconds, and
  a handler that reads `ClientCtx::effective_metadata()` (DynamoDB Streams'
  `GetRecords`/`GetShardIterator`, ADR 0042 §3/§7 — resolved *fresh, per
  call, per node*, by design, so an open-shard iterator can survive a seal)
  resolves against *that node's own* snapshot. `dynamo_streams.rs`'s
  `get_records_on_a_sealed_shard_works_from_every_node` originally polled
  only `nodes[0]` for the seal to land, then queried all three nodes in a
  tight loop — flaky roughly 1 run in 3 under `--test-threads=1`, always as
  "node 1/2: shard must exhaust" failing (a genuinely sealed shard's
  `GetRecords`, served by a node whose own catalog view hadn't caught up
  yet, fell through to the open-shard branch instead, which never nulls).
  Not a correctness bug in the handler — this is exactly the stream's own
  documented eventually-consistent contract self-healing within
  milliseconds — but a one-shot assertion right after the write is exactly
  the "fixed-deadline one-shot assert on an eventual property" this
  codebase's testing doctrine already warns against; the fix was polling
  `nodes.iter().all(|n| ...)` before entering the per-node assertion loop,
  not touching the handler. General rule: when a test's very *point* is "the
  same operation must behave identically issued through every node," the
  convergence wait that precedes the loop must also cover every node, or
  the loop races the propagation it's supposed to be testing past, not the
  behavior itself. (`crates/animusd/tests/dynamo_streams.rs`, ADR 0042/0043
  round-3 PR6, 2026-08-14.)
- **A test that exercises physical *removal* of a chained/derived-numbered
  entity for the first time needs at least two generations in the chain,
  not one — a single-entry test can pass for the wrong reason (or, worse,
  hang) because the very row it means to reclaim is structurally
  unreclaimable alone.** Building the DynamoDB Streams segment janitor
  (ADR 0043 §A9, round-3 PR7), the first retention test wrote one item,
  sealed it, waited for its row to be marked *and physically removed*, and
  timed out — not a bug in the removal logic, but in the test's own
  premise: `index_drain::seal_now`'s epoch numbering is "the chain's own
  highest existing row, plus one" (a design that only holds while the
  catalog never shrinks), so the janitor correctly refuses to ever
  physically remove a tablet's *current* highest-epoch row while the
  tablet still exists (removing it would let a future seal silently reuse
  the same epoch number for different data). A single-write test's only
  row is *always* the current max, so it can never be reclaimed by design
  — the fix was two writes/seals in sequence, so the first stops being the
  max once the second exists. General rule: before writing a test (or
  reviewing PR-added retention/GC/reclaim code) for "the Nth generation of
  a chained identity gets removed," check whether identity derivation for
  that chain reads *only currently-present* entries (a count, a `max()`, a
  `last()`) rather than an independent, ever-increasing counter — if so,
  removing the wrong generation (or testing removal with too few
  generations present) is a live correctness hazard, not just a
  test-construction detail. (`crates/animusd/src/segment_janitor.rs`,
  `crates/animusd/tests/stream_janitor.rs`, ADR 0043 §A9, round-3 PR7,
  2026-08-14.)
- **A pre-existing, timing-sensitive flake found incidentally while
  running the full workspace gate — not caused by, or related to, the
  change in flight — should be reported, not silently fixed or silently
  ignored.** `animusd`'s `tests/dynamo_txn.rs::
  transact_get_items_never_observes_a_torn_pair_under_concurrent_writes`
  failed once under `cargo test --workspace`, then failed again roughly 1
  in 4 *solo* re-runs (untouched by this PR's changes — a torn-snapshot
  assertion in the ADR 0018 §2/PR7 `TransactGetItems` quiescence-retry
  path, nothing to do with streams) — genuinely flaky on its own, not a
  regression this PR introduced (confirmed by repeated solo runs both
  passing and failing with identical code). Per this repo's own "separate
  PRs for incidental bugs" convention, the fix belongs in its own change,
  not folded into an unrelated PR's diff — but the *discovery* still
  belongs in this log and in the reporting PR's own description, so the
  next person who hits it doesn't have to re-derive "is this me?" from
  scratch. (2026-08-14.)
  **Baseline adjudication (round-3 PR8, so the eventual fix has numbers to
  work against)**: solo re-runs of exactly this test — `main` 4/10, the
  streams round-3 salvage boundary `064bbac` 4/10, `3b3c7ae` (PR7's tip,
  also this PR's own base — no txn-path changes landed between them) 5/10.
  Flat within noise across three points spanning the whole round-3 stack;
  streams work never touched this path. Genuinely pre-existing, not
  introduced or worsened by any PR in this stack.
  **Update (2026-08-15, torn-pair-fix stack PR2) — the mechanism, and a
  worse baseline that still isn't this PR's fault.** The torn-pair-fix
  stack (PR1: `mint_pushed` clock-witnessing-runaway fix; PR2: this file's
  `run_transact_get`/`quiescent_multi_get` uniform-single-shot-round fix,
  ADR 0018 §2's newest amendment) targeted two *read-timing* mechanisms
  that can produce a torn `TransactGetItems` snapshot. Both are fixed and
  independently verified (a dedicated `SimEnv` regression,
  `txn_serializable.rs::tight_pair_transactions_never_observe_a_torn_
  snapshot`, 0 failures across 30+ seeds). Yet solo re-runs of *this* wire-
  level test against the fixed stack still fail at a rate at least as high
  as ever (PR1-only baseline: 7/10; PR1+PR2: 17/20) — debugging traced *why*,
  not just confirmed *that*: the participant key ("b" in the test) simply
  **stops receiving any further writes partway through the writer's loop**
  (observed stuck anywhere from step 4 to step 14 of 15) while the anchor
  key ("a") keeps committing correctly to the very end, and the writer's
  own `TransactWriteItems` calls never see a failure throughout — i.e. this
  is a **write-side 2PC participant-write-loss** bug (the participant's
  own intent silently stops advancing while the coordinator keeps
  reporting success on every subsequent step), structurally unrelated to
  either read-side mechanism the torn-pair-fix stack closes. It reproduces
  identically with zero of this stack's code present, confirming (again)
  it is pre-existing, not introduced by either PR. **Still needs its own
  root-cause delivery** (a "Bug 3," in the participant-stage/recovery-push
  interaction, likely a duelling-decider-class race given how aggressively
  a live reader's own recovery pushes can now fire — worth checking first
  whether `ClientCtx::txn_prepare_pushing`'s `IntentBlocked` retry ever
  itself pushes the blocking transaction, since today it only waits and
  hopes something else clears it). Do not fold it into a future PR's diff
  without its own investigation and acceptance evidence — same convention
  as the entry above.
  **RESOLVED (2026-08-15, torn-pair-fix stack PR3)** — not a duelling-
  decider race after all: `ClientCtx::recovery_resolve` grouped a
  transaction's participants by table name alone, misrouting a resolve to
  the wrong tablet of a split table, and `KvCommand::TxnResolve` had no
  apply-time fence to catch it (every *other* key-writing variant did).
  See ADR 0018's 2026-08-15 amendment for the full mechanism/fix, and the
  three entries below for the generalizable lessons this investigation
  leaves behind.
- **`RaftKvNode::start_scoped` pins every group to `PRIMARY_STREAM` — a
  `SimEnv` test that starts more than one tablet group on the *same* set of
  node ids (any split/merge scenario sharing physical nodes across tablets,
  ADR 0026 Stage B) must use `start_hosted(.., stream = tablet_id.0)`
  instead, or the two groups' Raft traffic cross-talks on one node's shared
  inbox and corrupts both.** The DynamoDB Streams lineage-walk corpus's
  `combined_chaos` scenario (a leader-kill *and* a split on the same 3 node
  ids) initially livelocked leader election on the *parent* group the
  instant a sibling group started — `elect()` never found a stable leader
  even after 4 seconds of simulated time, because every `AppendEntries`/
  vote message either group sent was being delivered into whichever
  group's `RaftCore` happened to poll the shared inbox next, so both groups
  saw a stream of messages that made no sense to their own `RaftCore`.
  Every existing test that only ever runs ONE group per node id
  (`raftkv_linearizable.rs`, `txn_serializable.rs`'s three *independent*
  node-id sets) or that already knew to use `start_hosted`
  (`cross_group_lww.rs`/`narrow_scope.rs`'s split fixtures) never hits
  this; a new self-contained corpus copying the wrong sibling function is
  an easy trap. Production code never has this bug (`animusd`'s own
  `cp_join_host`/`host::Reconciler` always calls `start_hosted` with
  `stream = tablet.0`) — this is purely a test-harness footgun, but a
  silent, hard-to-diagnose one (the symptom is "election never converges,"
  not an obvious "wrong stream" error). (`crates/animus-test/tests/
  stream_lineage_corpus.rs`, ADR 0042/0043 round-3 PR8, 2026-08-14.)
- **A tuple-keyed (or any non-string-keyed) `BTreeMap`/`HashMap` field on a
  type that derives plain `Serialize`/`Deserialize` and gets JSON-encoded
  fails only at runtime, and only once the map is actually non-empty**:
  `serde_json`'s `MapKeySerializer` rejects any non-string map key
  (`Error("key must be a string")`), but an *empty* map serializes fine (no
  keys to reject), and `cargo build`/`clippy`/`fmt` all see a perfectly
  ordinary `#[derive(Serialize, Deserialize)]` and say nothing. This let
  `animus_control::Metadata::stream_shards: BTreeMap<(TabletId, u64),
  StreamShardRow>` (ADR 0042/0043's segment catalog) ship, merge, and pass
  every gate untouched for an entire round of streams work, because every
  existing test that round-tripped a whole `Metadata` through JSON
  (`meta::tests::metadata_round_trips_with_the_remove_member_variant_in_
  scope` and its siblings) happened to leave `stream_shards` empty. The bug
  was live on `main` from the moment the field was added, waiting for the
  first real seal anywhere in a running cluster to blank `animusd`'s whole
  `GET /admin/status` (the handler swallowed the encode error into
  `Value::Null`) and panic the serving connection for any wire caller of
  `ClientResponse::Status`/`WatchMetadata`'s full-clone fallback
  (`write_frame` `.expect()`s the encode). The generalizable rule: a "does
  this type round-trip through JSON" test must populate *every* collection
  field it owns, not just exercise one representative command path; an
  empty collection cannot exercise a map-key encoding rule at all, no
  matter how many other fields the test fills in. The fix
  (`#[serde(with = "stream_shards_codec")]`, encoding the map as a flat
  `Vec<{tablet, epoch, ...row fields}>` instead) keeps the in-memory
  `BTreeMap` untouched, so every `.get`/`.insert`/`.range` call site in
  `animus-control` is unaffected. See `crates/animus-control/src/meta.rs`'s
  `stream_shards` field doc and `crates/animus-control/CLAUDE.md`'s own
  entry. (2026-08-14.)
- **An integration test that greps a served JS asset for an exact literal
  (`core_js.contains(r#"data: [...]"#)`) is asserting the string, not the
  behavior it happens to encode today** — it goes red the instant a later
  change legitimately edits that literal, with no way to tell "this broke
  the gating logic" apart from "this is exactly the intended new tab list"
  from the failure alone; the fix is always to update the literal to match
  the new intended list, never to treat the failure as a signal something
  is wrong. Hit adding a Streams tab to `dashboard_core.js`'s `ROLE_TABS`
  (ADR 0042/0043's Console follow-up): `dashboard_endpoint.rs`'s
  `dashboard_role_gating_split_deployment` asserted the data role's tab
  list was exactly `["node", "browser"]`, so appending `"streams"` to that
  array failed the test even though the new list was exactly the intended
  change. Not worth restructuring away (a substring match still proves the
  asset actually ships the gating logic, which is the property ADR 0021 §6
  wants — "the JSON/JS it renders is the tested JSON/JS," not a new
  correctness proof) — just keep it front-of-mind when grepping a
  gate-failure diff against a change that touches one of these string
  literals: check whether the assertion's own expected string needs
  updating before assuming the change broke something. (2026-08-14.)
- **"The tab is hidden on this role" and "the backend genuinely can't serve
  this role" are two different claims — verify both against a live
  cluster, don't assume the first implies the second.** Widening the
  Streams tab to control-only consoles (ADR 0021 #10) required finding out
  which of the four Streams-read ops (`ListStreams`/`DescribeStream`/
  `GetShardIterator`/`GetRecords`) a control-only node's admin proxy
  (`POST /admin/data/dynamo`) can actually serve, since nothing had ever
  exercised that combination (every existing `dynamo_streams.rs` "every
  node" test only ever brings up combined-mode clusters). Reading the code
  first suggested `ListStreams`/`DescribeStream` were metadata-only (safe)
  and the other two needed a local CP data plane (unsafe) — true, but
  "unsafe" turned out to mean two *different* failure shapes, not one: (1)
  `GetRecords` on a **sealed** shard calls `ClientCtx::data()`
  unconditionally, which **panics** — not a returned error, an
  empty/dropped HTTP reply (`curl`: "Empty reply from server"), confirmed
  live by hitting a real split cluster's control-node admin port and
  finding `thread 'tokio-rt-worker' panicked … ClientCtx::data called on a
  control-only node` in its stdout, even though `ClientCtx::data()`'s own
  doc comment already says this exact call path must never happen; (2) the
  **open**-shard path (`GetShardIterator{LATEST}`/`GetRecords`) doesn't
  panic but silently stalls for the full `SCHEMA_COMMIT_TIMEOUT` (~10s)
  before failing, because a control-only node's `resolve_cp_route` has no
  local replica to derive a real leader hint from, so its blind-forward
  fallback picks the same (possibly wrong) replica every retry with nothing
  chasing the refusal's own hint — confirmed by timing the request (`time
  curl` showed `10.03s`) rather than assuming a quick failure. Getting a
  genuine sealed shard to test against needed no small `--stream-seal-
  bytes` tuning either: `UpdateTable{StreamEnabled:false}`'s F12-b
  disable-triggered final seal produces one unconditionally, and it's also
  the only seal path any of the deployment CLI's split-cluster modes
  (`--cluster-control`/`control --config`/`data --config`) actually expose
  today — none of them thread the `_streams`-suffixed seal-knob overrides
  the combined-mode `--cluster N`/`--config --node` paths do (a real,
  separate documented CLI gap, `animusd/CLAUDE.md`'s own "split-deployment
  CLI path is a named follow-up" note). Both findings changed the fix: the
  dashboard doesn't try to distinguish "will this shard's `GetRecords` call
  panic or hang," it just never dials either op from a control-only
  console at all, degrading the whole live-tail section with one static
  note — a narrower, correctly-scoped fix than trying to special-case one
  of the two failure shapes and being surprised by the other in production.
  The backend panic/timeout themselves were **not** fixed in this PR (a
  UI-scoped change; see "separate PRs for incidental bugs") but are now
  documented in `animusd/CLAUDE.md` and ADR 0021 #10 for whoever picks that
  up. (2026-08-15.)
- **A commit message's claim that a test helper is "now dead" must be
  verified by grepping the helper's name across its whole file, not
  inferred from "the one test that motivated the deletion is gone."**
  148b3ac (ADR 0044, removing tablet merge) deleted `streams_e2e.rs`'s
  local `admin()` helper alongside the one merge-stopgap test that called
  it, reasoning it was now unused — but three *other* tests already in
  that file (`admin_status_survives_a_populated_stream_shard_catalog`,
  `admin_data_dynamo_proxy_reaches_streams_read_api`,
  `admin_data_dynamo_proxy_rejects_unknown_op_cleanly`) also called it,
  so the deletion broke `cargo build --workspace --all-targets`/`cargo
  test -p animusd` outright — silently, since this repo's CI billing is
  broken (see the `docs/engineering-lessons-archive.md`/root `CLAUDE.md`
  history) and no automated gate ever ran it after merge. Sat unnoticed on
  `main` until the next agent to touch `animusd` hit it cold. Found and
  fixed while building ADR 0045 PR1 (index-status catalog plumbing,
  unrelated) — restored the helper verbatim from before the deletion
  commit. General form: when deleting a symbol because "its only caller is
  going away," grep the symbol's own name (not just its caller) across the
  file/crate before deleting it, exactly the same discipline this log
  already prescribes for gating `match` sites on a command enum.
  (2026-08-15.)
- **An internal design doc's paraphrase of a real external API's response
  shape is not the API — verify against the real shape before wiring a
  field, even when the doc sounds precise.** ADR 0045 §6 sketched
  `DescribeTable`'s new `Backfilling: bool` as a **table-level** flag ("any
  index `Creating`"); real DynamoDB places `Backfilling` **inside each
  `GlobalSecondaryIndexes[]` entry**, and only while that specific index is
  backfilling (the attribute is *absent*, never `false`, once finished).
  Building PR6 to the doc's wording as written would have shipped a
  plausible-looking but wrong wire shape no test would have caught, since
  every test in the same PR would have been written against the same wrong
  premise. Caught only because the task brief explicitly flagged the
  wording as "looser than AWS reality" and asked for the real shape to be
  checked — worth generalizing: **a design doc is a plan, not a spec of an
  external contract it merely describes; re-derive the actual third-party
  shape independently (from real API docs/behavior) rather than trusting a
  plan's summary of it**, the same way this codebase already insists on
  reading ADR text as *rationale*, not as the mechanism's ground truth.
  (`animus-dynamo/src/wire.rs`'s `index_desc`/`table_description_object`,
  2026-08-15.)
- **A commit-wait poll for a command that puts an object into a *transient*
  status must check the object's presence, not that it still holds the
  exact status value just proposed** — a concurrent convergent loop can
  legitimately advance past that status before the proposer's own next
  poll, especially on a small/fast-converging fixture in a test. `UpdateTable`
  Create (ADR 0045 §6) proposes `CreateTableIndex{status: Creating}` and
  waits for it to commit exactly like `create_table`'s own index-definition
  loop (presence-by-name only, `dynamo.rs::create_table`); the completion
  aggregator (`index_backfill_loop`, ADR 0045 §4) can flip that same index
  to `Active` within one tick of a tiny table's backfill finishing, which on
  a single-node test can race the proposer's very next `metadata_fresh`
  read. Polling for `status == Creating` specifically would then spuriously
  time out despite the create having fully succeeded. The already-shipped
  `set_index_status` (used by the drop cascade's `Deleting` transition)
  gets away with checking the exact target status only because nothing in
  this codebase ever proposes a *further* transition away from `Deleting`
  before `DropTableIndex` removes the definition outright — that is a
  narrower invariant than "commit-wait polls are safe to pin to an exact
  status," not a counterexample to this lesson. General rule: when a
  commit-wait's target value can itself be mutated again by some other
  loop before the waiter's next poll, wait on the mutation that is
  monotonic/permanent (existence, a monotonic counter, a specific id) —
  never on a value a *different* proposer can race past.
  (`animusd/src/dynamo.rs::create_index`, 2026-08-15.)
- **Heisenbug instrumentation: even lock-free/buffered hot-path logging can
  suppress the very race it's meant to observe — verify the failure rate is
  unchanged before trusting the captured timeline.** Investigating the
  torn-pair-fix stack's root cause (a `TransactGetItems` snapshot going
  torn under a tight, back-to-back writer), the first instinct was to add
  logging directly to the per-key read path (`ClientCtx::cp_get_local_
  resolving`/the new `cp_get_local_snapshot`) to capture exactly what each
  key observed at the moment of failure. Even a buffered `tracing`/
  `eprintln!` call on that hot path measurably changed the race's timing
  enough to suppress it in some runs — the fix was to instrument the
  *lowest-frequency* call site that still gives the needed signal (here,
  the point where a status query resolves to a decided outcome, not every
  single fast-path read attempt), and — the load-bearing check — to
  explicitly re-run the *un-instrumented* failure rate immediately after
  adding logging and confirm it's still statistically the same before
  trusting anything the captured timeline claims. A logging change that
  quietly halves a reproduction's failure rate is itself evidence the
  logging is perturbing the very thing under investigation, not
  confirmation the bug got rarer. (Torn-pair-fix stack, PR2, 2026-08-15.)
- **Multi-log-source clock anchoring: when correlating several log streams
  from one process for a timing bug, log one shared absolute-clock anchor
  event across all of them up front, before the race window opens.**
  Debugging the same torn-pair investigation meant correlating the
  writer's own step-by-step timeline against the reader's per-round
  observations against the coordinator/recovery internals' own traces —
  three log sources from the same test process, but on different clock
  bases (some `std::time::Instant`-relative, some `SimEnv`/`HlcTimestamp`
  wall-clock-derived). Without a single shared absolute-time anchor emitted
  by all three at the very start, aligning "what did the reader see at the
  exact moment the writer's step N committed" after the fact was
  genuinely ambiguous — a real gap in the evidence that better
  instrumentation design would have closed for free. Emit one anchor event
  (the same wall-clock read, or the same monotonic counter value) from
  every log source before anything interesting happens, not just whichever
  timestamp format was most convenient at each individual call site. (Same
  investigation, 2026-08-15.)
- **A value a child inherits from a parent that keeps mutating must be
  frozen at the inheritance event, never derived live from the parent's
  current state.** (Mechanism — `Metadata::stream_split_basis`, the zero-copy
  split's watermark inheritance — deleted in ADR 0050 Train B rung 7:
  copy-based split children are born with empty change logs, so no consumer
  offset crosses a split at all, the strictly stronger successor invariant.
  Full entry archived verbatim in `docs/engineering-lessons-archive.md`.)
- **A test-drain helper polling both a "closed epoch, replay from
  `TRIM_HORIZON`" path and a "current open tail, resume from last
  position" path must resume the SAME iterator when an epoch transitions
  from open to closed mid-poll, never re-mint a fresh `TRIM_HORIZON` walk
  for it.** `streams_e2e.rs`'s `drain_tablet_lineage`/
  `drain_all_tablets_lineage` always re-minted `TRIM_HORIZON` for a
  newly-closed epoch, discarding whatever position the open-tail poll had
  already reached in it one pass earlier — double-delivering any record
  the open-tail poll had already returned before that epoch sealed. This
  was invisible under `tiny_seal_knobs()` (`seal_bytes: 1`), whose open
  tail is always empty the instant it's polled (every write seals as its
  own epoch immediately), so no existing test before PR1's
  production-shaped-knobs regression cell ever left more than one record
  in an open tail across two poll passes. General form: a resumable
  iterator's identity survives a state transition (here: open → closed);
  a caller that mints a fresh one anyway on the transition, instead of
  continuing the one it already has, double-reads whatever the old one
  had already delivered.
- **Found but deliberately not fixed here (report, don't scope-creep):
  `streams_e2e.rs::auto_split_mid_stream_with_live_consumer_across_every_
  node` (D8) is flaky on `main` independent of any change in this
  PR** — confirmed by running the *unmodified* file against the same
  workload: an intermittent `exactly-once delivery` over-count (delivered
  > expected by a handful of records) under `tiny_seal_knobs()`'s
  size-1-triggered rapid resealing. A related, likely-connected symptom
  found independently while building PR1's own e2e cell: under
  *non-tiny* seal-byte knobs with a real write burst crossing the
  threshold many times in quick succession, a handful of records can go
  missing from *every* segment and the open tail alike (base row present
  via `GetItem`, change record nowhere) — reproducible with no split
  involved at all, so it isn't the split-basis bug this PR fixes. Both
  point at a timing sensitivity in `change_consumer_loop`'s seal arm
  (`animusd::index_drain`) under many-seals-in-quick-succession, not
  investigated further here; PR1's own new e2e cell sidesteps it entirely
  by using the **age** seal trigger (`seal_bytes` set high enough to never
  fire) instead of the byte one, so each side seals exactly once. Worth a
  dedicated investigation as its own PR.
- **D8's over-count flake (above) turned out to be three layered bugs, not
  one — fixing the first two exposed the third instead of clearing it, and
  only per-failure symptom classification (not raw pass/fail rate) told
  them apart.** Follow-up to the two entries above. (1) The original
  `drain_tablet_lineage`/`drain_all_tablets_lineage` open-tail-resume bug
  (fixed above): re-minting `TRIM_HORIZON` on an open→closed transition
  discovered via a *later* `chain_len` read. (2) A second, narrower
  interleaving of the *same* race the first fix didn't cover: when the
  open-tail poll's own `GetRecords` call is the one that witnesses the
  seal (`dynamo_streams.rs::get_records` re-checks live `Metadata` fresh
  on every call, so an outstanding iterator can flip to the sealed path
  and return that epoch's final records with a null `NextShardIterator`
  in the *same* response, rather than being discovered closed on a later
  pass), the helpers left `open_epoch`/`open_iterator` pointed at the
  now-exhausted iterator instead of advancing past it — the next pass's
  "resume" branch then replayed the same spent iterator and re-delivered
  records it had already collected. Only exercised at a real rate by D8's
  three-tablet cascading split under sustained write pressure; a single
  controlled split (the PR1 regression cell) leaves little chance of a
  poll racing the seal this precisely. Fixed in both helpers
  (`crates/animusd/tests/streams_e2e.rs`) by advancing the epoch cursor
  and clearing the stale iterator state the instant a poll's own `None`
  response reveals the seal, rather than waiting for a subsequent
  `chain_len` read to notice. (3) Even with both harness bugs fixed, a
  40-iteration baseline vs. 60-iteration post-fix comparison on the same
  machine showed the failure rate barely moved (52.5% → 45%, both
  over-count-dominated) — the fix was real but far from sufficient, the
  exact "no improvement" trap the entries above warn against trusting
  blindly. A diagnostic added to the test's own failure path (grouping
  `delivered` by `eventID` and by base item id — the discrimination the
  first entry above already named but never built) pinned the remainder
  immediately: every captured failure showed the *same* item under **two
  distinct `eventID`s** sharing identical trailing packed-HLC digits but
  different `shardId-<tablet>-<epoch>` prefixes — e.g. `e0011` sealed into
  both `shardId-1-3-...` (the parent's own epoch) and `shardId-2-0-...`
  (the freshly-split child's epoch 0). Zero repeated `eventID`s across
  every failure, ruling out a further harness double-read. This is a
  **genuine production cross-tablet duplication at the split boundary** —
  the same write's change-log record independently sealed by both tablets
  — categorically different from (1)/(2) and out of a test-only PR's
  scope; left open, tracked via the diagnostic (now permanent in the
  test's failure output) and the test function's own doc comment rather
  than re-investigated blind on the next red run. Working hypothesis, not
  yet confirmed against the code: PR #216's `stream_split_basis` freeze
  fixes what a *reader* sees (the watermark/`ParentShardId` derivation,
  preventing loss) but likely doesn't constrain what the **child's own
  change-log drain** is allowed to re-discover and re-seal from the
  shared underlying engine post-split — a `KIND_CHANGE` row the parent
  already sealed just before splitting can still be physically present
  (not yet trimmed) and hash-fall into the child's newly-owned range,
  and the child's drain (which restarts its own sweep from scratch,
  the same convention documented for the GSI backfill seeder) has no
  watermark excluding rows the parent already consumed. **General form,
  extending the lesson above**: when a fix closes a known bug and the
  failure rate doesn't correspondingly collapse, the honest reaction is
  another round of symptom-level diagnosis, not a bigger sample size or a
  shrug — a stubborn residual rate is itself evidence of a *different*
  bug hiding behind the fixed one, and the same "group by identity, not
  just by symptom" technique that separated harness-vs-production once
  works again one layer deeper. (2026-08-15/2026-08-15, D8 test-harness PR,
  base `1fd1a326`.)
- **Adjudicating a race in consensus does not order the physical writes
  below it — a shared mutable storage key underneath an agreed decision is
  its own, independent hazard, and the fix is to remove the sharing, not to
  make the adjudication "more correct."** Root-cause of the "handful of
  records missing from every segment and the open tail alike" symptom the
  entry above reported but didn't chase down: two independently-computed
  DynamoDB Streams seal attempts for the tablet's SAME open epoch (the
  realistic trigger is a brief dual-leadership window during a
  write-burst-induced re-election, not merely a same-leader crash-retry)
  both derived the identical deterministic `SegmentStore` id
  (`{table}/{label}/{tablet}/{epoch}`) and raced their physical `put`s
  there. `Metadata::apply`'s `SealStreamShard` arm correctly picked one
  winner (first-committer-wins on content) — the **consensus layer was
  never wrong**. But the segment store underneath it had no matching
  adjudication: `SegmentStore::put` was "idempotent overwrite,
  last-write-wins" by contract, so whichever attempt's `put` physically
  landed *last* won the bytes, with **no relationship** to which attempt's
  *proposal* won the catalog. The design's own safety argument for a
  retry ("a harmless superset, because a reader always slices to the
  catalog row's own committed range") tacitly assumed the later-landing
  `put` is always the *larger* one — true for a sequential same-leader
  retry (a strictly later scan, by HLC monotonicity), but false the moment
  two *independent* attempts race, and when the later `put` happened to be
  the *smaller* one, the gap between it and the catalog's own committed
  range was silently, permanently lost — a real data-loss bug that shipped
  and stayed undetected because the frozen fault-injection corpus's own
  harness read-and-applied against one shared `&mut Metadata` reference
  every call, structurally unable to express "two attempts from two
  different snapshots." A hand-scripted repro (not a seed sweep) is what
  actually found it. **The fix is structural, not a smarter tie-break**:
  make every attempt write at its own unique id
  (`animus_cp_data::segment::segment_object_id` — a deterministic prefix
  plus a suffix derived from the proposer's node id, its current Raft
  term, and a fresh `Rng` draw, so two attempts anywhere, ever, cannot
  collide) and make the store itself **write-once** (an identical-content
  re-put is a safe no-op; a *differing*-content re-put is a hard error).
  With no shared mutable key, there is nothing left for a "better"
  adjudication rule to get right — the losing attempt's object is simply a
  permanent, harmless orphan, reaped by a dedicated sweep
  (`animusd::segment_janitor::reap_orphans`) rather than something a later
  `put` ever revisits. **General form, worth checking on any future
  "consensus decides X" design**: whenever a committed decision names a
  key into some OTHER store (a filesystem path, an object-store key, a
  cache entry) that isn't itself inside the consensus log, ask whether two
  *independently-computed* decisions for the same logical identity could
  ever name the *same* key in that other store — if so, the store's own
  overwrite semantics are a second, unguarded decision point consensus
  never actually adjudicated. Fixed in `fix/segment-seal-write-once-ledger`
  (ADR 0042/0043 as-built amendments); red-then-green repro:
  `animus-test`'s `stream_lineage_corpus.rs::
  dueling_seals_orphan_hot_range`.
- **A corpus convention that models a distributed transition as atomic can
  only ever test the FIXED ordering — it structurally cannot express the
  real race where a local, un-replicated effect lags the replicated
  commit; and a cache-fed fence cannot protect against the cache itself
  being stale — only the state machine's own `apply` arbitrates.**
  (Mechanisms — the zero-copy split's `narrow_scope` lag window,
  `in_declared_range`, and the `SealStreamShard` `expected_range` CAS —
  deleted in ADR 0050 Train B rung 7: ranges are immutable and a split
  retires its parent whole, so the transition window itself no longer
  exists. Both full entries archived verbatim in
  `docs/engineering-lessons-archive.md`; the apply-arbitrates half lives
  on in `Freeze`'s own apply-time backstop.)
- **A lineage-walk drain helper that captures its tablet set once, before
  the drain starts, cannot see a split that lands DURING the drain — and a
  "cascading" (third-generation) split is exactly the case a single
  controlled split in a smaller test never exercises.** A further,
  purely-harness layer of D8's own flake (distinct from every bug above,
  which were all real production duplication/loss at the split boundary):
  `streams_e2e.rs`'s `drain_all_tablets_lineage` took its `tablets: &[
  TabletId]` argument as a fixed set for the whole drain. Under D8's
  sustained write pressure and tiny byte-auto-split threshold, a child
  tablet minted by the test's own first split could itself split again
  *while the drain was already mid-walk* — a real, correct auto-split, not
  a bug — minting a grandchild tablet id nobody had ever handed the drain
  helper. That grandchild's own change records were simply never read,
  producing a spurious **deficit** (`delivered < expected`, distinct in
  *direction* from the over-count bugs above) at a low but real rate
  (~1/20 iterations) that had been sitting in the test's own doc comment as
  an "adjudicate against, don't re-investigate" known limitation rather
  than fixed — unlike the production bugs in the entries above, this one
  needed no `src/` change at all, purely a harness fix. **Fixed**: the
  helper now re-resolves the *live* shard chain every pass via a fresh
  `DescribeStream`
  call (`stream_tablet_ids`, paginating `ExclusiveStartShardId`/
  `LastEvaluatedShardId` to a full page) — the same way any real DynamoDB
  Streams consumer discovers a new shard exists, never by peeking at
  `Metadata`'s tablet map directly for tablet *existence* (that stays
  reserved for a tracked tablet's own chain-*length* read, which has no
  wire equivalent this cheap). A newly discovered tablet id is folded in
  with a fresh `next_epoch = 0`, never touching an already-tracked tablet's
  in-flight open-tail iterator — preserving the resume-not-remint invariant
  the entries above fixed. **General form**: any test helper that walks a
  linearly-growing structure (a lineage, a partition set, a shard list) by
  snapshotting its membership once up front is implicitly assuming the
  structure is quiescent by the time the walk starts — under real
  background pressure (an auto-split loop, a compaction, a rebalance) that
  assumption can be false for the exact scenario the test exists to stress,
  and the fix is to make the walk's own membership-discovery step live
  (re-run every pass) rather than a one-time precondition. (2026-08-16,
  `test/streams-e2e-walk-cascading-split`.)
- **A hand-driven `MetaCommand::ProposeSchema` test fixture must target the
  INTRA port, not the client port, since ADR 0047's listener cutover** — a
  fresh test file (`f11_split_alignment.rs`) sent `ClientRequest::
  ProposeSchema(CreateTableSchema{..})` to a node's `client_addr()` (the
  shape every pre-0047 test used, and still the right address for
  `Put`/`Get`/`SplitTablet`, which stayed `Surface::Public`) and got back
  `Error("propose_schema is a cluster-internal request; send it to this
  node's intra port")` — `handle_request` refuses any `Surface::Intra`
  request (`ProposeSchema` among them, `surface_of`'s own match arm) on the
  client listener outright. `index_backfill.rs` already carries the fix
  as a one-line comment ("ADR 0047: `ProposeSchema` is intra-only") right
  above its own `intra_addr()`-targeted calls, but nothing greppable ties
  that convention to the wire type itself, so a fresh test file rediscovers
  it the hard way. **General rule**: when hand-driving a `ClientRequest`
  variant in a new test, check `surface_of`'s match arms (`lib.rs`) for
  which listener it's actually gated to — `Surface::Public` variants
  (`Put`/`Get`/`Scan`/`Delete`/`Txn`/`SplitTablet`/`Status`) work on
  `client_addr()`; every `Surface::Intra` variant (`ProposeSchema`,
  `Forwarded`, `KindWrite`/`KindScan`, the `Txn*` internal RPCs, etc.) needs
  `intra_addr()` instead — the error message names the fix, but only once
  you've already hit it. (2026-08-16, `growth/1-f11-fence`.)
- **`SimEnv`'s `Sleep` future logs a `TraceEvent::Timer` at its deadline
  unconditionally — even for a `select` branch that lost the race and was
  dropped long before that deadline arrives.** `Sleep::poll`'s *first* call
  inserts a `(deadline, Event::Timer(id))` entry into the global timeline;
  nothing removes that entry if the future is later dropped (no `Drop` impl,
  and `select` drops the losing branch outright), so the scheduler's normal
  timeline sweep fires it anyway at the original deadline, unconditionally
  pushing the trace line and popping (now-absent) `timer_wakers` — a
  functional no-op, but real trace noise. Consequence: raw
  `TraceEvent::Timer` counts over a window are **not** a clean proxy for
  "how many times did this task actually wait out its poll interval" the
  moment any of its sleeps race against something else (a message, a signal)
  that can resolve first — every such raced-and-abandoned sleep still
  contributes one *eventual* Timer line at its stale deadline, indistinguishable
  in the trace from a sleep that genuinely ran to completion. A clean
  wakeup-count assertion is only cheap for a **provably idle** window (no
  messages, no signals, nothing else racing the sleep) — anywhere traffic is
  interleaved, don't reach for a bare `TraceEvent::Timer` tally; either prove
  the window is idle first or instrument the call site directly (a counter
  bumped only where the sleep is entered). Found while evaluating a
  wakeup-count regression test for the ADR 0044 phase-1 apply-signal fix
  (`quiesce/1-apply-signal`, 2026-08-16) — skipped in favor of the three
  bounded-convergence tests in `tests/apply_signal.rs`, which don't depend on
  this distinction.
- **`Simulator::run_until_quiescent` cannot prove a specific subsystem's
  timerlessness once *any other* task in the same run keeps its own
  independent, deliberately-never-eliminated safety-poll timer alive.** Found
  while writing the ADR 0044 phase-1 PR3 quiescence corpus
  (`quiesce/3-core-state-machine`, 2026-08-16): the plan asked for
  `run_until_quiescent(max_steps) == true` as proof that an idle, quiesced
  `RaftKvNode` group posts zero `SimEnv` timeline events. That's unreachable
  by construction — the apply task's own idle back-off (PR1) races
  `ApplyPending` against a 250ms `APPLY_SAFETY_POLL` **forever**, regardless
  of Raft activity, a deliberate design (a missed/lost `ApplySignal` must
  still converge, not stall). One node's apply task alone keeps a scheduled
  timer event alive at all times, so `run_until_quiescent` can never observe
  a truly empty timeline for a *live* group — quiesced or not. This isn't a
  defect in the quiescence work; it's a different subsystem's already-shipped
  trade-off surfacing at a test assertion that assumed no other timer existed
  anywhere in the run. **General rule**: before reaching for
  `run_until_quiescent` (or any "the whole sim went idle" assertion) to prove
  *one* mechanism's timerlessness, check whether anything else in the same
  process — a different task, a different subsystem's own safety poll — has
  an independent timer that would prevent it from ever firing, even if the
  mechanism under test is working perfectly. When it does, assert the
  mechanism's own state directly instead (here, `RaftCore::next_deadline() ==
  None` on every replica) rather than inferring it from a whole-sim
  observation that a co-located concern can foil. See `tests/quiescence.rs`'s
  module doc for the full reasoning kept where the next person will read it.
- **A single-node test that manufactures a live tablet split *mid-write-burst*
  needs its own client-side retry, matching the discipline a real
  multi-node routed client already gets for free.** Found writing the ADR
  0044 phase-1 PR6 sweeper-skip regression
  (`a_rewoken_tablet_is_picked_back_up_by_every_sweeper_within_one_interval`):
  a 40-write burst against a tiny `--auto-split-bytes` threshold legitimately
  splits partway through, and a later write in the same burst can target a
  key the split just handed to a fresh child tablet — surfacing as a
  perfectly correct `"kind write outside this group's live range; retry"`
  rejection (the ADR 0028 write fence doing its job). A real deployment
  never notices this: `ClientCtx::cp_route`/`cp_forward`'s hinted-retry
  re-resolves the tablet on every attempt. A single-node test driving the
  wire API directly against one fixed address has no such re-resolution
  layer, so its own write helper must retry on this specific, already-
  retryable-shaped error (`"; retry"`) rather than hard-asserting `200` —
  never by loosening the shared helper every *other* test also uses (that
  would mask a genuine regression elsewhere), but with a locally-scoped
  retry loop in the one test that deliberately invites the race. The same
  class as the pre-existing, tracked "CreateTable first-write race" (`200`
  from `CreateTable` doesn't mean the tablet is ready for a concurrent first
  write) — any test that deliberately drives a live topology change under
  concurrent writes needs this discipline, not just the specific two cases
  found so far.

- **Changing a bulk write path's Raft-entry granularity is a throughput
  contract change, and the timing-budgeted e2e suites are its regression
  canary — bisect a "suddenly slow" suite before blaming the machine**
  (2026-08-16, found delivering ADR 0049 Train A rung 2). Rung 1 replaced
  `BatchWriteItem`'s one-`Batch`-entry-per-tablet fast path with per-item
  `KindBatch` proposals (chunked, concurrent). Its own gates ran green, but
  `backfill_seeder.rs::split_during_backfill_converges_with_correct_final_
  gsi` — a populate-heavy test with a 60s convergence budget — went
  deterministically red at the rung's tip (4/4, even single-threaded) while
  the immediate pre-rung commit passed in 17s: an order-of-magnitude
  convergence regression that a "flaky on this box" shrug would have
  shipped. Two lessons: (1) an entry-granularity change (N single-key
  entries where one multi-key entry used to be) multiplies per-entry apply/
  confirm costs and must be treated as perf-sensitive — re-run the suites
  whose comments document timing budgets several times before shipping;
  (2) the 30-second bisect (run the suite once on the parent commit) is
  what turns "pre-existing flake, dismissed" into "my rung's regression,
  fixed" — never classify a red integration test as environmental without
  that one run, exactly because this machine also has a *genuine*
  environmental flake class (`AddrInUse` bring-up TOCTOU) to hide behind.

- **A helper promoted from a background path to a client hot path carries
  its background-tuned cadence with it — bench the promoted path itself,
  never assume the promotion is free** (2026-08-16, ADR 0049 Train A rung
  5's bench gate). `cp_kind_raw_local` was built for the GSI drain's
  footprint/cursor writes, where its flat 10ms confirm-poll sleep was
  irrelevant; ADR 0049 quietly made it the confirm for *every* plain
  Dynamo/CQL/raw-protocol write, so nearly every sequential client write
  ate one whole 10ms tick — a 2.9× sequential-latency regression
  (13.6 ms/op vs 4.7 pre-train on the ADR's own bench) that every
  correctness gate ran green through, caught only because the ADR had
  demanded a before/after measurement as a shipping gate. The plain path's
  own confirm had used a 200µs-start exponential back-off all along; the
  fix was giving the promoted helper the same one. Corollary: when an ADR
  names an expected perf envelope ("low single-digit percent"), build the
  measurement into the train as a gate — this one paid for itself on its
  first run.
- **A test that uses a production feature as mere INFRASTRUCTURE (not as
  the thing under test) breaks the moment that feature is gated — audit
  which is which before parking anything.** When ADR 0050's storage pivot
  disabled the zero-copy split surface, every `cp_txn`/`dynamo_txn`/
  recovery test went red at once — not because their subject (2PC across
  Raft groups) broke, but because their shared harness split an EMPTY
  table purely to get a two-group topology. The fix was not parking the
  binaries (which would have thrown away all transaction coverage) but
  re-sourcing the topology at the layer that owns it: propose the
  metadata command directly (`Node::propose_meta`), sound precisely
  because the table is still empty (nothing to inherit). Only tests whose
  SUBJECT was the disabled mechanism (split-of-populated-tablet e2e) got
  parked. General form: for each red test under a feature gate, ask "does
  this test *test* the feature, or merely *use* it to build a fixture?" —
  the second kind wants a fixture rewrite, never an `#[ignore]`.
- **A filter enforced on N serve paths needs a regression per path × per
  record shape, even when every path calls one shared predicate — the
  shared function proves the paths *agree*, not that any given path is
  ever actually reached with the shape in hand** (2026-08-17, issue #267).
  The consumer-hidden stream-record filter (`ChangeRecord::
  consumer_hidden`, both `GetRecords` serve branches) had its
  streamed-mid-backfill `seeded` regression pinned only on the open-tail
  path: the test's own seal knobs were deliberately tuned so nothing ever
  sealed, which made the sealed segment-decode branch
  (`get_records_sealed`) — the very path the sealer's
  "seal seed markers into segments by design" decision routes those
  records through over time — structurally unreachable from the test. The
  `marker` shape meanwhile had sealed-path coverage and the `seeded` shape
  none: two shapes × two paths, only half the cells covered, invisible
  unless you draw the matrix. When a test tunes knobs so one serve path
  never engages, that tuning is the test's own blind spot — add the dual
  with the knobs inverted (`animusd`
  `tests/stream_backfill_seed_filter.rs`, the paired open/sealed tests).
- **When a filed race note "does not reproduce," the deliverable is a test
  pinning the specific guard arm that closes it — found by asking "which
  exact check makes each interleaving in the note impossible?", then
  red-proving that check** (2026-08-17, issue #266). A planning-time note
  flagged a residual cross-node LSI orphan-row race beyond the ADR 0046 U3
  funnel; re-verifying against the landed `TxnStage` stack showed three
  guards jointly close it — `KindBatch.conditions`' unresolved-intent arm
  (an intent is never a match), `TxnStage`'s C1 mandatory own-key OCC +
  foreign-intent block, and `TxnResolve` materializing kind writes from
  the intent-on-the-key only while that intent still stands. But the
  first of those arms — the one the note's own scenario turns on — had
  ZERO coverage (`kind_batch_conditions.rs` only ever exercised
  committed-value/absence conditions), so "closed" rested on code
  reading alone. The pinning test was cheap (stage an intent, land a
  stale conditioned batch astride it, resolve, assert exactly one
  derived row), and sabotaging the arm (`Intent => true`) proved it red.
  General form: "verified closed" without a red-provable test for the
  closing check is a claim about today's tree, not a regression
  guarantee — and the *arm-level* gap hides easily because the
  surrounding mechanism is otherwise well-tested.
  (`animus-cp-data/tests/txn_kind_writes.rs`,
  `animusd/tests/dynamo_index_writes.rs`.)
- **A shared `CARGO_TARGET_DIR` can serve a genuinely STALE test binary
  while `cargo test` reports `Finished ... in 0.1s` and no error** — caught
  chasing issue #278's streams_e2e flake fix: a loop of concurrent
  `cargo test` invocations (my own diagnostic script running several copies
  in parallel against the identical worktree + target dir, on top of other
  agents' own concurrent builds) produced panics whose message text
  (`"assertion \`left == right\` failed: ..."`) matched code I had already
  *replaced* with a retry loop — the binary being executed hadn't picked up
  the edit, even though the very same target dir's `cargo build` had
  compiled it correctly moments earlier. Root cause not fully isolated (a
  fingerprint/mtime race between concurrent `cargo` processes hammering one
  target dir is the leading theory), but the fix that made the signal
  trustworthy again was mechanical: `touch` the changed source file, run a
  single `cargo build` alone (not concurrently with any other `cargo`
  invocation against the same target dir), confirm via `strings
  target/debug/deps/<bin>-<hash> | grep <a string unique to the new code>`
  that the *specific* binary hash about to run actually contains the edit,
  and only then start a validation loop — and keep that loop to ONE
  `cargo test` process at a time (sequential iterations), never several
  invocations of the same test binary launched concurrently against a
  shared target dir. **General rule**: on a shared-`CARGO_TARGET_DIR` box,
  don't trust a failing (or passing) test loop's signal until you've
  positively confirmed the binary under test contains your latest edit —
  a "Finished" message is not proof of that, and a panic whose message
  text doesn't match anything in the current source is a tell that you're
  looking at a stale binary, not a new bug. (`animusd/tests/streams_e2e.rs`,
  issue #278.)
- **A DynamoDB wire edge can map the identical retryable server-side
  condition to DIFFERENT HTTP status codes depending on which API it's
  surfaced through** — `TransactWriteItems` maps a cancelled transaction to
  **400** `TransactionCanceledException` (matching real AWS DynamoDB's own
  convention), while `PutItem`/`Query`/the Streams read API map the same
  underlying ADR 0050 F8 freeze→cutover blip to a plain **500**
  `InternalServerError`. A test retry helper written against one shape
  (retry-on-500) silently fails to mask the other — the transact call kept
  one-shot-failing at ~1/15 even after every *other* one-shot assert in the
  same test was fixed, until the retry predicate was taught to also accept
  a 400 whose body is a `TransactionCanceledException` **and** whose own
  message says "retry" (never a bare 400, which can also mean a genuine
  condition-check failure that must still fail loudly). General form: don't
  assume one status code covers "this operation's own documented retryable
  window" across every API on the same wire edge — check what the specific
  handler actually returns for the specific condition before writing (or
  reviewing) a retry-on-status-code test helper.
  (`animusd/tests/streams_e2e.rs::dynamo_retrying_transact`, issue #278.)

- **A per-test `Node::shutdown()` teardown (plain, non-draining abort) racing
  a still-in-flight driver I/O op surfaces as a noisy `tokio-rt-worker` panic
  that has nothing to do with the test's own assertions — `Node::shutdown_
  graceful` (or `shutdown_and_wait`) exists precisely to close this window
  and should be the default choice for ordinary end-of-test cleanup; plain
  `shutdown()` is for a *deliberate* fault (a documented "kill node N",
  "crash the leader", or "the process goes away without decommissioning"
  scenario) where the abrupt, non-cooperative abort is the point. Sweeping a
  test tree for this: a `for node in &nodes { node.shutdown(); }` (or single
  final call) at the very end of a test body, with nothing observing that
  node afterward, is teardown — swap it. A `nodes[kill_idx].shutdown()` (or
  similarly-named) mid-test, with a comment about killing/crashing a node and
  the test continuing to assert against the *survivors*, is a deliberate
  fault injection — leave it. A `stop()`/`restart_same_addrs`-style helper
  used before rebinding the same addresses needs the graceful form
  regardless (see this file's own "long-poll request in flight at kill"
  entry and `animusd/CLAUDE.md`'s `Node::shutdown()` gotcha for why a bare
  `shutdown()` doesn't reliably free ports either). issue #278 item 1
  (`crates/animus-cp-data/src/lib.rs::persist_wal`,
  `animusd/tests/backfill_seeder.rs` and the crate's whole `tests/` tree).
- **A driver loop's own hard-`.expect()` on a durability I/O op should be
  gated by the same halted/shutdown latch its sibling error-handling site
  already uses, not panic unconditionally** — mirror the existing idiom
  (`animus-cp-data`'s apply-task compaction path already tolerated
  `env.replace` failing *only while `halted`*) rather than inventing a new
  shape. The two failure classes need to stay distinguishable: while running,
  the identical I/O error is a genuine durability fault and must stay a loud
  panic (crash-stop-before-ack); while halted, the same error is an artifact
  of the teardown itself (an aborted task's blocking-pool op surfacing
  `"background task failed"`, or a test's `TempDir` deleting the file out
  from under a still-draining loop) and should be tolerated — return early
  with **no** durability bookkeeping advanced (never claim a write is durable
  that never landed) and let the caller's own halted-check exit the loop on
  its next pass. **This is deterministically regression-testable under
  `SimEnv`** despite looking like a real-thread race: `animus-sim`'s
  `DiskConfig::set_error_prob(1.0)` (via `Simulator::set_disk_config_for`)
  forces every subsequent disk op on one node to fail, and since `SimEnv`
  only polls a node's driver task inside `run_for`/`run_until`, two
  *synchronous* calls back-to-back from the test body — mint a pending write,
  then latch `halted` — are guaranteed to land before the driver is next
  polled, so its next `persist_wal`-shaped pass finds the fault and the
  latch together, deterministically, no thread races or timing sleeps
  needed. Proof the test has teeth: temporarily reverting the fix reproduces
  the exact pre-fix panic message. (`animus-cp-data/tests/shutdown.rs::
  a_halted_nodes_pending_write_tolerates_a_wal_fault_with_no_panic`.)
- **Not every sibling site sharing a "halted-window" shape is deterministically
  regression-testable the same way — check whether the racing work source
  bypasses the driver loop's own gate before assuming a test can be ported.**
  Extending the persist_wal halted-gate to the apply task's `flush_pending`
  (`merge_batch`, issue #278 item 1 follow-up) is the *same* fix idiom, but
  **not** the same test idiom: `persist_wal`'s racing record reaches the core's
  pending-persist buffer via a bare *synchronous* `put()` call that never
  touches the driver loop's own `halted` check at all, so a `put`-then-
  `shutdown()` synchronous beat deterministically lands the fault "mid-window"
  under `SimEnv` (no task polling happens between two synchronous test-body
  calls). `flush_pending`'s data source (`drain_apply`) only becomes non-empty
  through the apply task's *own prior progress* — a test can't populate it via
  a bypass call — and its whole effects loop, once entered *after* that same
  iteration's own `halted` check already passed, runs uninterrupted to
  completion under `SimEnv` (disk ops resolve without yielding, so nothing
  hands control back to the executor mid-loop for an external driver to land a
  `shutdown()` "between" two calls inside it). Before porting a regression test
  idiom to a sibling call site with a superficially identical shape, trace
  *how* the racing work gets queued, not just where the two `.expect()`s sit —
  a bypass-the-check source is portable, a same-task-progress source is not
  (short of new pre-emption machinery this codebase doesn't have). Documented,
  not test-covered, in `animus-cp-data/CLAUDE.md`'s driver-split section.
- **A shared bring-up helper must borrow the caller's `TempDir` (or return
  its guard), never own-and-drop one internally** (issue #273).
  `dynamo_index_scan.rs::setup()` created `tempfile::tempdir()` locally and
  returned only `(nodes, addr)` — the guard dropped at the end of `setup()`,
  `remove_dir_all`-ing the data tree out from under the still-running 3-node
  LSM cluster the caller kept using, so a background WAL write later panicked
  `"wal group-commit sync failed"`. Every other bring-up helper in the suite
  already took `dir: &Path` from a caller-held guard; this was the one
  offender, found by a full sweep, not a pattern to assume is safe elsewhere.
  **The issue #278 halted-gated WAL tolerance does NOT retroactively make
  this safe**: that tolerance only applies while a driver loop's own
  `halted` latch is set (a deliberate shutdown in progress), and this
  cluster is never shut down — `halted` stays false the whole time, so the
  fault surfaces as the loud, unconditional durability panic, not the
  tolerated teardown case. Fixed by returning `(TempDir, Vec<Node>, SocketAddr)`
  and binding the guard at every call site (`let (_dir, nodes, addr) =
  setup().await;`). (`animusd/tests/dynamo_index_scan.rs`.)
- **A retry keyed on HTTP status code is strictly more robust than one keyed
  on a message substring, because distinct exhaustion points at the same
  call site share the status long after they diverge in wording** (issue
  #287). `streams_e2e.rs`'s `dynamo_retrying` (added by the issue #278 fix)
  retries any 500 unconditionally; the cascade-split lineage test's own
  `put` closure still asserted `status == 200` in one shot, and PR #294's
  retrofit of `dynamo_retrying` across this file's soak tests missed it.
  The CI failure this produced wasn't even the documented freeze→cutover
  blip `dynamo_retrying`'s own doc comment names — it was a *different*
  transient (`"CP kind write did not commit in time"`, a confirm-timeout
  under runner starvation) that happens to also surface as a 500, and the
  status-keyed retry absorbed it without needing to know its message at
  all. **General rule**: when retrofitting a proven retry helper across a
  file, grep every remaining single-shot status assert in that file, not
  just the one test the CI failure report named — a helper's own doc
  comment naming one root cause doesn't mean it's the only one the retry
  will end up masking, and that's a feature of status-code keying, not a
  bug to narrow away. (`animusd/tests/streams_e2e.rs`.)
- **A converged-or-timeout poll's budget needs headroom over the worst-case
  duration of a SINGLE retry attempt when that attempt rides a network-hop
  timeout, not just headroom over healthy wall-clock** (issue #281).
  `data_only.rs`'s `schema_ddl_via_a_data_node_relays_and_commits` retried
  `ProposeSchema` every 100ms inside a 20s poll — but a freshly-started
  data-only node has no leader hint yet, so a single attempt can
  legitimately fall all the way to broadcast and stall for the full 10s
  `CLIENT_TIMEOUT` before the next iteration even starts. A 20s budget is
  barely 2x one such worst-case attempt, not a real margin, and a starved
  CI runner routinely needs more than one. Same runner-aware-budget idiom
  as `split_cluster.rs`'s 30s→90s split-completion polls and
  `backfill_seeder.rs`'s 60s→180s `CONVERGE_BUDGET` (both issue #278 item
  9): size the multiplier against the attempt's own worst-case bound, not
  against how long the property takes to converge when nothing is
  contended. (`animusd/tests/data_only.rs`.)
- **A regression test for "does one writer's held lock stall an unrelated
  writer" is more reliably proven by a structural ordering check than by an
  absolute wall-clock threshold** (issue #285). The first design measured
  the unrelated write's elapsed time against a fixed millisecond bound —
  but on a resource-constrained sandbox, real backlog-induced apply lag
  turned out to have high, non-linear variance run to run (the *same*
  filler-flood configuration measured anywhere from ~150ms to over
  `CLIENT_TIMEOUT`, 10s, depending on ambient scheduler contention from
  concurrently-running sibling tests), and generous-but-fixed thresholds
  either passed spuriously under light backlog or risked flaking under
  heavy contention. The property under test doesn't actually need a
  number: `!slow_task.is_finished()` at the exact instant the unrelated
  write returns is a **hard ordering guarantee**, not a timing race —
  pre-fix, the unrelated write literally cannot even acquire the node-wide
  lock until the slow task's entire call (including its confirm-poll) has
  already returned and dropped the guard, so it can never observe the slow
  task as still in flight; post-fix, the slow task keeps grinding through
  its backlogged confirm well after releasing the lock, so the unrelated
  write routinely finishes first. Keep a loose absolute ceiling alongside
  it only as a hang guard (generous enough to never be the discriminating
  assertion), not as the property being proven. General form: when a
  regression is fundamentally about *ordering* (did A block on B, or not),
  prefer asserting the ordering directly (`JoinHandle::is_finished`, a
  shared flag, a channel) over inferring it from a wall-clock threshold —
  the latter only ever approximates the former, and approximates it worse
  the more the environment's real-time behavior varies.
  (`crates/animusd/src/lib.rs::confirm_futility_tests::
  an_unrelated_evaluated_write_is_not_stalled_behind_another_writes_confirm_wait`.)
- **An unthrottled continuous write flood against an INDEXED table racing a
  background convergence process can make that process livelock rather than
  exercise it** (issue #288). A first draft of the freeze-window regression
  ran 6 concurrent max-speed `PutItem` loops (fresh TCP connection per
  request, no pacing) against a table with a GSI, from just before a
  split's kickoff until cutover — and the split never converged within a
  90s budget, because split cutover itself gates on the GSI drain's own
  veto (it must catch up to the max pending change record before a parent
  can retire, `docs/streams-notes.md`), and the flood was generating new
  change-log backlog faster than the drain could clear it — a genuine
  live-lock, not a hang. It also incidentally triggered a real engine-level
  I/O error on the sandbox under that load. Pacing the flood down to 2
  lanes with a 20ms delay between attempts fixed both: the split converged
  in ~17s instead of timing out at 90s, and the test still reliably covers
  the (sub-second, ADR 0050 rung 8's F8 contract) freeze window with dense
  enough probing. General form: a test that races a continuous write flood
  against a background convergence loop must pace the flood below that
  loop's own throughput, especially when the convergence condition itself
  depends on catching up to the write volume — "hammer as fast as possible
  until X happens" silently assumes X's own progress is independent of the
  hammering, which is false whenever X is gated on draining exactly what's
  being hammered in. (`crates/animusd/tests/split_build.rs::
  probe_put_item_until_stopped`.)

- **Real-thread `ProdEnv` liveness/concurrency tests and deterministic
  `SimEnv` suites need different CI contracts, and the split enforcing that
  must be structural, not a hand-maintained list (issues #280/#286,
  2026-08-18).** `ci.yml`'s single `gates` job ran the whole workspace's
  `cargo test` on a 2-vCPU shared runner; the real-thread tests (`animusd`'s
  66 integration binaries + its real-thread `--lib` tests, plus 6 scattered
  binaries in `animus-control`/`animus-cp-data`/`animus-storage`/
  `animus-consensus`) blow their timing/convergence budgets under
  contention from everything else the job builds/runs, starving a different
  victim almost every run. Splitting them into a separate, bounded-retry
  `prod-liveness` job needed a way to keep them out of the deterministic
  `gates` job's plain `cargo test` that can't silently drift as tests are
  added later — a YAML exclude list naming test files by hand is exactly
  the kind of "two places must agree" hazard this file already warns about
  generally. Fix: give each of the 4 non-`animusd` crates an opt-in,
  default-off `prod-heavy` Cargo feature and declare each real-thread
  binary as an explicit `[[test]] required-features = ["prod-heavy"]`
  target — a plain `cargo test` (no `--features`) then skips building or
  running them automatically (verified directly: `cargo test -p
  animus-storage` builds and runs every other binary in the crate but
  neither `lsm_concurrent` nor `idle_engine_cost`, both of which build and
  run once `--features prod-heavy` is passed), while `clippy --all-features`
  still type-checks them on every push. A new real-thread test added later
  without this treatment simply runs inside `gates` and re-creates the
  starvation — the fix converts "don't forget to exclude it" into a
  compile-visible property (an un-gated binary is *always* in the plain
  `cargo test` run) instead of a rule someone has to remember. The retried
  tier is a stopgap for runner-class starvation, not a waiver: a test
  failing both attempts still blocks merge, per this file's standing
  "a flaky `ProdEnv` integration test is a real bug" rule.
  (`.github/workflows/ci.yml`, `crates/{animus-control,animus-cp-data,
  animus-storage,animus-consensus}/Cargo.toml`.)
- **A streams test that bursts several writes and then reads only
  `DescribeStream`'s `shards[0]` is asserting on a seal-timing coincidence, not
  on exactly-once delivery.** The age-trigger seal arm sweeps on the hard-coded
  200ms `INDEX_DRAIN_INTERVAL` tick, so a test whose `seal_age` is only a small
  multiple of that (300ms) can have its burst straddle a tick under real
  WAL-fsync-bound write latency and correctly produce *two* shards — a closed
  one plus an open tail holding the last write(s). Reading only the first then
  reports "must see every record exactly once" failing, which reads exactly
  like a product exactly-once bug but is the product behaving correctly (the
  missing record is always the *last-written* one — that signature is the
  tell). Walk the whole chain `DescribeStream` returns, including the trailing
  open shard, as `get_records_walks_the_shard_chain_and_drains_the_open_tail`
  already did. Do **not** "fix" it by enlarging `seal_age` or asserting a shard
  count: both only shrink the window, and the shard count is legitimate
  timing-dependent product behavior a test must not fight. Confirmed by walking
  the full chain and recovering every record, proving nothing was lost.
  (`crates/animusd/tests/dynamo_streams.rs`, issue-less flake off `main`,
  2026-08-20.)
- **A guard of the shape `if !still_exists(x) { skip }` placed immediately
  before unbounded-latency async I/O against `x` is a probability reducer, not
  a safety check** — the object can change state in the gap, and where the
  callee already re-validates itself fresh on every call (the right design),
  the caller's pre-check is an optimization only. The caller must still treat
  the callee's own authoritative "no longer valid" answer as an *expected
  outcome*, not a fatal one. A streams-lineage walker computed the next shard
  id client-side from a locally-cached `Metadata` snapshot, checked
  `tablets.contains_key`, then made two async round trips before `GetRecords`
  landed; a split cutover retiring the tablet in that gap made the
  speculatively-guessed epoch one that never existed, so the server's 400
  `TrimmedDataAccessException` was the *correct* answer and the test panicked
  on it. Note the scoping rule that came with the fix: handle such an expected
  terminal error **at the one call site that can legitimately provoke it**,
  never by widening a shared retry/allowlist helper — the same status on a
  transactional-write path still means a real bug. (#299,
  `crates/animusd/tests/streams_e2e.rs`, 2026-08-20.)
- **"Green locally under `yes`-loop contention" does not refute a hosted-runner
  flake — the two starvations are different in *shape*, not just degree.**
  Across four independent investigations into CI flakes, ~340 local executions
  under heavy synthetic load (CPU burners, 2-core `taskset` pinning, parallel
  same-binary runs) reproduced almost none of them. Raw thread oversubscription
  on a multi-core box is round-robined fairly by CFS and mostly yields *slower
  average* scheduling; a GitHub-hosted 2-vCPU runner throttles via cgroup
  CPU-bandwidth quota, which produces hard periodic full-stop stalls once the
  quota is exhausted. Races needing a single >100ms stall of one specific task
  surface readily under the latter and rarely under the former. Practical
  consequences: (a) do not treat "N clean local runs" as evidence a CI flake is
  fixed — say what it does and does not show; (b) prefer a cgroup-quota-
  throttled repro (`systemd-run --scope -p CPUQuota=…`, or a manual cgroup v2
  write) over busy-loops when trying to reproduce one; (c) a `yes`-loop repro
  campaign will happily trip *different* real bugs than the one under
  investigation — verify the failure signature matches before counting it as
  evidence. (2026-08-20.)
- **An intermittent deficit that resists root-causing should get a permanent
  on-failure diagnostic landed as its own commit, *before* any fix attempt.**
  Weakening or retrying the assertion would destroy the evidence; a speculative
  fix would be unfalsifiable. Land instead a dump that fires only on the
  failure path and captures whatever distinguishes the competing hypotheses —
  for a streams exactly-once deficit that is the missing ids, the shard each
  delivered record arrived under, live vs retired tablets, and **the per-tablet
  closed-chain length**. That last datum had already, once, redirected an
  investigation out of the wrong subsystem entirely (the seal/`Freeze` path)
  toward the right one (the open, never-sealed tail) — and it was recorded only
  in a *comment* on a predecessor issue, not in the issue body that inherited
  the investigation. Corollary: when an issue cites a prior issue or comment as
  its evidentiary basis, fetch that comment; an issue body is not a complete
  transcript of its own history. (#298,
  `crates/animusd/tests/streams_e2e.rs`, 2026-08-20.)
- **Headless-Chromium verification of a static page (the `website/`
  marketing site, or any HTML rendered headlessly): inject an inline
  `<script>` into a *copy* of the page and read the result back out of
  `--dump-dom`.** `--dump-dom` and `--virtual-time-budget` work fine; what
  does *not* work is `--evaluate-on-load-file`, which **silently no-ops** —
  the script never runs, the dumped DOM is the unmodified page, and there
  is no error, so a check that "passes" may never have executed. Verify a
  flag's effect once against a page whose title the script is supposed to
  change before trusting it as a gate. The working recipe: copy the page
  to a scratch dir, splice a `<script>` before `</head>` that writes the
  measurement into `document.title` (e.g. `clientWidth`/`scrollWidth` for a
  horizontal-overflow check), then
  `chrome --headless --window-size=W,H --virtual-time-budget=2500
  --dump-dom URL | grep -o '<title>[^<]*</title>'`. For anything needing
  real interaction (clicking a theme switch, a mobile nav) or a
  `prefers-color-scheme` override, drive the same pre-installed Chromium
  through Playwright instead (`node` at `/opt/node22/bin/node`; the
  package at `/opt/node22/lib/node_modules/playwright` is **not** on the
  default module path, and `NODE_PATH` does not affect ESM resolution, so
  import the absolute `/opt/node22/lib/node_modules/playwright/index.mjs`
  or run from inside that directory), launching with `executablePath:
  '/opt/pw-browsers/chromium-<rev>/chrome-linux/chrome'` and
  `args: ['--no-sandbox', '--disable-gpu']`.
- **A headless-Chromium screenshot narrower than ~485px is cropped, not a
  layout bug.** The browser enforces a minimum window width, so
  `--window-size=390,H` lays the page out at ~485px and captures a 390px
  slice of it — content looks clipped at the right edge and a centered
  container looks off-centre. Diagnosing that as responsive breakage sends
  you chasing a bug that isn't there. Measure overflow numerically
  (`scrollWidth` vs `clientWidth`, above) rather than trusting the
  narrow-viewport image, and treat ~500px as the narrowest *trustworthy*
  screenshot width. (website DynamoDB-focus pass, 2026-08-21.)

- **Verifying with `cargo test --workspace` does not reproduce this repo's
  CI, and manufactures failures CI would never see** (2026-08-22, the
  `ScanIndexForward` rung). CI deliberately splits animusd out:
  `cargo test --workspace --exclude animusd -- --test-threads=2`, then
  `cargo test -p animusd --lib --tests -- --test-threads=1`, because that
  crate's ~66 real-thread multi-node integration tests starve each other at
  default parallelism. A local `cargo test --workspace` runs exactly the
  configuration CI was restructured to avoid — every heavyweight cluster
  test concurrently — so a failure there is not yet evidence about the code.
  It cost a full investigation of `schema_ddl_on_a_follower_is_relayed_to_the_leader`
  that passed both in isolation and serially. The investigation was still
  right to run (the diff touched forwarded request enums, and a missed
  relay match site is exactly the bimodal per-process flake this log warns
  about) — but the *first* step should be re-running the way `ci.yml` does,
  before reading anything into the parallel sweep. Corollary: when a local
  gate disagrees with CI, check how CI actually invokes it before debugging
  the code; the invocation is part of the gate.

- **Two disk exhaustions in one session: a debug `--all-targets` build of
  this workspace is ~24GB, and `cargo clean -p <crate>` is the surgical
  tool** (2026-08-22). `target/debug/deps` reached 23GB across 94 test
  binaries over 100MB each (debuginfo=2 × animusd's test-binary count).
  Symptoms mislead: the failure surfaced as a linker `signal 7 (Bus error)`
  and `ld terminated`, not an obvious out-of-space error, and once as
  `failed to write dep-graph.part.bin`. `cargo clean -p animusd -p
  animus-cp-data -p animus-dynamo` freed 17GB while keeping every
  dependency build cached (a full `cargo clean` would have forced tokio,
  serde and the rest to rebuild); deleting `target/debug/incremental`
  alone freed 5GB more. Prefer targeted `cargo test -p <crate> --test
  <name>` over full sweeps when disk-constrained — it also happens to
  match how CI runs. **Addendum (2026-08-26): `CARGO_PROFILE_DEV_DEBUG=0`
  avoids the problem at the source** rather than cleaning up after it —
  running the five gates for a plumbing-sized PR (ADR 0059 Train 1 PR②)
  hit the identical symptom (`cargo build --workspace --all-targets`
  exhausting a fresh sandbox's whole root filesystem, not just this
  workspace's `target/`, since `/tmp` shares the same device), even after
  the incremental/`cargo clean -p` cleanup above: `target/debug/deps`
  alone held 2493 files, 24GB of which was debug-info-laden test/bin
  **executables** (not `.rlib`/`.rmeta`/`.d`, which incremental
  compilation actually needs) from binaries that had already run and
  would never be reused. Two independent fixes, both safe to combine:
  (1) `find target/debug/deps -maxdepth 1 -type f ! -name '*.rlib' !
  -name '*.rmeta' ! -name '*.d' -delete` reclaims a spent test binary's
  space without invalidating any dependency crate's incremental cache
  (unlike `cargo clean -p`, which does); (2) prefixing the build/test
  invocation with `CARGO_PROFILE_DEV_DEBUG=0` (a `dev`-profile override,
  not a workspace `Cargo.toml` edit — nothing to revert) drops full debug
  info from every artifact for that invocation, shrinking a from-scratch
  `cargo build --workspace --all-targets` from ~28GB to ~9GB. Prefer (2)
  proactively before a from-scratch `--all-targets` gate run in a
  disk-constrained sandbox; reach for (1) if a run has already ballooned
  `target/` and a full `cargo clean` would be too slow to recover from.
  **Addendum (2026-08-29): the disk is shared across sibling worktree
  agents, not just your own crate's `target/`.** A gate run flip-flopped
  between ENOSPC and 12GB free within minutes with zero commands run in
  between — `du -sh /home/user/animus-db/.claude/worktrees/*/target
  /home/user/animus-db/target` showed 12–17GB apiece in *other* agents'
  worktrees and the base checkout, ballooning and draining on their own
  schedule as those agents built and cleaned. `cargo clean` in your own
  worktree is necessary but can be insufficient — check sibling worktrees'
  sizes before concluding the sandbox itself is out of room, and retry a
  failed gate once or twice before escalating, since a neighbor's build
  finishing can free multiple GB with no action on your part. `cargo
  check --workspace --all-targets` (type-checks everything the `#[deny]`
  lint machinery cares about, no linking) is a much cheaper stand-in than
  `cargo build --workspace --all-targets` when only verifying that a
  lint-attribute or type-level change compiles, and `cargo test -p <crate>
  --lib -j 1` (low parallelism caps peak concurrent temp-file usage) plus
  a handful of `--test <name>`s targeted at the changed modules is a
  reasonable substitute for a full per-crate integration suite (84+ test
  binaries here) when the disk is this contended.
- **"Converges slowly" and "never converges" produce the same panic message
  and need different investigation methods — instrument the poll itself
  before theorizing** (2026-08-22, `streams_e2e.rs::multi_split_soak_
  streamed_gsi_table_under_mixed_load`'s GSI-convergence `await_true(60,
  "GSI never converged to one row per item", ..)` flake, ~1-in-5 at CI's
  own `--test-threads=1`). Reading the production code first surfaced a
  real, code-acknowledged candidate mechanism: a split's right child, when
  its own range boundary isn't token-aligned (the common case for any real
  string partition key — see `animus-cp-data/CLAUDE.md`'s `cursor.rs`
  entry), computes a GSI-drain cursor-advance write that **routes off its
  own tablet** and lands physically inside its left sibling's `KIND_CURSOR`
  scope instead (`index_drain.rs::drain_tablet`'s own comment already names
  this "a named, pre-existing follow-up"). That looked, on paper, like
  exactly the "descheduled and never re-triggered" production bug the
  investigation was supposed to rule in or out. **It wasn't** — tracing
  through `cursor_min_watermark`'s min-over-rows semantics (a stray row can
  only ever *depress* the computed watermark, never inflate it) shows the
  misrouted write can only cause redundant re-reconciliation and delayed
  trim on the affected tablets, never a wrong materialized row; an existing
  regression (`split_right_childs_cold_start_re_reconciles_from_zero_
  without_corrupting_the_gsi`) already proves the GSI itself stays correct
  through exactly this shape. That structural argument was worth having,
  but it was **theory** until measured: adding one `eprintln!` per poll
  iteration (`GSI_POLL n=.. total=.. per_tag=.. elapsed_ms=..`, and a
  second at the misroute's own write site) and reproducing under real
  thread-oversubscription CPU contention turned up the actual failure
  twice, and both times — plus every successful-but-slow run — the
  per-poll total was **monotonically non-decreasing**, climbing steadily
  to a timeout at 120/144 in one run and to full convergence at 65.5s and
  66s in two others; the misroute eprintln never fired in any reproduction.
  That per-poll trace is what actually answered the question, not the code
  reading: a permanently-wrong or flat-forever count would have looked
  identical to a slow-but-live one in the bare pass/fail result, so the
  fix is a measured, named ceiling
  (`GSI_CONVERGENCE_DEADLINE_SECS = 150`, ~2.3x the worst observed
  convergence), not a production patch. **General rule**: when a
  converged-or-timeout poll on an eventual property flakes, add a
  per-iteration trace *before* forming a theory about the cause — a
  plausible-looking code-level gap (especially one already flagged in a
  comment) can be a real, harmless inefficiency rather than the operative
  bug, and only the poll's own trajectory at the moment of failure can
  tell the two apart. Corollary confirming this file's own prior entry on
  the subject: raw local thread-oversubscription (`nproc`-many `yes`
  loops) took roughly a dozen attempts (several clean passes, one
  unrelated known flake, then the real one) to reproduce the failure here,
  consistent with it being a real but lower-probability-than-CI shape (a
  genuine 2-vCPU cgroup-quota-throttled runner is
  documented elsewhere in this file as harder to trip via busy-loops); a
  manual cgroup v1 CPU quota was attempted for a more faithful repro but
  the sandbox's cgroup filesystem silently no-ops `cgroup.procs` writes
  (confirmed via `/proc/<pid>/cgroup` after the write reported success) —
  worth knowing before spending time on that route in this environment.
  (`crates/animusd/tests/streams_e2e.rs`, `crates/animusd/src/
  index_drain.rs`, `crates/animus-cp-data/src/cursor.rs`.)
- **A test's doc comment claiming a "hard ordering guarantee, not a timing
  race" is itself a bug when the guarantee it names is really a wall-clock
  coincidence — and the confident wording is *why* it survived review.**
  Issue #285's regression test
  (`confirm_futility_tests::an_unrelated_evaluated_write_is_not_stalled_
  behind_another_writes_confirm_wait`, `animusd/src/lib.rs`) built its
  "write A is slow" scenario with a concurrent filler flood racing real
  apply backlog against real time, then asserted `!slow.is_finished()`
  when the unrelated write returned, with a comment insisting this
  followed "by construction." It doesn't: on a CPU-starved runner the
  flood is starved right along with everything else, so it can fail to
  build any backlog at all, and the "slow" write finishes first — CI
  reproduced this exactly, one parallel run of commit `97289e2` green and
  one red, the red one logging the unrelated write taking 104ms with write
  A *already* done. The fix's own #285 property (`rmw_lock` is not held
  across the confirm-poll) is a real structural guarantee; "write A is
  still in flight when write B returns" is not — it depends on which of
  two independently-starvable things loses its race, which is exactly what
  load inverts. The general lesson: when a test's comment asserts something
  is guaranteed "by construction," check that literally nothing about the
  assertion's truth depends on relative timing between two things that can
  each independently slow down under load — if it does, the comment is
  overclaiming, and overclaiming is worse than not commenting at all,
  because "by construction, not a race" is precisely the sentence that
  waves off the scrutiny that would have caught it being a race. The fix
  here (not a timeout bump, per the standing rule below) replaced the real
  backlog race with a `#[cfg(test)]` hook
  (`dynamo::rmw285_confirm_gate`) that holds write A's propose+confirm
  phase open for a fixed, generous delay under the test's own control —
  deterministic and immune to scheduler load — and rewrote the comment to
  say plainly that the remaining `elapsed < GATE_DELAY / 2` margin is a
  generous-but-finite budget, not a proof. See the "flaky `ProdEnv`
  integration test is a real bug" and "compare wall-clock against a clean
  run" entries above for the family this belongs to — this one adds: the
  fix for a race dressed up as a guarantee is to either remove the race
  (control the timing yourself) or, if that's truly unreachable, say in
  the comment that it's a margin, not a guarantee, so the next reader knows
  what they're actually relying on. (#285, 2026-08-22.)

- **A retryable, self-resolving condition must not share a fixed budget with
  a real error — and the fix is a bigger ceiling on the SAME poll, not a
  new mechanism** (2026-08-22, `streams_e2e.rs::multi_split_soak_streamed_
  gsi_table_under_mixed_load`). The test's `dynamo_retrying`/
  `dynamo_retrying_transact` helpers already did the right thing
  structurally: they only keep retrying while the status/message matches a
  documented transient (the ADR 0050 F8 `"; retry"`-suffixed freeze→cutover
  refusal, or the analogous election-wait 500), and fail immediately, zero
  retries, on anything else — a genuine error was never at risk of being
  masked. The actual defect was just that the shared ceiling (a flat 20s)
  was sized for the steady-state "sub-second" cutover ADR 0050 documents,
  not for what a cutover can take when the process is CPU-starved — and
  this specific soak test deliberately drives a CASCADE of overlapping
  splits (a 2KiB auto-split threshold against a continuously-written
  table) specifically to stress that path. Root-caused by reading the
  server side, not just the test: `ClientCtx::cp_kind_write_item`/
  `cp_kind_write_raw` already retry the freeze refusal internally (issue
  #288), but `ClientCtx::cp_txn` (the `TransactWriteItems` coordinator)
  deliberately does not — matching real DynamoDB, where a cancelled
  transaction is the client SDK's job to retry, not the server's to
  absorb — so for the transact path this test's own client-side loop is
  the *only* thing standing between a slow cutover and a spurious failure,
  with no server-side safety net underneath it. Fix: widened the shared
  ceiling from 20s to 90s (one named constant, `RETRYABLE_BLIP_DEADLINE`,
  used by all three call sites that shared this weakness:
  `dynamo_retrying`, `dynamo_retrying_transact`, and the structurally
  separate `get_records_allow_trim`) — not a new polling mechanism, since
  the loop already re-checked the real condition every pass rather than
  asserting once. The general check this leaves behind: before touching a
  timeout on a "flaky" `ProdEnv` test, verify **which** of the two
  brackets an assertion falls into — does it currently retry/wait on
  *anything*, or only on a specific documented-transient shape? If the
  latter, and the shape match is airtight, widening that specific ceiling
  is the correct fix, not the "don't bump the timeout" anti-pattern this
  log warns about elsewhere — that warning is about hiding an
  undiagnosed failure behind a longer wait, not about sizing a
  already-scoped retry correctly.

  **Also found, NOT fixed, flagged here per this repo's own
  incidental-bug convention:** validating the fix by running the full
  `streams_e2e` binary repeatedly with `--test-threads=1` (CI's actual
  invocation, per `ci.yml`'s `prod-liveness` job — the *default*
  unflagged `cargo test -p animusd --test streams_e2e` oversubscribes a
  small runner by running 13 heavyweight real-thread cluster tests
  concurrently, itself the exact anti-pattern the entry above this one
  documents, and produced a different, non-representative failure at the
  old `dynamo_retrying` retry site before the ceiling widened) surfaced a
  **separate, pre-existing** flake in the same test: its GSI-convergence
  check (`await_true(60, "GSI never converged to one row per item", ..)`,
  a Query-based poll over the four tag partitions) timed out twice in 14
  full-suite runs even after this fix, always with every other assertion
  in the same run green — including the exactly-once delivery check
  immediately before it, which is deliberately never loosened (issue
  #298's own note). This is structurally identical to the bug this entry
  fixes (a background-reconciler-driven eventual property on a fixed
  ceiling, likely too tight for a cascade of splits under real
  contention) but is a different code path (GSI backfill/query
  convergence, not the transact retry budget) and was never reported
  against — diagnosing and fixing it belongs in its own change with its
  own investigation, not folded into this one.
- **Weakening a default is a test-suite-wide event, and the compiler cannot
  see any of it (2026-08-23, ADR 0055).** Making `ConsistentRead: false` —
  DynamoDB's *default* — a genuinely eventually-consistent read broke a
  scatter of tests across unrelated files, all with the same shape: write
  something, then immediately read it back to check the write landed. Every
  one of them had been silently relying on the read path being *stronger than
  the API promised*. Two things worth carrying forward. **First, the failures
  are non-deterministic by construction** — a follower that happens to have
  applied in time passes — so "the suite went green once" proves nothing
  here; the real signal is auditing which reads exist to verify a write, not
  re-running until it passes. **Second, the fix is never to re-strengthen the
  default**: each such test asks for `ConsistentRead: true`, which is exactly
  what a real DynamoDB client must do, and the resulting test says out loud
  which consistency it depends on instead of inheriting one by accident.
  Generalized: when a change makes a default *weaker* (a read, a lock, a
  timeout, a durability level), expect the breakage to land in tests that
  never mention the thing you changed, expect it to be flaky rather than
  deterministic, and treat each break as a test that was under-specified
  rather than as evidence against the change.
- **A multi-endpoint read loop is only as consistent as its weakest
  endpoint — and that was free until it wasn't (2026-08-23, ADR 0055).**
  Several suites deliberately round-robin a paginated `Query`/`Scan` walk
  across all three nodes, with an explicit comment saying why: it exercises
  the *forwarded* read path, not just the node that happens to lead the
  tablet. That rotation was implicitly safe because every node forwarded to
  the same leader, so all three answered from one state and a convergence
  poll on **one** node covered all of them. Making the default read
  replica-local broke that silently: consecutive pages now sample different,
  independently-lagging replicas, so a walk can terminate a page early and
  drop an item into the gap — which is exactly what CI caught
  (`gsi_query_paginates_with_the_scan_cursor_shape`: node 1 had all 6 GSI
  rows, another node still had 5, and `sk=a5` was never returned). Two things
  generalize. **The rotation is worth keeping** — it is testing something
  real — so the fix is to make the data stable across endpoints, not to stop
  rotating: ask for the strong read where the API allows it, and where it
  does not (a GSI rejects `ConsistentRead: true`), converge on *every*
  endpoint before the walk, not just one. And more broadly: **when a change
  makes per-endpoint views diverge, audit every loop that talks to more than
  one endpoint and combines the results** — paginated walks, parallel-scan
  segment fleets, any "collect from each node then compare" assertion. None
  of them mention consistency, the compiler cannot see them, and most will
  pass most of the time.
- **A stale doc comment is a third way the same ADR 0055 bug hides, next to
  the two above.** `kind_scan.rs`'s LSI-query round-robin (the same
  every-node-in-turn pattern as the entry just above, but for an LSI) omitted
  `ConsistentRead` and asserted `Count:3` unconditionally; CI caught a
  lagging replica-local read returning `Count:2`. The test's own comment
  ("a query is strongly consistent — no polling needed") and `dynamo.rs`'s
  LSI-branch doc comments ("an LSI is strongly consistent by construction…
  `consistent_read` is simply dropped on that branch") both predated ADR
  0055 and were never updated when the flag started selecting a real read
  path there too, so a reader trusting either comment would conclude the
  missing annotation was fine. Fixed the same way: `ConsistentRead: true`,
  since the loop is a write-verification read exercising the forwarded path.
  Generalized: an ADR that changes a default's behavior obsoletes every doc
  comment written under the old behavior, not just the code calling it —
  grep for the old invariant's wording (`strongly consistent`, `by
  construction`, `is simply dropped`) wherever the changed path is
  documented, not only at its call sites.
- **A test can prove "this path never blocks" by how it drives the path.**
  `animus-cp-data`'s `tests/stale_read.rs` deliberately drives the ADR 0055
  eventual reads with `block_on` instead of this crate's usual spawn-and-
  `run_for` `drive` helper. Under `SimEnv` nothing advances the clock unless
  the simulator is driven, so a read that ever grew an internal `env.sleep`
  — a barrier, a ceiling wait, an intent chase — **hangs that test** instead
  of quietly costing what the expensive path costs. The cheap path's defining
  property is a budget, and a budget that nothing enforces is a comment; this
  turns it into a test failure. Applicable anywhere a "must not block / must
  not round-trip" invariant matters and would otherwise only be documented.
- **A theorized failure mode should be verified empirically before a test is
  built around it — the read-before-write architecture can make an "obvious"
  race unreachable through the surface you'd naturally test it from
  (2026-08-24, ADR 0018's `CancellationReasons` amendment, issue #374 C2b).**
  Building `TransactionConflict` reachability coverage, the natural test
  looked like: stage one transaction's intent on a key and never decide it,
  then send a real `TransactWriteItems` touching that same key through the
  DynamoDB edge, expecting `StageOutcome::IntentBlocked` (the apply-time
  writer-push-intents guard) to surface as `TransactionConflict`. It never
  did — every attempt produced a generic, slow (~5s) failure instead. The
  reason only became visible by tracing the actual call path: every DynamoDB
  write action reads the item's *current value* first
  (`ClientCtx::txn_stage_local` → `dynamo::eval_kind_txn_write` →
  `cp_get_local_resolving`, needed to evaluate the action's own
  `ConditionExpression` and diff LSI rows) — for a key already holding
  another transaction's local pending intent, that read itself blocks
  (`INTENT_WAIT_TIMEOUT`, 5s) or fails, so the apply-time guard the test
  meant to exercise is never even reached; the write never gets far enough
  to propose. The write path that DOES hit the guard directly — a raw,
  already-known-value write (`TxnTableWrite::plain`, the plain client
  protocol's own shape, no preceding read) — was reachable and fast (under
  2s) once tried. **General rule**: when a test keeps producing an
  unexpected result for what looks like a straightforward race, trace the
  actual code path the request takes end to end (not just the two states
  you expect to interact) before concluding the test needs a bigger timeout
  or a cleverer timing trick — a front-loaded read, a cache, or an
  early-return check can make a "later" failure mode structurally
  unreachable from a particular entry point, and the fix is choosing a
  different entry point (or documenting the narrower reachability, as this
  amendment's own ADR section does) rather than fighting the timing harder.
- **A per-item `WireError` raised inside a `TransactWriteItems` write action's
  evaluation does not keep its own error `code` by the time it reaches the
  wire (2026-08-24, issue #372 part 2).** `wire::apply_update`'s
  `ValidationException` (the 400 KB post-update-result cap, closed this
  change) propagates out of `eval_kind_txn_write` fine as a `WireError`, but
  `ClientCtx::txn_stage_local` immediately `map_err`s it to a plain `String`
  (`format!("txn prepare: leader-side evaluation failed: {e}")`) to satisfy
  `cp_txn`'s `Result<_, String>` signature, and `run_transact`'s `Err(e)` arm
  then wraps *that* string as `WireError::transaction_canceled(..)` —
  `TransactionCanceledException`, unconditionally, regardless of what the
  original code was. So the same validation failure surfaces as
  `ValidationException` through `UpdateItem` (which calls `apply_update`
  directly) but as `TransactionCanceledException` (with the original message
  merely nested in the text) through `TransactWriteItems`'s `Update` action.
  A test asserting the specific DynamoDB error code on a transactional write
  path must know which of these two shapes applies rather than assuming the
  bare `WireError` constructor's code survives — match on the nested message
  substring there instead. (`animusd/tests/dynamo_item_size_cap.rs`.)
- **A `RaftCore::new`/`RaftNode::start` bootstrap's `all_nodes` argument sets
  the node's own local `config` directly at construction, with no consensus
  involved at all — a test that includes a not-yet-added node's own id in its
  own `all_nodes` is trivially, unconditionally wrong from its very first
  line, regardless of any later replication (2026-08-24, ADR 0058 Train 1's
  learner corpus).** This bit despite `animus-cp-data/CLAUDE.md` already
  documenting the exact gotcha ("pre-start a to-be-added node knowing only
  the *current* voters, NOT itself") — reading the warning and then still
  writing `RaftNode::start(env, [0,1,2,3]..)` for the node with id 3 happened
  because the assertion that would have caught it fired *after* the real
  `add_learner`/`change_membership` had already replicated, at which point
  the (buggy) locally-bootstrapped value and the (correct) replicated value
  are byte-identical and the bug is invisible. It only surfaced because this
  corpus asserted the learner's own `config()` *before* any real membership
  change — `learner.config()` showed `{n0,n1,n2,n3}` immediately after
  `RaftNode::start` returned, before the leader had even proposed
  `add_learner`. **General rule**: when a test's own assertion checks a
  freshly-started node's config/role *before* the operation under test has
  had a chance to replicate anything to it, the "excluded from its own
  `all_nodes`" gotcha becomes load-bearing rather than cosmetic — and a
  pre-existing test that only asserts *after* replication (e.g.
  `animus-control/tests/control_membership.rs`'s own `add_a_node_...` test,
  which includes the new node's id in its own bootstrap set and gets away
  with it) is not proof the risky pattern is safe in general, only that its
  own assertions never exercised the window where it would matter.
- **A threshold-based "caught up" predicate measured as an absolute gap
  (`last_index - match_index <= threshold`) cannot distinguish "genuinely
  replicated" from "the log itself is short"** — found writing ADR 0058
  Train 1's reconciler-adoption corpus
  (`animus-cp-data/tests/reconciler_corpus.rs`,
  `tests/learner_reconfigure.rs`). A test meaning to catch a newly-added
  learner "still mid-catch-up" (e.g. to prove it survives a partition, or
  that the old quorum keeps committing without it) partitioned the learner
  immediately, then ticked the reconciler several times before asserting —
  and the assertion failed, because on a log only a few entries long, a
  learner with `match_index = 0` still satisfies `last_index - 0 <=
  RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD` (4) and gets promoted anyway,
  despite having received exactly zero `AppendEntries`. This is not a bug
  in `learner_caught_up` (the primitive is documented and used as designed
  — a fixed absolute threshold, not a fraction of the log) — it is a
  property of the design that every test exercising "still catching up"
  must account for: either grow the log well past the threshold *before*
  the fault (so a genuinely-unreplicated learner's gap stays provably
  large regardless of how short the log started), or assert immediately
  after the single tick that performs the add (a promotion cannot happen
  in the same call that proposes the add, so the state right after is
  unambiguous regardless of log length). **General rule**: when a
  liveness/catch-up gate is an absolute distance rather than a ratio,
  don't assume "hasn't replicated anything" and "gap is small" are the
  same condition in a test fixture — they coincide only once the log is
  long enough, and a fixture's own small scale can silently violate that
  precondition.
- **When a reconciler-driven test scenario lets a promoted learner become
  eligible for LEADERSHIP, the test's own tick/poll loop must include that
  node, not just the original voters** — the same corpus above
  (`learner_crash_is_replaced_by_a_new_target`) hung for the entire poll
  budget on one seed in ~14% of variants: the newly-promoted replica won
  the next election (perfectly legitimate — it is a real voter the moment
  it is promoted), but the test's convergence loop only ticked the
  *original* three nodes' reconcilers, so the one node that could actually
  see itself leading and propose the final "remove the old replica" step
  was never given a chance to. The group was correctly converged in every
  way that mattered (right voters, right learners, keeps serving) —only
  the test's own harness was blind to who was driving. **General rule**:
  a test that lets membership grow past its initial cast of leader
  candidates must poll/tick every member that could plausibly hold
  leadership by the time the property under test is checked, not just the
  set that started the scenario.
- **A "durable-before-visible" primitive that also hands back a live handle
  to what it just made durable has an inherent asymmetry an `Err`-implies-
  "nothing happened" test gets wrong (2026-08-25, ADR 0058 rung 2's
  `LsmEngine::clone_to`).** `clone_to`'s crash-safety contract is: the
  target's manifest write is the single commit point, so a fault before it
  leaves nothing durable at the target. But `clone_to`'s own last step —
  opening the just-committed target to return a usable engine handle — is
  itself fallible disk I/O, so a fault landing *there* (after the manifest
  commit already succeeded) still returns `Err` even though a fully valid
  clone now exists on disk. A fault-injection test built on the assumption
  "any `Err` from this call means the target must scan empty" failed
  intermittently for exactly this reason once the source had more than one
  SSTable (more disk ops inside `clone_to` means more chances for the fault
  to land in the trailing reopen rather than the commit). The fix was
  asserting the actual invariant — the target is always either **fully
  absent** or **fully valid**, never partial — not the stronger, false one.
  **General rule**: for any "commit, then hand back a live view of what was
  committed" operation, write the crash-safety test (and the doc comment)
  around "nothing or complete, never torn," not "success or no-op" — the
  post-commit step can still fail on its own without meaning the commit
  itself didn't happen.
- **Two independently-scheduled per-node loops that both react to the same
  underlying event, on different signals with different latencies, can race
  in a way `SimEnv` won't show you (2026-08-25, ADR 0058 Train 2 rung 3, the
  `animusd`-level in-place-split driver).** The in-place split fork
  (`KvCommand::SplitTablet`, Stages 1-3) is deliberately **not** a control-plane
  commit — the whole point of the in-place design is that minting the two
  child Raft groups happens inside the CP-data group itself, off the control
  plane's critical path. That left two consumers of "the fork happened" with
  very different reaction times: the per-node tablet-host reconciler
  (`animus-cp-data::host`) only wakes on `metadata_watch()` (which never fires
  for an in-place fork, since nothing changed in `Metadata`) or its own
  500ms fallback poll, while the new `animusd`-level cutover driver polls
  every 200ms via the existing change-consumer loop and, seeing no vetoes
  configured, proposed `CutoverSplit` almost immediately. `CutoverSplit`
  deletes the parent's `Metadata` row — the reconciler's only durable memory
  that a child tablet is a split product needing `MaterializeSplitChild`
  (clone the parent's engine slice, bootstrap with the fork's superset voter
  set) rather than an ordinary fresh `Host`. On any replica whose reconciler
  hadn't yet run *its own* post-fork tick before the cutover committed, the
  child was silently hosted via the ordinary path instead — empty engine,
  and (because ordinary non-initial-formation hosting deliberately excludes
  self from the voter list, for the "join an already-led group as a quiet
  non-voter" case) a **different, wrong voter config per replica** — a
  scenario that reads as pure success (no error, no panic) until you notice
  the group can no longer elect a leader and every write it took is gone.
  `SimEnv` corpora for the fork and for the reconciler each passed in
  isolation because each drives only one of the two loops; the bug only
  exists in the gap *between* two loops that a `ProdEnv` end-to-end test with
  a real paced writer across the fork→cutover window could show. Fixed
  entirely on the slow side — no protocol/replicated-command change: the
  reconciler fast-polls (50ms) for as long as any tablet cluster-wide carries
  an unresolved split intent (a fact every fork participant observes
  identically, well before Stage 3 applies, so all replicas speed up
  together), and the driver additionally requires a small local
  materialization-settle margin *and* direct confirmation via its own
  `hosted_groups()` before ever proposing cutover. **General rule**: when two
  loops key off "the same underlying fact changed" but one of them learns it
  from a slow/optional signal (a poll fallback, an absent notification) and
  the other from a fast/mandatory one, audit what the fast side can delete or
  invalidate before the slow side ever gets a chance to look — and prove it
  with a real-clock test that races them, since a single-loop sim corpus
  cannot expose an inter-loop race by construction.

- **A historical bench figure from a different host is not a baseline —
  rerun it alongside the new number, on the same host, in the same
  session (ADR 0058's rung-8-vs-Train-2 bench pass, 2026-08-25).** Asked
  to compare a new in-place split bench against ADR 0050 rung 8's
  documented 458ms/2,000-row copy-based figure, the honest move was to
  rerun the OLD bench too rather than diff a new number against an old
  one measured on unknown, unrelated hardware — this session's own host
  produced a copy-based blip of ~300ms (not 458ms) with a rock-steady
  ~108ms idle-read floor across every one of 6 runs, meaning the
  absolute numbers here are simply not comparable to the ADR's own
  historical figure, only to each other. Doing this also surfaced a
  result worth having in hand precisely because it wasn't flattering:
  the new (in-place) path's total wall clock was ~1.8x faster, as
  predicted, but its own write blip was ~2.4x *worse* than the
  same-host copy-based number, with zero retries needed on any run —
  a single un-retried request was simply slow, a different failure
  shape than the copy-based path's fast-refuse-and-retry blip, and one
  the design doc's own "near-zero" framing hadn't anticipated. **General
  rule**: when a task asks you to compare against a number from another
  session/host/PR, treat that number as an anecdote until you've
  reproduced it (or its equivalent) yourself under the same run — and
  report an unflattering same-host result exactly as measured rather
  than reframing it around the more flattering historical figure.

- **A one-shot admin action whose every refusal reason maps to the same HTTP
  status is not itself proof of progress — a test (or a caller) must
  distinguish "armed and in flight" from "failed outright" some other way,
  or retry on the status alone (issue #405).** `tests/
  heartbeat_live_destinations.rs::
  heartbeat_reaches_a_runtime_added_voter_after_it_becomes_leader` called
  `POST /admin/control/member/remove` (`ClientCtx::
  admin_remove_control_member`'s leader-self-removal branch) **once**,
  asserted `status == 409`, and then separately polled the new voter's own
  `is_control_leader()` for up to 15s. But `RaftCore::transfer_leadership`
  only arms if the target's `peer_match` has already caught up to
  `commit_index` **at that exact instant** (its own doc: deliberately no
  "eventually" — a caller that needs that is expected to re-arm every tick,
  which this one-shot admin action does not), and the control group's own
  background churn (the failure detector's liveness `UpsertMember` for the
  freshly-added voter, `reconcile_loop`'s placement bookkeeping) can advance
  `commit_index` past the runtime-added voter's replicated position in the
  narrow window between the test's own voter-set convergence check and this
  call — a window real machine contention widens. Every refusal this action
  can return — "could not arm," "armed but the target never finished
  stepping up within its own internal poll," "already not the leader" —
  maps identically to HTTP 409 (`admin.rs::action_remove_control_member`
  converts every `Err` the same way), so a single 409 says nothing about
  which of those happened: pre-fix, a "failed to arm" 409 (plausible under
  load, since arming needs the target's replication caught up at that exact
  instant) left the test's own 15s poll waiting on a transfer that was never
  actually driving forward — no code anywhere was going to retry the arm,
  since this admin action deliberately does not retry itself (a human
  operator does, per its own error text, which literally says "retry").
  **General rule**: when a test (or any caller) issues a single mutating
  admin call and then polls for a *separate* side effect, check whether that
  admin call's own documented contract is "fire once, the effect either
  starts or the response tells you to retry" — if so, the poll must retry
  the **call itself**, not just wait on the side effect, or a single failed
  attempt strands the poll for its entire budget with nothing left able to
  produce the effect. Fixed by folding the single call + separate poll into
  one loop that checks the side effect first and re-issues the admin call
  (tolerating repeated 409s) until it succeeds — safe here specifically
  because a re-arm of an already-armed *same* target is a documented no-op
  that doesn't reset the transfer deadline, so a retry can never make a
  transfer already succeeding worse. (`crates/animusd/tests/
  heartbeat_live_destinations.rs`, `crates/animusd/src/lib.rs::ClientCtx::
  admin_remove_control_member`, `crates/animus-control/src/raft.rs::
  RaftCore::transfer_leadership`.)

- **A converged-or-timeout poll on one read path does not prove convergence
  on a *different* read path over the same data — check which consistency
  contract each assertion actually rides before trusting an earlier poll's
  green result (issue #400).** `tests/update_table_drop_index.rs::
  create_drop_recreate_same_index_name_backfills_from_scratch` polled
  `await_row_count` (the raw client protocol's linearizable `Scan`,
  `stale: false`, direct against the recreated GSI's hidden table) to
  convergence, then immediately followed it with a **fixed, one-shot**
  per-row `Query` through the real DynamoDB wire — `assert!` on the very
  first response, no poll at all. That `Query` can never request
  `ConsistentRead: true` against a GSI (ADR 0041 §5 — a `ValidationException`),
  so it is unconditionally served by ADR 0055's eventually-consistent,
  replica-local path: the linearizable Scan proving the row durably present
  says nothing about whether the *specific replica this Query happens to
  land on* has caught up yet, and under real load (slower replica apply) it
  routinely hadn't — the CI failure shape was `Count:0` reading back a row
  the immediately-prior linearizable Scan had just confirmed present.
  **General rule**: when a test chains two assertions against what looks
  like "the same data," don't assume the second inherits the first's
  convergence — trace which consistency mode each request actually rides
  (a raw client-protocol call vs. the DynamoDB wire's `ConsistentRead`
  default, and whether the operation in question can even ask for the
  strong mode at all) and give **every** eventually-consistent assertion its
  own converged-or-timeout poll, never borrow one poll's outcome for a
  later, differently-served read. Fixed by wrapping each per-row `Query` in
  a poll across every node's dynamo address (`await_gsi_query`/
  `await_gsi_query_everywhere`, duplicated from `dynamo_query_pagination.rs`
  per this file's own "sibling test modules keep their own fixtures
  independent" convention — see that file's identical helpers for the
  general pattern any GSI-`Query`-asserting test should follow).
  (`crates/animusd/tests/update_table_drop_index.rs`.)
- **A rare, load-sensitive flake needs a matched A/B, not a bigger N on one
  side.** "6/6 clean on base, 1/3 failing on the branch" from small samples
  on a shared, variably-loaded machine is exactly the pattern ambient
  contention produces; the discriminating test is running the same stress
  harness (e.g. 4 parallel copies of the compiled test binary) back-to-back
  on both trees and comparing rates. Concrete case: `animusd`'s
  `dynamo_query_pagination::final_page_carries_no_last_evaluated_key`
  looked train-caused at small N, then reproduced at ~0.6% on both trees
  under the matched harness — the real mechanism was the ADR 0055
  `ConsistentRead: false` replica-local read racing the last write's apply,
  i.e. a pre-existing race, not the refactor under suspicion.
- **`cargo test`'s printed `Executable tests/foo.rs (/path/foo-HASH)` line
  is the only reliable way to name the binary that matches current
  source** — the deps-dir filename hash derives from package id/deps/
  profile, not source content, so the same hash is silently overwritten on
  rebuild; hardcoding a previously-seen hash in a bisection/stress script
  can run stale code with zero indication.

- **`prop_assume!` with a coin-flip-odds filter is a time bomb the moment
  anyone raises the case count — generate the dependent value directly
  instead (ADR 0061 Phase A, `next_compaction_plan` trigger-floor
  properties).** Two property tests wrote `l0 in 0usize..20, trigger in
  1usize..20` then `prop_assume!(l0 < trigger)` (and the mirror-image `>=`)
  to test the trigger floor from both sides. That passed at proptest's
  default case count (256) but aborted with "Too many global rejects" the
  moment case count was bumped for a manual stress run
  (`PROPTEST_CASES=3000`): proptest's global-reject ceiling is a **fixed
  constant (1024)**, not scaled to the requested case count, so a ~50%
  rejection rate needs roughly 2× that many attempts and blows the ceiling
  well before reaching a few thousand successes — completely independent of
  whether the property itself is fine. **General rule**: when a generated
  input has a companion input that must be above/below/equal to it, generate
  the dependent one directly off the first (`(1usize..20).prop_flat_map(|t|
  (Just(t), 0..t))` for "below", `(Just(t), t..t + N)` for "at or above")
  rather than generating both independently and filtering with
  `prop_assume!` — it also produces a better test (every attempt is a real
  case, not a discard) and stays correct if someone later runs the corpus at
  a deeper case count, which per this file's own "test-scaling knobs" table
  is exactly the kind of thing this repo's nightly/deep tiers do.
  (`crates/animus-storage/src/lsm.rs::compaction_policy_tests`.)

- **`prop_assert!`/`prop_assert_eq!`'s message argument goes through
  `concat!` internally, so it cannot implicitly capture identifiers the way
  a normal `format!("{x:?}")` can** — `prop_assert_eq!(got, want,
  "compare_numeric({a:?}, {b:?})")` fails to compile with "there is no
  argument named `a`" even though `a` is a real local in scope, because
  proptest's macro expansion routes the string through `concat!` before it
  ever reaches `format_args!`. Pass captured values as explicit trailing
  positional arguments instead (`"compare_numeric({:?}, {:?})", a, b`) —
  this is proptest-macro-specific, not a general Rust `format!` limitation,
  so it only bites inside `proptest!{ .. }` bodies. (ADR 0061 rung A5,
  `crates/animus-dynamo/src/condition.rs::decimal_differential_tests`.)

- **Extracting a `&self` method's body into a pure free function can strand
  a second, now-orphaned `&self`-taking helper as dead code, even though
  nothing about *it* changed** (ADR 0061 rung A3). `LsmEngine::
  next_compaction` called `self.level_table_budget(level)`; pulling
  `next_compaction`'s body out into a free `next_compaction_plan(tables,
  opts)` meant the new free function now calls a *new* free
  `level_table_budget(level, opts)` directly — and the old `&self` method
  wrapper, never called from anywhere else, became a silent `dead_code`
  warning (which is `-D warnings` under this repo's clippy gate, so it's a
  build failure, not just noise). Caught by running the crate's own
  `cargo clippy --all-targets` before committing, not by the extraction
  itself. **General rule for this class of refactor** (the same shape ADR
  0061's A6 keystone rung is about to do at much larger scale): after
  pulling a method's logic out into a free function, `grep` every sibling
  helper the old method used — a helper with exactly one caller doesn't
  survive the caller's disappearance.

- **A differential proptest against an arbitrary-precision reference type
  must compare parsed *values*, not rendered text** (ADR 0061 rung A5,
  `add_numeric`/`compare_numeric` vs. `bigdecimal::BigDecimal`). This
  crate's decimal arithmetic normalizes trailing zeros and `-0` (`"1.10" +
  "0.90"` renders `"2"`, never `"2.00"`), and that normalization is a
  deliberate, tested behavior, not a bug — so asserting on the reference's
  *string* output would fail on exactly the inputs the test most needs to
  cover. `BigDecimal`'s `PartialEq` is itself scale-normalizing (`4 ==
  4.00`), so parsing this crate's result string back into the reference type
  and comparing `BigDecimal == BigDecimal` sidesteps the whole issue for
  free — no reference-side normalization step needed. Anyone reaching for a
  reference-implementation differential test against a bignum/decimal type
  should check whether the reference's equality is value- or
  representation-based before writing the assertion, not after a spurious
  failure sends them chasing a phantom bug in the code under test.

- **A "cheap to clone, clones share state" test double handed to code that
  models independent replicas silently collapses replica independence —
  and the symptom looks exactly like a production consensus/tracking bug
  (issue #488, `crates/animus-test/tests/txn_serializable.rs`).**
  `Topology::start` built **one `MemoryEngine` per tablet group and
  `.clone()`d it into all 3 replica `RaftKvNode`s**; `MemoryEngine`'s own
  doc comment says outright "cheap to clone; clones share state"
  (`Arc<Mutex<Inner>>`), so the corpus's "3 independent replicas" secretly
  read and wrote one physical store. Only the Raft layer (log, term,
  leadership) and each replica's own in-memory `TxnTracker` were genuinely
  per-replica. Consequence: whichever replica's apply task happened to run
  first for a log index durably wrote the shared engine, so a *different*
  replica's own, separately-sequenced `apply_and_compact` call for the
  identical index could read back a status its own log processing hadn't
  actually decided yet — silently steering it into an already-decided/
  idempotent-replay no-op branch that (correctly, for a genuine replay)
  skips the real `Pending -> Committed` transition and the `TxnTracker`
  update that transition performs. When the two replicas that happened to
  take that no-op path were exactly the two that survived a leader kill,
  neither had a populated `TxnTracker::unresolved_decided` for the
  transaction, so the resolver's proactive re-propose never fired and a
  `KIND_LSI` derived row (materialized only inside a genuine local
  `TxnResolve` apply, never re-derived from engine state) was orphaned
  forever — a permanent, reproducible-at-depth divergence that looked
  exactly like a leader-only/liveness gap in `animus-cp-data`'s apply
  pipeline, and cost a full investigation to clear the production code
  before the harness was even suspected. Fix: one `MemoryEngine::new()`
  **per replica**, matching the sibling `raftkv_linearizable.rs` corpus's
  `Group::start` (`factory(&sim, id)` per node id), which never shared an
  engine across replicas. **General rule**: before handing the same
  instance of a "cheap to clone" test double (an `Arc`-backed fake store,
  an `Rc<RefCell<_>>` counter, anything whose `Clone` impl is documented or
  obviously implemented as a shared-handle copy) to code meant to model N
  independent participants, verify the sharing is what you actually
  intend — a fixture that looks like "one engine per group, replicated
  normally" reads as correct at a glance and only misbehaves under real
  timing skew between replicas' own apply rates, exactly the kind of thing
  fault-injection depth (not the default-depth corpus run) is what
  actually exposes it.

### Code patterns
- **A retryable-shaped error (the house `"; retry"` suffix) surviving string
  formatting into a caller's own error type is not the same guarantee as
  something in that caller's *call chain* actually checking for it — audit
  every hop between the throw site and the nearest retry loop, not just the
  throw site's own wrapping (issue #412).** `dynamo::kind_write_item_at_
  leader`'s leader-side old-image read failure (`ClientCtx::
  cp_get_local_resolving`, e.g. a "CP group leader moved; retry" leadership
  race) already produced a message ending `"; retry"` even before this
  fix — `internal(&format!("leader-side old-image read failed: {e}"))`
  places the interpolated error last, so the suffix survived by construction
  — and `ClientCtx::cp_kind_write_item`'s own retry loop (`read_should_
  retry`, suffix-matched) already retried it correctly. The *identical*
  wrapping in `eval_kind_txn_write` (the stage-time evaluator
  `TransactWriteItems` uses) produced an equally well-shaped
  `TxnAbortReason::Other(msg)` — but `ClientCtx::txn_prepare_pushing`'s own
  bounded retry loop only pattern-matched `StageOutcome::IntentBlocked`
  *inside* an `Ok` result; a stage attempt that failed **before ever
  reaching propose** (this read failure, or the identical leadership race in
  `leader.txn_stage`/`txn_stage_participant` returning `None`) came back as
  an `Err` from `txn_prepare` and escaped the whole loop via `?` on the very
  first attempt, regardless of how retryable-shaped its message was — no
  code anywhere in that path ever called `.ends_with("; retry")` on it. It
  then surfaced through `cp_txn`/`dynamo::run_transact`'s `TxnAbortReason::
  Other` arm as a terminal `TransactionCanceledException` for a condition
  the very next attempt would routinely have cleared. **The generalizable
  check**: when auditing "does X get retried," don't stop at confirming the
  error *carries* the retryable marker — trace every intermediate `?`/
  `map_err` between the throw site and the nearest loop that actually
  branches on that marker, since a marker that nothing reads is
  indistinguishable, at the call site, from one that was never set. Fixed by
  making `txn_prepare_pushing` catch `Err(TxnAbortReason::Other(msg)) if
  msg.ends_with("; retry")` from `txn_prepare` itself and retry the stage
  attempt exactly like `IntentBlocked` (safe because nothing was proposed
  yet on that attempt, and re-invoking `txn_prepare` re-resolves `cp_route`
  fresh each time — the same "safe to retry, re-route every attempt"
  discipline the ordinary write path already has). Regression:
  `issue_412_tests::txn_prepare_pushing_retries_a_leader_moved_read_
  failure_to_success` (`dynamo::leader_read_failure_gate`, a deterministic
  fault-injection hook mirroring `dynamo::rmw285_confirm_gate`'s idiom,
  used since orchestrating a real leadership change mid-read is not
  reliably reproducible on demand). (`crates/animusd/src/dynamo.rs`,
  `crates/animusd/src/lib.rs::ClientCtx::txn_prepare_pushing`.)

- **A convergent bookkeeping write must be routable to its own owner: derive
  its key from the owner's actual scope, not a normalized/truncated form of
  it.** Issue #355's root cause (see the Testing entry above for the full
  incident): `cursor::cursor_key` derived a cursor row's key by truncating
  the writing tablet's own `range.start` to a fixed-width token, on the
  assumption that "this tablet's own token" was a safe stand-in for "a key
  inside this tablet's own declared range." That assumption held only by
  coincidence — a hash-ring token happens to be a real byte prefix of a
  tablet's range only when the range boundary is itself token-aligned, which
  a real split boundary (chosen from row content) essentially never is. Any
  write that reaches a codebase's own key-based routing layer (here,
  `ClientCtx::cp_kind_write_raw`'s `cp_route`, which resolves a target purely
  from the write's own key bytes against each candidate's declared range)
  needs a key that is *actually, structurally* inside its intended owner's
  range — not merely "derived from" that owner in some way that seems close
  enough. The fix embeds the owner's real range boundary verbatim (with a
  trailing length so a fixed-width parse can still recover what follows it)
  instead of a lossy summary of it. The general check: before writing a
  bookkeeping/cursor/marker key that will be routed by content rather than
  written directly to an already-resolved handle, ask whether the key is
  provably inside the target's own bounds by construction, or only usually
  inside them for shapes a test happened to try.
- **Deleting a seam: grep the *verb* as well as the *noun*, then grep the
  prose.** Removing the `ReplicationMode` seam (ADR 0019's 2026-08-23
  amendment) had an obvious target list — the enum, the `TableSchema::mode`
  field, the `with_mode` builder, the `Metadata::table_mode` accessor — and
  that list was **incomplete**. A replicated-catalog field's *write* path in
  this codebase is a separate `MetaCommand` variant, not the struct field:
  `MetaCommand::SetTableMode` only surfaced from grepping the accessor and
  builder names, never from grepping the type. The general rule: for anything
  that lives in replicated `Metadata`, the removal set is
  `{type, field, builder, accessor, the MetaCommand variant that mutates it,
  every gating match site that variant appears in}` — and this repo's own
  standing lesson about `is_relayable_command`/`cp_serve_forwarded` allowlists
  is the reason the last item matters.

  The second half is compiler-invisible and therefore easier to miss: deleting
  a subsystem strands **documentation** pointing at it, and nothing fails the
  build. After this removal, `raftkv_linearizable.rs`'s module doc still opened
  with "This is the CP counterpart of the Accord corpus in `corpus.rs`" — a
  file deleted in the same change — and `hlc.rs` still told readers to "see
  `crates/animus-consensus/src/node.rs`'s `mvcc_version` doc," a path that no
  longer exists. Both passed fmt, clippy `-D warnings`, build and the full test
  suite. So after the code gate is green, grep the deleted names across `*.rs`
  doc comments, `CLAUDE.md` files and ADRs as a separate pass, and decide per
  hit: **re-anchor** it (a comparison that stood on its own — "the same poll
  every corpus uses"), or **mark it historical** where the lineage is the point
  ("unlike the `(logical, node)` encoding the deleted `animus-consensus`
  used"). What you must not leave is a live-voice pointer to a path or file
  that is gone.
- **A numeric-vs-byte comparison fix applied to one predicate over a type
  doesn't automatically reach a sibling predicate over the same type
  (2026-08-24, issue #373, `animus-dynamo::condition`).** This crate already
  had the *correct* pattern for DynamoDB `N`: `compare_values`/`compare_numeric`
  compare number text by decimal magnitude, with a doc comment explicitly
  contrasting that against `AttributeValue::key_bytes`'s lexicographic
  ordering (a documented simplification of on-disk key order, not something a
  filter should inherit). But `SortKeyCondition::matches` — a second, older
  predicate over the exact same `N` values, for `Query`'s `sk BETWEEN`/`sk =`
  — had never been switched over, and kept comparing raw `key_bytes`
  directly: `sk BETWEEN 5 AND 15` excluded `sk = 9` because `"15" < "9"` as
  text. The general check: when a type has more than one comparison/equality
  call site in a crate (here, `ConditionExpression`'s comparators and
  `SortKeyCondition`'s own), a numeric-ordering fix to one is a signal to grep
  every other site over the same type for the identical byte-vs-numeric
  divergence, not just extend the one you're already touching — the existing
  doc comment even named the divergence (`AttributeValue::key_bytes`'s
  lexicographic number order) but that cross-reference apparently never got
  checked against every reader of `key_bytes`, only the one being written at
  the time.
- **A unit-level fix to a pure predicate proves nothing about its production
  call sites if a caller reconstructs the predicate's input in a way the
  predicate's own type dispatch can't see through — always trace the actual
  value a real caller hands the fixed function, not just the type it accepts
  in principle (2026-08-24, issue #373 follow-up, `animus-dynamo::condition`
  / `animusd::dynamo`).** The entry above fixed `SortKeyCondition::matches`
  to compare `N` numerically once *both* sides are literally the `N`
  variant — and its own unit tests, which construct both sides as
  `AttributeValue::N`, genuinely proved that. But every production caller
  (`run_base_query`/`run_gsi_query`/`run_lsi_query` in `animusd`, and this
  crate's own `Table::query_with`) held only a scanned key's **raw bytes**,
  with no type tag, and wrapped them as `AttributeValue::B` before calling
  `matches` — so the numeric arm's `(N, N)` pattern match never fired at any
  real call site, even after the "fix" landed: `sort_key_cmp` fell through to
  `a.key_bytes().cmp(&b.key_bytes())`, which for a `B`-wrapped raw-bytes
  value is byte-identical to the *unfixed* behavior, since a `N`'s raw
  stored bytes are literally its own decimal text. Confirmed empirically
  (not just by code reading) with a throwaway `#[test]` calling `matches`
  once with the value typed `N` and once with the identical bytes wrapped
  `B`: the two calls returned different answers for the exact same logical
  comparison. The general check: after fixing a comparison predicate that
  dispatches on an enum variant (here, `AttributeValue`'s `N` vs `B`), grep
  every production call site and ask "does this caller actually have a
  value of the variant my fix's fast path checks for, or does it have raw
  bytes / a different representation that only happens to satisfy the type
  the function *accepts*?" — a function accepting `&AttributeValue` gives no
  static guarantee the caller passes the semantically-correct variant, and a
  fix's own unit tests, if they construct inputs "the right way" rather than
  the way production actually does, can pass while production stays broken.
  Fixed by adding `SortKeyCondition::matches_raw(&self, raw_bytes: &[u8])`,
  which reinterprets raw bytes as the condition's own declared operand type
  before delegating to `matches`, and switching every raw-bytes call site to
  it — so the type-correct reconstruction happens once, in the one place
  that knows the rule, instead of being (mis)implemented ad hoc at each
  call site. (`crates/animus-dynamo/src/condition.rs`,
  `crates/animus-dynamo/src/lib.rs`, `crates/animusd/src/dynamo.rs`.)
- **A pagination cursor that echoes back a superset of another cursor's
  attributes needs an *exact*-match validation, not a "the attributes I
  need are present" check (2026-08-22, DynamoDB `Query` pagination).**
  `animusd::dynamo`'s base/GSI/LSI `Query`/`Scan` cursors are all real
  `Item`s built by `key_item_of`/`gsi_key_item_of`/`lsi_key_item_of` — and a
  GSI or LSI cursor *always* carries the base table's own key attributes
  too (real DynamoDB's `LastEvaluatedKey` needs them for uniqueness). That
  means a base-table cursor's attribute set is a **subset** of every index
  cursor's set on the same table. A resume-key resolver that only checks
  "does this `Item` have the attributes I need" (the shape `resolve_key`/
  `gsi_resume_key`/`lsi_resume_key` already had, since they only ever read
  the keys they need and ignore the rest) will *silently accept* a GSI or
  LSI cursor replayed against the base table — it has `pk`/`sk` and then
  some, and "then some" is invisible to a presence check. The fix is a
  dedicated exact-set-equality check (`validate_query_cursor_shape`, an
  `Item`'s key names compared as a `BTreeSet` against the target's expected
  set) run *before* the lenient resolver, on every pagination path. The
  general form: whenever cursor/token shapes for related-but-distinct
  operations nest inside each other (a wider shape strictly containing a
  narrower one), presence-checking the narrower shape's fields is not
  enough to reject the wider shape — verify the field *set*, not just that
  the fields you need happen to be there. (`crates/animusd/src/dynamo.rs`.)
- **A single-closure injection seam that needs to grow into several
  fallible, parameterized operations should become a small trait, not a
  pile of more closures — but the widening must be audited to stay in
  *shape* only, never in *kind* (2026-08-20, ADR 0052 PR3, the Data
  Console's Config tab).** PR2 gave `console.rs` a `TableSnapshotFn`
  (`Arc<dyn Fn() -> Vec<TableSummary>>`) as its one seam into `lib.rs`'s
  cluster-aware world — exactly right for one parameterless, infallible
  read. PR3 needed six more operations (a per-table detail read plus five
  mutations), each needing a table name/request body and able to fail.
  Bolting five more `Arc<dyn Fn...>` fields onto `serve`'s signature would
  have worked mechanically but obscured the one property that actually
  matters here: that every operation's signature is still built only from
  plain owned types the seam itself declares, never the richer type the
  other side of the boundary actually has in hand. An `async_trait` trait
  (`ConsoleBackend`) makes that property easy to see and easy to keep
  honest at every call site — one `impl` block, one place to check that no
  method accepts or returns a cluster/schema-catalog type — where five
  separate closures would have made the same audit five separate
  fly-by-eye checks. The general form: when a seam must grow, prefer
  promoting it to a trait over multiplying its closures, but the reason to
  prefer the trait is auditability of the type boundary, not the trait
  keyword itself — a trait whose methods leak the richer type back through
  is no safer than the closures would have been. (`crates/animusd/src/
  console.rs`, `crates/animusd/src/lib.rs`.)
- **A wire adapter's `UpdateTable`-add-a-GSI decode path can silently
  ignore attribute types even though the equivalent `CreateTable` path
  requires them — check what a decoder actually reads before assuming its
  request shape mirrors its sibling operation's (2026-08-20).** This
  adapter's `GlobalSecondaryIndexUpdates` `Create` decoding
  (`animus_dynamo::wire::decode_index_updates` → `decode_index_entry`)
  reads `KeySchema` straight off the `Create` object itself and never looks
  at a top-level `AttributeDefinitions` at all — unlike `CreateTable`,
  where `AttributeDefinitions` feeds the base table's own `ColumnDef`
  types. A new GSI's hash/sort attribute therefore gets no explicit type
  recorded anywhere in the catalog; `IndexDef` stores only the attribute
  *name*. Before building a feature on top of an existing wire operation,
  read what its decoder actually consumes; a sibling operation's contract is
  not evidence the one you're calling shares it. **And when the gap means a
  UI control's value cannot survive its own round trip, remove the control —
  do not paper over it with a default.** The Config tab's Add-GSI form
  originally offered an `S`/`N`/`B` picker per key attribute; the pick was
  accepted with a `200 OK` and read straight back as `S`, so the screen
  contradicted itself within one interaction. Defaulting the *display* to
  `"S"` would have hidden that, which is worse than the bug: an invented
  value is indistinguishable from a recorded one. The fix was to delete the
  picker, stop sending the `AttributeDefinitions` the decoder ignores, and
  make the type explicitly nullable end-to-end (`console::IndexKeySummary`'s
  `Option<String>`, rendered as a bare attribute name) so the absence is
  visible rather than filled in. The decoder gap itself is issue #319 — an
  incidental pre-existing bug, so its own change with its own test, per the
  repo convention. General form: a fallback default is only honest when the
  fallback is unreachable in practice; where it *is* reachable, model the
  absence. (`crates/animus-dynamo/src/wire.rs`, `crates/animusd/src/
  console.{rs,js}`.)
  **Follow-up (2026-08-20): the ADR text this same PR wrote asserted the
  *sibling* `CreateTable` path did not share this gap — that assertion was
  wrong, and nobody had traced it to find out.** The Config tab's own ADR
  amendment claimed "a GSI declared at `CreateTable` time on the same table
  [gets a type]"; the create-table-form PR (PR6) actually traced
  `schema::to_control`/`index_to_control` before believing it, and found
  the identical gap: `to_control` only builds a `ColumnDef` for the base
  table's own partition/sort key, and `index_to_control` never receives
  `key_types` for *any* index, `CreateTable`-declared or not. The lesson
  generalizes past this one decoder: **an unverified claim about a sibling
  code path, once written into an ADR or a doc comment, is exactly as
  trustworthy as an unverified assumption — restating it in prose does not
  make it checked.** A task that says "verify against the decoder, don't
  assume it behaves like its sibling" applies even when the thing you'd be
  trusting is this repo's own prior documentation of that sibling. Trace
  the actual code for *every* new call site that offers a control backed by
  it, even one an earlier PR's ADR text already described with apparent
  confidence.
- **A "claim now, confirm later" state machine needs a release on *every*
  failure exit of the executor, not just the one the original design happened
  to handle (2026-08-19).** `animus-cp-data`'s tablet-host reconciler splits a
  pure `plan()` from an async executor: `plan` inserts a tablet into
  `LocalState::hosted` the instant it decides to emit `HostAction::Host`, and
  `tick` commits that state *before* running the actions. The teardown half of
  the discipline was built correctly and documented at length — a
  `Reclaim`/`Release` claim survives until the executor calls
  `confirm_torn_down`, so a timed-out driver shutdown is simply re-planned next
  tick. The host half had no such release: `host()`'s two early returns (the
  tablet gone from `Metadata`, or `EngineFactory::open` failing on real disk
  I/O) established no live handle and undid no claim, so `plan`'s own
  idempotence gate (`!next.hosted.contains(&tablet)`) then swallowed the
  tablet **permanently** — a phantom replica, degraded RF, no operator signal
  beyond one `warn!`, recoverable only by restarting the process. The doc
  comment two lines above the failure asserted the opposite ("`plan` re-emits
  it next tick"), which is the tell: prose describing a recovery path is not
  evidence the path exists. It survived because the sim-only `EngineFactory`
  always returned `Ok`, so no test could reach the branch at all — a fallible
  seam whose test double cannot fail is an untested seam. The mirror hole sat
  in `teardown()`, which returned early when no live handle existed without
  confirming, so a zombie claim re-planned its teardown forever. **The general
  check**: for any optimistic claim a pure planner takes ahead of an async
  executor, enumerate *every* way the executor can fail to land the action and
  confirm each one either completes the claim or releases it — and give the
  fault a test double that can actually inject the failure.
  (`crates/animus-cp-data/src/host.rs::{plan, Reconciler::host,
  Reconciler::teardown, LocalState::release_unconfirmed_host}`,
  `tests/reconciler.rs::reconciler_recovers_a_tablet_after_a_transient_engine_open_failure`.)
- **An engine precondition that was "impossible for a well-behaved caller"
  stops being impossible the moment the caller is a client — match the error
  variant, don't `.expect()` it (2026-08-19).** `animusd`'s admin
  `GET /admin/system-table` builds its scan lower bound by concatenating the
  client's `after` cursor (unvalidated base64url) with a `0x00` suffix, then
  scanned with `.expect("system-keyspace engine scan")`. Both `LsmEngine::scan`
  and `MemoryEngine::scan` return `StorageError::InvalidRange` when
  `start > end`, and the reserved namespace's end bound is a short ASCII-only
  value — so any cursor decoding above it (e.g. base64url of `0xFF`) panicked
  the request task, while the sibling `kind` parameter three lines up already
  returned a clean 400 for the same class of hand-crafted input. Note the
  contrast that makes this a pattern and not a one-off: `RaftKvNode`'s own
  `local_scan` deliberately does `.scan(..).ok()`, swallowing `InvalidRange` as
  an empty result — the codebase had already decided how to treat this error at
  its other call sites, and this one endpoint hadn't followed. **The rule**:
  wherever a `StorageEngine` scan bound is *derived from* bytes that crossed a
  wire edge, treat `InvalidRange` as client input reaching a precondition, and
  match the specific variant so a genuine backend fault still fails loudly
  rather than being blanket-caught into a 400.
  (`crates/animusd/src/admin.rs::system_table`.)
- **A runtime `assert!` on a type-level trait constant is a reachability claim
  the compiler will not check for you — grep the impls (2026-08-19).**
  `animus-control`'s `RaftCore::encoded_wal_image` /
  `PersistedState::encode_snapshot_record_from_blob` existed to serialize
  `Metadata` once per compaction instead of twice, guarded by
  `assert!(!S::DRIVER_APPLIED, ..)`. ADR 0038 then made both real state
  machines in the workspace (`Metadata`, `KvState`) `DRIVER_APPLIED = true`,
  which quietly made the pair unreachable — yet nothing flagged it: the
  functions still compiled, were still `pub`, and still had a passing unit test
  that constructed its own toy implementor with the "wrong" constant. Two
  cheap detectors: `grep -rn "DRIVER_APPLIED"` across every `impl` (including
  test files) settles reachability faster than tracing call sites forward, and
  when a doc comment cites a specific guard test as evidence a mechanism is
  exercised, **check the test exists** — this one cited
  `wal_compaction.rs::encoded_image_matches_wal_image_encoding`, which had
  never existed anywhere in the repo. A cited-but-phantom test is worse than no
  citation: it buys a reader's trust for free.
- **A defect whose trigger is a microsecond-wide thread interleaving cannot
  be closed by a test — make it unrepresentable, and say so where the test
  would have been (issue #279, 2026-08-18).** Decoupling the CP-data
  consensus loop's WAL `fsync` from its `select` (so a slow disk stops
  livelocking a tablet group) means outbound vote grants / append accepts are
  buffered until the persist covering them lands. Two successive attempts
  shipped that and were reverted after failing the end-to-end gate; the
  measured root cause was the WAL's *second* drainer — the apply task's
  compaction rewrite, on another OS thread — taking `RaftCore::pending` in
  the window between a step releasing the core lock and the loop's next look
  at it. The loop then saw nothing left to persist, started no round, and the
  buffered ack sat undelivered for up to 10.1 s, stalling the leader's commit
  index. The instinct on the third attempt was "write the real-thread
  regression that catches it." That test does not exist: with the bug
  deliberately reintroduced, a 400-write `ProdEnv` run with compaction firing
  a dozen times stayed green, and so did a two-node variant where the single
  follower's ack is *required* for quorum — the window is simply too narrow
  to hit by load, which is also why it took production split-during-backfill
  traffic (many groups × constant compaction) to surface at all. What worked
  instead was two structural closures: (1) one shared `drain_for_round`
  helper with the round-claiming primitive private to the module, so a
  drainer *cannot* take records without numbering them — the bug made
  uncompilable; and (2) an unconditional `fully_durable` release (nothing
  pending and no round in flight ⇒ everything buffered is already on disk),
  which is correct no matter what any drainer did with round numbers. The
  general rule: when a race's window is narrower than any test's resolution,
  budget for making the state unrepresentable rather than for detecting it,
  and write down in the test file what it does *not* prove — a real-thread
  test that passes against the known bug is worse than no test, because the
  next reader will trust it. (`crates/animus-cp-data/src/persist_round.rs`,
  `crates/animus-cp-data/tests/prod_compaction_persist_round.rs`.)

- **A "safe because the other task is parked/blocked" precondition is a
  liability with a name — go find the ones written down (issue #279,
  2026-08-19).** Porting the WAL-persist decoupling to the control-plane
  driver, the third drainer turned out to be a *public* method,
  `RaftNode::flush`, whose own doc comment stated the hazard outright: "because
  the driver is parked at that point, this is the sole WAL writer." That
  sentence was accurate when written and false the moment persistence moved off
  the loop. The generalisable move: when making a synchronous step concurrent,
  grep the touched subsystem for prose asserting *why* something is currently
  safe — "parked", "blocked", "the only writer", "cannot interleave", "under
  this one lock hold" — and treat each hit as a precondition to re-derive, not
  as documentation to preserve. They are cheaper to find than the races they
  turn into, and unlike the invariants living only in control flow, someone
  already did the work of writing them down.

- **When a driver stops doing something synchronously, audit every other
  writer of the state it used to own exclusively — "safe because nothing
  else can observe it mid-flight" is a precondition, not a property (issue
  #279).** `apply_and_compact` discarding the consensus loop's un-persisted
  `RaftCore::pending` under `wal_lock` was correct and documented for as long
  as the loop drained inline: the loop could not be mid-anything, because it
  was blocked. The moment persistence moved off the loop, that same discard
  became a silent theft. Nothing about the compaction code changed or looked
  wrong in review — the invariant it rested on was in the *other* task's
  control flow. When making a synchronous step concurrent, enumerate the
  state it touched and find every other writer; each one is a place where an
  unstated "…while the loop is blocked" may be doing load-bearing work.

- **An index-shaped watermark cannot express the durability of a state change
  that moves no index (issue #279).** The natural way to release a buffered
  Raft response is "wait until `durable_index` covers it" — and it silently
  never fires for a granted vote, because a vote persists `(current_term,
  voted_for)` and appends no log entry, so `mark_durable_through` is never
  called. Worse, `drain_persist` marks the hard state persisted *at drain
  time*, optimistically, so no core-level predicate can see a vote-only
  round in flight either. Count the I/O (rounds), not the log positions, when
  what you need to know is "has this batch reached the disk."

- **A cross-crate deletion stack must be grouped by MECHANISM (producer
  symbol + every consumer + every test asserting the behavior), not by
  crate — a crate-scoped rung of one logical deletion is structurally
  incapable of staying green, at both gates that matter.** Removing tablet
  merge (ADR 0044, split-only tablets) was first planned as "PR1:
  animus-cp-data deletes the reconciler's Absorb/WidenScope, PR2:
  animus-control deletes `MergeTablets`, PR3: animusd/CLI deletes the wire
  surface" — a clean-looking crate-by-crate split that turned out to be
  unbuildable/untestable at every intermediate rung, for two independent
  reasons discovered in sequence:
  1. **Compile-time**: deleting `animus_cp_data::host::MetadataView`'s
     `merged`/`absorbed_by` fields is invisible to `cargo build -p
     animus-cp-data` (a pub field with no internal reader triggers no
     dead-code lint — the crate can't see whether some *other* crate reads
     it) and passes `cargo test -p animus-cp-data` fully green, then breaks
     `animusd::lib.rs`'s `MetadataView { merged: .., absorbed_by: .., .. }`
     struct literal with `E0560` the moment `cargo build --workspace` runs
     — the same failure shape the "grep every gating match site when adding
     a variant to a replicated/forwarded command enum" lesson (below)
     already warns about for *enum* variants, generalized to plain *struct
     fields*, and to *deletion* rather than addition.
  2. **Run-time, and much easier to miss**: even after fixing the struct
     literal so the workspace *builds*, `cargo test --workspace` still
     failed — on real end-to-end `animusd` integration tests
     (`tests/tablet_merge.rs` in full, one test in `tests/split_cluster.rs`,
     one in-crate test in `index_drain.rs`) that call the *admin/wire*
     surface (`POST /admin/tablet/merge`, still fully present and
     functional) to commit a real `MetaCommand::MergeTablets`, then assert
     on the *data-plane* consequence (`HostAction::WidenScope`/`Absorb`
     actually widening the survivor's scope) that PR1 had just deleted. The
     command's own admin/CLI/wire plumbing compiles and runs fine in
     isolation — it is a specific downstream *test's assertion*, not a
     symbol reference, that silently depends on a mechanism owned by a
     different crate. No `grep`-for-a-symbol technique catches this class;
     only actually running the test suite does, and the failure reads
     exactly like an unrelated flake (a `"key .. outside tablet's current
     range; retry"` or "expected the absorbed sibling's own row" panic)
     until traced back to the deleted mechanism.
  A `cargo build -p <this-crate>` / `cargo test -p <this-crate>` pair going
  green is evidence the crate's *own* logic is internally consistent —
  **it is not evidence the deletion is safe**, for either compile-time or
  run-time consumers, whenever the mechanism being deleted has callers
  outside the crate. **The fix**: plan (and land) a cross-crate deletion as
  one rung per *mechanism* — producer symbol, every downstream construction
  site, and every test anywhere in the workspace that asserts the deleted
  behavior, all in one change — not one rung per crate. `cargo build
  --workspace --all-targets` **and** `cargo test --workspace` are the two
  gates that actually prove it, and both must run (and their output
  actually read, not just "exit code 0 assumed") even when a task's stated
  scope is "one crate only" — a green `-p` run proves nothing about a
  sibling crate's literals or its test assertions. When a genuine
  intermediate red state seems unavoidable, that is itself the signal the
  rung boundary is wrong (regroup by mechanism), not a cost to accept and
  document around.
- **A rustdoc comment naming a deleted *file* or *function* by string is
  invisible to every compiler gate — `grep` for the deleted symbol's bare
  name, not just its `Rust`-identifier occurrences, after any deletion.**
  While deleting `MergeTablets` (ADR 0044 PR2), `grep -rn "MergeTablets\|
  merged_tablets\|absorbed_by"` cleanly found every real reference, but a
  separate case-insensitive sweep for the bare word `merge` turned up doc
  comments in `animusd/src/lib.rs` still naming `tests/tablet_merge.rs` (a
  file PR1 had already deleted) and describing a "merge crossover"/
  "merge-residue cursor-row cleanup" that a *different*, already-deleted
  function used to handle — none of it a compile error, since `rustc` never
  parses doc-comment prose for symbol references. The fix is procedural:
  after grepping for and fixing every exact-symbol match, do one more
  broad, case-insensitive grep for the deleted mechanism's plain-English
  name across every file actually touched (not the whole workspace, which
  turns up too many unrelated senses of a common word like "merge" — LSM
  merge, git merge, `StorageEngine::merge`) and read each hit for staleness,
  not just symbol presence.
- **A doc comment naming its own caller by name goes stale the moment that
  caller is deleted — even in a completely different crate, in a PR that
  never touches the file carrying the comment.** Deleting `MergeTablets`
  and its wire surface (PR2, `animusd::index_drain::
  cleanup_merge_residue_cursor_rows`) silently orphaned doc comments one
  crate away, in `animus-cp-data` — a crate PR2's own diff never touched —
  naming that exact function as "the caller" of `cursor::token_of` and
  `RaftKvNode::cursor_rows_with_token` (`cursor.rs`/`lib.rs`). Both
  primitives still compiled, still had a unit-test caller
  (`tests/cursor_scope.rs`), and their own crate's build/clippy/test gates
  all stayed green — nothing about deleting a caller in one crate makes a
  *different* crate's doc comment describing that caller fail any gate.
  **General check for any deletion PR: grep every file the deletion
  touches for "no longer exists" callers of its own, but also grep the
  *deleted symbol's own name* workspace-wide one more time after the PR
  — a hit outside the files you touched is a doc comment describing a
  caller that is now fiction, in a crate the deletion diff never had a
  reason to open.**
- **Derived numbering from "the highest currently-present entry" is only
  safe for an append-only collection — the moment anything in the system
  starts physically *removing* old entries, that derivation can silently
  collide.** ADR 0042/0043's stream-shard epoch (`seal_now`'s `next_epoch`,
  `dynamo_streams::current_open_epoch`) was designed as "chain length,"
  computed fresh each time from `stream_shards.range(..).next_back()` —
  correct for two full rounds of PRs (4/5/6) because nothing ever removed
  a row yet. The instant round-3 PR7 added retention (the *first* code
  path that physically deletes a `stream_shards` entry), this became a
  live hazard: reclaiming a tablet's own highest-epoch row would make the
  very next seal recompute the *identical* epoch for genuinely different
  data — two objects claiming the same identity at different points in
  time, with nothing to tell them apart. The fix is a narrow, explicit
  guard at the one call site that removes rows (`segment_janitor.rs`'s
  `may_remove_row`: never remove a tablet's current max epoch while the
  tablet still exists), not a redesign of the numbering scheme — but the
  general lesson is the one to carry forward: **whenever a later PR adds
  the first deletion/reclaim path over a collection some earlier, already-
  shipped code derives an identity or ordering from via "count/max/last of
  what currently exists," go back and re-audit every such derivation** —
  the earlier code was correct when written, and the later PR's own review
  has no reason to re-examine code it never touches, which is exactly how
  this class of bug survives review. Grep for `.next_back()`/`.count()`/
  `.len()` over the same collection a new deletion path touches as a
  starting point. (`crates/animusd/src/segment_janitor.rs`, ADR 0043 §A9,
  round-3 PR7, 2026-08-14.)
- **A "does this write need the old value" gate and the "does this write
  take the richer commit path" fast-path gate must be the *same*
  predicate, expressed once — not two conditions that happen to agree
  today.** Building ADR 0042's stream write-path gate (`kind_writes_for_item`'s
  `None` fast path widening from `indexes.is_empty()` to `!indexes.is_empty()
  || stream.is_some()`) surfaced a real, independent, pre-existing gap: the
  DynamoDB edge's `PutItem`/`DeleteItem` handlers computed their own
  `needs_old` (whether to pay for a pre-read of the item) from
  `condition.is_some() || return_values == ReturnValues::AllOld` alone —
  never from whether the write was actually about to route through the
  kind-write path. An unconditional replace/delete on an *already-indexed*
  table therefore silently skipped the read `kind_writes_for_item`'s own LSI
  diff needs (to remove a stale row when the alt-sort attribute changes),
  and — once streams could also pull a table onto that path — a stream's
  `OLD_IMAGE`/`NEW_AND_OLD_IMAGES` change record would just as silently miss
  its old image. `UpdateItem` and `BatchWriteItem`'s indexed branch had
  independently, correctly always read old — only the two write paths
  nobody had reason to touch since ADR 0041 shipped kept the narrower gate.
  The fix factors both call sites' predicate into one function
  (`table_takes_kind_write_path`) `kind_writes_for_item`'s own gate and every
  write handler's `needs_old` both call — so the two structurally cannot
  drift apart again. When a "do we need X" decision and a "does this path
  apply" decision are supposed to always agree, don't let them be two
  separately-maintained booleans; a passing test suite proves today's
  agreement, not tomorrow's. (`crates/animusd/src/dynamo.rs`, ADR 0042 PR A3,
  2026-08-14.) **Second confirmed instance, found wiring ADR 0049
  (2026-08-16): the drift survived the fix's own review round.**
  `BatchWriteItem`'s fast-path gate stayed `meta.table_indexes(table)
  .is_empty()` — written against ADR 0041 (indexes-only), never re-checked
  when ADR 0042 widened "takes the kind path" to include streams — so a
  streamed-but-unindexed table's batch writes bypassed the kind path
  entirely and its stream silently lost every one of them (no LSI existed
  to corrupt, so nothing else surfaced it; found only because ADR 0049's
  gate flip forced re-reading every gate site). The factored-predicate fix
  above only protects call sites that *call the shared function* — grep for
  raw re-derivations of the same condition (`table_indexes(`,
  `table_stream(`) whenever the shared predicate's meaning widens, because
  a site that never adopted the function is exactly the one no widening PR
  ever touches. Regression: `stream_write_path_tests::
  batch_write_on_a_streamed_table_emits_change_records` (red on the
  pre-0049 code).
- **A marker key built by truncating a tablet's own `range.start` to a fixed
  prefix is disjoint from real data (if it lives in its own kind scope) but
  is *not* thereby proven to stay within `[range.start, range.end)` —
  disjointness and containment are two different properties, and a `Vec<u8>
  ++ suffix` construction only gets you the first for free.** ADR 0042/0043's
  `KIND_CURSOR` cursor-row key (`animus-cp-data/src/cursor.rs`) mirrors
  `txn.rs`'s `record_key` scheme (`token(8 bytes) || [0x00, TAG] ||
  payload`) closely enough that the escape-disjointness proof transfers
  verbatim — but `txn.rs`'s token is always the anchor write's *own*,
  currently-being-written key (trivially in-range), while a cursor row's
  token is a *tablet boundary value*, truncated to a fixed 8 bytes. Working
  through whether `range.start[..8] ++ marker` can ever land at or past
  `range.end` surfaces a genuine, if narrow, edge case (a `Binary`
  partition key starting `0x00`, positioned exactly at a split boundary)
  that the byte-comparison math does not rule out in general. The
  house convention for this — `txn.rs`'s own "a residual, documented gap"
  note about `split_key` not being token-aligned — is the right response:
  state the gap explicitly in the code and defer it to a targeted corpus,
  rather than either (a) assuming a structurally-disjoint key is also a
  contained one, or (b) blocking a foundational PR on solving a rare edge
  case a later fault-injection corpus is better positioned to stress
  anyway. When adding any new marker/cursor key that must survive
  `narrow_scope`/`widen_scope`/`engine_image`'s live-range bound, ask
  disjointness and containment as two separate questions.
- **When one member of a family of sibling primitives lacks the family's
  implicit behavior, a caller written from the family's reputation gets a
  structural, permanent failure — check the specific primitive's contract,
  not its siblings'.** `ClientCtx`'s CP write-side primitives almost all
  auto-provision a table's first tablet on demand (`cp_put`,
  `cp_kind_write`, `cp_batch_write`, `cp_batch_write_patient`, `cp_txn`,
  the Dynamo edge's `quorum_write`) — but `cp_write` itself, the rawest of
  them, does **not**: every existing caller provisioned upstream, so the
  gap was invisible. The ADR 0041 GSI drain then wrote a *brand-new*
  table's rows (a GSI's hidden index table, which nothing upstream ever
  provisions) through `cp_write`, and the result wasn't slowness but
  *never*: `cp_route` waited out its full `CLIENT_TIMEOUT` on a table with
  no tablet, failed, and the next 200ms tick repeated it, forever — while
  reading exactly like the "first convergence is just slow" hypothesis the
  handoff note recorded. The fix (the drain provisions lazily, first tick
  with records to apply) matters less than the diagnostic: when a
  convergence loop makes zero progress ever, suspect a step whose
  precondition is *never* established, and check who was supposed to
  establish it. (`animusd/src/index_drain.rs::drain_tablet`, 2026-08-13.)
- **A field on a durable record shipped ahead of the feature that will read
  it back can be structurally present but semantically empty — the type
  checker cannot catch "nobody actually populated this for the case that
  matters," only a caller that greps every writer can.** ADR 0018's
  `TxnRecord::intent_spans` (`animus-cp-data/src/txn.rs`) shipped in PR3
  (single-participant transactions) computed purely from the anchor's own
  writes — sound at the time, since the anchor was the only participant
  that existed. PR4 added real multi-participant transactions but never
  revisited the field: a non-anchor stage passed `spans: Vec::new()`
  ("no local record is ever created here" — true, but irrelevant to
  whether the *anchor's* record should have known about this participant
  anyway). The field kept compiling, kept round-tripping through
  encode/decode, and kept *looking* like "the transaction's spans" right
  up until PR5 needed to actually walk every participant for recovery and
  found the anchor's own record had never heard of anyone else. This is
  the same shape as PR4's own `record_table` fix one PR earlier (a bare
  record key not carrying the routing info a *later* feature needed) — a
  recurring pattern worth naming: **when a staged delivery's early PR
  creates a durable record/marker type "to be filled in as the design
  grows," the PR that actually needs the fuller picture must grep every
  site that constructs the type, not trust the field's presence/type
  signature as evidence it was fully populated for every case that now
  exists.** The fix pattern is also identical both times: whoever has the
  complete picture *before* the type is ever constructed (a coordinator
  that already grouped every participant by table/tablet) hands the fuller
  data to the constructor explicitly, rather than the constructor trying
  to reconstruct it locally from information it structurally doesn't have.
  See `docs/adr/0018-cross-tablet-transactions.md`'s PR5 amendment §2 for
  the full account and the closing fix.
  **Update: fixing a gap like this one is worth a second pass asking "what
  if this record doesn't exist at all yet?"** — review of the fix above
  (a second reviewer, not the original implementer) immediately surfaced a
  further corner the fix itself didn't close: PR4's prepare phase stages
  participants *concurrently*, so a participant's own intent can be
  discovered by a reader while the record that would name it never gets
  created at all (the anchor's own stage can silently no-op on a
  fence/seal miss, exactly like a participant's already could — the same
  class of gap, just on the *other* side of the anchor/participant split).
  Any "read this record to decide what to do" path needs a **third**
  branch beyond "found, decided" / "found, pending" — **"not found at
  all"** — with its own safe decision (here: always abort, never commit,
  since committing needs a participant list only the record would have
  provided), *and* a symmetric guard against a **late arrival of the
  thing that would have created the record** overwriting whatever that
  third branch already decided (a "resurrection" hazard — the same
  first-decision-wins principle the original fix already established for
  *conflicting* decisions, extended to record *creation* itself). The
  general check to run whenever a fix makes some entity's *fields* more
  complete: does the fix's own precondition ("the entity exists") still
  hold in every case the system can reach, or did fixing the fields
  quietly assume creation is atomic with the read that discovers a need
  for it? See the PR5 amendment's §2b for the full closing fix and its
  regression test.
  **Update (2026-08-12, task #18): the fix above closed the *primitive*'s
  shape but nobody ever verified its *real caller* actually used it — for
  three subsequent PRs (PR5 through PR7), `ClientCtx::cp_txn` kept calling
  `RaftKvNode::txn_stage` (an empty participant list) instead of the new
  `txn_stage_anchor(.., participant_spans)` this very fix introduced,
  so every production multi-participant transaction's `intent_spans` still
  only ever named the anchor's own keys — a live, exploitable atomicity
  violation on the recovery path (a transaction whose participant never
  staged could be wrongly recovered as `Committed`), not merely the
  observability gap it looked like on paper.** Nobody caught this because
  every test that exercised recovery's participant-verification logic
  called `txn_stage_anchor` **directly**, by hand, with a real span list —
  proving the primitive, never the coordinator's wiring of it. And every
  test that *did* go through the real coordinator (`animusd/tests/
  cp_txn.rs`'s PR5 coordinator-crash pair) always staged every participant
  genuinely before letting recovery run, so the verification loop's
  incompleteness (checking a list that was silently too short) was never
  exercised against a case where the answer would have been wrong — the
  loop just never found anything to disagree about. **The generalizable
  lesson, sharper than the one above**: when a fix teaches "the caller must
  supply the fuller data" and changes a primitive's signature to accept
  it, that is necessary but not sufficient — a follow-up (ideally the same
  change) must grep the actual production call site and confirm it was
  updated to *pass* the fuller data, not just that a test constructing the
  call by hand now can. An ADR/CLAUDE.md sentence asserting "the
  coordinator already computes X and hands it to the stage call" is a
  claim about a specific call site, not about the type system — verify it
  by reading that exact function, especially when what depends on it is a
  correctness property (recovery's own atomicity), not a nice-to-have. See
  ADR 0018's own corrective note on this section for the full account, the
  wire-level test that reproduced the live failure, and the fix.
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
  cache looked like it *doubled* compaction cost — the blob serialize **plus** the
  WAL `Snapshot` record's own metadata serialize — so a follow-on optimization
  reused the cached bytes for the WAL too (`serde_json` `RawValue` embedding the
  pre-serialized image verbatim). That half never actually shipped live and was
  **deleted on 2026-08-19**: ADR 0038 made `Metadata` `DRIVER_APPLIED` before it
  saw production traffic, and such a state machine's WAL `Snapshot` record carries
  only a default placeholder (the real state lives in the engine), so there was no
  large field there to double-serialize in the first place. The caching half below
  is the part that mattered. Two morals: (1) the cache must be pinned to
  `snapshot_index`'s state, serialized **eagerly at snapshot time** (in-core
  `metadata` advances past the base between compactions, so lazy-at-ship would ship
  a state *ahead of* its claimed index → the follower double-applies its log tail);
  (2) **this hazard is invisible to `SimEnv`** (virtual time never trips the
  wall-clock election timeout) — the teeth is a wall-clock-timed transfer
  (`install_snapshot.rs::large_snapshot_ships_in_o_chunk_time_not_o_state`: fix ~ms
  vs regression ~46s), because a *live* `ProdEnv` cluster catch-up races
  leadership/AppendEntries and won't reliably traverse a long chunk-stream.
  (`animus-control` `raft.rs::snapshot_chunk_for`/`snapshot_upto`.)
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
- **A workflow triggered on both bare `push:` and `pull_request:` runs twice
  per commit on every same-repo PR branch, and every workflow wants an
  explicit `concurrency` group.** GitHub fires both events for a branch that
  lives in the repo the PR targets, so `ci.yml`'s two jobs (`gates` +
  `prod-liveness`) were being run twice over the same tree for an identical
  verdict — pure duplicate load on the 2-vCPU shared runners this repo is
  already timing-constrained by. Scope `push:` to `branches: [main]` and let
  `pull_request:` cover everything under review; the `main` runs are still
  worth keeping (the README badge reads them, and they cover a commit that
  reaches `main` without a PR). The tradeoff to know about: a branch pushed
  with no PR open now gets no CI until the PR exists. Separately, without a
  `concurrency:` block a push train leaves N runs racing on the same branch,
  so every workflow declares one: key on `${{ github.workflow }}-${{
  github.ref }}` (a `pull_request` event's `github.ref` is that PR's own
  `refs/pull/N/merge`, so this is already per-PR), and set
  `cancel-in-progress` per *event*, not per workflow — `${{
  github.event_name == 'pull_request' }}` cancels superseded PR runs while
  leaving `main` pushes to queue and each merged commit to get its own
  verdict. For a long scheduled job (`corpus-deep`) cancel nothing: a
  nightly that never reports is a silent gap in the record, so an on-demand
  dispatch queues behind it instead.
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
- **Superseded by ADR 0044** (tablet merge removed entirely —
  `Metadata::merged_tablets`, `HostAction::Absorb`/`MetadataView::merged`
  no longer exist). Archived verbatim in
  `docs/engineering-lessons-archive.md`'s "Superseded by ADR 0044" section;
  the still-general lesson: **distinguish two structurally-identical
  vanish-reasons with an explicit marker, never infer one from the
  remaining state.**
- **Superseded by ADR 0044** (tablet merge removed entirely — the `Absorb`
  teardown this entry's drain-before-halt fix lived in no longer exists).
  Archived verbatim in `docs/engineering-lessons-archive.md`'s "Superseded
  by ADR 0044" section; the still-general lesson: **a teardown that
  deletes local state must drain first if that state is about to be served
  elsewhere.**
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
- **Superseded twice over** — `version_floor`, the cross-group LWW-version
  fix this entry documents, was retired by ADR 0018 PR2's range-seal design
  (an ordering-based fence replaced the version-space separation it relied
  on) independently of tablet merge, and the `MergeTablets` half of it was
  then deleted outright by ADR 0044. Archived verbatim in
  `docs/engineering-lessons-archive.md`'s "Superseded by ADR 0044" section;
  no still-general lesson pointer needed — the mechanism is retired, not
  just merge's half of it.
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
- **A read-only browse surface over a namespaced slice of a *shared* engine
  must scan the namespace's own bound, never `StorageEngine::entries()` —
  the moment the engine is shared with something bigger, `entries()` stops
  meaning "everything in my namespace" and starts meaning "everything on
  this node."** Building plan-syskv-ui's `GET /admin/system-table` (an ADR
  0038 addendum) — a read-only browse of the control plane's reserved
  system keyspace — `entries()` would have been the obvious one-liner (it's
  what `mirror::rebuild_metadata_from_engine` already calls, since *that*
  engine genuinely holds nothing else). But on a **combined** node this same
  physical `StorageEngine` is also the CP data plane's own storage (ADR
  0028) — every user table's every tablet's every key lives in it too — so
  `entries()` there is O(all user data on this node), not
  O(system-keyspace), even though the endpoint only ever wants the tiny
  reserved slice. The fix (`animus_control::syskv::reserved_scan_bounds()`,
  built from a general `prefix_successor` byte-lexicographic-successor
  helper) is a single bounded `StorageEngine::scan(start, end)` over exactly
  the namespace's own prefix range, with any further filtering (a `?kind=`
  query param) done **in memory** on the small resulting page, never by
  widening the engine-level scan. **General check before wiring any new
  read against a shared multi-tenant engine (ADR 0026/0028's whole shared-
  engine-per-node shape, which several planes already lean on): does this
  engine hold *only* what I think it holds, or is it namespaced/scoped
  inside something bigger? If the latter, `entries()`/an unbounded scan is
  a scaling foot-gun waiting for a "combined node" deployment shape to
  trigger it** — a control-only node (this endpoint's simplest case) would
  never have caught the bug, since its dedicated engine genuinely holds
  nothing else; only a combined node's shared engine exposes it, which is
  exactly the deployment shape most demos/dev clusters default to.

- **Merging two roles onto one shared resource silently breaks any
  aggregation code that assumed they were always distinct — audit every
  "sum across roles" call site, not just the assembly code that merged
  them.** ADR 0040 PR1 merged a combined node's two internal `ProdEnv`s
  (control + raftkv) into one shared env. `ClientCtx::metrics_text`/
  `metrics_json` had always summed "the control-role sink" +
  "the raftkv-role sink" as two `MetricsHandle` snapshots, on the correct
  pre-merge assumption that they were two distinct `Arc`-backed sinks; after
  the merge, both handles are clones of the identical sink (`ProdEnv::
  metrics()` is shared across every clone of one env, by design — see
  `animus-env/CLAUDE.md`), so summing their snapshots silently double-counts
  every counter for every combined node, forever, with no compile error and
  no test failure unless a test asserts an *exact* counter value (most
  don't). Caught only by re-reading the aggregation code while doing the
  merge, not by any gate. Fix: add `MetricsHandle::is_same_sink` (`Arc::
  ptr_eq` on the inner sink) and skip the second push when it's true — a
  small, generically reusable escape hatch for exactly this "two things that
  used to be different are now the same thing" class of bug. **General
  check: whenever two previously-independent resources (envs, connections,
  caches, sinks) get merged into one, grep for every site that iterates
  "each of the N distinct resources" and confirm it still holds — the
  n-way sum/aggregate is the shape most likely to go silently, quietly
  wrong.** (`animus-env::metrics.rs`, `animusd::ClientCtx::metrics_text`/
  `metrics_json`, ADR 0040 PR1.)
- **When an id-derivation scheme changes, every test bring-up helper's
  hardcoded "pick a known-free id" literal is a landmine — grep for bare
  integer id literals in test files, don't just fix the production
  derivation.** ADR 0040 PR1 collapsed the pre-existing `control_id(i) = i` /
  `raftkv_id(i) = 300 + i` split into one id per node (`node_id(i) = i`).
  Several `animusd` integration tests picked a "obviously free" id for a
  growth/control-add scenario by reasoning about the *old* two-space scheme
  (e.g. "control ids in this split config are `{0,1,2}`, raftkv ids start at
  300, so `3` is free" or "so `300` already exists as a data-plane member,
  useful as a collision target") — every one of those literals silently
  became wrong once ids unified: `3` collided with the first data-only
  node's own new id, `300` no longer named anything at all. These are
  exactly the kind of bug the compiler cannot catch (a valid `u64`, a valid
  HTTP call, a differently-shaped but still-well-formed JSON error body) —
  they only surface as a runtime test assertion failure, one test at a time,
  each with a different symptom. **General check: after any change to how
  node/member ids are derived or allocated, grep every test file for bare
  integer literals passed where an id is expected (`add_control_member`,
  `RoleAddrs`/`ClusterConfig` construction, expected-voter-set assertions)
  and re-derive each one from the new scheme's actual id space — don't trust
  a comment that describes the *old* scheme's reasoning, even one sitting
  right next to the literal it once justified.** (`animusd/tests/
  control_membership_admin.rs::{add_control_member_collision_shapes,
  grow_control_group_converges_everywhere}`, `control_membership_split.rs::
  grow_then_replace_a_voter_over_a_split_deployment_with_live_data_traffic`,
  ADR 0040 PR1.)
- **A compiler-error-driven auto-fixer that blindly applies the *primary*
  span of a "mismatched types" diagnostic corrupts call sites where rustc's
  primary span is the *callee*, not the mismatched argument.** Turning
  `NodeId` into a newtype (ADR 0040 PR2) meant sweeping ~300+
  now-mismatched-argument call sites; scripting "wrap the primary span's
  text in `nid(...)`" off `cargo build --message-format=json` worked for the
  common single-bad-argument shape, but rustc collapses a call with **two or
  more** simultaneously-mismatched arguments into one diagnostic titled
  "arguments to this method are incorrect" whose *primary* span underlines
  the **method name** (e.g. `partition_pair`) for the "where" location, with
  each individual argument's mismatch riding a separate **non-primary**
  span. Blindly wrapping the primary span turned `sim.partition_pair(1, 2)`
  into `sim.nid(partition_pair)(1, 2)` — syntactically bizarre but *some*
  of these slipped past a first pass because the corruption doesn't always
  fail in the same file/build batch it was introduced in (a cascading parse
  error elsewhere can suppress it from that round's diagnostics, so it only
  surfaces once the parse error blocking it is separately fixed). **Fix:
  detect the multi-argument shape from the diagnostic (rendered message
  `"arguments to (this method|this function) are incorrect"`, or simply:
  primary span text is an identifier matching the callee, not an
  expression) and wrap each individual **argument's own non-primary span**
  instead of the call's primary span.** A second, related hazard from the
  same sweep: wrapping array/`vec!` literal elements one at a time from
  per-element diagnostics can leave a literal **partially wrapped** (`[nid(0),
  1, 2]`) if only the first element's mismatch was reported in a given pass —
  always re-scan finished literals for a bare integer sitting next to an
  already-`nid(...)`-wrapped sibling before considering a file done. General
  rule for any future compiler-diagnostic-driven bulk rewrite: **verify one
  layer down from "which span does the tool report as primary" before
  trusting it as "the expression to edit"** — a fresh `cargo build
  --workspace --all-targets --keep-going` after every batch of edits (not
  just after the batch that seems to close out a file) is what caught these,
  since a still-corrupted call site fails loudly (parse error or a
  nonsensical type error) rather than silently. (ADR 0040 PR2, `animus-env`
  `NodeId` newtype sweep — corruption found and fixed in `animus-sim`,
  `animus-consensus`, `animus-cp-data`, and `animus-control` test files.)
- **A `NodeId` representation change (`u64` → a validated string) breaks tests
  that hardcode its *rendering*, not just its *type* — and these compile
  clean, so only a green-then-red gate catches them.** ADR 0040 PR3 changed
  `NodeId`'s `Display` from a bare integer (`"0"`) to `"n0"`/an
  operator-proposed string/an allocator-minted `"alloc-…"`. Two distinct
  failure shapes, neither a compile error: (1) `accord_backoff.rs`'s
  `sends_from` helper built its trace-grep needle as
  `format!("SEND {from}->")` with `from: u64` interpolated bare (`"SEND
  0->"`), but the actual trace line now renders `"SEND n0->n1"` — the needle
  silently matched **zero** lines forever, so a `sends >= 4` liveness
  assertion failed at its *frozen, fixed* seed on every run, not
  intermittently. Fix: build the needle from the same `NodeId` the trace
  formatter uses (`format!("SEND {}->", nid(from))`), never re-derive a
  numeric-looking string independently. (2) A test asserting an
  allocator-minted id "never collides with a small manual id" via `first >
  nid(302)` silently flipped from true to false: `"alloc-1000000"` sorts
  *before* `"n302"` lexicographically (`'a' < 'n'`), even though the ids are
  genuinely disjoint by their reserved-prefix *namespace*. **General rule:
  after any type whose `Display`/`Ord` semantics change from "numeric
  magnitude" to "opaque string," grep every test for `format!` needles built
  from the raw numeric seed instead of the real formatted value, and for
  `<`/`>`/`>=`/`<=` comparisons that encode a magnitude assumption — both
  compile fine and fail (or silently stop testing anything) only at
  execution.** (`animus-consensus/tests/accord_backoff.rs`,
  `animus-control/src/meta.rs::allocate_node_id_is_monotonic_and_disjoint_
  from_small_manual_ids`, ADR 0040 PR3 — that specific test, and the
  allocator/`"alloc-…"` mechanism it illustrated, were deleted in ADR 0040
  PR4; the general rule above outlives it and still applies to any other
  `Display`/`Ord` semantics change.)
- **A lowercase, single-letter-plus-parens test helper name (`fn c() -> T`)
  can collide with an equally-terse, ubiquitous local variable of a
  completely different type, and the resulting error reads as a type
  mismatch far from the real cause.** Renaming a PR2-era `const C: NodeId`
  (uppercase, never shadows anything) to a PR3-era `fn c() -> NodeId`
  (lowercase, matching this codebase's `nid`-helper convention) collided with
  `reconciler_corpus.rs`'s own near-universal `let mut c = Cluster::new(sim);`
  scenario-harness variable — every `c()` call after that point parsed as
  "call the local `Cluster` value named `c`," not the function, producing
  "expected function, found `Cluster`" at a dozen unrelated-looking call
  sites. **General rule: when a mechanical rename turns a `const` into a
  `fn`, or otherwise introduces a new lowercase short binding, grep the
  target file(s) for that exact identifier already in use as a *local
  variable* before trusting the rename is safe** — a real type-level
  namespace (`const`/`static`/type-level items don't shadow local `let`
  bindings the same way a same-named `fn` at module scope does once called
  with `()`) doesn't protect against this once the item becomes callable.
  Fixed by renaming the function to a distinct name (`node_c`) instead of
  chasing every shadowing call site. (`animus-cp-data/tests/
  reconciler_corpus.rs`, ADR 0040 PR3.)
- **Loosening a parsed type's charset can silently turn a test's "garbled,
  must-be-rejected" fixture into a now-valid value the parser accepts.**
  `animusd::topology::parse_not_leader_refusal`'s garbled-hint-suffix test
  used `"notanumber"` as its not-a-real-id fixture — correct while `NodeId`
  parsed as `u64` (that string could never parse), silently wrong once ADR
  0040 PR3 gave `NodeId` a permissive `[A-Za-z0-9._-]{1,64}` charset:
  `"notanumber"` is now syntactically a perfectly valid id, so the parse
  that was supposed to fail-and-fall-back to "no hint" instead succeeded,
  and the test failed asserting the old ("garbled") outcome against the new
  (correct) one. Fix: use a fixture with a character truly outside the new
  charset (a space), not a string that merely *used to* fail a stricter
  parse. **General rule: when a validated type's accepted-charset widens,
  grep tests for "deliberately invalid" string literals used as negative
  fixtures — a literal that was invalid only by the old, narrower rule
  needs replacing, not just recompiling.** (`animusd/src/topology.rs::
  not_leader_refusal_tolerates_a_garbled_hint_suffix`, ADR 0040 PR3.)
- **When a CLI's identity scheme changes from "operator-picked index" to
  "explicit-or-self-minted string id," a test-support helper that used to
  take the index can usually keep its own signature unchanged — just have it
  derive the new explicit id *from* the index it already takes
  (`config::node_id(index)`) at the one internal call site, rather than
  propagating the new `Option<NodeId>` parameter out through every test file
  that calls it.** ADR 0040 PR4 replaced `run_node_join(seeds, index: usize,
  ..)` with `run_node_join(seeds, id: Option<NodeId>, .., labels)` — a
  breaking signature change — but `animusd/tests/support/mod.rs`'s
  `join_fresh_deadline(seeds, index: usize, ..)` (called from
  `seed_join.rs`/`decommission.rs`/`cluster_growth.rs`) kept its own
  `index: usize` parameter and just changed its one-line internal call to
  `run_node_join(seeds, Some(config::node_id(index)), ..)`. Every caller
  outside `support/mod.rs` — which mostly just wants "a distinct, readable,
  deterministic test id for slot N," not "test the new CLI surface itself" —
  compiled and passed completely unchanged; only the two direct,
  bypass-the-helper call sites in `seed_join.rs` (a rejoin helper and an
  explicit collision-guard test) needed updating to pass the id directly.
  **General rule: when a lower-layer API's identity parameter type changes,
  look for the test-support layer that already had a stable, semantically-
  named wrapper around the old parameter before touching every call site —
  the wrapper is usually the one and only place that needs to translate the
  old convention into the new one, and preserving its signature is what
  keeps a large, otherwise-unrelated test suite's diff to nearly nothing.**
  (ADR 0040 PR4.)
- **`Hlc::witness(remote, now)` is the wrong tool for disambiguating a
  value that is *deliberately* far in the future relative to the clock's
  own normal progression — witnessing doesn't just validate the value, it
  adopts it as the clock's new baseline, poisoning every ordinary `mint`
  that follows until real wall-clock time catches up.** ADR 0018 §2/PR2b's
  logged-read-ceiling design proposes a ceiling candidate
  `uncertainty_upper(ts) = ts.wall_ms + max_offset` (deliberately ~500ms
  ahead, so ceiling proposals amortize across many reads instead of firing
  per-read) — but two `ensure_ceiling_above` calls that happen to compute
  the *same* millisecond-granular margin (`uncertainty_upper` collapses
  `logical` to 0) would otherwise propose byte-identical `ReadCeiling`
  entries, tripping the apply-time monotonicity assert every command must
  satisfy. The obvious fix — `self.hlc.witness(margin, now)` to
  disambiguate, since `witness`'s contract guarantees the result strictly
  exceeds both the margin and everything previously minted/witnessed — is
  actually a *worse* bug: witnessing a 500ms-future value drags the
  group's own `Hlc` forward to match it, so the very next *ordinary* read
  mints a `ts` already close to that inflated baseline, immediately
  exceeding the ceiling just committed and forcing a fresh proposal —
  turning an intended O(1)-amortized mechanism into O(N) (one proposal per
  read), caught by a test that specifically drove many sequential reads
  and counted proposals rather than just checking correctness. **General
  rule: reach for `witness` only when the goal is genuinely "fold this
  observed value into my notion of *now*"; when the goal is merely "make
  this candidate value unique against others like it" without changing
  what the clock reports for anything else, use a separate ratchet (a
  small CAS loop over its own counter) instead — sharing the *same* clock
  used for the rest of the system's ordinary time-keeping is exactly what
  causes the leak.** (`RaftKvNode::next_ceiling_candidate`,
  `crates/animus-cp-data/src/lib.rs`; regression in
  `tests/ts_cache.rs`'s amortization test.)
- **Superseded by ADR 0044** (tablet merge removed entirely — the
  `split_and_merge_over_a_split_deployment` test and the `Absorb` teardown
  half of this postmortem no longer exist). Archived verbatim in
  `docs/engineering-lessons-archive.md`'s "Superseded by ADR 0044" section;
  the still-general lesson: **the seal-ordering fix generalizes to split's
  `NarrowScope` gating, which is still live** — see `crates/animus-cp-data/
  CLAUDE.md`'s range-seal invariant entry for the mechanism as it stands
  today.
- **A marker key that must live *inside* an existing `StorageScope` (not
  engine-global) needs a different disjointness proof than "reserve a
  name no user schema may claim"** (ADR 0018 §2/PR3, the txn record). The
  range-seal/read-ceiling markers (`seal.rs`/`ceiling.rs`) prove
  disjointness from every table's keys by living **outside** every scope,
  under the control plane's `RESERVED_NAMESPACE` — a trick that only works
  because no user table can ever be *named* that reservation. A txn
  record can't use that trick: it has to be an ordinary in-scope logical
  key of one specific tablet (so it replicates through that tablet's own
  Raft log, ships with `engine_image`, and moves with a split/merge like
  real data), and there is no analogous "reserved partition key" a table's
  own row keys could be barred from — a client can pick *any* bytes for
  both the partition key and the row key. The fix was to find a
  **structural** invariant of the key-escaping scheme itself rather than a
  registry: `animus_tablet::escape`'s encoding can only ever start a
  real key's post-token suffix with `[0x00, 0x00]` (empty partition key)
  or `[0x00, 0x01, ..]` (a partition key starting with a literal `0x00`
  byte) — no partition key, however chosen, can make `escape(pk)`'s first
  two bytes be `[0x00, X]` for `X` outside `{0x00, 0x01}`. Picking `X =
  0x02` as the marker's own lead byte therefore makes it **provably**
  disjoint from *every* real key sharing that token, regardless of what
  the arbitrary, client-controlled row-key suffix contains — a stronger,
  narrower guarantee than "no collision in practice," derived from the
  encoding's own termination rule instead of a naming convention. General
  lesson: when a marker must sit inside a scope whose key contents are
  fully attacker/client-controlled, look for a byte-position where the
  *encoding itself* (not a value convention) constrains what's possible —
  a length-prefix boundary, an escape terminator, a fixed-width prefix —
  and prove disjointness there; a bare "pick an unlikely-looking prefix"
  approach is exactly the mistake the seal marker's own history already
  contains one instance of (its retired `[0x00, 0x00]` draft, see
  `seal.rs`'s doc) and would have been easy to repeat here in a
  differently-shaped way. See `animus-cp-data/src/txn.rs`'s module doc for
  the full proof and `tests::record_key_never_collides_with_any_escaped_pk_
  plus_rk` for the case-by-case regression.
- **Wrapping every value the apply path merges into a shared engine in a
  new envelope is a crate-internal change with a wide *test*-side blast
  radius, even when production callers are all safely routed through the
  crate's own accessors.** Introducing the ADR 0018 §2/PR3 value envelope
  (a leading tag byte on every committed value) required no changes
  outside `animus-cp-data`'s own apply path and read accessors — every
  production caller already went through `RaftKvNode::local_get`/
  `local_scan`/`linearizable_get`/`read_at`/etc., which unwrap it. But two
  *tests* in the same crate (`tests/reconciler.rs`,
  `tests/reconciler_corpus.rs`) read the engine's raw stored bytes
  directly (`storage.get(key).value`) to assert sibling-sparing/data-safety
  invariants at the physical-key level — a deliberate, valid testing
  technique that this change silently broke (both compiled and ran; they
  just started comparing against bytes one tag short). **Grep every test
  file for raw `storage.get`/`.scan`/`.entries` + `.value` access whenever
  a change alters what the engine's *stored bytes* mean, not just what a
  public accessor returns** — `cargo test`'s green/red signal alone caught
  this fine here, but the fix (documenting the envelope and updating the
  two call sites, one by expected-value literal, one by centralizing the
  unwrap in the shared `assert_present` helper) is exactly the kind of
  thing that's cheaper to anticipate than to debug from a confusing
  off-by-one-byte assertion failure.
- **A method's documented `assert!` — "a caller invariant, not a recoverable
  condition" — stops being safe the moment a new caller can reach it with
  untrusted input, even if the method itself never changes.**
  `RaftKvNode::txn_stage` (ADR 0018 §2/PR3) hard-asserts its anchor key is
  at least `TOKEN_BYTES` long; this was correct and safe when its only
  callers were tests and a Dynamo/CQL edge that always builds ADR-0022-
  shaped keys. ADR 0018 §2/PR4 added the first genuinely wire-facing caller
  (`ClientRequest::Txn` → `ClientCtx::cp_txn`), which can hand it an
  arbitrary client-supplied key — an unvalidated short key would have
  panicked the whole node process, a real DoS vector for a distributed
  database, not a graceful error. The bug was caught by the PR's own
  ProdEnv integration test, not code review, because the test used a raw
  literal key shorter than the invariant. **Whenever a change makes a
  previously-internal-only function reachable from an external/untrusted
  caller (a new wire request type, an admin action, a CLI flag), grep that
  function for every `assert!`/`panic!`/`unwrap!`/`expect!` documented as a
  "caller invariant" and add validation at the new boundary** — the fix
  belongs at the boundary (`cp_txn` now validates and returns a clean
  `Err`), not by softening the assert itself, which is still the right
  contract for the trusted internal callers.
- **Building the ADR 0018 multi-tablet transaction corpus (PR6) surfaced a
  real protocol bug and four harness bugs, all in the same short
  investigation, with a common thread: "the same outcome, computed a second
  time by a different actor, is not automatically the same value."**
  1. **`TxnCommit`'s apply arm treated "already Committed, different
     `commit_ts`" as impossible-by-construction and hard-asserted on it —
     it wasn't impossible.** `txn_commit_at_least`'s own `mint_at_least`
     mints a *fresh* timestamp every call; a still-live coordinator's own
     commit round trip and the recovery resolver's independent post-grace
     push can each legitimately decide "commit" for the same transaction
     with *different* minted values, and `animusd`'s own `CLIENT_TIMEOUT`
     (10s) being longer than `RECOVERY_GRACE` (5s) makes the overlap window
     reachable under nothing more exotic than an ordinary leader election —
     found live, deterministically, on the corpus's first fault-injection
     scenario. Fixed by extending the existing `Committed`-vs-`Aborted`
     duelling-decider no-op to also cover same-outcome-different-ts (first
     log position still wins, unconditionally) — see ADR 0018's PR5
     amendment §1 corrective note and `animus-cp-data/CLAUDE.md`'s
     "In-doubt recovery + decision semantics" entry. **The generalizable
     rule**: when a design lets two independent deciders each reach a
     conclusion (not just "commit vs. abort" but the *exact value* of a
     commit), a hard assert on "impossible for them to disagree" needs a
     stronger argument than "only one entity ever decides" — audit what
     happens when a *second*, equally legitimate decider computes the
     *same* answer through a *different* computation.
  2. **A resolve-side helper's OWN caller resolving with a *hardcoded*
     outcome, computed before checking what actually happened, is a torn
     resolve waiting to happen** — my own corpus coordinator's abort path
     proposed an abort, then unconditionally resolved every staged key as
     `Aborted` without re-reading the record's actual decided status first
     (a concurrent recovery commit could have already won). Fixed by
     re-reading before resolving, matching the discipline `ClientCtx::
     cp_txn`/`txn_recover` already follow in production (confirmed by
     auditing every real resolve call site — none of them had this bug;
     only my own test harness did).
  3. **A read-resolution helper that only *serves one read correctly* is
     not the same thing as a helper that *durably fixes storage*** —
     `RaftKvNode::resolve_intent_given_status` (and `animusd::ClientCtx::
     cp_get_local_resolving`, which calls it) compute the right answer for
     *this one read* without ever proposing a `TxnResolve`; the physical
     envelope stays an unresolved intent forever unless something else
     (the proactive resolver loop) does the durable rewrite. This is
     documented, accepted production behavior (`TxnTracker::
     unresolved_decided`'s own doc: an anchor stops tracking a transaction
     once *its own* keys resolve, even if a participant's intent on a
     different tablet never gets a proactive fan-out — "still resolved on
     demand the moment any reader hits it" means the *read* is correct,
     not that storage settles) — but a test harness's own "read the final
     state" check that uses a **raw, non-resolving** read (as this corpus's
     `final_state` deliberately does, to keep a meaningful cross-replica
     comparison) will never trigger that on-demand path for a key nobody
     reads again, and will misreport a durably-committed-but-never-resolved
     value as data loss. Fixed with a test-only helper that, unlike the
     production read path, *does* propose an actual `TxnResolve` once a
     foreign intent's status is known. **The general lesson**: when a
     system's "eventual consistency" story rests on "any reader passing by
     will fix it," a test that deliberately never reads the data again
     needs its own explicit "make sure something reads it" step — don't
     assume a converged-or-timeout poll alone reproduces that guarantee if
     the poll's own read path doesn't exercise the same code path a real
     reader would.
  4. **A helper that picks "the first replica reporting `is_leader() ==
     true`" must exclude replicas known to be faulted, or it can talk to a
     frozen, isolated node instead of the genuine leader** — a crashed
     replica keeps answering `is_leader() == true` from its last-known,
     pre-crash state forever (it never learns it lost the term; it's
     muted, not shut down). `raftkv_linearizable.rs`'s own `leader_among`
     helper already excludes known-crashed indices for exactly this
     reason; a new harness written independently (this corpus's own
     `leader_of`) didn't replicate it. Fixed more robustly than
     "thread a crashed-set through every call site": pick the reporting
     replica with the **highest `term()`** instead of the first by array
     index — any real election strictly increments the term, so a frozen
     replica's stale term can never out-rank a genuine new leader,
     without needing any external fault-tracking state at all.
  5. **A multi-participant intent must carry the ANCHOR's own table name,
     never the participant's own** — `record_table` (stamped into every
     `Envelope::Intent`) exists precisely so a reader hitting a foreign
     intent knows where to route its `TxnStatus` query; passing the
     participant's own table name there instead (an easy copy-paste-shaped
     mistake when the staging loop's own iteration variable is already
     named `table`) means that query always looks for the record in the
     wrong tablet's scope, finds nothing, and the intent never resolves —
     on demand or otherwise. Caught only by tracing one specific stuck
     key's own `IntentInfo` byte-for-byte back to which key's 8-byte token
     the record's own key was actually derived from, since the symptom
     (durability check reports one committed append as lost) looks
     identical to several other, unrelated causes.
  **Diagnostic lesson**: every one of these was found by adding a
  temporary, narrowly-targeted `eprintln!` at the exact decision point
  (which arm of a match fired, what a specific key's own `FastRead`
  variant was, what a specific txn_id's tracker state was on each resolver
  tick) and re-running the *one* failing scenario in isolation — never by
  guessing from the failure message alone. Four of these five bugs
  produced the *same* durability-check symptom ("lost acknowledged
  append") with completely different root causes; only tracing the actual
  runtime state, one hypothesis at a time, distinguished them — a lesson
  worth restating from this file's own Hlc/`propose_ordered` entry above,
  now at the scale of "chasing a bug through several confounding layers,"
  not just one.

- **Continuing to run the same corpus at depth after fixing one corpus-found
  bug found a second, unrelated one — and then a third layer, once the
  fix for the second bug was itself checked at depth.** Three separate
  findings, each only visible once the previous one stopped masking it:
  1. **The happy-path commit-report footgun is the mirror image of the
     abort-path one, and re-auditing every "decide, then report" call
     site after finding one instance doesn't catch the sibling** — the
     corpus's own coordinator (`run_txn`) fixed its *abort* path's
     torn-resolve bug (item 2 in the entry above) but its *commit* path
     still reported success straight off `txn_commit_at_least`'s
     `Some(ts)`, the identical "entry applied ≠ my decision won" mistake,
     just on the opposite branch. Found by a *different* corpus scenario
     (`anchor_leader_kill_mid`) than the one that found the abort-path bug
     — the two bugs happened to need different fault shapes to trigger.
     Fixed by always re-reading the record's actual status after every
     decide attempt, commit or abort alike, matching what a full
     re-audit of every *production* decide call site
     (`txn_decide_anchor`, `txn_recover`, and the apply-time `TxnTracker`
     bookkeeping itself) already confirmed they do correctly. **The
     lesson**: fixing one branch of a two-branch bug class doesn't mean
     the other branch got checked — a hypothesis this specific ("does the
     code re-read before reporting, at *every* decide point, not just the
     one the failing test happened to exercise") is worth stating and
     checking explicitly, not inferred from one green test.
  2. **A multi-key snapshot-read heuristic needed three redesigns before
     it stopped producing false-positive torn reads, and each of the
     first two replacements introduced a *new*, narrower race that only
     the next depth run exposed** — `animus-test`'s cross-tablet
     transaction corpus's read-only shape: (a) a single future-padded
     `read_at` snapshot ts turned out to be structurally undermined by
     the write-conflict-push mechanism (`RaftKvNode::mint_pushed`) itself
     — a write can be stamped *above* whatever ceiling an **earlier**
     read already pushed that group's clock to, and since `Hlc::mint` is
     monotonic that's a **permanent** floor, so no margin (fixed or
     dynamically sampled from the group's own state) can close it; (b)
     replacing it with "force-resolve once, then read every key
     sequentially" fixed that but introduced a *narrower* race — a slow
     key's own resolve/read can itself take real time, so a transaction
     touching an *earlier*, already-read key can still land before a
     *later* key in the same list is read; (c) making both passes
     **concurrent** (`futures::future::join_all`) narrowed the window to
     one round trip but *still* didn't eliminate it — group-to-group
     ReadIndex latency doesn't start in perfect lockstep even when every
     future is spawned at the same instant. The design that actually
     closed it: read **twice**, concurrently, and only accept the result
     once two consecutive rounds agree byte-for-byte — a positive proof
     of quiescence (nothing was in flight during the whole window),
     rather than a narrower and narrower guess at "surely nothing changed
     this fast." **The generalizable lesson**: when a "make this
     consistent" fix for a distributed read keeps getting narrower races
     rather than zero races, the fixable-margin approach is probably the
     wrong shape entirely — look for a **verifiable stability condition**
     (two independent observations agreeing) instead of a **tighter
     timing bound**, which can always be beaten by one more layer of
     concurrency the previous fix didn't anticipate.
  3. **Overwriting another transaction's still-unresolved intent doesn't
     erase it — MVCC keeps the old version — and a later transaction's
     own abort-restore only ever looks *one hop* back, so it can land on
     that stale intent instead of a real committed value, permanently
     hiding it.** Found at seed depth (`ANIMUS_TXN_SEEDS=10`), not in the
     frozen corpus — needs three sequential same-key transactions from one
     client (single-writer-per-key workloads make this the *ordinary*
     case, not a contrived one): the first commits, the second overwrites
     it and is abandoned before deciding, the third stages over the
     second's still-live intent (silently succeeding, pre-fix) and later
     gets decided `Aborted` — its restore's one-hop-back `get_at` finds
     the *second* transaction's intent, not the first's real value, and
     blindly re-merges it at a timestamp *higher* than the first
     transaction's own eventual correct `commit_ts`, so a later correct
     resolve can never win that race via ordinary LWW. Chasing the
     version chain back *multiple* hops on the read side was the obvious
     first fix and the wrong one: an intermediate hop skipped over could
     belong to a transaction that *later commits*, moving the identical
     unrepairable-LWW-loss corruption onto a *different* transaction
     rather than removing it. The fix that actually closes it structurally
     is CockroachDB's writers-push-intents discipline: reject the
     overwrite at **apply time** (a target key already holding a
     *different* transaction's unresolved intent makes the whole stage a
     no-op, whole-or-nothing, exactly like a fence/seal miss), so a key
     can hold at most one live intent at a time and a one-hop-back
     lookback is *always* sound. **This required a second, proposer-side
     fix to be safe at all**: since a stage call returning `Some(ts)` only
     ever meant "the entry applied," a coordinator that didn't check would
     go on to commit a transaction *without one of its own writes ever
     having happened* — worse than the original bug. Every coordinator
     (production `animusd::ClientCtx::txn_prepare_pushing` and the
     corpus's own `stage_anchor_pushing`/`stage_participant_pushing`) now
     verifies each staged key genuinely landed (`txn_verify_staged`, the
     same primitive a recovery push already uses) and retries, bounded,
     before giving up. **The generalizable lesson**: "reject the bad write
     at apply time" and "the proposer must not assume Some(ts) means my
     content is really there" are not two independent hardening options to
     pick between — a system with the second discipline already
     established (task #15's fix, above) needs it applied to *every* new
     apply-time rejection too, or the rejection alone just moves the false
     success from "wrong outcome" to "silently missing write."
- **A generic "re-check this key's value before committing" primitive is
  unsound when the key being re-checked is also the key being written by
  the same transaction** (ADR 0018 §2/PR7, atomic Dynamo
  `TransactWriteItems`). `animus-cp-data`'s `cp_txn` precondition mechanism
  (`(table, key, expected)`, re-read once before staging and again right
  before the commit decision) was designed for the classic cross-key
  read-modify-write shape — check key A, write key B — and is documented as
  such on `cp_txn` itself. An early version of `dynamo.rs::run_transact` fed
  *every* condition-gated action's observed value into it, including a
  `Put`/`Delete`/`Update`'s own key — i.e., "check that key K still looks
  like what I read, then commit a write that also targets K." That
  precondition's own re-read runs *after* this same transaction has already
  staged its own intent at K, so it can only ever observe either the
  pre-stage value (if racing ahead of the stage) or this transaction's own
  still-`Pending` intent (the common case) — which cannot resolve until the
  transaction itself decides, which hasn't happened yet in `cp_txn`'s own
  control flow. The read doesn't fail cleanly; it blocks in the ordinary
  client-facing read's retry loop until something *else* resolves the
  intent — here, the background in-doubt-recovery resolver, several seconds
  later (past `RECOVERY_GRACE`) — at which point the precondition "finds" a
  value that of course differs from the pre-stage observation and reports a
  spurious conflict. The bug didn't look like a hang; it looked like an
  intermittent, several-second-delayed false cancellation, caught by an
  existing regression (`animusd/tests/dynamo_schema.rs::extended_surface`)
  whose runtime jumped from under a second to several seconds before it
  started failing — the *timing* signature (roughly `RECOVERY_GRACE`) was
  the clue that pointed at "something's blocking on this transaction's own
  unresolved state," not a genuine data race. Fixed by restricting the
  precondition mechanism to keys structurally guaranteed distinct from
  every write in the same transaction (here, `ConditionCheck`'s key only —
  never a `Put`/`Delete`/`Update`'s own), and documenting the resulting,
  narrower guarantee for a write's own condition (protected only by
  whatever same-node serialization already existed, not cross-node OCC)
  rather than silently accepting the broken stronger claim. **The
  generalizable lesson**: before wiring a "verify unchanged, then commit"
  precondition/OCC/CAS mechanism onto a key, check whether that mechanism's
  own re-read can observe *this same in-flight operation's own effect* —
  if the thing being written and the thing being checked can be the same
  key, the mechanism's re-check window necessarily straddles this
  operation's own not-yet-decided state, and "verify unchanged" degrades
  into "wait for myself to finish," which for a mechanism whose only
  liveness backstop is a several-second background sweep looks exactly
  like an intermittent bug, not a hang.
  **Update (ADR 0018 §2, 2026-08-12 follow-up): the self-referential-stall
  case above is now closed by a different primitive, not the workaround.**
  `animus_cp_data::KvCommand::TxnStage` gained its own `conditions` field —
  byte-level OCC checked once, *inside the same apply arm* that stages the
  intent, against the key's pre-intent committed value — so a write's own
  condition never needs a re-read at all, self-referential or otherwise.
  The lesson above (never route a key through a re-read-based
  precondition mechanism if that same key is also being written by the
  operation being verified) still generalizes to any *other*
  re-read-based OCC design; what's new is the alternative it points
  toward: if the write and the check happen inside the same atomic
  decision step (an apply arm, a single critical section), there is no
  window for the check to observe the write's own not-yet-decided effect
  in the first place, so the whole class of stall is structurally
  unreachable rather than merely mitigated.
- **A per-entry outcome introspection primitive (`CasResults`-shaped:
  `BTreeMap<log_index, Outcome>`, read back after a coarse "did this index
  apply" check) is not implied by that coarse check — a snapshot install
  can satisfy the coarse check without ever populating the fine-grained
  one for the entry in question.** Adding `StageOutcome` (ADR 0018 §2's
  apply-time write-key conditions amendment, `animus-cp-data`) alongside
  the pre-existing `wait_applied(index) -> bool` (confirms only
  `engine_applied_index() >= index`) initially paired them as "wait, then
  unconditionally fetch and `.expect()` the outcome" — reasoning that
  "applied" and "has a recorded outcome" were the same fact, true for
  every *other* command in this crate. They are not, for any command whose
  outcome is recorded by the *individual* apply arm rather than derived
  from the post-apply engine state: a replica catching up via
  **`InstallSnapshot`** (after losing leadership) advances
  `engine_applied` in one jump from the received image, without
  individually re-running the apply arm for every entry the image already
  covers — so an entry whose outcome only that per-entry arm would have
  recorded can have `engine_applied_index() >= index` true while its
  outcome map entry is simply absent. `ANIMUS_TXN_SEEDS=5` over the
  multi-tablet corpus (`animus-test/tests/txn_serializable.rs`) hit this
  as a hard, deterministic panic — not a hang, not a wrong answer, a
  crash, because the code trusted a fact ("wait_applied implies outcome
  recorded") that happened to hold for every pre-existing caller of the
  wait-then-fetch pattern (`Cas`'s own `compare_and_swap` was never
  vulnerable, because it was never split into two steps — it polls
  `cas_result` *directly* in its wait loop from the start). **The
  generalizable check**: whenever a "wait for index N to apply" helper and
  a "look up what happened at index N" store are two separate calls,
  verify explicitly that nothing in the codebase can advance the wait
  condition without also having populated the store for that exact index
  — a snapshot/compaction/batch-glob path is the recurring way this
  invariant quietly breaks, since those paths exist specifically to
  advance a coarse watermark without replaying fine-grained per-entry
  work. The fix is to poll the fine-grained store directly (one loop, no
  separate coarse check), exactly like the one pre-existing caller that
  was never at risk already did — never a hard `.expect()` on a fact
  that's true "usually" rather than by construction.

- **A per-file copy of a bring-up helper doesn't inherit the shared helper's
  hard-won mitigations — and "it has its own helper" is invisible at the call
  site.** `control_mirror_restart.rs` carried a local `free_addr()` +
  `start()` pair rather than using `tests/support`, so it never picked up
  `restart_same_addrs`' bounded rebind retry for the documented port-TOCTOU. It
  flaked under `cargo test --workspace` (`.expect("control-only node starts")`)
  while passing in isolation and at 6× self-concurrency, because the thief is
  *another binary's* probe. Two distinct defects, worth separating:
  **(1) the missing retry**, fixed by the same bounded-rebind loop
  `support::restart_same_addrs` uses — a same-address restart test structurally
  *cannot* re-allocate around a thief, since rebinding the captured addresses is
  the thing under test. **(2) A latent bug the retry would have masked**: the
  file allocated its five ports with five *sequential* `free_addr()` calls, each
  binding `:0`, reading the port, and dropping the listener before the next call
  — so the OS was free to hand the same port back twice and configure a node with
  `internal == client`. `support::free_addrs(n)` holds all `n` listeners until
  they are all read precisely to prevent that; the local copy had lost the
  reason. **The practices**: when a shared test helper exists, a per-file
  reimplementation needs a stated reason (here the real one was "control-only
  bring-up isn't `run_node_with`" — legitimate, but it should then have *ported*
  the retry, not omitted it); and when you fix a flake in a copied helper, diff
  it against the shared original rather than only patching the symptom, because
  the copy has likely drifted in more than one way. Corollary: a helper whose
  correctness rests on *when a resource is released* (holding listeners) is
  especially prone to being "simplified" into a broken loop, so say so in the
  doc comment. (`animusd/tests/control_mirror_restart.rs`.)
- **A `BTreeMap` with a non-string key cannot round-trip through `serde_json`
  at all — and ADR 0003 actively steers you into writing one.** The
  determinism rule says "no `HashMap` in logic, use `BTreeMap`" (lint-enforced
  via `clippy.toml`), and the repo convention says data-plane values
  (de)serialize with `serde_json`. For a **byte-keyed** map those two rules
  collide: `BTreeMap<Vec<u8>, T>` derives `Serialize` happily, compiles
  clean, passes clippy — and then fails at **runtime** with
  `Error("key must be a string")`, because a JSON object key must be a
  string. Nothing in the type system or the gates catches it; only executing
  the encode does. Hit while building ADR 0041's `IndexFootprint`
  (`animus-dynamo/src/index.rs`), whose natural shape is "GSI rows keyed by
  base sort key". **The fix that keeps both rules**: a `Vec<Struct>` held
  **sorted by the key field**, with the ordering invariant maintained by the
  single mutator (`set_item`) and lookup by `binary_search_by`. That is
  deterministic by construction (the encoding cannot depend on insertion
  order), JSON-native, and no slower at the sizes involved. **The
  generalizable practice**: every new durable/serialized type gets an
  `encode`→`decode` **round-trip unit test** in the same change, not just
  tests of its constructors and accessors — a round-trip is the only thing
  that executes the serializer, and for `serde_json` the serializer is where
  a whole class of key-shape errors lives. A test asserting the *sort
  invariant under out-of-order insertion* is worth pairing with it, since
  that invariant is now what determinism rests on.

- **A test that reassembles a mapping the production type already owns is a
  second, silently-diverging copy of the spec.** `StorageScope` maps a logical
  key to its physical engine key (`prefix || key`), and `StorageScope::whole()`
  was documented as "every physical-key operation is an identity transform."
  Seven test files had quietly baked that in — reading `node.storage().get(k)`
  with a *logical* `k`, or building `physical()` as `prefix_for(TABLE) || key` —
  so they were asserting against a hand-copied duplicate of the mapping rather
  than the mapping itself. When ADR 0041 §3 inserted a row-kind byte
  (`prefix || kind || key`, making `whole()` no longer the identity), every one
  of those copies became wrong at once. **This time the divergence was loud** —
  the reads returned `None` and the tests failed — but that was luck, not
  design: a mapping change that made a test read the *wrong existing key*
  instead of a missing one would have failed silently or, worse, passed. **The
  practice**: when a type owns a logical→physical (or encode→store) mapping,
  give callers a public accessor for it (here `RaftKvNode::physical_key(kind,
  key)`) and let nothing outside reimplement it — a test that needs the physical
  key should *ask*, and a test with no handle to ask through should at least
  name the layout element explicitly rather than inheriting it by omission.
  Corollary for the doc: "X is the identity transform" invites callers to skip
  the mapping entirely, which is the most brittle form of this duplication —
  prefer documenting *that a mapping exists* over documenting that it currently
  happens to be free.

- **A write path's siblings can silently diverge in feature coverage, and
  nothing but a grep of every writer catches it.** ADR 0041's index-maintaining
  write path (`animusd::dynamo::index_aware_write` → `kind_writes_for_item`,
  committing an item's LSI rows + GSI change-log record atomically with the
  base row) was wired into the single-item `PutItem`/`DeleteItem` handlers
  only. `UpdateItem` (`run_update_item`), `BatchWriteItem`, and
  `TransactWriteItems` (`run_transact`) all still commit through the plain
  `quorum_write`/`cp_batch_write`/`cp_txn` primitives that predate ADR 0041 —
  so a table's secondary indexes silently never see a write made exclusively
  through any of those three ops, with no error, no warning, and no test
  currently exercising that combination (every existing GSI/LSI test happens
  to write through `PutItem`). This was found while replacing the old
  edge-local in-memory index (which every write path *did* feed, via a
  shared `note_put`/`note_delete` post-write hook) with the native
  replicated-row read path (ADR 0041 §5) — deleting that shared hook removed
  the one place all four write paths' index-maintenance obligations were
  unified, which is what made the pre-existing gap visible instead of merely
  latent. The general lesson (the same shape as this file's
  `ClientCtx::cp_write`/`cp_put` entry above): when a family of write
  operations is supposed to share one piece of derived-data maintenance,
  check *every member of the family* against the primitive that actually
  does it, not just the one the feature's own tests happen to exercise — a
  new mechanism wired into only the "obvious" call site looks complete right
  up until a sibling silently doesn't participate. Left unfixed here
  (deliberately out of scope for the read-path PR that found it — see
  "Separate PRs for incidental bugs"); tracked in `animusd/CLAUDE.md`'s and
  `animus-dynamo/CLAUDE.md`'s ADR 0041 entries.

  **Resolution (2026-08-13).** `UpdateItem` and `BatchWriteItem` now route
  through `index_aware_write` (the latter per-item, and only for a table
  that actually has an index — an unindexed table keeps its no-read
  `cp_batch_write` fast path unchanged). `TransactWriteItems` did **not**
  get the same treatment — it is rejected outright (`ValidationException`)
  whenever any write action targets an indexed table, because closing this
  gap for real would mean giving `cp_txn`'s `KvCommand::TxnStage` a
  multi-kind-write extension (staging LSI rows + a change record atomically
  with a transactional base-row write), which is a genuine `animus-cp-data`
  protocol change, not a `dynamo.rs`-local fix — named as a follow-up in ADR
  0041's as-built note under §2. The generalizable half of this entry
  stands on its own; the corollary worth keeping: **when the honest fix for
  one sibling in a "family of write operations" reaches into a lower layer's
  protocol, a loud rejection of that one combination is a legitimate
  interim closure of a correctness gap** — better than either leaving it
  silently wrong or scope-creeping a wire-edge fix into a data-plane
  protocol change. Regression: `animusd/tests/dynamo_index_writes.rs`.

- **When a "bounded on both ends" primitive's stated reason is "every caller
  scans one contiguous sub-range," that's a fact about the callers that
  exist *today*, not an invariant of the primitive — check whether a new
  caller's shape still satisfies it before reusing it unmodified.**
  `RaftKvNode::local_scan_kind`/`linearizable_scan_kind` (ADR 0041 §3) took a
  mandatory `end: &[u8]`, documented as deliberate: every existing caller (an
  LSI `Query`, the GSI drain's `pending_changes`) always had a finite bound
  in hand, so an unbounded form "would make an accidental full-tablet read
  easy to write." That reasoning is sound for those callers and wrong as a
  universal law: a table-wide LSI `Scan`'s fan-out (added in the same ADR's
  §5 follow-up) needs its tail tablet scanned unbounded-above, and *no
  finite byte string the caller could construct actually bounds it* — an LSI
  row's trailing base-sort-key segment has no length limit, so any fixed
  suffix (even one built from the maximum-width token) can be exceeded by a
  longer real key sharing its prefix. The fix wasn't "find a big enough
  bound" (structurally impossible) but recognizing the primitive already had
  the right shape one level up: `local_scan`/`linearizable_scan` (the base
  scope's equivalent) had solved exactly this by deriving the bound from
  **the scope's own** `StorageScope::physical_bounds()` when the caller
  passes `None`, rather than trusting the caller to supply one — mirroring
  that (changing `end` to `Option<&[u8]>`, falling back to the kind scope's
  own `physical_bounds()`) closed the gap without reopening the
  accidental-full-scan risk the original design worried about, because the
  bound still comes from the scope's own prefix, never `entries()`. The
  general move: before copying "bounded because every caller today has a
  bound" onto a new caller that structurally can't have one, check whether a
  *sibling* primitive already solved the same problem for the *other* scope
  — the fix is usually "generalize that," not "invent a new escape hatch."
  (`crates/animus-cp-data/src/lib.rs::local_scan_kind`/
  `linearizable_scan_kind`; `crates/animusd/src/lib.rs::cp_scan_kind_table`;
  ADR 0041 §5's 2026-08-13 as-built note, 2026-08-13.)
- **A crate `CLAUDE.md` that narrates its own PR history accretes without
  bound — the fix is to keep only what a fresh reader can't get elsewhere,
  not to keep everything that was once true.** By 2026-08-13,
  `animus-control`/`animusd`/`animus-cp-data`'s guides had each grown past
  ~60–100K chars (`animusd` past the ~40K threshold this repo treats as the
  memory-file warning tripwire) by accumulating three things that are all
  cheaply *derivable* rather than load-bearing: (1) **test-file rosters** —
  a paragraph per `tests/*.rs` restating what a `ls tests/` plus the file
  name already tells you; (2) **PR-by-PR changelog narration** — "PR3
  shipped X; PR4 closed X's gap with Y; PR5's own amendment then found Y
  had a residual case Z" — which is exactly what `git log`/the ADR's own
  PRn amendments already record, just harder to grep; (3) **method-by-
  method API dumps** — a bullet per public function restating its
  signature and one-line behavior, which the function's own doc comment
  already says at the source. None of the three is *wrong*, but keeping
  them in the guide means every future PR narrates its own history a
  fourth time (in the ADR, in the commit message, in the PR description,
  *and* in the guide), and the guide's actual load-bearing content —
  invariants, gotchas, "never do X" — gets buried in restatement a reader
  has to wade through every time. Fix: cut the roster/changelog/dump
  material down to a current-state contract + a one-line pointer to the
  ADR file or `tests/` directory; keep every invariant, embedded gotcha,
  and prohibition verbatim (compressing prose is fine, dropping the actual
  claim is not). Trimmed `animus-control/CLAUDE.md` 62.8K→40.0K,
  `animusd/CLAUDE.md` 102.8K→54.1K, `animus-cp-data/CLAUDE.md`
  99.8K→49.3K with no loss of the invariants/gotchas a fresh agent needs;
  see each file's git history for the before/after diff shape. General
  rule: when a guide file is this large, the question to ask before
  writing a new paragraph into it isn't "is this true" but "is this
  something only this file can tell a reader" — if `git log`, the ADR, or
  the source's own doc comment already says it, point there instead of
  restating it. (2026-08-13.)
- **A doc comment that says a safety property is "enforced, not assumed" is
  a claim about the code, not a substitute for grepping it.** Both ADR 0041
  §1 and `animus-dynamo/src/index.rs`'s own doc for `INDEX_TABLE_SEPARATOR`
  state that `$` is illegal in a user table name and that this is enforced
  at `Metadata::apply`'s `CreateTableSchema` arm, "alongside the existing
  `syskv::is_reserved_name` gate." Re-grounding the DynamoDB Streams work
  (ADR 0042/0043) in what the tree actually enforces required grepping
  every `is_reserved_name`/`$`/`INDEX_TABLE_SEPARATOR` call site — and
  turned up that the `$` rejection the ADR and the code comment both
  describe **did not exist anywhere in the codebase**. `CreateTableSchema`'s
  apply arm only ever checked `is_reserved_name`; no code path rejected a
  `$`-containing table name. The design had been sound in principle the
  whole time (nothing had ever proposed a `$`-named table, so no real
  collision had occurred), but "sound in principle, unenforced in practice"
  is exactly the gap a design that leans on the same unverified assumption
  could turn into a real, reachable, unenforced case. Fixed at the single
  call site both docs already named. The general move: before building
  anything whose own correctness argument leans on a property another
  ADR/doc claims is "enforced," verify it holds *now*, at the call site the
  doc names — "already enforced elsewhere" is a claim to check, not a fact
  to inherit, even (especially) when it's stated confidently in the very
  document you're extending. (`crates/animus-control/src/meta.rs::
  CreateTableSchema`; ADR 0041 §1's as-built correction, ADR 0042/0043
  round-3 streams salvage, 2026-08-14.)
- **A same-listener "dispatch" fork between two sibling services is a gate
  every shortcut caller must go through, not just the real edge.** ADR
  0042 §3 put the DynamoDB item API and the DynamoDB Streams read API on
  one listener, forked by `X-Amz-Target` prefix in `dynamo::dispatch`. The
  admin dashboard's write proxy (`POST /admin/data/dynamo` →
  `action_data_dynamo`) reused the item API's own decode+run helper
  (`dynamo::execute`) directly — a reasonable-looking shortcut at the time,
  since Streams didn't exist yet — but never went through `dispatch`
  itself. Once Streams landed one layer up, the proxy could build a
  perfectly well-formed `DynamoDBStreams_20120810.*` target and it would
  still 400 as "unknown operation," because it never reached the code that
  checks the prefix. Nothing caught this: the fork is a plain `if` in one
  function, not a match arm on an enum clippy/exhaustiveness can flag, and
  every dispatch-side test call went through the real edge, never the
  admin route. The general shape to watch for: when a request can enter a
  multi-service surface through more than one caller (a real wire edge and
  an in-process admin/test shortcut alike), extract the fork itself into
  its own named function and have *every* entry point call that function
  — never let a shortcut call the fork's *callee* on the assumption "this
  target will always be item-API." Fixed by factoring the `if target.
  starts_with(..)` fork out of `dynamo::dispatch` into `dynamo::
  execute_routed`, then pointing both the edge and the admin proxy at it.
  (`crates/animusd/src/dynamo.rs::execute_routed`; `crates/animusd/src/
  admin.rs::action_data_dynamo`; ADR 0042 §3, 2026-08-14.)
- **A "confirm by re-reading the value I just wrote" helper silently assumes
  the caller can predict that value's key ahead of the propose call — check
  that before reusing it for a new write shape.** `animusd::index_drain`'s
  existing confirm helpers (`cp_kind_write_raw`'s last-write probe,
  `cp_kind_local`'s base-row probe) all poll `local_get_kind`/`local_get`
  for an exact `(kind, key, expected value)` the *caller* chose before
  proposing. The ADR 0045 §2 backfill seeder needed to propose a
  `KvCommand::KindBatch` carrying **only** a change-log entry (no base/kind
  write at all) — and a change-log key's trailing HLC suffix is minted
  *inside* the propose call, under the group's own lock
  (`RaftKvNode::propose_ordered`), specifically so it agrees with the
  entry's log position. There is structurally nothing for the caller to
  predict and poll for. Reusing either existing helper here would have
  meant either faking a probe key (wrong — it wouldn't be the real
  change-log key) or skipping confirmation and acking on `Accepted` alone
  (wrong — ADR 0009's own "`Accepted` means appended, not committed" rule).
  The fix was a new confirm shape: `engine_applied_index() >= index` after
  a genuine `ProposeResult::Accepted { index }` — the same confirm-by-index
  primitive linearizable reads themselves already gate on, not a new
  invention. General rule: a confirm-by-probe helper's soundness depends on
  the caller being able to name the exact key/value the write produces
  *before* proposing it; a write whose own content is decided inside the
  propose call (a minted timestamp, a server-assigned id) needs
  confirm-by-index instead, and the two are not interchangeable by
  accident — check which one the write actually needs, don't default to
  copying the nearest existing helper's shape. (`crates/animusd/src/
  index_drain.rs::seed_change_log_record`; ADR 0045 §2, 2026-08-15.)
- **A generic "one fence covers every kind in this batch" rule is only sound
  for keys whose byte value actually falls inside the tablet's own live
  range — a kind-scoped *bookkeeping* key (not real user data) can silently
  violate that assumption even though it structurally belongs to this
  tablet.** ADR 0045 §2's backfill-seeder cursor advance
  (`ctx.cp_kind_write_raw`, whose fence is always `leader.scope_range()`,
  the tablet's *current live* range) was rejected as "outside this group's
  live range" on *every* real split, at every seed, with no fault injection
  needed — found by `animus-test/tests/backfill_fault_corpus.rs`'s very
  first run of its split cells (`concurrent_split_during_backfill`/
  `split_after_tablet_already_reported_done`), which hung forever ("backfill
  sweep never reached its end after 10,000 ticks") rather than failing
  loudly, because the *data* seed writes (keyed by real base keys, which do
  satisfy the fence) kept silently succeeding while only the *cursor's own
  persistence* failed — restarting the sweep from scratch every tick instead
  of resuming, masked in every prior test by tables small enough that one
  tick's `BACKFILL_SEED_BATCH` (256) covers a whole side in one pass
  regardless. Root cause: `cursor::cursor_key` truncates its `range_start`
  argument to a bare `TOKEN_BYTES`-wide token (`cursor::token_of`) — sound
  for the key's own *disjointness* proof (never collides with a real
  client key) but **not** for range-*containment*: a split's own
  `split_key` is essentially never token-aligned (chosen from real row
  content, never the hash ring), so the truncated cursor key sorts *below*
  the child's own (longer) `range.start` the instant the byte right after
  the token is non-zero — true of `escape(pk)`'s leading byte for
  essentially any real partition key. Fixed by giving the cursor's own
  advance write a dedicated path (`advance_backfill_cursor`, a direct
  `group.put_kind_batch_fenced(.., KeyRange::whole())`, bypassing
  `cp_kind_write_raw`'s auto-derived narrow fence entirely) — a cursor
  row's identity is already fully captured by its own token (disjoint from
  base data by row-kind, ADR 0041 §3) and immutable across a narrowing, so
  it needs no range-fencing at all, the same reasoning `seal.rs`/
  `ceiling.rs`'s engine-global markers already rely on for a *different*
  flavor of range-independent bookkeeping key. **General check: before
  reusing a "fence every key in this batch against the tablet's live
  range" primitive for a *new* kind of key, ask whether that key's own byte
  value is guaranteed to satisfy `range.contains` — a bookkeeping key
  derived from a truncated/hashed/otherwise-transformed version of the
  range boundary is not automatically safe just because it lives in a
  disjoint row-kind scope.** Also worth naming: the pre-existing `"gsi"`
  cursor tag has the *identical* underlying gap (its own cursor write goes
  through the same `cp_kind_write_raw` path) but it is harmless there only
  because that caller already tolerates a perpetually-`None` cursor as "just
  reconcile everything, always correct" — a latent bug can be real for one
  caller and merely a masked inefficiency for another, so "it works today"
  is not evidence a shared primitive is fencing correctly for a new use.
  (`crates/animusd/src/index_drain.rs::advance_backfill_cursor`; ADR 0045
  §2/§3, 2026-08-15.)
- **A convergent per-name cursor/checkpoint row that survives its owner's
  deletion can silently poison a same-named recreation — audit every
  "resume from where I left off" row for whether its *key* is unique to one
  lifetime of the thing it tracks, not just to its current name.** The
  backfill seeder's cursor row (`KIND_CURSOR`, tag `backfill:{index_name}`,
  ADR 0045 §2) is keyed purely by index *name*. Dropping an index (ADR 0045
  §5) removes its catalog entry and hidden table but, absent an explicit
  fix, leaves that cursor row exactly where it was — harmless in isolation
  (the row just describes a scan position for an index that no longer
  exists), until a *later* `CreateTableIndex` proposes a **new** index
  under the **same name**: its fresh seeder reads the old row, believes it
  has "already scanned" up to the deleted index's old position, and skips
  every partition before that point — the recreated index can flip
  `Active` having backfilled *nothing*, a silent, non-crashing correctness
  bug (the row is bytes-valid, just semantically stale). Two considered
  fixes: (1) make the cursor's key incorporate something that changes
  across a delete/recreate of the same name — not chosen here, since it
  would touch the already-shipped `IndexDef` wire shape for a problem step
  (2) closes with zero schema change; (2) **actively delete the row when
  the owner is deleted**, chosen — but a *single* delete pass raced a
  seeder tick that had already read the schema (as still-`Creating`) a
  moment before the owner's deletion committed and could still write a
  fresh, stale value *after* the delete. Closed the practical window by
  running the delete twice (immediately after the status-`Deleting`
  transition commits — after which the seeder's own gate excludes the
  index from every *new* tick — and again at the very end of the drop
  cascade), which is not a full formal proof but is the same
  documented-residual-gap posture `cursor.rs`'s own module doc already
  takes for a different byte-alignment gap; the create-drop-recreate
  regression (`tests/update_table_drop_index.rs::create_drop_recreate_
  same_index_name_backfills_from_scratch`) is the test that would catch a
  regression here. **General rule: whenever a delete path removes the
  *thing* a checkpoint row tracks but the checkpoint's own key could be
  reused by a future recreation of the same name, either scope the key to
  a value that can't repeat, or make deletion of the thing also delete the
  checkpoint — "the row is harmless garbage" is only true until the name
  comes back.** (`crates/animusd/src/index_drain.rs::clear_backfill_cursor`,
  `crates/animusd/src/dynamo.rs::drop_index`; ADR 0045 §5, 2026-08-15.)
- **A fix must cover every path to a dangerous primitive's sink, not just
  the caller that surfaced it.** ADR 0018 §2/PR2b's own
  `next_ceiling_candidate` doc named the hazard precisely: never call
  `Hlc::witness` on a value deliberately shifted `HLC_MAX_OFFSET` into the
  future, because witnessing permanently ratchets the group's shared clock
  toward it, poisoning every later ordinary mint. That fix built a
  separate CAS ratchet for the one call site that surfaced the bug
  (`ensure_ceiling_above`'s ceiling-candidate disambiguation) — but
  `RaftKvNode::mint_pushed` (`crates/animus-cp-data/src/lib.rs`) had its
  *own*, independent call to `self.hlc.witness(floor, ..)` on a floor that
  folded in the same future-shifted ceiling, unconditionally, on every
  write. Nothing caught this for weeks: the two call sites don't call each
  other, so grepping "does `next_ceiling_candidate`'s caller still do this
  right" finds nothing, and the existing amortization test only exercised
  reads, never interleaved reads-and-writes, so it never drove the second
  sink. The bug was a live, self-sustaining feedback loop (a write
  witnesses the ceiling forward, the next read mints near the poisoned
  clock and exceeds it almost immediately, forcing a fresh ceiling
  proposal, which the next write folds in and witnesses again) — a
  k×`HLC_MAX_OFFSET` runaway roughly one window per round, independent of
  real elapsed time, plus propose-path starvation from the resulting
  `ReadCeiling` churn. The general move: when a postmortem or doc comment
  names a primitive as dangerous in a specific way ("never call X with a
  value shaped like Y"), **grep every caller of that primitive**, not just
  the one under investigation — a doc that explains *why* a fix works for
  one call site is not evidence every other call site got the same fix.
  The regression this time had to be a genuinely different shape from the
  existing amortization test (interleaved reads *and* writes on a tight
  loop, asserting the group's clock never diverges from real elapsed time
  by more than a small bounded multiple of `HLC_MAX_OFFSET`) — a read-only
  workload structurally cannot reach a write-side sink. (`crates/
  animus-cp-data/src/lib.rs::mint_pushed`; ADR 0018 §2 amendment,
  `tests/ts_cache.rs::interleaved_reads_and_writes_never_let_minted_
  timestamps_outrun_real_time`, 2026-08-15.)
- **Every key-writing `KvCommand` variant must carry AND enforce an
  apply-time `fence` — an exception "reasoned" safe on a closed-world
  assumption is exactly where the next caller breaks the assumption.**
  `KvCommand::TxnResolve` was the one key-writing variant with no `fence`
  at all, on the theory that "every key here was already fence-checked at
  `TxnStage` time" — true for every in-crate caller, but not something the
  type enforced, and `animusd`'s own coordinator (`ClientCtx::
  recovery_resolve`, misrouting a resolve to the wrong tablet of a split
  table by grouping participants by table name alone, no tablet
  dimension) was exactly the counterexample: the wrong tablet applied the
  resolve for a key it doesn't own, stamped with its own clock onto the
  *same physical key* the owning tablet separately maintains (ADR 0028: a
  table's tablets share one `StorageScope` prefix on a shared engine) —
  an acked write silently and permanently lost. **The general rule this
  generalizes**: when a command variant is deliberately left without a
  safety check present on its siblings, the justification is a claim
  about every *current* caller's behavior, not a property the compiler
  verifies — grep for that same reasoning shape ("X can't happen because
  every caller already ensures it") whenever reviewing why one variant in
  a family lacks a guard the others have, and prefer adding the guard
  (cheap, whole-or-nothing, matching the siblings' own shape) over trusting
  the argument to stay true forever. Practically: **when adding a new
  `KvCommand`/wire-command variant, grep both the relay-gate pattern
  (`is_relayable_command`, `cp_serve_forwarded`, admin filters — a missed
  allowlist entry) and the fence-check pattern (every other key-writing
  variant's apply arm) — a missing fence is a silent data-corruption path,
  not a compile error or an obvious test failure, exactly like a missing
  relay-gate entry is a silent per-process-bimodal flake.** (ADR 0018 §2
  write-loss amendment, torn-pair-fix stack PR3, 2026-08-15.)
- **A trait contract designed for one architecture's semantics becomes a
  silent-failure footgun once a different architecture reuses the same
  trait with different assumptions — audit every return value the new
  architecture's callers now discard.** `StorageEngine::merge`'s "silently
  no-op on a stale/duplicate write" contract (`Result<bool>`, `false` =
  "did not take effect") was designed for the deleted leaderless-AP
  plane's replay-tolerance semantics (ADR 0001, gone under ADR 0019),
  where a stale re-application being silently ignored was exactly the
  intended behavior. The CP data plane (`animus-cp-data`) inherited the
  same trait and the same silent-`false` contract, but its own callers
  (`TxnStage`'s intent write, `TxnResolve`'s commit/abort-restore writes,
  `Cas`'s swap) all assume a write their *own* gating logic already
  accepted genuinely lands — none of them ever checked the returned bool,
  via a blanket `.expect(..)` that only asserted the call didn't *error*,
  never that it *took effect*. This is exactly the shape that let the
  write-loss bug above hide undetected: the merge silently no-op'd (fence
  correctly absent pre-fix, corruption aside) while the caller's own
  control flow (`StageOutcome::Staged`, a resolved commit/abort) had
  already decided the write landed, computed independently of the merge's
  own outcome. **The general check**: when a component built for one set
  of semantics is reused by a component with different ones (a shared
  trait, a shared library function, a shared protocol message), audit
  every return value / outcome the *new* caller silently discards — a
  contract that was safe to ignore under the old semantics may be exactly
  the signal the new semantics needed. The fix here
  (`surface_suspicious_merge_noop`, a metric + capped log, deliberately
  *not* a hard assert — a same-value idempotent WAL-replay re-application
  is a legitimate, expected `false` this distinguisher can't yet tell
  apart from a genuine violation) is a permanent guard against the next
  bug shaped like it, not a full fix for the root cause it happened to
  hide this time. (ADR 0018 §2 write-loss amendment, torn-pair-fix stack
  PR3, 2026-08-15.)
- **Fixing one bug in a multi-bug stack can unmask (or mask) another —
  re-measure the acceptance baseline after every layer, never assume a
  fix's own regression proves the end-to-end symptom is gone.** This
  incident's own three-layer history is the clearest example the repo has
  of this: PR1 (a clock-witnessing runaway) fixed a real bug and made the
  end-to-end torn-pair test's failure rate roughly *unchanged* (a
  coincidental clock-lockstep had been *masking* a second bug); PR2 (a
  read-shape race) fixed a second real bug with its own clean regression
  green at depth, yet the wire-level test's failure rate stayed just as
  high, because a *third*, structurally unrelated bug (this write-loss
  one) was still live underneath both. Each individual PR's own dedicated
  `SimEnv` regression was, correctly, green throughout — proving that
  fix's own protocol-level claim sound — but none of them could have shown
  the composed system was actually fixed, because each targeted a
  different mechanism than the one still causing the wire-level failure.
  **The general practice**: in a multi-bug investigation, treat the real
  wire-level/end-to-end reproduction as the only trustworthy signal for
  "is the user-visible symptom actually gone" — a per-fix unit regression
  proves that fix's own mechanism, never the composition. Re-run the full
  reproduction (not just the new regression) after every layer, record the
  baseline number, and don't stop until it reaches the required bar (here,
  0/20 solo, the strictest baseline in this stack's own history) —
  anything less and a fourth bug could still be hiding under the same
  symptom. (Torn-pair-fix stack, all three PRs, 2026-08-07 through
  2026-08-15.)
- **A point read's on-demand intent resolution can mask a physical
  write-side bug from any test that only ever reads via a point `Get` —
  use a raw physical-storage probe, or at minimum a `Scan`, to prove a
  resolve actually landed.** Both `RaftKvNode::local_get` and a
  linearizable point read resolve a still-`Pending` intent *at read time*
  (`resolve_once_step`/`resolve_decided`) the moment they can determine
  the covering transaction's decided status — which they usually can, since
  the transaction's own `TxnCommit`/`TxnAbort` record is a separate,
  independently-correct write from the per-key resolve this bug breaks.
  A test asserting only "the value reads back correctly" after some fix
  can pass for a completely different reason than the fix working: the
  physical intent never got rewritten to `Committed` at all, and the read
  path quietly served the right answer anyway by re-deriving it from the
  record every time. Caught while writing this incident's own regression:
  an `animus-cp-data` `SimEnv` test asserting on `RaftKvNode::local_get`
  showed a misrouted, fence-rejected resolve as "succeeded" (the *read*
  came back correct) even though the physical envelope tag was still
  provably `Intent`, not `Committed` — the fix was to read the raw stored
  bytes directly (`StorageEngine::get`, checking the envelope's leading
  tag byte) instead of going through any resolve-aware accessor. A `Scan`
  is a partial substitute at the wire level (`resolve_scan_rows` omits a
  row whose transaction it cannot determine is decided, rather than
  chasing it down) — but only a *foreign* record lookup is genuinely
  gated on cross-tablet routing; on a small cluster where every node
  happens to host every tablet (see the next entry), even a `Scan` can
  still resolve on demand via engine co-location, and only a true raw
  physical read is unconditionally trustworthy. (ADR 0018 §2 write-loss
  amendment, torn-pair-fix stack PR3, `animus-cp-data/tests/
  fenced_commands.rs`, 2026-08-15.)
- **A small cluster where every node hosts every tablet (`n == RF`) can
  silently mask a cross-tablet routing bug — a "foreign" record lookup
  that should require genuine cross-node routing can instead succeed by
  accident via same-node engine co-location.** `RaftKvNode::resolve_once_
  step`'s "is this transaction's record local" check reads the record's
  physical bytes directly off `self.storage` using the *querying* tablet's
  own `StorageScope` prefix — which is safe and correct precisely because
  ADR 0028 puts every tablet of one table's replicas on the *same node*
  under one shared engine and prefix, so a "local" read really does mean
  "this replica's own copy." But that same design means that on a
  3-node/RF-3 cluster (the default `bring_up(3, ..)` shape most `animusd`
  integration tests use), literally every node hosts every tablet of
  every table — so a tablet that does *not* logically own a key can still
  physically read another tablet's record through the identically-prefixed
  shared engine on the same node, purely because nothing about placement
  spread them apart. A wire-level regression for this exact write-loss bug
  (`animusd/tests/txn_recovery_participant_spans.rs::recovery_resolve_
  correctly_commits_both_tablets_of_a_two_tablet_transaction`) was found
  to pass identically whether or not the coordinator-side grouping fix was
  present, for exactly this reason — a real fail-before/pass-after
  demonstration of *that specific fix* needs a cluster large enough to
  force the anchor's and participant's tablets onto genuinely disjoint
  replica sets (well beyond a 3-node default), or a lower-level
  `animus-cp-data` `SimEnv` test that constructs the shared-engine shape
  directly without relying on real placement (which is what this
  incident's actual fail-before/pass-after evidence uses instead — see the
  entry above). **When a wire-level integration test is meant to prove a
  cross-tablet/cross-node property, check whether the cluster's own size
  relative to the replication factor could make "every replica has every
  tablet" true** — if so, the test may still be a useful regression, but
  it cannot discriminate the specific bug it was written to catch, and
  that gap should be documented rather than assumed away. (Torn-pair-fix
  stack PR3, 2026-08-15.)
- **Three independent implementations of "what does a consumer offset
  inherit across a split" each got it wrong in a different way, because no
  ADR ever named the shared invariant they were all separately solving.**
  The GSI drain's cursor, the Streams sealer's watermark, and the backfill
  seeder's cursor (ADR 0041/0042/0043/0045) each re-derived split
  inheritance on its own — yielding the #216 split-watermark data-loss bug,
  a backfill-cursor fence rejection + same-named-index-recreation poisoning,
  and the #220 split-seal duplication race, three genuinely different root
  causes converging on the same missing rule: *a consumer offset crossing a
  split must be inherited from a basis frozen at the cut, never re-derived
  live from the parent's later state.* ADR 0046 (the tablet log model) names
  this, and the other invariants like it, explicitly so a fourth offset
  tracker gets checked against a stated rule instead of re-deriving its own.
  **General rule**: when a second near-identical background loop/cursor/
  offset mechanism is about to be built next to an existing one, look for
  the shared invariant first — writing it down once is cheaper than paying
  for its absence a third time. (ADR 0046, 2026-08-16.)
- **A node-local lock cannot protect a cross-node invariant — move the
  evaluation to the node that already serializes the data (its leader),
  don't try to distribute the lock.** `index_aware_write`'s DynamoDB write
  path read a table's prior item and diffed its LSI/change-record image at
  whichever edge node received the request, serialized only by a
  **node-local** `rmw_lock`. Two edge nodes writing the same item never
  contended on that lock — each could read → diff against the same stale
  prior value, and the loser's stale LSI row orphaned forever (nothing
  reconciles a stale LSI row once written; only the GSI drain, being a full
  re-derivation, self-heals). No amount of making the lock "smarter" at the
  edge fixes this, because the edge is fundamentally the wrong place to
  hold it — a lock only serializes callers that actually contend on the
  *same instance* of it, and two different processes never share one. The
  fix (ADR 0046 U3, "evaluate at leader") moves the read-diff-propose
  sequence onto the item's own tablet leader — the one node every write of
  that item reaches regardless of which edge node the client connected to
  — so the identical lock, now held on the leader, actually serializes
  what it was always meant to. General form: before adding or hardening a
  lock to close a race, ask whether every racing caller can actually reach
  the *same* lock instance; if the callers are different processes/nodes
  and the lock is per-process, the lock is decorative, and the fix is to
  move the guarded work to a single point of serialization that already
  exists for other reasons (a leader, a single writer, an owning shard) —
  not to invent a new distributed lock. **A repro for this class of bug
  needs genuine cross-node contention, and unrelated setup noise can mask
  the actual assertion before it ever runs — the fix is to get past the
  noise, not to accept a differently-shaped failure as good enough.** The
  first version of this stack's hammer test hard-panicked
  (`assert_eq!(200)`) on any non-200 write reply; against the unfixed
  baseline it reliably (8/8 runs) failed via a request-level
  `InternalServerError` ("CP kind write did not commit in time" / "relay to
  peer node failed") on the very first write of the very first iteration,
  never by surviving to the intended "assert exactly one live LSI row"
  check. The actual root cause turned out to be a genuine but **separate**
  t=0 race: a brand-new tablet's own Raft group needs its own leader
  election, and every node's tablet-host reconciler needs to observe it
  should host/relay for it, before either edge node's very first write can
  resolve a route — `CreateTable`'s own success only guarantees the
  *catalog* entry committed, not that every node has already reconciled
  hosting for it. The unfixed design's extra edge-to-leader hop (a separate
  forwarded read before the forwarded propose, versus the fix's single
  forwarded RPC) doubled this window's exposure, which is exactly why the
  fix *looked* like it closed the race outright rather than merely
  shrinking a pre-existing, unrelated one. Two changes closed the gap
  between "a failure" and "the failure this test is for": (1) a few
  retried, sequential warm-up writes against a key the race never touches,
  settling routing before the concurrent hammer starts, and (2) making the
  hammer loops tolerate a write's own transient non-200 (counting
  acked/failed rather than asserting on any one outcome — a write that
  times out at *confirm* is not known to have failed at *propose*, so
  asserting on it asserts against an unknown) while gating on a run-health
  minimum (most writes must still land) so a vacuously "healthy" run
  doesn't silently masquerade as a real one. With both in place, the
  **same** unfixed baseline failed differently and far more informatively:
  every one of 7 runs reached the actual orphan-row assertion and failed it
  outright (7/7, 100%), with 2–6 live LSI rows (vs. the expected 1) each
  time, containing verifiably stale `alt` values from iterations neither
  loop's own latest write named. **The general lesson: when a repro's failure
  mode doesn't match the mechanism it's supposed to demonstrate, don't
  rationalize the mismatch as "still legitimate evidence" and move on —
  the mismatch is itself a signal that something upstream of the intended
  assertion is misfiring, and it is usually cheap to isolate and fix
  (here: settle routing first, tolerate transient noise, then assert).**
  (ADR 0046 U3, `crates/animusd/tests/dynamo_index_writes.rs::
  cross_node_racing_unconditional_puts_never_orphan_an_lsi_row`,
  2026-08-16.)
- **A freshly `CreateTable`d table's very first write from a second node can
  race the new tablet's own leader election / that node's tablet-host
  reconciliation — plausibly reachable on `main` today, independent of any
  particular write path.** Found while chasing the above: `CreateTable`
  returning `200` only guarantees the *catalog* entry (the schema/tablet-map
  row) committed on the control-plane leader — it says nothing about
  whether the new tablet's own CP Raft group has finished its (normally
  sub-second) internal leader election, or whether every node's
  tablet-host reconciler has yet observed it should host or relay for that
  tablet. Two edge nodes issuing their very first write to a table
  immediately after `CreateTable` returns can each hit this window and see
  a hard `InternalServerError` (a genuine `relay_request` transport
  timeout, or the propose confirm-poll's own timeout) — not a logical
  rejection, a real failure. Not fixed here (out of scope for the ADR 0046
  U3 stack that found it — flagged for a separate report); a few retried
  warm-up writes reliably get past it, which is itself informative: the
  window seems to close quickly once *any* write succeeds, consistent with
  a one-time per-tablet cost (election + first reconcile tick) rather than
  a sustained instability.
- **A doc comment paragraph whose second line happens to start with `+` (or
  `-`/`*`) fails `clippy -D warnings`'s `doc_lazy_continuation` lint even
  though nothing about the prose is a list** — `rustdoc`/clippy parse `///`
  blocks as Markdown, and Markdown reads a line beginning `+ ` as a new list
  item; every subsequent line of that same paragraph is then flagged as an
  unindented "continuation" of a list item that was never intended to
  exist. Hit writing the ADR 0045 "E1" phantom-seed-record fix: "...closed
  by this PR's `ChangeRecord::seeded` flag\n/// + `animusd::dynamo_streams`'s
  filter):" read as "start a list item `+ ...`," and clippy flagged every
  following line of the same paragraph (not just that one) as mis-indented.
  The fix is prose, not indentation: reword so no line of a doc comment
  starts with a bare list-marker character unless a list is actually
  intended. General rule: when clippy's `-D warnings` gate rejects a doc
  comment you just wrote with "doc list item without indentation," check
  the *first character* of every line in that paragraph for `-`/`*`/`+`
  before assuming the lint is confused — it almost always isn't. (ADR 0045
  follow-up "E1" fix, 2026-08-16.)
- **When one primitive gains an optimization, check its documented siblings
  for the identical gap before assuming it's isolated.** `ClientCtx::
  cp_scan_kind_table` (the LSI `Scan` table-wide fan-out, ADR 0041 §5) never
  threaded its caller's `limit` into each tablet's own `KindScan` — it
  fetched every overlapping tablet's whole matching sub-range and truncated
  once, in the coordinator, after every reply was already in hand. Its
  base-scope sibling `cp_scan` had threaded `limit` all the way to
  `RaftKvNode::local_scan`/`linearizable_scan` since ADR 0023's original
  audit; `cp_scan_kind_table` was added later (ADR 0041 §5) by pattern-
  matching `cp_scan`'s *shape* without carrying forward that specific
  optimization, and nothing caught it because the two are behaviorally
  identical either way — just one wastes wire payload and coordinator
  memory on a table whose per-tablet share vastly exceeds a small `Limit`.
  A parity gap like this survives review precisely because it's invisible
  at the call site and invisible in tests that only check final
  correctness, never per-tablet reply size. **Precise wording matters when
  fixing it**: this is a *per-tablet cap*, not "pushdown" — `StorageEngine::
  scan` has no limit parameter of its own, so a tablet still reads its whole
  matching sub-range off the engine; only the wire reply and coordinator
  memory shrink. Calling it "pushdown" in a commit message or ADR note
  overclaims a reduction in engine I/O that never happened. (ADR 0041 §5
  as-built amendment, 2026-08-16.)
- **A doc-mandated test case ("cover an unresolved intent") can be provably
  inapplicable to the primitive under test — check the primitive's own
  invariants before reaching for test-harness tricks to force the case.**
  Asked to prove `RaftKvNode::local_scan_kind`'s new `limit` truncates
  *after* its intent-drop filter (mirroring `local_scan`'s existing
  ordering), the natural instinct is to scan over a row holding an
  unresolved `Envelope::Intent` and check it doesn't consume a `limit` slot.
  But `local_scan_kind`'s own doc (and `linearizable_scan_kind`'s) already
  states a non-base row-kind scope **only ever holds committed values** —
  only `KvCommand::KindBatch` writes them, and it always commits outright;
  no external test harness constructs an intent there without reaching into
  crate-private construction functions. Forcing the scenario anyway would
  either not compile against the public test surface or would silently test
  something other than the real code path. The regression this repo settled
  for instead documents *why* the case can't arise (in the test's own
  comment) and proves the ordering-relevant contract that legitimately can
  be tested (limit bounds the materialized count, not the raw scan width) —
  a `Some("if the existing harness makes that cheap")`-qualified test
  request is exactly this: adapt or skip with a documented reason, don't
  contort the harness to satisfy the letter of the ask.
- **A trigger's "how due is it" computation and its "is anything pending at
  all" gate are two different questions — conflating them makes the
  expensive one run on every tick instead of only when something's actually
  due.** The DynamoDB Streams seal arm's age trigger (`animusd::index_drain
  ::seal_tick`, ADR 0042/0043) used to call `CpGroup::pending_changes()` — a
  full scan of the `KIND_CHANGE` scope, up to `--stream-seal-bytes`' worth
  of bytes — on *every* tick of *every* streamed led tablet (5×/s at
  `INDEX_DRAIN_INTERVAL`), purely to find the oldest unsealed record's own
  HLC for the age trigger and a backlog metric, even on ticks where neither
  trigger ends up firing. On an idle streamed tablet this was ~100% wasted
  work, and — more importantly — it structurally blocked such a tablet from
  ever quiescing (ADR 0044 phase 1's whole premise). **Fix (ADR 0042 fork
  G, 2026-08-16)**: split the trigger into a cheap existence gate
  (`approx_bytes_kind(KIND_CHANGE) > 0`, an accessor already read for the
  size trigger) and a cheap age basis (`Metadata::last_seal_wall_ms`, an
  O(log n) catalog lookup — "time since this tablet's own last seal," not
  "age of the oldest unsealed record"), with the full scan
  (`pending_changes()`) moved to run in exactly one place: inside the
  branch that has *already decided* to seal.

  **A never-sealed tablet (no catalog row yet) needed its own basis, and
  the first two designs tried for it were both wrong in the same general
  way, caught only by a real multi-node `ProdEnv` test.** Design 1 seeded a
  bare driver-local "now" timestamp the first tick a tablet was ever seen
  with nonzero bytes. This is wrong for a split child: the backlog it
  inherits is *physically* whatever its parent hadn't sealed yet (the
  shared engine only narrows the declared range at split time; it never
  touches the records), so seeding "now" silently forgets how old that
  inherited backlog already was — understating its age, and, since ADR
  0034's auto-split runs on its own fixed interval independent of sealing,
  *compounding* across a cascade of splits (each further child restarts its
  own clock from "now" too). Design 2 tried to patch this by having a new
  child inherit its parent tablet's own memoized basis from the same
  driver-local map — which is *also* wrong, more subtly: the fallback map
  is per-node, in-memory state, but which node leads a split's child is a
  **placement** decision, completely independent of which node led the
  parent. In a real cluster a child is routinely led by a different node
  than its parent, and that node's own map has never even heard of the
  parent tablet — the "inherit from the map" lookup silently misses and
  falls back to "now" anyway, reproducing design 1's bug exactly whenever
  placement happens to split leadership across nodes. Both designs passed
  every single-node unit test in the sealer-tests matrix (which structurally
  cannot exercise cross-node placement) and both went red/flaky under
  `streams_e2e.rs::manual_split_with_unsealed_backlog_under_production_
  seal_knobs` — a real 3-node cluster test that pre-dated this fork and
  was not intentionally being touched by it, first deterministically (same
  node, wrong basis) then intermittently (~50%, once fixed for the same-node
  case but still wrong cross-node). **The actual fix**: a *one-time* real
  `pending_changes()` scan of the true oldest pending record's own HLC, run
  only the first tick a tablet is ever observed with a nonzero, never-sealed
  backlog, memoized from then on. This reads the real data — correct and
  identical regardless of which node leads which tablet — while still
  eliminating the overwhelming majority of the original cost: once per
  tablet's entire lifetime (between "created" and "its first seal ever
  commits") instead of once per tick forever.

  **General rule 1**: when a periodic trigger's "should I act" evaluation and
  its "what should I act on" data-gathering are the same function call,
  check whether the gate can be answered from something already computed
  for a *different*, cheaper trigger sharing the same tick — if so,
  restructure so the expensive gather only runs inside the branch that
  already decided to act, and prove it by construction (make the expensive
  call unreachable from the idle path, not merely "unlikely" to be reached).

  **General rule 2, the more important one**: *a value that must remain
  correct across a leadership or placement change cannot be reconstructed
  from purely driver-local, per-node, in-memory state* — not even by having
  the new owner "ask the old owner's own local state," since the new owner
  is a different process that may never have run on the same node as the
  old one at all. If the true value isn't cheaply available from replicated
  state, either accept a bounded, one-time real read to establish it
  correctly (as done here — "run the expensive path once, memoize, never
  again" is a very different cost profile than "run it every tick forever"
  and is often an acceptable trade against a from-scratch replicated-field
  design), or make the imprecision's failure mode explicit and *safe* (here,
  it would have needed to bias toward firing *too early*, never too late —
  a plain "now" timestamp biases the wrong direction, toward firing *late*,
  which is what actually broke the test). A single-node or single-process
  test harness cannot catch this class of bug at all; it needs a real
  multi-node integration test exercising actual placement, which is exactly
  why `manual_split_with_unsealed_backlog_under_production_seal_knobs`
  (pre-existing, not written for this fork) caught what the fork's own new
  unit tests could not.

  See `crates/animusd/src/index_drain.rs`'s `seal_tick` doc and ADR 0043
  §A3's own amendment for the full design, and `Metric::StreamSealBacklogMs`'s
  doc for the accepted metric-semantics change this required (level metrics
  that reuse a trigger's own working data inherit that trigger's own
  precision trade-offs — document the change on the metric itself, not just
  in the code that produces it).

- **A staged intent in a scope whose only reader skips intents is a
  silent-loss mechanism, not a visibility delay.** The obvious-looking fix
  for `TransactWriteItems` on an indexed/streamed table was "stage the LSI
  row / change-log record as an intent in its own kind scope, resolved
  later, the same way a base row is staged today" (recorded as the planned
  design in ADR 0041 §2/ADR 0042 §16 before this was built). It looks like
  ordinary eventual consistency — "the row appears a little later, once
  resolved" — but it isn't: every consumer of a kind scope (the GSI drain,
  the Streams sealer, the backfill seeder) scans **forward from a
  watermark** and is *defined* to skip an intent outright (only a
  base-scope reader ever resolves one — `RaftKvNode::local_get_kind`'s own
  doc states the invariant it relies on: "these scopes only ever hold
  committed values"). A record staged at `ts=10` and resolved at `ts=40`,
  after a consumer's watermark has already passed 10, is gone forever —
  not late, not stale, **never delivered, with no error**. The fix
  (`docs/adr/0046-tablet-log-model.md`'s Decision 2/materialize-at-resolve,
  ADR 0018 §2's 2026-08-16 amendment) rides the derived payload inside the
  *base* write's own intent instead, materializing it at resolve in the
  same atomic apply that finalizes the base value — kind scopes never
  gain an intent-resolution step at all. **General form**: before staging
  anything as an intent in a scope, check whether that scope's readers are
  built to resolve one; a scope whose entire contract is "no intents here"
  cannot safely grow one just because the writer's *other* scope (the base
  row) already tolerates staging.

- **A test corpus's own workload mix can reproduce the exact bug class an
  invariant is built to catch, from the harness alone, with the mechanism
  under test fully correct.** Adding a `kind_consistency` check to
  `animus-test/tests/txn_serializable.rs` (every committed transaction's
  derived `KIND_LSI` row must equal its own base row) failed immediately,
  consistently, for 3 of 9 keys — looking exactly like "a committed
  transaction's kind write silently lost." The actual cause: the corpus's
  write-only and read-modify-write transaction shapes both append to the
  *same* client-owned keyspace, and only the write-only shape's own writes
  had been given a kind payload — an RMW-authored append correctly updated
  the base row but (by design, at the time) carried no kind payload at
  all, leaving the derived row one commit stale. Diagnosed by reading the
  raw stored envelope (tag byte + version) directly off both the base and
  kind physical keys — confirming *both* were durably `Committed` (not one
  merely inferred at read time from a still-`Pending` intent, which was
  the first, wrong hypothesis) but at different versions, proving two
  independently-committed writes rather than one delayed resolve. **General
  form**: when extending an existing multi-shape workload harness with a
  new payload on only one shape, audit every *other* shape that can touch
  the same keys — a corpus's own test-design gap reproduces as a false
  positive that looks identical to the real bug the invariant exists to
  catch, and the fastest way to tell them apart is a raw storage-layer read
  that distinguishes "durably committed, wrong content" from "still
  pending, inferred at read time." (2026-08-16, `TxnStage` kind-writes
  stack PR3.)

- **Holding a node-local lock across a call that can recurse back into the
  same lock, on the same node, is a self-deadlock waiting for the one
  deployment shape that makes the recursion local.** `dynamo.rs::
  run_transact` held `ctx.data().rmw_lock` across its entire span,
  including the `cp_txn` call at the end — safe for as long as `cp_txn`
  never itself tried to take `rmw_lock`. Adding kind-write-path evaluation
  (`eval_kind_txn_write`, inside `ClientCtx::txn_stage_local`) gave it
  exactly that: a *second* acquisition of the same lock, reached the
  instant a write targets a table whose tablet leader is hosted
  **on this same node** — true for every combined-role/single-node
  deployment, i.e. most local dev and every single-node test. A
  `tokio::sync::Mutex` is not reentrant, so this is not a rare race; it is
  a guaranteed hang the first time the code path is exercised on the
  deployment shape that makes it local. Found immediately by a real
  `ProdEnv` integration test hanging (not a `SimEnv` corpus, which cannot
  express real-thread self-deadlock at all — see this doc's own "a flaky
  `ProdEnv` test is a real bug" entry). **General form**: before adding a
  new call inside a function that already holds a lock across its own
  return path, check whether the new call's *own* call graph can reach the
  identical lock — "it never has before" is not evidence it never will
  once the new code path funnels a same-node case through it; scope the
  guard to the exact span that needed it, not the whole function, unless
  every downstream call is provably lock-free. (2026-08-16, `TxnStage`
  kind-writes stack PR2.)

- **A `leader_hint`-shaped field grows a second meaning the moment a second
  network segment exists.** Adding the intra-cluster port (ADR 0047) meant
  `ControlHandle::leader_addr_hint()`/`RemoteControlClient.leader_hint`
  suddenly had **three** existing consumers wanting different address
  flavors off the same field: `propose_schema`'s relay preference
  (machine-to-machine, wants the new intra address),
  `not_leader_error`'s human-facing "retry on {addr}" message surfaced
  through the admin HTTP endpoint (must stay the client address — a human
  operator dials it), and the dashboard's own leader-hint display (same,
  explicitly documented as "the client-API address"). A naive
  find-and-replace repoint would have silently broken the two human-facing
  consumers on any `ControlHandle::Remote` node. Fix: add a **parallel**
  hint (`intra_leader_hint` alongside `leader_hint`) rather than repoint the
  existing one — before repointing any existing hint/route field to a new
  address flavor, audit **every consumer's intended audience** (human
  operator vs. machine relay), not just its current call sites. Standing
  rule this established: machine relay → `intra_leader_hint`; anything a
  human reads → `leader_hint`. (2026-08-16, ADR 0047 intra-port-split
  stack.)

- **A "seed a static route table, then let a sync loop overlay
  `Metadata` on top" pattern needs a real, non-empty static seed if any
  consumer resolves through it *synchronously*, before the loop's first
  tick.** Adding `intra_route` (ADR 0047, mirroring the pre-existing
  `client_route`/`route_sync_loop` shape) first tried an empty static seed
  on the theory that the sync loop's 200ms cadence would converge it from
  `Metadata` quickly enough — reasonable for most consumers, which
  tolerate "not yet known, retry." It broke the growth-node/join-node
  mirror's own seed-building (`start_with_streams`'s `ctx.intra_addr(id)`
  call, feeding `remote_metadata_sync_loop`), which runs **synchronously**
  at ctx-construction time and captures its `seeds` argument once, by
  value, at spawn time — an empty seed there is permanent, not
  transient, since the loop it feeds never re-reads the route table itself.
  Fix: thread the real seed (`intra_route: BTreeMap<NodeId, SocketAddr>`)
  as a full sibling parameter everywhere `client_route` already is,
  including through `ClientResponse::JoinInfo`. **General form**: when
  copying an existing "static seed ∪ replicated overlay" pattern for a new
  address axis, check whether *every* consumer of the new table reads it
  lazily (tolerates emptiness) or captures a value from it once,
  synchronously, at construction time — the latter needs the seed
  populated for real, not deferred to the first tick.

- **A `Surface`/authorization reclassification's test blast radius includes
  every direct low-level construction of the reclassified variant across
  the whole `tests/` tree, not just its production call sites.** ADR 0047
  reclassified `ProposeSchema`/`WatchMetadata`/`Forwarded` (and friends) as
  intra-only; the production retargeting was contained, but ~15
  pre-existing test files drove one of these variants directly against a
  node's `client_addr()`/`.client` as a test-setup shortcut (most commonly:
  hand-driving schema DDL without going through the DynamoDB/CQL edge) —
  found only by running the full suite, not by inspecting the production
  diff. **General form**: before estimating the blast radius of
  reclassifying a wire-protocol variant's reachability, `grep` every
  direct construction of that variant across `tests/`, not just its
  production callers — a test helper reusing the client address for
  convenience is a real consumer of the old classification, indistinguishable
  from inspection alone from one that doesn't need to be.

- **A "no local replica, forward blindly to whoever else has one" fallback
  path is easy to miss when retargeting an address-resolution axis, because
  it looks like an edge case rather than a primary path.** ADR 0047
  retargeted every named machine-relay resolver (`cp_leader_hint`,
  `other_tablet_replica_addr`, `propose_schema`'s relay/broadcast) to the
  new intra routing table, but missed `resolve_cp_route`'s own
  zero-local-replica fallback (the very first guess a node with no local
  replica of a tablet makes) — it kept reading `route_snapshot()` (client
  addresses). Invisible by code review (nothing about it looks
  forwarding-specific at a skim), it surfaced immediately as a real
  `ProdEnv` test failure once a control-only node tried to forward a write
  (`cluster_split.rs`'s `single_shot_first_write_through_control_node_
  succeeds`): `Error("forwarded is a cluster-internal request; send it to
  this node's intra port")`. **General form**: when retargeting "which
  address flavor answers a forwarding question," grep every function whose
  *return type* is an address/route candidate (not just the ones with
  "leader"/"forward" in the name) — a last-resort/degenerate-case branch
  inside a bigger routing function is exactly where a mechanical retarget
  misses a spot. (2026-08-16, ADR 0047 intra-port-split stack.)
- **A validation/transformation rule enforced at only SOME of several call
  sites that all build the same command is a gap, not redundancy — move it
  to the one function every caller actually funnels through.** F11 (ADR
  0042 §14, a streamed table's split key must round down to its own token
  boundary) was implemented inside `auto_split_loop` only; the two manual
  paths (`POST /admin/tablet/split`, `ClientRequest::SplitTablet`) both
  called `ClientCtx::trigger_split` directly with the caller's raw key,
  bypassing the rounding entirely — a manual split on a hot partition could
  silently separate one token's own records across two **sibling** tablets
  with no parent/child relation, the exact per-item ordering violation the
  rule exists to prevent. The fix (growth PR2) generalizes: **grep every
  call site that builds the guarded command** (here, every caller of
  `trigger_split` — there were exactly three), confirm they all fall through
  one shared function, and move the rule INTO that function rather than
  duplicating it at each site (which just recreates the same gap the next
  time a fourth caller appears). Add the apply-time seatbelt (the ADR 0028
  fence idiom) as defense in depth, not as the primary fix — a structural
  check at apply guards against a *future* bypass of the choke point, but a
  caller that reaches apply through the guarded rule was never the bug.
  Corollary the fix also had to handle: rounding a key can produce a
  **degenerate** result (here, collapsing onto the tablet's own
  `range.start` when a single hot token owns the whole tablet) that the
  underlying command legitimately rejects — decide up front whether that's
  an error (a manual, explicit caller should hear about it) or an expected,
  metered no-op (a periodic background trigger retrying forever should
  neither spam a warning nor silently loop with no signal at all); conflating
  the two callers' needs into one behavior gets one of them wrong.
  (2026-08-16, `growth/1-f11-fence`.)
- **A propose-and-confirm retry loop's confirmation predicate must prove
  THIS call's own effect, not merely "something with the expected shape now
  exists" — an allocator-derived id computed from a possibly-stale read can
  collide with a *different* concurrent command's id, and a confirmation
  that can't tell them apart silently reports success for a proposal that
  was actually rejected.** `ClientCtx::trigger_split` (`animusd`) computed a
  new tablet id once (`next_free_tablet_id()` from one metadata snapshot),
  proposed `SplitTablet`, then confirmed via `tablets.contains_key(&new_id)`
  — reasonable in isolation, but growth PR3's `grow_stream` calls
  `trigger_split` for **two different source tablets** in quick succession,
  each independently computing `new_id` from its own (possibly differently
  stale, possibly cross-node-forwarded) metadata read. When both attempts
  computed the identical `new_id`, the control leader correctly rejected
  the second as "id already exists" — but that second call's own
  confirmation loop kept polling, and the moment the FIRST call's real
  commit replicated to it, `tablets.contains_key(&new_id)` turned true and
  it reported `PutOk` for a split that never happened (only one of the two
  source tablets actually gained a child). Found by a real-cluster test
  going from "2 tablets → should be 4" to "2 tablets → 3," flaky at roughly
  50%. The fix: recompute the allocator-derived id fresh on every retry
  (so a corrected later attempt, once this node's own metadata catches up,
  mints a genuinely free id) and confirm via a property **intrinsic to the
  call's own target** — here, the source tablet's own epoch having
  advanced past what was observed at the start (the CAS-gated apply arm
  only ever bumps it on a real committed split of that exact tablet) —
  rather than a derived, potentially-colliding id. **General form**: when a
  retry loop proposes command X and separately polls "did X's effect
  land," ask whether the poll can also be satisfied by some OTHER command's
  effect that merely looks the same from the outside; if the answer isn't a
  clean no, the confirmation is checking the wrong thing. (2026-08-16,
  `growth/2-stream-grow`, `ClientCtx::trigger_split`.)
- **Converting an unconditional idle poll into a wake-on-X signal requires
  enumerating every *state transition* that creates the work, not every
  *call site* that might seem related — and some of those transitions live
  entirely inside a different subsystem's own timer, with no shared event to
  hook.** The apply task's `APPLY_IDLE_POLL` (ADR 0044 phase-1 PR1,
  `animus-cp-data`) looked at first like it needed a signal wherever
  `RaftCore::apply` could run (a `mark_durable_through` call, a follower's
  in-line apply inside `handle`, a completed snapshot install's commit-index
  jump, a single-node group's own commit-advancing propose) — all genuinely
  correlated with `commit_index` advancing, so one before/after comparison
  after stepping the core plus one call at `mark_durable_through` covers all
  of them. But `RaftCore::take_snapshot_needed` — the lazy on-demand
  snapshot-image-build request the apply task must also notice — is set by
  `snapshot_chunk_for`, reached from the leader's ordinary
  heartbeat/replicate cycle discovering a reconnected follower's log has
  been compacted away, with **no commit advance anywhere in that step**: it
  is purely a consequence of the consensus loop's own timer, which the apply
  task has no reason to know about. Trying to add a fourth explicit signal
  point for this would mean piping a new plumbing path across the two-task
  split for one rare, already-bounded case. **General rule**: after wiring
  the signal for every transition you can name, ask whether a transition
  exists that's driven by a *different* loop's own timer/tick with no data
  dependency the signal's owner can observe — if so, don't chase it with more
  plumbing; keep (or add) a bounded safety-poll fallback and prove
  convergence through it with a test, which is both simpler and strictly
  safer than an enumeration you can't be sure is exhaustive. (2026-08-16,
  `quiesce/1-apply-signal`, `tests/apply_signal.rs`'s
  `apply_converges_via_safety_poll_on_a_signal_less_snapshot_build`.)
- **A pure core method with no `now` parameter can't record "this happened at
  time X" itself — give the driver a companion method to call, don't widen
  the core method's signature.** `RaftCore::propose`/`change_membership` take
  no `Nanos` (proposing/reconfiguring never needed wall-clock time before ADR
  0044 phase-1 PR3's `last_activity` idle-clock), and both are called from
  dozens of test files across two crates plus every production driver — so
  adding a `now: Nanos` parameter to either to let them bump
  `last_activity`/clear `quiesced` inline would have rippled through all of
  them for one new feature's benefit. Instead, `RaftCore::note_local_activity
  (now: Nanos)` is a tiny, separate, `now`-taking method the *driver* (which
  already has `now` at every call site that matters) calls immediately after
  confirming `ProposeResult::Accepted`, inside the same held `core` lock —
  `become_leader`/`transfer_leadership` do the equivalent inline since they
  already take `now`. **General rule**: when a new feature needs a
  time-stamped side effect from an existing widely-called pure method that
  doesn't carry the needed input, don't widen that method's signature for
  every caller — add a narrow companion method the *caller* invokes with the
  input it already has, at the one or two call sites that actually need the
  new behavior. (2026-08-16, `quiesce/3-core-state-machine`.)
- **A "reconciler-maintained latch" closing a stale-read window is only
  sound if it's checked against something at least as fresh as the read it
  guards — a periodically-updated flag derived from the reconciler's own
  tick cadence is not, no matter how the plan phrases it.** Found delivering
  ADR 0044 phase-1 PR4's `hot_read` scope-transition latch (the ADR 0043
  residual): the literal ask was a boolean the reconciler sets when it
  first notices a scope mismatch and clears once `narrow_scope` executes.
  Since detection and execution happen in the *same* tick
  (`Reconciler::tick`'s `gather_facts` → `plan` → execute is one atomic
  pass with no other writer), such a flag is false throughout the entire
  window that actually matters — from the moment a split commits in
  `Metadata` until this replica's *next* tick even starts (bounded by
  `metadata_watch` wake latency plus, worst case, a 500ms fallback poll) —
  because the reconciler cannot raise a flag for a change it hasn't
  observed yet. The sound fix skipped the "maintained flag" entirely and
  cross-checked a value that has **no observation lag by construction**
  (`RaftKvNode::scope_range()`, current the instant the reconciler mutates
  it) against the **freshest obtainable** comparison point
  (`metadata_fresh()`, never `effective_metadata()`/`metadata_cached()`) —
  no new shared state, no periodic-refresh lag to reason about. **General
  rule**: when a design calls for "a component maintains a flag reflecting
  some external fact," check whether the flag-maintainer's own update
  cadence has a lag the flag's consumer can't tolerate; if the maintainer's
  *state* (not a derived boolean about that state) is already live and
  read-only-safe to expose, prefer exposing the state itself and letting
  the consumer do a live cross-check over inventing a flag that inherits
  the maintainer's own staleness window. See ADR 0048 and ADR 0043's
  residual section for the full incident and the D8 before/after evidence.
- **A level-gauge `Metric` sampled across every owner sharing ONE
  `MetricsHandle` sink cannot be maintained by per-owner increment/decrement
  — it needs a single periodic re-count.** Adding `Metric::CpGroupsQuiesced`
  (ADR 0044 phase-1 PR7, "how many of this node's hosted CP groups are
  quiesced right now"): every tablet group on a node shares the *same*
  `MetricsHandle` (one per-node env sink, ADR 0026), so a naive "each
  group's own consensus loop increments on quiesce, decrements on wake"
  would have every group blindly mutating one shared counter with no
  coordination — correct only if every transition from every group is
  captured exactly once, which is unverifiable from any single group's own
  vantage point. Counters that record a *transition* (`CpQuiesces`/
  `CpUnquiesces`) are fine as per-owner increments (each transition really
  is independent and additive); a gauge that claims to reflect *current
  aggregate state across owners* is not, and needs one periodic sampler
  with a view across every owner (here, `metrics_sample_loop` walking
  `ClusterEdgeState::hosted_groups()` and calling `MetricsHandle::set`
  once) rather than N independent mutators each guessing at the whole.
  **General rule**: before wiring a level gauge via per-event increment/
  decrement, check whether every event source shares one sink — if so, a
  single periodic re-aggregation is both simpler and the only version
  that's actually correct.
- **Making a conditional predicate constant-true silently universalizes
  every branch keyed on it — re-key each one to the property it actually
  meant, and grep the old predicate's consumers for documented
  instabilities first** (ADR 0049 Train A rung 1 fixup 2, 2026-08-16). ADR
  0049 flipped `table_takes_kind_write_path` to constant-true so every
  table gets a change log. But `cp_txn`'s awaited-bounded resolve branch
  was keyed on "stages any pending kind write" — a faithful proxy for that
  predicate — so every plain-table transaction silently moved onto the
  awaited `resolve_all_parallel` configuration. That exact configuration
  was already documented, in `resolve_all_parallel`'s own comment, as
  reproduced-red on `dynamo_txn`'s torn-pair hard-gate test when applied
  universally ("green again scoped like this") — and the hard gate duly
  went intermittently red again (2/7 solo here; a budget-expired ack racing
  the writer's next same-key stage into `TXN_STAGE_PUSH_ATTEMPTS`
  exhaustion), bisected across three rungs before the cause was spotted
  sitting in a comment nobody re-read. The fix re-keys the branch on the
  property D1's rationale actually names (`table_change_records_carry_
  images` — an index/stream consumer exists whose visibility the await
  protects), restoring the proven-stable fire-and-forget sequential
  resolve for every marker-only transaction. **General rule**: a
  universalization change's review checklist must include every branch
  keyed on the predicate (or its proxies — here `pending.is_some()`), and
  each one either genuinely wants the universal behavior or gets re-keyed
  to the narrower property it was really about; the documented caveats of
  the path being universalized (ADR text and load-bearing code comments
  alike) are the first place to look for what will break.

- **A forwarded RPC's serve arm must run the SAME confirm implementation as
  the caller's own local arm — two implementations for one RPC diverge the
  moment a new payload shape arrives, and the failure is
  leader-placement-bimodal.** `cp_kind_write_raw`'s local arm confirmed a
  raw kind batch on its *last* write, tolerating a tombstone (`None`)
  probe; `cp_serve_forwarded`'s `KindWrite` arm confirmed the identical
  batch via `cp_kind_local`, whose confirm *requires* a `Some`-valued base
  write. The two agreed for every payload shape that existed when they were
  written (the GSI drain's cursor/footprint puts) and disagreed on the
  first new shape (ADR 0049 Train A rung 2's whole-partition CQL DELETE — a
  batch whose base write is a tombstone): the delete succeeded iff the
  serving node happened to lead the tablet, an election-dependent bimodal
  failure that one pre-existing e2e (`cql_clustering`) only caught by luck
  of leader placement. Two lessons: (1) when a request can be served
  locally or forwarded, extract the serve body into ONE function called
  from both arms (`ClientCtx::cp_kind_raw_local`) — the local/forward split
  is transport, never semantics; (2) the existing "every internal RPC needs
  at least one non-leader-issued call in its suite" rule applies per
  *payload shape*, not per RPC — a new shape through an old RPC needs its
  own follower-connected regression
  (`cql::cql_kind_write_tests::cql_whole_partition_delete_serves_from_every_node`,
  red on the two-implementation code with exactly the diagnosed refusal).
- **A change-log consumer's resume cursor must be a commit-order (HLC)
  watermark, never a key-position cursor** (ADR 0050 rung 4, the
  split-build tail). `pending_changes`' key order is prefix-then-HLC, NOT
  commit order — a later write to a *lower* prefix inserts *below* any
  key-position cursor and is skipped forever. The sealer learned this once
  (its load-bearing re-sort in `seal_now`, recorded only as a code
  comment); the split-build driver re-made the identical mistake with a
  "resume after the last key I saw" cursor, caught red by its own e2e
  (`split_build.rs`, 4 of 16 racing writes silently missing while the
  build reported converged). Within one tablet, HLC order IS commit order
  (`assert_ts_monotonic`), so filtering the scan by a packed-HLC watermark
  (the key's own trailing 8 bytes) is complete where any key cursor is
  not. Advance the watermark only after the tick's work fully succeeds, or
  a failed ship loses its dirty set. General form: before giving any
  key-ordered scan a positional resume cursor, ask what order NEW entries
  arrive in — if insertion order ≠ scan order, a positional cursor is a
  silent-loss bug.
- **A success ack that confirms a metadata commit does not confirm the
  asynchronous machinery that commit triggers — a client-facing "created"
  reply must wait for *serveability*, not hand the client the formation
  window.** `CreateTable`'s 200 (DynamoDB edge; CQL's `CREATED` result had
  the identical shape) waited only for the `CreateTableSchema`/
  `CreateTablet` commits; the tablet's Raft group then forms and elects
  asynchronously (each replica's tablet-host reconciler → election, ≥ one
  election timeout), so a client's immediately-following first write landed
  inside that window and only succeeded via the election-wait machinery
  (`cp_forward`'s backoff pass / the local `RouteDecision::Wait`) — burning
  much of `CLIENT_TIMEOUT`, or failing outright, under unlucky timing. The
  root fix is a converged-or-timeout wait on the *served* property itself
  (`ClientCtx::await_table_serveable`: a linearizable probe read through
  the ordinary routing machinery — ReadIndex success implies an elected
  leader with quorum contact, so "readable" covers "can commit a write
  promptly" too), never a longer client timeout, and never a wait on a
  *proxy* (leader-hint gossip, reconciler state) that can diverge from what
  a real routed request experiences. The regression test's load-bearing
  assertion is deliberately **one-shot at ack time**
  (`tests/create_table_ready.rs`): the property under test is "already
  true when the reply arrives", so a poll there would mask exactly the
  race being pinned — the inverse of the usual converged-or-timeout rule,
  which governs *eventual* properties, not ack-implied ones. General form:
  when an API reply means "you can now use X", the reply path must itself
  exercise X the way a client would; auto-provision paths whose own next
  op already rides the waiting machinery (`cp_route`) need no such gate.
- **A primitive documented "single-waiter by design" is a silent
  lost-wakeup hazard waiting for its second consumer — audit every consumer
  before sharing its handle, or better, make it multi-waiter the moment a
  second one shows up** (issue #276, 2026-08-18). `animus-control`'s
  `MetadataWatch` (ADR 0031) started as a single `AtomicWaker`-backed
  watermark, correct for its one designed caller (the per-node reconciler)
  and documented as such on the type and in the ADR. ADR 0035 PR5 later
  handed the very same handle to a second, independent concurrent consumer
  — a combined-mode node's `WatchMetadata` RPC long-poll — without anyone
  re-checking the single-waiter contract. `AtomicWaker::register` silently
  *evicts* whatever waker was previously registered rather than erroring or
  queuing, so the reconciler loop's own periodic re-registration (every
  `RECONCILE_FALLBACK_INTERVAL`) would evict a parked long-poll's waker; the
  evicted waiter never got woken by the real commit and only ever resolved
  via its own independent fallback timeout (8s). The result reads as
  **bounded-but-large latency**, not a crash or a wrong answer — exactly
  the shape that gets misread as scheduler contention or CI runner
  slowness instead of a lost wakeup, because the symptom (a slow-but-legal
  reply) has its own innocent-looking explanation and the fallback timeout
  makes it *look* like the safety net working as intended. Fix: made
  `MetadataWatch` genuinely multi-waiter (a `Mutex<BTreeMap<u64, Waker>>`
  slot registry, one slot per parked `changed()` future, removed on
  `Drop`) instead of trying to re-establish single-consumer discipline by
  convention or a doc comment, since the next handle-sharing change would
  just break it again the same way. General form: when a change hands an
  existing "intentionally single-waiter" handle to any additional caller —
  even one that looks read-only, even one added for an unrelated feature —
  grep every existing consumer of that handle first; if more than one
  concurrent waiter is now possible, the primitive itself needs to become
  multi-waiter, not merely re-documented. (`crates/animus-control/src/
  node.rs`, `crates/animusd/src/lib.rs::watch_metadata`.)
- **A confirm-wait fast-fail bounds per-attempt latency but doesn't make it
  safe to hold a serialization lock across the wait — when a lock is
  provably redundant with an apply-time check, scope it to read+eval only**
  (issue #285). `dynamo::kind_write_item_at_leader` held `ctx.data().
  rmw_lock` (one lock per node, shared by every table/tablet this node
  leads) across its own read *and* the full `cp_kind_local` propose+
  confirm-poll — so one item's slow confirm (apply backlog stretches this
  even with the #268 `confirm_wait_is_futile` fast-fail, which only bounds
  *this* attempt, not how long that attempt takes to even resolve under
  load) stalled every *other* evaluated write on the node behind it, not
  just racing writes of the *same* item. The lock was never the thing
  making concurrent writes of one item safe in the first place — the
  apply-time OCC seatbelt (`KindBatch.conditions`, checked byte-for-byte
  against the actual committed value on every replica) already had to work
  lock-free, since `txn_resolver_loop`'s recovery pushes never take this
  lock at all. Once a lock is provably redundant with an apply-time check
  like this, its only remaining job is a same-node collision-rate
  optimization, so it only ever needs to span the read+evaluate that
  produces the value the check is *based on* — never the propose/confirm
  that verifies it. **The scoping pattern already existed one function
  away**: `ClientCtx::txn_stage_local` takes the identical `rmw_lock` only
  around its own read+evaluate loop, dropping it before staging — grep
  sibling functions touching the same lock/primitive for an already-
  established narrower scoping before assuming a wider one is the
  house convention just because it's what you found first.
  (`crates/animusd/src/dynamo.rs::kind_write_item_at_leader`.)
- **A `"; retry"`-suffix convention is opt-in per caller, not a property of
  the error itself — unifying call sites onto a shared primitive can
  silently drop a retry loop that used to live at a since-deleted higher
  layer** (issue #288). `FROZEN_REFUSAL` (ADR 0050's split-cutover freeze
  refusal) is emitted deliberately in the house `"; retry"` shape, and the
  low-level primitives that can hit it (`cp_kind_local`, `cp_kind_raw_
  local`, `seed_rows_local`) all return it correctly. But *retrying* on
  that suffix is something each caller has to opt into by actually writing
  a loop — it doesn't happen automatically just because the string ends
  the right way. `ClientCtx::cp_kind_write_item`/`cp_kind_write_raw` (the
  two caller-facing entry points every Dynamo/CQL/raw-protocol write funnels
  through since ADR 0049's write-path unification) were each a single
  `cp_route` + one attempt, no loop at all — so a write racing a split's
  freeze window got a terminal 500 instead of the retry every *other*
  retryable-error caller in this file performs. The bug likely predates
  the unification: an older, now-deleted higher layer plausibly retried
  this for the plain-write path, and folding every write shape onto one
  shared low-level primitive (rung 1 of ADR 0049) preserved the primitive's
  own correct error shape while dropping whatever retry loop used to wrap
  it above. **Audit method that would have caught this**: don't trust a
  doc comment's claim about retry behavior (or an issue's own premise —
  this one *also* wrongly assumed the plain-write arm already retried,
  when it never had coverage either way) — trace each entry point down to
  its terminal single-attempt primitive and grep for an actual `loop { ...
  }` shape wrapping the call, the same discipline `cp_read`'s own
  deadline-bounded loop already demonstrates as the house pattern. A
  refactor that unifies several call sites onto one shared implementation
  is exactly the moment a caller-side concern (retry, backoff, dedup) that
  lived above the old, now-deleted per-shape code paths is most likely to
  quietly vanish — grep for it explicitly rather than assuming the
  unification preserved it.
  (`crates/animusd/src/lib.rs::cp_kind_write_item`, `cp_kind_write_raw`.)
- **A batched fast path plus an unbatched incremental path over the same
  data is a performance bug waiting for its first big input — the
  incremental one silently costs one round trip per ITEM where its sibling
  costs one per MEGABYTE** (ADR 0050's split build, 2026-08-19). The bulk
  copy pass batched rows into 256 KB `SeedBatch` chunks; the tail pass that
  chases writes arriving during the copy called the very same `ship()`
  helper *inside its per-dirty-unit loop*, so every partition key bought a
  full consensus round + apply-confirm (plus a forwarded hop for an
  off-node child). Both paths looked correct and shared the same primitive
  — the batching lived in the caller, and only one caller did it. Made
  vastly worse by a second, independent conservatism: the tail's watermark
  started at 0, so its FIRST pass classified every change record in the log
  as dirty and re-shipped the whole table one key at a time, every merge an
  idempotent no-op. Together: on a 20,000-row split, ~6,000 no-op Raft
  entries per child and ~85% of the build's wall clock spent re-copying
  data it already had — while the children's key counts sat visibly flat.
  **The generalizable rules.** (1) When one loop batches and a sibling loop
  over the same rows doesn't, that asymmetry is the bug — an accumulate-
  and-flush-on-budget shape is usually a few lines and needs no semantic
  argument, because an idempotent, versioned batch doesn't care where the
  chunk boundaries fall. (2) A "safe" zero/empty starting watermark is not
  free when a cheap, *sound* starting value is available from a pass the
  code already makes: this one was recoverable from the same pre-bulk
  read that already computed the version floor, under the identical
  monotonicity argument. Ask what the conservative default actually costs
  on the first large input, not whether it's correct. **The diagnostic
  that made it obvious in minutes**: one consensus entry == one Raft log
  index, so a receiver's own `commit_index` growth divided by rows
  received IS the effective batch size — visible from `/admin/raftkv`
  with no instrumentation, and it turned "the split feels slow" into
  "6,000 entries moved 0 rows." That ratio is now the regression's
  assertion, too: an entry-count budget catches a re-introduced per-row
  ship where a wall-clock assertion would just go flaky.
  (`crates/animusd/src/index_drain.rs::tail_pass`, `split_driver_tick`.)

- **Adding a variant to `animus-dynamo::wire::Operation` breaks `animusd`'s
  exhaustive `match op { .. }` dispatch by construction — that's a downstream
  crate's job to fix, not a sign your pure-crate slice failed (2026-08-19).**
  Building ADR 0051 TTL's `Operation::UpdateTimeToLive`/
  `DescribeTimeToLive` in `animus-dynamo` (a crate deliberately kept
  dependency-free of `animusd`) left `cargo build --workspace --all-targets`
  failing on `crates/animusd/src/dynamo.rs`'s non-exhaustive `match` with a
  clear `E0004` naming exactly the two new variants. This is the expected,
  narrow shape of the split: a pure-crate agent adds the wire vocabulary,
  a separate `animusd`-owning agent wires it up; a compile error whose
  *only* content is "new match arm needed" in a crate you were told not to
  touch is a handoff marker, not a regression to chase — confirm the error
  names only your new variants (nothing pre-existing broke) and report it
  rather than reaching into the other crate. The reusable check: `git status`
  before troubleshooting a cross-crate build break in a parallel-agent tree
  — if the crate the error is in shows as untouched while sibling crates
  show real diffs, the owning agent for that crate simply hasn't landed the
  consuming change yet.
- **`clippy::collapsible_if` on a nested nullable check often wants an
  `if cond && let Some(x) = opt { .. }` let-chain, not a manual flatten
  (2026-08-19).** `describe_time_to_live_response`'s `if enabled { if let
  Some(name) = attr { .. } } }` tripped `collapsible_if` under `-D warnings`;
  clippy's own suggested fix (`if enabled && let Some(name) = &attr { .. }`)
  compiles cleanly on this workspace's toolchain (let-chains are stable
  here) and is both shorter and more direct than restructuring the logic by
  hand — read the lint's `help:` suggestion before reaching for a manual
  rewrite, it's frequently already the answer.
- **A small pure bridge struct (mirroring `StreamDescription`'s existing
  precedent) is the right way to hand a distributed-system layer's real
  state into a pure crate's response encoder, without adding a dependency
  the crate doesn't already have.** `wire::TtlDescription` (ADR 0051) is
  filled in by `animusd`, which holds the replicated catalog's actual TTL
  configuration; `animus-dynamo` never needs `animus_control` types to
  render `DescribeTimeToLive`'s JSON. Before inventing a new response-input
  shape, check whether an existing sibling (`StreamDescription`,
  `index_statuses`'s side-channel) already establishes the pattern — it
  usually does, and matching it keeps the crate's encoders uniform.
  (`crates/animus-dynamo/src/wire.rs`.)
- **A read-without-waking background loop (ADR 0048) has real, pre-existing
  building blocks — verify against the source before assuming a wake is
  unavoidable, don't just gate it and hope.** Building the TTL reaper
  (ADR 0051 §6, `crates/animusd/src/ttl_reaper.rs`) needed a scan that
  never wakes a quiesced `CpGroup`. Rather than trust the ADR's prose,
  reading `animus-cp-data`'s actual source confirmed `local_get_kind`/
  `local_scan_kind`/`pending_changes` are pure `self.storage.{get,scan}`
  calls with **no** path anywhere near `RaftKvNode::wake`/`WakeSignal`/
  `RaftCore` — they never touch the consensus loop at all, so they
  structurally cannot reset a group's idle-activity clock. That made the
  "scan without waking, wake only to act" design directly buildable with
  existing primitives, not a new mechanism. The general rule: before
  reporting a documented contract undeliverable (or silently violating it),
  read the primitive's own implementation — a `local_*`-prefixed accessor
  in this codebase is a strong (but still worth confirming) naming signal
  that it bypasses the network/consensus path entirely.
- **Widening a shared write-path helper's signature (e.g. adding a new
  trailing `bool`/enum discriminator) must be grepped across the *whole*
  crate, not just `tests/*.rs` — an in-crate `#[cfg(test)] mod` at the
  bottom of `lib.rs`/`dynamo.rs`/`index_drain.rs` calls the same private
  function and is invisible to a search scoped to the external test
  tree.** Threading `ChangeRecord::ttl_expired`/`kind_write_item_at_leader`'s
  new `ttl_expired: bool` parameter (ADR 0051 §7) had five real call
  sites, not the four a `crates/animusd/tests/` grep alone would find —
  the fifth pair lived in `lib.rs`'s own `rmw_285_a`/`rmw_285_b`
  in-crate regression module (issue #285, see this crate's own `CLAUDE.md`
  for why those tests can't live in `tests/`). `grep -rn
  "kind_write_item_at_leader(" crates/animusd/src/` (source, not just
  `tests/`) is what actually finds every call site of a `pub(crate)`
  helper this crate's own module-map documents as having in-crate test
  consumers.
- **An introspection surface whose cost scales with the data it describes
  needs an explicit answer to "what happens when something polls this every
  few seconds?" — because a dashboard eventually will** (2026-08-19).
  `/admin/raftkv` computed `key_count`/`byte_size` by materializing every
  hosted tablet's rows per request. Its own doc comment blessed this —
  "this is a debug surface, so the materialize-then-count cost is
  acceptable" — and that was *true when written*, for a human occasionally
  curling a route. It stopped being true when the Console started fetching
  the same route from every node on a 5s auto-refresh, and nothing
  re-examined the original judgement, because the cost lives in one file
  and the polling lives in another. Measured: with a 20,000-row table
  mid-split, polling it every 3s stretched the split's build from 4.5s to
  41.8s (~9x). **The pathology is specifically an observer that perturbs
  what it observes** — the operator watching a slow split was making it
  slower, and every "why is this slow?" measurement taken through that
  surface was measuring partly itself. I hit this as a *debugging* failure
  before finding it as a bug: my own sampler polled the same route, so my
  first published wall-clock numbers for an unrelated fix were ~2x
  inflated and had to be corrected afterward. **Rules.** (1) When you add
  or bless an O(dataset) read on a debug route, write down what polls it;
  if the answer is "a UI, on a timer," it needs a cheap default and an
  opt-in exact path (`?exact=1`), not a comment saying the cost is
  acceptable. (2) When measuring anything, account for your own
  instrument: prefer counters the system already maintains
  (`storage_sstable_block_reads` made this both diagnosable and testable)
  over polling a rich endpoint in a loop, and when you must poll, measure
  the same workload unpolled to size your own footprint. (3) A cheap
  estimate that the *enforcement* path already uses (here `auto_split_loop`'s
  own `approx_key_count`/`approx_bytes`) is often the better default even
  ignoring cost, because it makes the UI agree with the mechanism instead
  of showing a truer number the system never acts on.
  (`crates/animusd/src/lib.rs::CpGroup::raft_view`, `admin.rs::raftkv_view`.)
- **Before claiming a parallelization's speedup, measure what fraction of
  the work you are actually parallelizing — "these two are independent, so
  it's ~2x" is a statement about the code shape, not about the clock**
  (2026-08-19). The split build shipped to its two children serially; they
  are two independent Raft groups at disjoint homes, so making the ships
  concurrent looked like a clean ~2x on the copy, and that is what I told
  the maintainer before measuring. Quiet-parent A/B at 20,000 rows, n=6 per
  side: median 6.05s -> 5.25s, mean 6.13s -> 5.67s, stdev ~0.85s on both —
  **a real direction, but inside one standard deviation, so not a
  demonstrated speedup at all.** The cost model explains it exactly: the
  ships were only ~1.2s of a ~6s build, so halving them buys ~0.6s, and
  what actually dominates is three *full engine scans* of the tablet (the
  version-floor pre-pass, the bulk scan, the final image). **The rules.**
  (1) Estimate the serialized component's share of total time BEFORE
  writing the parallel version; if you cannot, say "unknown" rather than
  quoting a ratio derived from the shape. (2) Pick the benchmark that
  isolates the phase you changed — the repo's existing split bench drives a
  continuous writer, which pins every run to `SPLIT_MAX_TAIL_PASSES` and
  made the change measure as *literally zero* (8.06s vs 8.08s); only a
  quiet-parent run could see it at all. (3) n=1 is not a measurement when
  the run-to-run spread is comparable to the effect: the same binary
  produced 5.0s and 34.6s on consecutive runs until the concurrent-writer
  confound was removed. (4) Ship the honest number, including when it
  undercuts your own earlier estimate — the change here is still worth
  making (it removes a structural serialization and its fault test covers a
  previously untested cancellation path), but selling it as a 2x win would
  have been false.
  (`crates/animusd/src/index_drain.rs::ship_all`.)
- **A crate's own gotcha bullet can itself go stale — verify a "the log
  index is the version" premise against the primary source before trusting
  it enough to build on (2026-08-19).** Tasked with dropping the split
  driver's version-floor pre-pass scan (`index_drain.rs`) in favor of an
  O(1) `group.engine_applied_index()` read, on the stated premise "CP
  writes need no client-assigned version — the Raft log index *is* the
  MVCC version" (verbatim, then-current text of this crate's own
  `CLAUDE.md`). That premise was true once but had been superseded over a
  year earlier: ADR 0018 §2/PR2 (2026-08-11) retired the Raft-index MVCC
  encoding and replaced it with a packed HLC commit timestamp
  (`hlc::pack(ts) = wall_ms << 20 | logical`) — a completely different
  value space from a Raft log index (wall-clock milliseconds vs. an entry
  count), so the proposed substitution was unsound in both directions:
  under real workloads it would under-filter back to the exact unfiltered-
  final-image regression a prior rung of the same ADR had fixed, buying
  nothing for a known cost. (A first draft of this entry also claimed it
  could *over-filter* under `SimEnv`; that was withdrawn on review —
  `animusd` has no `animus-sim` dependency, so this driver never runs
  under a simulated clock. Reviewing your own supporting arguments as
  hard as the conclusion is part of the lesson: the conclusion held, one
  of its three legs did not.) The tell was
  in the *type* the target field held (`ts: HlcTimestamp` on every
  `KvCommand` variant, `KvCommand`'s own doc comment naming `hlc::pack` as
  "the engine's MVCC version at apply") — one grep away from the code the
  premise was supposedly about, and a mismatch the crate's own summary
  bullet had quietly drifted away from. **Rule:** before implementing an
  optimization whose soundness rests on an invariant stated only in a
  summary doc (a `CLAUDE.md` gotcha bullet, a one-line ADR recap), grep the
  actual type/field the invariant is about and read its own doc comment —
  the summary is a pointer, not the source, and it can lag the code by
  exactly as long as nobody happened to need that bullet to be right. This
  generalizes the existing "before implementing a 'close this documented
  gap' task, grep the code" rule (root `CLAUDE.md`) to invariants, not just
  missing-feature claims. Found and corrected in the same change that
  fixed the stale bullet (`crates/animusd/CLAUDE.md`'s "CP writes need no
  client-assigned version" entry) and recorded the rejected optimization
  (ADR 0050's 2026-08-19 "investigated and rejected" amendment) so it
  isn't re-attempted on the same false premise.
- **A cross-task veto fed by a periodic sweep needs a freshness contract
  expressed in the same value space the consumer already gates on — a
  wall-clock stamp compared against an activity marker is not equivalent, even
  when the arithmetic looks right.** `RaftCore`'s quiesce veto (ADR 0048 fork
  D) was a bare `AtomicBool` refreshed by `animusd`'s 200ms
  `change_consumer_loop`; a write landing between one sweep's observation and
  the next left a stale `false` in place, and `RaftCore` had no way to know it,
  so an idle-looking group could quiesce while still owing stream work. Two
  traps sat in the obvious fixes. First, the natural stamp — record the sweep's
  `Nanos`, require it `>= last_activity` — is **unsound**, because
  `last_activity` is bumped at *propose* time while the sweep observes *applied
  engine content*: a sweep racing that gap passes the check while describing
  pre-write state. The sound version indexes the observation in the checker's
  own coordinate space (`engine_applied_index()` compared against
  `commit_index`). Second, a valid lower bound requires reading that index
  **before** the scan it bounds; reading it after is symmetrically unsound, as
  a write committing in between would be absent from the scan yet counted as
  observed. General rule: when bridging an async-observed fact into a sync
  invariant check, version the observation in the checker's coordinates, and
  read the version before the observation. (#302,
  `crates/animus-control/src/raft.rs`, `crates/animus-cp-data/src/lib.rs`,
  `crates/animusd/src/index_drain.rs`, 2026-08-20.)
- **"Never observed" and "observed but stale" are different states, and
  collapsing them by defaulting a new freshness field to `0` silently regresses
  every category the observer structurally never visits.** Adding the quiesce
  freshness gate uniformly with a `0` default would have permanently blocked
  quiescence for `Building` split children and hidden GSI-table tablets —
  categories `change_consumer_loop` already, deliberately, never sweeps — thus
  destroying ADR 0048's whole wakeup-reduction win, in a way the invariant test
  ("a group with a non-empty change log must never quiesce") could never catch,
  because it only asserts the *safety* direction. A `u64::MAX` sentinel ("no
  constraint") preserves prior behavior for the unvisited. Before tightening
  any invariant fed by a periodic sweeper, enumerate which categories that
  sweeper skips and argue each one's safety explicitly; a stricter rule applied
  to a component that never receives the signal is a liveness regression, not a
  safety win. (#302, `crates/animus-control/src/raft.rs`, 2026-08-20.)
- **A margin that a design silently depends on must be enforced in code, or it
  is just the original bug one layer down.** The quiesce veto's safety in
  production rested on an unstated 25x ratio between `--quiesce-after` (5s
  default) and the hard-coded 200ms sweep interval; nothing enforced it, and
  the test that exposed #302 used a 1.5x ratio. The fix pairs the correctness
  change with an enforced floor (`animusd::MIN_QUIESCE_AFTER`, validated on the
  CLI flag and `debug_assert`ed at node start), which also turns the test's
  tight knob into a genuine regression guard rather than a restatement of the
  bug's own "usually fine" margin. When a mechanism's safety argument contains
  the words "much larger than", make the comparison executable. (#302,
  `crates/animusd/src/{lib,main}.rs`, 2026-08-20.)
- **Adding a required (no-default) field to a struct with dozens of literal
  construction sites: let the compiler enumerate them, don't grep-and-hope
  (2026-08-19, ADR 0052's `RoleAddrs::console` port).** `RoleAddrs` (the
  per-node listener-address struct, ADR 0047's `intra` port set the
  no-`#[serde(default)]` precedent this field followed) has ~60 literal
  `RoleAddrs { .. }` construction sites across `animusd`'s `src/` and
  `tests/` — a grep for `RoleAddrs {` finds most of them, but a grep for the
  *stride arithmetic* (`6 * i`, `free_addrs(n * 6)`, hardcoded offsets like a
  hand-computed `addrs[18]` for node index 3) is exactly the kind of
  multi-shape, easy-to-undercount search the root `CLAUDE.md`'s "grep every
  gating match site" lesson already warns about — and this field additionally
  needed the *stride itself* to change (6 → 7), not just one new field
  line, so a per-site fix also had to renumber every sibling offset in the
  same literal. The reliable sequencing: add the field to the struct
  definition **first** (with no default), then run `cargo build -p animusd
  --all-targets` and fix every `error[E0063]: missing field` site the
  compiler actually reports — repeating until clean. This is exhaustive by
  construction (a missed site is a compile error, not a silent gap) where a
  grep pass can only ever be "probably complete." A generic per-site fixup
  script (regex over the fixed six-field block shape, deriving the seventh
  field's expression from the sixth's) handled ~30 of the ~32 remaining test
  files in one pass; the two genuine outliers — a hand-computed hardcoded
  multi-node offset block, and the struct's own `generate`/`generate_split`
  functions building the stride formula directly — still needed a human
  read, which the compiler-driven approach surfaced as compile errors as
  reliably as everything else, rather than as something a grep could have
  silently missed entirely.
- **Removing a stride field is the mirror of adding one (above) and needs the
  identical compiler-driven discipline — but a regex-based mass fixup script
  for the ~50 call sites carries a distinct, silent-corruption risk the
  compiler does NOT catch for free: a field-name-keyed regex
  (`^(admin|intra|console):`) matches the first colon of an unrelated
  `admin::SomeType { .. }` module path exactly as readily as a
  `RoleAddrs { admin: p(3), .. }` struct-literal field** (2026-08-22, ADR
  0053's CQL-port removal, `RoleAddrs::cql` deleted, the 7→6-port stride).
  The script's per-line regex couldn't distinguish "a field named `admin`
  inside a `RoleAddrs` literal" from "the start of the path `admin::CpTxnView`
  used as an ordinary expression" — both are `admin:` followed by more text
  at the start of a line — so it rewrote several `console::TableSummary { .. }`/
  `admin::CpRaftView { .. }`/`dynamo::txn_resolve_awaited(..)` call sites into
  `console: :TableSummary { .. }` (space-then-colon), a token sequence that
  parses as "the struct field `console`, with its value starting with an
  unexpected bare `:`". This *did* immediately fail `cargo build`, with a
  parse error at the corrupted line — so the mistake was never silent in the
  sense of shipping unnoticed — but the error site is generic enough
  (`expected one of \`!\`, \`.\`, \`::\`, ...\`, found \`:\``) that it does not
  self-explain "your last mass-edit script mangled unrelated `Type::path`
  expressions"; diagnosing it means recognizing the shape, not just reading
  the compiler's own words. **General rule: a scripted mass edit keyed on
  `NAME:` (a struct-field shape) must exclude or specifically detect
  `NAME::` (a path shape) — grep the touched files for `\bNAME: :` (or any
  of your field names) as a mechanical post-check before trusting a clean
  build, and always run the full build (not just the files you *meant* to
  touch) immediately after any regex-driven multi-file edit, since the
  script's blast radius is whatever text matches its pattern, not whatever
  you intended it to match.**
- **Adding a field beats adding a variant when a codebase gates on variants
  (2026-08-23, ADR 0055).** Threading "is this read allowed to be stale"
  through `ClientRequest` could have been three new variants
  (`GetStale`/`ScanStale`/`KindScanStale`) or one `#[serde(default)] stale:
  bool` on the three existing ones. The field won for a reason specific to
  this repo: a new `ClientRequest` variant must be classified in ADR 0047's
  exhaustive `surface_of` table and checked against every gating match site
  (`is_relayable_command`, `cp_serve_forwarded`, admin filters) — and the
  standing lesson about those is that a *missed* allowlist is a bimodal
  per-process flake the compiler can't catch. A new **field**, by contrast,
  makes `error[E0063]: missing field` enumerate every construction site for
  you, and a `#[serde(default)]` keeps old peers decoding. General rule: when
  the alternative is "the compiler finds every site" vs. "I grep for every
  gate," take the compiler — and note that this is the same instinct behind
  the `RoleAddrs` port-addition advice above, applied to an enum instead of a
  struct.
- **A comment added "next to" a value inside a raw string becomes part of the
  value (2026-08-23).** While adding `"ConsistentRead":true,` to JSON request
  bodies held in `r#"…"#` literals, one site got an explanatory `// ADR 0055 …`
  appended on the same line — inside the raw string, so the request body
  shipped a `//` comment as JSON and the edge answered `400` on every page.
  It failed deterministically, which is the good case; the trap is that the
  edit *looks* right in a diff, because a trailing `//` comment is exactly
  what you would write one line earlier or later in real code. Rule: when
  annotating a change inside a string literal, the comment goes **outside the
  literal** (above the `format!`/`let`, or at the call site) — and grep the
  touched files for a comment marker inside quotes (`'"[^"]*//'`) as a
  mechanical post-check, the same way the `NAME:`-vs-`NAME::` entry below
  recommends for path-shaped mangling.

- **A "hide this table from clients" requirement and a "reuse the existing
  per-table TTL reaper" requirement can be mutually exclusive under a
  hidden-table naming convention that predates the second requirement
  (2026-08-24, ADR 0018's `ClientRequestToken` amendment).** The obvious
  design for an internal, client-invisible table was to reuse the `$`-
  separated hidden-table convention a materialized GSI/LSI already uses
  (`animus_dynamo::index::index_table_name`) — it looked like exactly the
  "invisible internal table" primitive needed. It structurally cannot work
  for a table that also needs the ADR 0051 TTL reaper: `Metadata::apply`'s
  `CreateTableSchema` arm rejects any `$`-containing name outright (so a
  `$`-named table never gets a `Metadata.schemas` entry at all), and the
  reaper's own per-tick sweep requires **both** a `table_ttl` entry **and**
  a `table_schema` entry before it will scan a table — so a `$`-named table
  could never be TTL-enabled even if the first guard were relaxed. The fix
  was not to weaken either guard (the `$` guard is exactly what keeps a
  hidden index table's identity collision-free) but to pick an ordinary,
  schema-registered table name instead, and grow the *visibility* story
  (`is_internal_table_name`, checked at every client-facing entry point)
  as its own, separate mechanism. **General form**: before reaching for an
  existing "hide this from normal traffic" convention to solve a *new*
  hiding requirement, check what else that convention's own definition
  structurally excludes the hidden thing from — a convention built to
  answer one question (avoid a name collision) can silently foreclose an
  unrelated later question (participate in a background sweep) that never
  came up when it was designed, and the foreclosure is in the *existing*
  guards, not a new one you'd think to look for.

- **A regex mass-edit over Rust must be brace-balanced, not
  next-delimiter-based (2026-08-23).** Adding `stale: false,` to every
  `ClientRequest::Get { … }` literal across the test tree with
  `re.sub(r"ClientRequest::Get \{\n(?:.*\n)*?( *)\},", …)` silently
  corrupted two distinct shapes: a struct literal that ends in `};` (a `let`
  binding) let the match run past it into an *unrelated* later literal —
  `SplitTablet` acquired a `stale` field it has no business having — and a
  literal whose last field had no trailing comma produced
  `table: "kv".to_string()\n    stale: false,`. Both were caught only by the
  build. The reliable shape is to find the opening `{` and walk forward
  counting braces to its real partner, then insert relative to *that*; and to
  check the preceding token for a comma before appending a field. This is the
  same family as the 2026-08-22 `NAME:` vs `NAME::` entry below/above — a
  scripted edit's blast radius is whatever its pattern matches, so prefer
  patterns that can't run past the construct they name.

- **A hand-rolled recursive-descent parser must not give a separator token
  (`,`) lookahead-based "maybe this ends the list" behavior — that
  reintroduces exactly the spelling-based ambiguity a real tokenizer was
  built to remove (2026-08-24, issue #372).** Replacing
  `animus-dynamo::wire::decode_update_expression`'s substring keyword scan
  with a real clause tokenizer required, per the DynamoDB grammar, that
  `SET`/`REMOVE`/`ADD`/`DELETE` only start a new clause at a genuine
  clause-start *position* (expression start, or right after a completed
  action with no separator) — never merely because a token is spelled that
  way, so `SET set = :v` (an unaliased attribute literally named `set`)
  parses correctly. The first implementation still broke on `SET add = :v,
  remove = :w`: after finishing the `add` action it saw the following `,`
  and *peeked past it* — if the next token happened to spell a clause
  keyword, it guessed "trailing comma before a new clause" and stopped the
  `SET` clause early, misreading `remove` as a `REMOVE` clause instead of the
  second `SET` action's attribute name it actually was. The grammar itself
  already disambiguates this without any lookahead: a `,` immediately after
  a completed action is *always* an action separator within the current
  clause (the grammar has no other legal use for it there), full stop — only
  the unconditional absence of a following token (a bare trailing comma at
  the very end of the expression) is safe to special-case, because there is
  nothing left to misinterpret. **General rule: when a parser's separator
  token has to decide "continue this production or end it," that decision
  must come from the separator's own unambiguous grammar role, never from
  inspecting or guessing at what a subsequent token might mean** — peeking
  past a separator to pattern-match on the next token's spelling is the same
  category of bug as the substring scan the rewrite was fixing in the first
  place, just moved one token later. Caught by a test the rewrite's own task
  required (`reserved_words_as_attribute_names_in_a_set_action_list`), not
  by any pre-existing test — a reminder that a new parser needs the
  ambiguous-looking cases in its own test corpus, not just the cases the old
  implementation happened to get right.

- **A convergence veto that guards a correctness property must be
  accelerated, never bounded or bypassed — the bound belongs on the *load*
  that's slow to drain, not on the gate itself (issue #288).** The
  split-cutover GSI-drain veto (`index_drain.rs::split_driver_tick`
  stage 3a) blocks `CutoverSplit` until the parent's `"gsi"` cursor reaches
  the highest pending change record — because cutover retires the parent and
  the reconciler reclaims its engine outright (no drain-before-halt exists
  post-ADR-0044, see `animus-cp-data/CLAUDE.md`'s "Superseded by ADR 0044"
  entry), so firing
  cutover past an un-drained cursor would silently lose GSI updates forever
  (children are born with empty change logs by design). An unthrottled write
  flood racing the split made this veto converge too slowly (several
  10s-of-seconds retries under load, see the "unthrottled continuous write
  flood" entry above) — but the correct fix was never to loosen the veto
  (e.g. force cutover after N stalled ticks, mirroring `SPLIT_MAX_TAIL_
  PASSES`'s bounded chase for the *build* phase). `SPLIT_MAX_TAIL_PASSES`
  is safe to bound because its own correctness never depended on the lag
  being zero (the post-freeze final drain + final image still transfer
  everything regardless of the bound); the GSI-drain veto's correctness
  *is* exactly "the lag is zero" — there is no compensating post-cutover
  mechanism, so a bound here would be a straightforward data-loss bug, not
  a liveness relaxation. The sound fix exploits a fact the *build* phase
  doesn't have: once the parent is frozen (Freeze rejects every later user
  write), the backlog this veto watches is fixed, not growing — so driving
  the drain to exhaustion in a tight loop, right there in the frozen
  endgame, has zero fairness cost and only removes the artificial one-tick
  (`INDEX_DRAIN_INTERVAL`, 200ms) lag between "a drain pass makes progress"
  and "the veto notices," including surviving a transient propose failure
  under load without waiting a full extra tick to retry it. **General rule**:
  before touching a gate that's "too slow to satisfy," classify it — is the
  gate a correctness invariant (something bad happens if you proceed before
  it holds) or a liveness heuristic (nothing unsafe happens, it's just an
  imperfect proxy for "caught up")? Only the second kind may ever grow a
  bounded-chase escape hatch; the first kind's only legal fix is making the
  thing it's waiting on happen faster, exploiting whatever makes the wait
  bounded now (here: the parent going static at freeze) rather than relaxing
  what "caught up" means.
  (`crates/animusd/src/index_drain.rs::split_driver_tick`,
  `FROZEN_ENDGAME_GSI_DRAIN_MAX_PASSES`.)
- **A field added to a shared, generic type (`RaftCore<C, S>`'s `LogEntry`/
  `RaftMsg`) needs a matching update at every *hand-rolled* encoder for it,
  not just wherever `#[derive(Serialize, Deserialize)]` already covers it —
  the "grep every gating match site" lesson (root `CLAUDE.md`) applies to a
  codec's field list exactly as much as to a command enum's match arms
  (2026-08-24, ADR 0058 Train 1's `learners` field).** `LogEntry`/
  `RaftMsg::InstallSnapshot` pick up `#[serde(default)]` handling for free
  from `animus-control`'s own `serde_json`-based WAL/wire path, but
  `animus-cp-data::codec.rs` is a **second, independent, hand-rolled binary
  encoder** for the identical types (ADR 0017 A.2, built to avoid
  `serde_json`'s ~3-4x `Vec<u8>` blowup) — its `put_entry`/`read_entry` and
  `InstallSnapshot` arms enumerate every field explicitly, by hand, with no
  compiler-enforced exhaustiveness the way a `match` on an enum has. A new
  struct field added to `LogEntry`/`RaftMsg` compiles cleanly against this
  codec with the field simply *never encoded* — no error, no warning, just a
  silent drop the moment any message carrying it crosses the wire this codec
  serves (which is every data-plane Raft message; `animus-control`'s own
  `serde_json` WAL path is unaffected, since it never goes through this
  codec at all). **General rule**: when a type has more than one encoder
  (a derive-based one and a hand-rolled "compact wire format" one, or any
  other duplicate-by-design serialization), a new field's checklist must
  name *every* encoder explicitly — a derive macro's own exhaustiveness
  cannot protect a sibling encoder it doesn't know exists. Caught here only
  because the codec's own round-trip test (`every_wire_variant_round_trips`)
  was updated deliberately as part of the same change, not because anything
  would have failed on its own.

- **A membership-change primitive (`add_learner`/`change_membership`) only
  ever reconfigures a group's own agreed-upon peer SET — it does nothing to
  ensure the target actually has a live protocol instance running to
  receive the traffic that config change generates (2026-08-25, ADR 0058
  Train 2 rung 3).** Building the in-place split's Stage 1 (add the union
  of both children's homes as learners to the parent), it was tempting to
  assume "the reconciler adds a learner, Raft's own `AppendEntries` flow
  handles the rest" — but a node named only in a CHILD's own `replicas`
  (never the parent's) has, before this design, no reason to ever host the
  PARENT tablet at all: the ordinary `plan_join_host`/`Host` candidate test
  is "am I in `Tablet::replicas`," which such a node structurally fails.
  Calling `add_learner` on it anyway appends a config entry the LEADER
  replicates outbound — into a network inbox nothing on the target node is
  consuming, since no `RaftKvNode` for that tablet exists there yet. The
  fix widened the host-candidate test itself (a node recruited via EITHER
  child's `replicas` of an in-place split intent also hosts the parent, as
  a quiet non-voter — the identical shape `plan_join_host`'s own "joining
  an already-led group" branch already uses, just reached by a different
  membership test), and a corresponding second fix was needed on the
  RELEASE side: the same recruited node is never in the parent's own
  `replicas` either, so the ordinary release check would fire on it
  immediately, and — a learner is never in `RaftCore::config()` by
  construction — the release path's own "config actually excludes me"
  safety anchor reads trivially true for a still-a-learner recruit too,
  offering no protection. **General rule**: before wiring any new
  membership-change call site (a replica move, a split, a rebalance) onto
  an existing Raft membership primitive, ask "does the target already have
  a running protocol instance for this group, and by what path did it come
  to have one" — a primitive that changes *agreed state* is not the same
  thing as a primitive that ensures a *listener* exists to act on it, and
  the existing candidate/release tests for "should this node host this
  tablet at all" were both written before any workflow needed a node to
  host a tablet for a reason OTHER than being in its own `replicas`.
  (`crates/animus-cp-data/src/host.rs`'s `plan` phase 1's second
  host-candidate branch and phase 3's `recruited_for_split` exclusion.)
- **A "clone this engine" primitive must take the caller's own already-open
  source handle, never a bare identity it re-opens itself — re-opening the
  same on-disk state from two independent in-process instances is a
  correctness hazard, not just wasted I/O (2026-08-25, ADR 0058 Train 2
  rung 3).** `EngineFactory::clone_engine`'s first draft took `source:
  TabletId` and called `self.open(source)` internally, mirroring every
  other `EngineFactory` method's shape (`open`/`probe`/`destroy` all
  address a tablet by id, not a handle). This compiles and even passes the
  `MemoryEngine` test double cleanly (its `open` is a cheap shared-`Arc`
  registry lookup, so a second "open" of an already-open tablet is
  harmless) — but the REAL production engine this trait also has to serve,
  `LsmEngine`, does genuine on-disk WAL/manifest/compaction coordination
  assuming exactly one process-local writer per prefix; a second `open()`
  of the same prefix constructs a second, completely uncoordinated
  in-process instance contending over the same files with no shared lock —
  silent corruption under real concurrent use, invisible in the test
  double that happened to make the unsafe shape look fine. The fix changed
  the signature to take the source's own already-open handle
  (`source: &S`) — which the caller (the host reconciler) already holds in
  its own per-tablet engine cache for any currently-hosted tablet (the
  clone's source, here, is always a currently-hosted parent) — so no
  second open ever happens. **General rule**: a trait method that clones,
  snapshots, or otherwise reads a live engine's current state should take
  the engine handle the caller already has, not an identity it re-derives
  a handle from internally — and a fast/shared-state test double (a
  `MemoryEngine`-backed factory) can make exactly this class of bug
  invisible, so the question "would this be safe against a REAL, exclusive-
  writer-per-instance backend, not just the sim double" is worth asking
  explicitly whenever a new `EngineFactory`-shaped seam gains a method.
  (`crates/animus-cp-data/src/host.rs::EngineFactory::clone_engine`'s own
  doc comment states the final contract and the hazard by name.)
- **Reusing a "one instance per node" primitive for a second, independent
  instance needs the primitive to stop hardcoding its own singleton
  identity first — the same "exclusive resource per instance" class of bug
  as the `clone_engine` entry above, but at the network layer instead of
  disk (2026-08-26, ADR 0059 Train 1 PR②, `animus-cp-data::
  cluster_segment_store`).** `ClusterSegmentStore`'s own doc was explicit
  and correct: "`(node, stream)` is single-consumer (ADR 0026), and this is
  THE ONE task that consumes this node's `SEGMENT_STREAM` inbox" — but
  `SEGMENT_STREAM` was a hardcoded `pub const`, not a constructor
  parameter, because until this PR only one subsystem (DynamoDB Streams)
  ever built one. Wiring `animusd::build_backup_store` — a second,
  independent `ClusterSegmentStore` instance for on-demand backups,
  constructed on every combined/data-only node by
  `BackupStoreConfig::default() == Cluster` — reused the type without
  reading that invariant as a constraint on the *type*, not just on "don't
  call `start` twice from the same call site": both instances' serving
  tasks called `env.recv_stream(SEGMENT_STREAM)` on the same node,
  racing for one single-consumer inbox and silently stealing each other's
  requests/replies. **This was NOT caught by `cargo build`, `cargo clippy`,
  or a single test binary run in isolation** — every test still passed
  individually, because a lone `ClusterSegmentStore` on its own inbox has
  no contender. It surfaced only as intermittent, seed-independent
  failures across *unrelated* `animusd` integration tests
  (`dynamo_streams.rs`, `streams_e2e.rs` — 1 to 4 tests failing per run,
  different tests and different symptoms — a timeout once, a spurious
  "shard has been trimmed" 400 once) once every test node's bring-up
  started constructing the second store by default, reproducing even with
  `--test-threads=1` on a single binary in isolation (ruling out cross-test
  contention as the cause — the two racing consumers live *inside* one
  node's own bring-up). **The generalizable rule**: before reusing any
  "exactly one of these per node/process" primitive for a second logical
  consumer, grep its own doc for the word "exactly" or "the one" and verify
  the singleton identity it's protecting (a stream id, a file prefix, a
  port, a lock name) becomes a real per-instance parameter, not an
  implicit constant — the type system cannot catch a second value of a
  type whose identity is baked into a `pub const` rather than a field.
  Fixed by threading a `stream: u64` field through
  `ClusterSegmentStore::{new,with_k,start,start_with_k}` and `serve_loop`,
  with the pre-existing `SEGMENT_STREAM` staying the streams call site's
  explicit argument and a new, equally explicit
  `animus_cp_data::backup::BACKUP_SEGMENT_STREAM` for the backup call
  site. Regression:
  `animus-cp-data/tests/cluster_segment_store.rs::
  two_cluster_segment_stores_on_the_same_node_stay_isolated_by_stream`
  (two instances, two streams, a same-object-id racing `put` from the same
  node, asserting each store's own local copy holds its own payload).
- **A full design-token rewrite ("keep the names, change the values") is only
  safe once you've counted every consumer, and a mockup's literal CSS can
  silently redefine what an existing token means (2026-08-25, ADR 0056,
  the "Ledger" visual system).** Rewriting `tokens.css` in place (new
  palette, light-default instead of dark-default, glow-means-live replaced
  by keyline-means-live) while keeping every token *name* stable — so the
  two consumer stylesheets (`dashboard.css`, `console.css`, ~600 lines
  combined) and every `dashboard_*.js`/`console.js` render function needed
  zero call-site edits — only worked because a `grep -c "var(--$t)"` sweep
  across every consumer was run *before* writing the new file, for every
  single token name. That surfaced two things a values-only read of the old
  file would have missed: (1) several tokens (`--glow-*`, `--live-underline`,
  `--accent-hi`, `--shadow-recessed`, motion timing) had **zero** consumers
  outside `tokens.css` itself, so they were safe to gut/repoint freely
  without a wider search; (2) `button.primary`/`.btn-new`/`.btn-save`/
  `.seg-group button.active`/`.seg-opt.selected` all filled with
  `var(--accent)` + `var(--accent-ink)` under the old system, but the new
  mockups' own literal CSS filled the equivalent controls with the *ink*
  color (`background:#211f1a;color:#fafaf9` in the light mockup) — i.e. the
  new system's "ONE accent role" rule (accent for links/underlines/hatch
  only, never a solid fill) is a *component* decision the token file alone
  can't express; it required rewriting those five call sites' `background`/
  `color` properties, not just relying on `--accent`'s new value. Trusting
  the token rename to carry that change silently would have shipped
  accent-filled primary buttons that merely looked different (a legal but
  spec-violating shade) instead of catching that the button recipe itself
  had changed. **General rule**: before a "same names, new values" token
  rewrite, grep every consumer for every token name being touched — a
  zero-hit token is safe to redefine freely (or even fold into another
  token), and a token whose *default recipe* the new mockups visibly
  contradict (a filled control's authoritative markup uses a different
  color than the token that used to supply it) needs its call sites edited
  by hand, because the rename alone will compile clean and still be wrong.
  (`crates/animusd/src/tokens.css`, `dashboard.css`, `console.css`.)
- **Renaming a branded UI surface needs its Rust test *assertions* fixed to
  stay green, but its doc-comment prose is a separate, lower-priority sweep
  — don't conflate the two passes (2026-08-25, ADR 0056, admin/console
  rename).** Renaming the operator dashboard's brand text ("AnimusDB
  Console" → "animusd admin") and the data app's ("AnimusDB Data Console" →
  "animusd console") broke exactly two integration-test files
  (`dashboard_endpoint.rs`, `console_endpoint.rs`) whose `body.contains(...)`
  assertions checked the old literal strings — found by grepping `tests/`
  for the old names *before* editing the HTML, per this repo's standing
  rule, then fixed alongside the HTML in the same change so the gate never
  went red. Dozens of *other* hits for the same old strings remain, on
  purpose: module-doc `//!` comments and inline comments across
  `dashboard.rs`, `dashboard_core.js`, `console.js`, and every
  `crates/animusd/tests/console_*.rs` file's own header comment. None of
  those are asserted by any test (confirmed by grep), so they don't fail
  the gate — but they are exactly the "stranded documentation" class this
  log already names (see the `ReplicationMode`-removal entry above): prose
  that now describes a surface by a name the code no longer uses, silently,
  with nothing failing to point at it. Left as a deliberate, separate
  follow-up (out of this change's stated file scope) rather than folded in,
  since a partial prose sweep across a ~2000-line crate guide risks
  introducing exactly the kind of drift it would be fixing. **General
  rule**: when a rename's brief says "update every asserted string," that
  is a narrower, harder requirement than "update every occurrence" — grep
  for assertions specifically (`.contains(`, `assert_eq!` against the
  literal, etc.) to find the must-fix set, and treat every remaining
  prose hit as a tracked, intentional gap rather than either silently
  ignoring it or scope-creeping the change to chase it down.
- **A theme toggle whose default writes an explicit attribute defeats a
  static dark-mode QA render unless the toggle script is neutralized first
  (2026-08-25, website rebuild on the Ledger system).** `site.js` applies
  `stored()` on every load — and once "light" (not "system") is the
  documented default, that call sets `data-theme="light"` on `<html>`
  explicitly rather than leaving the attribute absent. A QA render that
  hand-edits a scratch copy of a page to add `data-theme="dark"` and then
  loads it with the page's own script still attached gets silently
  overwritten back to light before first paint — the screenshot comes out
  byte-identical to the light render (same file size, confirmed before
  looking closer), which reads as "dark mode is broken" when the actual bug
  is the QA method fighting the app's own initialization order. The fix is
  to strip the theme script from the scratch copy for a static-render check
  (the CSS is what's under test, not the runtime toggle), not to debug the
  CSS. **General rule**: before concluding a themed page's dark styling is
  broken from a scripted/automated screenshot, check whether the page's own
  JS re-applies a *default* state on load — a `checked`/`data-*`/class
  toggle with a persisted-with-fallback default will clobber any attribute
  a test harness sets by editing the static HTML, and the fallback is easy
  to miss precisely because it used to be a no-op ("system" removed the
  attribute) before the default changed to an explicit value.
- **A pure predicate trapped behind a stateful handle is extracted by
  widening its parameter list to the primitive facts the caller already
  reads off that handle, not by trying to make the handle itself pure**
  (ADR 0061 rung A6, `animusd`'s `decide` module). `ClientCtx::
  confirm_wait_is_futile`/`frozen_refusal` looked `&CpGroup`-shaped
  (`fn confirm_wait_is_futile(leader: &CpGroup, accepted_index: u64) ->
  bool`), and `CpGroup` wraps a real `RaftKvNode<ProdEnv, _>` -- genuinely
  impossible to construct in a unit test without full cluster bring-up. But
  every line of each function's *body* only ever called two or three cheap,
  already-`pub(crate)` accessors on it (`leader.engine_applied_index()`,
  `leader.is_leader()`, `leader.is_frozen()`). Changing the signature to
  take those return values directly (`fn confirm_wait_is_futile
  (engine_applied_index: u64, is_leader: bool, accepted_index: u64) ->
  bool`) turns an apparently-entangled method into a plain, directly
  unit-testable function with a full truth table in under ten lines -- the
  caller's one-line change is reading the same fields it already read, just
  before the call instead of inside it. The tell that a function is a false
  negative for "needs bring-up" is exactly this: grep its body for what it
  actually touches on `&self`/the handle parameter, not what the
  parameter's *type* looks like capable of.
- **When a pure free function's existing `#[cfg(test)]` module doesn't
  actually test the function it's named after, don't move it just because
  the function moved** (same ADR 0061 rung A6 sweep). `confirm_futility_
  tests` sits right next to `confirm_wait_is_futile` in `lib.rs` and reads,
  from the name, like its unit tests -- but every test in it is a real
  `#[tokio::test(flavor = "multi_thread")]` standing up a whole single-node
  cluster and asserting on wall-clock timing and error-string shape of the
  *wired* fast-fail behavior, never calling the predicate directly. Moving
  it into the new bring-up-free `decide` module would have silently broken
  that module's whole reason for existing (no `&self`/`ProdEnv`/`tokio`,
  plain `#[test]`s only) for a module that only looks related by proximity
  and naming. It stayed in `lib.rs`, documented as proving the wired
  behavior rather than the predicate, and the predicate got its own fresh
  truth-table tests in `decide` instead -- the two are complementary
  coverage, not one relocated. General rule: before moving a test module
  alongside an extracted function, read what it actually calls, not what
  it's named after or sits beside.
- **Splitting a large `impl` block across files by line-range extraction:
  a naive "walk upward from the `fn` signature, stop at the first line
  that isn't a doc comment" heuristic silently orphans multi-line and
  trailing-comment attributes, and the resulting compile error points at
  the wrong function (ADR 0061 rung C5 step 2, `animusd`'s `ClientCtx`
  split into `schema.rs`/`read_path.rs`/`write_path.rs`/
  `txn_coordinator.rs`/`forwarding.rs`).** Two attribute shapes break a
  naive upward walk that only recognizes `///`/`#[...]` (single-line,
  balanced) directly above a signature: a **multi-line** attribute
  (`#[tracing::instrument(\n    name = "...",\n    skip(self),\n)]`,
  rustfmt's own style once an attribute's arguments don't fit one line)
  and a **single-line attribute with a trailing line comment**
  (`#[allow(clippy::too_many_arguments)] // mirrors ClientRequest::...`).
  Neither matches "line starts with `#[` and ends with `]`", so the walk
  stops one line short, extracts everything from the signature down but
  leaves the attribute behind — and because the attribute is syntactically
  valid on its own, it silently reattaches itself (per Rust's "doc
  comments/attributes attach to the next item" rule) to whatever function
  ends up sitting below the extracted one after the move. The resulting
  error is **not** "attribute not found" at the extraction site — it's
  `error: expected item after attributes` if nothing follows, or a
  `too_many_arguments`/wrong-`#[allow]` clippy failure on an unrelated
  function if something does — neither of which points at the actual
  mistake. Fix: track bracket balance (`(`/`)`/`[`/`]`, since `#[attr(args)]`
  mixes both) walking upward from a line that looks like an attribute's
  *tail* (matches `^\)*\]\s*,?\s*(//.*)?$`) until the balance returns to
  zero at a line that starts with `#[` — that's the true start; verify by
  re-grepping the moved chunk and the source file afterward for the
  attribute's own text (`#[allow(clippy::` or `#[tracing::instrument`,
  whatever the file actually uses) rather than trusting the tool's line
  count alone. The corollary compiler-privacy lesson from the same
  rung: this codebase's module tree makes every file a *descendant* of
  `lib.rs` (the crate root), so a formerly-bare `fn` (no `pub`/`pub(crate)`)
  that only needed to be reachable from sibling files staying in `lib.rs`
  needed **no** visibility change to keep compiling once callers moved
  into new child modules — Rust's privacy rule is "visible in the
  defining module and its descendants," and a parent's private items stay
  visible to every descendant regardless of which descendant. The
  widening was only needed in the other direction: a method that itself
  moved into a new child module (`schema.rs` et al.) and is called from a
  *sibling* child module or from code that stays in the parent needs
  `pub(crate)` — 30 of the ~83 relocated methods needed it for exactly
  that reason (`cp_serve_forwarded`, moved last, turned out to be the
  single biggest source of these: it calls into every other cluster by
  name). `cargo build`'s `E0624: method \`x\` is private` errors name the
  exact call site, so the mechanical move-then-build-then-widen loop
  catches every miss — but only if each intermediate build is actually
  run, since a batch of moves without an intervening build can mask one
  widening behind another's cascading errors.
- **`Cargo.lock` is deliberately gitignored (`.gitignore`, present since the
  repo's initial commit, no lockfile ever tracked in history) — a container
  build or any other recipe that assumes a committed lockfile breaks
  (ADR 0060 e2e work).** The Dockerfile's own builder stage says as much
  (`COPY Cargo.toml` only, never `Cargo.lock` — "cargo build mints its own
  lock file from the registry cache mount"), and this is exactly what a
  real build does: `docker build`'s first line is `Locking N packages to
  latest Rust <version>-compatible versions`, freshly resolved every build,
  not reproduced from a checked-in file. The repo carries no separate
  written rationale for the choice beyond that comment; treat it as "this
  workspace resolves dependencies per-checkout, not pinned across commits"
  and design any new build/CI recipe around that — never add a `COPY
  Cargo.lock` step or a lockfile-presence check expecting one to exist, and
  don't be surprised when two builds an hour apart pull a different patch
  version of some transitive dependency.
- **A sandboxed dev/build host can lack `CAP_SYS_RESOURCE` at the
  kernel/hypervisor level, and no per-container `--cap-add` can restore
  it** (ADR 0060 e2e work, ~3 hours root-causing a `kind create cluster`
  failure). Every `kind` node's own Kubernetes control plane (`etcd`/
  `kube-apiserver`/`kube-scheduler`/`kube-controller-manager`, static pods)
  gets a **negative** `oom_score_adj` from kubelet unconditionally — not
  configurable via pod spec or kind config, standard "protect the critical
  pods from the OOM killer" behavior. Applying a negative value needs
  `CAP_SYS_RESOURCE` at container-create time inside `runc`'s own `nsexec`,
  and a capability absent from the outermost privilege domain can never be
  regranted to a nested/privileged container — confirmed here by `docker
  run --cap-add SYS_RESOURCE` being flatly rejected as "not supported by
  your kernel or not available in the current environment," not merely
  denied at use. The symptom at the `kubectl`/kubelet layer gives almost no
  hint of this: containerd launders the real error into the generic `can't
  get final child's PID from pipe: EOF`, which looks exactly like a cgroup-
  driver mismatch, a containerd-version regression, or a seccomp profile
  issue — all three were tried and ruled out (`SystemdCgroup` true/false,
  two node images spanning containerd 1.7 and 2.1, an unconfined seccomp
  profile) before a direct `runc create --debug` reproduction against a
  hand-built OCI bundle isolated the actual line: `nsexec: failed to update
  /proc/self/oom_score_adj: Permission denied`. **General rule**: when a
  nested-container workload fails identically across every runtime-version/
  cgroup-driver/seccomp permutation you can think to vary, stop varying
  *its* configuration and check the *host's own* capability set directly
  (`capsh --print`, or `docker run --cap-add <X> ... true` for the specific
  capability) — a wrapped, generic runtime error can be hiding a single
  missing capability that no amount of downstream reconfiguration can work
  around. See `crates/animus-operator/CLAUDE.md`'s e2e section for the full
  diagnosis and the exact log signature to grep for.

### Parallel-agent orchestration
- **A gate command piped into `tail`/`tee` without `pipefail` reports the
  pipe's *last* command's exit status, so the gate can fail while the
  wrapper exits 0 (2026-08-26, ADR 0059 Train 1 PR② merge).** A
  `cargo check --workspace --all-targets 2>&1 | tail -3` "validation" of a
  merge-conflict resolution exited 0 while `cargo check` itself had failed
  with E0061 — the compile error was real (main had added new
  `run_node_with_streams_quiesce_and_split_mode` call sites in
  `split_build.rs`/`split_lifecycle.rs` while the branch being merged had
  widened that signature; textual auto-merge sees no conflict in files
  only one side changed), and CI caught what the local wrapper claimed to
  have checked. Two rules: (a) any pipeline whose exit status gates a
  push runs under `set -o pipefail` (or checks `PIPESTATUS[0]`); (b) a
  merge of a moved `main` into a branch that changed a function signature
  gets its gate run on the *merged* tree specifically because the
  dangerous call sites are the ones only `main` has — the same
  missed-allowlist class as the "grep every gating match site" rule, at
  merge time.
  QA (2026-08-25, website mobile pass)** — in this harness,
  `chromium --headless=new --window-size=390,H --screenshot=...` lays the
  page out at the default ~800px viewport and then *crops* the PNG to
  390px, which is visually indistinguishable from the page overflowing
  (text "clipped" mid-glyph at the right edge). A mobile layout was
  wrongly diagnosed as broken twice this way while the DOM was fine.
  **Rule:** for any viewport-dependent check, drive the browser with
  Playwright (`/opt/node22/lib/node_modules/playwright`, executablePath
  `/opt/pw-browsers/chromium`) and set `viewport: {width, height}` on the
  page, asserting `document.documentElement.scrollWidth` and
  element `getBoundingClientRect()` from inside the page; keep raw
  `--screenshot` for fixed-width artboards only. Note `scrollWidth`
  alone can also pass while content is cut (an `overflow: hidden`
  ancestor eats the evidence) — pair it with a bounding-rect sweep for
  elements extending past the viewport that are not inside a deliberate
  `overflow-x: auto` container.
- **A subagent that launches its validation gate as a *background* task and
  then ends its turn to "wait for the notification" stalls the whole
  pipeline (2026-08-25, Ledger design delivery)** — background-task
  completion notifications go to the *orchestrating* session, not to a
  subagent that has already stopped; the subagent just sits "finished"
  with an uncommitted tree while the orchestrator sees a completion notice
  whose result is "waiting for tests". Two of three implementation agents
  in one delivery did this independently, each needing a manual resume
  ("your test task already finished — run checks foreground and commit").
  **Rule:** subagent briefs must say *run every check as a synchronous
  foreground command and do not end your turn before the commit exists* —
  and when an agent's completion notice reads like a status update rather
  than a result, resume it immediately with that instruction instead of
  waiting for a notification that will never come.
- **A single long-lived session can exhaust the disk on `target/` alone, with
  no parallel fan-out involved (2026-08-19)** — a solo `cargo build
  --workspace --all-targets` on this repo hit `rustc-LLVM ERROR: IO failure …
  No space left on device` and `ld terminated with signal 7 [Bus error]` with
  `target/debug` alone at 30 GB against a filesystem reporting single-digit
  megabytes free (despite a large nominal size — the real quota is much
  smaller than `df`'s `Size` column implies on this harness). `cargo clean`
  reclaimed the full 30 GB in seconds and the rebuild succeeded; there was no
  need to hunt for a partial/targeted clean. **Rule:** if `cargo
  build`/`test`/`clippy` fails with an I/O or linker error whose message
  mentions space (not a compile error), check `df -h` before debugging the
  "failure" as if it were a code problem, and prefer a full `cargo clean`
  over trying to selectively prune `target/` — the incremental cache is the
  overwhelming majority of the size and buys little across a full rebuild
  anyway. **Refinement (2026-08-24):** when the *linked test binaries*
  dominate rather than the incremental cache — `du -sh target/debug/deps`
  showing tens of GB of ~150-200 MB-each executables, one per `tests/*.rs`
  file, is the tell — deleting only those executables (`find target/debug
  target/debug/deps -maxdepth 1 -type f -executable ! -name '*.so'
  -delete`) frees the same space in seconds while leaving every compiled
  `.rlib`/`.rmeta` dependency artifact intact, so the next `cargo build`/
  `test` only **relinks** the binaries it actually needs instead of
  recompiling ~150 dependency crates from scratch. A full `cargo clean` is
  still the right call when the incremental cache itself is the bulk of the
  size (the original 2026-08-19 case, `target/debug/incremental` at 600
  MB+); check which directory is actually large before choosing between
  the two — they solve the same symptom at very different costs.
  **Refinement (2026-08-24, issue #374 C2): at genuinely 0 bytes free (not
  just "low"), the agent harness's own Bash tool output capture fails with
  ENOSPC too** — `df`, `echo hi`, even a `Write` truncation of an unrelated
  file all failed with the harness's own "temp filesystem is full" or
  `ENOSPC` errors, on the SAME root filesystem the repo and `/tmp` share
  here. Every synchronous (foreground) Bash call was unusable in this state.
  What worked: issuing the cleanup (`rm -rf target`, no other commands
  chained) with `run_in_background: true` — a background command apparently
  needs less headroom to start than a foreground one needs to capture its
  own output, and once it actually deleted enough (a full `rm -rf target/`,
  not a partial executables-only sweep, since the workspace build that
  caused this had already grown the whole tree past what executables-only
  deletion could recover), ordinary foreground commands worked again.
  **General rule**: if disk hits exactly 0 and even trivial read-only
  commands (`echo`, `df`) start failing with a filesystem-full error, stop
  trying to diagnose via Bash — go straight to a `run_in_background` deletion
  of the single largest, safely-regenerable directory (a repo's own
  `target/`), and only resume foreground commands once one such background
  job reports genuine free space back. A `CARGO_PROFILE_DEV_DEBUG=0`
  environment variable on every subsequent `cargo build`/`test`/`clippy`
  invocation (not just the validation-pass advice below, but every single
  command afterward) then keeps the rebuilt tree from reaching the same
  ceiling again — debug info alone was the difference between builds that
  fit in the freed headroom and ones that didn't, in the same session.
- **Parallel agents share one `target/` dir; three concurrent
  `--all-targets` builds exhaust the session disk (2026-08-19).** Fanning three
  implementation agents across disjoint crates avoids *source* conflicts but
  not *build* conflicts: each ran its own `cargo build --workspace
  --all-targets` in the same `target/`, which grew past 22 GB and hit ENOSPC —
  killing builds mid-link (`ld terminated with signal 7`), producing a
  transient compile error in one agent from another's half-written file, and
  eventually filling the harness's own scratch filesystem so that even `df`
  could not run. Deletes still succeed when writes don't, and
  `target/debug/incremental` is the cheapest large thing to drop first.
  **Rules for a parallel fan-out on this repo**: give each agent a
  crate-scoped gate (`cargo clippy -p <crate>`, `cargo test -p <crate>`) and
  keep the one workspace-wide `--all-targets` build for the orchestrator to
  run *serially* at the end; set `CARGO_PROFILE_DEV_DEBUG=0` for validation
  passes (debug info dominates target size and changes nothing the gates
  check); and tell agents to stop and report on ENOSPC rather than polling for
  space, since a blocked agent burns its context waiting on a condition only
  the orchestrator can clear. The orchestrator should also re-run every gate
  itself afterwards — an agent whose build was killed by someone else's disk
  usage will honestly report "inconclusive," and two of the three fixes here
  reached the working tree never having been compiled (one did not: a
  `MutexGuard` held across an `.await` in a new fault-injection test made the
  future non-`Send`, which only the serial re-run caught).
- **A hand-rolled two-PR stack strands its top PR if the base merges first —
  use `gh-stack` (or retarget to `main` before merging), and verify the
  default branch actually received the change (issue #279, 2026-08-19).**
  The #279 fix shipped as PR #304 based on PR #303's branch, stacked by hand
  rather than with the repo's `gh-stack` convention. Both were merged within
  18 seconds: #303's branch → `main` first, then #304 → that same branch. The
  second merge is a no-op as far as `main` is concerned — its commit landed on
  a branch that had already stopped feeding the default branch — so `main` got
  #303's `DiskConfig::set_sync_delay` (groundwork) and *not* the driver fix it
  was groundwork for, while GitHub cheerfully reported both PRs "Merged".
  Every surface lies in this state: both PRs show green and merged, the branch
  still exists carrying the work, and only two things give it away —
  `git merge-base --is-ancestor <fix-sha> origin/main` returns false, and the
  linked issue stays **open** with an empty `closed_by_pull_requests` (a
  `Fixes #N` trailer only fires when the commit reaches the default branch,
  which is the cheapest available "did this actually land" signal).
  **Rules**: build a real stack with `gh-stack` so the tooling retargets the
  child when the parent merges; if stacking by hand, retarget the child onto
  `main` *before* anyone merges the parent, or merge strictly top-down; and
  after any stacked merge, confirm the change is on the default branch rather
  than trusting the PR's merged badge.
- **Do not override git author/committer to a human identity in a web
  session (2026-08-23).** This container's commit-signing hook requires the
  committer to be the pre-configured `Claude <noreply@anthropic.com>`
  identity; a subagent that sets `user.name`/`user.email` (or `git commit
  --author=...`) per-commit to a maintainer's name produces a commit the push
  gate flags **Unverified**, forcing the orchestrator to rewrite it before it
  can land. Let the configured identity stand and keep `git commit -s`
  sign-offs matching it — signing off as someone else is exactly the mismatch
  the hook exists to catch.

- **`gh stack checkout <N>` silently switches the CURRENT worktree's checked-
  out branch** — in a worktree-isolated agent, this is indistinguishable at a
  glance from a scratch/tracking branch staying put, and it can happen mid-air
  underneath a long-running background build or test. Finishing the ADR 0047
  stack, a `gh stack checkout 228` run purely to inspect the stack's shape
  (branch order, which PRs it already contained) reset this worktree's HEAD
  from a local scratch branch to `intra/2-cutover` — while a `cargo test
  --workspace` run was still executing in the background against the old
  (correct) source tree. The build didn't crash immediately (already-linked
  test binaries kept running), but the source tree was no longer the one the
  run was supposed to verify, so its results couldn't be trusted — the safe
  fix was killing the run, switching back, and restarting from scratch,
  costing a full extra `cargo test --workspace` cycle. **General rule**:
  before running any `gh stack`/similar branch-management subcommand (not
  just the obviously-destructive ones), check `git branch --show-current`
  immediately after — never assume a "read-only-sounding" stack-inspection
  command left the working tree's checked-out branch untouched, and never run
  one while a background build/test against the current tree is still in
  flight.
- **A stacked series' final "docs/ADR finalization" PR must treat the stack's
  own shipped PR bodies (`gh pr view`) as the authoritative source for
  divergences from the plan — not the plan doc, and not just the final code
  state.** ADR 0040's 6-PR stack had one implementer agent per PR, each of
  which discovered and documented a real divergence from the delivery plan
  as it built (e.g. PR4's `RegisterNode` CAS keying on `node_addrs` alone,
  not `addrs`+`labels`, and its separate control-role-never-claims-`members`
  fix) — but each agent's own PR body is the *only* place some of these
  divergences are recorded end to end, since the shipped code and crate
  `CLAUDE.md`s describe the *result* without always narrating *why it
  diverged from what was planned*. Finalizing ADR 0040 (this PR) by reading
  only the code + crate guides + the plan doc would have produced a
  plausible-sounding but subtly wrong Decision C (the plan's original
  labels-inclusive CAS design, not the shipped node-addrs-only one) — the
  gap only closes by reading every prior PR's own body (`gh pr view
  <N>`) for its "Deviations from the plan/brief" section before writing the
  ADR's final Decision text. **General rule: when a task hands you a stack
  of already-landed PRs to finalize/document, fetch and read each one's own
  PR body before trusting the plan doc or the current code alone — a PR body
  is where an implementer records the reasoning for a mid-flight design
  change that neither the plan (written before) nor the code (silent on
  *why*) captures.**
- **Partition work by disjoint crate ownership — exactly one owner per shared
  crate/file.** The assembly points (`animusd`, `animus-control`) are
  chokepoints; if several agents must touch `animusd`, split by *file*
  (`dynamo.rs` / `admin.rs` / `lib.rs`) and expect a small `lib.rs` merge.
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
- **A worktree-isolated agent must never hardcode the main checkout's path,
  even for a `Read` — recurred (plan-syskv-ui / ADR 0038 PR6, 2026-08-10),
  with a refinement to "a clear refusal for `Edit`/`Write`" above: that
  refusal is not reliably active from the very first tool call of a
  session.** Mid-session, after an
  infrastructure-watchdog stall and resume, this agent's assigned worktree
  path silently changed out from under it (env block said one path at
  session start; the Bash tool's actual cwd for every call turned out to be
  a *different* worktree entirely, discovered only via `pwd` + `git status`
  after the resume). Several `Read`/`Edit` calls in between had used bare
  `/home/guillaume/Code/animus-db/...` paths (no worktree segment at all) —
  and **succeeded**, silently editing the shared main checkout, including
  two `Edit` calls that added real content (not just a `Read`). Only a
  *later* `Edit` attempting to *revert* that same file was refused with
  "This session is now isolated in \<worktree\>; edit the worktree copy of
  this file instead" — meaning the guard exists and does fire, but not from
  the start of every session or across every call in one, so "Edit/Write
  refuses" is not a safety net an agent can lean on; only session-start
  verification is. **Recovery when this is discovered after the fact**:
  `git diff` the polluted shared-checkout file to confirm every hunk is
  really yours (here, confirmed via `git -C <main> diff -- <file>` showing
  exactly the two intended additions, nothing else); if an *unrelated*
  agent's own uncommitted edits are *also* present in the same shared
  checkout (they were here — a sibling agent's in-flight `heartbeat_loop`
  rework, and an untracked test file), leave those alone entirely and only
  attempt to revert your own file. If the harness then refuses even a
  read-only `git show`/`git diff`/`git checkout --` against that path (it
  did, for every git subcommand, not just mutating ones, once the guard was
  active), you cannot self-heal the pollution via git or `Edit`/`Write`
  either — re-apply your intended change fresh in the correct worktree
  (confirmed via `pwd` immediately before this recovery, not trusted from
  the session's opening env block) and say so plainly in the final report;
  do not claim the shared checkout was cleaned up when the tooling itself
  prevented it.
- **A worktree-isolated subagent must push its branch before reporting the
  work done or mergeable.** An orchestrator (or the user) cannot verify,
  review, or recover work that exists only in the agent's local worktree, and
  agent worktrees are churned/reclaimed (see the dead-agent entry above) far
  more casually than a pushed branch ever is. "Mergeable" is a claim about
  the *remote*: until `git push -u origin <branch>` has succeeded, report the
  work as in-progress, never done. (The other half of subagent git hygiene —
  never `cd` into the main checkout — is the entries above.)
- **The shell's cwd can silently drift across worktrees mid-session** (the
  watchdog-stall/resume entry above is one confirmed mechanism), so any
  command whose *meaning* depends on which tree it runs in — `git status`/
  `diff`/`add`/`commit`, `cargo build`/`test`, anything resolving relative
  paths — should be fused as `cd <worktree-abs-path> && pwd && <command>` in
  a single invocation: the explicit `cd` re-anchors the command no matter
  what the ambient cwd has drifted to, and the `pwd` echo leaves proof in
  the transcript of where it actually ran. A bare `<command>` that trusts
  the ambient cwd is the thing that silently runs against the wrong tree.
- **The multi-agent worktree-churn survival kit.** When several
  worktree-isolated agents run in parallel, the individual failure modes
  above compound, and the defenses are cheap enough to be standing practice:
  (1) give every agent explicit absolute paths (its worktree root, the files
  it owns) in its prompt rather than letting it derive them; (2) have agents
  push checkpoints — push after each meaningful commit, not only at the end
  — so a dead agent's work survives its worktree; (3) verify any PR an agent
  reports creating actually exists (`gh pr view <N>`) — a report of "opened
  PR #N" can outlive a failed creation; (4) poll on silence — an agent gone
  quiet may be stalled or dead, so inspect its worktree and pushed-branch
  state instead of waiting indefinitely.
- **Stacked-PR series: retarget before merging, and never delete a branch
  that is still some open PR's base.** Two gotchas that recur when landing a
  `gh-stack` series bottom-up: (1) before merging PR N+1, confirm its base
  has been retargeted onto the branch its diff should be measured against
  (once PR N merges, N+1's base must move to N's own base) — merging against
  a stale base lands the remaining stack's commits in one PR, or shows a
  reviewer a diff full of already-landed work; (2) deleting a merged PR's
  branch while a later PR in the stack still names it as base can close that
  open PR or corrupt its diff — delete stack branches only after the entire
  series has landed.
- **Closed (ADR 0037 hardening trio's PR3): the ADR 0036 allocator is now
  wired into `control-add`**, closing the last of ADR 0037's three deferrals
  (heartbeat-liveness/hardening PR1, quorum-guard/hardening PR2, this one).
  Two lessons worth keeping from wiring it:
  - **A function whose only prior nonce source was a documented, narrowly-
    scoped `Env`-seam exception is not license to reuse that exception
    elsewhere.** ADR 0036's `generate_join_nonce` deliberately draws real
    (non-`Env`) randomness, but its own doc scopes that exception tightly to
    one real-process, pre-bind CLI boundary no `SimEnv` test ever drives.
    `admin_add_control_member`'s new minted-id branch runs in-process on a
    live control leader instead — exactly the kind of place a `SimEnv` test
    *does* drive (and this PR's own tests do) — so its nonce comes from
    `leader.env().next_u64()` instead, keeping the `Env`-seam rule (ADR 0003)
    intact with no exception invoked. When extending a helper that has a
    documented seam exception, re-read the *scope* of that exception before
    assuming a new call site inherits it — most don't.
  - **"Wire an allocator into an existing add-member action" is not
    automatically "make the action mint-then-bind-ready for a real new
    process."** The obvious design — mint an id, then have the operator's
    already-running new node discovered via the same `GET /admin/config`
    liveness check the operator-supplied form uses — doesn't compose: a
    physical `RaftCore`'s own self-id is fixed at bind time, so a server-
    minted id (unknown until the admin call returns) can only ever match a
    process that binds *after* learning it, not one already running. The
    shipped design accepts this: the omitted-node form takes the raw
    control-Raft address directly (no `/admin/config` resolution, no
    post-call convergence poll), mints, registers, and adds the voter in one
    call, and tells the operator to bring the physical process up
    *afterward* with `--node <minted-id>` — deliberately not solving "start
    a not-yet-`--node`-known process and have it discover its own minted id"
    (that would need its own join-allocated-style bootstrap entry point,
    scoped out as future work, same as ADR 0037's own coordination note
    anticipated). Test coverage follows the same shape as the existing
    concurrent-add regression (`concurrent_control_add_surfaces_in_flight_
    as_a_clean_retryable_error`): a fake, never-connected addr is enough to
    prove the admin-plane mint + register + `change_membership` mechanics
    and the `GET /admin/control/members` convergence signal, without needing
    a real second process — real Raft catch-up for a runtime-added voter is
    already covered elsewhere (`grow_control_group_converges_everywhere`).
    When a design reverses an existing action's "resolve-then-add" order
    into "add-then-instructs-resolve," check every step that assumed the
    old order (liveness pre-checks, convergence polls keyed off information
    that no longer exists yet) rather than threading the new `Option` through
    unchanged.
- **ADR 0040 PR6 (orphan-member auto-reclaim sweep). Two generalizable
  lessons from adding a "was this member ever real" sticky flag next to an
  existing status field:**
  - **A flag meant to distinguish "never happened" from "happened, then
    reverted" must be set wherever the state can be *directly* reached, not
    just at the one call site you're thinking about when you add it.** The
    plan's own framing ("set `has_activated` true the first time the
    detector promotes Down→Active") was almost a structural hole: a
    bootstrap-declared member is inserted `Active` **directly**, never
    passing through a `Down`→`Active` transition at all, so gating the flag
    on that one transition alone would leave every founding cluster member
    permanently `has_activated: false` — indistinguishable from a genuine
    orphan the instant it later legitimately crashes to `Down`, and thus
    wrongly swept. The fix was to compute the flag inside `Metadata::apply`
    itself, keyed purely on "does this command's desired status equal
    `Active`," so it's set correctly regardless of *which* caller (detector
    promotion, bootstrap, a future admin action) produces that state —
    catching every current and future path structurally instead of
    special-casing the one path a plan happened to name. When a new field
    is meant to answer "has X ever been true," audit every place the target
    state can be reached directly, not just the one transition path that
    prompted the field.
  - **A removal command's existing "member absent → no-op" branch can be
    silently wrong once a *second* claim shape without a member row
    exists.** `RemoveMember` predates `RegisterNode`'s claim-without-member
    shape (a control-role registration that, by design, never claims a
    `members` row) by several PRs; its own "already absent, idempotent
    retry" branch quietly assumed "no `members` row" meant "nothing to
    clean up," which was true until a command that can create an
    address-only claim existed. The fix (checking `node_addrs` inside that
    same branch and pruning it if present) was small, but finding it
    required explicitly asking "what does this cleanup command actually
    clean up, for *every* shape a claim can now take" rather than trusting
    a comment written before the second shape existed. Any command whose
    contract is "remove everything this id ever claimed" needs re-auditing
    every time a new claim-creating command ships, not just when it was
    first written.
  - **A structural safety property (an interleaving that must never
    corrupt state) is proven more rigorously as a pure unit test driving
    the exact functions production uses than as an approximated race under
    `SimEnv`'s timer-driven scheduling.** The catastrophic case here — a
    sweep proposal computed from a stale (pre-activation) view must never
    actually remove an already-`Active` member — depends on which of two
    independently-timed async loops' `propose()` call executes first,
    which `SimEnv`'s deterministic-but-still-concurrent-task scheduling
    cannot be reliably steered into either order from a black-box test
    (unlike a fixed heartbeat-vs-detector cadence race, there's no clean
    knob to force the ordering). Testing it as two direct, hand-ordered
    `Metadata::apply` calls (both orders) instead proves the guarding
    precondition exhaustively and is easier to read as a specification of
    the invariant than any timing-dependent integration test could be — and
    the complementary "no resurrection" half (a stray late heartbeat must
    never resurrect a removed claim) is provable the same way by calling
    the actual crate-private decision function (`liveness_transitions`)
    directly with the member already absent, rather than trying to force a
    heartbeat to arrive at exactly the wrong instant. When a plan asks you
    to prove "X never happens under this race," check whether the safety
    property is actually a precondition/postcondition of a pure function
    first — if so, a direct unit test of that function is the rigorous
    proof, and a `SimEnv` integration test (if written at all) is corroborating
    color, not the thing that actually establishes the property.

- **ADR 0018 PR2 (HLC commit timestamps as the CP-plane MVCC version, the
  range-seal design replacing `version_floor`) — three generalizable lessons:**
  - **A "provably disjoint reserved key prefix" claim needs an actual proof
    against the escaping scheme in use, not a plausibility check.** The
    original design for the seal marker's physical key proposed a bare
    `[0x00, 0x00]` lead pair, reasoning informally that `animus_tablet::escape`
    never emits it mid-string. True, but incomplete: `escape("")` — the
    **empty** table name, i.e. exactly the legacy whole-keyspace tablet's own
    `StorageScope` prefix — **is** `[0x00, 0x00]`, a real collision with an
    already-shipped physical key space. The fix was to re-derive disjointness
    from `escape`'s actual documented property (injective + prefix-free) and
    anchor the new key under an *already-enforced* reservation
    (`animus_control::syskv::RESERVED_NAMESPACE`, which `is_reserved_name`
    already forbids any table from claiming) rather than inventing a second,
    separately-unenforced reservation. When a design proposes a new "reserved"
    byte prefix for anything, check it against every *existing* prefix
    convention in the codebase (including the empty/default case), and prefer
    reusing an already-enforced reservation over minting a new one that would
    need its own enforcement wired through separately.
  - **Retiring a version-space-based invariant can silently orphan a
    non-obvious consumer of its arithmetic.** `RaftKvNode::erase_scope`
    stamped its tombstone version as `last_applied() + 1` — not itself named
    `version_floor` anywhere, so a grep for that literal name did not surface
    it, but it depended on exactly the same property (`effective_version`
    monotonically exceeding everything this group ever stamped) the floor
    scheme provided. Removing the floor without updating `erase_scope` left a
    real, silent regression (GC tombstones landing at a version *below* live
    HLC-packed data, so per-key LWW would keep the "erased" value forever) —
    caught only by re-running the existing `narrow_scope.rs` test, not by any
    grep. When retiring an invariant, grep for what it *guaranteed*
    ("strictly exceeds everything ever stamped"), not just its name — a
    caller can depend on the guarantee via completely different code with no
    lexical connection to the mechanism providing it.
  - **A new field on a replicated state machine (`Metadata`) is not complete
    until it's mirrored.** Adding `split_parents` (and, at the time, its
    merge-side counterpart `absorbed_by` — since removed, ADR 0044) to
    `Metadata` and updating `apply` was not enough — `animus-control`'s
    system-keyspace mirror (`syskv.rs`'s `EntityKind`, `mirror.rs`'s
    `apply_and_derive_mirror`/`apply_key_write`) has to independently learn
    to derive and decode writes for the new field, or the incremental
    delta-consumer path (`WatchMetadata`, ADR 0038 PR5) silently diverges
    from a full-fetch `Metadata` — caught by the crate's own differential-
    oracle test (`incremental_delta_apply_matches_direct_apply_for_deletes`),
    which is exactly what that test exists to catch. Any new `Metadata`
    field needs a matching `EntityKind` + key builder + both directions of
    `apply_key_write`, not just a field and an `apply` arm — grep
    `mirror.rs`'s exhaustive `match command` (no wildcard, by design) to find
    every site a new command variant must touch.
  - **A marker key meant to persist "this event happened, for this specific
    range" must be keyed by (proposer, range), not by proposer alone, the
    moment the same proposer can raise the event more than once over its
    lifetime with different payloads.** A tablet-id-only seal-marker key (as
    originally specified) would let a *second* split of the same source
    tablet silently overwrite the first split's marker with a narrower
    range before every waiting successor had observed it — a genuine,
    easy-to-miss liveness/correctness hazard for exactly the "one source,
    several splits over its life" case the design otherwise handles fine.
    Keying by `(proposer, payload)` instead makes every event's marker its
    own permanent record; re-verify this shape whenever a marker-key design
    assumes "this only ever happens once per identity."
- **Found, not yet root-caused: `cluster_growth.rs`'s
  `dashboard_health_recovers_after_grown_cluster_loses_an_original_node` can
  hang indefinitely (300s+ backstop, no recovery) rather than merely lag,
  when all three of this file's tests run concurrently in the same binary —
  and this is very likely a real reconciliation livelock, not the ordinary
  ADR 0038 apply-task lag the rest of this file's polls were converted to
  tolerate.** Discovered while modernizing this file's flat-deadline polls
  into the `poll_until_or_stalled` shape above: the new idle-progress
  diagnostics showed the control-plane leader's OWN `/admin/raft`
  `commit_index`/`last_applied`/`engine_applied_index` frozen **solid**
  (zero movement across a 200s+ instrumented window, sampled every 3s) while
  one tablet sat under-replicated (2 of 3 replicas, both non-voting ADR 0030
  growth nodes, after an ORIGINAL control-voter was killed) — i.e. the
  leader wasn't slowly catching up, it had stopped proposing *anything* new.
  Reproduction data: 6/6 solo runs of this test clean (~18s each); every
  pairwise combination with this file's other two tests clean; only the
  full three-test-concurrent binary reproduces it, and only intermittently
  (roughly 40% of sampled full-binary runs in this investigation). This
  rules out a simple "always-broken" logic bug (solo and pairwise runs
  prove the repair logic itself is correct and fast) but does NOT look like
  ordinary contention-driven lag either — genuine lag should still make
  *some* progress over 200s of sampling, not read as frozen at every 3s
  sample. Left as a known, precisely-characterized open issue rather than
  chased further in the poll-modernization PR that found it (this repo's own
  convention: root-cause+fix an incidental live bug as its own PR, not
  folded into unrelated work) — the `poll_until_or_stalled` conversion
  itself is unaffected and still correct (it just now reports this failure
  mode far more precisely, in ~60–300s with a frozen-watermark diagnostic,
  instead of the old flat 120s timeout's opaque "never repaired" message).
  Next step for whoever picks this up: reproduce with `RUST_LOG`/tracing on
  the control-plane leader specifically (not just `/admin/raft` polling) to
  see whether the placement reconciler's event loop is scheduled at all
  during the stall, or whether it runs but its `replan`/rebalance step
  concludes no action is needed for a still-under-replicated tablet whose
  only remaining replicas are both non-voting growth nodes.

  **Update: root-caused, and it's the second branch above, not a livelock at
  all.** Instrumenting `reconcile_loop` directly (a raw `eprintln!` per tick,
  removed before committing) showed it ticking exactly on schedule the
  entire time, correctly leader-elected, with a fully accurate `PlacementView`
  — and correctly computing **zero** proposals, every tick. The leader was
  never stuck; it was correctly enforcing a policy that was itself wrong.
  Tracing into `animus_placement::replan` found it: the stuck tablet's
  recorded RF was **2**, not 3 like its siblings — `2` replicas legitimately
  satisfies a policy of RF 2 forever, no matter how large the cluster grows.
  The bug was in `animusd::ClientCtx::provision_tablet`
  (`crates/animusd/src/lib.rs`): a tablet's placement policy was set to
  `PlacementPolicy::simple("cp-rf", t.replicas.len())` — the size of its
  *initial* replica set, observed at creation — instead of the fixed target
  `MAX_REPLICATION_FACTOR`. Under `cluster_growth.rs`'s heavy
  three-concurrent-cluster contention, the very first `put()` on a
  freshly-bootstrapped 3-node cluster could race ahead of all 3 original
  members' `Active` promotion landing in `Metadata`, provisioning the
  table's tablet with only 2 replicas — a legitimate, expected best-effort
  *initial* set — but then permanently recording RF 2 as the *policy*,
  which growing the cluster to 5 nodes later never revisited (`reconcile_
  placement` only repairs *violations of the recorded policy* — an
  under-observed RF simply becomes a new, permanently-satisfied target).
  Fixed by no longer deriving the policy from the observed replica count at
  all: it now always records `MAX_REPLICATION_FACTOR`, so a best-effort
  under-sized *initial* set self-heals via the reconciler's existing
  violation-repair path (the same one that already replaces a killed
  replica) the moment enough candidates are `Active` — see `provision_
  tablet`'s own doc, `meta::tests::reconcile_with_insufficient_candidates_
  is_a_stable_noop` (the "no proposal storm while under-candidated" proof),
  and `animusd/tests/tablet_rf_self_heals.rs` (the end-to-end regression:
  provision on a genuinely 2-node cluster, grow to 3, assert the tablet
  grows to 3 replicas too — which fails/hangs against the unfixed code,
  confirmed by temporarily reverting the fix and re-running it). Two
  generalizable lessons follow, below.
- **A "frozen" progress signal can mean "correctly nothing to do," not
  "stuck" — instrument the decision loop itself before assuming
  starvation.** The investigation above spent real time on two starvation-
  shaped hypotheses (a parked/starved reconcile task; cross-test port reuse
  poisoning a connection) before a direct `eprintln!` inside `reconcile_
  loop` — printing `is_leader`, the full `PlacementView`, and the proposal
  count every tick — immediately showed the loop healthy and the *decision*
  wrong. A frozen `engine_applied_index`/`commit_index` (the generic
  progress signal from the DRIVER_APPLIED entry above) only tells you
  "nothing committed" — it cannot distinguish a starved proposer from a
  proposer that correctly has nothing to propose. When a progress signal is
  frozen for far longer than any documented contention precedent (here:
  200s+ solid vs. the ~60s the DRIVER_APPLIED entry above had already
  characterized as normal-under-load), don't keep widening the timeout or
  chasing scheduling theories — instrument the specific decision function in
  the loop that would need to fire, and read its actual inputs/output. It is
  almost always cheaper than the starvation hypotheses it rules out.
- **Recording a policy derived from a point-in-time observation makes a
  transient condition permanent — record the *target*, and let
  reconciliation close the gap from observation to intent.**
  `provision_tablet` conflated two different things that happened to be
  computed from the same data at creation time: the tablet's *initial*
  replica set (legitimately best-effort — however many candidates are
  `Active` right now) and its *policy* (a durable, ongoing commitment to a
  desired state). Deriving the policy from the initial set's observed size
  meant a transient "not everyone has promoted yet" moment got baked in
  forever, because nothing ever re-derives an already-recorded policy from
  a fresher observation. The fix wasn't reading fresher data (that was
  already tried once for this exact call site — see the `metadata_fresh()`
  entry above — and only narrowed the window, it didn't close it, because
  *any* read, however fresh, can still land inside a real convergence-in-
  progress). The fix was to stop deriving the policy from an observation at
  all: record the fixed target, and lean on the reconciler's existing
  violation-repair path (already proven correct for "a replica died,
  replace it") to grow an under-sized initial set the moment reality
  catches up to the target. General check: when code sets a persistent,
  non-retried field from "whatever I can currently observe," ask whether
  that quantity is supposed to be an *intent* (should stay fixed regardless
  of when it's read) or a *snapshot* (fine to vary with timing) — and if
  it's an intent, a downstream repair loop must be able to re-derive and
  close the gap, not just react to future violations of whatever got
  recorded first.
- **A different pre-existing failure, exposed by the same `--no-fail-fast`
  workspace run that found the RF policy bug above: `animusd/tests/self_heal.rs`
  panicked under concurrent client load with `assert_ts_monotonic` — a
  hard-assert HLC/MVCC invariant (ADR 0018 §2), "raftkv apply: HLC ts ...
  did not strictly exceed the last applied ... the witnessing chain is
  broken."** Root-caused and fixed in `animus-cp-data`; two distinct bugs,
  found in sequence, both in the same neighborhood:
  1. **Minting a proposal's `ts` and appending it to the Raft log were two
     separate, unsynchronized steps with no `.await` between them.** Every
     mutating propose method did `let ts = self.mint_pushed(..); self.
     propose_and_wake(command)` — two sequential, non-yielding calls. Two
     concurrent proposers could mint ts=A then ts=B (A < B, correctly
     monotonic *as mints* — `Hlc`'s own mutex guarantees that much) but race
     to actually call `core.propose(..)` in the *opposite* order, so B's
     entry lands at a *lower* log index than A's — apply then sees ts=B
     then ts=A, a real decrease. **This specific shape is `ProdEnv`-only,
     provably**: with no `.await` point between mint and propose, two tasks
     can never be preempted mid-way under `SimEnv`'s single-threaded
     cooperative scheduler — only genuine OS-thread parallelism
     (`ProdEnv`'s multi-threaded tokio runtime) can interleave two
     sequential, non-yielding function calls from different tasks. Every
     other regression in this 25-binary crate drives `SimEnv`; this bug
     needed the one real-thread exception (`tests/prod_concurrent_ts_
     monotonic.rs`) to even exist, let alone catch. Fixed by
     `propose_ordered`: hold the group's own `core` lock across "compute
     `ts`" *and* "propose" as one atomic step — since every proposal to one
     group already funnels through that lock to get ordered at all, this
     adds no new bottleneck, it just closes the gap between two steps that
     already needed to agree.
  2. **A narrower, purely-logical bug surfaced only once (1) was fixed**:
     the write-push floor (`mint_pushed`) and the read-ceiling ratchet
     (`next_ceiling_candidate`) both needed a *new* floor — this leader's
     own last-*proposed* (logged, not necessarily applied yet) `ts` — since
     `committed_ceiling`/`ts_cache` only reflect *applied* state, and the
     apply task can lag the consensus loop by design (the driver-liveness
     split, ADR 0017). The first attempt folded this new floor in as
     `margin.max(last_proposed_ts)` and returned it **unmodified** whenever
     it beat the ratchet's own history — reproducing the exact bug it was
     supposed to fix, one level up: a `ReadCeiling` proposed right after a
     write could get the write's *exact* `ts`, an exact tie (not an
     inversion) `assert_ts_monotonic` also rejects. `margin` (always
     freshly `HLC_MAX_OFFSET` in the future) was safe to return verbatim;
     `last_proposed_ts` — a value some *other* command just used — was not.
     Fixed by treating both `last_proposed_ts` and the ratchet's own
     history as floors to *strictly exceed* (reusing the same
     bump-the-logical-component branch for both), never handing either
     back unmodified.
  **Diagnostic lesson**: found entirely by adding a temporary `eprintln!` at
  each `assert_ts_monotonic` call site printing `(index, command variant,
  ts)`, then re-running the failing test until it captured the exact
  colliding pair — for bug 2 this immediately showed `index=388 Put
  ts={13257,3}` followed by `index=389 ReadCeiling ts={13257,3}`, an
  *exact* tie between a *different* command type, which is what pointed
  straight at `next_ceiling_candidate` rather than back at the mint/propose
  race bug 1 had already fixed. A vaguer signal (just "ts inverted") would
  have wasted time re-litigating the already-fixed bug; printing the actual
  command types and indices at the failure point turned a second red
  herring into a five-minute diagnosis. General rule: when a hard assert
  fires inside a hot, generic loop (here, six near-identical `match` arms
  each calling the same assert), instrument with enough context to
  distinguish *which* case fired, not just that one did — cheap to add,
  removed before committing, and often the single highest-leverage step in
  the whole investigation.
  Regression: `animus-cp-data/tests/prod_concurrent_ts_monotonic.rs` (a
  real-thread `ProdEnv` test, confirmed to fail reliably against each of
  the two unfixed states in turn by temporarily reverting just that piece
  and re-running) plus `self_heal.rs` itself, now green. See
  `animus-cp-data/CLAUDE.md`'s `propose_ordered`/`next_ceiling_candidate`
  entries for the full mechanism.
  **The generalizable lesson**: *an invariant spanning two locks is not an
  invariant.* `Hlc`'s own mutex made minting monotonic in isolation;
  `core`'s mutex made proposing (log-order) serial in isolation — but
  nothing tied the two together, so "mint order == log order" was true by
  coincidence under low contention and false under real concurrency. A
  monotonic source feeding an ordered sink (a clock feeding a log, a
  sequence number feeding a queue, a version counter feeding a commit) must
  mint and enqueue in **one** critical section, or the two invariants each
  hold individually while their composition doesn't. When reviewing code
  that reads "compute X, then use X somewhere ordered," ask what stops a
  second caller's "compute X" from running between those two steps — if
  the answer is "nothing, but it's fine because X's own source is
  monotonic," that reasoning is the bug.
- **Per-crate `CLAUDE.md` guides re-drifted past the 40K-char memory-file
  warning by 2026-08-15, five days after the 2026-08-13 PR-changelog/
  test-roster trim above — this time the dominant offender was a different
  failure mode: restating a module's own `//!`/rustdoc doc comment**, not
  narrating PRs. `animus-control`/`animusd`/`animus-cp-data` had each grown
  a bullet (`meta.rs`, `segment_janitor.rs`/`index_drain.rs`,
  `cluster_segment_store.rs`/`cursor.rs`) that duplicated, near-verbatim, an
  80–95-line module `//!` doc the source already carried — and `animus-dynamo`
  had grown a full method/type inventory per module that `cargo doc` already
  renders. The doc comment and the guide bullet then drift independently the
  next time either is edited, and a reader can no longer tell which one is
  current. Fix, same shape as the 2026-08-13 trim: cut a duplicated
  inventory/essay to a one-or-two-line pointer bullet ("what it is + see its
  `//!` doc + ADR ref"), while keeping every gotcha/failure-contract/
  prohibition verbatim (compressing surrounding prose is fine; dropping the
  claim itself is not). Where a genuinely non-derivable contract lived
  buried inside an otherwise-duplicative section (`animusd`'s DynamoDB
  Streams wire-edge contracts and sealer-knob call-site detail — real
  content, just misfiled under a module that also duplicated its own doc
  comment), it was moved verbatim to a dedicated companion doc
  (`docs/streams-notes.md`) rather than deleted, with the guide left holding
  only a pointer. Trimmed `animus-control` 48.6K→29.6K, `animusd`
  87.3K→52.5K (plus the new companion doc), `animus-cp-data` 60.1K→40.5K,
  `animus-dynamo` 30.1K→15.6K, `animus-storage` 31.5K→26.4K (only its
  test-narration section, per the 2026-08-13 lesson's own pattern) — several
  landed above their aspirational target because the crate's actual
  gotcha/invariant content, kept in full per the rule above, is simply
  larger than the target for that crate. **General rule for review**: a
  guide addition that could be produced by pasting a module's own doc
  comment, or that `cargo doc`/`ls` already renders, is a sign to point at
  that source instead of copying it — reject it the same way a
  PR-changelog paragraph already gets rejected. A doc comment and a guide
  bullet describing the same mechanism are not two independent sources of
  truth; only one of them can be current. (2026-08-15.)
- **A cross-plane classification can't be one code-checked table when the
  dependency direction is one-way** (the consumer-offset consolidation, ADR
  0046's third amendment, 2026-08-16). `SplitPolicy`'s classification table
  has three entries — `"gsi"`, `"backfill:{index_name}"` (both
  `animus-cp-data::cursor`, a `KIND_CURSOR` row) and the stream seal
  watermark (`animus-control::Metadata`, a replicated field, not a cursor
  row at all). `animus-cp-data` cannot depend on `animus-control` (data
  plane never depends on control plane) or vice versa in a way that would
  let one function classify all three and have a compiler-checked test
  cover all three uniformly — a real registry spanning both would be the
  over-unification the approved plan explicitly rejected. The resolution:
  put the code-checked enumeration (`classify_tag` + a test asserting every
  *known* tag this crate constructs maps to a policy) in the lower crate,
  covering only what that crate can see, and add the cross-plane member as
  a **doc-level-only** row in the same table with an explicit note on why
  it has no corresponding code check. This is weaker than a single
  compiler-enforced table — a human, not the compiler, is the safety net
  for the doc-level row staying in sync — but it is the honest version of
  what a one-directional dependency graph allows, and it is strictly better
  than the alternative of not naming the third case at all. **General
  rule**: when a concept genuinely spans two components with a one-way
  dependency, don't force a single shared type/table across the boundary —
  split the enumeration at the boundary (code-checked on the side that can
  see everything relevant, doc-level-only for the rest) and say so
  explicitly in both the code comment and the table itself, rather than
  quietly under- or over-claiming what's actually verified.
- **A cursor tag's own literal string can't be imported across the same
  one-way dependency, and re-declaring it is the honest option, not a
  smell** (same delivery, 2026-08-16). `animus-cp-data::cursor::
  classify_tag` needs to compare against `"gsi"`/`"backfill:"`, but the
  canonical constants (`animusd::index_drain::GSI_TAG`, `backfill_tag`)
  live in a crate one layer *above* it in the dependency graph — the same
  direction constraint as the lesson above. Restating the literals as named
  constants with a doc comment pointing at the upstream source they must
  stay byte-identical to (rather than either an inline bare literal with no
  such pointer, or contorting the dependency graph to share them) keeps the
  duplication visible and intentional instead of accidental — the
  regression test that checks the *result* of the classification is the
  real safety net; the doc pointer is what tells the next person touching
  either side that the two need to move together.
- **A change that makes previously-permanent state transient must revisit
  every earlier test that observed that state live — and the fix is an
  erasure-proof accounting signal, never a race-tolerant wait** (ADR 0049
  Train A rung 4, 2026-08-16). Rung 4 extended the hot-trim arm to every
  table, which turned plain-table marker records from
  accumulate-forever into trimmed-within-a-tick — and silently broke (or
  made load-flaky) three earlier rungs' own marker-*emission* regressions,
  which counted live `pending_changes()` rows: under suite load the trim
  tick could win the race and delete the evidence between the write's ack
  and the test's read. No sleep/poll tuning can fix that shape — the
  window is real and the test's observable is genuinely transient. The
  sound fix is a signal the eraser itself maintains:
  `Metric::ChangeLogTrimmedTotal` counts every record the trim deletes, so
  the tests assert `live + trimmed-delta == N` — a union a racing trim
  cannot erase, and one that still fails on a genuine emission regression
  (both terms zero). General form: when rung N+1 adds a deleter for rung
  N's observable, rung N's tests must switch from observing the state to
  observing state-plus-deletions, and the deleter should export the
  deletion count as a real metric (it is genuine operational observability,
  not test scaffolding).

- **Regression cells that model a mechanism at the primitive level die
  with the mechanism — delete them WITH a tombstone naming where the
  replacement coverage lands, never silently** (ADR 0050 Train B rung 2,
  2026-08-16). F2b's immutable ranges deleted `narrow_scope`, which ~10
  test cells across four binaries used to *model* the zero-copy split
  (narrow + sibling-over-shared-rows) — including the #216/#220 data-loss
  and duplication regressions, the highest-value cells in the streams
  corpus. They could not be adapted (the seam they defend is structurally
  unrepresentable now), so the honest move is deletion plus an in-file
  tombstone stating (1) which cells died, (2) why they cannot be
  re-expressed, (3) which surviving tests carry any still-live property,
  and (4) which future rung rebuilds coverage on the successor mechanism.
  A parked `#[ignore]` is NOT available for this class — ignored tests
  still compile, and the API they call is gone; deleting without the
  tombstone would make the coverage loss invisible to the very review
  that must weigh it. Corollary: when a pivot disables a feature
  mid-train, every corpus that exercised the feature's *defense stack*
  (not just its happy path) needs an explicit disposition line in the
  rung report — coverage debt is tracked like code debt.

- **A freeze/quiesce-class gate must classify its writers, or it deadlocks
  the drain-before-retire ordering it exists to enable** (ADR 0050 Train B
  rung 5, 2026-08-17). The split-cutover freeze first rejected *every*
  write on the frozen parent — but the cutover's own vetoes wait for the
  GSI drain and backfill seeder to finish consuming that parent, and
  finishing requires those consumers to WRITE (cursor rows, footprints,
  synthetic seed records). Result: a structural deadlock — the gate blocked
  the very progress it was waiting on, caught red by the revived
  split-during-backfill e2e. The general form: any "stop the world, let
  consumers drain, then retire" sequence has two writer classes — user
  data (the thing being frozen) and consumer bookkeeping (the thing that
  measures drain progress) — and the gate is only sound if it blocks the
  first class alone. Corollary found the same day: run NO consumer arms at
  all on a not-yet-serving (`Building`) replica-to-be — its bookkeeping
  rows can land in a *sibling's* scope (the token-truncated cursor-key
  shape) and poison the sibling's own min-over-rows watermark.

- **Writes with no change record are invisible to every change-log-derived
  copy/tail — inventory them before trusting O(delta)** (ADR 0050 Train B
  rung 5, 2026-08-17). Transaction decisions (`TxnCommit`/`TxnAbort`) and
  resolves rewrite base rows without emitting any change record (ADR 0049
  gave every *client* mutation a record; these apply-side rewrites predate
  that contract). The split build's change-log tail therefore structurally
  misses them: a child could inherit a stale `Pending` txn record for an
  acked-committed transaction, and in-doubt recovery would later abort it —
  silent acked-write loss. The v1 answer is a full final-image re-scan of
  the frozen parent (state transfer, not log transfer — immune to signal
  gaps by construction); the O(delta) restoration (apply-side markers for
  signal-less rewrites) is a named follow-up. General form: before building
  anything on "the change log sees every mutation," grep every apply arm
  that calls the engine and list the ones that bypass record emission —
  the tail is only as complete as that list is empty.

- **A catch-up convergence predicate needs a liveness bound, and the load
  that breaks it is *sustained*, not bursty — bench with a continuous
  writer** (ADR 0050 Train B rung 8, 2026-08-17). The split driver's
  "converged = the latest tail pass shipped zero new records" was green
  through every e2e (their racing writes were finite bursts) and
  structurally un-satisfiable under a continuous sequential writer: every
  200ms tick's pass found that tick's own fresh writes, so the hot tablet
  — exactly the one that needs splitting — could never freeze. The rung-8
  bench (a *continuous* writer, not a burst) found it on its first run;
  the fix is a bounded chase (`SPLIT_MAX_TAIL_PASSES`) because the
  workflow's correctness never depended on the lag being zero — only the
  cutover blip's size did. Same run, same shape one layer down: the
  unfiltered final image re-shipped the whole table *inside the freeze
  window* (blip scaling with table size, not write rate) — fixed by a
  pre-bulk version floor. General form: for any "quiesce then flip"
  workflow, ask (1) can the quiesce condition ever be satisfied under
  worst-case sustained load, and (2) what inside the flip window scales
  with total size rather than recent activity; a bench with a
  continuous-load client answers both where burst-shaped e2es answer
  neither.

- **"Sweep for the retry-amplification shape" has to be re-run every time a
  new hand-rolled propose loop lands — the sweep's own corollary caught its
  third instance** (issue #268, 2026-08-17). `ClientCtx::provision_tablet`
  re-proposed `CreateTablet`/`SetTabletPolicy` on every 50ms poll tick for
  its whole 10s commit budget, exactly the unpaced shape `propose_and_await`
  fixed one layer up ("the pattern's most common instance was hiding one
  layer below") — measured at 264 `CreateTablet` + 240 `SetTabletPolicy`
  proposals for six tables' worth of first-put provisioning under a
  deliberately slowed (~80ms-fsync) disk, each duplicate a real control-log
  append fsynced and replicated under exactly the slow-commit conditions
  that made the wait long. On a starved 2-vCPU CI runner this
  self-amplification is what turned "commit is slow" into "provision burns
  its whole 10s budget, twice in a row" — the direct mechanism behind
  cp_txn.rs's 25s seed-put flake. Fixed with the same
  `SCHEMA_PROPOSE_PATIENCE` pacing (inline, not via `propose_and_await`,
  because the create arm must re-derive its allocator id + replica set
  fresh per proposal — the `trigger_split` stale-allocator lesson — and the
  needed command switches to `SetTabletPolicy` mid-loop); regression:
  `tests/provision_propose_pacing.rs`, which pins the leader's own log
  growth while provisioning grinds against a quorumless control plane.
  **Known remaining instances of the shape, deliberately left for their own
  PR** (they are not on the flake's path): `dynamo.rs`'s seven hand-rolled
  propose-then-poll loops (`create_table`'s schema + per-index waits,
  `enable_stream`/`disable_stream`, `create_index`, `set_index_status`,
  `drop_table_index`) — all fixed-command loops that could ride
  `propose_and_await` directly — and, pathological-state-only,
  `detect_loop`'s per-tick re-propose of an uncommittable liveness
  transition (visible while a control plane has lost quorum, i.e. while
  nothing can commit anyway; bounded by member count per tick).

- **A propose-then-confirm loop must end its wait the moment confirmation
  is provably futile, not "wait out the client timeout, which is correct"**
  (issue #268, 2026-08-17). The CP write confirm loops (`cp_put_local`/
  `cp_delete_local`/`cp_batch_local`/`cp_kind_local`/`cp_kind_raw_local`)
  polled value-equality for the full 10s `CLIENT_TIMEOUT` whenever an
  *accepted* entry's effect never appeared — a deposed leader's truncated
  entry, a freeze/seal apply-time no-op, a failed `KindBatch` condition.
  Each such attempt is a 10s client-visible stall, and the caller's retry
  then starts another: under the brief election churn a starved CI
  runner's slow fsyncs produce, two stacked burns exceeded cp_txn.rs's
  whole 25s put budget (observed live as an 11s "kind batch did not apply
  in time" attempt whose immediate retry succeeded in milliseconds).
  `animus-cp-data`'s own `wait_stage_outcome` already had the right shape
  (`!is_leader()` bails immediately); the animusd confirm loops now share
  it via `ClientCtx::confirm_wait_is_futile` — futile once
  `engine_applied_index() >= accepted_index` without the effect (sound
  because the apply task advances `engine_applied` only after merges are
  readable, and any re-elected leader's no-op pushes apply past a
  truncated index promptly) or once `!is_leader()`. **The coarse signal
  only ever ends a wait with a retryable error, never acks one** — success
  still requires exact effect equality (the false-ack hazard
  `cp_put_local`'s doc spells out is unchanged). Regression:
  `confirm_futility_tests` (in-crate, `cargo test -p animusd --lib`) — a
  condition-failed `KindBatch` no-ops at apply and must surface as a fast
  `"; retry"` error, not a 10s generic timeout. General form: when a
  confirm poll can distinguish "still in flight" from "can no longer
  land," burning the full timeout on the latter converts transient churn
  into stacked client stalls that read as unavailability.

- **A retry loop whose "retry" recomputes from unchanged inputs is a spin,
  not a retry — and only a cluster LARGER than the replication factor can
  prove a tablet-id-addressed forward** (ADR 0050 fork F5 fallout,
  2026-08-17). Every tablet-id-addressed internal RPC (`SeedRows`,
  `ForceSeal`, `TriggerAutoSplit`, `ClearBackfillCursor`, `StreamHotRead`)
  used a resolve → relay-once → on-"not the leader here"-refusal
  re-resolve-from-scratch loop, and one even documented that shape as
  "correct (converged-or-timeout)". It converges only when the calling
  node hosts a replica of the target tablet: the local replica's own
  leader hint is what changes between iterations. With **no** local
  replica, `resolve_cp_route`'s fallback deterministically returns the
  tablet's *first* metadata replica every time, that follower refuses
  with the real leader's address embedded in the refusal every time, and
  the loop threw that hint away every time — an infinite spin dressed as
  a retry. The split driver hit it the first time anyone ran a split on a
  cluster with more nodes than RF: fork F5 places children at fresh
  balance-chosen homes, so the parent's leader routinely hosts no replica
  of one child, and seeding that child spun forever — the parent parked
  `Splitting` holding every key with an empty `Building` child beside it,
  indefinitely (the "auto-split made 2 new tablets but never rebalanced
  the keys" field report). Every split e2e ran 3-node clusters at RF 3,
  where every node hosts every tablet and the no-local-replica branch is
  structurally unreachable. Two general forms: (1) for any retry loop,
  name the input that CHANGES on a failed attempt — if the answer is
  "none", it is a spin, and the fix is feeding the failure's own payload
  (here: the refusal's leader hint) back into the next attempt, done once
  at a shared choke point (`forward_to_tablet_leader`, now backing
  `cp_forward` and every tablet-addressed RPC alike); (2) the existing
  "test through a follower-connected node" rule is not enough for
  tablet-addressed forwards — the caller must host *no replica at all* of
  the target, which requires a cluster larger than RF
  (`split_build.rs::split_completes_when_a_child_lives_off_the_parent_leader_node`
  is the 5-node teeth).

- **An in-crate `#[cfg(test)]` bring-up (one that can't reach
  `tests/support`) needs the documented port-TOCTOU retry exactly as much
  as an external integration test does — the isolation from the shared
  helper doesn't exempt it from the race.** `crates/animusd/src/cql.rs`'s
  `cql_kind_write_tests::cql_whole_partition_delete_serves_from_every_node`
  brought up a 3-node cluster with a single pass of `free_addrs` + `run_node`
  per node and no retry, so it panicked `AddrInUse` under
  `cargo test --workspace` contention (CI run referenced in issue #278 item
  3) exactly like the pre-fix external tests this same lesson log already
  covers. Swept every other in-crate mod with a hand-rolled bring-up for the
  same gap: `cql_kind_write_tests::single_node`, `dynamo::
  stream_write_path_tests::single_node`, `index_drain::
  gsi_drain_cursor_tests::single_node`, `index_drain::stream_sealer_tests::
  single_node_with_streams`/`single_node_with_streams_and_quiesce_after`/
  `f11_end_to_end_auto_split_on_a_streamed_table_lands_a_token_aligned_boundary`'s
  inline bind, and `confirm_futility_tests::single_node` — all fixed with the
  same fresh-config-per-attempt bounded retry (16 attempts/50ms,
  `tests/split_build.rs::bring_up`'s shape). `index_drain::
  gsi_drain_cursor_tests::crash_mid_reconcile_recovers_without_skipping_or_corrupting_the_gsi`
  needed *both* idioms in the same test: its **first** bring-up can and does
  retry with fresh ports, but its **restart** rebinds the exact captured
  config/dir (the property under test) so it retries the rebind itself on a
  bounded wall-clock deadline instead, mirroring `tests/support/
  mod.rs::restart_same_addrs`. **Followed the established per-mod-duplication
  precedent already in this file** (each mod's own comment: "a different
  compilation unit... duplicated rather than shared") rather than
  introducing a new shared `pub(crate)` test helper, since the codebase had
  already made that call for the surrounding `single_node`/`free_addrs`
  fixtures these bring-ups live beside. **General rule: when auditing for a
  known test-infra race, grep for the raw primitive it exploits (here,
  every unretried `run_node`/`Node::bind` call reachable from a `#[cfg(test)]`
  mod), not just the file the bug report named** — the same hand-rolled
  bring-up shape gets copied wherever a private handle forces an in-crate
  test module, and each copy is independently exposed.
  (`crates/animusd/src/{cql,dynamo,index_drain,lib}.rs`.)

- **A halted-gated durability assert must audit every path that can
  abruptly stop the guarded driver, not just the one path someone
  remembered to wire it into** (issues #282/#279, 2026-08-18).
  `animus-cp-data`'s WAL/apply I/O tolerance (`persist_wal`/
  `flush_pending`) hard-panics on a live I/O error and tolerates one only
  while a group's `halted: AtomicBool` is set — but that flag latches via
  `RaftKvNode::shutdown()`, a distinct concept from `animusd::Node::
  shutdown()` (raw task-abort, the doc-blessed "kill node N"
  fault-injection idiom), and only `Node::shutdown_graceful` (the restart-
  test path, via `shutdown_all_cp_groups`) called it. Two much more common
  ways to abruptly stop a node never did: bare `Node::shutdown()` itself
  only `task.abort()`ed and tore down the env, and `Node` had no `Drop`
  impl at all, so a test that panics mid-poll and drops its `Vec<Node>`
  left every hosted driver task for the `#[tokio::test(multi_thread)]`
  runtime's own teardown to hard-cancel later, mid-I/O — with `halted`
  never latched either way. Both windows turn a routine kill or panic
  unwind racing an I/O hiccup into an unconditional panic
  indistinguishable from a genuine live durability fault. Fix: factor the
  latch step out (`ClusterEdgeState::halt_hosted_cp_groups` — a cheap,
  synchronous snapshot-and-store over every locally-registered group, no
  wait for the driver to actually stop) and call it first from bare
  `shutdown()`/`shutdown_and_wait()` too, plus from a new `Drop for Node`
  that does nothing else — deliberately preserving the pre-existing
  "dropping a `Node` without `shutdown()` leaves its tasks running"
  contract, since only the assert those still-live tasks can now safely
  race is what needed fixing. Safe to call from `Drop` specifically
  because `RaftKvNode::shutdown()` bottoms out in a plain `AtomicBool`
  store plus two `Notify` wakes: no I/O, no lock held across an `.await`,
  no dependency on a live tokio runtime (`Drop` can run inside or outside
  one) — verify this before ever calling anything from `Drop`, since most
  async primitives are not this safe. General form: when a correctness
  assert is gated on a flag latched by some "graceful shutdown" call, grep
  every way the guarded resource can be torn down — an explicit graceful
  call, a bare/forceful kill idiom, AND an implicit `Drop` from a panic
  unwind — not just the one call site whichever earlier fix's test
  happened to exercise. (`crates/animus-cp-data/tests/shutdown.rs`,
  `crates/animusd/src/lib.rs`.)
- **In a sequential multi-agent delivery chain, an implementation agent
  that ends its turn to "wait" for its own background command stalls the
  chain even though the harness auto-resumes it on completion** — the
  orchestrator cannot assume "no further message" means "still working";
  it must treat every completion notification as a checkpoint to verify
  the working tree/commit state directly rather than trusting the agent's
  last message, and agent briefs must say explicitly that a background
  command's completion re-invokes the agent and it must then continue to
  the end of the task, not stop again to wait.
- **A long chain of `cargo build`/`cargo test` runs across feature variants
  in one session can exhaust a fixed disk allowance mid-chain, and the
  failure it produces looks exactly like a compile bug, not a disk
  problem** — an ENOSPC-killed `rustc` surfaces as a plain nonzero exit
  (often 101) with truncated/garbled output, the same shape as a real
  compile error, so a session that hasn't been tracking free space burns
  time debugging source code that was never broken. Check free disk space
  before diagnosing a surprise compile failure that shows up late in a
  session, especially right after a `--all-features`/multi-crate build
  sweep.
- **When several agents edit the same workspace in parallel, a
  workspace-wide `cargo build`/`clippy` failure needs its error message
  read, not just its exit code, before deciding whose slice broke it**
  (TTL catalog slice, ADR 0051, 2026-08-19). Building `--workspace
  --all-targets` while sibling agents have half-finished edits elsewhere
  in the tree routinely fails for reasons that have nothing to do with
  your own change — e.g. an `error[E0004]: non-exhaustive patterns` on
  `animus-dynamo`'s `Operation` enum while implementing a `MetaCommand`
  addition in `animus-control` is a different agent's wire-adapter slice
  mid-edit, not a fallout from the `MetaCommand`/schema change. Confirm
  scope by grepping the error for your own new symbol names and by
  building/testing your own crate in isolation (`cargo build -p <crate>
  --all-targets`, `cargo test -p <crate>`) as the real gate — that must be
  genuinely green — and report the cross-crate failure verbatim rather
  than "fixing" code another agent is still writing.
- **Adding a variant to a replicated config command that, unlike its
  closest precedent, mints no identity label changes its idempotency
  rule, not just its payload shape** — modeling DynamoDB TTL's
  `MetaCommand::SetTableTtl` directly on `SetTableStream` would have made
  a same-attribute re-enable an error, because `SetTableStream` rejects
  re-enabling specifically to protect its minted `label` from going
  stale. TTL has no label to protect, so the correct rule is the opposite:
  re-enabling with the same value is a no-op, and changing the value in
  place (no disable/re-enable round trip) is `Applied` — both are real,
  legal DynamoDB operations. When cloning the shape of an existing
  replicated command for a new field, check *why* each of its rejects
  exists before copying it, not just what the reject is guarding on the
  surface.
- **Follow-up to the disk-space entry above: when it genuinely is ENOSPC**
  (real "No space left on device" errors from `rustc`/the linker, not a
  garbled compile error), `rm -rf target` alone often doesn't create enough
  headroom to finish a `--workspace --all-targets` build — this workspace's
  ~90 `animusd` integration test binaries each statically link the same
  large dependency set (tokio, opentelemetry, reqwest, icu\*, …) with full
  debug info by default, and linking one of them can itself need several
  hundred MB of scratch space. The fix that actually restores headroom
  without touching the checked-in `Cargo.toml` (no `[profile]` section
  exists there, so this is a machine-local, non-committed change): add
  `[profile.dev]`/`[profile.test]` `debug = 0` (plus `incremental = false`
  if the incremental cache itself is a large chunk of the growth) to
  `$CARGO_HOME/config.toml` (e.g. `/root/.cargo/config.toml`) — Cargo reads
  `[profile.*]` from config files, not only from a manifest — which cuts
  every test binary to a fraction of its debuginfo-enabled size and turns a
  session that can't link `cargo test --workspace` even once into one that
  fits comfortably. Confirm it took effect via the build summary line
  (`unoptimized` vs. `unoptimized + debuginfo`), and check `df -h` before
  and after a `rm -rf target` — if avail space right after the wipe is
  already within a few hundred MB of what one large link step needs, the
  wipe bought too little margin and the very next build can ENOSPC again
  mid-link.
- **Triaging a `ProdEnv` suite failure: compare its wall-clock against a
  clean run first — a markedly *shorter* run points at starvation, not at a
  logic bug.** The instinct is that a struggling run takes longer; the
  opposite is true here, because these suites are timeout-guarded. A test
  whose guard trips exits at the guard, while the same test passing runs
  its full body, so the failing run finishes *sooner*.
  `dynamo_index_writes` failed once at **57s** against a clean run's
  **127s**, and the cause was nothing in the diff: it had been launched
  alongside other `cargo` invocations of my own, and the CPU it lost to
  them was enough to trip a guard. Six subsequent runs (3 on the branch, 3
  on its base, run alone) all passed in ~127s.
  Two rules follow. **Operationally**: run a `ProdEnv` integration sweep
  *alone* — a concurrent `cargo test`/`clippy` on the same box is enough to
  manufacture failures that look like real ones. **In triage**: before
  reaching for the branch-vs-base comparison (which costs ~12 minutes
  here), check whether the change is even *reachable* from the failing
  suite — `grep`ping the suite for the new request field took seconds and
  proved the new code paths were unreachable and the emitted bytes
  identical, which is a stronger argument than any number of green reruns.
  Run the comparison to confirm, not to decide.
- **The `debug = 0` cargo-config fix for this workspace's disk pressure is
  worth applying *before* the first ENOSPC, not after the fourth.** The
  entry above describes it as the remedy once you are already wedged; in
  practice a session that builds `animusd --all-targets` more than a couple
  of times will get there, because each rebuild of the ~90 integration test
  binaries re-links the same large dependency set with full debuginfo.
  Measured in one session: writing `[profile.dev]`/`[profile.test]`
  `debug = 0`, `incremental = false` to `/root/.cargo/config.toml` and
  re-running took free space from **3.6 GB to 18 GB** and left the full
  build passing in 1m21s. Confirm it took by the build summary line —
  `unoptimized` with no `+ debuginfo`. Nothing is lost that matters here:
  these are integration tests asserting on HTTP responses, not something
  anyone attaches a debugger to. The cost of *not* doing it is worse than
  lost disk — an ENOSPC surfaces as a linker `cc` failure or a garbled
  compile error, so it reads as a code problem and costs a diagnosis
  before it costs a cleanup.

- **`cargo test --exact` with an incomplete test name runs ZERO tests and
  exits 0 — which reads as a pass.** `--exact` matches the *full* path
  including the module (`confirm_futility_tests::the_test`), not the leaf
  name. Give it a partial name and you get `test result: ok. 0 passed;
  0 failed; N filtered out` and a zero exit code. Every shell loop that
  counts successes by exit status will report a clean sweep having run
  nothing. This fooled the same session three separate times, including
  once where "6 passed under load" was six runs of nothing, and once while
  *verifying a fix* — the most expensive place to be wrong, because a
  green-looking mutation test is indistinguishable from a fix that works.
  **The rule: a zero test count is a failure, not a pass.** Assert on
  `1 passed` (or the expected count), never on the exit code alone, and
  establish a baseline run that the test executes *before* trusting any
  mutation or repetition result built on it. The `N filtered out` figure is
  the tell.
- **A removal ADR's own "every reference is updated" claim needs the same
  skepticism as any other ADR-vs-code drift — verify it, don't cite it.**
  ADR 0053 (2026-08-22, drop the CQL wire adapter) asserted a complete sweep:
  "every `cql`/`CQL` reference across the workspace... is updated... or
  reworded to note the history." An independent read found it false in the
  load-bearing places that matter most — the two ADRs it explicitly claimed
  to amend (ADR 0047's port-stride formula was never touched) plus two more
  it didn't even claim (ADR 0052, ADR 0035, both still asserting a
  "seven-port" stride as current fact with `cql` in the list), a live admin
  endpoint documented as still-present after its own deletion (ADR 0020's
  `POST /admin/data/cql`), a whole design-doc section for a UI panel deleted
  from the actual dashboard (ADR 0021's CQL query panel), a crate `CLAUDE.md`
  still listing a deleted `Metadata` field and a deleted `apply` arm, and a
  pre-existing test assert string naming a `MetaCommand` variant
  (`CreateKeyspace`) that no longer compiles anywhere else in the tree. **A
  stale ADR that claims it swept is worse than one that says nothing**,
  because a future reader trusts the claim instead of grepping to check it
  themselves — the fix (this entry's own change) treats "amends ADR N" as a
  testable assertion: either ADR N's own text now reflects the change, with
  a dated amendment note in the ADR 0001/0019/0044 style, or the claim gets
  softened to name what was actually swept (the structural, current-fact
  references) versus what was deliberately left alone (the much larger body
  of older ADRs' and the dated lessons log's own historical narrative
  prose, which is *supposed* to keep describing a deleted feature as it
  stood at each document's own date — rewriting those would launder history,
  not fix it). **When asked to "sweep every reference to X," grep the whole
  tree at the end and read every hit** — not just the files the task
  description named — because a sweep claim is exactly the kind of thing
  that silently narrows from "everything" to "everything I happened to
  touch" without anyone noticing until an independent verifier greps it.
- **An apply-time outcome channel keyed by Raft log index alone can tell
  no-op from failure, but it is NOT a confirm-of-success by itself — the
  proposer must also prove the applied entry is genuinely its OWN entry**
  (found in review of PR #334, 2026-08-23). `KindBatch`'s outcome channel
  (`KindBatchOutcomes`, `animus-cp-data`) was modeled directly on `Cas`'s
  `CasResults` — record what an entry did, keyed by the Raft log index
  `ProposeResult::Accepted` handed the proposer — and that shape is sound
  for CAS (a *committed* entry's index is unambiguous once you have it,
  because `compare_and_swap` only ever reads `cas_result` after confirming
  the entry applied via a value/ceiling read that already implies commit).
  It stopped being sound the moment `animusd::poll_probe` used the outcome
  alone, ahead of any value check, to end the wait: `ProposeResult::
  Accepted{index}` means "appended to **my own log**," never "committed" —
  and an appended-but-not-yet-committed entry's index can be **reoccupied**
  by a completely different command if this node loses leadership first
  (Raft log-matching truncates the original and a new leader's own entry
  lands at the identical position). Every replica — including the original
  proposer, once it reconnects as a follower — then records `Applied` at
  that index for the *reoccupying* entry's content, not the original
  proposer's. A `poll_probe` that trusted `Some(KindBatchOutcome::Applied)`
  alone read exactly that false signal and returned `Confirmed` to the
  client — a silently dropped write reported as a success, the precise
  failure class the at-most-once confirm-loop work (issue #268, this same
  log) exists to prevent, and worse for a non-idempotent numeric `ADD` than
  for an idempotent `Put` (a lost increment can't be told from a landed
  one by re-reading). **The sibling channel already had the fix**:
  `TxnStage`'s own `StageOutcome` carries the identical "index alone means
  no-op-vs-failure, never success" caveat in its doc, and every real
  coordinator (`ClientCtx::txn_prepare_pushing`) pairs a `Some(ts)` stage
  result with `txn_verify_staged` — an explicit read proving the staged
  content is genuinely present — before ever trusting it; `KindBatchOutcome`
  reused `StageOutcome`'s shape (index-keyed, apply-time-recorded) without
  reusing that verification discipline, because unlike a transaction stage a
  `KindBatch`'s own apply is fire-and-forget from the state machine's side —
  there was no second "verify" call anywhere in the design to carry the
  fix. The closed fix pairs the outcome with the entry's own Raft **term**
  (`ProposeResult::Accepted` now carries `term` alongside `index`;
  `KindBatchOutcomes` records `(term, outcome)`) and requires `term ==
  accepted_term` before ever treating `Applied` as a confirm — sound by
  Raft's log-matching property (identical index **and** term implies
  identical entry, cluster-wide, for the life of the log), the same
  identity guarantee a content check would need to approximate with a
  fingerprint (rejected here: a fixed-size hash risks a — admittedly
  astronomically unlikely — collision reintroducing the exact false-ack
  class the fix exists to close, where a cheap integer-term comparison
  carries zero such risk and needs no extra bytes in the bounded outcome
  map). **The general rule**: before reusing an existing outcome-channel
  *shape* for a new apply-time signal, ask what verification discipline the
  original shape's callers relied on to stay safe (a value check that
  implicitly proved commit, an explicit `verify_staged`-style read, a
  requirement that the caller only ever consult the channel after already
  knowing the entry committed) — copying the struct without copying (or
  deliberately, consciously replacing) that discipline is how a channel
  that was safe in its original home becomes a false-ack in its new one.
  Regression: `animus-cp-data/tests/kind_batch_outcome_identity.rs`
  (isolates a leader, lets it accept two entries that never commit, lets
  the survivors elect a new leader whose own election no-op and first real
  `KindBatch` occupy the identical two log positions, heals the partition,
  and asserts the truncated write never appears on any replica — proven red
  pre-fix by reverting `KindBatchOutcomes::record`'s term to a constant);
  `animusd`'s `kind_batch_signal_tests` module (a focused, table-driven unit
  suite for the extracted `classify_kind_batch_outcome` predicate
  `poll_probe` now calls, including the term-mismatch case — proven red
  pre-fix by dropping the predicate's term-equality guard).
  **Amendment (2026-08-29): the two siblings this entry's own "audit every
  sibling" rule pointed at — `Cas`'s `CasResults` and `TxnStage`'s
  `StageOutcomes` — turned out to have the identical gap, and this entry's
  own earlier claim that `CasResults`' shape was "sound for CAS" is
  corrected here rather than left to mislead a future reader.** That claim
  reasoned `compare_and_swap` only ever consulted `cas_result` "after
  confirming the entry applied via a value/ceiling read that already
  implies commit" — but the actual code never did any such confirming read:
  `compare_and_swap`'s own poll loop called `cas_result(index)` directly,
  with no value/ceiling check and (worse) no `is_leader()` guard either,
  despite a comment claiming a step-down check existed. `stage_outcome`/
  `wait_stage_outcome` had the `is_leader()` guard but the identical
  index-only lookup. Both are now fixed exactly like `KindBatchOutcomes`:
  `CasResults`/`StageOutcomes` store `(term, outcome)`, and
  `cas_result`/`stage_outcome` take the caller's own accepted `term`,
  returning `None` (never a stale `Some`) on a mismatch — propagating up
  through `wait_stage_outcome`, `txn_stage_anchor`/`txn_stage_participant`,
  and `compare_and_swap` (which also gained the missing `is_leader()` check
  its own comment had wrongly implied was already there). Regression:
  `animus-cp-data/tests/cas_outcome_identity.rs`, the `Cas` mirror of
  `kind_batch_outcome_identity.rs` — same isolate/accept/elect/collide/heal
  shape, proven red pre-fix by reverting `cas_result` to its index-only
  form, plus an end-to-end check that the public `compare_and_swap` async
  entry point itself never surfaces a false `Some(_)` for a truncated
  attempt. **`TxnResolve` has a related but distinct gap — it has no
  outcome channel at all, not a term-unsafe one — tracked separately, not
  closed by this round** (see this file's "A resolve's silent no-op is
  invisible to its own proposer" entry). **The generalizable lesson,
  restated**: "audit every sibling" is not satisfied by naming the siblings
  in a doc comment — it means actually reading each sibling's own call
  chain down to its lowest-level accessor before asserting any one of them
  is safe by a different mechanism; an assumption of safety that isn't
  independently verified is exactly as dangerous as the missing fix itself,
  because it makes a future auditor skip the very sibling that needed it.
- **A `CARGO_TARGET_DIR` shared across concurrently-running agent
  worktrees can silently link one session's build against ANOTHER
  session's stale source** (2026-08-23, discovered mid-fix on the
  `KindBatchOutcome` false-ack above). Two sibling sessions building
  crates at the same package/version/profile into the same shared target
  dir produce output artifacts whose filename hash is derived from the
  dependency/profile graph, not from the source files' own content or
  absolute path — so a session on a branch that has NOT yet landed a
  struct change (e.g. an unmodified worktree still on `main`) and a
  session that HAS landed it can both write to the identical `.rlib`/
  `.rmeta` path. Observed directly: `cargo test -p animus-cp-data --test
  <new file>` failed to compile with `variant Accepted does not have a
  field named term` and `expected Option<KindBatchOutcome>, found
  Option<(_, KindBatchOutcome)>` — both flatly contradicted by `grep`ping
  the very source files cargo had just reported compiling — while `ps
  aux` showed a sibling session's `cargo build`/`cargo test` running
  concurrently against the same `CARGO_TARGET_DIR` from a different
  worktree path. The error vanished on the next attempt with no source
  change, confirming a race rather than a real compile error. **Diagnosis
  rule**: a compile error that contradicts what's actually on disk (the
  compiler complaining about a shape the source doesn't have) is a
  first-class signal to check for a concurrent `cargo`/`rustc` process
  (`ps aux | grep cargo`) before debugging the "wrong" source — don't
  trust a single failing compile as proof the edit is broken. **Mitigation
  used here**: point `CARGO_TARGET_DIR` at a private, session-scoped
  directory (e.g. under the scratchpad) for the remainder of validation,
  accepting the slower from-scratch build, then re-verify once against the
  shared dir for the final gate run. This is a real gap in the "shared
  build cache" convention the root `CLAUDE.md`/session setup currently
  documents as safe by default — it is only safe when every concurrently-
  building session's *relevant* crates are source-identical, which is not
  guaranteed for parallel agents mid-way through independent, uncoordinated
  changes to the same crate.
- **A standing instruction buried as a Conventions bullet doesn't set session
  posture — put binding defaults at the top of CLAUDE.md *and* inject them at
  boot (2026-08-23).** The subagent-delegation / background-execution /
  stack-by-default rules had lived for weeks as one bullet deep in CLAUDE.md's
  Conventions, and the maintainer still had to re-issue all three as live
  instructions in session after session: agents either did token-heavy work
  inline in the main thread or ran subagents in the foreground and went
  silent, and shipped flat PRs where a stack was wanted. The lesson is about
  *placement and delivery*, not wording — an entry-point doc is skimmed once
  at boot with attention concentrated at the top, and a rule that must shape
  a session's default behavior (rather than answer a lookup) competes badly
  from position 8 of a bullet list 300 lines in. **Fix pattern**: promote
  behavioral defaults to a short, numbered "Session operating mode" section
  at the very top of CLAUDE.md, and have the SessionStart hook `cat` a
  faithful summary to stdout — a SessionStart hook's stdout is added to the
  agent's context, which turns the posture into a boot-time instruction that
  arrives the way a maintainer's own message would. Keep the two in sync by
  making CLAUDE.md the named source of truth and the hook text explicitly a
  summary of it; and make the rules *thresholded* so they need no judgment
  call (delegate anything multi-file/exploratory/build-heavy, inline only
  trivial one-file work; stack whenever there is more than one reviewable
  logical step, flat PRs state why).
- **A finished-but-unpushed tree is one container recycle from gone — commit
  per logical unit AND push the WIP branch after every commit, not just at
  the end (2026-08-27, ADR 0059 Train 3 PR②).** A first attempt at this
  exact task (`RestoreTableToPointInTime`) ran for a long single session,
  accumulated the full catalog/wire/replay-driver/e2e implementation, and
  was lost in its entirety to a container recycle before a single commit
  landed — every finding, every fix, every test had to be redone from
  scratch by the next session. The fix is procedural, not technical, and
  costs almost nothing: split the work into its natural logical units
  (catalog+validation; replay mechanism; wire+driver; corpus; docs) *as a
  task-planning decision made up front*, and after each one, commit **and**
  `git push` immediately — including WIP-quality commits on a branch with
  no PR yet. A branch pushed to the remote survives a container recycle;
  an uncommitted working tree does not. This session split the redo into
  exactly two implementation commits plus one corpus commit plus this docs
  commit, pushing after each, so no unit larger than roughly "one build+test
  cycle" was ever at risk again. **Generalizable rule**: for any
  long-running implementation task in an ephemeral session, treat "commit
  and push" as part of finishing a logical unit, not as a wrap-up step
  saved for the end — the session's own lifetime is not a resource the task
  plan can assume.

## Duplicated config needs a test, not a comment (ADR 0056)

The design tokens have to exist twice — the site ships static files, the
consoles `include_str!` theirs into the binary, and ADR 0021 bans the build step
that would generate one from the other. The previous revision handled this with
a comment in each file saying "same values as the other one". They drifted
anyway; that is what commissioning a design system turned up.

The fix that generalises: **when two files must stay identical and no mechanism
can make them one file, the mechanism is a test.**
`dashboard::tokens_css_matches_website_copy` is a three-line `assert_eq!` over
two `include_str!`s, and it makes the drift impossible rather than discouraged.
A comment asking humans to remember is not a mechanism.

The corollary is knowing what must NOT be in the check. The `@font-face` blocks
carry the same faces by different delivery (URL vs base64 `data:` URI) because
the deployments differ, so they are deliberately outside it. A check that
over-reaches gets disabled the first time it is legitimately wrong.

## Read the constraint before designing around it (ADR 0056)

ADR 0021 says the dashboard ships "no external fonts, no CDN". That was read as
"no webfonts", and the surfaces ran system stacks for it. The rule actually bans
*fetching from a third party* — self-hosting an OFL face, or embedding it in the
binary, always satisfied it. A whole design constraint was self-imposed by a
misreading of one clause.

Related, and worth checking before rejecting a face on weight: both faces here
turned out to be **variable** fonts, so one 22–23 KB Latin file covers the entire
weight range. The cost was first estimated per weight, which was wrong by about
4x and nearly drove a font substitution that was not needed. Fetch the file and
look before costing it.

## Closing a deferred prose sweep: rewrite in place as "new name (ADR NNNN's 'old name')" (2026-08-25, ADR 0056/0021/0052 rename follow-up)

The Ledger delivery's first PR deliberately left a prose sweep as a tracked
follow-up (see the "Renaming a branded UI surface..." entry above): dozens
of module-doc `//!`/`//` comments across `dashboard.rs`, `lib.rs`,
`console.rs`, every JS file header, every `console_*.rs` test header, two
crate `CLAUDE.md`s, and the root `CLAUDE.md` still said "AnimusDB Console"/
"AnimusDB Data Console" after the code was renamed to "animusd admin"/
"animusd console". Closing that follow-up found two things worth keeping
as a pattern:

1. **The old strings do not live in one obvious place.** They span doc
   comments (`rustdoc`-visible, so a stale one looks authoritative),
   ordinary `//`/`//!` file-header comments, and test-file header comments
   in three different crates — `rg -in "<old name>"` across the whole
   `crates/`+`docs/` tree (excluding the ADRs themselves and the
   engineering-lessons log, both of which are supposed to keep the
   historical name on record) is the only way to find the full set; a
   scan scoped to "the crate that owns the feature" misses the sibling
   crate's references (`animus-dynamo/CLAUDE.md`'s two mentions of "the
   Data Console" had nothing to do with `animusd` and would not have
   turned up in an `animusd`-scoped grep).
2. **Rewrite each hit as `new name (ADR NNNN's "old name")`, not a bare
   swap.** A plain find-and-replace to the new name loses the old name
   entirely, which breaks anyone (or any future grep) still searching for
   the term the ADR itself, an old commit message, or an old bug report
   uses; keeping the literal old string in quotes right next to the new
   one keeps both searchable from the same line, at the cost of a few
   extra words per comment. This is the same move ADR 0021's and ADR
   0052's own "naming disambiguation" amendments already made in prose;
   applying it at the comment-string level too avoids re-litigating "did
   we mean the old system or the new one" the next time someone greps for
   either name.

General rule: when a rename's brief says "sweep the deferred prose," grep
the *exact* old string(s) tree-wide first, do not trust the file list a
memory of "where the feature lives" would produce, and prefer
`new (old)`-shaped rewrites over silent replacement wherever the old name
might still be a search target (an ADR title, a test name, a changelog).

## Move a timer's trigger, not its mechanism — and check what the seam already gives you for free (ADR 0058 rung 4)

The in-place split's write-blip regression (measured ~726ms vs. the copy
path's ~300ms, rung 3's own bench) traced to one cause: a freshly-forked
child Raft group has no leader until *some* replica's cold, randomized
election timeout eventually fires. The fix was **not** a new consensus
primitive — it was making the replica that already knows it should lead
(the parent's own current leader, a fact every replica already computes
locally) call the *existing* pre-vote round immediately instead of waiting
for `tick`'s timer to expire. `RaftCore::campaign_now` is three lines: a
role guard, then `self.start_pre_vote(now, entropy)` — the identical
function a real timeout calls.

Two things worth generalizing from this:

1. **When only *when* something fires needs to change, not *what* it does
   when it fires, reuse the existing mechanism at the new trigger point
   rather than building a parallel one.** A hand-rolled "immediate leader"
   path would have needed its own safety argument for every interaction
   pre-vote already has one for (a peer that hasn't started yet, two
   replicas racing to self-nominate, a lease check against a live leader).
   Calling `start_pre_vote` early inherits all of them for free — including
   the fallback: a round that wins no majority in time re-arms the ordinary
   election timer via the same `reset_election_timer` call the timeout path
   uses, so "campaign failed" and "never campaigned at all" converge on the
   identical retry, with no second code path to keep in sync.
2. **Before assuming an early message needs new machinery to reach a
   not-yet-started peer, check whether the transport already queues by
   destination regardless of consumer readiness.** ADR 0026's multiplexed
   `(node, stream)` addressing already queues an inbound message whether or
   not anything is currently polling `recv_stream` for that stream — true of
   both `ProdEnv`'s per-stream `Demux` and `SimEnv`'s inbox map. That made
   "send a `PreVote` to a peer whose own `RaftKvNode` for this child hasn't
   started yet" a non-problem: the message simply waits in that peer's inbox
   until its own bootstrap reaches its first receive, at which point it's
   just this group's first inbound message. No new buffering, no "is the
   peer up yet" probe, no retry-until-reachable loop.

The general shape: a live-timeout problem is not automatically a
"needs-new-protocol" problem — check first whether calling the existing
timeout handler early, at a caller-computed trigger, already has every
safety property the new call site needs, and whether the seam underneath it
already tolerates the ordering you're about to introduce.

## A syskv composite key's physical encoding order and a `Metadata` map's logical key order are two separate decisions (ADR 0059 §3)

Adding the backup catalog's per-tablet progress collection
(`Metadata::backup_tablet_progress: BTreeMap<(BackupId, TabletId), _>`)
needed a `syskv` key for the same `(backup_id, tablet)` pair. The existing
precedent (`index_backfill_key(tablet, index)`) encodes its fixed-width
field first — `TabletId`'s 8 bytes, then the variable-length index name —
so `decode_index_backfill_id` never has to guess where the boundary is.
That precedent's field order happens to *also* match `Metadata::
index_backfill`'s own map-key order, which made it easy to assume the two
orders are the same constraint. They aren't: the physical key only needs
its fixed-width field first (a decoding requirement); the `Metadata`
field's own tuple order is a separate, independent choice about what
reads naturally for that collection's own consumers (here, ADR 0059 §3
explicitly wants `(backup_id, tablet)` — a `DescribeBackup` reader groups
by backup first). Encoding `backup_progress_key` as `(tablet, backup_id)`
while keeping `Metadata::backup_tablet_progress`'s key as `(BackupId,
TabletId)` satisfies both constraints at once, at the cost of one
documented swap at the two points that cross the boundary
(`mirror.rs`'s `apply_put`/`apply_delete`). Worth stating plainly rather
than silently reusing the sibling kind's field order out of habit: check
each key shape's *own* two constraints (decodability, and what the owning
collection's readers want) rather than assuming a lookalike precedent's
order was load-bearing in both places it happened to hold.

## Derive a command's payload from already-committed state at apply time, not from what the proposer captured (ADR 0059 §3)

`BeginBackup`'s manifest stub (the source table's schema snapshot and its
current tablet list) could have been computed once by the proposing wire
node and carried on the command, the way a naive first draft would write
it. That would reproduce the exact hazard `CreateTablet`/`BeginSplit`
already avoid: two proposers (or one proposer retried after a stale read)
computing the stub from two different snapshots would make `apply`'s
result depend on *which proposal landed first*, even though every replica
runs the identical deterministic function — a Raft replica's job is to
agree on one input and compute the same output, not to trust an
already-computed output riding along. The fix is the same one this
codebase already uses for `BeginSplit`'s child ranges and `CutoverSplit`'s
child recomputation: `BeginBackup`'s apply arm reads `self.schemas`/
`self.tablets_for_table` itself, at apply time, and derives the stub from
current agreed state — the command carries only the identity fields
(`backup_id`, `table`, the ADR-0051-style wall-clock stamp) that *can't*
be derived from replicated state. When a new command's payload could
either be captured by the proposer or derived from `Metadata` already
committed to that point, derive it — the proposer-captured version always
requires arguing every replica sees the same input, and pure derivation
makes that argument for free.

## A new kind-scan primitive must filter transaction-record marker keys too, not just resolve envelopes (ADR 0059 §4/§5)

Building `RaftKvNode::local_scan_kind_snapshot` (the backup capture
driver's snapshot-pinned, intent-resolved sweep) by composing `storage
.scan_at` + `resolve_once_step` looked complete — every value came back
correctly resolved (committed, or silently dropped if still `Pending`).
The very first test against a genuine staged-but-unresolved transaction
failed anyway: the decoded row set contained an extra entry that turned
out to be `txn.rs`'s own internal record-marker key (`txn::record_key`,
the atomic-commit-point row `TxnStage` writes into the anchor's own
scope), not anything a caller ever wrote. `resolve_scan_rows` — the
existing shared post-processing step every other scan in this crate
already goes through — has always dropped these (`if
txn::is_record_key(&key) { continue; }`) before resolving, but that
check lives in `resolve_scan_rows` itself, not in `resolve_once_step`
(the lower-level per-row resolver `local_scan_kind_snapshot` composed
directly, to get its own cursor/limit semantics). Building a new scan
primitive directly on `resolve_once_step` instead of the existing
`resolve_scan_rows`/`local_scan_kind_ordered` wrappers silently loses
every filter those wrappers apply, not just the ones a superficial read of
`resolve_once_step`'s own doc would expect. General rule: when a new read
primitive needs its own cursor/limit shape but the *value-resolution* part
is identical to an existing scan, grep every filter step the existing
wrapper applies (record-marker keys today; whatever gets added next) and
carry each one forward explicitly — never assume "I called the same
low-level resolver" is equivalent to "I inherited the same scan
semantics." A unit test exercising a real staged-and-never-resolved
transaction (not just a value-only round trip) is what caught this in one
run; a primitive whose only test coverage is "committed values resolve
correctly" would have shipped this defect silently.

## A catalog's "who satisfies this obligation" predicate must be the SAME accessor everywhere it's asked, or a stale report double-counts (ADR 0059 §6)

The backup-vs-split race (a pinned tablet retires mid-capture; its live
`split_lineage` descendants take over its share) has a subtler failure
mode than "the retired tablet's own report is missing": it's still
*possible* for the retired tablet to have reported successfully **before**
splitting (a `RecordBackupTabletComplete` that legitimately committed
first, on an ordinary unrelated split racing an already-finished tablet).
That report is real and harmless to leave on file — but once the split
lands, the retired id's own row in `Metadata::backup_tablet_progress`
becomes a **stale, superseded** entry: the tablet's current live
capture-frontier is its two children, and if a naive "sum every progress
row tagged with this backup id" accessor is used to build the final
manifest, the retired parent's full-range capture and its two children's
own (later, independent, narrower-range) captures all land in the
manifest side by side — a real content bug (every row in the split
range appears in the backup twice), not just an efficiency loss. The fix
was to derive one canonical accessor
(`Metadata::backup_manifest_tablet_progress`, built on
`live_split_descendants`) that always answers "the tablet whose id is
authoritative for the completed capture of this pinned tablet's range,
right now" — and to route *every* consumer through it: the readiness
check (`backup_ready_to_complete`), the manifest assembly (the completion
aggregator), and even the pre-existing `backup_total_bytes` display
figure (which had the identical latent double-count bug before this PR,
just never triggered — no code path had ever produced two progress rows
covering the same range before ADR 0059 §6 made that possible). General
rule: when a catalog gains a "this identity's obligation may transfer to
a different identity later" relationship (a retired-and-replaced key), a
single row's own presence is never sufficient to answer "has this
obligation been met" — build one shared accessor that resolves identity
to its *current* authoritative set first, and audit every existing
consumer of the raw per-identity map for the same latent assumption, not
just the new caller that motivated the change.
- **A per-tick consumer arm superseded by a dedicated driver during one
  lifecycle phase needs the SAME exclusion guard as every sibling arm the
  driver also supersedes — audit them together, not one at a time (issue
  #298, `animusd::index_drain::change_consumer_loop`).** `trim_janitor`
  was correctly gated `if !splitting` (a `Splitting` tablet's own endgame
  driver holds trim for the whole build/freeze window), but `seal_tick` —
  two lines below it, in the same per-tick loop, over the same
  `splitting` boolean already in scope — was not, so it could fire
  concurrently with that same driver's own dedicated final-seal loop for
  the identical tablet. Reproduced directly: the same record delivered
  twice under two *adjacent* epochs of one tablet (never within one
  epoch), which is the tell that distinguishes this from a same-epoch
  proposal collision (that shape is already caught and retried around by
  a content-aware confirm) — two independently-computed, non-colliding
  epoch numbers whose *coverage* silently overlapped instead. The general
  rule: when a lifecycle phase hands one concern (trim, seal, drain,
  whatever) to a dedicated driver for its duration, grep every *sibling*
  per-tick arm in the same loop for the identical exclusion condition —
  a driver superseding one arm and not its neighbor is exactly the kind
  of asymmetry a reviewer skims past because the superseded arm's own
  gate reads correctly in isolation.
- **"A read feeding a non-retried, permanent decision must use
  `metadata_fresh()`" extends to "a decision that becomes immutable
  bytes the instant it's made" — not just literal one-shot writes (issue
  #298, `animusd::index_drain::seal_now`).** This crate's own
  `CLAUDE.md` already names the first case (a schema commit-wait, a
  conditional-write existence gate). `seal_now`'s watermark/next-epoch
  computation looked like a routine, retried-if-wrong read — it feeds a
  loop that keeps calling `seal_now` until nothing is left to seal, so a
  wrong read *seems* self-correcting. It isn't: the read decides which
  bytes go into a segment that is written to the object store and
  cataloged in the same call, and a sealed segment is never revised —
  only ever superseded by a later, non-overlapping one. Under
  `effective_metadata()`'s ordinary staleness (a local node's own
  just-committed control-plane proposal not yet reflected in its own
  cache), a "retry" is really a *second, independent* decision computed
  against a floor that doesn't yet exclude what the first one already
  covered, silently producing overlapping immutable output with no later
  correction point. The generalized discipline: a decision is
  "non-retried" for this purpose if *what it decides* is committed to
  something immutable in the same call, even if the *calling loop*
  retries — the loop retrying doesn't make the decision retriable if
  each attempt's own output can never be taken back.
- **An "unreachable from this caller by construction" branch is a claim
  about the absence of concurrent structural change, not a permanent
  fact — re-derive it whenever splits (or any other lifecycle event that
  moves/removes state) enter the picture (issue #298, `animusd::
  txn_resolver_loop`).** The comment justifying `None` as `txn_recover`'s
  orphan-path hint was correct the day it was written: `pending_txns`
  only ever tracks a genuine, locally-anchored `Pending` record, so the
  record-absent branch that hint feeds "can't" run for this caller. That
  reasoning implicitly assumed the record stays reachable at its logical
  position for the whole recovery window — true until a transaction
  record turned out to be an ordinary logical key of its anchor tablet,
  riding the identical split clone/trim path every other row does. Once
  splits could relocate or (per a related, still-open investigation)
  possibly drop that row, the "unreachable" branch became reachable, and
  with no hint it had no fallback at all — it reported "still pending"
  forever, matching an observed transaction stuck reporting
  "TransactionConflict: ongoing" for a full test budget. The fix (passing
  the `created_ts` this caller already had on hand, previously discarded
  as `_created_ts`) is a no-op when the original assumption still holds
  and only takes effect in exactly the case the comment said couldn't
  happen. The general check: when a comment justifies skipping a
  fallback because "X can't happen from this caller," ask whether X's
  impossibility depends on nothing else in the system moving the state X
  would need — a lifecycle mechanism added *after* that comment was
  written (here, in-place split) is exactly the kind of change that can
  quietly invalidate it without touching the caller at all.

## A wake that only shortcuts discovery of a durable fact never needs to be durable itself (ADR 0058 rung 4 layer 1)

The rung-4 fix above closed the first-vote latency; a second residual
remained — the SECOND voter a fresh child needs to grant that vote was
still discovering the fork on its own next *scheduled* poll (up to the
50ms fast-poll interval, on top of this host's ~100ms round-trip floor).
The fix followed the identical "move the trigger, not the mechanism"
shape one more time: a new signal (`ForkSignal`, the same `AtomicBool` +
`AtomicWaker` pattern `animus-cp-data` already uses for `ProposeSignal`/
`ApplySignal`/`WakeSignal`) wakes the reconciler's tick the instant the
apply task observes the fork, instead of waiting out the poll — but the
materialization logic it wakes is untouched.

The part worth generalizing on its own: **the new signal is deliberately
not durable, and that is fine precisely because the fact it announces
already is.** A crash between the apply task raising `ForkSignal` and
anything consuming it loses the wake completely — a restarted replica has
no memory it ever fired. That is safe by construction, not by luck,
because the wake never became the *only* way to learn the fact: the
reconciler's ordinary periodic tick already re-derives "did this tablet
fork" from a durable marker (`pending_split()`) on every single pass,
whether or not any wake ever arrived. The eager path is purely additive —
it makes the common case fast; the unconditional fallback (already
required for the crash-recovery case a slower poll always had to handle)
is what makes correctness independent of whether the wake landed at all.

This generalizes beyond this one signal: **when adding an eager
notification on top of an existing polling/fallback loop, resist the urge
to make the notification itself crash-safe.** Persisting it (or rebuilding
it deterministically across a restart) is extra machinery solving a
problem that doesn't exist as long as the poll loop it shortcuts remains
correct and unconditional on its own. The corollary is a real, checkable
test obligation, not just an argument: prove the crash-loses-the-wake case
explicitly (this rung's `crash_after_apply_loses_the_eager_wake_but_
reconciler_fallback_recovers` scenario), rather than treating "the eager
path worked in every other test" as evidence the fallback still does its
job unattended.

A second, smaller point the same rung's own corpus cell needed: **an eager
notification and the fallback it shortcuts are, by construction, going to
race** — the eager wake fires a tick, and the ordinary periodic tick can
still land moments later on the identical already-handled state. This is
not a bug to prevent; it is a property to prove benign. If the mechanism
being triggered is already idempotent (as any correct crash-recovery path
must be — G4's optimistic-claim-then-execute discipline here), the fix
needs no debouncing, coordination, or "only once" bookkeeping between the
two triggers at all — just a test that fires both, back to back, and
asserts nothing changed the second time.

## Flipping an enum's `#[default]` silently re-points every `.default()` pin — audit callers for ones that meant "the current default," not "whatever the default is" (ADR 0058 rung 4 layer 2)

`animusd::config::SplitMode` had a documented "pinned to `Copy`" convention:
every deployment shape/test that didn't have an explicit override called
`SplitMode::default()`, and the doc comments at each of those call sites
said things like "byte-for-byte the original ADR 0050 workflow." Flipping
`#[default]` from `Copy` to `InPlace` (the whole point of this layer) is a
one-line change at the enum, but it silently changes the behavior of every
one of those `.default()` call sites at once — including ones nobody was
thinking about when they wrote `SplitMode::default()` instead of
`SplitMode::Copy` explicitly. That fan-out is exactly the feature for
production code (it is what makes "every deployment shape splits in-place
unless told otherwise" a one-line change) and exactly the hazard for tests:
a test that calls a `.default()`-threading bring-up helper because it
never needed to think about the knob is fine either way, but a test that
calls it because it happens to currently produce the behavior the test
actually asserts on will silently start asserting on the wrong thing, with
no compiler error and — if the two workflows converge to similar-looking
end states — sometimes no test failure either.

The concrete miss this rung found: two test files
(`animusd/tests/split_lifecycle.rs`, `animusd/tests/admin_endpoint.rs`'s
`admin_split_kicks_off_the_copy_based_workflow`) poll for a `Splitting`
parent with exactly two `Building` children — an intermediate metadata
shape that only the ADR 0050 copy workflow ever produces (the ADR 0058
in-place fork mints both children directly `Active` at cutover, with no
`Building` row ever recorded). Both files brought clusters up through
`animusd::run_node`, which threads `SplitMode::default()` with no override.
Before this layer, that was an accurate (if implicit) pin to `Copy`; after
it, the exact same code silently starts requesting `BeginSplitInPlace`
instead, and the poll would simply never observe `building.len() == 2` —
a hang-then-timeout failure with a confusing symptom (the assertion
message names the state it never saw, not the mode that made it
unreachable). `split_build.rs` (ADR 0050's own end-to-end file: build,
freeze, tail, cutover, and the copy bench) had the identical exposure. All
three were fixed the same way — call the split-mode-taking entry point
directly with `SplitMode::Copy` explicit, with a comment naming *why* (this
file/test is about the copy workflow's own mechanics, not "a split" in
general) so a future default flip doesn't quietly re-break the same
assertion.

The generalizable rule: **when a type's `#[default]` is about to change,
grep every `Type::default()` call site, not just the ones with a comment
already flagging them as pinned** — a caller that never explicitly named
the variant is exactly the one most likely to be *relying* on today's
default without saying so. Classify each one: does this caller want
"whatever the type's default currently is" (generic behavior, safe to let
ride the flip — and often the *point* of flipping the default, since it's
how the new behavior gets exercised by the existing test suite for free),
or does it want "the specific variant that happens to be the default
today" (needs an explicit pin, or it silently starts testing something
else, or nothing at all). The tell for the second category in this
codebase was assertions on a workflow's own *intermediate* state shape
(not just its converged end state) — two different mechanisms that
converge to equivalent-looking final outcomes can still have completely
different, mutually exclusive transient states along the way, and a test
built around one mechanism's transient will simply never fire under the
other's.

## A much faster mechanism doesn't just change latency numbers — it changes how often a fixed test budget exercises the thing that was already flaky (ADR 0058 rung 4 layer 2)

Two more `animusd/tests/streams_e2e.rs` failures surfaced flipping
`SplitMode`'s default, past the intermediate-state-shape ones above, and
they generalize differently.

**The first was a pre-existing, mislabeled test bug this flip merely made
visible.** `multi_split_soak_streamed_gsi_table_under_mixed_load`'s
"zero lost writes" check read an item back via a bare `GetItem` — no
`ConsistentRead: true` — exactly the ADR 0055 gotcha this crate's own
`CLAUDE.md` already documents ("a read that verifies a write must ask for
the linearizable read"). Under the copy workflow's slower, seconds-long
per-split timeline, the eventually-consistent replica this test happened
to read from had ample time to catch up between the write and the
verification read, so the missing annotation never mattered in practice.
In-place's much faster convergence didn't introduce a new staleness
window — it didn't shrink the window fast enough relative to the rest of
the test to hide the *pre-existing* one, and the read started actually
observing it. Fixed the only way ADR 0055 sanctions: add
`ConsistentRead: true` to the read that is asserting durability, not
staleness tolerance.

**The second is a real, already-tracked-but-unresolved bug (issue #298,
an exactly-once duplication/deficit at a split boundary, root cause
unknown) that the flip made dramatically easier to hit — not by changing
its trigger condition, but by changing how many times a fixed-duration
test exercises that trigger.** `multi_split_soak_streamed_gsi_table_under_
mixed_load` runs a fixed workload (120 writes, a 300s budget) that
auto-splits repeatedly; under copy's per-split cost, that budget fits
however many splits copy's cadence allows, and #298 was rare enough to be
"occasionally sighted" in that regime. In-place's ~1.8x-faster convergence
lets dramatically more splits complete inside the identical fixed budget
— every one of them another roll of the dice against whatever race #298
actually is — and three consecutive runs under in-place reproduced it
every time (a deficit-shaped failure on one run, an over-count-shaped one
on another — both are #298's documented symptom family, just its two
different faces). Pinning this one soak back to `SplitMode::Copy`
(`start_streamed_cluster_full_copy_pinned`) restored its original,
rare-in-practice flake rate — 3/3 green in the retest — without touching
#298 itself, which stays out of scope here (a pre-existing bug gets its
own investigation and its own change, never a drive-by fix riding an
unrelated default flip).

**The generalizable point**: when a change makes something *faster*, audit
every fixed-duration/fixed-iteration-count test that exercises the sped-up
path for whether it now completes measurably more repetitions of that path
per run — a soak/stress test's own bug-detection power is a function of
repetitions-per-budget, not just wall-clock coverage, and a large enough
speedup can turn "reproduces rarely enough to file and defer" into
"reproduces every run" without the underlying bug changing at all. That
shift is worth surfacing loudly (as this entry does) rather than either
silently loosening the newly-flaky assertion or silently pinning it away
without a trail: **`Copy`'s eventual deletion (this same ADR's next rung)
removes the option to pin away from this exact soak**, so whoever does
that deletion needs to know, going in, that issue #298 will need to
actually be resolved (or the soak's own budget/iteration count
deliberately re-tuned) before that layer can ship — not rediscovered cold
at that point.

## A mobile `grid-template-columns: 1fr` override silently reintroduces the desktop overflow it was meant to fix (website responsive pass, ADR 0056 skin)

The site's desktop grids (`.hero-grid`, `.split`, `.foot-grid`) were all
written correctly, as `minmax(0, 1fr) minmax(0, 1fr)` — the standard
CSS-grid guard against a track's "automatic minimum size" defaulting to the
*min-content* width of whatever's inside it. But their `@media` overrides for
the mobile single-column layout were written as plain `grid-template-columns:
1fr;`, dropping the `minmax(0, ...)` the desktop rule had. Nothing looked
wrong reading the CSS in isolation — a single `1fr` column obviously fills
100% of the container, so it read as strictly *simpler* than the two-column
desktop version, not weaker.

It broke exactly where a grid item contained an unbreakable line: a
`.plate .cl` terminal/diff line has `white-space: pre`, and one such line
(`- endpoint: dynamodb.eu-west-1.amazonaws.com`) was longer than the mobile
viewport. Its nested `overflow-x: auto` container did make *it* scroll
locally as intended — but the plain-`1fr` grid track one level up still sized
itself to that line's min-content width first, so the grid item's own box
(not just its content) rendered wider than the viewport and the page itself
gained horizontal scroll. A descendant's own `overflow: auto` does **not**
rescue an ancestor grid/flex track's auto-min-size calculation; only
`minmax(0, ...)` (or `min-width: 0` on the item) on the track/item that is
actually oversized does that — matching what the desktop rule already relied
on for the exact same content.

General rule: **`minmax(0, 1fr)` is not a two-column-only idiom** — carry it
into every breakpoint override of a `grid-template-columns` declaration,
including the single-column ones, whenever any descendant might contain
unbreakable content (`white-space: nowrap`/`pre`, a long token, a wide inline
code/terminal line). A quick audit technique: `grep grid-template-columns` the
stylesheet and check that every bare `1fr`/`Npx` track (not already wrapped in
`minmax(0, ...)` or `min(...)`) has one — a bare `repeat(N, 1fr)` mobile
override is the same bug with more columns. Verifying "no viewport overflow"
by reading the CSS's collapse breakpoints is not enough; render at the target
width and check `document.documentElement.scrollWidth`, because this class of
bug is invisible in the source and only appears against real content.

- **A Playwright `waitUntil: 'networkidle'` goto against the `website/`
  pages can hang indefinitely in this sandbox (2026-08-26, mobile spacing
  pass)** — the pages pull Google Fonts over HTTPS through the proxy; most
  of the time that request resolves quickly, but occasionally it (or some
  other cross-origin request) stays pending and `networkidle` never fires
  because it waits for a quiet network, not a deadline. A batch script
  driving many pages back-to-back (an 11-page x 5-width overflow matrix)
  looked like it was making progress — earlier `ok` lines kept appearing in
  the output — right up until it silently wedged on one page, with no error
  and no timeout to report. Two fixes, both worth doing together: pass an
  explicit `timeout` on every `goto` (Playwright's default is generous but
  finite; the real risk is a bare `networkidle` promise with no timeout
  argument at all in a batch loop), and for anything that doesn't need the
  fonts to actually render (an overflow/scrollWidth check, not a screenshot)
  `page.route('**://fonts.*/**', route => route.abort())` before `goto` and
  use `waitUntil: 'load'` instead — it removes the flaky external dependency
  entirely and the check runs in a fraction of the time. Keep `networkidle`
  (with a timeout) for visual screenshots where you want the real fonts
  loaded, and keep it consistent between a before/after pair — a pixel-diff
  comparing a real-fonts render against a fallback-fonts render reports
  every glyph as changed, which reads as a layout regression that isn't one.

## When a tracker's own gap is already covered by an independent safety net, "fix the gap" means "bound the retry, don't fabricate a decision" (issue #298 residuals, `unresolved_decided`)

`TxnTracker::unresolved_decided`'s own doc already documented that its
staleness is "deliberately approximate... still safe": a straggling
unresolved remote intent is resolved on demand the moment any reader hits
it (the foreign-intent read-path push, ADR 0018 §2/PR5 §3), independent of
this tracker or the background loop that walks it. `txn_resolver_loop`'s
sweep over `unresolved_decided()` had no fallback at all when its
`txn_record_view` lookup failed (e.g. the record's own tablet retiring
mid-recovery) — it just `continue`d forever, silently, with no bound.

The naive fix-by-analogy would have copied `pending_txns`'s sibling fallback
(`txn_recover`'s orphan path) verbatim: track first-seen, and past
`RECOVERY_GRACE`, *decide* something. That doesn't typecast here: the
orphan path exists because the coordinator's own verdict is genuinely
unknown, so "decide" means synthesizing a conservative abort. In
`unresolved_decided`, the verdict (`Committed`/`Aborted`) is **already
known** and carried right in the tuple — the only thing a failed lookup
withholds is `intent_spans`, the list of *which keys* to resolve. There is
no decision left to make; "decide-after-grace" would have to mean
fabricating a participant list out of nothing, which cannot be done safely.

The generalizable shape: **before extending a grace-then-act pattern to a
second call site, check whether the two sites are actually deciding the
same *kind* of thing** — one may be resolving genuine uncertainty (safe to
synthesize a conservative default) while the other is blocked purely on
*data availability* for an already-known fact (nothing safe to synthesize;
the only sound move is to stop claiming progress and let an independent
mechanism carry correctness). The actual fix here: keep retrying quietly
forever (a transient failure should still self-heal), but past grace, log +
meter *once* (a new counter, `CpTxnUnresolvedDecidedStuck`) that background
resolution has stopped making progress on this one entry, then keep
retrying without re-warning every tick — bounding operational noise and
giving observability, while leaning on the already-documented on-demand
safety net for correctness rather than duplicating it. Regression-tested
end to end by `cp_txn.rs`'s
`decided_but_unresolved_record_survives_its_own_tablet_splitting_before_resolve`
— which also surfaced, while designing it, that the *ordinary* version of
this shape (a decided record's key riding the normal split clone/trim path
onto a child) was already self-healing via `rebuild_txn_tracker`'s
group-start re-derivation from the child's own cloned engine state, so the
gap this fix closes is narrower than "any split during recovery" — it's
specifically "the lookup keeps failing for longer than a tick or two,"
which the fix bounds without needing to know why.

## A day-sized test-hook judgment call needs the SAME reasoning both times it's made, and a growing list of shapes it doesn't yet cover (issue #298 residuals, seal-boundary regression)

Two consecutive rounds of the same investigation independently reached the
same conclusion — a deterministic `ProdEnv` regression for the seal-boundary
overlap race (`seal_now`'s `metadata_fresh()` fix) needs new test-hook
plumbing (a `#[cfg(test)]` pause point mirroring `dynamo::
rmw285_confirm_gate`'s precedent, but on `animus-control`'s `DRIVER_APPLIED`
metadata-apply cache-refresh timing rather than a lock-scope boundary) and
judged it larger than a bounded pass both times. That repeated, independent
agreement is itself useful signal — worth recording explicitly rather than
re-deriving from scratch a third time — but it is not a substitute for
actually building the hook: **`animusd` has no `animus-sim` dependency, so
none of its race conditions are `SimEnv`-reachable**, which means every one
of them stays soak-detected-only (real timing, real flake rate) until
someone budgets the dedicated pass to build the missing test-hook seam. A
fifth, still-unconfirmed duplicate-delivery shape surfaced in this same
round's bounded 20-run soak (a within-one-already-sealed-shard duplicate
for one member of a transacted write pair) — extensive code-first
re-verification of `TxnResolve`'s per-key apply idempotency, its always-
fresh-`ts`-minting, and `trim_split_child`'s boundary math did not reach a
confirmed mechanism, so no speculative fix was attempted (per this repo's
own standing rule: fix only what's confirmed). The generalizable point:
when a bounded-round investigation defers the SAME piece of groundwork
twice in a row for the SAME stated reason, that is the signal to schedule
it as its own dedicated task rather than let a third round re-litigate the
same feasibility judgment — the growing list of shapes it would let get
regression-tested (now: the seal-boundary overlap AND this fifth shape) is
the accumulating cost of not doing so.

## A bounded retry against a live producer is a starvation flake by construction — snapshot the point-in-time state instead of retrying until it goes quiet (2026-08-26, `LsmEngine::clone_to`)

The issue #298 fix (immediately above the code this touches, ADR 0058 rung
2/Train 2) closed a correctness hole in `clone_to` — a single `flush()`
call could silently no-op while `applies_in_flight > 0`, permanently
dropping an acked-but-still-memtable-only row — by retrying `flush()` in a
loop (`CLONE_FLUSH_MAX_RETRIES = 1_000`, 1ms apart) until the memtable read
empty. That fix was correct about the *hole* but wrong about the *shape of
the fix*: "retry until X goes quiet" only terminates if X is guaranteed to
go quiet within the retry budget, and nothing here guaranteed that. A
*persistent* concurrent writer — exactly `lsm_clone_concurrent.rs`'s own
regression workload, and a real shape in production (a frozen split
parent's own frozen-tablet exemption still lets consumer-bookkeeping
writes land) — can refill the memtable faster than any bounded number of
flushes drains it, so the loop doesn't just get *slow* under contention, it
has no liveness guarantee **at all**: CI (PR #404, unrelated to this crate)
reported the identical code failing with "memtable still non-empty after
1000 flush retries" on a busier runner, while the same seed had passed
locally days earlier. This is the general shape of a *starvation flake*,
not an infra fluke, and the tell is structural, not statistical: a fixed
retry bound racing an **unbounded** producer will eventually lose on some
runner, no matter how generous the bound — raising it only moves the
flake's probability, never removes it (explicitly avoided here per the
task's own instruction, and worth calling out as a trap: "just retry more"
is the natural first fix to reach for and is never actually a fix for this
shape of race).

**The actual fix drops the goal of "reach quiescence" entirely.** `clone_to`
doesn't need the memtable to ever go empty — it needs to capture every
write ACKED before the call started, and a single point-in-time snapshot
already guarantees that for free: the write path (`log_and_apply`) only
returns to its caller after a record is both WAL-synced and applied to the
memtable **under the same lock** `clone_to` snapshots through, so any
write a caller has already observed as acked is provably present at the
next acquisition of that lock, no matter how the retry-vs-write race plays
out. One best-effort `flush()` (kept purely as an optimization — it
produces the pre-existing pure-SSTable-only clone shape when the source is
quiescent) followed by one atomic snapshot of `(manifest.tables,
memtable-contents)` is sufficient and involves no retrying, no sleeping,
and no dependency on the writer ever pausing. Whatever the snapshot finds
still resident in the memtable is written out as one new SSTable **inside
the clone's own namespace** (never the source's — the fix touches nothing
about the source's manifest/WAL/files beyond the pre-existing hard-link
scheme), sized to exactly that one point-in-time snapshot regardless of
how long or how fast the writer keeps going afterward. Bounded, one-shot
work replaces unbounded-in-principle retrying.

**General rule**: when a fix for "a producer can race my read of shared
state" takes the shape of "retry until the state stops changing," stop and
ask whether the actual correctness contract needs the state to stop
changing at all, or only needs a *consistent snapshot* of it. The retry
loop is usually solving the wrong problem — it's trying to wait out an
unbounded producer instead of taking a well-defined instant of that
producer's output, and any fixed bound put on such a loop is a deferred
flake, not a fix, discoverable only by asking "what happens if the
producer never stops" rather than "what happens in the common case." See
`crates/animus-storage/CLAUDE.md`'s `clone_to` entry for the as-built
mechanism and `crates/animus-storage/tests/lsm_clone_concurrent.rs`'s
`clone_to_completes_under_a_writer_that_never_pauses` for the liveness
regression (reproduces the CI failure directly against the old
bounded-retry code, real multi-thread `ProdEnv`, `SimEnv` cannot reach this
race — see that file's own module doc).

## Issue #298, a fifth shape: two candidate mechanisms found, neither confirmed as root cause (2026-08-26)

A fresh investigation (ADR 0058's G5 row, all four previously-fixed #298
mechanisms already on `main`) re-ran `streams_e2e.rs::multi_split_soak_
streamed_gsi_table_under_mixed_load` un-pinned from `SplitMode::Copy` to the
default (`InPlace`) 21 times. 4/21 (~19%) failed, in two distinct shapes —
consistent with the rung-4-layer-2 entry's own point that in-place's higher
splits-per-budget just gives a rare pre-existing race far more rolls of the
dice, not a new trigger condition.

**Shape A (2 runs) — the literal "shape 5" this investigation was scoped
to**: the delivered count came back *over*, not under, expected
(`delivered=146/144`), and one member of a transactional write pair
appeared **twice under the SAME shard (tablet+epoch)** with two distinct
sequence numbers — a genuine double-append into the hot change log before
either copy was ever sealed, not a seal-boundary race (which would show as
two *different* epochs, per mechanism (2)'s own already-fixed signature).
In one of the two runs, the OTHER member of the identical pair *did* show
that already-fixed cross-epoch signature in the same run — the clean
interpretation is one shared underlying event (the transaction's own
resolve running more than once) landing on two different tablets, one of
which happened to have a seal race between the two applies (cross-epoch)
and one of which didn't (same-epoch, hence "shape 5"'s literal signature) —
not two independent bugs coincidentally co-occurring.

**Shape B (2 runs) — found by the same soak, not previously documented
under #298**: `ClientCtx::txn_recover`'s in-doubt-recovery sweep decided
**Abort** (diagnostic: `all_staged=false`) for a transaction whose write the
test's own client had already recorded as acked (a `TransactWriteItems` 200
response) — permanently losing that item (one run failed the immediate
`ConsistentRead: true` read-back; a second, independent run failed the same
way further downstream, via the lineage-delivery deadline timing out one
record short). This is a live instance of the "duelling decider" hazard ADR
0018 §2/PR5's own amendment names as *legal* — but that legality rests on
both deciders reaching an objectively correct decision from independently
verified state, an assumption a false-negative verify breaks.

**Method**: `crates/animus-test/tests/stream_lineage_corpus.rs` (the
`ANIMUS_STREAM_SEEDS` corpus) was checked first, per this round's own
"SimEnv repro is worth more than a ProdEnv soak hit" instruction, and ruled
out as a repro vehicle for this specific bug — it drives the copy-based
split's lineage *purely at the `Metadata`/`MetaCommand::BeginSplit`/
`CutoverSplit` level* (`complete_copy_split`), never through `animusd`'s own
async orchestration (`cp_txn`, `txn_resolver_loop`, `resolve_all_parallel`'s
timeout) where this bug's candidate mechanisms live — and `animusd` itself
has no `animus-sim` dependency at all (already named as a gap in ADR 0058's
G5 row for mechanism (2)), so a deterministic seed-reproducible repro of
this specific race is not reachable without new cross-crate test
infrastructure, out of scope for this pass. The soak itself reproduced
fast: ~30s/run, first hit on run 5 of the first 7 unpinned attempts.
Temporary `eprintln!` instrumentation (propose/resolve/recovery tracing at
`cp_txn`'s participant-stage-error, abort-vs-actual-outcome, awaited-resolve
timeout, and `TxnStage`'s apply-time "would this re-stage land on an
already-Committed value" check; `txn_recover`'s own decide call) — modeled
on this file's own "land a permanent diagnostic before any fix attempt"
entry above, but kept local to the investigation and reverted before
committing, never shipped — captured shape B live (the exact `all_staged=
false`/`proposed=Aborted` sequence immediately preceding the lost-write
panic) across ~15 further runs, but never caught a live full causal chain
for shape A's own double-materialize, and the "re-stage over an
already-Committed value" diagnostic never fired in any captured run either
— so the candidate mechanism below for shape A is a **confirmed code gap
via reading**, not a confirmed root cause via a captured trace.

**Two structural gaps found, presented as candidates for the next
investigation, neither fixed this round (no speculative fix, per this
round's standing rule)**:

1. `KvCommand::TxnStage`'s apply arm (`crates/animus-cp-data/src/lib.rs`,
   the `blocked_by` computation) only rejects a stage that would overwrite a
   *different* transaction's currently-unresolved `Envelope::Intent` — it
   never checks whether the target key's current value is already
   `Envelope::Committed`. Same-txn re-staging is documented as deliberately
   unaffected ("a WAL-replay re-application"), but nothing distinguishes
   that from a genuinely late/duplicate `TxnStage` propose landing *after*
   its own transaction has already fully resolved: such a propose would
   silently resurrect the key from `Committed` back into `Intent`, and a
   later resolve (the normal flow, the resolver-loop safety net, or a
   recovery push) would then re-run `materialize_derived` a second time, at
   a fresh HLC — the literal "two distinct sequence ids for one write"
   signature. (The per-key resolve path itself, checked carefully during
   this investigation, *is* idempotent once a value is genuinely
   `Committed` — `TxnResolve`'s own apply only materializes when the target
   key currently holds `Envelope::Intent` naming that exact `txn_id`; the
   gap is specifically that `TxnStage` has no equivalent guard preventing a
   value from re-entering `Intent` state to begin with.)
2. `ClientCtx::txn_recover`'s `all_staged` loop (`crates/animusd/src/
   lib.rs`) folds `Ok(false)` (genuinely not staged) and `Err(_)` (the
   verify call itself failed — most commonly `txn_verify`'s own
   `"no CP group leader reachable"`/forwarding error, exactly what a
   participant's tablet mid-fork/cutover produces routinely) into the same
   `all_staged = false` bucket. A transient routing failure during exactly
   the high-split-cadence window this soak stresses gets treated identically
   to a permanent "never staged" fact, which can push recovery to Abort a
   transaction whose coordinator (`cp_txn`) is concurrently deciding — or
   has already decided — Commit from its own, unaffected view.

**General lesson**: an exactly-once investigation under a much-higher
split cadence should expect **more than one** symptom shape to fall out of
the same soak run, not just the one shape it was scoped to chase — shape B
here was found purely because the same instrumented soak was run enough
times to surface it, not because it was anticipated. Treat every distinct
panic/diagnostic signature a soak produces as its own data point even when
only one was the assignment, and say so explicitly in the writeup rather
than only reporting against the originally-named shape. Related to the
"an intermittent deficit... should get a permanent on-failure diagnostic
landed... before any fix attempt" entry above: here the diagnostic was
temporary and reverted (this round's own instruction), which is the right
call for an exploratory pass, but it means shape A's own root cause is
still only a well-argued candidate, not a captured fact — a durable,
committed diagnostic (gated by an env var or `#[cfg(test)]`, never firing
in production) is the natural next step before the next investigation round
attempts a fix.

## A post-propose confirm-poll must accept "the effect already fully happened" as success, not just "the intermediate state I expected" (ADR 0059 §3, Train 1 PR④, `DeleteBackup`)

`animusd::dynamo::delete_backup` proposed `MetaCommand::MarkBackupDeleted`
(the two-phase janitor's own mark step) and then polled
`metadata_fresh(ctx).await.backup(backup_arn)` waiting to observe
`BackupStatus::Expired` before building its response. On a lightly loaded
single-node test cluster this flaked intermittently: the backup janitor
(`animusd::backup_janitor`, a 200ms tick) sometimes observed the mark,
reclaimed every object, and proposed the finalizing `MetaCommand::
DeleteBackup` (removing the row entirely) **before** `delete_backup`'s own
next poll ever ran — so the poll found `meta.backup(backup_arn) == None`
forever after, never `Some(Expired)`, and spun until `SCHEMA_COMMIT_TIMEOUT`
(5s) before returning a bogus `InternalServerError` ("did not commit... no
leader reachable?") for an operation that had, in fact, already fully
succeeded moments earlier.

The general shape: **when a proposer's own confirm-poll checks for one
named intermediate state of a value that a second, independent, faster
process can advance PAST that state (here: past `Expired` all the way to
"row removed"), the poll must treat every state at-or-beyond the expected
one as success — not just an exact match.** Checking only for `Expired` was
implicitly assuming this caller would always be the fastest reader, which
held in every manual/low-concurrency test run but not under the specific
timing this bug needed (a fast local reclaim + a slightly-delayed next
poll). The fix: capture the row's own data **before** proposing (needed for
the response body regardless, since a client-visible `DescribeBackup`-shaped
reply must describe *some* row state), then treat `None` on the post-propose
read as an equally valid success signal — "gone" only ever follows a
successful mark in this design (nothing else removes a row), so it can never
be confused with "never happened."

This generalizes past this one call site: any commit-wait loop watching for
one named state on a value that a background convergent process can advance
past that state (a retention janitor, a compaction sweep, an aggregator)
needs to ask "has it been reached or superseded?", not "does the value
currently equal exactly this?" — the same class of bug as polling for
`status == Creating -> Active`
transitions while ignoring that a stuck-timeout path can skip straight to
`Failed`+reclaimed+gone without ever stopping at an intermediate value the
poll was watching for. Caught by a real flake under `cargo test`, not by
design review — the codebase's own "a flaky `ProdEnv` test is a real bug"
rule held exactly as advertised. See `crates/animusd/src/dynamo.rs`'s
`delete_backup` for the fix in place.

**The same PR shipped a sibling instance of this exact bug in its own
regression test**, not just the production code above:
`delete_backup_on_a_follower_is_relayed_to_the_leader`
(`crates/animusd/tests/schema_ddl_relay.rs`) issues `DeleteBackup` against a
follower, then polls every node's `Metadata::backup(&backup_arn)` waiting for
`BackupStatus::Expired` — the identical named-intermediate-state trap, one
level up: fixing the production confirm-poll to accept "gone" as success
didn't also fix a *test* that independently re-derived the same wrong check
against the same janitor race. It flaked ~50-55% under a repeated single-test
loop (`cargo test -p animusd --test schema_ddl_relay
delete_backup_on_a_follower -- --test-threads=1`, run 15-20x), always with
"backup not marked Expired within 20s" on whichever node the fast 200ms
janitor tick reclaimed-and-finalized first. Fix mirrors `delete_backup`'s own:
accept `Some(Expired) | None` as convergence, not `Some(Expired)` alone. The
lesson generalizes one more notch: **when fixing a "poll for an intermediate
state that can be skipped past" bug, grep for other pollers of the same
value** — a test asserting the same field is exactly as exposed to the race
as the production code was, and copying the assertion pattern (rather than
the fix) into a new test silently reintroduces the bug.

## A live-derived accessor scoped to "currently resolvable" state silently drops to a degenerate value once its inputs are torn down — freeze the figure while it's still derivable (ADR 0059 §3, Train 1 PR④, `BackupRow::total_bytes`)

`Metadata::backup_total_bytes` sums a backup's captured bytes via
`backup_manifest_tablet_progress`, which resolves each pinned tablet to its
**currently live** `split_lineage` descendant(s) (`live_split_descendants`)
— deliberately, so a tablet that split mid-capture is credited via whichever
descendant actually reported, never double-counted against a stale,
split-superseded ancestor's own orphan report. That "live" scoping is
exactly right while the source table still exists. The moment the whole
table is dropped, every one of the backup's tablets vanishes from
`Metadata::tablets` at once, `live_split_descendants` returns nothing for
every pinned tablet (its own fallback path only returns `[ancestor]` when
`self.tablets.contains_key(&ancestor)`), and `backup_total_bytes` silently
collapses to `0` — even though the catalog row and its per-tablet progress
records are untouched (ADR 0024's own explicit carve-out is working exactly
as designed for *those*). Nothing in `backup_total_bytes`'s own signature or
doc comment flags this: it reads as an ordinary accessor, and it answers
`0` — a legitimate-looking value, not an error or a panic — so a caller has
no signal that the answer just became meaningless.

This was caught by re-reading the accessor's own implementation against ADR
0059 §3's "a backup outlives its source table" promise while wiring
`DescribeBackup`'s size field, not by a failing test (none existed yet that
dropped a table between capture-complete and describe — added afterward,
`dynamo_backup.rs`'s `create_backup_round_trip_survives_table_drop_and_
janitor_reclaims`, asserting the reported size is byte-identical before and
after the drop). The fix generalizes: **any accessor whose answer depends on
resolving through currently-live state (a tablet map, a membership set, a
liveness check) must have its result frozen into durable storage at the one
point in the object's lifecycle where that resolution is still meaningful,
if any later consumer needs the answer to survive the inputs going away.**
Re-deriving on every read is only safe for the lifetime of whatever the
derivation depends on; past that lifetime it isn't "stale," it's simply
answering a different, degenerate question that happens to typecheck.
`BackupRow::total_bytes` is exactly this freeze, written once by
`MetaCommand::CompleteBackup`'s own apply arm at the last moment the live
accessor is still authoritative.

## A byte-passthrough merge primitive and a value-producing read primitive can disagree about who owns the envelope tag (ADR 0059 §7, Train 2 restore)

Building the restore driver (`animusd::backup_restore`), the first real
end-to-end test run panicked on an ordinary `ConsistentRead: true` `GetItem`
against a freshly-restored row: `txn: unknown envelope tag 123 (corrupt
engine value)`. Not a fault-injection scenario — every seed, every time,
on the very first row read back.

The root cause was two correct primitives disagreeing about a byte-format
contract neither one's own doc stated explicitly enough to catch by
inspection. `animus-cp-data`'s apply path wraps every ordinary write's
value in a 1-byte-tagged envelope (`0` = committed, `1` = intent) before
merging it into the engine — every read path unwraps this before a caller
ever sees a value. `KvCommand::SeedBatch` (the split-build driver's own
history-transfer command, reused verbatim by restore per ADR 0050) is
deliberately the *exception*: it merges the exact bytes handed to it,
envelope tag included, because a split child's rows are still-enveloped
physical bytes from the same live transaction blast radius as their
parent — copying them verbatim is what lets an in-flight intent continue
resolving correctly wherever it lands.

Backup capture (ADR 0059 §5) reads through intent resolution by design —
it deliberately stores each row's already-*resolved*, plain value, with no
envelope tag at all, specifically so a restored table never carries a
dangling, unresolvable intent envelope pointing at an anchor that may not
even exist anymore. That decision is exactly right on capture's own side.
It just means the restore driver's own input (a plain resolved value) and
`SeedBatch`'s own contract (an already-enveloped physical byte string) are
not the same shape — feeding one into the other merges a byte string whose
first byte the read path's decoder can't recognize as either envelope tag,
producing exactly the panic above the moment anything ever reads the row
back.

**The general form**: a merge primitive that is deliberately "verbatim
bytes in, verbatim bytes out" (no re-encoding, by design, for its own
documented reason) is not a safe target for a *different* producer whose
own output was already decoded/normalized one layer down from what that
primitive expects — even when both producers are "giving it a value" in
the loosest sense. The fix is never to make the merge primitive smarter
(that would break the property it exists for); it's to make the seam
between the two explicit: `animus_cp_data::backup::encode_restored_value`
is a one-line, clearly-doc'd wrapper the restore driver calls on every
captured value before it ever reaches `SeedBatch`, named after what it's
for rather than what it does, so a future caller reads its doc before
reusing the pattern instead of rediscovering the panic. Before wiring a
second producer into an existing "verbatim passthrough" primitive, check
what shape its *existing* callers actually hand it — "the same trait
method" is not "the same byte contract."

Caught by the project's own first real integration test for the feature,
not by review or a fault-injection sweep — a reminder that an end-to-end
test exercising the full production stack (not just the unit-level pieces)
remains the cheapest way to catch a cross-module contract mismatch that
both sides' own type signatures happily agree on.

## A newly-discovered pre-existing flake: `delete_backup_on_a_follower_is_relayed_to_the_leader` (~25-30% under real time, confirmed unrelated to ADR 0059 Train 2)

While running the full `animusd` gate for the Train 2 (restore) work, the
**pre-existing** Train 1 PR④ test `schema_ddl_relay::delete_backup_on_a_
follower_is_relayed_to_the_leader` failed once in a full-suite run
(`node 1: backup not marked Expired within 20s of follower-relayed
DeleteBackup`) and intermittently in isolated reruns on this branch. Before
assuming Train 2 had regressed it (a plausible worry — Train 2 spawns a new
per-data-node background loop, `backup_restore_loop`, into the same task
set every combined/data-only node already runs), it was reproduced on the
**pre-Train-2** tip (`claude/backup-wire-apis`, no restore code at all) via
a disposable `git worktree`: 2 failures in 6 isolated reruns there too, the
identical ~21.8-22.4s timing signature (just over the test's own 20s
budget). **Confirmed pre-existing, not a regression** — recorded here
rather than silently worked around, per this log's own standing rule that a
flaky `ProdEnv` test is a real bug, not noise, even when it isn't the one
you're currently touching.

The mechanism: the test's 20s budget covers a **two-hop convergent
process** — the relay itself (`ProposeSchema` one hop to the control
leader) *plus* the backup janitor's own two-phase reclaim (mark → object
delete → finalize), which polls on its own independent tick. On an
unloaded machine this converges in ~1.8s; under any real scheduling
pressure (a `--test-threads=1` run is still real OS thread/process
contention across whatever else is running, and CI runners are noisier
still) it occasionally needs more than one janitor tick's worth of slack
past 20s and the test times out outright rather than converging late. This
is the general shape the root `CLAUDE.md` warns about with "an eventual
property gets a converged-or-timeout poll, never a fixed-deadline
one-shot" — the poll here already isn't a one-shot, but its **budget**
was sized without accounting for the second independent convergent
process riding underneath the first one it was visibly testing.

**Not fixed in this change** (per the standing rule: an incidental
pre-existing bug found mid-task gets its own separate PR, never a
drive-by fix folded into an unrelated diff) — noted here so the next
person to see it red doesn't waste time bisecting a change that isn't the
cause; worth its own tracked issue alongside #406/#298's own flake family. A
real fix likely widens the timeout or asserts on the *janitor's own*
tick cadence rather than wall-clock margin.

## A shared test-harness "fresh engines map" must genuinely be fresh per tablet, not per scenario (ADR 0059 §9, Train 3 PITR corpus)

Building `pitr_fault_corpus.rs`'s split scenario (a parent tablet cutting
over to two children, each sealing its own PITR segment independently), the
very first run failed with every group's own decoded content showing
exactly double the records it should have. Root cause: the scenario created
**one** `engines()` map (`BTreeMap<NodeId, MemoryEngine>`) and passed it to
`start_group` for the parent AND both children — since `MemoryEngine::
clone()` is a cheap handle clone (shared underlying state, not a deep copy),
all three "sibling" tablets ended up sharing the identical physical engine
per node. A child's own `pending_changes()` scan then legitimately saw the
parent's pre-split records too (nothing in the harness partitions by
tablet — that separation is what a *real* per-tablet-private engine, ADR
0050 rung 1/2, provides in production, and what `StorageScope`'s declared
range narrows only the *logical* key space within, not the physical engine
instance). `stream_lineage_corpus.rs`'s own `copy_split_children_born_
empty` scenario already gets this right — three separate `engines()` calls
(`parent_engines`/`left_engines`/`right_engines`) — but this file's first
draft, written by close analogy rather than by copying that scenario's
exact structure line-for-line, missed it.

**The generalizable lesson**: when a sim-test harness models "sibling
tablets" (a split, or any other multi-group scenario), a fresh engines map
is required **per group**, not per scenario — reusing one `engines()` call
across more than one `start_group` call silently reintroduces exactly the
shared-physical-storage hazard the production tablet-privacy design (ADR
0050) exists to prevent, and the resulting corruption (double-counted
records, not a crash) is easy to misattribute to the mechanism actually
under test rather than the harness. When copying a multi-group scenario's
shape from a sibling corpus, copy the engine-provisioning lines exactly,
don't just replicate the general pattern from memory.

## `BeginBackup` is missing from `is_relayable_command` — a pre-existing Train 1 gap, found but out of scope (ADR 0059 §9, Train 3)

While auditing the relay allowlist for the new PITR `MetaCommand`s
(`UpdateContinuousBackups`/`SealPitrSegment`/`MarkBackupPitrBase`, all
added to `is_relayable_command`), a grep for every existing `MetaCommand::
BeginBackup` construction site turned up that `MetaCommand::BeginBackup`
itself is **not** on the allowlist, despite `dynamo.rs::create_backup`
calling `ctx.propose_schema(&MetaCommand::BeginBackup { .. })` — the exact
same "may run on any node, must relay to the control leader" shape every
other DDL-class command (`SetTableTtl`, `SetTableStream`, etc.) already has
an allowlist entry for. `ClientCtx::propose_schema` relays via
`ClientRequest::ProposeSchema` whenever the local node has no live control
leader handle of its own; the receiving node's `is_relayable_command` gate
would reject `BeginBackup` outright, and `create_backup`'s own commit-wait
loop would then exhaust `CREATE_BACKUP_ID_ATTEMPTS` retries (minting a
fresh id each time, since it can't distinguish "rejected" from "never
reached a leader") and finally return a timeout error — meaning
`CreateBackup` issued against a **follower-connected** node in a real
multi-node deployment likely fails today, every time, until a data-role
node happens to become the control leader.

Not fixed here: this is a genuine, real bug, but it predates this PR
(Train 1 PR④), is unrelated to PITR's own mechanism, and root `CLAUDE.md`'s
own engineering practices are explicit that "an incidental pre-existing bug
discovered during a task gets its own separate PR ... never a drive-by fix
folded into an unrelated diff." Recorded here so it isn't silently
rediscovered later: the fix is one line (`MetaCommand::BeginBackup { .. }`
added to `is_relayable_command`'s allowlist) plus a
`schema_ddl_relay.rs`-style regression test mirroring `update_time_to_live_
on_a_follower_is_relayed_to_the_leader`. **Generalizable lesson**: when a
new wire operation's `MetaCommand` gets a relay-allowlist entry, grep for
every *sibling* command proposed the same way (same wire-handler shape,
same `ctx.propose_schema` call) while you're in the allowlist anyway — a
gap next to the one you're fixing is exactly the kind of thing a narrowly-
scoped PR walks right past.

## A driver stamped a wall-clock field from the monotonic clock — the two `Env` time sources look interchangeable until a value crosses a wire boundary (ADR 0059 §9/§10, Train 3 PR②)

`pitr_seal_now`/`pitr_tick` (`animusd::index_drain`) stamped
`PitrSegmentRow::seal_wall_ms` from `ctx.env.now()` — `Nanos`,
monotonic-since-process-start, the ordinary timer/timeout/backoff seam —
instead of `ctx.env.wall_now()`, ADR 0051's one real-calendar-time seam.
Both compile, both return a plausible-looking integer, and every existing
test passed: nothing *inside* the sealing path ever compares
`seal_wall_ms` to a real timestamp, so the bug was invisible until a
consumer that does — `PitrSpec::enabled_wall_ms` (genuinely wall-clock,
set from a wire-facing `UpdateContinuousBackups` call) compared against
this field to derive `LatestRestorableDateTime` — silently collapsed the
whole PITR restore window to zero width, forever, the instant any tablet
ever sealed. It shipped in Train 3 PR① and sat undetected through that
PR's own full corpus and review, because PR①'s own scope never *read*
`seal_wall_ms` against a real timestamp; PR②, the first consumer to do so
in earnest, hit it on its very first end-to-end run.

**The generalizable rule**: when a field's own doc or name says "wall
clock" / "real time" / anything that will eventually be compared against a
value carried in from *outside* the simulation seam (a wire timestamp, an
external system's clock, ADR 0051's `wall_now()` in this codebase), audit
every site that *writes* it as carefully as the sites that read it —
`env.now()` and `env.wall_now()` return the same Rust type from the same
trait and both "just work" in isolation, so a writer-side mixup produces
no type error, no panic, and no test failure until something finally
diffs the written value against a genuinely wall-clock one. A codebase
with two clock seams needs a grep sweep of every write site for a
wall-clock-typed field whenever that field gains its first real consumer,
not just a review of the consumer's own new code.

## An accessor scoped to "currently live" is the wrong tool for "everything that ever happened," even when it's the closest existing primitive (ADR 0059 §10, Train 3 PR②)

`Metadata::pitr_replay_segments`'s first implementation was built on
`live_split_descendants` (ADR 0059 §6's own on-demand-capture
re-planning accessor: "given a pinned tablet, which of its *currently
live* descendants must report for the backup to be considered complete").
It looked like the obvious tool for "given a base snapshot's pinned
tablet, which tablets' segments must be replayed" — both questions start
from a pinned tablet and walk forward through splits — but the two
questions are not the same question: capture-completeness only cares
about tablets that still *exist*, while replay must cover every tablet
that *ever* held relevant data, including one retired by an ordinary
`DropTableTablets` (no split at all). `live_split_descendants` answers
empty for exactly that case (a dropped-not-split tablet has no
`split_lineage` entry), so replay silently produced zero segments for
any deleted table's own un-split tablet — caught by this PR's own first
end-to-end test (a deleted-table PITR restore), not by review, and fixed
by writing a direct forward DFS over `split_lineage` that includes every
*visited* tablet regardless of current liveness.

**The generalizable rule**: when a new need "starts from a pinned entity
and walks forward through the same lineage table" as an existing
accessor, don't reach for that accessor just because the traversal shape
matches — check what its own *filter* condition means, since a
completeness predicate ("must this thing still exist to matter") and a
coverage predicate ("did this thing ever hold matter") are easy to
conflate when they happen to agree on every input a first draft's tests
exercise (a table that's still alive, or was split rather than dropped).
Two nearly-identical unit-test names in this PR's own regression suite —
one proving the accessor is re-planned correctly onto live split
descendants, the other proving it still finds a dropped-never-split
tablet's own segments — is the shape that catches this: write the
"the entity is GONE, not just re-planned" case explicitly rather than
assuming a lineage-walking accessor's existing split coverage implies
drop coverage too.

## A corpus's own leader-kill-then-seal step needs a confirmed write between them, not just a re-election (ADR 0059 §9/§10, Train 3 PR② corpus)

The PITR restore corpus's flagship leader-kill scenario called
`pitr_seal_now` on the newly-elected leader immediately after killing the
old one, with no write in between. At `ANIMUS_PITR_SEEDS=100` this
produced a genuine (harness-only) failure: "the group has a leader" and
"the group's own apply cursor has caught up to everything the crashed
leader had committed" are two different facts, and nothing forces the
second to be true the instant the first becomes true — `pending_changes()`
read on the fresh leader before its apply loop caught up saw a truncated
backlog, corrupting the scenario's own expected seal content. The fix
was to reorder the scenario: move the leader kill to *before* that
round's own write burst rather than immediately before the seal, so the
burst's own confirm-by-applied-index wait (already present, since every
write in this corpus confirms before moving on) forces the catch-up as a
side effect, with no new synchronization primitive needed.

**The generalizable rule, restated for corpus/harness authors
specifically** (the underlying principle — durable-before-visible,
leadership isn't apply-completeness — is already a top-level house rule):
a hand-scripted scenario that kills a leader and then immediately reads
group-local state on the replacement must interpose a confirmed write
(or an explicit apply-catchup wait) between the kill and the read, exactly
as a real client would experience via its own confirm loop — "the group
elected a new leader" is not the same wait condition as "the group is
caught up," and a scenario that conflates them can manufacture a
test-only data loss that looks identical to a real one.

## A correctness fix that changes record SIZE broke a sibling test's hardcoded-epoch assumption, caught only by the final full-suite gate (ADR 0059 §9/§10, Train 3 PR②)

Fixing `table_change_records_carry_images` to include PITR (recorded above:
a PITR-only table used to get image-less markers, which PITR replay cannot
reconstruct content from) is unambiguously correct and necessary. It has a
side effect nothing in that fix's own review caught: every change record on
a PITR-enabled table is now a **full item image** instead of a tiny marker
— dramatically bigger. `index_drain.rs`'s own pre-existing
`pitr_seal_happy_path_and_disable_reenable_continues_epoch_chain` test (from
Train 3 PR①, unmodified by this PR) writes 10 padded items against a
`seal_bytes: 200` threshold and then hardcodes `pitr_segments[&(tablet, 1)]`
as "the seal that happens after re-enable." With markers, the whole 10-item
burst apparently stayed within one seal (epoch 0); with full images, the
same burst legitimately crosses the 200-byte threshold **three times**
before the test ever reaches its own disable call (epochs 0, 1, and 2 all
minted under generation 1, confirmed by instrumenting the test directly)
— so the test's own hardcoded `(tablet, 1)` lookup found a stale
pre-disable row instead of the new post-re-enable one, and asserted the
wrong generation. This was caught only by this task's own final full
`cargo test -p animusd --lib --tests -- --test-threads=1` gate — neither
`cargo test -p animusd --lib` scoped to the new code, nor this PR's own new
e2e suite, exercises that pre-existing test at all, and its silent
pre-existing passing state gave no signal that its own assumption had
become load-bearing on marker-sized records specifically.

**The fix**: make the test observe reality instead of asserting a specific
epoch number — capture "whatever the chain's own current tip epoch is"
immediately before the disable call, then wait for and assert against
`tip + 1` afterward, exactly mirroring what the analogous stream test
(`disable_final_seal_then_reenable_continues_the_epoch_chain`) already does
by using a deliberately *huge* `seal_bytes` so its own burst never
size-triggers on its own — a pattern worth copying directly rather than
reinventing when writing a new disable/re-enable epoch-continuity test.

**The generalizable rule**: a test that hardcodes a specific epoch/index/
sequence number as "the Nth thing to happen" is implicitly asserting a
fixed number of trigger firings from a fixed byte budget — a completely
independent, unrelated correctness fix that changes per-record *size*
(image vs. marker, a longer key, an added field) can silently invalidate
that count without changing anything the two features would ever be
reviewed together for. Prefer asserting against a *chain's own observed
tip* (or an explicitly huge/disabled trigger threshold, forcing the seal to
happen only at the deliberate point the test controls) over a literal
epoch/sequence number whenever the trigger is size- or count-based rather
than a single explicit action. And: run the full existing test suite (not
just new/directly-touched tests) before considering ANY change to a shared
gating predicate (`table_change_records_carry_images` here) complete — a
predicate widening is exactly the shape of change whose blast radius is
everything downstream of its own `true` branch, not just the feature that
motivated it.
### Amendment (2026-08-26, later the same day): shape B confirmed and fixed; a second, sibling conflation found and fixed alongside it

A follow-up round re-instrumented both candidate sites from the entry above
(temporary `eprintln!`s, reverted before this amendment's own commit) and
re-ran the un-pinned `SplitMode::InPlace` soak. **Shape B fired and was
captured live within the first ~12 runs**: the exact predicted sequence —
`ClientCtx::txn_verify` returning `Err` for a participant span, folded into
`all_staged = false`, immediately followed by `txn_recover` proposing
`Aborted` and the test's own "acked write lost" panic on the identical item.
Confirmed mechanism: `txn_recover`'s `all_staged` loop (`crates/animusd/src/
lib.rs`) treated `Ok(false)` (genuinely not staged) and `Err(_)` (the verify
query itself failed, most commonly a transient routing hiccup while a
participant's tablet is mid-fork/cutover) identically. **Fixed** by
separating them: any `Err` now makes the whole push **inconclusive** —
`txn_recover` declines (returns `Pending`, proposes nothing) instead of ever
letting an unconfirmed span feed a decision. A new `txn_resolver_loop`-local
grace tracker (mirroring `unresolved_decided`'s own lookup-failure tracker,
issue #298 residuals commit) logs+meters once (`Metric::
CpTxnRecoveryStuckInconclusive`) if a transaction stays inconclusive well
past `RECOVERY_GRACE`, purely a liveness signal — correctness never depends
on it firing.

**A sibling conflation, found while chasing a SECOND "acked write lost"
recurrence after the first fix landed**: `RaftKvNode::txn_record_view`
(`animus-cp-data`) — the primitive `txn_recover`'s *orphan-record* branch
reads to decide "does no record exist at all, past grace, so an abort
tombstone is safe" — had the exact same shape of bug, one level up: its
plain `Option` return conflated "not served" (this replica's own read
barrier failed, e.g. mid-fork/cutover) with "genuinely no record" into the
same bare `None`. `ClientCtx::txn_record_view`'s wrapper turned *any* `None`
into `Err`, and `txn_recover`'s orphan branch treated *any* `Err` as
license to proceed toward an orphan-abort decision once past grace — so a
transient barrier failure on this ONE read could synthesize an abort
tombstone for a transaction whose record was fine (staged, or already
committing) and simply unreachable by this one query attempt. This is the
**general lesson generalized**: an "an `Err`/`None` from a query is UNKNOWN,
never evidence of absence" audit must cover *every* query a decision is
built on, not just the first one found — a fix scoped to the `intent_spans`
verify loop alone left an identically-shaped gap one level up in the same
function. **Fixed** by widening `RaftKvNode::txn_record_view` to the
existing `stale_get_served`/`linearizable_get_served` "served" discipline
this crate already uses elsewhere: `Option<Option<TxnRecordView>>` — outer
`None` = not served (decline), `Some(None)` = definitively no record
(the ONLY value that may feed an orphan-abort decision), `Some(Some(view))`
= found. Propagated through `ClientCtx::txn_record_view` (now `Result<Option
<TxnRecordView>, String>`, not `Result<TxnRecordView, String>`) and the
`ClientRequest::TxnRecordView`/`ClientResponse::TxnRecordViewReply` wire
pair (`view: Option<TxnRecordView>`, `TxnRecordView` gaining `Serialize`/
`Deserialize`). Every caller that only ever collapsed both `Option` layers
for a best-effort read (the `/admin/txns` diagnostic view, `animus-cli`'s
raw-reply printer) keeps doing exactly that — `.flatten()` is correct there,
since nothing downstream makes a safety decision off the distinction; only
`txn_recover`'s own two call sites needed to keep the layers apart.

**Method note**: a captured live trace matching a predicted symptom exactly
(diagnostic fires, panic follows immediately, same item) is strong enough
confirmation to fix from directly — no need to *also* chase a fully
deterministic `SimEnv` repro when the mechanism is this legible from one
clean trace, especially given `animusd` has no `animus-sim` dependency at
all (a standing gap this round didn't attempt to close). The nearest thing
to a deterministic regression is `animus-cp-data/tests/
txn_record_view_served.rs`, proving the FIXED primitive's own "served"
contract directly (a genuinely absent key answers `Some(None)`; a deposed/
partitioned leader's own barrier failure answers the outer `None`; a real
staged record answers `Some(Some(view))`) — and `animus-test/tests/
txn_serializable.rs`'s own `push`/`resolver_tick` (a SimEnv-based
reimplementation of this exact recovery protocol, ADR 0018 §4 PR6) was
independently carrying the identical two conflations in its own test-double
logic; both were fixed to match, closing the gap for that corpus's own
future fault-injection scenarios too, even though today's frozen scenario
set never happened to trip either one.

### Amendment (2026-08-26, later still): shape A's literal double-delivery confirmed live — but its real trigger is a THIRD, deeper mechanism than either candidate named

The same re-instrumented soak that caught shape B (above) also caught
shape A's own literal signature directly: `delivered=146/144`, with
`x0079a` appearing **twice under the identical shard** (tablet 67, epoch
0, two distinct sequence numbers) and its transactional sibling `x0079b`
appearing twice **cross-epoch** (the already-fixed mechanism-2 pattern) —
exactly the "one shared underlying event manifesting two ways depending on
whether a seal happened to land between the two applies" shape the
original investigation predicted. The `TxnStage`-over-`Committed`
diagnostic fired for both keys at the moment of the resurrecting stage,
confirming the structural gap named in the base entry above is real and
reachable.

**But tracing the captured `txn_id`s through the trace showed the live
mechanism is narrower AND deeper than either original candidate**: the
*resurrecting* stage used a genuinely fresh `txn_id`, distinct from
whatever transaction first wrote `x0079a`/`x0079b` (never captured, since
the diagnostic only fires on the resurrecting attempt) — this is not "the
same transaction re-staging its own already-resolved key" (the shape the
`blocked_by` gap's literal wording suggested), it is **two independent
transactions, each individually legitimate from `KvCommand::TxnStage`'s
own narrow point of view, racing to write the identical logical item**.
The only way that happens for ids the workload never reuses is a
**client-level retry**: `dynamo_retrying_transact`'s own retry loop
resubmits `TransactWriteItems` (an un-tokened call, exactly DynamoDB's own
documented duplicate-execution risk) whenever the response is a 500, or a
400 whose message carries the house `"; retry"` convention — and
`cp_txn`/`txn_prepare_pushing`'s own error messages for a **confirmation
loss** (`"CP group leader moved during participant/anchor stage/commit;
retry"`, minted whenever `wait_stage_outcome`/the leader handle races a
leadership change mid-poll) carry that exact suffix. Unlike a
`StageOutcome::Fenced` outcome — provably a no-op, since the apply-time
gate rejected it before anything landed — a confirmation loss proves
**nothing** about whether the underlying stage/commit actually applied; it
only means *this* call couldn't learn the answer. A retry that mints a
fresh `txn_id` after a confirmation-lost first attempt that in fact
succeeded races its own already-committed work, and `KvCommand::
TxnStage`'s missing already-Committed check (the base entry's own finding)
is exactly what lets the retry silently win instead of being rejected.

**This is the general lesson from the shape B amendment above, at a third
layer**: "an unconfirmed outcome is UNKNOWN, never evidence of a specific
result" needed applying not just to `txn_recover`'s own two queries, but
to the **coordinator's own error-reporting convention** — marking a
confirmation-loss message retryable with the same blanket `"; retry"` tag
a provably-safe `Fenced` refusal uses conflates "retry costs nothing" with
"retry might race your own prior success." Not fixed this round (a
coordinator-side fix — verify-before-erroring on a lost confirmation,
mirroring the self-verification `txn_prepare_pushing` already does for the
*known* `IntentBlocked` case — is a genuinely new, substantial mechanism,
out of scope for a same-round stacked fix per this repo's own "an
incidental bug gets its own PR" convention); see ADR 0018's shape B
amendment §6 for the pointer carried forward.

**What WAS fixed this round, on its own merits, regardless of not being
the live trigger**: `KvCommand::TxnStage`'s apply arm now rejects a stage
targeting a key that THIS EXACT `txn_id` already resolved on this group
(`TxnTracker::recently_resolved`, a bounded best-effort seatbelt, checked
by `(key, txn_id)` identity — the same "never trust an outcome without
confirming it names the SAME thing" discipline the `KindBatchOutcome`
false-ack fix established). This closes the narrower same-txn resurrection
the original candidate mechanism's wording named (a genuine gap, still
worth closing even though it wasn't what fired in the captured trace) and
is red/green proven directly:
`animus-cp-data`'s in-crate `pr5_orphan_and_resurrection_tests::
a_resolved_key_rejects_a_same_txn_restage_issue_298_shape_a`. **It does
not close the live trigger** — a genuinely different, fresh `txn_id`
staging over an already-`Committed` value from an unrelated (or, as here,
duplicate-client-retried) transaction is the ordinary, correct write path
and must keep succeeding; only same-identity resurrection is rejected.

**Method note, reinforcing the base entry's own lesson**: tracing a
captured diagnostic's own identity fields (here, `txn_id`) all the way
through — not just confirming the diagnostic fired at the predicted
symptom — is what separated "the named candidate mechanism is confirmed"
from "a structurally real but not-the-live-cause gap, with the ACTUAL live
cause one layer further out." A diagnostic firing at the right place and
the right time is necessary but not sufficient evidence that the
candidate mechanism it was built to catch is the one actually operating.

### Amendment (2026-08-26, the proof soak itself): both fixes hold, but a fourth residual keeps the pin in place — soak NOT un-pinned

With both fixes above landed, the mandated 30-run un-pinned `SplitMode::
InPlace` proof soak was run in several batches (some contaminated by
concurrent `cargo build`/`cargo test` invocations on the same host, which
measurably inflated the failure rate — a real methodological trap worth
naming on its own: **a real-thread `ProdEnv` soak's failure rate is not
trustworthy while anything else on the host is competing for CPU**; the
election timers, `RECOVERY_GRACE`, and split-cadence knobs this soak
depends on are all wall-clock-relative, so host contention can manufacture
timing-sensitive failures indistinguishable from real ones without a
controlled re-run). Under genuinely contention-free conditions across
~70 total runs: the two fixed mechanisms (shape A's literal resurrection,
shape B's wrong-abort-from-unconfirmed-query) **did not recur even once**
with instrumentation re-armed and watching for them directly. But the
soak is still not clean — three residual failure categories remain, at a
combined rate noticeably lower than the pre-fix ~19% baseline but not
zero:

1. **The already-documented "deep shape A" mechanism** (this entry's own
   prior amendment): a client-level retry of an un-tokened
   `TransactWriteItems` racing its own already-committed first attempt.
   Not fixed this round, named there.
2. **The pre-existing lineage-delivery-timeout residual** (ADR 0058's G5
   row, present since before this round): `drain_all_tablets_lineage`
   hits its own deadline one or a few records short, distinct from the
   exactly-once assertion itself — a slow convergence, not a lost or
   duplicated write. Unrelated to shapes A/B; already an acknowledged open
   item this round did not scope in.
3. **A newly-observed, NOT-yet-root-caused variant, caught live with the
   fix's own instrumentation still watching**: `txn_recover`'s
   `all_staged` loop computing a **genuine, non-inconclusive**
   `all_staged=false` (every `txn_verify` call returned an affirmative
   `Ok`, at least one `Ok(false)` — the exact case this round's fix
   correctly leaves alone, since a confirmed negative is real evidence)
   for a transactional pair, immediately coincident with (in one capture)
   an "acked write lost" panic on the same item, and (in a separate,
   otherwise-passing run) with no visible ill effect. Both captures share
   an unexplained structural oddity worth flagging for whoever picks this
   up next: `view.intent_spans` held **two** entries covering **both**
   members of what the workload only ever issues as a plain two-item
   `TransactWriteItems` (one anchor + one participant, per `cp_txn`'s own
   construction — `participant_spans` is built strictly from `groups`
   *after* the anchor's own group is removed) — meaning the record's own
   anchor was apparently a **third**, distinct key neither of the two
   panicking/logged item ids, which this round could not identify before
   time ran out. Plausibly the same "deep shape A" client-retry family
   (a correctly-aborted abandoned first attempt, with the eventual
   "acked write lost" panic actually caused by a *different*, successful
   retry attempt racing it) rather than a fourth independent mechanism —
   but this is a hypothesis, not a confirmed account, and is recorded here
   precisely so the next investigation starts from the raw captured shape
   instead of re-deriving it. Two raw captures (redacted only for length):
   `all_staged=false`, `intent_spans` = point-spans for both `x0069a` and
   `x0069b` immediately before the soak's own "acked write x0069a lost"
   panic; and the structurally identical shape for `x0029a`/`x0029b` in an
   otherwise-clean run.

**Per this round's own instructions, none of these three residuals is
fixed here, and the soak stays pinned to `SplitMode::Copy`** — un-pinning
requires 30 *clean* runs, and none of the three most recent honest
attempts (uncontaminated by host contention) reached that bar. Rung 4's
remaining copy-workflow-deletion layer stays blocked on all three,
exactly as it was blocked on shape A/B before this round root-caused two
of what turned out to be (at least) five distinct mechanisms sharing the
same soak.

## A durable idempotency cache needs its OWN confirmed-vs-unconfirmed audit — the recovery path that reads state isn't the only place that writes it (issue #298 "deep shape A", closed 2026-08-27)

This round closed the one residual named — and deliberately left open — by
this file's own two amendments above: **residual 1, a client-level retry of
an un-tokened `TransactWriteItems` racing its own already-committed first
attempt.** See ADR 0018's 2026-08-27 amendment for the full account; this
entry is the generalizable lesson.

**The bug, once found, was almost embarrassingly close to the two this file
already fixed twice over**: `dynamo.rs::run_transact`'s `ClientRequestToken`
idempotency preflight (already shipped, 2026-08-24 amendment — a durable
`token → outcome` record, conditionally claimed so the transaction executes
at most once per token regardless of bookkeeping) recorded **every** `cp_txn`
failure as a confirmed `CANCELLED`, including a genuinely ambiguous one (a
leader move mid stage, no leader reachable at all, or a `StageOutcome::
Fenced` naming a concurrent in-doubt-recovery decision) where the
transaction might have committed via a path that exact call never observed.
This is the identical "an unconfirmed `Err` is UNKNOWN, never evidence of a
specific outcome" defect this file's two amendments above already fixed in
`RaftKvNode::txn_recover`'s `all_staged` loop and, one level deeper, in
`RaftKvNode::txn_record_view` — found a **third** time in a completely
different function, written by a different amendment, three days apart.

**The generalizable lesson**: when a system gains a durable idempotency/
outcome cache sitting in front of an existing fallible operation, the cache's
own write path needs the identical confirmed-vs-unconfirmed audit the
*read*/*recovery* side already got — auditing `txn_recover`'s two queries
alone (as both prior amendments did, carefully) left an identically-shaped
gap sitting in the ONE OTHER function in the codebase that also decides
"did this transaction commit or not" from a fallible call's result, because
it was added later, for an unrelated feature, and nobody re-ran the same
audit against it. **A search for this defect class should grep for every
site that classifies a `Result`/`Option` from a distributed call into a
committed/aborted decision, not just the ones a previous investigation
already found** — "already fixed this bug class" is not the same claim as
"already fixed every site the bug class reaches," and a feature shipped
between two rounds of the same investigation is exactly the kind of site an
audit scoped to "the functions this investigation started with" will miss.

**The fix generalizes too**: reuse the SAME retryability convention the rest
of the codebase already carries (the `"; retry"` message suffix,
`Self::read_should_retry`'s own shape) rather than inventing a second
ambiguity taxonomy — `TxnAbortReason::is_ambiguous` is a one-line
`.ends_with("; retry")` check, and auditing every `TxnAbortReason::Other`
construction site against it found exactly one real gap (`CpRoute::None`'s
message was missing the suffix) rather than needing a new mechanism. Where a
false-negative "definitely didn't happen" is the failure mode (not a false
"definitely succeeded" — the two are not symmetric; only the negative
direction here can cause a client to safely-in-appearance retry into a
double-execution class of bug), the safe default on "I genuinely don't know"
is to retry the underlying operation a bounded number of times first (a
fresh attempt after a transient blip usually just works — bounded internal
retry absorbed the overwhelming common case here, mirroring
`txn_prepare_pushing`'s own `IntentBlocked` retry one layer down), and if
still unconfirmed, leave the cache exactly as ambiguous as it already was
(`PENDING`, not a fabricated `CANCELLED`) rather than manufacturing a
confident wrong answer — the identical "bound the retry, don't fabricate a
decision" rule this file's own `unresolved_decided` entry already states for
the sibling recovery-side case, now confirmed to generalize to a cache
sitting in front of the same underlying operation, not just to the
operation's own recovery path.

**Verifying a soak failure under host contention, reinforced**: the mandated
30-run un-pinned `SplitMode::InPlace` proof-soak batches for this round (see
below) hit the exact contention trap this file already names — a
`cargo test --workspace` run surfaced one `multi_split_soak_streamed_gsi_
table_under_mixed_load` failure (`drain_all_tablets_lineage` one record
short of 144, the already-documented lineage-delivery-timeout residual, ADR
0058's G5 row), which reran clean in isolation on the first attempt. Treated
as contention noise, not counted against the soak, per this file's own
standing instruction — restated here only because it is the mechanism this
round's own dedicated 30-run batches (isolated, one at a time) were run to
avoid in the first place.

## A safety-critical retry classification must be an allowlist of proven-safe cases, never a denylist of known-dangerous ones (issue #298 "deep shape A", same round)

The fix above went through three shapes before it was safe, and the
difference between the second and third is the more important lesson of
the two. **First shape**: retry every ambiguous `cp_txn` outcome
unconditionally. Reproduced the target bug immediately (a fresh `TxnId`
racing an original that had, in fact, already fully committed via a path
the retrying call never observed) — expected, this shape was never meant to
ship. **Second shape**: retry every ambiguous outcome EXCEPT a small,
explicitly-named denylist of the two message shapes then believed to be
the only genuine confirmation losses (`"CP group leader moved during
{anchor,participant} stage"`). This shape looked principled — it was
built by tracing the actual apply-time code paths, not guessed — and it
still reproduced the identical `delivered=146/144` duplicate-pair
signature on essentially the next soak run. **The denylist had missed a
whole call-site FAMILY**: `resolve_all`'s own DECIDE-phase confirmation-
loss messages ("CP group leader moved during anchor commit/abort", "after
decide", "during orphan abort", and their forwarded-hop twins) were never
enumerated, so by construction of "retry unless denylisted" they fell
through to "safe to retry" — and a confirmed DECIDE, unlike a confirmed
STAGE, fully materializes every participant's derived writes, so retrying
one with a fresh identity is exactly the race being closed. **Third shape,
the one that shipped**: invert the predicate into an ALLOWLIST — name only
the reasons proven safe (occurring before any propose for this transaction
could have applied at all), and let everything else, known or not-yet-
discovered, default to the conservative, never-retried path.

**Why this generalizes beyond this one bug**: a denylist and an allowlist
look symmetric in code (`if excluded { deny } else { allow }` vs. `if
included { allow } else { deny }`) but are not remotely symmetric in
failure mode. A denylist's blind spot is silent and unbounded — any call
site this file's author didn't happen to trace, present today or added by
someone else next month, is automatically "safe" the moment it starts
emitting the same `"; retry"` convention every other transient error here
already uses, with no compiler warning and no test failure until a soak
happens to exercise it. An allowlist's blind spot is loud and bounded — an
unrecognized reason degrades to the SAFE default (here, "leave the
idempotency record `PENDING`, self-heal via TTL"), which can cost latency
but never correctness, and is trivially auditable by reading one function
top to bottom rather than having to prove a negative across the whole
codebase. **The general rule: when classifying inputs for a safety-
relevant decision (retry-or-not, trust-or-not, allow-or-not) where the two
outcomes have asymmetric cost — one side is merely slow, the other side is
silently wrong — enumerate the cheap, provably-safe side and default
everything else to the expensive-but-safe side, never the reverse.** This
is the identical shape as `Self::is_confirmation_loss`'s own doomed first
draft mirrors: a denylist of "known-dangerous" strings is a poor substitute
for a real characterization of *why* a case is safe, since a denylist can
only ever encode what's already been found, and one soak run is all it
took to find the gap here — see `TxnAbortReason::is_safe_to_retry_fresh`
(`crates/animusd/src/lib.rs`) for the shipped allowlist and its own unit
test for the denylist's exact counterexamples now pinned as regressions.

**Soak result with the shipped (allowlist) design: 24/25 clean, one residual
found, soak stays pinned to `Copy`.** Two isolated, contention-free batches
(15 runs, then a further 10 chasing the one failure with a targeted
diagnostic that did not happen to recur) totalled 24 clean runs and one
failure — none of the earlier double-materialize signature, and none of the
old lineage-delivery-timeout residual either. The one failure was a
**genuine, non-ambiguous** `TransactionCanceledException` ("cached
cancelled outcome for this ClientRequestToken") on a transaction the soak's
own writer never conditions and never reuses a key for — meaning either a
real `ConditionFailed` (impossible by construction here) or a real
`TransactionConflict` (`IntentBlocked` exhausted). **Leading hypothesis, not
confirmed**: `is_safe_to_retry_fresh`'s own allowlisted retries mint a
FRESH `TxnId` per attempt; if one such attempt's `Fenced`/frozen-refusal
classification is a false negative for one specific replica's own read
(i.e. the stage in fact partially applied on some participant despite the
coordinator seeing a provably-safe-shaped refusal), a SUBSEQUENT retry's
own fresh `TxnId` could hit that first attempt's still-unresolved intent on
the same key and exhaust `TXN_STAGE_PUSH_ATTEMPTS`' worth of
`IntentBlocked` pushes before `txn_resolver_loop`'s passive sweep ever gets
a chance to clear it (`TXN_STAGE_PUSH_ATTEMPTS`/`TXN_STAGE_PUSH_BACKOFF`'s
combined budget, 3 × 250ms, is far shorter than `RECOVERY_GRACE`'s 5s) —
a genuine `TransactionConflict`, correctly definite from `cp_txn`'s own
point of view, but a self-inflicted one this feature's own retry loop
created rather than a pre-existing race. **Not confirmed live** — this
round's time ran out before a diagnostic could capture the exact
`TxnId`/key of a reproduced instance — recorded here, per this file's own
standing "capture the raw shape, don't re-derive it" instruction, so the
next investigation starts from a real hypothesis instead of from zero. If
confirmed, the fix is likely narrow: either shorten
`is_safe_to_retry_fresh`'s own backoff so a fresh retry only fires after
giving the FIRST attempt's own possible partial effects a chance to
resolve, or have `txn_prepare_pushing`'s `IntentBlocked` handling
recognize "blocked by a `TxnId` this same coordinator minted moments ago
for the identical logical request" as a signal to wait longer rather than
exhausting the ordinary cross-transaction contention budget. **Genuinely
correctness-safe either way**: a real `TransactionConflict` is a definite,
non-ambiguous abort — `run_transact` correctly records `CANCELLED` for it
and never risks a double-materialize; the residual is a spurious-failure/
liveness cost, not a data-safety one, unlike the bug this round's own fix
closed.

**Un-pin decision: NOT taken.** Per this task's own standing instruction, a
single failure in the mandated clean-run bar means the `SplitMode::InPlace`
soak stays pinned to `Copy` — `start_streamed_cluster_full_copy_pinned`'s
pin is unchanged from `main`. Rung 4's remaining copy-workflow-deletion
layer stays blocked on this one residual, down from the three this round
began with.

## A resolve's silent no-op is invisible to its own proposer — a write-side sibling of the read path's foreign-intent push, plus two bugs found auditing around it (issue #298, confirmed and closed 2026-08-27)

This round confirmed live the one residual the amendment above left open:
a fresh-`TxnId` retry (`TxnAbortReason::is_safe_to_retry_fresh`) hitting
`StageOutcome::IntentBlocked` on **its own coordinator's immediately-prior
attempt's** still-live intent. See ADR 0018's matching 2026-08-27 amendment
for the full account; this entry is the generalizable lessons.

**The confirmed mechanism, and the general shape it is an instance of**:
`ClientCtx::txn_resolve_participant`'s `CpRoute::Local` branch called
`leader.txn_resolve(..).await` and discarded its own `Option<HlcTimestamp>`
return, unconditionally reporting success. The underlying `KvCommand::
TxnResolve` entry had, in fact, silently no-op'd — its own `fence` check
rejected it because the target tablet's range had shifted (a concurrent
split) between the coordinator's `cp_route` lookup and the entry's actual
apply. **`RaftKvNode::txn_resolve`'s only signal is "did this entry apply,"
never "did it resolve anything"** — a fence-miss no-op and a genuine
resolve are indistinguishable to the caller, because `TxnResolve`, unlike
`TxnStage`, never got a `StageOutcome`-shaped per-attempt outcome channel.
**The general lesson: when one command in a family gets a "why did this
no-op" outcome channel because a bug demanded it (`TxnStage`'s
`StageOutcome`), audit every SIBLING command that can ALSO silently no-op
via the identical gate (a fence check, a seal, an OCC condition) for the
same missing channel — the fix that closed the first one does not
generalize automatically just because the mechanism looks similar.** This
gap is named, not fixed, in the ADR amendment (§3) — the fix that shipped
closes the one reachable symptom (a blocked stage) without adding the
larger channel, and a second, more severe consequence of the same gap
(§4 below) was found but NOT closed this round.

**The fix, and why it targets the symptom rather than the root gap**:
`StageOutcome::IntentBlocked` gained the blocker's own `record_key`/
`record_table` (already carried by every `Envelope::Intent`, exactly what
the READ path's `IntentInfo` already exposes for the identical reason) so
`ClientCtx::push_resolution_if_decided` — the write-side sibling of the
read path's `confirm_or_push`/`resolve_intent_given_status` — can query the
blocker's own decision and, if already `Committed`/`Aborted`, push its
resolution with FRESH routing (sidestepping the stale-fence race the
original resolve hit) before retrying the stage. This closes the reachable
symptom without touching `TxnResolve`'s apply arm at all — a smaller,
lower-risk change than adding an outcome channel, and correct regardless of
whether the root gap is ever closed.

**Two more real bugs found auditing the surrounding code the same way,
worth their own note**:

1. **A wrapping `format!` around an already-classified message is itself a
   classification bug.** `txn_prepare_pushing`'s exhaustion arm nested a
   retryable-shaped `Other` message (already proven safe to retry — it
   never even reached its own propose) inside new sentence text ending in
   `")"`. This silently moved the house `"; retry"` suffix `is_ambiguous`
   keys on to *before* the closing paren, and moved the message out from
   under `is_safe_to_retry_fresh`'s own `starts_with(...)` check — a
   message that was safe to retry a moment earlier was recorded a
   **definite** `CANCELLED` purely because of how the exhaustion text
   happened to be assembled. **The general rule this reinforces (already
   stated for `TxnAbortReason::Other` construction in this file's sibling
   entries, now shown to apply to any RE-wrapping of one too): any code
   that builds a new message around an existing classified one must
   preserve the classification, not just the informative content — the
   safest way to guarantee that is to not wrap it at all** (the fix here:
   pass the message through byte-for-byte, `Err(TxnAbortReason::
   Other(msg))`, rather than re-deriving a "did this preserve the suffix"
   invariant by hand every time the wrapper's own prose changes).
2. **An allowlisted-safe reason is not safe at every call site it can fire
   from.** `TxnAbortReason::is_safe_to_retry_fresh`'s own safety argument
   ("an anchor staged without every participant confirming can only ever
   be recovered as Abort") is true for a STAGE-time freeze but silently
   false for the IDENTICAL `FROZEN_REFUSAL` string reached from `cp_txn`'s
   own DECIDE step, once every participant has already staged — the
   premise "not every participant staged" doesn't hold there at all,
   because by construction of reaching that branch every one of them did.
   Retrying with a fresh `TxnId` there raced `txn_recover`'s own legitimate
   `all_staged`-driven commit for the ORIGINAL `txn_id`, reproducing the
   double-materialize signature this whole file's own prior entries already
   closed once. **The general lesson: a message-string-keyed allowlist
   predicate's safety argument is a claim about the CALL SITE the reasoning
   was built from, not about the string itself — the same string reached
   from a different call site can carry a different, unstated precondition.
   Audit every call site a shared error constant (`FROZEN_REFUSAL` here)
   can actually be reached from, not just the one the original bug report
   named.** Fixed by retrying the SAME decision (never a fresh `TxnId`) —
   `ClientCtx::txn_decide_anchor_retrying`, mirroring `cp_kind_write_item`'s
   issue #288 freeze-refusal retry shape — since retrying an idempotent
   decide can never abandon already-staged work the way a fresh identity
   would.

**A background passive safety net can rescue a broken regression test just
as easily as it rescues production traffic — and that is a test bug, not a
feature.** The first version of this round's own regression test called
the full `txn_prepare_pushing` (sleep-based retry loop included) and passed
identically whether `push_resolution_if_decided` was live or `return`-
stubbed to a no-op. Root cause: local single-voter leader election alone
ate roughly 750ms of the test's own setup, leaving just enough real
wall-clock time before the test's own final assertion for
`txn_resolver_loop`'s independent, unconditional one-second passive sweep
to win the race and clean up the deliberately-unresolved intent on its own
— a coincidence indistinguishable from the fix actually working, from the
test's own point of view. **The fix was to stop testing through the
timing-sensitive retry loop at all**: call the two `txn_prepare` attempts
directly (bypassing `txn_prepare_pushing`'s sleeps entirely) with the fix
under test invoked explicitly in between — deterministic, no `sleep`
anywhere in the critical path, and the whole test completes in well under
a tenth of the background sweep's own interval. **The general rule: a
`ProdEnv` regression test for a fix that races a background periodic task
must either complete fast enough that the periodic task provably cannot
have ticked yet, or must disable/bypass that task for the duration of the
test — "it passed" is not evidence the fix under test is what made it pass
unless something has ruled out every OTHER path to the same outcome,
background loops very much included.** Caught here only because this
round deliberately verified red-before-green by temporarily disabling the
fix and re-running — the same "temporarily invert the fix, confirm the
test fails" discipline this file's own Testing section already recommends
for retry-loop regressions, now shown to also catch a test that a
production safety net (not test flakiness) was silently propping up.

**Found, NOT fixed, and the reason the un-pin stayed blocked yet again**:
chasing the confirmed residual through further soak runs surfaced a
FOURTH, independent, and more severe failure — a `TransactWriteItems` call
that returned `Ok` outright (no retry, no ambiguity, nothing logged) for a
transaction whose own later `ConsistentRead: true` read of one of its own
keys came back completely empty. Leading, unconfirmed hypothesis: the same
`TxnResolve`-has-no-outcome-channel gap named above, but on the
COMMITTER's own resolve path rather than a blocked stage — under the
un-pinned soak's cascading-split cadence, every resolution attempt for one
committed write (the awaited `resolve_all_parallel` call, and
`txn_resolver_loop`'s own passive sweep afterward) could in principle keep
racing a fresh split before applying, each one silently no-op'ing and each
one reporting apparent success, so the write's own intent never actually
resolves anywhere while nothing ever signals a problem. Not root-caused
live — recorded here, per this file's own "capture the raw shape, don't
re-derive it" instruction, exactly so the next round does not have to
start from zero. **Soak result (41 runs total across the fix-confirmation
and gate phases, tallied honestly rather than stopping at a clean-looking
subset): the two sibling bugs (message-wrapping, decide-time-fresh-retry)
did not recur even once once their own fixes landed. The confirmed
self-conflict `TransactionConflict` residual DID recur once more, 17 clean
runs into the formal gate, even with the write-side push active** — the
captured trace shows the identical false-success resolve shape recurring a
second time on the same key, but at a log level that didn't capture
whether the push itself found the blocker unconfirmable or hit the
identical fence race on its own resolve attempt; not root-caused to that
depth, recorded for the next round rather than guessed at. **This new
"acked write lost" residual also recurred** (verified in isolation, a
genuine reproduction, not host-contention noise) — `SplitMode::InPlace`
stays not un-pinned, and the write-side push should be read as a real,
tested narrowing of the residual's reachable surface, not a categorical
close of it; `TxnResolve`'s own missing outcome channel is the more
durable fix a future round should reach for instead of another
individually-discovered-symptom patch.

**Amendment (2026-08-29): the outcome channel above shipped, and it surfaced
a SECOND, independent bug hiding behind the same missing signal.**
`KvCommand::TxnResolve` now records a `ResolveOutcome` (`Resolved`/
`Fenced`/`OutcomeMismatch`) per apply, keyed by Raft log index and paired
with the entry's own term — the exact `StageOutcome`-shaped channel this
entry's own closing paragraph named as the durable fix, built the same way
`CasResults`/`StageOutcomes`/`KindBatchOutcomes` already are (see this
file's own entries on those three for the shared term-identity doctrine).
`RaftKvNode::txn_resolve` now returns `Option<(HlcTimestamp,
ResolveOutcome)>`; `animusd::ClientCtx::txn_resolve_participant` returns
`Result<ResolveOutcome, String>`; a new bounded-retry wrapper
(`txn_resolve_participant_retrying`) re-resolves routing **fresh** on every
attempt when the outcome comes back `Fenced`, instead of the old behavior
of treating any applied entry as done.

**The second bug, found wiring the channel in, not in a soak**: the apply
arm's own `TxnTracker::unresolved_decided.remove(&txn_id)` call — which is
exactly what `txn_resolver_loop`'s passive per-second sweep reads to find
a decided-but-still-unresolved transaction to keep pushing — ran
**unconditionally**, before the entry's own fence/mismatch outcome was even
computed. A `Fenced` resolve therefore erased this group's own memory that
the transaction still needed resolving, in the identical tick the fence-miss
happened — the passive safety net had already given up on exactly the
transaction it exists to rescue, with no way to tell from the outside. This
is a second, independent, concrete mechanism behind the "resolve reports
success but the intent stays live" shape this whole entry chases, found
purely by reading the apply arm's own code once the new outcome value gave
something to condition the clear on — **the general lesson: adding an
outcome channel to an apply arm is also the moment to re-audit every OTHER
side effect that same arm already performs unconditionally, since "this
entry applied" and "this entry did what it meant to" were conflated
everywhere in that arm, not just in the one return value with a name**.
Fixed by gating the `remove` on `resolve_outcome == ResolveOutcome::
Resolved`.

**A pre-existing regression test's own unstated assumption broke the moment
this landed, which is the correct outcome, not a regression to work
around.** `animus-cp-data/tests/txn_recovery.rs::
pending_txns_reflects_applies_across_restart` resolved with `TxnOutcome::
Committed { commit_ts: txn_id.ts }` — the pre-decision *candidate*
timestamp, not the real decided value `commit_at_least` actually returns
(`mint_at_least` mints strictly above the candidate, so the two are
essentially never equal) — and asserted `unresolved_decided` cleared
afterward. Before this fix, the resolve's own `outcome_mismatch` no-op
still got treated as "done" by the then-unconditional clear, so the test
passed for the wrong reason: it never actually exercised a genuine resolve
at all. After the fix, the same stale value correctly no-ops
(`OutcomeMismatch`) and leaves the tracker untouched, failing the
assertion — not because the fix is wrong, but because the test was
silently relying on the exact bug this round closed. **The general lesson,
sharpened from this file's own recurring theme: a test asserting an
eventual-consistency-shaped postcondition ("X eventually clears") can pass
for years on a value that's subtly wrong, as long as something ELSE
(here, an unconditional side effect one call away) makes the postcondition
true regardless of whether the call under test actually did its job —
fixing the masking bug is what makes the test start asserting what it was
always supposed to.** Fixed by resolving with `commit_at_least`'s own
returned ts (sound specifically because this test's own scenario has no
concurrent recovery decider to race — the general rule the crate's own
docs already state, that a caller must re-read the record's *actual*
status rather than trust a propose call's own return, still applies
everywhere a second decider is possible).

New regression at the primitive level: `animus-cp-data/tests/
txn_resolve_outcome.rs` (a real in-place split forking the **participant's**
own tablet between the anchor's commit and the participant's resolve,
proving `Fenced` and — since the participant holds no local copy of the
anchor's record to reconstruct the value from — that the key stays
genuinely unreachable, not silently marked done). **Still not attempted**:
the mandated fresh 30-run un-pinned `SplitMode::InPlace` soak — this round
closes the structural gap the soak's own investigation named as the
clearest path forward, but per this file's own "any failure keeps the pin"
discipline, only that soak can actually move the pin. See ADR 0018's
2026-08-29 amendment and ADR 0058's matching note.

## A converged-or-timeout poll can still race if its condition is weaker than what the assertion after it needs (issue #421)

`cluster.rs`/`per_process.rs`'s `await_bootstrap` helpers polled for
`!n.metadata().members.is_empty()` — true the instant the *first* member
registers — then the test's very next line asserted the *full* member
count (`meta.members.len() == 3`). Membership registration is one node at
a time, so this is the same "eventual property, one-shot assert" shape
the rest of this log warns about, just hiding one level down: the poll
loop itself wasn't a bare one-shot, but the condition it converged on
wasn't the condition the caller actually depended on. The fix is to poll
for the exact predicate the following assertion needs (here,
`n.metadata().members.len() == nodes.len()` on every node), not a weaker
stand-in that merely correlates with it. When reviewing a `await_*`-style
helper before trusting it to cover an assertion, check that its loop
condition and the assertion's condition are the same fact, not two facts
that are merely close together in bring-up order.

## A stale-looking type-mismatch error can mean a shared `CARGO_TARGET_DIR` served an rlib from before this session's own edit (ADR 0060 advertise/dial split)

Mid-refactor, `cargo build -p animusd` reported `expected BTreeMap<NodeId,
String>, found BTreeMap<NodeId, SocketAddr>` at an `env.set_peers(...)` call
site whose surrounding code — both the caller's `.collect()` and
`ProdEnv::set_peers`'s own signature — had already been edited to agree on
`String`, moments earlier in the same session, with `animus-env` untouched
since. The error made no sense read literally. Cause: this environment's
mandatory `CARGO_TARGET_DIR=/home/user/shared-cargo-target` is a directory
shared across concurrent agent sessions/worktrees, and the build was
linking against an `animus-env` rlib compiled from a *different* worktree's
copy of `prod.rs` (pre-dating this session's own `set_peers` signature
change) rather than picking up the edit just made in *this* worktree — a
`touch crates/animus-env/src/prod.rs && cargo build -p animus-env` forced a
fresh compile from this worktree's own source, and the identical `animusd`
build then succeeded with no further code changes. **The tell**: a type
error whose two sides both look correct in the code you can see, especially
one naming a crate two or more levels below the one you're editing and that
you haven't touched THIS session. Before hunting for a bug in code that
reads right, force a rebuild of the suspect lower-level crate on its own
(`touch` the file, or `cargo build -p <crate>`) and re-check — cheaper than
a wrong-theory debugging detour, and the fix leaves no trace once the real
build lands.

## A static hostname alias (`localhost`) cannot simulate a moved endpoint — a real "same name, new address" test needs a name whose resolution the test itself can change (ADR 0060 advertise/dial split)

Testing "a node restarts on a different bind IP but keeps the same
advertised host, and peers still route to it" with `advertise_host:
Some("localhost")` seemed like the obvious choice — until the restarted
node rebound on `127.0.0.2` and the *other* node's dial of
`"localhost:{port}"` kept resolving to `127.0.0.1` (where nothing was
listening anymore), so the two-voter control group could never regain
quorum and the test hung at its second bootstrap wait until timeout.
`localhost` is a **static** `/etc/hosts` entry — nothing in the test moved
it, so of course it kept pointing at the address it always pointed at; the
real-world mechanism this ADR's advertise/dial split is built for (a
Kubernetes `Service`'s DNS updating when a pod is rescheduled) has no
sandboxed equivalent unless something in the test can actually make the
name re-resolve. Diagnosing this cost a debug-instrumented rerun (temporary
periodic `eprintln!` of both nodes' `is_control_leader`/`members`/
`node_addrs` inside the poll loop) that showed the restarted node's own
metadata forever empty — never even a rejected connection, just silence,
because the *leader's* traffic toward it was the direction silently going
nowhere. Fixed by managing a real, test-owned `/etc/hosts` entry (add
before bind, rewrite to the new IP at the moment of "restart", remove via
`Drop` so a panic mid-test still cleans up) — the only way in this sandbox
to make a hostname's resolution genuinely change mid-test, which is the
actual behavior being proven. General form: when a test needs to prove
"reachability follows a name, not an address, across the address
changing," a fixed alias proves nothing — either control the name's
resolution directly (as here) or design the assertion around what's
provable without dynamic re-resolution (e.g., the self-registered address
*string* staying unchanged, decoupled from whether a live connection can
currently complete).

## Heavy concurrent multi-agent `cargo build`/`test` on one shared `target/` dir can exhaust disk mid-gate, unrelated to the diff under test (2026-08-28)

Validating a small, self-contained `animusd` change (ADR 0061 rung A6) hit
repeated `error: ... No space left on device` / `couldn't create a temp
dir` / `LLVM ERROR: IO failure on output stream` failures from `cargo
build --workspace --all-targets`, `clippy`, and even single-test-binary
`cargo test` runs — not from any error in the code, but because several
other agent sessions were compiling in parallel against the *same*
`target/` directory (confirmed via `ps aux` showing concurrent `cargo
build --workspace --all-targets` from a different shell PID) on a
container whose real free space (`df -h /`, `Avail` column) is far smaller
than nominal size and swings from single-digit GB to under 100MB within
minutes as those builds run. `rm -rf target/debug/incremental` reliably
frees the most space per byte of risk (it is a recompute-only cache, never
a linked artifact another process depends on) but the freed space can be
consumed again within one more `cargo build --all-targets` invocation
(hundreds of MB per linked test binary, and this crate alone has ~100).
**Rules that held up**: prefer `cargo check`/`clippy` over `cargo build`
for a broad multi-crate sanity pass (no linking, far less disk); prefer
`-p <crate>` or a single `--test <name>` over `--workspace`/`--all-targets`
when disk is tight, since each linked binary is the expensive step, not
compilation; set `CARGO_INCREMENTAL=0` before a build run immediately
after clearing `incremental/` so the clearing isn't racing the same
build's own writes into it; and when a from-scratch full-workspace gate
genuinely cannot be completed, trust an **earlier, already-green run of
the identical byte-for-byte source** (verified via `diff -q` against a
saved copy) over repeatedly re-attempting a gate that fails purely on
`ENOSPC`/linker I/O errors — re-running it does not gain information once
the failure signature is unambiguously disk-exhaustion rather than a
compiler diagnostic. Never `rm -rf target/` itself while sibling sessions
may be mid-build; that destroys artifacts they still need mid-link, unlike
`incremental/`.

## A "shared X" refactor across N near-identical copies needs a captured before/after mapping, not just a source read — and a Cargo dev-dependency cycle is fine (ADR 0061 rung B1, shared corpus harness)

Extracting `animus-test::corpus` (`name_seed`/`seeds_from_env`/`seed_expand`/
`for_each_seed`) out of 9 corpus files' hand-rolled copies looked, from
reading the source, like one identical FNV-1a function duplicated nine
times. It wasn't: four of the nine (`backup_fault_corpus.rs`,
`backfill_fault_corpus.rs`, `stream_lineage_corpus.rs`,
`pitr_fault_corpus.rs`) OR'd the low bit onto the hash
(`h | 1`, no comment anywhere explaining why) while the other five didn't —
a genuine, silent divergence that a "these all look the same, let me
dedupe" pass would have collapsed into a single behavior, quietly moving
every one of those four corpora's committed regression seeds. The fix
was to keep both hash flavors as two distinct public functions
(`name_seed`/`odd_name_seed`) rather than unify them, and to structure the
migration so drift was provable rather than asserted: **before** touching
each corpus file, append a throwaway `#[test] fn temp_dump_seed_map()`
that prints every frozen scenario's `(name, seed)` pair, capture that
output; migrate the file onto the shared module; rerun the identical dump
test; `diff` the two captures for byte-for-byte equality; only then delete
the temp test. Nine independent before/after diffs, all empty, is what
actually backs the "seeds didn't move" claim — the corpus's own internal
`assert_eq!(seed, name_seed(name))`-style guards are not independent
evidence, because after the refactor they check the refactored function
against itself. The general form: when consolidating N call sites that
"do the same thing," capture each site's *observable output* independently
of the code before assuming the code is identical enough to merge, and
treat that captured baseline (not a source-level read) as the diff target
for the refactored version.

The second finding was a design-constraint, not a bug: two of the corpora
being migrated (`animus-cp-data`, `animus-control`) live in crates
`animus-test` **already** dev-depends on (for its own corpus tests), so
adding `animus-test` as those crates' dev-dependency looked like it would
create a manifest cycle. It doesn't — `cargo check -p animus-cp-data
--tests`/`-p animus-control --tests` built clean immediately. Cargo
permits a dependency cycle that exists **only** through `[dev-dependencies]`
edges on both sides, because a dev-dependency is never required to build a
crate's own library — only its own tests/examples/benches, which are never
on the path of building anything that depends on that crate normally. Two
crates whose test suites need each other's test-only library code is a
legitimate shape, not a smell to route around with a lower shared crate;
worth checking with an actual `cargo check` before assuming a cycle is
real and re-architecting to avoid it.

Third, smaller finding: two authoring shapes for "expand a frozen scenario
into K seed variants" existed and both were worth preserving as distinct
primitives rather than forcing one on the other. Corpora that build a
`Vec<Scenario>` up front (`raftkv_linearizable.rs`, `txn_serializable.rs`,
`reconciler_corpus.rs`, `inplace_split_reconciler.rs`) share a
`seed_expand<T: SeedVariant>(cells: Vec<T>, k: usize) -> Vec<T>`, generic
via a two-method trait (`scenario_name`/`reseeded`) so the harness owns the
expansion loop without forcing every corpus's `Scenario` into one shape.
Corpora that drive one named scenario directly with no `Vec<Scenario>`
(`backup_fault_corpus.rs` and its three siblings) share a closure-based
`for_each_seed(name, k, body)` instead — trying to unify these into one
API would have meant either building a throwaway `Vec<Scenario>` where none
existed before, or threading a trait through call sites that have no
struct to hang it on. Two of the four `seed_expand`-style corpora
(`reconciler_corpus.rs`, `inplace_split_reconciler.rs`) also carried a
`name: &'static str` field purely to dodge threading an owned `String`
through expansion, with `reconciler_corpus.rs` actually paying for it via
`Box::leak` (a small, bounded, but real per-test-run leak) while
`inplace_split_reconciler.rs` avoided the leak by rebuilding the struct
field-by-field with a Copy-only reasoning that quietly relied on `name`
staying `&'static str` forever. Switching both to owned `String` (and
deriving `Clone`) removed the leak entirely with no observable behavior
change — the field's `'static`-ness was never load-bearing, just an
artifact of the original author reaching for the same shape as a sibling
corpus that also never needed `'static` originally.

## Extending a seed-gated fault model: share the roll, don't add a parallel one; and a "frozen node" fault needs the primitive it freezes to know who owns it (ADR 0061 rung B2/B3)

Adding `DiskConfig::set_enospc_prob` (an ENOSPC-distinguishable disk fault,
alongside the pre-existing generic `error_prob`) was tempting to implement
as a second, independent RNG roll — draw once for "does the generic error
fire", draw again for "does ENOSPC fire". That would have been wrong even
though both draws would individually be deterministic: it changes the
**number** of RNG draws `inject_disk_fault` makes for a config that only
ever set `error_prob` (now it would draw for ENOSPC too, even at threshold
0 — no, gating each draw on its own threshold avoids that specific case —
but it still changes draw *count* the moment both are configured together,
and more importantly it's a second source of truth for "did a fault fire
at all" that has to agree with the first). The fix that actually preserves
byte-identity for every existing config is **one shared roll, two
buckets**: draw once, and check `roll < enospc_threshold` then `roll <
enospc_threshold + error_threshold`. With the new knob at its default (0),
the combined check degenerates to *exactly* the original single comparison
against `error_threshold` — not merely "the same number of draws", the
identical draw feeding an identical comparison. The general form: when a
new fault variant is meant to be mutually exclusive with an existing one on
the same op, don't reach for "another independent `if` with another
independent draw" — fold it into the existing roll as an additional bucket,
so the old knob's byte-identity proof needs no new argument at all (it's
still "the same draw, the same comparison" — just with a widened, but
default-empty, lower bucket carved out of the front of the range).

Implementing `Simulator::pause(node, dur)` (alive-but-frozen: no timer
fires, no send leaves, no delivery lands, until a bounded resume instant)
surfaced a structural gap: the simulator's `Sleep` future — the thing that
actually schedules a `Timer` event on the shared timeline — had never once
needed to know *which node* it belonged to, because nothing before this had
a reason to treat one node's timers differently from another's. A `pause`
that must defer only the *paused* node's timers needs exactly that
ownership, so `Sleep` gained a `node: NodeId` field (populated from
`SimEnv`'s own `self.node_id`, already on hand at the `Clock::sleep` call
site) and `SimState` gained `timer_owner: BTreeMap<TimerId, NodeId>`,
written once, the first time a `Sleep` actually schedules a timeline entry
(not on every poll — a `sleep` that resolves immediately never needs an
owner, since there's no timer to ever defer). The general lesson: a new
fault that targets "state belonging to one node" can require retrofitting
ownership tracking onto a primitive that has lived fine without it for
years, because nothing about that primitive's own job ever needed to
distinguish nodes — the fault is what introduces the need, not a
pre-existing design gap. Look for this before assuming a new fault is a
pure additive knob: does it need to single out state by node/owner that the
mechanism it's modifying currently treats uniformly?

Two smaller traps hit while building the corpus for these: (1) a spawned
async block that does `out.lock().unwrap().push(some_async_call().await)`
in one expression fails `Send` bound checking (`spawn_task` requires `Send
+ 'static`) because the temporary `MutexGuard` from `.lock()` is deemed
live across the `.await` even though it's logically dropped before the
push's argument is evaluated — split it into `let x = foo().await; guard =
lock(); push(x);` on two statements, the same fix this crate's own
`append`/`sync` doc comments already document for *production* code, which
turns out to bite test code identically. (2) A test asserting "an fsync
that lies still loses the *second* write but the first, genuinely synced
write survives" needs the fault **installed after** the first write's
already-successful sync, not from the start of the simulator run — with
`fsync_lie_prob` set globally from t=0, *every* sync lies, including the
one meant to be the test's trusted baseline, and the failure surfaces as
"everything is gone", easy to misdiagnose as the fault implementation being
too aggressive rather than the test's own setup being wrong. The general
form (already documented elsewhere in this log for disk error injection,
worth restating for a new fault kind): when a test needs a "this part is
real, only the later part is faulty" baseline, install the fault config
*after* the baseline operations run on the same `Simulator`, not before.

Building the ADR 0061 rung B4 failure minimizer surfaced a lesson worth
generalizing beyond this one facility: **an ADR's Consequences section can
promise something that isn't literally buildable as stated, and the fix is
to amend the ADR honestly rather than force-build the literal words.** ADR
0003 promised "shrinking a failure to a minimal seed becomes possible" —
but a `SimEnv` run is a pure function of an *opaque* seed by design (that
opacity is the whole point of a seed), so no seed is "smaller" than
another and there is nothing to shrink *to*. Two different seeds are two
unrelated executions, not a big and a small version of the same one. The
actually-buildable, actually-useful thing was minimizing a failing
scenario's own *parameters* while holding its seed fixed — a different
target than the ADR's own words named. Don't let a stale promise's exact
phrasing constrain the design of the thing that fulfills its *intent*;
when you build the right thing under a different name than the original
promise used, say so explicitly in the doc you're amending (a reader
diffing "shrinking" against what shipped needs the mapping spelled out, not
left to be inferred), rather than silently reinterpreting old prose to
have meant what you built.

A second, more mechanical lesson from the same task: **when validating a
new "fires on failure, does nothing when green" diagnostic path (a
minimizer, an auto-triage tool, a report generator gated on an assertion
failing), a synthetic in-memory predicate proves the algorithm but not the
wiring** — the wiring has to be exercised against a real failure from the
real harness it's attached to, which for an already-green corpus means
either fabricating one temporarily (edit in a forced `result.ok = false`
for one named case, run the real corpus loop with the new tool enabled,
observe, then revert the edit before committing — never leave the forced
failure in the tree) or, better where it fits, constructing a *genuine*
regression: a real scenario whose real, deterministically-computed outcome
is the failure you want to minimize, without needing an actual production
bug or weakening any real assertion. `raftkv_linearizable.rs`'s shrink demo
uses the latter (`read_pct = 100` makes the workload structurally never
write, so `ok_writes == 0` is a genuine simulator-derived fact, not a
fabricated boolean) — it's the same trick `negative_control.rs` uses for
the Elle checker (hand-built histories the checker *must* reject), applied
one layer up: prove a piece of test *infrastructure* against a
deliberately-constructed-but-real case, not a toy stand-in and not a wish
that a production bug will conveniently exist to test against.

## `disallowed-methods` can't name a unit struct, and a package-level Cargo `[lints]` override beats scattering `#![allow]` across dozens of same-crate test-binary roots (ADR 0061 rung B5)

Two mechanical findings from adding `clippy.toml` `disallowed-methods`
entries for the non-`HashMap`/`HashSet` half of ADR 0003's determinism rule
(`Instant::now`, `SystemTime::now`, `tokio::spawn`,
`tokio::time::{sleep,timeout}`, `thread_rng`, `OsRng`).

**A `disallowed-methods` entry for a type used as a bare value (`OsRng`,
`rand::rngs::OsRng` — a zero-sized unit struct instantiated inline, e.g.
`&mut rand::rngs::OsRng`) silently fails its own config, not the build.**
Clippy accepts the `clippy.toml` entry but emits `warning: expected a
function, found a struct` at every crate root the moment anything in the
workspace triggers a clippy pass — a real signal, easy to read as noise and
scroll past since it doesn't fail `-D warnings` on its own (a malformed
`disallowed-methods` config entry is a warning, not an error). The fix is
routing that specific item through `disallowed-types` instead (same
`clippy::disallowed_types` lint HashMap/HashSet already use, same
`[workspace.lints.clippy]` level already set to `"warn"` for it) — a type
name goes in `disallowed-types`, a callable path goes in
`disallowed-methods`, and clippy's own warning names which one you picked
wrong. Generalizable check before adding any `disallowed-methods` entry: is
the target actually a function/method, or a type that merely gets
constructed/referenced bare? `OsRng`, `PhantomData`, a marker/unit struct
used as a value — all belong in `disallowed-types`, regardless of how
"function-like" the call site reads (`&mut rand::rngs::OsRng` looks like it
could be an argument to a function named `OsRng`, but it's a value of type
`OsRng`).

**Cargo's `[lints]` table is package-scoped, not crate(-root)-scoped — it
applies to every target the package builds (lib, every `[[bin]]`, every
`tests/*.rs` integration-test binary, every bench), which makes it the
right tool for exempting a lint across a package with many separate
compilation-unit roots, not just a `#![allow]` at one crate root.** A
source-level `#![allow(clippy::disallowed_methods, reason = "...")]` inner
attribute only covers the one file it's written in plus any `mod`s it
pulls in from the *same* crate root — it does **not** reach a sibling
`tests/other_file.rs`, since each file under `tests/` compiles as its own
independent crate root under Cargo's integration-test model. A package like
`animusd` with one `lib.rs`, one `main.rs`, and ~70 separate `tests/*.rs`
crate roots therefore has ~72 independent places a file-level `#![allow]`
would need to go to fully exempt the package by that method — worse than
useless, since it reads as "someone forgot 12 of them" rather than "this is
deliberately exempted." A package-level override
(`crates/animusd/Cargo.toml`'s `[lints.clippy] disallowed_methods =
"allow"`, replacing the `[lints] workspace = true` shorthand with the
workspace's other lints copied in by hand plus this one override) is a
single, visible, one-time decision that covers every target in the package
by construction — no target can silently fall outside it the way a missed
file would. The general principle: when a lint needs to be off for "this
whole package, as a documented exception," reach for the package's own
`Cargo.toml` `[lints]` table before reaching for `#![allow]` — the latter
is the right tool for a *specific file or item* inside an otherwise-linted
package, not for exempting the package itself, and picking the wrong one
for the latter produces an incomplete-looking wall of near-duplicate
annotations that a reviewer has to trust is exhaustive rather than a single
statement that obviously is.

Third, softer finding: **before assuming a new lint needs a lot of
`#[allow]`s, grep the real call sites first** — `grep`-ing every crate this
task actually targeted (the ones this ADR's `Env` seam is meant to cover)
for real code (not doc comments mentioning the disallowed name) found
`src/` already at zero violations everywhere except `animus-env`'s own
`ProdEnv` and the crate this rung already expected to be the hard case
(`animusd`). The discipline the lint was about to start enforcing had, in
every other crate, already held under review alone — worth confirming
before writing a single `#[allow]`, since it changes the shape of the work
from "find and annotate a pile of violations" to "confirm there's nothing
to annotate, then handle the two known exceptions."

## Feature-gating an item everything transitively depends on (ADR 0061 rung C0)

Gating `animus-env`'s `ProdEnv`/`FsSegmentStore` behind a default-off `prod`
feature (so a crate can depend on `animus-env` with `default-features =
false` and genuinely not have `ProdEnv` in its build) turned up two
generalizable points, beyond the "grep real code, not doc comments" one
above (the same trap bit again here: `tracing` was declared as an
unconditional dependency, but every non-doc-comment `tracing::` call in the
crate lived in `prod.rs` — the crate had been unknowingly carrying a
prod-only dependency as if the whole crate needed it, for a `tracing`
version bump exactly zero call sites outside `prod.rs` would ever notice).

**First: to prove a feature gate actually closes the door, don't just read
the `#[cfg]` and reason about it — compile against it.** A tiny scratch
crate (own `Cargo.toml`, path-dependency on the gated crate with
`default-features = false`, one line naming the item that should be
unreachable) is cheap and unambiguous: either it fails to compile with a
"configured out ... gated behind the `X` feature" note pointing at the
`#[cfg]`, or it doesn't, and either way you have the compiler's word for it
instead of your own reading of the manifest. (Needs the target crate's
`rust-toolchain.toml` copied alongside it if the workspace pins a toolchain
newer than whatever `rustc` is on `PATH` — otherwise the scratch build fails
on an unrelated MSRV mismatch before it ever gets to the question being
tested.) The same technique generalizes to proving any "X should not be
reachable from Y" claim about a manifest, not just this one.

**Second: `cargo tree -e features -p <crate>` on its own is not the tree
that ships** — by default it includes the package's `[dev-dependencies]`,
so a dev-only feature (like `prod` on a `[dev-dependencies]` entry added
specifically so the *library* stays clean) shows up in the default `tree`
output and can look like it leaked into the library when it didn't. Add
`--no-dev-dependencies` to see the graph `cargo build -p <crate>` (library
only, no tests/benches) actually resolves — that is the tree that answers
"does the library itself pull this in." Separately: with the workspace's
resolver (`resolver = "3"`, the edition-2024 default), a package's own
`[dev-dependencies]` features are *not* unified into its own library build,
confirmed by comparing `--no-dev-dependencies` output (no `prod`) against
plain `cargo tree` output (shows `prod`) for the same package — but a
single `cargo build --workspace --all-targets` invocation still unifies
features across every target it builds *together*, including other
packages' test binaries, so putting a feature on `[dev-dependencies]`
documents "the library doesn't need this" precisely rather than
mechanically *enforcing* it under every possible invocation — say so
plainly rather than overclaiming the boundary when reporting work like
this.

## Overriding a workspace-inherited dependency's `default-features` (ADR 0061 rung C1)

Creating `animus-node` with `animus-env = { workspace = true, default-features
= false }` fails at manifest-parse time — "`default-features = false` cannot
override workspace's `default-features`" — even though the root
`[workspace.dependencies]` entry for `animus-env` doesn't mention
`default-features` at all (so the *effective* default is already "no
features on," since `animus-env`'s own `[features]` table declares no
`default = [...]` list). Cargo's inheritance rule cares about the presence
of the override relative to what the root entry states, not about what the
resulting feature set would actually be — a member cannot set
`default-features = false` on an inherited dependency unless the root entry
already says `default-features = false` too. The fix is not to touch the
root entry (that would flip every other consumer's default, the same
one-crate-widens-scope-for-everyone hazard the ADR 0061 rung C0 entry above
warns about with feature gates generally) but to stop inheriting for that
one line: `animus-env = { path = "../animus-env", default-features = false
}`, a direct path dependency, sidesteps workspace-dependency inheritance
entirely and takes the override with no restriction. General lesson: when a
new crate needs a *stricter* feature configuration than a workspace
dependency's inherited default for one line only, reach for a direct `path`
(or `version`) dependency rather than fighting the inheritance override
rule — it's not a workaround, it's the mechanism Cargo actually offers for
"this one dependency, this one consumer, opts out of workspace defaults."

## Moving a `pub(crate)` item across a new crate boundary (ADR 0061 rung C1)

Carving `animus-node` out of `animusd` (wire types, `topology`, `decide`)
surfaced a mechanical but easy-to-miss step: every moved item that was
`pub(crate)` in `animusd` had to become `pub` in `animus-node`, and every
moved struct's individual fields that were `pub(crate)` (not just the
struct itself) had to widen too. `pub(crate)` scopes to the crate that
*declares* the item — once the item lives in a different crate than its
callers, `pub(crate)` silently becomes "unreachable from anywhere but
`animus-node`'s own tests," a privacy error at every call site in
`animusd`, including inside a `pub use` re-export shim (`pub use
animus_node::topology;` compiles, but `animusd::topology::decide_cp_route`
from outside `animus-node` does not, unless `decide_cp_route` itself is
`pub`). This is easy to miss because the compiler error at the *re-export*
site is clean (`pub use` of a private path is legal within the same crate,
since `lib.rs` is inside `animusd`, the crate importing it) — the failure
only shows up downstream, at whichever call site first tries to actually
*use* the re-exported name, with a plain "function is private" error that
doesn't obviously point back at the moved declaration's visibility. Two
concrete instances this rung hit: `PendingKindWrite`/`TxnTableWrite`'s
struct fields (some call sites build them via struct literal, not just the
`TxnTableWrite::plain` constructor, so the fields themselves — not just the
types — needed widening), and every function/enum in `topology.rs`/
`decide.rs` (all `pub(crate)` in the original single-crate layout, all
needing `pub` once `animusd` became a foreign-crate consumer). General
lesson: when moving code across a *new* crate boundary (as opposed to
refactoring within one crate), grep every moved item's visibility
modifier, not just its `pub`/private status — `pub(crate)` is exactly the
one visibility level that silently means something different depending on
which side of the boundary you're standing on, and `cargo build` catches
it late (at use, not at declaration) rather than at the point where the
mistake was actually made.

## Host-capability traits let a leaf loop move before its "brain" does (ADR 0061 rung C2)

Phase C's plan called the six `animusd` background loops (`ttl_reaper`,
`backup_janitor`, `pitr_janitor`, `segment_janitor`, `backup_completion`,
`index_backfill`) easy first movers into `animus-node`. Scoping the rung
found that wrong: every one of them takes `ClientCtx` by value or reference,
and `ClientCtx` is the crate's 5,569-line brain, not scheduled to move until
rung C5. On the plan's own ordering, nothing in C2 could move at all.

The fix generalizes beyond this one rung: when code B (a loop, a handler, a
consumer) can only move because it depends on code A (a big, not-yet-movable
"god object"), don't wait for A to move and don't reimplement A's logic
inside B's new home. Instead, scope exactly which **operations** of A, B
actually calls — usually a small, named slice, even when A itself is huge —
define a narrow trait for just that slice, implement it for A as a **thin,
logic-free delegation** (translate the shape, call the existing method,
nothing more), and move B generic over the trait. A stayed exactly where it
was; B stopped depending on its *type*, only on a few of its *operations*.
Three traits came out of this rung (`ControlLeaderHost<E>`,
`BackupObjectStore`, `TtlScanHost` — see `animus-node/CLAUDE.md`'s own rung
C2 entry), sized by cohesive capability rather than one trait per loop or one
fat trait for everything; a trait every implementor exercises in full is a
good sign, a trait that exists only to make one specific move compile is not.

This is *better* for testability than moving the loops unchanged would have
been, not merely a workaround: a loop generic over a capability trait can be
driven under `SimEnv` against a synthetic fake implementing just that trait
— no cluster, no sockets, no `ClientCtx` — which is deterministic coverage
those loops had never had. A loop moved but left coupled to a concrete
`ClientCtx` would still have been untestable until C5 landed; the capability
trait is what makes the move worth doing now rather than later.

Corollary, worth stating because it looks like a shortcut and isn't one: not
every loop in the batch has to move. `segment_janitor.rs` stayed in
`animusd` this rung — its replica-repair phase makes real placement/
membership decisions (which replicas are still live, where to push a
repaired copy), not a value nameable as one narrow I/O delegation the way
"durably store these bytes" is. Forcing it to move would have meant either
smuggling real decision logic into the leaf crate (exactly what this phase
is supposed to prevent) or building a capability surface wide enough to
expose that logic anyway, which is a contorted trait wearing a narrow one's
clothes. A partial rung with a precise, per-loop account of what moved and
why the rest didn't is a better outcome than forcing every item on the list
to move.

## `propose()`'s `Accepted` says nothing about apply-time rejection, and a test can silently misconstruct its own fixture because of it

Building a `SimEnv` test for the (now moved) `index_backfill` loop, a
two-tablet-per-table fixture was built by calling `MetaCommand::CreateTablet`
twice for the same table. Both calls returned `ProposeResult::Accepted` (the
`assert!(matches!(.., Accepted))` on each passed), so the test proceeded
believing both tablets existed — but the *second* `CreateTablet` is a
deliberate apply-time rejection (`Metadata::apply`'s own rule, ADR 0023: "one
`CreateTablet` per table; every further tablet comes only from a real
split"). Since `Metadata` is `DRIVER_APPLIED` (ADR 0038), `propose()`'s
`Accepted` means only "appended to my own Raft log" — the semantic
accept/reject decision happens later, asynchronously, in the apply task, and
`propose()`'s return value cannot see it. The test's own straggler-tablet
assertion then passed for the wrong reason: with only one tablet actually in
`Metadata.tablets`, "every tablet has reported" went vacuously true the
moment that one tablet reported, which looked identical to the intended
"both tablets must report" property from the assertion's own perspective.

General lesson: **a green assertion on `ProposeResult::Accepted` is not
evidence a command's own semantic rule accepted it** — for any
`DRIVER_APPLIED` state machine (`Metadata` here; the CP-data `KvState`
plane the same way), check the *post-apply* state (`node.metadata()`,
re-read after enough `sim.run_for`/`run_until` for the apply task to have
run) before trusting a fixture built from a sequence of proposals, especially
one hand-built for a test rather than driven through a real client that
would have surfaced the rejection. The existing `animus-control` test suite
already knows this rule (`complete_backup_requires_every_pinned_tablet`
drives `BeginSplit`/`CutoverSplit` for exactly this reason, with a comment
saying so) — the lesson here is that the same trap is easy to walk into
fresh when writing a *new* crate's *first* `SimEnv` fixture, where there is
no existing sibling test to copy the pattern from.

## A type alias at the concrete instantiation site can keep a "genericize this type" rung from cascading into its every consumer

ADR 0061 rung C3c genericized `animusd`'s `ControlHandle`/
`RemoteControlClient` (`Local(RaftNode<ProdEnv>)` → `Local(RaftNode<E>)`;
`RemoteControlClient`'s one real-I/O method reaching a new `RelayClient`
trait instead of a concrete `relay_request` free function) and moved both
whole into `animus-node`. The natural worry going in: `ClientCtx.control:
ControlHandle` is a field on the crate's ~5,500-line hub struct, so
"genericize `ControlHandle`" sounds like it should force `ClientCtx` itself
to grow `<E, R>` parameters — which would have meant threading them through
every function that takes or returns a `ClientCtx`, the exact C5-shaped
blast radius this rung was explicitly scoped to avoid.

It didn't happen, because `animusd` only ever needs *one* concrete
instantiation of the newly-generic type: `ControlHandle<ProdEnv,
AnimusdRelayClient>`. A crate-local type alias (`pub(crate) type
ControlHandle = animus_node::control_handle::ControlHandle<ProdEnv,
AnimusdRelayClient>;`) binds the generic type back down to a concrete one at
its single point of use, so every existing `ControlHandle::Local(..)`/
`ClientCtx.control: ControlHandle` site keeps compiling completely
unchanged — `ClientCtx` never sees a type parameter, because from its point
of view the alias *is* a concrete type. Only the two real constructor call
sites (`RemoteControlClient::new`/`with_mirror`) needed a one-line update
(pass the new `relay`/`timeout` arguments) since a constructor's arity
genuinely changed; every read-only accessor call site was untouched.

General lesson: when scoping "genericize type `T` and move it to a lower
crate," check whether the *consuming* crate only ever needs one concrete
instantiation of `T` before assuming the generic parameters must propagate
into every struct that holds a `T`. If so, a local type alias at the
instantiation site is the seam that contains the diff to "the type moved
and gained parameters" rather than "half the crate grew two more generic
parameters" — the same shape `animus-node`'s own `ProdEnv`-free boundary
already relies on one level up (this crate depends on `animus-control`
un-genericized from *its* point of view too, via `animus-control`'s own
`E`-generic types resolving concretely wherever `animusd` instantiates
them). This is a smaller instance of the same "ask what narrow thing the
caller actually needs" question ADR 0061's standing C4/C5 guidance already
names for capability traits — here applied to a type's own generic
parameters rather than a trait's method set.

## `cargo build --workspace --all-targets` can exhaust disk long before `cargo clippy --workspace --all-targets` does — they are not equivalent load for a space budget

On a workspace this size, `cargo clippy --all-targets --all-features`
type-checks every test/bench binary in every crate (including
`animus-operator`'s `kube`-client dependency tree) without linking full
executables for most of them, so it fits comfortably in a constrained
`target/` budget. `cargo build --workspace --all-targets` does the full
codegen + link for the *same* set of binaries — every `#[test]`
binary and bench in every crate, not just the ones a given task's gate
list actually needs — and on a session with single-digit GB of headroom
this can run the disk to zero mid-build (`rustc-LLVM ERROR: IO failure on
output stream: No space left on device`), which then fails *unrelated*
crates (`animus-storage`'s `lsm_clone_concurrent` test, `animus-cp-data`'s
`apply_signal` test) that have nothing to do with the change in flight —
a confusing false signal if read as "the change broke something," when the
actual cause is disk exhaustion from building targets the task's own gate
never asked for. This repo's stated validation gate is `cargo build
--workspace` (no `--all-targets`); reach for `--all-targets` only when a
task specifically needs every test binary to *build* (not just
type-check), and clean `target/debug/{deps,build,incremental}` proactively
between build attempts on a constrained disk rather than after the error —
the clippy pass already proved the code type-checks, so the extra
`--all-targets` build was buying confirmation the task didn't ask for at a
cost the disk couldn't afford.

## A member-access grep that only matches one line misses `receiver\n.method()` sites rustfmt wrapped onto two lines (ADR 0061 rung C4d)

Scoping the `AdminHost` capability trait (rung C4d) started from a
single-line regex — `grep -noE "ctx\.[a-zA-Z_]+" admin.rs` — to enumerate
every `ClientCtx` member `admin.rs`'s handlers actually touch, the same way
earlier rungs' scoping passes had. It found 14 members and matched the
ADR's own "a 15-method cluster-shape slice" estimate closely enough to look
confirmed. It wasn't: `ctx.stream_change_rates()` and `ctx.trigger_split(..)`
were invisible to it, because rustfmt had wrapped both call sites as

```rust
let stream_change_rates: Vec<Value> = ctx
    .stream_change_rates()
```

— `ctx` and `.stream_change_rates()` on separate lines, so a pattern
anchored on `ctx\.` never sees the method name at all. A second pass that
first collapsed `re.sub(r'ctx\s*\n\s*\.', 'ctx.', text)` before matching
found 19 members, not 14 — `admin_add_control_member`,
`admin_remove_control_member`, `cp_kind_write_item`, `stream_change_rates`,
and `trigger_split` had all been missed, some of them (`cp_kind_write_item`,
`trigger_split`) real write-path primitives whose absence from a
capability-trait scoping pass would have been a correctness gap, not just a
miscount, had it gone unnoticed and the trait shipped one method short.

This is worth being deliberate about any time a task's *validation*, not
just its implementation, rests on grepping a large `rustfmt`-formatted
file for one identifier chained off another (`x.method()`,
`self.field.method()`): rustfmt line-wraps a chain at ~100 columns with no
regard for keeping the receiver and the first `.` on the same line, so a
single-line regex silently undercounts on exactly the call sites long
enough to wrap — which correlates with exactly the call sites most likely
to be doing something nontrivial. Either collapse whitespace-then-dot
before matching (as above), or use a tool that already understands Rust
syntax (`cargo expand`, an AST-aware grep, or simply reading the file)
rather than trusting a plain-text line-oriented pattern to enumerate a
type's own usage.

## Driving a trait's `async fn` in a `tokio`-free crate's own unit tests: `std::task::Waker::noop()` + `std::pin::pin!`, no `unsafe`

`animus-node` has no `tokio` dependency at all, in `[dev-dependencies]`
either (ADR 0061's rung C0/C1 boundary is enforced for the whole crate, not
just its library target) — so unit-testing `animus_node::admin::dispatch`
(an `async fn` calling into `#[async_trait]` `AdminHost` methods) against a
fake host couldn't reach for `#[tokio::test]`. The fix isn't a hand-rolled
`RawWaker` (which needs `unsafe` for `Waker::from_raw`, and this workspace
lints `unsafe_code` at `forbid`): `std::task::Waker::noop()` (stabilized
well before this workspace's MSRV) is a ready-made no-op waker, and
`std::pin::pin!(fut)` stack-pins a future with no `unsafe Pin::new_unchecked`
and no heap allocation. A ~10-line `loop { if let Poll::Ready(v) =
fut.as_mut().poll(&mut cx) { return v; } }` around those two is a complete,
safe, dependency-free `block_on` — sound whenever the future under test
never genuinely parks (resolves on its first poll, or every intermediate
`Pending` is guaranteed transient), which covers exactly the shape a pure
routing/dispatch test wants: a fake implementor whose methods return
immediately. Reach for this before adding a `futures`/`pollster`
dependency (or, worse, quietly loosening the crate's "no `tokio`, no
`unsafe`" invariants) just to drive a small `async fn` in a test.

## A default type parameter contains "genericize this type" blast radius the same way a type alias does — but it only resolves in *elided type positions*, never inside an enclosing generic scope (ADR 0061 rung C5 step 1, `CpGroup<E>`/`ClientCtx<E>`)

Rung C5 step 1 genericized `animusd`'s `CpGroup`/`SharedEngine`/
`ClusterEdgeState`/`CpRoute`/`ClientCtx` over `E: Env` **in place**
(same crate, nothing moved yet — see ADR 0061's fifth 2026-08-28
amendment). Unlike C3c's `ControlHandle` (a type consumed at exactly one
concrete instantiation, so a crate-local `type ControlHandle =
animus_node::control_handle::ControlHandle<ProdEnv, AnimusdRelayClient>;`
contained the whole diff to the type's own definition — see the entry
above), these five types are each still *named directly*, by their own
name, at roughly 200 call sites apiece throughout the crate — no single
alias site to bind. The equivalent seam here is a **default type
parameter on the definition itself**: `enum CpGroup<E: Env = ProdEnv>`,
`struct ClientCtx<E: Env = ProdEnv>`, etc. This is legal, ordinary Rust
(confirmed with a standalone `rustc` snippet before relying on it
workspace-wide) and has the identical effect for every call site that
never spells a type parameter: a bare `CpGroup`/`Option<ClientCtx>` in a
non-generic function, a struct field, or a `match` arm still means
`CpGroup<ProdEnv>`, so none of ~400 existing bare references anywhere
in the crate needed to change.

**The one place the analogy breaks, and the one worth knowing before
attempting this**: a default type parameter resolves to its **default**
whenever it is *elided in a type position* — including inside a
function that is *itself* generic over the identical-looking parameter
name. Verified directly:

```rust
enum CpGroup<E: Env = ProdEnv> { A(E) }
fn takes_ref<E: Env>(x: &CpGroup) {}   // x: &CpGroup<ProdEnv>, NOT &CpGroup<E>
```

Calling `takes_ref::<SimEnv>(&some_cp_group_e)` is a plain `E0308`
mismatch — the bare `CpGroup` in the signature never "sees" the
function's own `E`, because default-parameter resolution only consults
the default, never an enclosing scope's same-named parameter. The
practical consequence: once `CpGroup`/`ClientCtx` grew a default `E`,
every method/associated-fn signature *inside* the two `impl<E: Env>
ClientCtx<E> { .. }` blocks that named `CpGroup`/`CpRoute` explicitly
(`leader: &CpGroup`, `-> Option<CpRoute>`, …) had to be rewritten to
`<E>` by hand — about 30 sites, all caught by `cargo build` as plain
type-mismatch errors naming the exact line, not silently accepted with
the wrong meaning. Three sibling free functions the impl blocks call
into by name (`index_drain::{seal_now, pitr_seal_now, hot_read,
clear_backfill_cursor}`, `dynamo::{kind_write_item_at_leader,
eval_kind_txn_write, collection_bytes_at_leader}`, this crate's own
`median_split_key`) needed the identical `<E: Env>` treatment for the
same reason — a generic caller passing a `&ClientCtx<E>`/`&CpGroup<E>`
into a callee whose signature still says bare `ClientCtx`/`CpGroup`
hits the same mismatch, one level removed.

**What did NOT need fixing, and why it matters for scoping this kind of
change**: pattern-matching against an already-typed value
(`CpRoute::Local(leader) => ..`) and constructing a value whose type is
inferred from a `return`/call-argument context (`Some(CpRoute::
Local(leader))`) both resolve through ordinary type inference from the
already-known concrete type, not through default-parameter elision —
default resolution only applies when nothing else pins the type. Of the
roughly 90 `CpRoute`-mentioning lines inside `ClientCtx`'s two `impl`
blocks, only the 2 function *signatures* returning `CpRoute` needed a
change; the ~60 match arms and constructions elsewhere were untouched.
Anyone repeating this pattern for another type should expect the same
split: audit signatures (return types, parameter types, struct/enum
field types) exhaustively by hand or by grep, then let `cargo build`
catch anything missed — but expect the match/construction sites to be
free, and don't waste time annotating them defensively.

## A second default-typed generic parameter can force a supertrait bound that only shows up when a downstream call site needs it — and a blind regex conversion silently mis-targets `self`-free functions (ADR 0061 rung C5 step 3a/3b)

Two lessons from genericizing `ClientCtx<E: Env = ProdEnv>` a second time,
over `R: RelayClient = AnimusdRelayClient` (step 3a), then converting the
91 raw `tokio::time`/`tokio::spawn`/`tokio::select!` sites the resulting
`impl<E: Env, R: RelayClient> ClientCtx<E, R>` blocks still held (step
3b).

**1. A second generic parameter's supertrait bounds are invisible until a
*specific* method needs them — and the fix belongs on the trait, not on
one `impl` block.** Adding `R: RelayClient` alone compiled fine for every
method that never called `.clone()`/spawned a future capturing `self`.
The first sign of trouble was `txn_coordinator.rs`'s existing `let this =
self.clone()` (for a parallel resolve fan-out) and this rung's own
conversion of a `tokio::spawn(resolve_all(..))` into
`self.env.spawn_task(resolve_all(..))`: both need `ClientCtx<E, R>: Clone
+ Send + Sync + 'static` to hold **generically**, and nothing about `R:
RelayClient` implied any of those — `AnimusdRelayClient`, the one
concrete implementor that exists today, trivially satisfies all four as a
zero-sized type, so nothing forced the question until a specific method
in a specific file tried to rely on it. The tempting narrow fix — bound
just `txn_coordinator.rs`'s own `impl` block (`impl<E: Env, R: RelayClient
+ Clone + Send + Sync + 'static> ClientCtx<E, R>`) — turns out to not stay
narrow: `read_path.rs` calls `self.txn_status(..)`, a method defined in
that same stricter-bounded block, so the bound would have had to cascade
into every file that transitively calls into it, which given
`forwarding.rs`'s "calls into every sibling by name" role is most of the
five-module call graph. Adding `Clone + Send + Sync + 'static` as
supertrait bounds on `RelayClient` itself (mirroring `Env`'s own
supertrait shape exactly) closed it in one place instead — a design the
type's own pre-existing doc comment had already implicitly promised
("cheap to clone... the `RelayClient` implementor itself must make cheap
to clone") without the compiler enforcing it. When a widened generic
bound seems to demand a per-call-site or per-`impl`-block fix, check
whether the *shape* of what every caller needs is already the same shape
a sibling supertrait (here, `Env`) already carries — the fix usually
belongs on the trait.

**2. A regex-driven `tokio::time::Instant::now()` → `self.env.now()`
conversion is only safe inside a function that actually has `&self` —
and a file can have both kinds without any visual signal.** `write_path.rs`
has 13 methods on `impl<E: Env, R: RelayClient> ClientCtx<E, R>`, but 7 of
them (`poll_probe`, `cp_batch_local`, `cp_batch_propose`, `cp_put_local`,
`cp_delete_local`, `cp_kind_local`, `seed_rows_local`) are deliberately
`Self`-free — they take `leader: &CpGroup<E>` as their first parameter
instead of `&self`, a design choice from ADR 0061 rung C5 step 1 (they're
called from `dynamo.rs`'s free functions, which never construct a
`ClientCtx` reference just to reach them). A single crate-wide
`tokio::time::Instant::now()` → `self.env.now()` regex substitution
compiled clean in `read_path.rs`/`forwarding.rs`/`txn_coordinator.rs`
(every method there takes `&self`) but produced 14 `error[E0425]: cannot
find value \`self\`` sites in `write_path.rs` alone — caught immediately
by `cargo build`, never by re-reading the diff, since the substituted
line looks completely ordinary in isolation (`let deadline =
self.env.now().saturating_add(CLIENT_TIMEOUT);` reads fine whether or not
`self` is in scope three lines up). The fix was `leader.env()` instead
(`CpGroup<E>`'s own private `env(&self) -> &E` accessor, already visible
to any descendant module per the standing privacy-widening lesson) — a
mechanical, per-call-site substitution once the seven functions were
identified, but identifying them required reading every function
signature in the file first, not trusting the regex to have been
scoped correctly. **The general rule this reinforces**: a `sed`/regex
conversion pass over a body of `&self`-shaped code must never be trusted
crate-wide without an immediate `cargo build` right after — and the build
output's line numbers, not a visual diff, are what actually finds the
functions the assumption didn't hold for.

Two smaller findings from the same rung, worth a shorter note: a
`Self`-free associated function (`ClientCtx::cp_kind_local`, called
`ClientCtx::cp_kind_local(leader, ..)` with no `self` argument at all)
gives type inference nothing to pin a newly-added generic parameter on —
`error[E0283]: type annotations needed`, fixed with an explicit turbofish
at each of its three call sites, exactly as the compiler's own suggestion
said; and a pattern match against a type alias only resolves generically
when the alias itself is imported *as the generic item* — `crate::
ControlHandle::Local(raft)` (this crate's own `ProdEnv`/
`AnimusdRelayClient`-bound alias) fails to match a `ControlHandle<E, R>`
value for generic `E`/`R` with a plain `E0308`, confirmed with a two-line
standalone `rustc` snippet before touching the real file, and fixed by
importing `animus_node::control_handle::ControlHandle` (the generic enum)
directly under the same name in the one file that needed the generic
match, leaving every other file's `crate::ControlHandle` (still matched
against a concrete value there) untouched.

## Driving a production `E: Env`-generic type under `SimEnv` for the first time in a new crate: `block_on` hangs, a receiver borrow can conflict with a later move, and clippy catches what `cargo test` can't (ADR 0061 Phase C's closing rung)

Building the first `SimEnv`-driven `ClientCtx<SimEnv, _>` harness in
`animusd`'s own tests (no crate move — see that ADR's seventh/eighth
2026-08-28 amendments) surfaced three small, generalizable gotchas, all
likely to recur verbatim when Phase D's `SimCluster` does the same thing at
multi-node scale.

**1. `futures::executor::block_on` silently hangs over any future that can
genuinely `.await` an `env.sleep()` — there is no error, just a stuck test
process.** `ClientCtx::cp_kind_write_raw`/`cp_get` both contain retry loops
that `.await self.env.sleep(..)` (the confirm-poll backoff, the
route-resolution wait). `block_on` polls a future on the *calling* thread
with no relationship to `Simulator`'s own cooperative executor — nothing
ever advances the virtual clock or fires the pending timer, so a future
that would resolve instantly under real time never resolves at all here.
The fix (already established practice in `animus-cp-data`'s own tests, but
easy to reach for the wrong tool anyway when a codebase's "obvious" way to
run one `async fn` synchronously is `block_on`): spawn the future onto the
env (`env.spawn_task`) and drive it forward with `Simulator::run_for`/
`run_until`, capturing the result through a shared `Arc<Mutex<Option<T>>>`
slot read back out afterward. Any first-time `SimEnv` harness for a type
that wasn't written with `SimEnv` in mind from the start should budget for
finding at least one call site that can genuinely suspend, and reach for
this shape by default rather than trying `block_on` first and debugging the
hang.

**2. A method call's receiver-borrow and a later argument's whole-value move
of the same struct conflict, even when the method itself takes `&self`.**
`ctx.env.spawn_task(async move { .. ctx.cp_kind_write_raw(..) .. })` fails
to borrow-check: evaluating the receiver `ctx.env` borrows `ctx` for the
duration of the call, and the `async move` block constructed as the
*argument* to that same call tries to move the whole `ctx` — a conflict the
error message reports as a move-while-borrowed on `ctx`, not obviously
pointing at "the receiver and the argument are both touching the same
value." The fix is mechanical once recognized: bind the needed field to its
own local (`let env = ctx.env.clone(); env.spawn_task(async move { ..
ctx.cp_kind_write_raw(..) .. })`) so the move and the borrow no longer share
an expression. `animus-node`'s own sim tests already do this
(`let loop_env = node.env().clone(); loop_env.spawn_task(..)`) — worth
recognizing as a named idiom rather than rediscovering the borrow-checker
message each time a new harness hits it.

**3. `cargo test` proves the code compiles and the assertions pass; it does
not run clippy, and a fresh sim test's own literals are exactly the kind of
thing clippy catches that a passing test run won't.** A hand-picked hex seed
literal grouped for readability (`0x51_4E_0001`, meant to loosely spell a
mnemonic in hex) passed `cargo test -p animusd --lib` clean but failed
`cargo clippy -p animusd --all-targets --all-features -- -D warnings` on
`clippy::unusual_byte_groupings` (hex digits must group in fours from the
right). Unsurprising once stated, but worth the reminder that finishing a
rung's gate list in test-only order (test, then clippy) can produce a false
sense of "done" after step one — the validation gate's own ordering (fmt,
clippy, build, test) exists partly to catch exactly this before a test pass
is mistaken for a full green gate.

**A fourth, purely operational finding, worth recording beside the existing
"`cargo build --workspace --all-targets` can exhaust disk long before
`cargo clippy --workspace --all-targets` does" entry above**: the same gap
holds at single-crate scope, not just workspace scope. `cargo build -p
animusd --lib --tests` (compiling and *linking* all ~100 of this crate's
test binaries in one invocation) exhausted this session's disk mid-build,
while `cargo clippy -p animusd --all-targets --all-features -- -D warnings`
— checking the exact same set of targets — completed in well under a
minute using only a few GB, because clippy (like `cargo check`) never links
a final binary for any target it checks. When a task's own validation gate
calls for a crate-scoped `--all-targets` clippy run, it is safe to run as
written even under this repo's documented disk constraints; a
`--all-targets` *build* or *test* invocation for the same crate is the one
to keep scoped down (`--lib`, or one `--test <name>` at a time).

## A read site with a `tablets_for_table`-shaped scan must filter to the current authoritative owner explicitly (ADR 0059/0050, `BeginBackup` pinning)

`BeginBackup`'s apply arm derived its `pinned_tablets` list from
`self.tablets_for_table(table)` with no state filter — reasonable-looking,
since for an in-place-split table (ADR 0058, the default) every row
`tablets_for_table` returns really is the current authoritative owner of
its slice of the key range. It stops being true the moment a *copy-based*
split (ADR 0050, still selectable via `--split-mode copy`) is mid-flight:
`BeginSplit` mints both children as `Building` rows in `meta.tablets`
immediately, long before `CutoverSplit` ever flips authority to them, and
the parent stays `Splitting` — still serving every read/write — for the
whole build/tail window. A `tablets_for_table` scan during that window
returns THREE rows covering one range at once, and a caller that doesn't
know to filter treats the `Building` children as if they were as
authoritative as the parent: pinning (or otherwise processing) all three
double-counts the range and points at rows that are both unroutable
(`topology::tablet_for_key` excludes `Building`) and an incomplete copy.
The general rule this generalizes to: any new read site built on
`tablets_for_table` (or an equivalent whole-table tablet scan) must decide,
explicitly, which `TabletState`s are "the current authoritative owner" for
its purpose — for split lineage that's `Active`/`Splitting`, never
`Building` — rather than assuming the scan itself already excludes
not-yet-live rows. The existing `is_relayable_command`/`cp_serve_forwarded`
"grep every gating match site" lesson above is the write-side sibling of
this same failure mode: a set-returning primitive whose membership changes
mid-lifecycle needs every caller to state its own filter, because the
primitive can't know which callers only want the settled members.

## `Simulator::stop` does not clear the `crashed` flag `Simulator::crash` set — composing them silently mutes the reconstructed node forever (quiescence corpus fault-primitives wiring)

Writing a crash-based test cell for `animus-cp-data/tests/quiescence.rs`
(ADR 0061 Decision 3, giving `DiskConfig::torn_tail_on_crash`/
`corrupt_on_crash` real teeth — see the next entry) needed the genuine
process-restart shape the sibling `raftkv` corpus's `StopRestart` nemesis
already uses: `Simulator::stop` (kills tasks + volatile state, keeps
durable disk) followed by a fresh `RaftKvNode::start` on the same node id,
which recovers from the durable WAL. But `torn_tail_on_crash`/
`corrupt_on_crash` only fire inside `Simulator::crash`, not `stop` (see the
"a crash-only fault has zero test teeth" pattern this composition exists to
avoid) — so the natural-looking sequence is `crash` (to tear/corrupt the
un-synced tail) → `stop` (to kill the task so a fresh one can be
constructed) → reconstruct. That sequence silently breaks: `crash` inserts
the node into `Simulator`'s shared `crashed: BTreeSet<NodeId>`, and `stop`'s
own doc says outright "Unlike `crash`, this does not mute or set the node
`crashed`" — meaning it also doesn't **clear** it. Every message to or from
a node still in `crashed` is dropped (`DROP ... (crashed)` /
`DROP ... (sender-crashed)` in the trace), so the freshly reconstructed
node — despite having brand-new tasks and a live env — never sends or
receives a single message for the rest of the run. This is genuinely quiet:
no panic, no error, just a replica that sits at its pre-crash term with
`engine_applied_index() == 0` forever, which reads exactly like "the
recovered WAL was empty" rather than "the network is silently muted" — the
first draft of this test's own failure looked like a WAL-recovery bug for
several debugging passes before an `eprintln!` of `is_leader`/`term`/
`engine_applied_index` plus the trace tail made the all-drops pattern
obvious. The fix is one extra call: `sim.crash(id); sim.stop(id);
sim.restart(id);` (clears `crashed`; `restart`'s own re-arm step finds
nothing to re-arm, since `stop` already removed every task it owned) —
*then* construct the fresh node. **General rule**: two fault primitives
that individually look composable (each has its own clear, narrow doc)
should still be traced through each other's state machine before combining
them in a new way no existing test does — `crash`+`stop` is exactly the
kind of pairing where each method's doc is accurate in isolation but their
combined effect on a third piece of shared state (`crashed`) is only
obvious from reading both source bodies side by side, not from either doc
comment alone.

## `corrupt_on_crash`'s single-byte WAL corruption can produce a syntactically-valid-but-wrong `HlcTimestamp` that hard-panics a later apply — a fresh, reproducible instance of the "no per-record WAL checksum" gap (quiescence corpus fault-primitives wiring)

Composing `DiskConfig::set_fsync_lie_prob` (accumulate several writes'
worth of un-synced WAL bytes on a live leader) with a later
`torn_tail_on_crash`+`corrupt_on_crash` crash and a genuine restart (see
the entry above) — the only way to give either disk-tearing field real
teeth — reliably reproduces a **hard `assert!` panic**, not just stale or
wrong served data: `animus_cp_data::assert_ts_monotonic` (`lib.rs`, ADR
0018 §2), "HLC ts ... did not strictly exceed the last applied ... the
witnessing chain is broken," once the recovered replica catches up and
applies an entry past the corrupted record. Isolated directly: the
identical scenario and seed with `torn_tail_on_crash` alone (no corruption)
converges cleanly (`engine_applied_index` matches the honest survivors'
exactly); adding `corrupt_on_crash` to that same seed panics the whole test
process every time. The mechanism is exactly what `WalRecord::decode`'s own
doc and the sibling `raftkv` corpus's `wal_fault_disk_config` doc already
name as a residual gap, now confirmed to reach further than either
anticipated: the Raft WAL's on-disk record framing is plain
newline-terminated `serde_json` with **no per-record checksum**, so a
single flipped byte that happens to land inside a numeric JSON field (here,
a packed `HlcTimestamp`) can produce a record that still **decodes
successfully** — just with the wrong value — rather than the torn/
unparseable trailing line `decode` is built to tolerate. The already-known
version of this gap (the sibling `raftkv`/`txn` corpora's own documented,
unfixed `NetConfig::set_corrupt_prob` finding, an allocator-abort `SIGABRT`
in `animus-cp-data::codec`'s wire decoder) is the *wire* half of this same
root cause; this is the *WAL* half, a different call site with a different
failure shape (a hard-panicking safety assert instead of an OOM abort) but
the identical missing-checksum cause. **Handled the same way the wire half
already is**: excluded from the corpus cell (`corrupt_on_crash` stays
armed-off, `torn_tail_on_crash` alone still gives the cell real, working
teeth), documented in full in the test's own doc comment, and left as a
named, unfixed, real finding for its own follow-up issue/PR rather than
folded into the corpus PR — a fault primitive that reliably hard-panics the
process is out of scope for an ambient corpus cell's assertions the same
way `set_enospc_prob`/`set_error_prob` already are for a different reason
(they hit this crate's own `persist_wal` `halted` assert). **General
lesson**: when a repo already has one documented, unfixed "no checksum on
this framing" finding for one call site of a shared codec/record format,
treat every *other* call site of that same un-checksummed framing as a
credible candidate for the identical class of bug before assuming a
freshly-discovered hard panic under `corrupt_on_crash`/`set_corrupt_prob`
is a coincidence or a test-harness mistake — reach for isolating which
disk/net-fault knob actually causes it (toggle one off, keep the seed
fixed, re-run) before suspecting the new test code itself.

## `DiskConfig::torn_tail_on_crash`/`corrupt_on_crash` were never actually exercised against a `RaftCore`-backed WAL before (ADR 0061 Decision 3, `stream_lineage_corpus.rs` fault wiring)

Both fields are documented and unit-tested in `animus-sim` itself
(`tests/disk_faults.rs`) and exercised against `animus-storage`'s
`LsmEngine` (`tests/lsm_disk_faults.rs`), but grepping the whole workspace
for either name turned up no use anywhere in `animus-control` or
`animus-cp-data` — meaning the shared Raft consensus WAL format
(`animus_control::persist::{WalRecord, PersistedState}`, used by *both* the
control plane's own `RaftCore<MetaCommand, Metadata>` and every per-tablet
`RaftKvNode`'s `RaftCore<KvCommand, KvState>`) had never actually been
crashed-and-torn under simulation before this corpus's own
`torn_tail_crash_survives_a_true_restart` cell. Worth confirming *why* it's
safe before assuming so: `PersistedState::decode`/`decode_tagged`
(`persist.rs`) frame the WAL as newline-delimited JSON and recover via
`bytes.split(b'\n').filter_map(|line| serde_json::from_slice(line).ok())`
— any line that fails to parse, not merely a strict trailing partial one,
is silently dropped. This is a *more* permissive recovery contract than
`animus-storage`'s own length-prefixed-plus-CRC WAL format (`lsm/wal.rs`),
which goes out of its way to distinguish a genuinely torn trailing record
from mid-file corruption by position (`wal_resync_point`) and hard-errors
on the latter — but it's still sound for this WAL's own recovery
discipline (an un-replayed record's effect was, by definition, never acted
on), so `torn_tail_on_crash`/`corrupt_on_crash` compose safely with it with
no risk of a hard panic or silent divergence, unlike the LSM format's own
stricter contract. The general check before wiring either fault into a new
corpus: confirm which WAL/durable-record codec the crashed component
actually uses, and whether that codec's own recovery already documents (or
can be shown to have) a total, non-panicking answer for "this record failed
to parse" — not just a trailing-partial-write answer, since `corrupt_on_
crash` can in principle land its flipped byte anywhere inside the retained
(non-dropped) tail, not only in what would otherwise have been a clean
trailing partial line.
## A crash-only disk fault has zero test teeth unless something later reads the crashed node's own post-crash state back (`pitr_fault_corpus.rs`, ADR 0061 Decision 3 wiring)

Wiring `DiskConfig::torn_tail_on_crash`/`corrupt_on_crash` into a new
`scenario_wal_torn_on_crash_kill_sealing_leader` cell surfaced a design trap
worth naming on its own, separate from the already-documented `crash` →
`restart` → `stop` sequencing gotcha (see the entries this generalizes from,
found by the sibling backup-corpus PR): in this corpus,
`current_open_pitr_epoch` — the very value the new cell exists to protect —
is derived **only** from the hand-scripted `Metadata` struct the test drives
directly with `.apply()` calls, never from anything read off the crashing
node's own Raft WAL or storage engine. Copying `scenario_kill_sealing_leader`
verbatim (crash a node, fail over to the two survivors, never look at the
crashed node again) and simply turning on the torn-tail disk config would
have compiled, run green, and proven **nothing**: the tear sits on a file
nothing in the scenario ever reads again. The fault only gets real teeth once
the crashed node is brought all the way back — `crash` → `restart` → `stop` →
a fresh `RaftKvNode::start_hosted` on the same id/engine — and made to
participate in a *further* round of writes and a second seal, so its own
recovery from the torn/corrupted tail can actually influence what the next
`pending_changes()` scan (and therefore the next epoch number) sees. The
general check this generalizes to: before wiring a disk- or network-fault
primitive into an existing scenario as a "near-copy plus one fault call",
trace where the property-under-test's own inputs actually come from — a
fault that lands on state nothing downstream ever reads is a no-op with a
green checkmark, not a stronger test.

## Getting a torn/corrupted WAL tail ahead of a *fresh-process* restart needs `crash` → `restart` → `stop`, not `stop` alone (`backup_fault_corpus.rs`, ADR 0061 Decision 3)

Wiring `DiskConfig::torn_tail_on_crash`/`corrupt_on_crash` into a
`RaftKvNode`-level corpus scenario that also wants a genuine fresh-process
restart (the `sim.stop` + a brand-new `RaftKvNode::start_hosted` on the same
id/engine idiom `capture_driver_node_crash_restart`/
`raftkv_linearizable.rs`'s `StopRestart` nemesis both use) runs into a real
mismatch between the two primitives: the tear/corrupt logic lives **only**
inside `Simulator::crash` — `Simulator::stop`'s own disk handling
unconditionally clears any buffered-but-unsynced bytes with no tear
consideration at all (see both methods' doc comments), so a scenario that
only ever calls `stop` can set `torn_tail_on_crash: true` all it wants and
never actually exercise it. Reaching first is not enough either: `crash`
also inserts the node into `Simulator`'s `crashed` set, which both the
send and deliver paths check permanently — a node left in that set (nothing
but `restart` ever removes it) has every future send and delivery silently
dropped, including everything a freshly-constructed `RaftKvNode` on that
same id would try to do, which reads as the fresh node being inexplicably
unable to participate in Raft at all rather than as a leftover crashed-flag
bug. The composed fix, safe because nothing drives the executor between
these three synchronous calls (so `restart`'s re-armed old tasks are
removed by the immediately-following `stop` before they're ever re-polled):
`sim.crash(node)` (applies the configured tear/corruption to whatever was
genuinely buffered at that instant), `sim.restart(node)` (clears the
`crashed` flag — its task re-arming is a harmless no-op here), `sim.stop
(node)` (drops those tasks, keeps the now-torn durable state), *then*
construct the fresh node. Generalizes beyond this one corpus: any scenario
combining a `DiskConfig` crash-only fault with a `stop`-based fresh-restart
idiom needs this three-call sequence, not a straight substitution of
`crash` for `stop`.
## Wiring fault primitives into a per-group corpus needs `set_*_for` scoping, and running a corpus at its own nightly depth for the first time can surface an unrelated pre-existing failure (ADR 0061 Decision 3, `txn_serializable.rs`)

Porting `raftkv_linearizable.rs`'s ADR 0061 Decision 3 fault-primitives
wiring (`DiskConfig::set_fsync_lie_prob`, a compound `NetConfig`,
`Simulator::set_clock_drift_for`) to `txn_serializable.rs` needed one real
adaptation, not a copy-paste: the raftkv corpus is a single Raft group, so
its `Nemesis::FsyncLie` sets `DiskConfig` **globally**
(`Simulator::set_disk_config`); the txn corpus has 3 *independent* tablet
groups sharing one `Simulator`, so a global disk fault would lie to every
group at once regardless of which one a scenario means to target —
`set_disk_config_for`/`set_clock_drift_for` (per-node, not the bare
`set_disk_config`/no per-node clock-drift equivalent needed by the
single-group corpus) are the right primitives once a corpus has more than
one independently-faulted unit sharing a `Simulator`. The general form:
when porting a fault-wiring pattern from a single-topology corpus to a
multi-topology one, re-derive which scope (`_for`, whole-`Simulator`, or
per-link) is correct for the *new* corpus's own shape — don't assume the
source corpus's scope choice transfers, since a single-group corpus never
had a reason to need anything narrower than global. `heal_all` resetting
these per-node overrides individually (not just the global `NetConfig`) is
the same "a fired fault must not outlive its window" trap the raftkv PR's
own `heal_all` already documents, just for a `_for`-scoped fault instead of
a global one — resetting the global default does nothing for a per-node
override that was never routed through the global setter at all.

Separately: `ANIMUS_TXN_SEEDS=40` (the depth `corpus-deep.yml`'s existing
nightly tier already configures for this corpus) surfaced a **pre-existing,
unrelated** `check_kind_consistency` divergence on `main`, with no new
fault involved — `compound_lossy_and_anchor_kill_s25` (seed
`8035380114809936673`, a `Lossy` + `AnchorLeaderKill` cell) reproduces
identically on the unmodified checkout. This corpus's own default depth is
1, so nightly's `=40` had evidently not actually been run to completion
against this specific check before — a fault-primitives PR that happens to
validate at that depth is not the same thing as nightly CI having done so.
The general lesson: a corpus's documented "held green at depth K" claim is
only as trustworthy as the last time someone actually ran it at K and
looked at the result — treat validating a change at a corpus's own stated
nightly depth as an opportunity to check that claim is still true, not just
a formality for the change at hand, and when it turns out not to be, that
is a real, separate finding (this repo's "incidental bug gets its own PR"
convention) — report it plainly rather than quietly dropping the depth or
excluding the offending cell to get a clean run.

**Resolved (issue #488):** this divergence was diagnosed as a test-harness
bug, not a production one — `Topology::start` shared one `MemoryEngine`
across a group's 3 replicas; see the Testing section's "cheap to clone,
clones share state" entry for the full mechanism and the fix. The corpus
is green at `ANIMUS_TXN_SEEDS=40` (including `check_kind_consistency`)
with each replica given its own engine.

## Wiring `animus-sim`'s fault primitives into a real corpus for the first time (ADR 0061 Decision 3, `raftkv_linearizable.rs`)

`animus-sim`'s deepened fault vocabulary (`NetConfig::set_duplicate_prob`/
`set_corrupt_prob`/`set_heavy_tail_prob`, `DiskConfig::set_fsync_lie_prob`/
`torn_tail_on_crash`/`corrupt_on_crash`) had been proven correct in its own
meta-tests (`animus-sim/tests/net_faults.rs`, `disk_faults.rs`) but never
actually driven through a real protocol-correctness corpus. Wiring it into
the flagship raftkv linearizability corpus surfaced two lessons, one a
harness trap and the other a real, still-open production finding.

**1. A `Nemesis` that sets a global `Env` fault config for "the rest of the
run" must be reset by the runner's heal step, or it silently keeps firing
past its own scenario's intended fault window.** Every pre-existing
`Nemesis` variant in this corpus only ever touched partitions/crashes
(healed by `Simulator::heal`/`restart`) or `NetConfig` (reset by
`heal_all`'s pre-existing `self.sim.set_net_config(NetConfig::default())`).
The moment a variant (`FsyncLie`) set a global `DiskConfig` for the first
time, `heal_all` needed the identical reset
(`self.sim.set_disk_config(DiskConfig::default())`) or the lie would keep
firing for every scenario cell that happens to run after it in the same
process — invisible in this corpus's own harness (each scenario gets a
fresh `Simulator`, so nothing here actually leaked), but exactly the trap a
future corpus sharing one `Simulator` across cells (or a future `Nemesis`
composing several fault-window phases within one scenario) would fall
into silently. General rule: every new global-fault-setting `Nemesis`
needs its own explicit line in whatever "return to steady state" step the
harness already has for the fault classes that came before it — don't
assume the existing reset call already covers a fault dimension it
predates.

**2. A wire codec that "never panics" can still abort the whole process —
and this corpus caught a real instance of it within minutes of running at
depth.** The task briefing (based on a prior investigation) asserted
`NetConfig::set_corrupt_prob` was "verified safe" for this corpus because
`animus_cp_data::codec::decode_wire` is bounds-checked and its
`KvWire`/`RaftMsg` match sites `warn!`/drop on a decode `Err` rather than
panicking. That's true for the primitive per-byte reads (`Cursor::u8`/`u32`/
`u64`/`bytes` all bounds-check via `take()` before ever touching memory) but
false for a whole class of *array* decoders: `read_raft`'s `AppendEntries`
arm (`codec.rs`) reads an untrusted `n: u32` entry count straight off the
wire and calls `Vec::with_capacity(n as usize)` **before** a single one of
those `n` entries has been validated against the cursor's remaining bytes.
A single bit-flip landing inside those 4 length bytes can turn `n` into
something close to `u32::MAX`; since each `LogEntry<KvCommand>` is a
multi-field struct, `with_capacity` then requests on the order of a
terabyte, which Rust's allocator failure path (`handle_alloc_error`)
**aborts the process on** — not a catchable panic, and nothing a
`std::panic::catch_unwind` or a `Result`-based decode error contract can
intercept. Reproduced deterministically at `ANIMUS_RAFTKV_SEEDS=20`:
`chaos_early_3_s09`, seed `422907917505132688`, `SIGABRT`. Grepping
`codec.rs` for the same `let n = c.u32()?; ... Vec::with_capacity(n as
usize)` shape found **at least a dozen more sites** (`read_kind_writes`,
`read_change_logs`, `TxnStage`'s `puts`/`conditions`, `SplitTablet`'s
`replicas`, `TxnRecord`'s `spans`/`conditions`, `SeedBatch`'s `rows`, and
`read_raft`'s own `AppendEntries`/`InstallSnapshot`-adjacent arms) — this is
a **systemic gap in the codec's array-decoding pattern**, not a one-off in
one match arm. Deliberately **not fixed as part of this corpus PR** (the
repo's own "an incidental bug gets its own PR" rule, root `CLAUDE.md`
Conventions) — `Nemesis::Chaos` ships without `set_corrupt_prob` instead
(see its own doc in `raftkv_linearizable.rs`), the same "primitive excluded
from this corpus, real fix deferred to its own crate/PR" treatment this
corpus already gives `DiskConfig::set_enospc_prob`/`set_error_prob` for the
identical hard-abort-vs-graceful-`Err` reason. **The general lesson**: when
a decode path's own doc (or a fault-injection plan) claims "bounds-checked,
never panics," verify that claim against every *array*-shaped decoder
specifically — a scalar bounds check (`take(n)` before slicing) is not the
same guarantee as a *count*-driven pre-allocation being bounded, and the
difference is invisible until something (a corrupted wire byte, a
malformed peer, a fuzzer) actually supplies an adversarial count. The real
fix (tracked as a follow-up, not implemented here) is straightforward:
either drop the `with_capacity` hint for array decoders entirely (a
`push`-per-iteration loop already bounds itself correctly via `Cursor::
take`'s own check, just without the allocation being *pre*-sized) or cap
the hint at some cheap function of the cursor's actual remaining byte
length before ever calling `with_capacity`.

## A nightly CI job's sequential steps without `continue-on-error` let the first failure hide every corpus after it (`corpus-deep.yml`)

`corpus-deep.yml` ran all ten deep-seed fault-injection corpora as
sequential steps inside one job, with no `continue-on-error` and no
`if: always()` on any step. GitHub Actions' default behavior for a
plain sequential step list is: the first failing step aborts the job and
skips every step after it. Since the multi-tablet transaction corpus
(ADR 0018) happened to be listed first and was failing a real assertion at
nightly depth (`ANIMUS_TXN_SEEDS=40`, `check_kind_consistency` on
`compound_lossy_and_anchor_kill` — see the sibling lesson on that bug once
filed), every one of the other nine corpora was silently skipped on every
single nightly run since the workflow's creation — 13 consecutive runs,
all reporting "failure" for the same first-step reason, without a single
one of the other nine corpora ever actually executing at depth. The
workflow's own `conclusion: failure` status looked like a signal, but a
human (or an agent) glancing at "corpus-deep is red" would reasonably
assume the *listed* failing step was the whole story, not that nine other
tests never ran at all — the gap was invisible without opening the job's
step list and noticing the `skipped` conclusions.

The general lesson: a CI job whose value comes from *reporting on N
independent things* (here, ten unrelated corpora) must not let any one of
them gate whether the others get a chance to report. `continue-on-error:
true` + a per-step `id:` + a final `if: always()` aggregation step that
turns the recorded per-step outcomes back into one job-level pass/fail is
the fix — CI still goes red on a real regression, but only after every
corpus has actually run and had its result recorded, so `skipped` never
silently substitutes for a genuine result. Worth checking any other
multi-step job in this repo's CI for the same pattern before assuming its
"only some things are failing" read of a red run is complete.

## An untrusted length-prefix pre-sizing a `Vec` is a distinct DoS class from an unbounded-panic one — bounds-checked reads don't close it (`animus-cp-data::codec`, `animus-storage::lsm`)

`codec.rs`'s own doc comment claimed decoding untrusted wire input was
"bounds-checked... never panics," and every individual field read really
was: `Cursor::take` checks `pos + n <= bytes.len()` before every slice, so
a truncated or malformed frame always surfaces as a clean `Err`, never an
out-of-bounds panic or a raw slice-index abort. What that guarantee does
**not** cover is a different failure mode entirely: about a dozen call
sites read an untrusted `u32`/`u64` **count** off the wire (an
`AppendEntries` entry count, a `KindBatch`/`TxnStage` write count, …) and
handed it straight to `Vec::with_capacity(n as usize)` **before** a single
one of its `n` elements had been validated against the remaining buffer.
For a multi-byte element type (`(Vec<u8>, Vec<u8>)`, a `LogEntry<KvCommand>`,
…), a single corrupted or adversarial length-prefix byte setting `n` near
`u32::MAX` turns into a request for hundreds of GB to several TB. Rust's
global allocator does not return an `Err` or unwind a panic for an
allocation that large or that exceeds `isize::MAX` — it calls
`handle_alloc_error`, which **aborts the whole process** (`SIGABRT`), not a
catchable failure at any level above it. Reproduced live: corrupting
`read_raft`'s `AppendEntries` entry-count field crashed
`cargo test -p animus-test --test raftkv_linearizable` outright — the whole
test binary, not one failing assertion. Since this reads off real
inter-node Raft/Kv wire traffic, it was a process-wide denial-of-service
vector reachable from any peer able to send a corrupted or crafted message
to a node, not a theoretical concern.

**The fix is not "validate `n` before allocating"** — there is no cheap way
to validate an element *count* against a buffer when each element's own
encoded size is variable (a byte string, a nested struct, …); the only
honest validation is decoding the elements themselves, which is exactly
what the loop right after the allocation already does. The fix instead
**caps the requested capacity**, never the actual number of elements
decoded: `Vec::with_capacity(n.min(1 << 20) as usize)`. A legitimate
message's cost is completely unchanged (a real `n` is always far below the
cap); a hostile/corrupted `n` can now request at most `1 << 20 *
size_of::<T>()` bytes up front — large enough to never matter for any real
message, small enough to never abort — and the *actual* loop below still
only ever pushes as many elements as the buffer genuinely contains, since
each element read still fails with a graceful `DecodeError` the moment the
buffer runs out (an attacker gets at most O(remaining buffer bytes) worth
of pushes/reallocations, never O(n) for an unbounded `n`). This was
already the house convention in two sibling decoders in the same crate
(`backup.rs`'s `declared.min(1 << 20)`, `segment.rs`'s
`declared_count.min(1 << 20)`) that simply hadn't been applied to
`codec.rs` yet — worth grepping for by name (`with_capacity(n as usize)`,
`with_capacity(len as usize)`, `with_capacity(count as usize)`, and their
`u64`/`usize`-cast siblings) whenever adding or reviewing a new hand-rolled
wire/WAL/manifest decoder, since nothing about this shape triggers a
compiler warning or a clippy lint — it looks like an ordinary, idiomatic
size-hint optimization.

**The same shape existed one layer down, in `animus-storage`'s WAL record
decoder and its manifest decoder** (`lsm.rs`) — found only by a deliberate
workspace-wide grep for the pattern once it was clear `codec.rs` wasn't the
only hand-rolled length-prefixed decoder in the tree. The WAL record
decoder's own count reads happen only after a CRC-32 check on the whole
frame (`try_parse_wal_frame`), which makes the "one corrupted byte survives
undetected" case astronomically unlikely for ordinary bit rot — but CRC-32
is not cryptographically adversary-resistant, and the shape is identical
either way, so it got the identical `.min(1 << 20)` cap as defense in
depth. The **manifest** decoder has no CRC protecting it at all (a plain
`Disk::replace`-swapped file), so a corrupted on-disk manifest byte was a
real, not merely theoretical, instance of the same abort — fixed
identically. `animus-cp-data`'s `txn.rs` (the value-envelope decoder) and
`split.rs` (the in-place-split fork marker) had the same shape too, in code
whose own doc comments describe it as "this crate only ever reads back
what it itself wrote" (a hard-bug, not a recoverable-error, doctrine) — but
both are engine-marker values that ride inside a tablet's `InstallSnapshot`
image, which is untrusted wire content until it's actually been applied;
"this crate wrote it" is true of the *normal* path, not a guarantee against
a corrupted/adversarial snapshot from a peer, so both got the same cap
rather than being left on the "assumed trusted" side of the line.

**General rule for reviewing (or writing) any hand-rolled length-prefixed
decoder — wire, WAL, or manifest — going forward**: bounds-checking every
individual read (via a `Cursor`-style `take` that checks the remaining
buffer) is necessary but not sufficient for allocation safety. Grep
specifically for `with_capacity` fed by any value that was itself just read
from the buffer being decoded, and cap it — dropping the pre-allocation
entirely (falling back to `Vec::new()`/organic `push` growth) is an equally
valid fix where performance doesn't matter enough to justify the extra
literal; this repo's own precedent leans toward the capped form since it
preserves the fast path's zero-realloc behavior for every legitimate
message. A doc comment claiming a decoder "never panics" or is
"bounds-checked" should be read as a claim about individual reads only,
never as a claim about allocation safety, unless it says so explicitly —
`codec.rs`'s own doc comment now does, and is the template for any other
decoder's doc that makes a similar claim.

## A depth knob's existence in `CLAUDE.md` doesn't mean CI ever runs it deep — `corpus-deep.yml` needs its own explicit entry per corpus (2026-08-30, `ANIMUS_RAFTKV_SEEDS`)

`raftkv_linearizable.rs` — the flagship Elle-checked linearizability corpus
for the per-tablet CP data plane (ADR 0017) — had a documented
`ANIMUS_RAFTKV_SEEDS` depth knob (and a second, `ANIMUS_RAFTKV_LSM=1`, for
the `LsmEngine<SimEnv>` tier) held green at depth 20/×10 in
`crates/animus-test/CLAUDE.md`, but `.github/workflows/corpus-deep.yml`'s
nightly job never referenced either env var. Every push still ran the
corpus at its default depth (1 = the frozen byte-identical set) via the
normal `gates` job, so the deep-seed proof this corpus exists for was
simply never exercised in CI — the crate guide's "held green at depth N"
line documents a `cargo test` invocation someone ran by hand at some point,
not a standing CI guarantee. **A corpus's env-var depth knob and its
`corpus-deep.yml` matrix entry are two separate things that must both
exist** — adding the knob (and proving it works locally) is not the same
as wiring it into the nightly tier, and nothing fails loudly when the
wiring is missing; the gap is silent unless someone reads the workflow
file against the crate guide's knob table. When adding a new
fault-injection corpus with a depth knob, add its `corpus-deep.yml` step
in the same change (or a tracked follow-up), not "later." Also worth
naming: a corpus that gates an entire alternate code path behind an
off-by-default env var (`ANIMUS_RAFTKV_LSM=1`, the durable
`LsmEngine<SimEnv>` tier vs. the always-on `MemoryEngine` one) needs its
own nightly step distinct from that corpus's plain depth-scaled step, not
just a deeper seed count on the same invocation — the two exercise
different storage-engine code, not just "more of the same" scenarios.

## Promoting a hardcoded-seed-loop test to the house corpus shape: the seed's literal value was never load-bearing, and default-depth coverage is *expected* to shrink (`lsm_crash.rs`/`lsm_disk_faults.rs`, ADR 0061 rung B1)

Converting `animus-storage`'s two `LsmEngine` fault tests
(`tests/lsm_crash.rs`, `tests/lsm_disk_faults.rs`) from hand-rolled
`for seed in [0xA1u64, 0xB2, ...]` loops onto `animus_test::corpus`'s
standard `Scenario`/`seed_expand` shape surfaced two things worth stating
plainly, since both look like regressions at a glance and aren't.

**The specific magic-number seeds were never part of what the tests
prove.** Every scenario in both files asserts a generic invariant ("every
acked write survives a crash", "a corrupted block/record surfaces a clean
error, never silent loss") that must hold for *any* interleaving — none of
them compare against an expected value computed from that particular seed.
So replacing `[0xA1, 0xB2, 0xC3, 7, 42, 1337]` with `corpus::name_seed(name)`
per newly-named cell changes which specific interleavings get exercised but
proves exactly the same thing; there was no need to preserve the literal
hex constants, and doing so would have fought the house doctrine ("a
scenario's seed is always a deterministic function of its own name," see
`animus-test/src/corpus.rs`'s module doc) for no benefit. Where the
original loop crossed a real *structural* axis with its own seed list
(`torn_wal_tail_crash_recovers_all_acked_writes`'s `corrupt: bool`,
`corrupted_manifest_fails_open_cleanly`'s corruption `offset`), each
combination became its own named cell (`..._corrupt`, `..._offset_4`, …)
sharing one extracted body function — the axis is what deserves a name,
the seed list crossing it doesn't.

**Converting a file that unconditionally ran N seeds every push into a
corpus at default depth 1 is a deliberate reduction in per-push seed count,
not a coverage regression** — it's the entire point of the house corpus
doctrine, whose default is *always* 1 (see the root `CLAUDE.md`'s
knob table and every existing corpus in this repo), with seed-sweeping
depth pushed to `corpus-deep.yml`'s nightly tier (`=40`) instead of paid on
every push. Before this conversion, `injected_wal_errors_surface_and_
lose_no_acked_write` ran 6 seeds unconditionally on every `cargo test`;
after, it runs 1 by default and 40 nightly — fewer per-push runs, far more
nightly ones, exactly like every other corpus in the repo. A task framed as
"preserve current behavior/coverage, must not regress existing CI" is
about outcomes (the test still builds, still passes, still exercises the
same fault-injection mechanism), not about the literal per-push seed count
— conflating the two would mean no hardcoded-seed-loop test could ever be
promoted to the standard shape without also being read as a downgrade.
- **When a value can be produced two ways — an explicit config field and a
  positional/minted default — grep the field's read sites and make sure at
  least one fixture in the suite actually diverges the two.** The
  `--config FILE --node I` entry points bound each node under the minted
  `config::node_id(index)` (`"n{index}"`) instead of the config entry's own
  `id` field; every fixture in the repo built ids with the same minting
  convention, so `addrs.id == node_id(index)` held everywhere by
  coincidence and the wrong read was invisible. The first config with
  operator-style ids (`"{cluster}-{ordinal}"`) then failed in the most
  silent way possible: each node's *claimed* identity was absent from its
  own genesis voter set, `is_voter()` was false on every node, and no one
  ever *started* an election — nothing to log, nothing to time out, just a
  cluster that never elects. Regression:
  `crates/animusd/tests/config_node_identity.rs` (a config whose ids
  deliberately do not follow the minting convention).
- **An accept loop must never treat a transient `accept()` error as
  fatal.** `ProdEnv`'s listener task returned on any `accept()` error —
  one transient `EMFILE`/`ECONNABORTED` during a bootstrap burst and the
  node was permanently deaf to inbound connections while looking otherwise
  healthy (observed live: 2 of 3 nodes deafened during a DNS-lag
  bootstrap window). Retry with a short backoff instead; any future accept
  loop at a process boundary gets the same review scrutiny.

## A shared `CARGO_TARGET_DIR` across worktree sessions can silently serve a stale test binary missing newly-added `#[test]` fns — `--list` (or a forced `touch`) before trusting a count (2026-08-30, `pitr_fault_corpus.rs` merge validation)

Resolving PR #490's `docs/engineering-lessons.md` merge conflict and then
validating with `CARGO_TARGET_DIR=/home/user/animus-db-shared-target cargo
test -p animus-test --test pitr_fault_corpus` (as instructed, to avoid
duplicating multi-GB build artifacts across worktrees) first reported **"12
passed; 0 failed"** — a clean-looking green run — even though the checked-out
file on disk plainly had 17 `#[test]` functions (confirmed by `grep -c
'^#\[test\]'`) and the PR's own commit message and crate-guide entry both
say "17 tests". `cargo test`'s own summary line only ever counts what its
test binary actually contains; it has no way to notice that the binary
itself is stale, so a naive "did the count match expectations and did
everything pass" check would have reported a false clean bill of health
while silently missing the five tests this very PR exists to add
(`wal_fsync_lie_kill_sealing_leader`,
`chaotic_network_pitr_rollover`/`chaotic_network_idle_group_never_
proposes_a_pitr_seal`, `wal_torn_on_crash_kill_sealing_leader`,
`restore_to_random_second_under_clock_drift`) — the exact set most likely to
carry a real bug, being new. `cargo test -- --list` against the same
un-touched binary reproducibly showed the same wrong 12; `touch`-ing the
source file to force a fresh `rustc` invocation (visible via `-v`) made the
list jump to the correct 17, all passing. The mechanism was never fully
isolated (candidates include a leftover fingerprint/object from an earlier
build of an older revision of this same file at this same worktree path,
whose mtime cargo's fingerprint check treated as not-older-than the fresh
checkout's), but the shared-target-dir setup itself is exactly the
precondition that makes a stale-fingerprint hit both possible and likely: the
same crate/test-target combination gets rebuilt from this same worktree path
across many separate sessions over time, all landing in one directory tree
cargo's own dep-graph fingerprinting was never designed to be shared this
widely. **The general check going forward**: whenever a validation run's
result matters (a merge/rebase gate, a "should be N/N" acceptance check) and
`CARGO_TARGET_DIR` points at a directory shared across worktrees/sessions,
don't trust the bare pass count — run `cargo test -- --list` (or diff the
printed test names against a `grep -c '^#\[test\]'` of the file) at least
once to confirm the binary's test set actually matches the source before
reading a "N passed" line as proof of anything; a stale binary reports
green precisely because it silently tests less, never because it tests the
same thing and fails to notice a problem.
## A racing-proposers workload can't tell "won" from "lost" by presence alone — it has to confirm by content (`animus-control`'s `control_corpus.rs`, ADR 0061 rung B1 sibling)

Building `control_corpus.rs` — a new seed-depth corpus for `animus-control`'s
own machinery (the ADR 0038 apply task, the schema-catalog exclusivity
guarantee), modeled on `raftkv_linearizable.rs`'s harness shape — the
schema-race workload's first draft had each racer's confirm loop treat "the
table now exists" as its own success signal. That's the exact
`ProposeResult::Accepted`-isn't-apply-time-truth trap the entry above already
names, but with an extra twist specific to a **race**: here `Accepted` isn't
even ambiguous about *whether* the command took effect (a single proposer's
own straggler command, `CreateTablet`-fixture-style) — it's ambiguous about
*whose* content took effect, since `CreateTableSchema` rejects outright on an
existing name (first-committer-wins, not idempotent-on-identical the way
`RegisterNode`'s CAS is) and TWO different proposers can each see "yes, a
schema for this table now exists" as true. A confirm loop that stops
retrying the instant presence flips true will, for the losing racer, log a
false "I won" — exactly the durability check's "this confirmed effect must
survive" assertion firing on content that was never actually this
proposer's own. The fix: read the table's *actual* schema back and compare
it, structurally, against the exact value this proposer proposed —
`Some(existing) if *existing == schema => won`, `Some(_) => lost, stop
retrying (nothing left to retry against a name that already belongs to
someone else)`, `None => keep trying`. General lesson: **when a workload
races N proposers for one identity and the state machine's own accept rule
is "first-committer-wins, reject the rest" rather than idempotent-on-match,
"does the effect exist" is the wrong confirm predicate — it has to be "does
the effect that exists match MINE," or a losing proposer misreports itself
as a winner and a durability check built on that misreport is checking a
claim nobody should have made.**

## Don't copy a sibling corpus's engine-tiering generic ceremony without first checking whether the new node type is generic over the storage engine at all (`animus-control`'s `control_corpus.rs`)

`raftkv_linearizable.rs` (the explicit template `control_corpus.rs` was
told to copy the *architecture*, not the content, of) is generic over both
`E: Env` and `S: StorageEngine` because `animus-cp-data::RaftKvNode<E, S>`
itself is — its `EngineFactory<S>` type alias and the `Group<S>`/`Node<S>`
plumbing exist to let the corpus run the identical scenario set over both
`MemoryEngine` and `LsmEngine<SimEnv>`. A first draft of the new corpus
started copying that same `<S: StorageEngine>` shape onto `Group`/`Node`
before checking whether it was needed — it wasn't:
`animus_control::RaftNode<E>` is generic **only** over `E`; `start<S:
StorageEngine>(..)` is a generic *associated function*, not a type
parameter of `RaftNode` itself, so the engine type is erased the moment a
node is constructed and there is nothing for a second generic parameter to
thread through. Carrying the extra `<S>` ceremony over unused would have
meant a `PhantomData` or a spurious "this corpus supports engine tiers"
claim the harness never actually exercises. General lesson: **before
copying a template corpus's generic shape onto a new one, check whether the
concrete type the new corpus actually drives is generic the same way the
template's was — a sibling module's own genericity is a property of *that*
module's dependency, not a fixed feature of "how a corpus harness in this
repo looks."**

## Not every "the same id was confirmed twice" is a double-assignment — check whether the racing proposals were content-identical first (`animus-control`'s `control_corpus.rs`, `AllocatorRace`, PR② of the control-corpus stack)

Building `Workload::AllocatorRace`'s invariant #4 (allocator injectivity),
the first draft's `check_allocator_injectivity` had two parts: (1) a
content-aware sampler (`Shared::sample_tablets`) that flags a `TabletId`
observed with two *disagreeing* fingerprints, and (2) a second check that
every `TabletId` a client's own confirm loop reported as applied
(`confirmed_tablet_ids`) was pairwise distinct. Part (2) immediately failed
the fault-free baseline: every `AllocatorRace` client races
`MetaCommand::CreateTablet` for the identical shared table, so every
racer's proposal is byte-identical **except for the candidate tablet id**
(same table name, same range, same replica set). Before any proposal has
committed, several racers legitimately read the same stale
`next_free_tablet_id()` and each proposes with that same candidate id — and
once the tablet that actually lands carries that id, EVERY one of those
racers correctly observes "the tablet that now exists carries my own
candidate id" and calls `confirm_tablet_id`. That is not a bug: there is no
meaningful sense in which "whose literal `CreateTablet` call committed" is
distinguishable when the content besides the id is identical — multiple
racers correctly recognizing the identical, single, real assignment is
expected, not a double-assignment. The fix was to delete part (2) entirely
and rely solely on the content-aware sampler, which is strictly the
stronger and correct check (it only flags a *disagreeing* fingerprint for
the same id, never a repeated agreement). Contrast this with
`BeginSplit`'s own phase of the same workload, where each racer's split key
is deliberately distinct per proposer index — there, a "confirm by content"
check (the child's actual range boundary matching MY split key, not just
presence of my candidate ids) is exactly right, and is what the racing-
proposers lesson above (`won`-vs-`lost` by content, not presence) already
prescribes. **General lesson: when a race's confirm signal is "this
proposal's content == what committed," first ask whether every racer's
proposal *could be* content-identical except for the field the race is
actually about — if so, a raw "confirmed exactly once" assertion over that
field alone is checking a stronger, false property; the real invariant is
"no two DIFFERENT contents were ever attributed to the same identity,"
which only a fingerprint/content comparison (not an occurrence count) can
state correctly.**

## An open cross-plane fault-finding (issue #495, the shared WAL-corruption gap) does not automatically reproduce in every plane that shares the vulnerable codec — confirm per-plane before assuming (`animus-control`'s `control_corpus.rs`, PR②)

Issue #495 is a confirmed, reproducible hard panic in `animus-cp-data`:
composing `DiskConfig::torn_tail_on_crash` with `corrupt_on_crash` lets a
corrupted-but-still-JSON-valid WAL record (`animus-control::persist::
WalRecord`, no per-record checksum — the codec `animus-control` and
`animus-cp-data` **share**) decode successfully with a wrong value, which
that plane's `assert_ts_monotonic` (an HLC-timestamp monotonicity invariant)
then trips on once a later entry applies past it. Building `control_corpus.rs`'s
own `#[ignore]`d regression probe for the identical composition, the natural
assumption was "the codec is shared, so the panic should reproduce here
too" — it did not, across a deliberate 80-combination sweep (many seeds ×
`PlainChurn`/`AllocatorRace` workloads × `LeaderKill`/`FollowerKill` ×
with/without an `FsyncLie`-accumulated un-synced buffer before the crash,
done during development, not committed as code). The underlying codec gap
is real in both places (the corruption fires identically — confirmed by
inspecting `DiskCorrupt`/`DiskTear` trace events), but `animus-control`'s
`Metadata::apply`/recovery path has no invariant as strict as
`assert_ts_monotonic` for a wrong-but-decodable *numeric* field to trip —
this plane's commands carry no HLC timestamp at all, and its epoch/CAS
checks *reject* a mismatch rather than *asserting* on one, so a corrupted
epoch or tablet id just fails a CAS instead of panicking. **General lesson:
a fault-finding confirmed in one plane over a codec/primitive that plane
shares with another does not transfer by assumption — the reproducing
mechanism is downstream of the shared corruption (some specific invariant
the corrupted-but-valid value eventually trips), and a sibling plane may
share the corruption but not the invariant. Confirm (or rule out) the
composition explicitly in each plane it could plausibly reach, and record a
negative result as carefully as a positive one — it is what tells a future
reader whether the standing regression probe is still watching for
something that could happen, or has already been checked and cleared for
that plane's current invariant set.**

**Update**: issue #495's underlying codec gap (`animus-control::persist::
WalRecord` having no per-record checksum) is now fixed — every WAL line
carries a CRC32 checksum, and a corrupted-but-parseable record is dropped
at decode time (along with everything physically after it in the file)
instead of decoding into a wrong value. The methodology lesson above is
unaffected by the fix (it's about how fault-findings do or don't transfer
across planes, not about this specific bug's status); `control_corpus.rs`'s
`control_corrupt_on_crash_may_hard_panic_issue_495` stays in place as a
standing regression probe, now expected to stay clean.

## `Simulator::crash`+`restart` is not a stand-in for a real process restart — a "does recovery actually rebuild from disk" test needs `stop` + a fresh constructor (`animus-control`'s `control_corpus.rs`, PR③)

`animus-sim`'s `Simulator::crash` mutes a node (drops its un-synced disk +
volatile inbox, keeps its tasks alive but silent) and `restart` un-mutes and
re-arms those SAME still-live tasks — the node's `RaftCore`, driver loop,
and (for `animus-control`) ADR 0038's async apply task never actually stop
running; they just stop being able to send/receive for a while. This is the
right primitive for "the node was briefly unreachable/froze," and it's what
every pre-existing `LeaderKill`/`FollowerKill`-shaped nemesis in this
repo's corpora uses. It is the WRONG primitive for proving a genuine
crash-recovery code path — anything gated on "a fresh process just started
and has to rebuild its in-memory state from durable disk" (here:
`meta_apply_loop`'s `mirror::rebuild_metadata_from_engine` call plus
reseeding its `engine_applied` watermark from the engine's own
`syskv::applied_index_key()`, rather than trusting `core.last_applied()`)
never runs at all under crash+restart, because the in-memory state that
code path exists to rebuild was never actually destroyed. Proving that path
needs `Simulator::stop` (removes the node's tasks entirely — genuinely
nothing left running under that node id) followed by constructing a BRAND
NEW driver/node that reopens the SAME retained durable-engine handle
(mirroring `tests/restart.rs`'s pattern: keep the `MemoryEngine` alive in a
variable outside the node so a later `RaftNode::start` can re-clone it,
exactly like a real disk surviving a process exit). **General lesson: when
a fault nemesis is meant to exercise "recovery from durable state after a
restart," check whether the simulator primitive it uses actually destroys
the in-memory state the recovery code is supposed to rebuild — a
crash-and-resume primitive that merely mutes/re-arms the same live task
will make the test pass for the wrong reason (nothing to recover, so
nothing can prove the recovery path works) rather than for the right one.**

A second, sharper gotcha discovered building this nemesis: `Simulator::stop`
does **not** clear a `crashed` flag a prior `Simulator::crash` on the same
node id set — the two are independent pieces of state (`crashed: BTreeSet`
vs. simply having no owned tasks). A cell that ever crashes a node and then
later `stop`s + reconstructs it on the same id, without an intervening
`Simulator::restart` (which is what actually clears `crashed`), ends up with
a fresh `RaftNode` whose every outbound send is silently swallowed forever
by the stale mute — with no panic, no error, just an isolated node that
looks alive but never converges, i.e. exactly the shape of bug this session
already burned time on once before with a different composition (see the
sibling entry on issue #421 above). The fix is a fixed compose order —
`crash; stop; restart` (the `restart` call clears the mute even though
there are no tasks left for it to re-arm) — *before* constructing the fresh
node. This repo's `control_corpus.rs` has no cell that currently composes
`StopRestart` with a prior crash on the same id, but the defensive order is
applied unconditionally in `Group::stop_node` anyway, since it costs
nothing and rules the entire hazard class out by construction rather than
by "don't do that" convention.

## A real multi-chunk `InstallSnapshot` transfer completes in single-digit milliseconds of virtual time — a coarse `Vec<(Duration, Nemesis)>` fault schedule cannot reliably land "mid-transfer" (`animus-control`'s `control_corpus.rs`, PR③)

Composing a fault with an in-flight chunked snapshot transfer sounds like it
should fit this repo's usual `faults: Vec<(Duration, Nemesis)>` scenario
shape (schedule the fault at some duration into the run, mirroring every
`LeaderKill`/`PartitionLeader` cell elsewhere in this corpus). An
exploratory run (millisecond-granularity polling of a real, healed,
multi-chunk transfer between two real `RaftNode`s) found this doesn't work:
once the leader starts shipping chunks to a caught-up-eligible follower,
the WHOLE transfer — first chunk received through fully reassembled
`Metadata` — completes in roughly 3ms of virtual time, because this plane's
replication path has no artificial per-chunk delay and virtual time costs
no real wall-clock proportional to its size. A fault scheduled "2.2 seconds
into the run" has essentially zero chance of landing inside a 3ms window
whose START time itself isn't even known in advance (it depends on when
the leader notices the follower is behind past its compacted prefix, which
depends on heartbeat timing, election history, etc. — all seed-dependent).
The fix was to replace the duration guess with a **condition-based poll**:
step virtual time in small (200µs) increments, checking a directly
observable proxy for "the transfer has started but not finished" (the
receiving follower's `snapshot_index() > 0` — set from the FIRST chunk's
base index — while its reassembled state, e.g. `metadata().members.len()`,
is still short of the target), and inject the fault the instant that holds.
This lands inside the real window regardless of a given seed's exact
timing, and the test itself asserts it actually caught the window (rather
than the transfer racing past a coarse poll entirely) before proceeding —
so a future change that made the transfer effectively instantaneous would
fail loudly here instead of silently degrading into a fault-free no-op
cell. **General lesson: before reaching for a scenario harness's existing
`Duration`-based fault-scheduling shape to hit a specific in-flight window,
measure how long that window actually is in virtual time — a data-plane
"lazy, on-demand, lasts-until-idle" mechanism can complete in microseconds
to milliseconds once triggered, which no second-or-millisecond-granularity
fixed schedule can reliably intersect; a condition-based poll on a directly
observable proxy for "in progress" is the fix, and asserting the poll
actually caught the window (not just that the scenario as a whole
converged afterward) is what keeps the test honest about whether it
exercised the fault window at all.**

## A CI check whose failure mode is "intermittent, unresolved root cause" doesn't need a diagnosis before it can be fixed — remove the risky mechanism instead (issue #466, `.github/workflows/dco.yml`)

`tim-actions/get-pr-commits` + `tim-actions/dco` intermittently died with
`Argument list too long` (`E2BIG`) on some PRs. The issue's own first theory
— "our commit messages are unusually large, and that's what crosses
`ARG_MAX`" — was directly disproved by an A/B comparison: a 37-commit/42KB
PR failed twice while a 63-commit/73KB PR (~74% more payload) passed on the
same runner fleet minutes apart. The true trigger inside the third-party
action was never identified, and didn't need to be: the fix wasn't "explain
the flake," it was "stop doing the thing that has this failure mode at
all." Both actions round-trip every commit message through argv/env
(`get-pr-commits`'s JSON output becomes `dco`'s `commits:` input, which the
action re-serializes onto a command line internally) — replaced with an
in-workflow script that walks `git rev-list --no-merges
<merge-base>..<head>` and checks `git log -1
--format='%(trailers:key=Signed-off-by,valueonly)' <sha>` per commit,
capturing trailer content only into a shell variable that's compared/
printed, never re-interpolated into another command or eval'd — commit
SHAs (fixed-width hex from `rev-list`, never derived from message content)
are the only thing that reaches a command line. **General lesson: when a CI
failure is real, reproducible, but resists root-causing (especially inside
a third-party action's own internals), don't burn more budget chasing the
exact trigger before fixing it — if the failure mode has a known shape
(here: "large untrusted content on argv/env can exceed `ARG_MAX`"), a
local reimplementation that structurally can't hit that shape closes the
bug regardless of which precise condition was tripping it, and is usually
smaller than bisecting a dependency's black box.** Separately: computing the
PR's commit range via `git merge-base "$BASE_SHA" "$HEAD_SHA"` (both
supplied by the `pull_request` event, `head.sha`/`base.sha`) rather than
diffing against the base branch tip works identically for same-repo and
fork PRs, because GitHub mirrors a fork PR's commits into the base repo's
own ref namespace — a plain `actions/checkout` with `fetch-depth: 0` and an
explicit `ref: ${{ github.event.pull_request.head.sha }}` (not the default
merge-ref checkout) is enough to fetch them all from `origin`.
