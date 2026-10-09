# ADR 0074 — Production-readiness exit criteria: what "beta" means, overload semantics, and the release policy

- **Status:** Accepted
- **Date:** 2026-10-04
- **Origin:** `docs/roadmap.md` R-01 ("Production-readiness pass: exit
  criteria for leaving pre-alpha"). The project calls itself pre-alpha
  (root `CLAUDE.md`, `website/index.html`) with no written definition of
  what would end that. This ADR is the first PR of R-01: it fixes the
  definition and the three policy decisions the later sub-tracks need,
  and ratifies the checklist in
  [`docs/production-readiness.md`](../production-readiness.md).
- **Amends:** none. **Depends on:** ADR 0003 (determinism — what the sim
  does and does not prove), ADR 0006 (wire adapter), ADR 0015 (metrics
  seam), ADR 0059 (backup/PITR), ADR 0060 (operator), ADR 0064 (TLS),
  ADR 0065 (per-table throttling, `ProvisionedThroughputExceededException`),
  ADR 0069 (encryption at rest), ADR 0072 (AWS-faithful service limits),
  ADR 0073 (upgrade compatibility: per-format version tags, golden
  fixtures, "readable forever" support window).

## Context

Correctness is strong *in simulation* (ADR 0003 and the seeded corpora).
What the simulation cannot prove is listed in ADR 0003 itself: real-thread
liveness, real I/O behaviour, real resource consumption. Checked against
the tree on 2026-10-04, the following are absent:

- no `fuzz/` directory and no `cargo-fuzz` target (the word "fuzz" appears
  only in in-crate decoder tests, e.g. `crates/animus-cp-data/src/codec.rs`,
  `crates/animus-env/src/handshake.rs`);
- no `CHANGELOG*`, no `SECURITY.md`, no SBOM, no image signing or
  attestation (`.github/workflows/image.yml` builds and pushes to GHCR on
  `main` and `v*` tags and does nothing more); workspace version is
  `0.0.0` (`Cargo.toml`);
- no operations runbook (`docs/runbook/` does not exist);
- no chaos or multi-day soak tooling (the only chaos/netem mentions are
  in simulation-related lessons and test comments);
- **no connection cap or admission control on the DynamoDB edge.** The
  accept loop in `crates/animusd/src/dynamo.rs` spawns one task per
  accepted connection with no bound, and `crates/animus-node/src/http.rs`
  bounds only the request body (`MAX_BODY`, 1 MiB). Per-table throttling
  (ADR 0065) bounds a *table's* rate, not the node's concurrency;
- disk-full (`ErrorKind::StorageFull`) exists only as a `SimEnv` fault
  (`crates/animus-sim/src/lib.rs`, `DiskConfig`); nothing in
  `animus-storage` or `animus-env` handles ENOSPC deliberately, and its
  behaviour on a real node is untested.

What the code *does* already emit on the DynamoDB wire, relevant to
overload: `ProvisionedThroughputExceededException` (ADR 0065, per-table,
HTTP 400), `ServiceUnavailable` (HTTP 503; `error_status` in
`crates/animusd/src/dynamo.rs`, minted by `WireError::service_unavailable`
in `crates/animus-dynamo/src/wire.rs` for an exhausted server-side retry
budget on a transient refusal, issue #994) and `InternalServerError`
(500). `ThrottlingException` is **not** emitted anywhere today;
`RequestLimitExceeded` appears only as a member of AWS's per-statement
`BatchStatementErrorCodeEnum` in a `wire.rs` doc comment.

## Decision

### 1. What "beta" means

AnimusDB leaves pre-alpha and may call itself **beta** when **every
criterion in `docs/production-readiness.md` is Met, or carries an
explicit, signed-off waiver listed in that document's Waivers table**.
Nothing else defines beta.

- A criterion is a checkable statement with a status (`Met`, `Not met`,
  `Pending-dependency`), evidence (a path to a test, workflow, file or
  ADR section that exists), and an owner sub-track (a-g of R-01, or
  "cross-cutting"). A criterion is `Met` only when its evidence exists
  in the tree and is green; prose is not evidence.
- A waiver names the criterion, the reason, the risk accepted, who signed
  it off (a maintainer, by name, in the PR that adds the row) and the
  date; it is reviewed at every release. A `Pending-dependency` criterion
  blocks beta exactly like `Not met` unless waived.
- The criteria document may *add* criteria freely (a new one starts
  `Not met`). Removing or weakening a criterion needs an amendment to
  this ADR, not a drive-by edit.
- Beta is a status of the project, announced by editing root `CLAUDE.md`
  and `website/` in the PR that turns the last criterion green. "Beta"
  is not a data-safety guarantee beyond what the criteria establish;
  `website/` keeps saying what is and is not covered.
- The sim corpora remain the correctness proof (root `CLAUDE.md`,
  ADR 0003). Criteria (a)/(b) check the `ProdEnv` seams the sim cannot;
  they do not replace the corpora, and a chaos failure that a seed can
  reproduce becomes a seeded corpus cell.

### 2. Overload semantics (sub-track d)

**Principle: every queue is bounded, and overload is a prompt, typed,
retryable refusal on the DynamoDB wire, never unbounded queuing and never
a silent hang.** Concretely:

1. **Bounded queues only.** No unbounded channel, unbounded per-connection
   buffer, or unbounded spawned-task fan-out on a path reachable from an
   untrusted peer. Every channel/buffer on such a path has a documented
   capacity; the sub-track (d) audit enumerates them and each bound gets
   a test (sim with fault injection where the bound is logic, a `ProdEnv`
   test where it is real-thread behaviour, per ADR 0003).
2. **Connection cap.** Each client-facing listener (`dynamo`, `admin`,
   `console`, and the intra relay port) has a configured maximum of
   concurrent connections (default finite and settable; the client-facing
   DynamoDB port has no "unlimited" setting). Beyond it a new connection
   is answered with HTTP `503` carrying the DynamoDB error body
   `ServiceUnavailable` and then closed, or, if the response cannot be
   written without blocking, closed outright; it is never parked in an
   unbounded accept backlog or task queue.
3. **Admission control.** A node-wide bound on in-flight DynamoDB requests
   (and a per-connection bound on pipelined requests) is checked before
   any work is queued. A request over the bound is refused immediately
   with `ServiceUnavailable` (HTTP 503) — the code every AWS SDK's default
   retry policy already retries with backoff, and the code
   `WireError::service_unavailable` already mints. It is a *distinct
   signal* from a per-table throttle: node-level overload is not a table
   exceeding its provisioned capacity, so it must not reuse
   `ProvisionedThroughputExceededException`.
4. **Error-code map (normative).**

   | Condition | Wire error | HTTP | Source |
   |---|---|---|---|
   | table over provisioned RCU/WCU | `ProvisionedThroughputExceededException` | 400 | ADR 0065, unchanged |
   | node connection cap or in-flight admission exceeded | `ServiceUnavailable` | 503 | this ADR |
   | storage full on the write path (below) | `ServiceUnavailable`, message names `StorageFull` | 503 | this ADR |
   | request over a size/shape limit | the limit's own validation-class error | 400 | ADR 0072, unchanged |

   `ThrottlingException` and `RequestLimitExceeded` are **not** adopted as
   top-level overload codes: neither is emitted by the code today, AWS
   documents `RequestLimitExceeded` as an account-level quota signal that
   a self-hosted node has no account to attribute, and a single retryable
   code for node overload keeps the client contract small. A later feature
   that needs them (control-plane DDL rate, per-account quotas) amends
   this table.
5. **Observable.** Each refusal increments a named counter by reason
   (`conn_cap`, `admission`, `storage_full`) through the metrics seam
   (ADR 0015), so sub-track (f) can alert on it.
6. **Internal planes are bounded too**, but their overload response is
   protocol-specific (Raft backpressure, `ProposeResult` refusal) and is
   audited under (d), not given a wire code here.

**Disk-full behaviour.**

1. A node that cannot append or fsync (ENOSPC / `ErrorKind::StorageFull`)
   **refuses the write with a named error and does not acknowledge it.**
   "An ack means fsynced" (root `CLAUDE.md`, durable-before-visible) is
   absolute: a write that could not be made durable is never reported as
   accepted, never visible, and no half-applied state is left behind.
2. The error is internally a named kind (`StorageFull`) so logs, metrics
   and the admin surface can say so; on the DynamoDB wire it is
   `ServiceUnavailable` (503) whose message names `StorageFull` and ends
   with the house `"; retry"` suffix, so SDKs back off and retry.
3. **Reads keep being served** from already-durable state. A node whose
   log cannot take appends does not participate in commitment (a leader
   steps down, a follower does not ack) rather than acking un-fsynced
   entries; the group stays safe while a quorum still has space.
4. **Recovery needs no operator action beyond freeing space:** when space
   returns the node resumes accepting writes without a restart; no acked
   write is lost and none duplicated across the episode. Compaction/GC
   that itself needs scratch space must not wedge the node permanently.
5. Proven by a `SimEnv` fault-injection cell (the primitive exists in
   `animus-sim`) plus a `ProdEnv` test on a real size-limited filesystem
   for the seams the sim cannot reach.

### 3. Release policy (sub-track g)

1. **SemVer for the binary, independent of format versions.** Releases are
   `vMAJOR.MINOR.PATCH` git tags driving `image.yml`. The binary version
   and ADR 0073's per-format version tags (`lsm-wal` v2, `control-wal`
   v2, ...) are **separate axes**: a binary release may bump zero, one or
   many format versions; a format bump does not by itself force a binary
   MAJOR. Each changelog entry states which format versions it
   introduced; ADR 0073's inventory stays the source of truth for formats.
2. **Pre-1.0 meaning.** While the version is `0.y.z`: MINOR may change
   the wire API surface, CLI flags, config schema and metric names (each
   called out in the changelog); PATCH is bug fixes only. **Durable
   formats are exempt from that looseness**: from the ADR 0073 baseline
   (`9a9f972f`, 2026-09-29) every post-baseline format version stays
   readable by every later binary, forever, and an existing golden
   fixture is never edited or deleted (`scripts/check-format-fixtures.sh`)
   — pre-1.0 does not weaken this. `1.0.0` is cut only after beta (every
   criterion Met or waived) plus ADR 0073 Phase 3 (rolling upgrades);
   from 1.0 the wire API, config and CLI follow strict SemVer.
3. **Changelog per release.** A `CHANGELOG.md` (Keep a Changelog layout),
   one entry per tagged release, listing user-visible changes, format
   version bumps, wire/config/flag changes and security fixes; a tag
   without an entry fails the release workflow.
4. **Signed, attested artefacts.** Every release publishes multi-arch
   images and binaries signed with **cosign keyless** (GitHub OIDC
   identity, transparency-logged), an **SBOM** (CycloneDX or SPDX,
   generated in CI, attached to the release and the image) and a
   **build-provenance attestation**. `main` builds may be pushed
   unsigned only if tagged as non-release; a `v*` tag that cannot be
   signed does not publish.
5. **Vulnerability disclosure.** A root `SECURITY.md` names a private
   reporting channel, the supported-versions statement (latest MINOR
   pre-1.0), an acknowledgement SLA and a coordinated-disclosure window;
   advisories ship through GitHub security advisories and the changelog.
6. **Deprecation policy for wire and format changes**, consistent with
   ADR 0073:
   - *Durable formats*: never removed. A format change is a new version
     tag, a new fixture and a decoder that keeps the old one under
     `legacy` (ADR 0073's Phase 1 checklist); the support window for
     post-baseline versions is forever, so there is no deprecation of a
     readable format.
   - *Client wire (DynamoDB API)*: AWS-faithful (ADR 0072); behaviour is
     never moved away from AWS's. An AnimusDB-specific extension is
     deprecated by a changelog notice in release N and removed no earlier
     than the next MINOR (pre-1.0) or MAJOR (post-1.0).
   - *Internal node-to-node wire*: governed by ADR 0073 Phase 2/3 (a
     replicated cluster version and feature gate); until Phase 2 completes (P2-A and P2-C have landed; P2-B and P2-D have not),
     mixed-version clusters are unsupported and release notes say so.
   - *CLI flags, config keys, metric names*: deprecated for at least one
     MINOR with a warning before removal (pre-1.0).
7. **Supported-platform matrix** (OS, kernel, filesystem — ext4/xfs and
   their fsync semantics — architectures, Kubernetes versions via the
   `kind` matrix) is published in the docs and gates a release; the
   concrete matrix is sub-track (g)'s deliverable.

## Consequences

- `docs/production-readiness.md` is the single place that says whether
  the project is beta. Sub-track PRs flip rows from `Not met` to `Met`
  with evidence.
- Sub-track (d) has a fixed contract (error-code map, bounded queues,
  disk-full invariants) to implement and test against.
- Sub-track (g) has a fixed policy; the version stays `0.0.0` until the
  first release PR bumps it and adds the changelog.
- This ADR's PR is documentation only. The Context claims are
  re-verified against the tree by each sub-track before it claims a row.
- Sub-tracks refine decisions here by amending this ADR rather than
  writing a new one; a decision that contradicts it needs an explicit
  amendment.

## Open questions

- Whether the 7-day soak (a) is a hard beta gate or a waiver candidate for
  an early beta; it is a gate until a maintainer signs a waiver.
- Default values for the connection cap and in-flight bound (sized from
  C-17's per-node density numbers when available).
- Whether disk-full should additionally flip `/admin/health` readiness;
  decided in (d) with the runbook author.

## Amendment 2026-10-05: disk-full implemented (issue #1185)

Disk-full (D-7) is implemented for the WAL path. As built: ENOSPC marks the
group's WAL suspect (never appended to or fsynced again), the group refuses
writes with a named 503 `StorageFull ...; retry` (`overload_storage_full`), and a
backoff probe rewrites the WAL from the in-memory log onto a fresh file and
resumes without a restart; no persisted-format change. This differs from the
design sketched in `docs/resource-bounds.md` before the change in two ways:
there is no `requeue_unpersisted` (the in-memory log is a superset of every
drained round, so a whole-image rewrite suffices) and there was initially no
leader step-down (added by the issue #1219 amendment below). The open
question above is decided: `/admin/health` reports a degraded `storage_full`
field but its status code does not flip, because readiness would also pull the
node's reads. Still open: exporting
`spawned_task_panics`, and a `ProdEnv` size-limited-filesystem test. Proven by
`ANIMUS_DISK_FULL_SEEDS`; see `docs/resource-bounds.md` section 3.

## Amendment 2026-10-05: LSM-engine ENOSPC (issue #1218)

The residual "ENOSPC inside the LSM engine still panics" is closed. As built:
`StorageError::StorageFull` is the recoverable class (a failed call changed
nothing durable or visible); the apply task pauses and retries the identical
engine call instead of panicking, which preserves apply order by construction
and surfaces as the existing `StorageFull` state (writes refused with the 503,
`/admin/health` `storage_full`) until the call succeeds; flush and compaction
fail cleanly and remove their orphan outputs; inline post-write maintenance
ENOSPC is deferred rather than failing the already-durable write; and an
ENOSPC-failed WAL batch cuts the segment back to its last durable length before
the next batch. The disk-full semantics for clients are unchanged. No
persisted-format change. The disk-full corpus now also runs over
`LsmEngine<SimEnv>`.

## Amendment 2026-10-05: leader step-down on StorageFull (issue #1219)

The residual "a StorageFull leader keeps leadership" is closed, which makes the
earlier clause in section 2 item 3 ("a leader steps down") true as written. As
built: the per-tablet consensus loop feeds the group's storage-full state
(suspect WAL or ENOSPC-stalled apply task) into `RaftCore::set_storage_full`; a
storage-full leader arms the existing `transfer_leadership` toward its most
up-to-date voter (`RaftCore::storage_full_step_down`), and a storage-full node
never campaigns and declines `TimeoutNow`, so leadership cannot ping-pong back.
A transfer only arms: the leader keeps leading until a healthy target wins, an
unanswered transfer (a full target) aborts at its deadline and the retry rotates
to the next voter, so there is no permanent leaderlessness and a whole-cluster
disk-full still converges when space returns. A follower with a suspect WAL
already acked nothing it could not persist (the failed round gates every later
ack); that is now pinned by tests. No wire or persisted-format change; the
control-plane group is unchanged. Proven by the disk-full corpus (writes acked
inside a leader-only window, plus linearizability); see
`docs/resource-bounds.md` section 3. Still open: exporting `spawned_task_panics`
and a `ProdEnv` size-limited-filesystem test.

## Amendment 2026-10-06: every replica full (issue #1228, chaos findings F-1 / F-3)

The 2026-10-05 amendment left one case open: with **every** replica of a group
full the group could lose its leader and become unreadable until space returned
(`docs/chaos.md` F-1), and the phase-2 "writes are refused with a named 503"
assertion of `chaos_disk_full` had to be downgraded to "reported" (F-3). Root
causes, found from a real run's per-group dumps rather than assumed:

1. **The step-down deposed the one node that could still serve.** A full leader
   handed leadership to its most caught-up voter, but a voter's fullness was
   unknown to the leader (a full follower acked nothing, so it could not even
   say so), and a handoff to a voter that has not yet noticed it is full wins a
   term it can persist while the other (full) voters cannot grant a vote: the
   old leader stepped down on the higher term, no election could complete, and
   full nodes never campaign, so the group was leaderless until space returned.
2. **A full leader could not serve a linearizable read.** The read path mints a
   timestamp and needs a *committed* `ReadCeiling` above it (ADR 0018 section
   2); the ceiling covers 500 ms, and extending it needs a quorum that can
   persist.
3. **A full leader's ReadIndex could never be applied**: with one healthy
   follower the commit index runs past what the leader's own WAL holds, and the
   leader applies only what it has made durable.

What changed (no wire or persisted-format change; ADR 0073 needs no gate):

- **A full follower keeps acking, frozen.** `RaftCore::handle_append_entries`
  clamps the ack's `match_index` to the follower's own `durable_index` while it
  is storage-full, the consensus loop ships an `AppendEntriesResp{success}`
  whose `match_index <= durable_index` ahead of the (never-landing) round, and
  the ack's existing `check_pending` flag carries "I cannot vote" (now also true
  when storage-full, `cannot_vote_yet`). Safety: the ack vouches only for
  entries already on disk, so `maybe_advance_commit` can never count a full
  follower toward an entry it did not persist (a unit test and
  `raftkv_disk_full_follower_acks_nothing_it_could_not_persist` pin it;
  `log_truncate` now also lowers `durable_index`, so a truncated-and-replaced
  tail is never read as durable). The term it echoes is the leader's own
  (a success ack requires `term >= current_term`), and a node never makes a
  *vote* without first persisting it, so a frozen ack cannot enable a second
  vote in a term. A reject, a vote grant and any ack claiming more stay held
  exactly as before. A leader stops treating the frozen ack as a reason to
  resend immediately (the first version spun at zero latency: 1.4 M simulator
  events in 10 s, now bounded by the corpus).
- **Step down only to a successor that can lead.** `storage_full_step_down`
  (and every `transfer_leadership` caller: the G-01 preferred-leader step,
  rebalance) refuses a target that reported `check_pending`, and the full-leader
  handoff happens only when a *majority* of the other voters reported healthy on
  a recent ack (`RaftCore::healthy_followers`). With fewer, no replica could win
  an election or commit under any leader, so the leader stays: leadership is
  stable, reads are served and writes are refused. The driver waits one election
  timeout after entering the full state before picking, so followers that learn
  of their own fullness from the same failed write have reported it first. The
  storage-full step-down therefore also wins over a placement preference: a
  preference can only arm a transfer a healthy successor could complete.
- **A full leader serves linearizable reads at its committed floor.**
  `read_serve_ts`: once the ceiling lapses a storage-full leader serves at the
  highest version its engine holds (applied write versions and the ceiling
  marker) instead of proposing a ceiling it cannot commit. Linearizable: the
  read barrier still confirms leadership by quorum and engine-applied progress,
  every acknowledged write is at or below the floor, and any future write on any
  leader is minted above everything that leader applied or witnessed, so it can
  never land below a read served here. The barrier's ReadIndex for a full leader
  is its first-term entry (its election no-op), not the commit index: its
  engine can never reach a commit index its apply is paused short of, but every
  earlier-term acknowledged write is at or below that entry (which the barrier
  confirms committed) and every own-term acknowledged write was applied before
  its ack, so the engine at serving time holds them all.
- **The eventual-read gate no longer needs a current leader, nor a fully
  caught-up engine, for a full replica that has had one**
  (`had_leader_contact`, sticky for the process's life): a paused apply stops
  between entries, so its engine is an in-order prefix of the log, which is what
  an eventual read promises. The one excluded state is mid-`InstallSnapshot`
  (`engine_applied < snapshot_index`), and the "never initialised" protection is
  unchanged for a process that has never heard a leader.

**Decision: a leader that dies while every replica is full is not replaced.**
Allowing a full node to win an election (and merely refuse writes) was
considered and rejected: an election needs the candidate's term bump and
self-vote, and every voter's grant, to be durable before it is sent (a vote
that was not persisted can be cast again after a restart: two leaders in one
term). A full node cannot make anything durable, so it neither campaigns nor
grants. A reserved hard-state file would not be portable (copy-on-write
filesystems can fail an overwrite). The group is therefore leaderless from the
leader's death until space returns on a quorum, but it is not unavailable for
what it can safely serve: every surviving replica keeps serving eventual reads
(above), nothing acknowledged is lost, and when space returns the group elects
and writes resume (`raftkv_disk_full_all_replicas_leader_crash_*`). The residual
is a restart *during* the window: a freshly started process has had no leader
contact, so it serves no eventual read until one exists.

Proven by: the animus-control unit tests (`storage_full_step_down.rs`), the
disk-full corpus cells (`raftkv_disk_full_all_replicas_*`, the healthy-quorum
boundary RF3/RF5, the leader-crash cell, mem and LSM), the quiescence cell
(`quiescence.rs` (ix)), the `SimCluster` wire test
`sim_cluster_dynamo_disk_full` (prompt 503 `StorageFull` on every node, strong
and eventual reads served, leader unmoved), and `chaos_disk_full`, whose phase 2
again **asserts** reads served and at least one 503 `StorageFull` refusal. The
control-plane group is unchanged (it never sets `storage_full`; its full
follower still holds its acks).

## Amendment 2026-10-09: a recovered voter is not a successor until it stays healthy (chaos finding F-4)

`chaos_disk_full` failed on PR #1260 with `consistent [0, 0, 0]` of every read
of a written key while every disk was full (eventual reads 4 of 4). It
reproduced locally (1 run in 30 on the PR; the precursor, a leadership
hand-back at all-full time, also shows without the PR's cluster-version
finalize, so the finalize is not the cause). The per-group dump of a failing run showed the
mechanism: tablet 3 at term 3, leader n0, commit 514, n0's durable 516, n1/n2
full. Every node's disk filled within about a second, but a node only learns it
is full from a failed write, and a full leader's refused writes never reach its
followers. So the leader `n0` handed off to `n1` (both followers looked healthy,
one of them stale), and a second later `n1`, itself now full, handed leadership
**back** to `n0`. `n0` had regained 96 KiB from its own WAL rewrite (the old
WAL file freed) and so reported `check_pending == false` for a moment; `n2`'s
last ack still said healthy because it had not yet failed a write. Two of two
other voters "healthy" satisfied the quorum rule, `n0` won term 3, appended its
election no-op, and no follower could persist it: the first-term entry never
committed, and Raft section 6.4 forbids a ReadIndex read before it does. Eventual
reads still worked (the sticky `had_leader_contact` gate).

What changed (no wire or persisted-format change): the leader remembers when a
voter flipped from `check_pending == true` to `false` within its stint
(`peer_recovered_at`), and `healthy_followers` omits that voter until it has
stayed healthy for 20 election timeouts. A voter that was never seen unhealthy
is trusted as before, so an ordinary one-full-node handoff is not delayed. A
relapse restarts the clock; the window is 20 election timeouts, because a
150 ms one still let a hand-back through (the WAL rewrite that frees the space is
retried on a 100 ms to 2 s backoff). Looped locally with the fix: 0 of 45 runs
reached term 3 in the all-full dump (2 of 22 before) and 45 of 45 passed. Tests:
`storage_full_step_down::a_voter_that_just_recovered_is_not_a_successor_until_it_stays_healthy`
and `a_relapse_restarts_the_sustained_health_clock` (red before).

**Still open:** the lazy discovery itself. A node whose disk is full and which
has received no write does not know it (health is learned from a failed write,
not probed), so a handoff to a voter that is in fact full and merely has not
noticed can still happen and leaves the new leader unable to commit until space
returns. Closing it needs an active free-space probe on followers; the
hysteresis above removes the flapping-node form of it that the chaos run hit.
