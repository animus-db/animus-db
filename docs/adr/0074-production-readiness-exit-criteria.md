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
     replicated cluster version and feature gate); until Phase 2 lands,
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
