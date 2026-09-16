# A `Building`-minted tablet has no self-heal path — an empty initial replica set is permanent, not merely under-replicated (`dynamo_import.rs` CI flake, `prod-liveness-animusd` shard 3/4, 2026-09-06)

A single flaky CI run (`import_skips_malformed_items_and_counts_them`
timing out its 30s "import did not reach a terminal state" poll, on a
2-vCPU shard running four `animusd` integration-test binaries
concurrently) turned out to have nothing to do with the import driver
(`import.rs`) a prior read-only analysis had suspected — a hypothesized
"no propose-side patience causes duplicate `SeedBatch`/`CompleteImport`
re-proposes" shape, the issue #268 amplification family. That shape would
still show the driver *doing* something every tick (malformed-item decode
debug lines, at minimum, since decoding runs before any propose); the
actual failure showed the driver doing **nothing at all** for the full 30s
— `import_loop`'s "not hosted here yet" branch fired on every single
200ms tick from the first to the last, never once transitioning.

**Reproduction**: a foreground loop of the standalone `dynamo_import` test
binary (`cargo build -p animusd --test dynamo_import`, then `timeout 45
target/debug/deps/dynamo_import-* --exact
import_skips_malformed_items_and_counts_them` repeated 50-150×) run
alongside three sibling `animusd` integration-test binaries
(`dynamo_txn`/`dynamo_streams`/`streams_e2e`) looping continuously in the
background on the same 4-core host — reproduced at a ~5-9% rate (4-7
failures per 50-100 iterations), matching the described CI shard's
contention shape closely enough to trust. Zero reproductions running the
same test alone.

**Diagnosis**: this workspace's test binaries carry no default `tracing`
subscriber, so a `--nocapture` run of even a failing test showed nothing
by default — the general lesson this doc already has an entry for
("reach for the driver's own tracing output on the *one* failing test
first"), but this investigation needed one step further: since the
failure signature (zero driver activity, not "stuck retrying") pointed
*upstream* of `import.rs` entirely, the useful instrumentation wasn't in
the suspected driver at all. `tracing::debug!` added to
`tablet_host_reconciler_loop`'s own per-tick view (temporary, reverted
before commit) confirmed the destination tablet's `Metadata` row read
`replicas: []` for the tablet's entire recorded lifetime; a second,
`tracing::debug!` added to `dynamo::finish_import_kickoff`'s own member
snapshot (also temporary, reverted) caught the exact moment: `members =
[("n0", Down)]` — the single node's own control-plane failure detector
(ADR 0012) had marked it `Down` (a false positive from a delayed
self-heartbeat under real CPU contention — the "SimEnv proves logic,
ProdEnv proves real-thread liveness" class of bug, not reproducible any
other way) at the exact instant `finish_import_kickoff` filtered
`Metadata.members` to `Active` for its replica pick, netting zero
candidates.

**Why this is permanent, not merely slow to recover**: `ClientCtx::
provision_tablet`'s identical "first `min(N, MAX_REPLICATION_FACTOR)`
`Active` members" replica pick (`schema.rs`) already has a guard for
exactly this race — `if !replicas.is_empty() && ...` — added for the
issue #268-era hardening. It works there because `CreateTablet` mints an
`Active` tablet: even an under-shot initial replica set is later grown by
`reconcile_placement`'s ordinary policy-driven self-heal
(`SetTabletPolicy` + `CasTabletReplicas`, `animus-control/src/meta.rs`).
`finish_import_kickoff` (ADR 0068 §6, S-05 PR 2) copied the *selection*
but not the *guard* — and worse, `reconcile_placement` only repairs a
tablet whose `state == TabletState::Active`
(`crates/animus-control/src/meta.rs`, `reconcile_placement`'s own filter,
predating this feature) — `BeginImport` mints its destination tablet
`Building`, and it stays `Building` for its entire seeding lifetime by
design (only `CompleteImport` activates it). A `Building` tablet with an
empty replica set can therefore never self-heal by any existing mechanism:
`plan_join_host`'s `replicas.contains(&base_id)` check trivially fails for
every node against `[]`, so no reconciler on any node ever hosts it, the
tablet can never seed, and it can never reach the `Active` state that
would make it eligible for repair in the first place — a structural dead
end, not a slow recovery. The reproduction bore this out exactly: every
failing run showed the identical "not hosted" tick firing for the *entire*
30-second window with no recovery, not an occasional slow one that
eventually succeeded.

**Fix** (`crates/animusd/src/dynamo.rs`, `finish_import_kickoff`): wait,
bounded by the function's own existing per-attempt `SCHEMA_COMMIT_TIMEOUT`,
for at least one `Active` member before computing `replicas`; skip
proposing `BeginImport` (mint a fresh id, retry) if the wait still ends
empty. The same guard shape `provision_tablet` already uses, applied to
the one call site that had regressed the lesson. Proven: the identical
50-150-iteration contention loop, 0 failures across 250+ post-fix
iterations (versus a consistent 5-9% failure rate pre-fix); `cargo test -p
animusd --test dynamo_import` green ×3.

**Twin defect, confirmed but NOT fixed here (own PR, per this repo's
incidental-bug convention)**: `dynamo::finish_restore_kickoff`
(`RestoreTableFromBackup`'s kickoff, same file) has the byte-for-byte
identical unguarded replica computation feeding the byte-for-byte
identical `Building`-state freeze (`BeginRestore` mints its own
destination tablet `Building` too) — not yet observed failing live, but
the code shape is proven vulnerable to the identical race by this
investigation and should get the identical fix.

**Update (issue #657, 2026-09-06): fixed.** `finish_restore_kickoff` now
calls the identical `await_active_metadata_for_new_tablet` wait/retry
helper `finish_import_kickoff` was refactored to use (the two loops had
become byte-for-byte identical, so this fix factored them into one shared
function rather than pasting a second copy) — both kickoffs wait, bounded
by their own per-attempt `SCHEMA_COMMIT_TIMEOUT`, for at least one `Active`
member before computing `replicas` via the shared
`active_replicas_for_new_tablet`, and both skip their propose (retrying
with a fresh id) if the wait still ends empty. Since `finish_restore_kickoff`
is shared by both `RestoreTableFromBackup` and `RestoreTableToPointInTime`,
fixing the one function closes the gap for both wire entry points at once.
Regression is a `Metadata`-only pin (`active_replicas_tests`'s
`restore_kickoff_shares_the_import_kickoffs_selection`/
`restore_kickoff_sees_no_replicas_when_every_member_is_down`), the same
"pure selection is unit-testable, the async wait/retry shape itself is
real-thread-liveness-only and stays untested" split this entry's own fix
already established — no new general lesson beyond what this entry already
records, since it's the identical mechanism applied to the identical twin.
Issue #657's second half (`backup_restore.rs`'s own missing propose-side
patience/confirm-timeout logging, described two paragraphs below) is
unrelated to this kickoff-guard half and remains open in its own PR.

**A separate, real defect family found along the way while chasing the
original (wrong) hypothesis, also NOT fixed here**: `backup_restore.rs`'s
restore driver (`propose_local`, confirming a `SeedBatch` propose by
applied index; `complete_restore`, discarding its own `CompleteRestore`
propose's accepted/rejected result) has zero propose-side patience and
zero logging on a bare confirm-timeout or discarded-reject — the exact
issue #268 retry-amplification shape `ClientCtx::provision_tablet` was
hardened against, inherited unmodified by `import.rs`'s own `propose_
local`/`complete_import` when that driver was built from `backup_
restore.rs`'s template (ADR 0068 §6 PR 2's own doc says as much: "mirrors
[`backup_restore::propose_local`]"). This investigation added `tracing::
warn!` to `import.rs`'s own confirm-timeout branch (kept, since a silent
`NoProgress` is indistinguishable from "driver never ticked at all" — this
bug's own signature — without it); `backup_restore.rs`'s identical branches
(`propose_local` around its `while ... { sleep }` loop's fallthrough,
`complete_restore`'s `let _ = ctx.propose_schema(...)`) still have none.
Neither the amplification itself nor the missing logging is fixed in
`backup_restore.rs` here.

**General form**: (1) **A newly-minted tablet's placement state, not just
its replica count, decides whether it can ever self-heal.** A tablet
minted `Active` (however under-replicated) is reachable by the ordinary
policy-driven convergence machinery; a tablet minted `Building` (or any
other non-`Active` state `reconcile_placement` excludes) is not, and never
will be until something *else* first gets it hosted and active — which an
empty replica set structurally prevents. Any new hand-rolled "pick N
`Active` members" replica selection feeding a `Building`-minting (or
otherwise placement-frozen) `MetaCommand` needs its own non-empty guard;
it cannot borrow `reconcile_placement`'s eventual repair the way an
`Active`-minting one can. Grep for the pattern (`NodeStatus::Active` +
`.truncate(MAX_REPLICATION_FACTOR)` or similar) before adding a new
propose site that mints a tablet in any state other than the default
`Active`. (2) **A background driver's fully-silent no-progress path makes
two very different bugs look identical from the outside**: "the driver is
retrying forever, wastefully" (issue #268's shape — visible if instrumented,
since something proposes repeatedly) and "the driver never got to run a
single real tick" (this bug's shape — invisible even instrumented, if the
instrumentation lives only in the driver itself, since the driver's own
tracing never fires). Distinguishing them needs tracing at the tick's own
entry gate (hosted? leader?), not just inside the tick body — see this
doc's existing "reach for the driver's own tracing output" entry, extended:
when a "stuck, no logs at all" symptom doesn't even show a driver's own
per-item logging that should be unconditional (decoding, in this case),
suspect a failure to ever reach the driver at all, not a retry loop within
it.
