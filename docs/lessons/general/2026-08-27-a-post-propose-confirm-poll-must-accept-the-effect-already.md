# A post-propose confirm-poll must accept "the effect already fully happened" as success, not just "the intermediate state I expected" (ADR 0059 §3, Train 1 PR④, `DeleteBackup`)

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
