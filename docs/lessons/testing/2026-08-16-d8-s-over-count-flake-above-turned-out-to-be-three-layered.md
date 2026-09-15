# D8's over-count flake (above) turned out to be three layered bugs, not one — fixing the first two exposed the third instead of clearing it, and only per-failure symptom classification (not raw pass/fail rate) told them apart.

**D8's over-count flake (above) turned out to be three layered bugs, not
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
