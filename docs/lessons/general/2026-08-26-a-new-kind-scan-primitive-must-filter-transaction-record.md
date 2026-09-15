# A new kind-scan primitive must filter transaction-record marker keys too, not just resolve envelopes (ADR 0059 §4/§5)

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
