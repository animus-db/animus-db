# `corrupt_on_crash`'s single-byte WAL corruption can produce a syntactically-valid-but-wrong `HlcTimestamp` that hard-panics a later apply — a fresh, reproducible instance of the "no per-record WAL checksum" gap (quiescence corpus fault-primitives wiring)

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
