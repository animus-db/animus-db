# A "confirm by re-reading the value I just wrote" helper silently assumes the caller can predict that value's key ahead of the propose call — check that before reusing it for a new write shape.

**A "confirm by re-reading the value I just wrote" helper silently assumes
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
