# A lying `sync` revealed by a crash isn't a storage-layer bug to fix — the fix is one layer up, and the corpus already proves what it looks like (2026-09-02, issue #554, `LsmEngine`).

**A lying `sync` revealed by a crash isn't a storage-layer bug to fix —
the fix is one layer up, and the corpus already proves what it looks
like (2026-09-02, issue #554, `LsmEngine`).** Root-caused `corpus-deep`'s
nightly `raftkv_lsm_full_corpus_is_linearizable` failure (scenario
`fsync_lie_stop_restart_early_3_s03`, seed=15482874842363184593) to a
precise interleaving, neither of the issue's own two starting
hypotheses: `LsmEngine::flush` writes a new SSTable, calls
`env.sync(&file)`, and — trusting its `Ok` — swaps the manifest and
`remove`s the WAL segments that data is now believed durably captured
in. Under `DiskConfig::set_fsync_lie_prob`, that `sync` can return `Ok`
while leaving the bytes buffered — invisible at flush time (a read-back
merges buffered+durable, indistinguishable from honest). A later crash
(`Simulator::stop`, modelling `StopRestart`) drops the file's un-synced
buffer, its only on-disk copy, since the fallback WAL segments were
already removed. `LsmEngine::open` already does the right thing here —
a loud `Err` (`corrupt sstable index: ...`), never a silent short open;
the panic was the test harness's own `.expect()` on that `Err` (fixed
separately, same issue).
**The system-level answer was already sitting in the corpus**: its
`MemoryEngine` tier restarts every `StopRestart` victim with a
genuinely empty engine and rebuilds it purely from the node's durable
Raft WAL plus the leader's on-demand snapshot, and passes every
`fsync_lie_stop_restart` cell — an empty engine has nothing a lying
sync could have corrupted. That's the correct recovery for `LsmEngine`
too: when a replica's local engine won't open, treat it as lost —
destroy its files (`EngineFactory::destroy` already exists in
`animus-cp-data::host`) and reopen fresh, letting Raft rebuild it from
the group — rather than today's `ensure_engine`
(`animus-cp-data/src/host.rs`, ~line 893-906), which warns and retries
the same open forever and never heals. That fix belongs in the
reconciler (and the corpus's own `lsm_engine()` helper should mirror
it), not in `animus-storage` — storage's job stops at detecting and
loudly reporting the lie, which it already does. **Built**: `host.rs`'s
`ensure_engine`/`materialize_split_child` now destroy-and-reopen on a
first `open` failure (ADR 0031's 2026-09-02 addendum) — but building it
surfaced a second, deeper gap first (a fresh engine silently believing
itself caught up to a compacted log it holds none of), closed
separately; see the entry immediately below this one for that
discovery and the needs-snapshot mechanism that actually makes this
destroy-and-reopen recovery safe past a replica's own compaction point.
What shipped here first: the residual named in `lsm.rs`'s "Crash
safety" doc and `animus-storage/CLAUDE.md`, plus a regression
(`lsm_crash.rs::fsync_lie_flush_survives_as_a_clean_open_error`) pinning
the loud-`Err` contract the reconciler-side fix depends on.
