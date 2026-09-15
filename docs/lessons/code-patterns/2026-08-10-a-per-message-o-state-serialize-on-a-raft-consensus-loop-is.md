# A per-message O(state) serialize on a Raft consensus loop is a latent election-storm hazard, and a *cache* to fix it must not double the work it replaces — reuse the one serialization everywhere the state is needed.

**A per-message O(state) serialize on a Raft consensus loop is a latent
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
