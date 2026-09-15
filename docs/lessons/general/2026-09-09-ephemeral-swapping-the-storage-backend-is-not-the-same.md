# `--ephemeral` swapping the storage backend is not the same claim as "this run's directory default is safe to reuse" — `animusd`'s `--cluster N` shared a fixed default `--dir` across runs regardless (2026-09-09)

`animusd --cluster N`/`--cluster-control N --cluster-data M` (the in-process
dev-convenience commands) defaulted `--dir` to one fixed path
(`$TMPDIR/animusd`) whenever it was omitted — `--ephemeral` only swaps the
CP-data `StorageBackend` to `MemoryEngine`; it never touches the
control-plane `ProdEnv`'s on-disk WAL (`Node::bind` always writes
`dir.join("internal")` on real disk, backend-independent). So two
back-to-back runs with no `--dir` silently rehydrated the first run's
control-plane WAL, and — confirmed by hand while building the regression
test below — a **different-sized** second run (e.g. `--cluster 3` then
`--cluster 5`) can leave some nodes permanently `control_leader_known:
false` (majority-quorum expectations from a membership that no longer
exists), while a **same-sized** restart re-elects fine (every node keeps
the same deterministic id and the whole group's on-disk state stays
mutually consistent across a same-process kill) — so the CLI-level fix
(`main.rs::resolve_cluster_data_dir`) makes both commands' default a fresh
directory unique to the process (`$TMPDIR/animusd-{cluster,ephemeral}-
<pid>`) **regardless of `--ephemeral`**: unlike `--config FILE --node I`'s
per-index default (a genuine "resume this same logical node" feature),
these two commands re-mint every node's OS-assigned port on every
invocation, so there is no stable prior state for a fixed default to
legitimately let a second run resume in the first place. **Lesson**: when
a flag *sounds* like it should make a whole run's footprint disappear
("ephemeral"), verify what it actually covers against every subsystem that
persists state, not just the one the flag's own doc sentence names — the
crate guide already had a hand-written note about this exact gap
(`crates/animusd/CLAUDE.md`), but it took two separate investigations
losing time to a rehydrated stale WAL before anyone acted on it.
