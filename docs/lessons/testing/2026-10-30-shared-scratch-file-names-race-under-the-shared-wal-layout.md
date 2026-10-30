# Shared scratch-file names race under the shared-WAL layout; SimEnv's atomic replace hides it

**What happened.** The free-space canary (`probe_free_space`, #1331) wrote
`{wal}.probe` via `Disk::replace`, then removed `{wal}.probe` and
`{wal}.probe.tmp`. With `--shared-wal` (the default) every tablet group on a node
passes the *same* wal name. `ProdEnv::replace` stages through `{file}.tmp` and
renames, so during ENOSPC concurrent probes shared one `.probe.tmp`: one
group's cleanup deleted another's in-flight tmp, the rename failed with ENOENT
(not ENOSPC), and `recover_suspect_wal`'s caller asserted and panicked the node
(`chaos_disk_full`).

**Why the gates missed it.** `SimEnv::replace` is atomic and has no `.tmp`
sibling, so no seed can reproduce a tmp-file race. Only real threads over
`ProdEnv` can.

**Rules.**
- Any scratch/probe/staging file name derived from a name that several groups
  share must carry a per-call unique suffix (`env.next_u64()`, deterministic
  under `SimEnv`), and cleanup must touch only that call's own files.
- A fix in this class needs a real-thread `ProdEnv` test that fails on the
  unfixed code (here `animus-control` `tests/probe_free_space_prod.rs`,
  `--features prod-heavy`: 16 concurrent probes x 300 rounds on one name).
- A catch-all `assert!` on a recovery error should name which step failed, so
  a non-ENOSPC error from a "cheap" probe is not mistaken for a rewrite fault.
