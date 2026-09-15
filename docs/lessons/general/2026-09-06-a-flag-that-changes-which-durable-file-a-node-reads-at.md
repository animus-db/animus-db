# A flag that changes which durable file a node reads at startup needs a loud mismatch check before the first read, not a documented "silent reset is fine" judgment call (C-05 PR 2, `--shared-wal` layout mismatch)

The first cut of the `--shared-wal` layout-mismatch design (ADR 0028's
amendment) reasoned that flipping the flag against an existing data
directory was safe because the two layouts (`raftkv.wal.shared` vs.
`raftkv.wal.<stream>`) live at disjoint filenames — no file gets
overwritten or corrupted, so the newly-selected layout just "starts from
whatever it already has" (empty, on a first flip) while the old layout's
files sit unread beside it. That reasoning is correct as far as it goes,
and is exactly why it read as a reasonable, deliberate scope cut at the
time ("no additional loud-failure check... which this PR judged
sufficient for an internal, off-by-default tuning flag"). It missed the
actual hazard: "no file gets corrupted" is not the same claim as "no data
is lost." A node started with the flag flipped reads the *other* file —
the durably-empty one — and recovers every hosted tablet's Raft state
(log, term, `voted_for`) as if it had never persisted anything, which is
indistinguishable, to the recovery code, from a legitimate first boot. For
an off-by-default flag that a later PR (this same mechanism's own PR 3)
intends to default ON across every existing deployment, "silently reset
every tablet's Raft state on the next restart with no operator action
beyond a version upgrade" is not a tolerable failure mode — it needed to
be a hard startup refusal from the start, not a documented risk accepted
for later.

**Fix**: `animus_cp_data::host::check_wal_layout(env, shared_wal)` — a
directory listing (`Env::list()`, not a file open, so it costs nothing and
can run before any recovery path touches a durable file at all) that
refuses to start whenever `shared_wal` disagrees with what the data
directory already holds under the *other* layout, naming both layouts and
the flag in the error. Called once, before `SharedWal::open` and before
any tablet's own `drive()` recovery. Proven both directions with a
`SimEnv`-backed unit test and, since a directory-listing check is cheap
enough to be worth proving through the real production entry point too,
a real-`ProdEnv` end-to-end test asserting the actual `main.rs`-visible
error text after a genuine process restart with the flag flipped.

**General form**: when a boolean (or enum) startup flag selects which
*durable file* a node's recovery path reads — not just which code path
runs — a mismatch between the flag and what's already on disk is a
silent-data-loss hazard by construction, even when the two candidate
files are individually well-isolated from each other on disk. The
question to ask before shipping such a flag isn't "can flipping it
corrupt a file" (usually no, if the files are disjoint) but "can flipping
it make a real recovery read from an empty/wrong file and proceed as if
that were legitimate" (often yes) — and if so, a loud pre-flight check
belongs in the same PR that introduces the flag, not deferred to "if it
ever proves surprising in practice," especially when a follow-on PR in
the same feature train plans to flip the flag's default for every
existing deployment.
