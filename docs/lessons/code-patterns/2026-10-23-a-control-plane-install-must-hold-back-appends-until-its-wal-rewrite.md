# The control plane's InstallSnapshot also opened the #1368 gap; hold appends back, don't log the base early

**Context.** After #1370 fixed the data plane, a real-process soak (kill -9 +
restart of one node every 120 s) halted a *control-plane* node after ~18
restarts: "recovered log is not contiguous with its snapshot base 0: the entry
at position 63 carries index 65". Index 64 is `SNAPSHOT_THRESHOLD`.

**Mechanism.** #1370 deliberately left the control plane out
(`set_wal_records_install` off): a `Snapshot` record would put the WAL base
ahead of the engine, which only the data plane recovers from. But then nothing
else covered the same window: the follower installs base 64, the consensus loop
appends entries 65.. onto the old file (tail 1..63), and the apply task's
rewrite only fires when `behind >= SNAPSHOT_THRESHOLD` or an image is needed --
right after an install `behind` is 0, so the rewrite may be many appends away.
A crash in between replays `[1..63] ++ [65..]`. Before #1370's check this was a
silent positional shift; with it, a halted node (an availability regression
that only a restart-heavy real-process soak reached).

**Fix.** `RaftCore::install_rewrite_owed` (local, set by an install when the
base is not logged): the consensus loop (`persist_wal`) appends nothing while it
is set, and the apply task rewrites the whole image promptly (condition added
to the compaction gate, after the engine install is drained). The rewrite
carries the base plus every entry received since and releases the gated acks.
No format change.

**Rule.** When one half of a "durable reshaping" fix is rejected because it
would put store A ahead of store B, the *other* half is "stop writing past the
reshaping until it is durable", not "leave the window open". Grep every driver
that shares the core for the flag you made opt-in, and check what the
non-opted driver does instead. The crash-window regression test (stop the node
at seeded ms offsets after the install while a leader keeps proposing, then
replay its WAL) is `tests/it/control_install_crash_window.rs`; the small
offsets pass even on the broken code, so cover a spread, not one point.
