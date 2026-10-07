# A real-filesystem disk-full run found two defects the ENOSPC sim corpus passes

**Run the disk-full contract on real mounts, not only in `SimEnv`** (issue
#1221). The seeded ENOSPC corpus passed for #1218/#1219, yet the first real run
(`chaos_disk_full`, one tmpfs per node, ballast file) showed two things the
sim never exercises: with every node full, reads time out (a full follower acks
nothing, even a bare heartbeat, so the leader loses quorum contact and no read
path can serve; the sim corpus only asserts writes and recovery), and with the
2PC workload on, a disk-full window leaves an unresolved intent that blocks a key
forever. Both are timing-dependent on a real kernel and real scheduling.

- Assert what holds **per window**: the one-node-full window can assert strict
  read and write continuity; the all-full window cannot assert which node refuses
  (some refuse, some forward to a mid step-down leader and time out), only that a
  named `StorageFull` refusal happens and counters move.
- Do not commit a scenario that is red on a known finding: narrow it (here, 2PC
  ops off behind `ANIMUS_CHAOS_DISK_TXN=1`), document the finding with its
  reproduction, and hand it off as an issue.
- A mount needs `CAP_SYS_ADMIN`: skip with a message, and let CI set
  `ANIMUS_CHAOS_REQUIRE_MOUNT=1` so a runner that silently cannot mount fails.
- Disk hygiene: `CARGO_INCREMENTAL=0` and clearing `target/debug/incremental`
  matter on a shared sandbox; a 100%-full root filesystem makes tool output
  itself fail (background-task output files could not be written).
