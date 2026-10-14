# A "resend everything unacknowledged" replication loop is quadratic in ack lag, and it feeds the lag

Issue #1304 (an `LsmEngine` node at 2.7 GB RSS in 60 s) looked like a storage
problem and was a replication one. `RaftCore::replicate_to` shipped
`next_index..` on every propose, every success ack and every heartbeat, and
`next_index` only moves on an ack. With `w` entries in flight, each ack
provoked another `w`-entry message: 8.7 GB sent in 20 s for 0.2 GB of entries,
450-700 MB of encoded frames parked in `send_stream` tasks, and a saturated
peer acked later, so the load that slowed it made the leader send it more.

What found it, in order (each step cheap, none needed a heap profiler):

1. **Sample RSS and the structure sizes together.** The RSS slope alone said
   nothing; adding `apply_lag_sum` / `log_len_sum` to the rig's `BP-LIVE` line
   and a temporary counter of bytes handed to `send_stream` showed the wire,
   not the engine, moved first.
2. **Count bytes per logical entry, not bytes.** 30 entry-sends per entry
   written is unmistakable; a bare "11 GB sent" is not.
3. **massif on a scaled-down run** (valgrind needs `--max-threads=3000` for a
   tokio blocking pool) named the allocation site (`encode_for_send` ->
   `put_bytes`); a path-aware parse of the tree (innermost app frame of each
   leaf) beat reading `ms_print` by eye.
4. **Change one thing and re-measure** before polishing: a batch cap of 16
   instead of 512 did nothing (the amplification was in the number of sends,
   not their size), which ruled out the obvious knob.

Two traps in the fix, both caught only because the sim corpora are large:

- **Do not turn it into a classic pipeline** (`prev = last sent`). A reordered
  or lost message becomes a reject, the reject arm does `next_index -= 1`, and
  the simulator's jitter walked a slow voter down into `InstallSnapshot` (43
  installs vs a bound of 10). Keep every message anchored at the acknowledged
  position and stop *sending* once the window is deep.
- **Leave the shallow case byte-identical.** Throttling only at depth >= 4
  kept the pinned election-backpressure digests, the one-round-trip write
  latency and `release_race_corpus` timing unchanged; the first, unconditional
  version broke six unrelated tests whose timing assumed the old resend.

A follow-up bug the change exposed (a follower committing a stale suffix
because its commit was `min(leader_commit, own last index)` instead of
`min(leader_commit, last entry the message vouched for)`) was invisible for
years because the sender always overwrote the suffix first. When you stop a
sender from "helpfully" repeating itself, audit every receiver rule that relied
on the repetition.
