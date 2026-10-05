# A wall-clock confirm-latency bound over quorum fsyncs on a shared disk measures the disk too; instrument per-phase and attribute with an independent disk probe, never widen the limit (issue #1222, 2026-10-05)

**What happened.** `prod_compaction_persist_round`'s `WORST_CONFIRM` (1.5 s,
single-sample max over 400 writes) tripped once in CI at write 9 (1.87 s) --
before any compaction, with no code change on the write path. The failure
message carried only the number, so the stall could not be attributed.

**Why the test is sensitive.** A "confirm" is leader resolution + put +
3-node commit (leader and follower WAL `fsync`s, all on ONE filesystem in the
test) + ReadIndex read-back (20 ms `READ_POLL` quantum, so a healthy write is
~44 ms). Any stall of that filesystem's journal -- CI runs right after a heavy
`sudo rm -rf` on the same runner disk -- lands straight in the number. Nothing in
the release path can beat a multi-second `fsync`.

**How it was attributed.** The test now records per-write phase timings
(leader resolve, put accepted, commit-to-read-back, read-barrier time, read
count, leader index + term at put and read) and the term changes seen during
the run. Reproduction under contention (concurrent buffered `dd conv=fsync` +
create/`rm -rf`/`sync` loops on the same device) showed: no election (term
stays 1, same leader), `resolve`/`accepted` always microseconds, and the whole
stall inside `commit_to_readback`/`read_barrier` -- 250-350 ms on metadata
stress (clustered on the compaction rewrites, ~every 58 writes), 1.0-3.3 s
under buffered-writeback stress. Raising `APPLY_SAFETY_POLL` to 1 s did not move
the stalls, ruling out a lost apply wake.

**The fix is in what is measured.** A `DiskProbe` thread (own OS thread, same
temp tree, 1-byte `write + fsync` every 10 ms) records every interval where its
own fsync took >= 100 ms. A write whose window overlaps one is charged to the
disk (still bound by `WRITE_BUDGET`, and reported); every other write must beat
`WORST_CONFIRM`, unchanged at 1.5 s. A release-path regression (late ack, lost
wake) strands a write on a healthy disk, so it still fails. Under buffered
writeback stress the unattributed worst stayed at 130-200 ms while the overall
worst (the old metric) hit 1.0-1.3 s.

**Generalizes.** (1) A latency bound whose workload contains `fsync` needs a
way to tell "the code was slow" from "the disk was slow"; sample the disk
independently rather than loosening the bound. (2) Make a tripped bound print
its attribution (phases, leader/term, disk stalls) so one CI failure is
diagnosable. (3) The probe is a proxy: compaction rewrites (create + rename +
dir fsync) can stall longer than a 1-byte fsync, so sub-second residuals
without a probe overlap are expected under metadata stress.
