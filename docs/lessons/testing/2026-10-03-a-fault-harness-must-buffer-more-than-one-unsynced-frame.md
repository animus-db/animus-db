# A torn-tail proof is only tested if the fault harness leaves more than one un-synced frame behind.

**A torn-tail proof is only tested if the fault harness leaves more than one
un-synced frame behind.** The LSM WAL decoder tolerated a bad frame only if no
valid frame followed it. That rule is sound for one un-synced frame and unsound
for a coalesced group-commit batch (several writers' frames, one append, one
fsync): `corrupt_on_crash` flips one byte anywhere in the kept prefix of the
un-synced region, so a correct writer plus a crash yields "bad frame, valid
frame" and recovery was refused (issue #1142: 76 of 300 seeds with 1..=7
coalesced writers). `lsm_crash` and `lsm_disk_faults` had armed both faults for
months and stayed green at 300 seeds, because every cell buffered exactly one
frame (`buffer_unsynced_wal_record`, or one failing `put`). The lesson from
#1132 ("check the LSM WAL, untested against this fault shape") sat as
"unconfirmed" until a probe was written.

Rules: (1) when a proof says "at most one X can be in flight", write the test
that puts N of them in flight (here: N spawned writers whose group fsync fails,
then crash with tear+corrupt) and count the proof's false positives over a few
hundred seeds *before* designing the fix. (2) Read the fault model before
classifying refusals: `Simulator::crash` only ever damages the retained
un-synced region, so any refusal on a correct writer is a bug; corruption of
already-synced bytes needs `corrupt_durable` and must stay a refusal (keep both
tests: the tear reopens, the synced-frame flip is refused). (3) The fix is the
#1132 shape: a CRC-checked marker written only after a successful sync, here
piggybacked on the next batch's append so no extra fsync is paid; a segment
whose header says v1 keeps the old lenient rule and gets no markers.
