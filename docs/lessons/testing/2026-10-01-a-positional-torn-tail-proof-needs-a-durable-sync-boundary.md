# A "valid frame follows, so it is not a torn tail" proof is only sound if at most one frame can be un-synced; otherwise the reader needs a durable sync boundary.

**A "valid frame follows, so it is not a torn tail" proof is only sound if
at most one frame can be un-synced; otherwise the reader needs a durable
sync boundary.** The 2026-08-10 lesson (`distinguishing-crash-torn-tail-from-
mid-file-corruption`) says: tolerate a bad frame only if no later valid
checksummed frame exists. That rests on "a crash tears only the physical end",
which is true, but the *un-synced end* can hold many complete frames: a persist
round appends N records and syncs once, and `animus-sim`'s `corrupt_on_crash`
flips one byte anywhere in the kept prefix of that region. So a correct writer
plus a crash legitimately produces "bad frame, then valid frame". Issue #1132
(the line-framed `CWL1`/`SWL1` WALs silently dropping everything after a rotted
early line) could not be fixed by copying the LSM rule: measured over 300 crash
seeds with a correct writer, 72 left a CRC-bad line followed by a valid one (49
of them the first line). The fix is a durable boundary the reader can trust:
after every `fsync` that returns `Ok`, append a CRC-checked marker line
carrying the file offset it sits at; a bad line before a valid marker is
corruption, anything after the last marker is a tail. Rules that fell out of
it: write the marker **after** the sync (a pre-sync marker can survive a
kept-prefix tear next to a flipped byte in the same round); scan past the first
bad line for markers (or a rotted first line hides every proof); cut the torn
tail back on open (or later appends sit after garbage and the next recovery
refuses the file); a disk that lied about `fsync` and lost acked bytes now fails
loudly, which is correct. The LSM WAL's resync had the same
exposure and it was **confirmed** (76/300 seeds, issue #1142; fixed with the
same marker design, see
`2026-10-03-a-fault-harness-must-buffer-more-than-one-unsynced-frame.md`). How to verify a proof is sound: write
the probe first (N appends, crash with the faults armed, count seeds where the
"proof" fires on a correct writer) before designing the rule.
