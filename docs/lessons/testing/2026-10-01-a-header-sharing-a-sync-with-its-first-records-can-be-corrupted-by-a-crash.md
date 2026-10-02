# A format header that shares one `sync` with its first records can be torn *and corrupted* by a crash, so "bad header" is ambiguous unless the header is synced first.

**A format header that shares one `sync` with its first records can be torn
and corrupted by a crash, so "bad header" is ambiguous unless the header is
synced first.** The LSM WAL prepended `LWL1`+version to a segment's first
batch and synced once. `SimEnv::crash` with `torn_tail_on_crash` +
`corrupt_on_crash` keeps a strict prefix of the un-synced buffer and flips
one byte in it; when the buffer was a fresh segment, the flip landed in the
header (0x01 -> 0xFE, or the magic) and the strict reopen failed with
`UnsupportedFormatVersion { found: 254 }` / `PreBaselineFormat`: a node that
can never restart after a crash during segment creation. A decoder that only
tolerated a *true prefix* of the header could not tell this from rot.

Fix shape: make the distinction provable on the write side. Sync the header
alone before appending any record (one extra fsync per segment), so every
file longer than the header has a durable header and a bad one is real
corruption (stays loud); a shorter file never had its header synced and holds
no acked data, so the decoder accepts it as empty regardless of content (the
sim's flip lands in the kept prefix, which is <= 4 bytes for a header-only
write). Decoder-only relaxation would have been unsound.

Lessons: (1) an "intolerant decoder" error on a crash-recovery path needs a
corpus that opens engines *strictly* right after a corrupting crash; a
destroy-and-reopen fallback in a test helper masked this for the raftkv
corpus. (2) Tests that count `sync` calls by ordinal (`CrashEnv::new(.., n)`)
shift when a sync is added. (3) Check sibling formats for the same pattern
(control `CWL1`, `SharedWal`). Regression:
`animus-storage/tests/lsm_crash.rs::crash_during_segment_header_creation`.
