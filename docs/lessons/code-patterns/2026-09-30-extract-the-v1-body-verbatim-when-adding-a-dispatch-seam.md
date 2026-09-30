# Adding a version-dispatch seam: move the v1 body verbatim, keep the error path shared

When introducing the `match version { 1 => decode_v1(..), v => Err(..) }` seam
(ADR 0073 Phase 1) to an already-shipping decoder, the safe refactor is a pure
move: the old post-header body becomes `decode_vN` untouched, and the
unsupported-version error is built in exactly one place (the `match` fallback,
or a shared helper such as sstable's `unsupported_format`, reused by the
open-time `check_format`). That keeps "same accepted inputs, same errors"
provable by the existing fixture and unsupported-version tests with no edits
to them — if you had to touch a test to make the seam pass, the refactor
changed behavior. Note the ordering constraints to preserve: sstable
`read_block` verifies the block CRC *before* consulting the format, and the WAL
/ manifest check magic (pre-baseline error) *before* the version.
