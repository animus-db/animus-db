# Double hashing only works if the two base hashes are independent

`lsm/bloom.rs` derived its `k` probes as `g_i = h1 + i*h2` (Kirsch-Mitzenmacher)
with `h1`/`h2` two FNV-1a passes that differed only in the 64-bit offset basis.
The measured false-positive rate at the 1 % sizing target was 3.4 % for random
keys and 4.1 % for sequential keys (issue #1389), and it looked like "just
Bloom variance".

Root cause: FNV-1a is `h = (h ^ byte) * P` per byte. Changing the offset basis
changes the starting state, but the same multiply-xor chain is applied to the
same bytes, so the two outputs differ by a key-structure-dependent but highly
regular relation; they are not independent. The Kirsch-Mitzenmacher guarantee
assumes `h1`, `h2` behave as independent uniform values, so correlated inputs
collapse the probe positions of different keys onto overlapping sets.

Fix: one FNV-1a pass seeds a SplitMix64 generator and its first two outputs are
`h1` and `h2 | 1` (bijective avalanche finalizer: no new dependency,
deterministic, platform independent). Measured at 100k keys: 0.98 % hashed,
1.01 % sequential.

Lessons:
- Seed variation of a non-avalanching hash is not a second hash function. Take
  both words from one strong mix of a single pass.
- A hash that decides what is in a persisted structure is part of the on-disk
  format. The old filters' bits were set by the old hash, so each filter now
  carries a scheme tag (manifest v2 `bloom_scheme`; v1 decodes to the legacy
  scheme and keeps being probed with it until compaction rewrites the table).
  Switching hashes without the tag would give false negatives (lost reads), not
  just a worse FP rate.
- Test the statistical property, not only "no false negatives": the FP-rate
  test uses hashed and sequential keys at 100k and a negative control showing
  the legacy scheme fails the same bound.
