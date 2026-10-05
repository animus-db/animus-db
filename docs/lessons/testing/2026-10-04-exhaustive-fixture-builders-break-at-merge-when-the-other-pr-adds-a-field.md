# Exhaustive fixture builders break at merge when the other PR adds a field

Two PRs touched the same wire enum from opposite sides: one added an additive
`#[serde(default, skip_serializing_if)]` field (`JoinInfo.cluster_version`), the
other added a fixture test that builds every variant with struct literals. Each
was green alone; the merge did not compile (E0063). Git reported no conflict.

- A textually clean merge is not a green merge: build `--all-targets` before
  running anything, including the test target the other PR added.
- The fix for a skipped-at-default field is to set it to the default in the old
  fixture builder; the Phase 1 bytes must not move, and the byte-identity assertion
  proves that.
- When two PRs disagree on a classification pin (here, whether
  `FinalizeClusterVersion` is relayable), the pin test is where it surfaces as a
  semantic conflict; resolve the behaviour first, then rename the pin so it states
  the decision rather than the old one.
