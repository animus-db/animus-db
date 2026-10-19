# A/B a suspected-regression flake with separated binaries and a direct measurement

Issue #1341 follow-up. A rare `upgrade-previous-release` failure (a follower
~1100 entries behind for 300 s) was suspected to be the LSM flush-lane PR.
Three things made the early local evidence worthless, and one made the
conclusion possible:

- **Two A/B arms that share `target/debug/animusd` are one arm.** Rebuilding
  between runs overwrote the binary the other arm was using. Copy each arm's
  binaries to their own directory and pass them explicitly
  (`ANIMUS_UPGRADE_CURRENT_BIN`, `ANIMUS_CLI_BIN`); log the binary's hash.
- **Measure the suspected component directly.** An `#[ignore]`d ProdEnv test
  (`animus-storage/tests/lsm_apply_catchup_prod.rs`) drives a follower-style
  sequential applier against the PR and `origin/main` `lsm.rs`, plain and with
  `CATCHUP_BG=8` sibling engines or `CATCHUP_W2=1` (second writer). The PR was
  faster in every variant (main's tail stalls reached 1-2 s: the flush it
  skipped under in-flight applies is the starvation #1341 fixes), so the lane
  split cannot explain a 60 entries/s follower.
- **Compare the symptom's distribution, not one pass/fail.** The report JSON's
  `restart_to_healthy_ms` ranges 2 s to 70 s on both arms; a rare 300 s outlier
  is the tail of a heavy-tailed catch-up, not a regression signal by itself.
- **Fewer flushes is not "faster"**: main flushed about half as often as the
  64 KiB threshold implies only because it skipped flushes while an apply was
  in flight. Compare flush counts against the threshold, not against main.
