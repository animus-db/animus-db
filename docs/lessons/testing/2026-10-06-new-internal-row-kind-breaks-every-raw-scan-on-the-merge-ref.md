# A new internal row kind on main breaks every raw-scan consumer on a branch that passed alone

CI tests the PR merged with `main`, not the branch head. Main added the
`txn-resolved-marker` row (value leads `0xA1`, `txn::is_resolved_marker_key`)
and widened every client-facing scan to `txn::is_internal_key`. The G-d branch
had a new raw-scan consumer (`RaftKvNode::local_scan_for_ship`, the MREC
shipper window) that still filtered with `is_record_key` alone, so on the
merge ref it ran `decode_envelope` over a marker: `unknown envelope tag 161`,
in every `sim_world_mrec_corpus` cell at the default seed (`0x4D52_0000` =
1297219584; the seed was never different, the code was).

- "Passes locally at K=20" means nothing for a branch behind `main`: merge
  `main` (or test in a throwaway worktree of the merge) before debugging a CI-only
  failure. Diff `HEAD..origin/main` for new row kinds first.
- A new scan over engine rows filters with `is_internal_key`, never
  `is_record_key`; grep for `is_record_key` callers when a new internal kind lands.
- Regression: `animus-cp-data` `tests/it/mrec_ship_scan_markers.rs`.
