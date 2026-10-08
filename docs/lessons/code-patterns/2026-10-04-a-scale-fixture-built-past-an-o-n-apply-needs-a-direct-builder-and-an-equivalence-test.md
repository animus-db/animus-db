# A scale fixture built past an O(N)-per-call apply needs a direct builder and an equivalence test

**Context.** C-17 Tier 1 needed `Metadata` with 50,000 tablets. `Metadata::apply(CreateTablet)` does
`tablets_for_table(..)` (a full-map filter) and `next_free_tablet_id()` (a full-key `max`) on every call,
so building N tablets through the real command path is O(N²) (2.5e9 steps at 50k, minutes in a debug
build). The fix was not to skip the real path but to bypass only the O(N) command: insert `Tablet`
rows directly (the fields are `pub`), and drive members, schemas and policies through the real
`apply`.

**Lessons.**
- A builder that bypasses `apply` measures a fiction unless something pins it to the real thing. The
  module carries `sim_cluster_scale_builder_matches_real_apply_path`: at N=40 it builds the same
  cluster both ways and asserts byte-equal `serde_json` of `Metadata` **and** byte-equal system-keyspace
  image rows (derived from the state vs. accumulated from `mirror::apply_and_derive_mirror` writes).
  A future field or mirror row makes that test fail instead of silently skewing every size.
- The O(N)-per-apply cost is itself a finding (control-plane apply of `CreateTablet` scales with the
  tablet count); note it rather than hiding it behind the faster builder.
- Pick the measured function per question: image size from the real `encode_syskv_image_bytes`, delta size
  from the real `apply_and_derive_mirror`, plan cost from the real `host::plan`, chunk count from a real
  `RaftCore` pump — never a re-derived formula.
