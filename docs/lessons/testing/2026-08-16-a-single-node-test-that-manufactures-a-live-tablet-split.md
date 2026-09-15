# A single-node test that manufactures a live tablet split *mid-write-burst* needs its own client-side retry, matching the discipline a real multi-node routed client already gets for free.

**A single-node test that manufactures a live tablet split *mid-write-burst*
needs its own client-side retry, matching the discipline a real
multi-node routed client already gets for free.** Found writing the ADR
0044 phase-1 PR6 sweeper-skip regression
(`a_rewoken_tablet_is_picked_back_up_by_every_sweeper_within_one_interval`):
a 40-write burst against a tiny `--auto-split-bytes` threshold legitimately
splits partway through, and a later write in the same burst can target a
key the split just handed to a fresh child tablet — surfacing as a
perfectly correct `"kind write outside this group's live range; retry"`
rejection (the ADR 0028 write fence doing its job). A real deployment
never notices this: `ClientCtx::cp_route`/`cp_forward`'s hinted-retry
re-resolves the tablet on every attempt. A single-node test driving the
wire API directly against one fixed address has no such re-resolution
layer, so its own write helper must retry on this specific, already-
retryable-shaped error (`"; retry"`) rather than hard-asserting `200` —
never by loosening the shared helper every *other* test also uses (that
would mask a genuine regression elsewhere), but with a locally-scoped
retry loop in the one test that deliberately invites the race. The same
class as the pre-existing, tracked "CreateTable first-write race" (`200`
from `CreateTable` doesn't mean the tablet is ready for a concurrent first
write) — any test that deliberately drives a live topology change under
concurrent writes needs this discipline, not just the specific two cases
found so far.
