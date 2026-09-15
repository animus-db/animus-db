# A rare, load-sensitive flake needs a matched A/B, not a bigger N on one side.

**A rare, load-sensitive flake needs a matched A/B, not a bigger N on one
side.** "6/6 clean on base, 1/3 failing on the branch" from small samples
on a shared, variably-loaded machine is exactly the pattern ambient
contention produces; the discriminating test is running the same stress
harness (e.g. 4 parallel copies of the compiled test binary) back-to-back
on both trees and comparing rates. Concrete case: `animusd`'s
`dynamo_query_pagination::final_page_carries_no_last_evaluated_key`
looked train-caused at small N, then reproduced at ~0.6% on both trees
under the matched harness — the real mechanism was the ADR 0055
`ConsistentRead: false` replica-local read racing the last write's apply,
i.e. a pre-existing race, not the refactor under suspicion.
