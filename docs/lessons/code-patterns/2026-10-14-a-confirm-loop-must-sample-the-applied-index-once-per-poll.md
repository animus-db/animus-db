# A confirm loop must sample the applied index once per poll, or a just-applied own entry reads as "superseded" and the retry double-applies.

Issue #1308 (`cascade_split_walks_...` in `streams_e2e`, `delivered=121 expected=120`, 1 in ~30 under CPU hogs): one item had two change records in adjacent epochs. Not a seal bug: the write itself was applied twice.

The apply task records an entry's outcome (`kind_batch_outcome`) BEFORE it publishes `engine_applied_index` (the "effects readable" signal). The `cp_kind_raw_local` / `cp_kind_eval_local` / `cp_kind_eval_batch_local` loops read `engine_applied_index()` once for `classify_kind_batch_outcome` (behind the index: own `Applied` recorded but not readable, so `Inconclusive`), read the outcome, then read `engine_applied_index()` a SECOND time for `kind_batch_confirm_superseded`. Under load the index advances between the two reads; the second one reports "applied past my index, outcome not confirmed" and the loop returns the `superseded ... retry` error for an entry that had just applied. The retry applies the write again.

Rule: any poll that makes two decisions from a moving counter (confirm, then superseded) must take ONE sample and feed both (`animusd::kind_poll`). The outcome must be read after the sample (outcome-before-index publish order then guarantees an at-or-past-index sample sees its own outcome). `poll_probe`, and the put/delete local paths (ClientCtx cp_put_local, cp_delete_local), had a re-check before giving up and were not affected.

Symptom to remember: an exactly-once violation with distinct event ids and adjacent HLCs for one item is a double-APPLIED write (client-visible success path retried server-side), not a double-sealed segment.
