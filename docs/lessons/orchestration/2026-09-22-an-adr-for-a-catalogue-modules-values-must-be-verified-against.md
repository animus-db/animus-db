# An ADR documenting a constants catalogue must re-verify every value against the code, not trust the task prompt's summary, even when the prompt says the summary was "verified against the code by an earlier sweep."

Writing ADR 0072 (DynamoDB service limits), the task prompt supplied a
complete list of limits, their values, and which were enforced today, with
a note that this had already been checked against the code. Re-grepping
`crates/animus-dynamo/src/wire.rs`, `crates/animus-item/src/size.rs`, and
`crates/animus-item/src/numkey.rs` anyway confirmed every number (400 KB
item size, the 25/100/100/100/25/25 batch-family caps, 20 GSI/5 LSI, 38
significant digits, `ClientRequestToken` 1–36, `BackupName` 3–255,
`ProvisionedThroughput` ≥ 1, the 80,000/40,000 `DescribeLimits` figures,
the 5-year TTL guard) — but the check was cheap (a handful of targeted
greps) relative to the cost of an ADR asserting a wrong constant value
that then gets cross-referenced by three more layers of a stacked series
and a website page. An ADR is a load-bearing, long-lived document other
agents and PRs will cite by number; a constants table inside one is exactly
the kind of content where a stale or transposed digit is easy to write
confidently and hard to notice later, since nothing about the prose reads
as wrong. **General rule: when an ADR (or any doc) is going to assert a
concrete constant, limit, or code fact that already exists in the repo,
grep it and quote the real definition site even if the task description
already hands you the "correct" answer — the marginal cost is low and the
failure mode (a wrong number baked into a document everything downstream
treats as ground truth) is expensive.** This generalizes past ADRs to any
documentation task where the prompt pre-supplies facts about a codebase.
