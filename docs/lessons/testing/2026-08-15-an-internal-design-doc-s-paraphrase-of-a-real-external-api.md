# An internal design doc's paraphrase of a real external API's response shape is not the API — verify against the real shape before wiring a field, even when the doc sounds precise.

**An internal design doc's paraphrase of a real external API's response
shape is not the API — verify against the real shape before wiring a
field, even when the doc sounds precise.** ADR 0045 §6 sketched
`DescribeTable`'s new `Backfilling: bool` as a **table-level** flag ("any
index `Creating`"); real DynamoDB places `Backfilling` **inside each
`GlobalSecondaryIndexes[]` entry**, and only while that specific index is
backfilling (the attribute is *absent*, never `false`, once finished).
Building PR6 to the doc's wording as written would have shipped a
plausible-looking but wrong wire shape no test would have caught, since
every test in the same PR would have been written against the same wrong
premise. Caught only because the task brief explicitly flagged the
wording as "looser than AWS reality" and asked for the real shape to be
checked — worth generalizing: **a design doc is a plan, not a spec of an
external contract it merely describes; re-derive the actual third-party
shape independently (from real API docs/behavior) rather than trusting a
plan's summary of it**, the same way this codebase already insists on
reading ADR text as *rationale*, not as the mechanism's ground truth.
(`animus-dynamo/src/wire.rs`'s `index_desc`/`table_description_object`,
2026-08-15.)
