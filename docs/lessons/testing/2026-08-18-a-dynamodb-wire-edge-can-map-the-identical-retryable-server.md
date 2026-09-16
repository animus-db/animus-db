# A DynamoDB wire edge can map the identical retryable server-side condition to DIFFERENT HTTP status codes depending on which API it's surfaced through

**A DynamoDB wire edge can map the identical retryable server-side
condition to DIFFERENT HTTP status codes depending on which API it's
surfaced through** — `TransactWriteItems` maps a cancelled transaction to
**400** `TransactionCanceledException` (matching real AWS DynamoDB's own
convention), while `PutItem`/`Query`/the Streams read API map the same
underlying ADR 0050 F8 freeze→cutover blip to a plain **500**
`InternalServerError`. A test retry helper written against one shape
(retry-on-500) silently fails to mask the other — the transact call kept
one-shot-failing at ~1/15 even after every *other* one-shot assert in the
same test was fixed, until the retry predicate was taught to also accept
a 400 whose body is a `TransactionCanceledException` **and** whose own
message says "retry" (never a bare 400, which can also mean a genuine
condition-check failure that must still fail loudly). General form: don't
assume one status code covers "this operation's own documented retryable
window" across every API on the same wire edge — check what the specific
handler actually returns for the specific condition before writing (or
reviewing) a retry-on-status-code test helper.
(`animusd/tests/streams_e2e.rs::dynamo_retrying_transact`, issue #278.)
