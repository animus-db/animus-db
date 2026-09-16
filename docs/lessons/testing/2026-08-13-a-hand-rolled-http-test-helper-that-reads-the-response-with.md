# A hand-rolled HTTP test helper that reads the response with `read_to_end` MUST send `Connection: close` — an HTTP/1.1 request without it deadlocks against a keep-alive server, and the hang lands on the *first* request, before any of the test's own bounded assertions can fire.

**A hand-rolled HTTP test helper that reads the response with
`read_to_end` MUST send `Connection: close` — an HTTP/1.1 request without
it deadlocks against a keep-alive server, and the hang lands on the
*first* request, before any of the test's own bounded assertions can
fire.** The server (correctly, per HTTP/1.1 defaults) keeps the connection
open and parks waiting for a next request; the helper waits for EOF that
never comes. The ADR 0041 GSI-drain e2e test shipped with exactly this
bug in its `dynamo()` helper — every *other* dynamo test's helper sends
`Connection: close`, this one was written fresh and dropped it — and the
result was a test that hung ~47 minutes (until externally killed) at
`CreateTable`, while masquerading as the drain bug it existed to expose.
Corollary, the meta-lesson that cost the real time: **a WIP handoff's
"known broken" note describes the last run its author observed, not
necessarily the committed code — re-verify the recorded failure signature
(run the test, watch *where* it stops) before debugging from the note.**
The note said "times out waiting for the first index rows" (a clean 30s
bounded panic); the committed helper couldn't even reach that assertion.
The two bugs were independent, and fixing the noted one first while the
unnoted one hid behind it turned a 30-second failure into an apparent
hang. (`animusd` `tests/dynamo_gsi_drain.rs`, 2026-08-13.)
