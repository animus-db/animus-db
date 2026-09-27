# An aggregate byte cap belongs at whichever side of the operation — request or response — is where AWS's own accounting actually happens, not reflexively at decode time.

**An aggregate byte cap belongs at whichever side of the operation —
request or response — is where AWS's own accounting actually happens, not
reflexively at decode time.** `TransactWriteItems` and `TransactGetItems`
share one constant (`MAX_TRANSACT_BYTES`, ADR 0072) but need two different
enforcement sites: a `TransactWriteItems` request already carries every
`Put`'s full item on the wire, so it's checkable — and rejectable — at
decode, before any I/O (`animus_dynamo::wire::decode_transact_write`). A
`TransactGetItems` request carries only keys (at most 100 of a few KB each,
which can never reach the 4 MiB cap on its own); DynamoDB's real rule there
is on the *fetched result*, so the only place to check it is after the read,
in `animusd::dynamo::run_transact_get` — checking a `TransactGetItems`
request at decode would be enforcing a cap that request can never trip,
giving false confidence that the limit is covered. Before wiring a cap
shared by a read and a write operation, work out which one's *request*
actually carries the size and which one's *response* does, rather than
copying the sibling's enforcement site.

A related shape on the same task: `BatchGetItem`'s 16 MiB **response** cap
is not an error at all — real DynamoDB pages the response and returns the
overflow in `UnprocessedKeys`, the same shape it already uses for a
per-key throttle refusal. Recognizing "this cap's AWS behavior is a
degraded-but-successful response, not a `ValidationException`" up front
avoids reaching for the wrong error-handling shape (`Result`-based
rejection) for a limit that isn't actually a rejection.

Also worth checking before assuming a "stop the fan-out early instead of
cutting the result after the fact" byte-budget implementation is free: it's
only free when the fetch loop is already sequential. `animusd`'s
`BatchGetItem` arm reads one key at a time (never a concurrent fan-out), so
latching a `budget_exhausted` flag and skipping the read entirely for every
key after the cap trips costs nothing extra to check — a concurrent
fan-out would have had to eagerly launch every read and cut the *result*
instead, since there'd be no cheap way to stop reads already in flight.
