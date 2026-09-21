# An internal `"; retry"` convention and a client-facing status code are different layers — map between them only at the terminal return, never by touching the retry condition

**Context (issue #994).** `decide::read_should_retry` (the house
convention: an error is transient iff its message ends `"; retry"`) is
what every retry loop in `animusd` — `write_path.rs::cp_kind_write_item`,
`cp_kind_write_raw_bounded`, `forward_to_tablet_leader`, and others — uses
to decide *whether to keep trying*. That convention was working exactly as
designed: a split-cutover freeze (`decide::FROZEN_REFUSAL`) or an exhausted
forward chase (`forwarding::FORWARD_BUDGET_EXHAUSTED`) both carry the
suffix, so every retry loop correctly kept retrying them. The bug was one
layer further out: once a loop's own bounded budget ran out on one of
these still-transient errors, the terminal `Err` it returned kept
`WireError::internal`'s `InternalServerError` code, which the DynamoDB
wire edge renders as a bare `500` — a terminal-looking code for a
condition that was never permanent, and one no AWS SDK's default retry
policy treats specially.

**The lesson.** A caller-facing HTTP/wire status code and an internal
"should my own loop retry this" convention are two independent contracts,
even when they're read off the very same string. Conflating them — e.g.
by trying to make the retry loop itself "succeed" differently on
exhaustion, or by changing what counts as `"; retry"` — is the wrong fix
and risks changing retry *behavior*, which was already correct. The right
fix is a **pure mapping step at the terminal return, after the retry
decision has already been made**: if the last error the loop is about to
give up on is still transient by the internal convention, translate it
into the caller-facing code that means "transient, please retry" in the
*caller's* vocabulary (here, `WireError::service_unavailable`, DynamoDB's
own documented `ServiceUnavailable`/503) instead of the generic internal
one. Nothing about *when* to keep retrying changes; only what the giving-up
case reports downstream.

**Why this generalizes.** Any place a bounded retry loop's own internal
"is this worth retrying" signal doubles as (or gets wrapped into) a
value that later crosses a trust/protocol boundary — a wire status code,
an exit code, a queue's own dead-letter reason — has the same seam: the
loop's *retry condition* and the boundary's *terminal reporting* are
separate decisions, and the fix for "the boundary reports this wrong"
almost never belongs inside the retry condition itself. Grep every site
that already classifies a message as transient (`decide::
read_should_retry`'s own callers, in this case) before assuming there is
only one place to fix — this bug had two independent terminal-return
sites (`write_path.rs::cp_kind_write_item`'s own loop exit, and
`dynamo.rs::map_throttleable_error`, the shared mapping point for five
other plain-`String`-error primitives) that both needed the identical,
independently-applied translation.
