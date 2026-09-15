# A `Local` control/consensus handle's own "not leader" refusal carries no retry-able address — only a `Remote` mirror's does (S-07d, `POST /admin/control/member/add`)

Designing S-07d's "retry a `member/add` call against the leader when the
first-tried voter refuses" step, the natural instinct (mirroring a human
operator's own runbook, or the admin dashboard's "not leader" message,
which *does* carry a `leader_addr_hint`) was to parse an address out of
the refusal and retry against it. That hint is populated from
`ControlHandle::leader_addr_hint()`, which is **always `None` for
`Local`** — a genuine control-plane voter has no separate notion of "the
leader's own address" the way a `Remote` data-only node's mirror does
(that method's own doc: "always `None` for `Local` — a genuine control
voter has no separate notion of the leader's client address; callers that
need one resolve it via `ClientCtx::route_addr` on `leader()`'s id").
Every pod `member/add` is ever called against in this design (any
already-confirmed voter ordinal) is genuinely `Local` — so the retry
target this design needed simply has no address-hint mechanism available
at all, despite one existing (and working) elsewhere in the same admin
surface for a different node role.

The fix that shipped (`add_control_voter`: try every already-confirmed
voter ordinal in turn, stopping at the first 2xx, relying on the call's
own documented idempotence to make trying the "wrong" ones first free) is
simpler than address-hint parsing would have been anyway, but the lesson
is the general one: **an admin/RPC action's "who do I retry against"
answer can differ by which *role* is refusing** — a `Remote` mirror and a
`Local` voter can both return the identical-looking "not leader" error
text/shape while only one of them can name a next hop. Check the actual
handle variant an automated caller will hit (not just the human-facing
error message shape) before designing a retry strategy around an address
hint.
